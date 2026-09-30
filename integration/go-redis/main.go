// Minimal equivalent of the official hash cache-aside wire pattern. The
// authoritative store is a local fixture; no sleeps or proxy hooks coordinate it.
package main

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"flag"
	"log"
	"net/http"
	"strconv"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
)

func main() {
	addr := flag.String("redis", "", "proxy address")
	listen := flag.String("listen", "", "HTTP address")
	mode := flag.String("mode", "broken", "broken or fixed")
	protocol := flag.Int("resp", 3, "RESP mode (client default is 3)")
	flag.Parse()
	client := redis.NewClient(&redis.Options{Addr: *addr, Protocol: *protocol})
	defer client.Close()
	const key = "product:42"
	const lockKey = "lock:product:42"
	acquire := redis.NewScript("return redis.call('SET',KEYS[1],ARGV[1],'NX','PX',ARGV[2]) and 1 or 0")
	release := redis.NewScript("if redis.call('GET',KEYS[1]) == ARGV[1] then return redis.call('DEL',KEYS[1]) end return 0")
	var mu sync.Mutex
	version := 1
	snapshot := func() int { mu.Lock(); defer mu.Unlock(); return version }
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		respond := func(v int) { json.NewEncoder(w).Encode(map[string]int{"version": v}) }
		fail := func() { http.Error(w, "fixture cache operation failed", 502) }
		if r.URL.Path == "/health" {
			w.Write([]byte(`{"ready":true}`))
			return
		}
		if r.URL.Path == "/authoritative/42" && r.Method == "GET" {
			respond(snapshot())
			return
		}
		if r.URL.Path != "/items/42" {
			http.NotFound(w, r)
			return
		}
		ctx := r.Context()
		if r.Method == "PUT" {
			var input struct {
				Version int `json:"version"`
			}
			if json.NewDecoder(http.MaxBytesReader(w, r.Body, 4096)).Decode(&input) != nil {
				w.WriteHeader(400)
				return
			}
			mu.Lock()
			version = input.Version
			mu.Unlock()
			if client.Del(ctx, key).Err() != nil {
				fail()
				return
			}
			respond(snapshot())
			return
		}
		if r.Method != "GET" {
			w.WriteHeader(405)
			return
		}
		cached, err := client.HGetAll(ctx, key).Result()
		if err != nil {
			fail()
			return
		}
		value, hit := 0, len(cached) > 0
		if hit {
			value, err = strconv.Atoi(cached["version"])
			if err != nil {
				fail()
				return
			}
			if *mode == "fixed" && value != snapshot() {
				hit = false
			}
		}
		if !hit {
			var tokenBytes [16]byte
			if _, err = rand.Read(tokenBytes[:]); err != nil {
				fail()
				return
			}
			token := hex.EncodeToString(tokenBytes[:])
			ok, err := acquire.Run(ctx, client, []string{lockKey}, token, 30000).Int()
			if err != nil || ok != 1 {
				fail()
				return
			}
			defer release.Run(ctx, client, []string{lockKey}, token)
			value = snapshot()
			pipe := client.TxPipeline()
			pipe.Del(ctx, key)
			pipe.HSet(ctx, key, "version", value)
			pipe.Expire(ctx, key, 5*time.Second)
			if _, err = pipe.Exec(ctx); err != nil {
				fail()
				return
			}
		}
		respond(value)
	})
	log.Fatal(http.ListenAndServe(*listen, handler))
}
