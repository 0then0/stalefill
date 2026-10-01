// External validation fixture. Neither HTTP reads nor the loader wait for the
// background setter. The Redis hook records metadata only and never gates I/O.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/eko/gocache/lib/v4/cache"
	"github.com/eko/gocache/lib/v4/store"
	redisstore "github.com/eko/gocache/store/redis/v4"
	"github.com/redis/go-redis/v9"
)

const key = "stalefill:gocache:item:1"

type requestID struct{}

type event struct {
	Seq       int    `json:"seq"`
	ElapsedUS int64  `json:"elapsed_us"`
	Name      string `json:"event"`
	Request   int    `json:"request,omitempty"`
	Command   string `json:"command,omitempty"`
	Operation int    `json:"operation,omitempty"`
	Result    string `json:"result,omitempty"`
}

type observer struct {
	mu       sync.Mutex
	start    time.Time
	requests int
	events   []event
}

func (o *observer) record(ctx context.Context, name, command, result string, operation int) int {
	o.mu.Lock()
	defer o.mu.Unlock()
	id, _ := ctx.Value(requestID{}).(int)
	e := event{Seq: len(o.events) + 1, ElapsedUS: time.Since(o.start).Microseconds(), Name: name, Request: id, Command: command, Operation: operation, Result: result}
	o.events = append(o.events, e)
	return e.Seq
}

func (o *observer) nextRequest() int {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.requests++
	return o.requests
}

func (o *observer) snapshot() []event {
	o.mu.Lock()
	defer o.mu.Unlock()
	return append([]event{}, o.events...)
}

func (o *observer) DialHook(next redis.DialHook) redis.DialHook { return next }
func (o *observer) ProcessPipelineHook(next redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return next
}
func (o *observer) ProcessHook(next redis.ProcessHook) redis.ProcessHook {
	return func(ctx context.Context, cmd redis.Cmder) error {
		// Ignore connection handshakes. Never record keys, arguments or values.
		name := cmd.Name()
		if name != "get" && name != "set" && name != "del" {
			return next(ctx, cmd)
		}
		operation := o.record(ctx, "redis_started", name, "", 0)
		err := next(ctx, cmd)
		result := "ok"
		if errors.Is(err, redis.Nil) {
			result = "miss"
		} else if err != nil {
			result = "error"
		}
		o.record(ctx, "redis_completed", name, result, operation)
		return err
	}
}

func main() {
	redisAddr := flag.String("redis", "127.0.0.1:6380", "StaleFill proxy address")
	listen := flag.String("listen", "127.0.0.1:8080", "HTTP address")
	protocol := flag.Int("resp", 2, "RESP protocol")
	flag.Parse()
	o := &observer{start: time.Now()}
	client := redis.NewClient(&redis.Options{Addr: *redisAddr, Protocol: *protocol, DialTimeout: 2 * time.Second, ReadTimeout: 2 * time.Second, WriteTimeout: 2 * time.Second})
	client.AddHook(o)
	defer client.Close()
	var authoritative struct {
		sync.RWMutex
		value string
	}
	authoritative.value = "V1"
	snapshot := func() string {
		authoritative.RLock()
		defer authoritative.RUnlock()
		return authoritative.value
	}
	loadable := cache.NewLoadable[string](func(ctx context.Context, _ any) (string, []store.Option, error) {
		value := snapshot()
		o.record(ctx, "loader_completed", "", "", 0)
		return value, nil, nil
	}, cache.New[string](redisstore.NewRedis(client)))
	mux := http.NewServeMux()
	respond := func(w http.ResponseWriter, v any) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(v)
	}
	mux.HandleFunc("GET /health", func(w http.ResponseWriter, r *http.Request) {
		respond(w, map[string]bool{"ready": true})
	})
	mux.HandleFunc("GET /diagnostics", func(w http.ResponseWriter, r *http.Request) {
		respond(w, o.snapshot())
	})
	mux.HandleFunc("GET /authoritative/item", func(w http.ResponseWriter, r *http.Request) {
		respond(w, map[string]string{"value": snapshot()})
	})
	mux.HandleFunc("GET /item", func(w http.ResponseWriter, r *http.Request) {
		ctx := context.WithValue(r.Context(), requestID{}, o.nextRequest())
		o.record(ctx, "http_get_started", "", "", 0)
		defer o.record(ctx, "http_get_handler_returned", "", "", 0)
		value, err := loadable.Get(ctx, key)
		if err != nil {
			http.Error(w, "cache read failed", http.StatusBadGateway)
			return
		}
		o.record(ctx, "loadable_get_returned", "", "", 0)
		respond(w, map[string]string{"value": value})
	})
	mux.HandleFunc("PUT /item", func(w http.ResponseWriter, r *http.Request) {
		var input struct {
			Value string `json:"value"`
		}
		if json.NewDecoder(http.MaxBytesReader(w, r.Body, 4096)).Decode(&input) != nil || (input.Value != "V1" && input.Value != "V2") {
			http.Error(w, "expected V1 or V2", http.StatusBadRequest)
			return
		}
		ctx := context.WithValue(r.Context(), requestID{}, o.nextRequest())
		authoritative.Lock()
		authoritative.value = input.Value
		authoritative.Unlock()
		o.record(ctx, "authoritative_changed", "", "", 0)
		if err := loadable.Delete(ctx, key); err != nil {
			http.Error(w, "cache invalidation failed", http.StatusBadGateway)
			return
		}
		o.record(ctx, "loadable_delete_returned", "", "", 0)
		respond(w, map[string]string{"value": snapshot()})
	})
	server := &http.Server{Addr: *listen, Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	serveDone := make(chan error, 1)
	go func() { serveDone <- server.ListenAndServe() }()
	select {
	case <-ctx.Done():
	case err := <-serveDone:
		if !errors.Is(err, http.ErrServerClosed) {
			fmt.Fprintln(os.Stderr, err)
		}
	}
	shutdown, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = server.Shutdown(shutdown)
	// Lifecycle cleanup only, after all CLI probes have finished. Close never
	// runs inside GET/PUT/prepare, and is not used to repair the async schedule.
	_ = loadable.Close()
	_ = json.NewEncoder(os.Stdout).Encode(o.snapshot())
}
