package scenario

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/0then0/stalefill/internal/resp"
	"github.com/0then0/stalefill/internal/testredis"
)

// This fixture intentionally returns without joining its publication worker.
// Unit tests in episode_test force both lifetime orderings using events; this
// test verifies the entire baseline/race/oracle path over HTTP and the proxy.
func TestDetachedRunner(t *testing.T) {
	for _, fixed := range []bool{false, true} {
		t.Run(fmt.Sprintf("fixed=%v", fixed), func(t *testing.T) {
			upstream := testredis.Start(t)
			var addr string
			var authoritative atomic.Int64
			var workers sync.WaitGroup
			query := func(args ...string) (resp.Frame, error) {
				conn, err := net.DialTimeout("tcp", addr, time.Second)
				if err != nil {
					return resp.Frame{}, err
				}
				defer conn.Close()
				conn.SetDeadline(time.Now().Add(time.Second))
				if _, err = conn.Write(resp.Encode(args...)); err != nil {
					return resp.Frame{}, err
				}
				return resp.Read(bufio.NewReader(conn))
			}
			app := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				value := authoritative.Load()
				var err error
				switch r.URL.Path {
				case "/prepare", "/write":
					value = 1
					if r.URL.Path == "/write" {
						value = 2
					}
					authoritative.Store(value)
					_, err = query("DEL", "product:42")
				case "/item":
					var f resp.Frame
					f, err = query("GET", "product:42")
					if err == nil {
						if !f.Null {
							value, err = strconv.ParseInt(string(f.Data), 10, 64)
						}
						if f.Null || fixed && value != authoritative.Load() {
							value = authoritative.Load()
							workers.Add(1)
							go func(snapshot int64) {
								defer workers.Done()
								_, _ = query("SET", "product:42", strconv.FormatInt(snapshot, 10))
							}(value)
						}
					}
				case "/authoritative":
				default:
					http.NotFound(w, r)
					return
				}
				if err != nil {
					http.Error(w, "cache error", 502)
					return
				}
				json.NewEncoder(w).Encode(map[string]int64{"version": value})
			}))
			defer func() { app.Close(); workers.Wait() }()
			addr = freeAddress(t)
			var c Config
			if err := json.Unmarshal([]byte(Template), &c); err != nil {
				t.Fatal(err)
			}
			c.Redis.Listen, c.Redis.Upstream = addr, upstream
			c.Prepare.URL, c.Write.URL = app.URL+"/prepare", app.URL+"/write"
			c.Read.URL, c.Verify.URL = app.URL+"/item", app.URL+"/item"
			c.Authoritative.URL = app.URL + "/authoritative"
			for i := 0; i < 24; i++ {
				r := Run(context.Background(), c, false)
				want := Fail
				if fixed {
					want = Pass
				}
				if r.Outcome != want {
					t.Fatalf("run %d: %+v", i, r)
				}
				var reads, baselineFills int
				for _, event := range r.Events {
					if event.Event == "read_completed" {
						reads++
					}
					if event.Event == "baseline_publication_completed" {
						baselineFills++
					}
				}
				if reads != 1 || baselineFills != 2 {
					t.Fatalf("lost/double-consumed reader or undrained baseline: %v", logicalEvents(r))
				}
				workers.Wait()
			}
		})
	}
}
