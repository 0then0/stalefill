package scenario

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/0then0/stalefill/internal/resp"
	"github.com/0then0/stalefill/internal/testredis"
)

func TestRealOpaqueHashPublicationCannotPass(t *testing.T) {
	upstream := os.Getenv("REDIS_TEST_ADDR")
	if upstream == "" {
		t.Skip("set REDIS_TEST_ADDR for opaque hash publication regression")
	}
	info := testredis.Command(t, upstream, "COMMAND", "INFO", "HSETEX")
	if len(info.Items) != 1 || info.Items[0].Null {
		t.Skip("server does not implement HSETEX")
	}
	for _, regex := range []bool{false, true} {
		for _, target := range []bool{false, true} {
			t.Run(fmt.Sprintf("regex=%v/target=%v", regex, target), func(t *testing.T) {
				const key = "stalefill:protocol:opaque-hash"
				const other = key + ":other"
				testredis.Command(t, upstream, "DEL", key, other)
				t.Cleanup(func() { testredis.Command(t, upstream, "DEL", key, other) })
				addr := freeAddress(t)
				var authoritative atomic.Int64
				query := func(args ...string) (resp.Frame, error) {
					conn, err := net.DialTimeout("tcp", addr, time.Second)
					if err != nil {
						return resp.Frame{}, err
					}
					defer conn.Close()
					conn.SetDeadline(time.Now().Add(5 * time.Second))
					if _, err = conn.Write(resp.Encode(args...)); err != nil {
						return resp.Frame{}, err
					}
					f, err := resp.Read(bufio.NewReader(conn))
					if err == nil && f.IsError() {
						err = fmt.Errorf("fixture Redis command failed: %s", args[0])
					}
					return f, err
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
						_, err = query("DEL", key)
					case "/item":
						var f resp.Frame
						f, err = query("HGETALL", key)
						if err == nil {
							if len(f.Items) > 0 {
								value, err = strconv.ParseInt(string(f.Items[1].Data), 10, 64)
							} else {
								_, err = query("HSET", key, "version", strconv.FormatInt(value, 10))
								if err == nil && value != authoritative.Load() {
									mutationKey := other
									if target {
										mutationKey = key
									}
									_, err = query("HSETEX", mutationKey, "FIELDS", "1", "version", strconv.FormatInt(authoritative.Load(), 10))
								}
							}
						}
					case "/authoritative":
					default:
						http.NotFound(w, r)
						return
					}
					if err != nil {
						http.Error(w, "fixture Redis operation failed", 502)
						return
					}
					json.NewEncoder(w).Encode(map[string]int64{"version": value})
				}))
				defer app.Close()
				var c Config
				if err := json.Unmarshal([]byte(Template), &c); err != nil {
					t.Fatal(err)
				}
				c.Redis.Listen, c.Redis.Upstream = addr, upstream
				c.Scenario.Key = key
				if regex {
					c.Scenario.Key, c.Scenario.KeyRegex = "", "^stalefill:protocol:opaque-hash$"
				}
				c.Prepare.URL, c.Write.URL = app.URL+"/prepare", app.URL+"/write"
				c.Read.URL, c.Verify.URL = app.URL+"/item", app.URL+"/item"
				c.Authoritative.URL = app.URL + "/authoritative"
				r := Run(context.Background(), c, false)
				want, finding := Fail, "SF001"
				if target {
					want, finding = Unresolved, "SF006"
				}
				if r.Outcome != want || len(r.Findings) != 1 || r.Findings[0].ID != finding {
					t.Fatal(r)
				}
				if r.FillMode != "hash" {
					t.Fatal("did not exercise the released hash publication", r)
				}
			})
		}
	}
}
