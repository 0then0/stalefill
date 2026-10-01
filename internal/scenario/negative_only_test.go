package scenario

import (
	"bufio"
	"context"
	"encoding/json"
	"net"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/0then0/stalefill/internal/resp"
	"github.com/0then0/stalefill/internal/testredis"
)

func TestNegativeOnlyCaching(t *testing.T) {
	for _, doctor := range []bool{false, true} {
		t.Run(map[bool]string{false: "race", true: "doctor"}[doctor], func(t *testing.T) {
			var c Config
			if err := json.Unmarshal([]byte(Template), &c); err != nil {
				t.Fatal(err)
			}
			c.Redis.Listen = freeAddress(t)
			c.Redis.Upstream = testredis.Start(t)
			c.Scenario.Type = "negative_cache_resurrection"
			c.Scenario.Timeout = "400ms"
			c.Prepare.JSON = []byte(`{"exists":false}`)
			c.Write.JSON = []byte(`{"exists":true}`)
			c.Read.Assert = &Assertion{Path: "$.exists", Equals: []byte(`false`)}
			c.Verify.Assert = &Assertion{Path: "$.exists", Equals: []byte(`true`)}
			c.Authoritative.Assert = &Assertion{Path: "$.exists", Equals: []byte(`true`)}
			c.Verify.CachePublication = "none"
			var exists atomic.Bool
			query := func(ctx context.Context, args ...string) (resp.Frame, error) {
				conn, err := (&net.Dialer{}).DialContext(ctx, "tcp", c.Redis.Listen)
				if err != nil {
					return resp.Frame{}, err
				}
				defer conn.Close()
				stop := context.AfterFunc(ctx, func() { conn.Close() })
				defer stop()
				conn.SetDeadline(time.Now().Add(time.Second))
				if _, err = conn.Write(resp.Encode(args...)); err != nil {
					return resp.Frame{}, err
				}
				return resp.Read(bufio.NewReader(conn))
			}
			h := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				value := exists.Load()
				var err error
				switch {
				case r.URL.Path == "/authoritative":
				case r.Method == "PUT":
					var in struct {
						Exists bool `json:"exists"`
					}
					if json.NewDecoder(r.Body).Decode(&in) != nil {
						w.WriteHeader(400)
						return
					}
					value = in.Exists
					exists.Store(value)
					_, err = query(r.Context(), "DEL", "product:42")
				case r.Method == "GET":
					var f resp.Frame
					f, err = query(r.Context(), "GET", "product:42")
					if err == nil {
						if !f.Null {
							err = json.Unmarshal(f.Data, &value)
						} else {
							value = exists.Load()
							// Ordinary negative-only caching: present records are not cached.
							if !value {
								_, err = query(r.Context(), "SET", "product:42", "false")
							}
						}
					}
				}
				if err != nil {
					http.Error(w, "cache error", 502)
					return
				}
				json.NewEncoder(w).Encode(map[string]bool{"exists": value})
			}))
			defer h.Close()
			c.Prepare.URL = h.URL + "/item"
			c.Write.URL = c.Prepare.URL
			c.Read.URL = c.Prepare.URL
			c.Verify.URL = c.Prepare.URL
			c.Authoritative.URL = h.URL + "/authoritative"
			report := Run(context.Background(), c, doctor)
			want := Fail
			if doctor {
				want = Pass
			}
			if report.Outcome != want {
				t.Fatalf("negative-only cache: got %s %v, want %s; events %v", report.Outcome, report.Findings, want, logicalEvents(report))
			}
			if !doctor && report.Findings[0].ID != "SF002" {
				t.Fatal(report)
			}
		})
	}
}
