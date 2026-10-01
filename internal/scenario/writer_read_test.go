package scenario

import (
	"bufio"
	"context"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/0then0/stalefill/internal/resp"
	"github.com/0then0/stalefill/internal/testredis"
)

func TestWriterMayReadCache(t *testing.T) {
	c, app := demoConfig(t, testredis.Start(t), false, false)
	c.Scenario.Timeout = "1s"
	h := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/writer/42" {
			conn, err := net.DialTimeout("tcp", c.Redis.Listen, time.Second)
			if err != nil {
				http.Error(w, "cache error", 502)
				return
			}
			conn.SetDeadline(time.Now().Add(time.Second))
			_, err = conn.Write(resp.Encode("GET", "product:42"))
			if err == nil {
				_, err = resp.Read(bufio.NewReader(conn))
			}
			conn.Close()
			if err != nil {
				http.Error(w, "cache error", 502)
				return
			}
			// An ordinary writer consults cache, then updates authoritative state and DELs.
			r.URL.Path = "/items/42"
		}
		app.ServeHTTP(w, r)
	}))
	defer h.Close()
	c.Redis.Listen = freeAddress(t)
	app.Redis = c.Redis.Listen
	c.Prepare.URL = h.URL + "/items/42"
	c.Read.URL = c.Prepare.URL
	c.Verify.URL = c.Prepare.URL
	c.Write.URL = h.URL + "/writer/42"
	c.Authoritative.URL = h.URL + "/authoritative/42"
	r := Run(context.Background(), c, false)
	if r.Outcome != Fail || r.Findings[0].ID != "SF001" {
		t.Fatalf("writer cache read: got %s %v, want FAIL SF001; events %v", r.Outcome, r.Findings, logicalEvents(r))
	}
}
