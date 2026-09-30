package scenario

import (
	"context"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/0then0/stalefill/internal/demo"
	"github.com/0then0/stalefill/internal/testredis"
)

func freeAddress(t *testing.T) string {
	t.Helper()
	l, e := net.Listen("tcp", "127.0.0.1:0")
	if e != nil {
		t.Fatal(e)
	}
	addr := l.Addr().String()
	l.Close()
	return addr
}
func demoConfig(t *testing.T, upstream string, fixed bool, negative bool) (Config, *demo.App) {
	t.Helper()
	var c Config
	if e := json.Unmarshal([]byte(Template), &c); e != nil {
		t.Fatal(e)
	}
	c.Redis.Listen = freeAddress(t)
	c.Redis.Upstream = upstream
	c.Scenario.Timeout = "3s"
	app := demo.New(c.Redis.Listen, fixed)
	h := httptest.NewServer(app)
	t.Cleanup(h.Close)
	c.Prepare.URL = h.URL + "/items/42"
	c.Read.URL = c.Prepare.URL
	c.Write.URL = c.Prepare.URL
	c.Verify.URL = c.Prepare.URL
	c.Authoritative.URL = h.URL + "/authoritative/42"
	if negative {
		c.Scenario.Type = "negative_cache_resurrection"
		c.Prepare.JSON = []byte(`{"version":1,"exists":false}`)
		c.Read.Assert = &Assertion{Path: "$.exists", Equals: []byte(`false`)}
		c.Authoritative.Assert = &Assertion{Path: "$.exists", Equals: []byte(`true`)}
		c.Verify.Assert = &Assertion{Path: "$.exists", Equals: []byte(`true`)}
	}
	return c, app
}
func logicalEvents(r Report) []string {
	var names []string
	for i, e := range r.Events {
		if e.Seq != i+1 {
			panic("nonmonotonic sequence")
		}
		names = append(names, e.Event)
	}
	return names
}
func testDemos(t *testing.T, upstream string) {
	for _, negative := range []bool{false, true} {
		for _, fixed := range []bool{false, true} {
			name := "positive/broken"
			if negative {
				name = "negative/broken"
			}
			if fixed {
				name = strings.ReplaceAll(name, "broken", "fixed")
			}
			t.Run(name, func(t *testing.T) {
				c, _ := demoConfig(t, upstream, fixed, negative)
				var first []string
				for i := 0; i < 24; i++ {
					r := Run(context.Background(), c, false)
					want := Fail
					if fixed {
						want = Pass
					}
					if r.Outcome != want {
						t.Fatalf("run %d: %s %+v events=%v", i, r.Outcome, r.Findings, logicalEvents(r))
					}
					if !fixed {
						wantID := "SF001"
						if negative {
							wantID = "SF002"
						}
						if len(r.Findings) != 1 || r.Findings[0].ID != wantID {
							t.Fatal(r.Findings)
						}
					}
					names := logicalEvents(r)
					if i == 0 {
						first = names
					} else if !reflect.DeepEqual(first, names) {
						t.Fatalf("logical event order changed on run %d: %v vs %v", i, first, names)
					}
				}
			})
		}
	}
}
func TestDeterministicDemos(t *testing.T) { testDemos(t, testredis.Start(t)) }
func TestRealRedisDemos(t *testing.T) {
	addr := os.Getenv("REDIS_TEST_ADDR")
	if addr == "" {
		t.Skip("set REDIS_TEST_ADDR to a disposable real Redis/Valkey instance")
	}
	testDemos(t, addr)
	testNegativeHTTPStatuses(t, addr)
}

func TestNegativeHTTPStatuses(t *testing.T) { testNegativeHTTPStatuses(t, testredis.Start(t)) }

func testNegativeHTTPStatuses(t *testing.T, upstream string) {
	t.Helper()
	for _, tc := range []struct {
		name               string
		fixed, refreshRead bool
		finalStatus        int
		finalBody          string
		outcome, finding   string
	}{
		{name: "stale_404", outcome: Fail, finding: "SF002"},
		{name: "protected_200", fixed: true, outcome: Pass},
		{name: "protected_read_refreshes_to_200", fixed: true, refreshRead: true, outcome: Pass},
		{name: "unexpected_500", finalStatus: 500, finalBody: `{"exists":false}`, outcome: InfrastructureError, finding: "SF100"},
		{name: "old_value_with_new_status", finalStatus: 200, finalBody: `{"exists":false}`, outcome: Unresolved, finding: "SF007"},
		{name: "new_value_with_old_status", finalStatus: 404, finalBody: `{"exists":true}`, outcome: Unresolved, finding: "SF007"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c, app := demoConfig(t, upstream, tc.fixed, true)
			var reads atomic.Int32
			h := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				read := int32(0)
				if r.Method == "GET" && r.URL.Path == "/items/42" {
					read = reads.Add(1)
				}
				rr := httptest.NewRecorder()
				app.ServeHTTP(rr, r)
				w.Header().Set("Content-Type", "application/json")
				if read == 4 && tc.finalStatus != 0 {
					w.WriteHeader(tc.finalStatus)
					w.Write([]byte(tc.finalBody))
					return
				}
				if read == 3 && tc.refreshRead {
					w.Write([]byte(`{"exists":true}`))
					return
				}
				var body struct {
					Exists bool `json:"exists"`
				}
				if json.Unmarshal(rr.Body.Bytes(), &body) != nil {
					w.WriteHeader(502)
					return
				}
				if read != 0 && !body.Exists {
					w.WriteHeader(404)
				} else {
					w.WriteHeader(rr.Code)
				}
				w.Write(rr.Body.Bytes())
			}))
			defer h.Close()
			c.Prepare.URL = h.URL + "/items/42"
			c.Read.URL = c.Prepare.URL
			c.Write.URL = c.Prepare.URL
			c.Verify.URL = c.Prepare.URL
			c.Authoritative.URL = h.URL + "/authoritative/42"
			c.Read.Status = 404
			r := Run(context.Background(), c, false)
			if r.Outcome != tc.outcome || (tc.finding != "" && (len(r.Findings) != 1 || r.Findings[0].ID != tc.finding)) {
				t.Fatalf("outcome=%s findings=%v events=%v", r.Outcome, r.Findings, logicalEvents(r))
			}
			completed := false
			for _, event := range r.Events {
				if event.Event == "stale_set_completed" {
					completed = true
				}
			}
			if !completed {
				t.Fatal("HTTP oracle tested without a complete schedule")
			}
		})
	}
}

type cancelReadTransport struct {
	base            http.RoundTripper
	reads           atomic.Int32
	cancel          context.CancelFunc
	connectionError bool
}

func (rt *cancelReadTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	if req.Method == "GET" && req.URL.Path == "/items/42" && rt.reads.Add(1) == 3 {
		rt.cancel()
		if rt.connectionError {
			return nil, context.Canceled
		}
		return &http.Response{StatusCode: 200, Body: io.NopCloser(strings.NewReader(`{"version":1,"exists":true}`)), Header: make(http.Header), Request: req}, nil
	}
	return rt.base.RoundTrip(req)
}

func TestCancellationBeforeFillHeld(t *testing.T) {
	base := http.DefaultTransport
	defer func() { http.DefaultTransport = base }()
	for _, connectionError := range []bool{false, true} {
		for i := 0; i < 25; i++ {
			c, _ := demoConfig(t, testredis.Start(t), false, false)
			ctx, cancel := context.WithCancel(context.Background())
			http.DefaultTransport = &cancelReadTransport{base: base, cancel: cancel, connectionError: connectionError}
			r := Run(ctx, c, false)
			cancel()
			if r.Outcome != InfrastructureError || len(r.Findings) != 1 || r.Findings[0].ID != "SF100" {
				t.Fatalf("iteration %d: %+v", i, r)
			}
			for _, event := range r.Events {
				if event.Event == "stale_set_released" || event.Event == "stale_set_completed" {
					t.Fatal("canceled fill forwarded")
				}
			}
			listener, e := net.Listen("tcp", c.Redis.Listen)
			if e != nil {
				t.Fatalf("listener not stopped: %v", e)
			}
			listener.Close()
		}
	}
}

func TestStatusMismatchBeforeRelease(t *testing.T) {
	for _, stage := range []string{"baseline_read", "write", "authoritative"} {
		t.Run(stage, func(t *testing.T) {
			c, app := demoConfig(t, testredis.Start(t), false, false)
			var writes, authoritativeReads atomic.Int32
			h := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				rr := httptest.NewRecorder()
				app.ServeHTTP(rr, r)
				mismatch := false
				if r.Method == "PUT" && writes.Add(1) == 4 && stage == "write" {
					mismatch = true
				}
				if r.URL.Path == "/authoritative/42" && authoritativeReads.Add(1) == 2 && stage == "authoritative" {
					mismatch = true
				}
				if mismatch {
					w.WriteHeader(202)
				} else {
					w.WriteHeader(rr.Code)
				}
				w.Write(rr.Body.Bytes())
			}))
			defer h.Close()
			c.Prepare.URL = h.URL + "/items/42"
			c.Read.URL = c.Prepare.URL
			c.Write.URL = c.Prepare.URL
			c.Verify.URL = c.Prepare.URL
			c.Authoritative.URL = h.URL + "/authoritative/42"
			if stage == "baseline_read" {
				c.Read.Status = 201
			}
			r := Run(context.Background(), c, false)
			if r.Outcome != InfrastructureError || len(r.Findings) != 1 || r.Findings[0].ID != "SF100" {
				t.Fatal(r)
			}
			for _, event := range r.Events {
				if event.Event == "stale_set_released" {
					t.Fatal("released despite failed status assertion")
				}
			}
		})
	}
}
func TestDoctorAndWiring(t *testing.T) {
	upstream := testredis.Start(t)
	c, _ := demoConfig(t, upstream, false, false)
	r := Run(context.Background(), c, true)
	if r.Outcome != Pass {
		t.Fatal(r)
	}
	for _, e := range r.Events {
		if e.Event == "stale_set_held" {
			t.Fatal("doctor armed barrier")
		}
	}
	app := demo.New(upstream, false)
	h := httptest.NewServer(app)
	defer h.Close()
	c.Prepare.URL = h.URL + "/items/42"
	c.Read.URL = c.Prepare.URL
	c.Write.URL = c.Prepare.URL
	c.Verify.URL = c.Prepare.URL
	c.Authoritative.URL = h.URL + "/authoritative/42"
	r = Run(context.Background(), c, false)
	if r.Outcome != Unresolved || r.Findings[0].ID != "SF004" {
		t.Fatalf("bypass incorrectly classified: %+v", r)
	}
}
func TestInfrastructureAndCanceledRun(t *testing.T) {
	c, _ := demoConfig(t, testredis.Start(t), false, false)
	c.Redis.Upstream = freeAddress(t)
	r := Run(context.Background(), c, false)
	if r.Outcome != InfrastructureError {
		t.Fatal(r)
	}
	c, _ = demoConfig(t, testredis.Start(t), false, false)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	started := time.Now()
	r = Run(ctx, c, false)
	if r.Outcome != InfrastructureError || time.Since(started) > time.Second {
		t.Fatal(r)
	}
	l, e := net.Listen("tcp", c.Redis.Listen)
	if e != nil {
		t.Fatal(e)
	}
	defer l.Close()
	r = Run(context.Background(), c, false)
	if r.Outcome != InfrastructureError {
		t.Fatal(r)
	}
}
func TestUnexpectedFinalValueIsNotStaleProof(t *testing.T) {
	c, app := demoConfig(t, testredis.Start(t), false, false)
	var reads atomic.Int32
	h := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == "GET" && r.URL.Path == "/items/42" {
			if reads.Add(1) == 4 {
				w.Header().Set("Content-Type", "application/json")
				w.Write([]byte(`{"version":3}`))
				return
			}
		}
		app.ServeHTTP(w, r)
	}))
	defer h.Close()
	c.Prepare.URL = h.URL + "/items/42"
	c.Read.URL = c.Prepare.URL
	c.Write.URL = c.Prepare.URL
	c.Verify.URL = c.Prepare.URL
	c.Authoritative.URL = h.URL + "/authoritative/42"
	r := Run(context.Background(), c, false)
	if r.Outcome != Unresolved || r.Findings[0].ID != "SF007" {
		t.Fatal(r)
	}
}
func TestConfigAndRedaction(t *testing.T) {
	var c Config
	json.Unmarshal([]byte(Template), &c)
	if e := c.Validate(); e != nil {
		t.Fatal(e)
	}
	c.Redis.Upstream = "192.0.2.1:6379"
	if c.Validate() == nil {
		t.Fatal("remote upstream allowed by default")
	}
	c.Redis.AllowRemote = true
	if e := c.Validate(); e != nil {
		t.Fatal(e)
	}
	c.Redis.Listen = "0.0.0.0:6380"
	if c.Validate() == nil {
		t.Fatal("remote listen allowed")
	}
	c, _ = demoConfig(t, testredis.Start(t), false, false)
	c.Read.Headers = map[string]string{"Authorization": "Bearer SECRET_AUTH", "Cookie": "SECRET_COOKIE"}
	c.Scenario.Key = "SECRET_KEY"
	r := Run(context.Background(), c, false)
	b, _ := json.Marshal(r)
	for _, secret := range []string{"SECRET_AUTH", "SECRET_COOKIE", "SECRET_KEY", c.Read.URL, "127.0.0.1"} {
		if strings.Contains(string(b), secret) {
			t.Fatalf("report leaks %s", secret)
		}
	}
}
func TestExtraction(t *testing.T) {
	for _, tc := range []struct{ path, want string }{{"$.a[1].v", "2"}, {"$.a[0]", "null"}, {"$", `{"a":[null,{"v":2}]}`}} {
		v, e := extract([]byte(`{"a":[null,{"v":2}]}`), tc.path)
		if e != nil || string(v) != tc.want {
			t.Fatalf("%s: %s %v", tc.path, v, e)
		}
	}
	if _, e := extract([]byte(`{}`), "$.missing"); e == nil {
		t.Fatal("missing field accepted")
	}
}

func TestNumericJSONEquality(t *testing.T) {
	for _, pair := range [][2]string{{"120", "120.0"}, {"1e2", "100"}, {"9007199254740993", "9007199254740993.0"}, {`{"v":1.0}`, `{"v":1}`}} {
		if !equalJSON([]byte(pair[0]), []byte(pair[1])) {
			t.Fatal(pair)
		}
	}
	if equalJSON([]byte(`9007199254740993`), []byte(`9007199254740992`)) {
		t.Fatal("lost integer precision")
	}
	if equalJSON([]byte(`1e999999999`), []byte(`2`)) {
		t.Fatal("invalid equality")
	}
}

func TestMissingScheduleEvents(t *testing.T) {
	for _, missing := range []string{"fill", "invalidation"} {
		t.Run(missing, func(t *testing.T) {
			c, app := demoConfig(t, testredis.Start(t), false, false)
			c.Scenario.Timeout = "300ms"
			var writes, reads atomic.Int32
			h := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path == "/items/42" && r.Method == "PUT" && writes.Add(1) == 4 && missing == "invalidation" {
					w.Write([]byte(`{"version":2}`))
					return
				}
				if r.URL.Path == "/items/42" && r.Method == "GET" && reads.Add(1) == 3 && missing == "fill" {
					w.Write([]byte(`{"version":1}`))
					return
				}
				app.ServeHTTP(w, r)
			}))
			defer h.Close()
			c.Prepare.URL = h.URL + "/items/42"
			c.Read.URL = c.Prepare.URL
			c.Write.URL = c.Prepare.URL
			c.Verify.URL = c.Prepare.URL
			c.Authoritative.URL = h.URL + "/authoritative/42"
			r := Run(context.Background(), c, false)
			id := "SF005"
			if missing == "invalidation" {
				id = "SF003"
			}
			if r.Outcome != Unresolved || r.Findings[0].ID != id {
				t.Fatal(r)
			}
			for _, e := range r.Events {
				if e.Event == "stale_set_released" {
					t.Fatal("released without required events")
				}
			}
		})
	}
}
func TestCancellationWhileFillHeld(t *testing.T) {
	c, app := demoConfig(t, testredis.Start(t), false, false)
	writeStarted := make(chan struct{})
	var writes atomic.Int32
	h := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/items/42" && r.Method == "PUT" && writes.Add(1) == 4 {
			io.Copy(io.Discard, r.Body)
			close(writeStarted)
			<-r.Context().Done()
			return
		}
		app.ServeHTTP(w, r)
	}))
	defer h.Close()
	c.Prepare.URL = h.URL + "/items/42"
	c.Read.URL = c.Prepare.URL
	c.Write.URL = c.Prepare.URL
	c.Verify.URL = c.Prepare.URL
	c.Authoritative.URL = h.URL + "/authoritative/42"
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan Report, 1)
	go func() { done <- Run(ctx, c, false) }()
	select {
	case <-writeStarted:
	case <-time.After(time.Second):
		t.Fatal("fill not held")
	}
	cancel()
	select {
	case r := <-done:
		if r.Outcome != InfrastructureError {
			t.Fatal(r)
		}
		for _, e := range r.Events {
			if e.Event == "stale_set_completed" {
				t.Fatal("aborted fill was forwarded")
			}
		}
	case <-time.After(time.Second):
		t.Fatal("canceled run hung")
	}
}

func TestProtectedReadCanRefreshItsResponse(t *testing.T) {
	c, app := demoConfig(t, testredis.Start(t), true, false)
	var reads atomic.Int32
	h := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/items/42" && r.Method == "GET" && reads.Add(1) == 3 {
			recorded := httptest.NewRecorder()
			app.ServeHTTP(recorded, r)
			if recorded.Code != 200 {
				w.WriteHeader(recorded.Code)
				return
			}
			// A mitigation refreshes the in-flight response after its fill completes.
			w.Header().Set("Content-Type", "application/json")
			w.Write([]byte(`{"version":2,"exists":true}`))
			return
		}
		app.ServeHTTP(w, r)
	}))
	defer h.Close()
	c.Prepare.URL = h.URL + "/items/42"
	c.Read.URL = c.Prepare.URL
	c.Write.URL = c.Prepare.URL
	c.Verify.URL = c.Prepare.URL
	c.Authoritative.URL = h.URL + "/authoritative/42"
	r := Run(context.Background(), c, false)
	if r.Outcome != Pass {
		t.Fatal(r)
	}
}
