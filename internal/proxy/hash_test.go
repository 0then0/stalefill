package proxy_test

import (
	"bufio"
	"context"
	"net"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/0then0/stalefill/internal/proxy"
	"github.com/0then0/stalefill/internal/resp"
	"github.com/0then0/stalefill/internal/scenario"
	"github.com/0then0/stalefill/internal/testredis"
)

func TestRealHashPublication(t *testing.T) {
	upstream := os.Getenv("REDIS_TEST_ADDR")
	if upstream == "" {
		t.Skip("set REDIS_TEST_ADDR for real hash/transaction checks")
	}
	for _, protocol := range []int{2, 3} {
		t.Run("RESP"+strconv.Itoa(protocol), func(t *testing.T) {
			const key = "stalefill:protocol:hash"
			testredis.Command(t, upstream, "DEL", key)
			hooks := scenario.NewScheduler(key, "")
			server, err := proxy.Start(context.Background(), "127.0.0.1:0", upstream, hooks)
			if err != nil {
				t.Fatal(err)
			}
			defer server.Close()
			c, err := net.Dial("tcp", server.Addr())
			if err != nil {
				t.Fatal(err)
			}
			defer c.Close()
			c.SetDeadline(time.Now().Add(3 * time.Second))
			r := bufio.NewReader(c)
			call := func(args ...string) resp.Frame {
				t.Helper()
				if _, err := c.Write(resp.Encode(args...)); err != nil {
					t.Fatal(err)
				}
				f, err := resp.Read(r)
				if err != nil || f.IsError() {
					t.Fatalf("%s: %v %q", args[0], err, f.Data)
				}
				return f
			}
			call("HELLO", strconv.Itoa(protocol))
			hooks.Arm()
			f := call("HGETALL", key)
			kind := byte('*')
			if protocol == 3 {
				kind = '%'
			}
			if f.Kind != kind || f.Null || len(f.Items) != 0 {
				t.Fatal("wrong actual hash miss representation", f)
			}
			var pipeline []byte
			for _, cmd := range [][]string{{"MULTI"}, {"DEL", key}, {"HSET", key, "field", "SECRET_HASH_VALUE"}, {"EXPIRE", key, "5"}, {"EXEC"}} {
				pipeline = append(pipeline, resp.Encode(cmd...)...)
			}
			if _, err = c.Write(pipeline); err != nil {
				t.Fatal(err)
			}
			for i := 0; i < 4; i++ {
				f, err := resp.Read(r)
				want := "QUEUED"
				if i == 0 {
					want = "OK"
				}
				if err != nil || string(f.Data) != want {
					t.Fatalf("pipeline reply %d: %v %q", i, err, f.Data)
				}
			}
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			if err := hooks.WaitHeld(ctx); err != nil {
				t.Fatal(err)
			}
			if f := testredis.Command(t, upstream, "HGETALL", key); len(f.Items) != 0 {
				t.Fatal("EXEC forwarded before barrier")
			}
			hooks.Event("write_started")
			testredis.Command(t, server.Addr(), "DEL", key)
			if err := hooks.WaitInvalidation(ctx); err != nil {
				t.Fatal(err)
			}
			hooks.Event("write_completed")
			hooks.Event("authoritative_confirmed")
			if err := hooks.Release(); err != nil {
				t.Fatal(err)
			}
			f, err = resp.Read(r)
			if err != nil || f.Kind != '*' || len(f.Items) != 3 || string(f.Items[1].Data) != "1" {
				t.Fatal("EXEC response changed", f, err)
			}
			if err := hooks.WaitApplied(ctx); err != nil {
				t.Fatal(err)
			}
			f = call("HGETALL", key)
			if f.Kind != kind || len(f.Items) != 2 || string(f.Items[1].Data) != "SECRET_HASH_VALUE" {
				t.Fatal("hash changed")
			}
			call("DEL", key)
		})
	}
}

func TestRealAbortedAndFailedEXEC(t *testing.T) {
	upstream := os.Getenv("REDIS_TEST_ADDR")
	if upstream == "" {
		t.Skip("set REDIS_TEST_ADDR for real EXEC failure checks")
	}
	for _, failure := range []string{"watch", "runtime", "queue", "discard"} {
		t.Run(failure, func(t *testing.T) {
			const key = "stalefill:protocol:abort"
			testredis.Command(t, upstream, "DEL", key)
			hooks := scenario.NewScheduler(key, "")
			server, err := proxy.Start(context.Background(), "127.0.0.1:0", upstream, hooks)
			if err != nil {
				t.Fatal(err)
			}
			defer server.Close()
			c, err := net.Dial("tcp", server.Addr())
			if err != nil {
				t.Fatal(err)
			}
			defer c.Close()
			c.SetDeadline(time.Now().Add(3 * time.Second))
			r := bufio.NewReader(c)
			call := func(args ...string) resp.Frame {
				t.Helper()
				c.Write(resp.Encode(args...))
				f, err := resp.Read(r)
				if err != nil {
					t.Fatal(err)
				}
				return f
			}
			if failure == "watch" {
				call("WATCH", key)
			}
			hooks.Arm()
			call("HGETALL", key)
			call("MULTI")
			if failure == "queue" {
				call("HSET", key, "field")
			} else {
				call("HSET", key, "field", "value")
			}
			if failure == "discard" {
				call("DISCARD")
			} else {
				c.Write(resp.Encode("EXEC"))
				if failure != "queue" {
					ctx, cancel := context.WithTimeout(context.Background(), time.Second)
					defer cancel()
					if err := hooks.WaitHeld(ctx); err != nil {
						t.Fatal(err)
					}
					hooks.Event("write_started")
					testredis.Command(t, server.Addr(), "DEL", key)
					if err := hooks.WaitInvalidation(ctx); err != nil {
						t.Fatal(err)
					}
					if failure == "watch" || failure == "runtime" {
						testredis.Command(t, upstream, "SET", key, "wrong-type", "EX", "5")
					}
					hooks.Event("write_completed")
					hooks.Event("authoritative_confirmed")
					if err := hooks.Release(); err != nil {
						t.Fatal(err)
					}
				}
				if _, err := resp.Read(r); err != nil {
					t.Fatal(err)
				}
			}
			_, problem, infra := hooks.Snapshot()
			if (problem != "SF008" && problem != "SF006") || infra != "" {
				t.Fatal("failed publication classification", problem, infra)
			}
			for _, e := range mustEvents(hooks) {
				if e.Event == "fill_exec_completed" {
					t.Fatal("failed publication marked completed")
				}
			}
			testredis.Command(t, upstream, "DEL", key)
		})
	}
}

func mustEvents(s *scenario.Scheduler) []scenario.Event { events, _, _ := s.Snapshot(); return events }

func TestRealSameConnectionEXECDoesNotDeadlock(t *testing.T) {
	upstream := os.Getenv("REDIS_TEST_ADDR")
	if upstream == "" {
		t.Skip("set REDIS_TEST_ADDR for real same-connection pipeline check")
	}
	const key = "stalefill:protocol:same-connection"
	testredis.Command(t, upstream, "DEL", key)
	hooks := scenario.NewScheduler(key, "")
	hooks.Arm()
	server, err := proxy.Start(context.Background(), "127.0.0.1:0", upstream, hooks)
	if err != nil {
		t.Fatal(err)
	}
	defer server.Close()
	c, err := net.Dial("tcp", server.Addr())
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	c.SetDeadline(time.Now().Add(time.Second))
	r := bufio.NewReader(c)
	c.Write(resp.Encode("HGETALL", key))
	if _, err := resp.Read(r); err != nil {
		t.Fatal(err)
	}
	var pipeline []byte
	for _, args := range [][]string{{"MULTI"}, {"HSET", key, "f", "v"}, {"EXEC"}, {"DEL", key}} {
		pipeline = append(pipeline, resp.Encode(args...)...)
	}
	c.Write(pipeline)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if hooks.WaitInvalidation(ctx) == nil || ctx.Err() != nil {
		t.Fatal("same-connection invalidation not rejected promptly")
	}
	_, problem, _ := hooks.Snapshot()
	if problem != "SF006" {
		t.Fatal(problem)
	}
	if f := testredis.Command(t, upstream, "HGETALL", key); len(f.Items) != 0 {
		t.Fatal("unsupported held EXEC applied")
	}
}

func TestRealCancelledEXECIsNotPublished(t *testing.T) {
	upstream := os.Getenv("REDIS_TEST_ADDR")
	if upstream == "" {
		t.Skip("set REDIS_TEST_ADDR for cancellation at a real EXEC barrier")
	}
	const key = "stalefill:protocol:cancel-exec"
	testredis.Command(t, upstream, "DEL", key)
	hooks := scenario.NewScheduler(key, "")
	hooks.Arm()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	server, err := proxy.Start(ctx, "127.0.0.1:0", upstream, hooks)
	if err != nil {
		t.Fatal(err)
	}
	defer server.Close()
	c, err := net.Dial("tcp", server.Addr())
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	c.SetDeadline(time.Now().Add(time.Second))
	r := bufio.NewReader(c)
	c.Write(resp.Encode("HGETALL", key))
	if _, err := resp.Read(r); err != nil {
		t.Fatal(err)
	}
	var pipeline []byte
	for _, args := range [][]string{{"MULTI"}, {"DEL", key}, {"HSET", key, "field", "not-published"}, {"EXPIRE", key, "5"}, {"EXEC"}} {
		pipeline = append(pipeline, resp.Encode(args...)...)
	}
	c.Write(pipeline)
	wait, stop := context.WithTimeout(context.Background(), time.Second)
	defer stop()
	if err := hooks.WaitHeld(wait); err != nil {
		t.Fatal(err)
	}
	cancel()
	done := make(chan struct{})
	go func() { server.Close(); close(done) }()
	select {
	case <-done:
	case <-wait.Done():
		t.Fatal("cancelled EXEC did not close")
	}
	if f := testredis.Command(t, upstream, "HGETALL", key); len(f.Items) != 0 {
		t.Fatal("cancelled EXEC was forwarded")
	}
	for _, e := range mustEvents(hooks) {
		if e.Event == "fill_exec_completed" {
			t.Fatal("cancelled EXEC marked applied")
		}
	}
}

func TestRealTransactionIsolationAndDisconnect(t *testing.T) {
	upstream := os.Getenv("REDIS_TEST_ADDR")
	if upstream == "" {
		t.Skip("set REDIS_TEST_ADDR for real transaction connection isolation")
	}
	const discarded = "stalefill:protocol:discarded"
	const committed = "stalefill:protocol:committed"
	testredis.Command(t, upstream, "DEL", discarded, committed)
	server, err := proxy.Start(context.Background(), "127.0.0.1:0", upstream, scenario.NewScheduler("other-target", ""))
	if err != nil {
		t.Fatal(err)
	}
	defer server.Close()
	connect := func() (net.Conn, *bufio.Reader) {
		t.Helper()
		c, err := net.Dial("tcp", server.Addr())
		if err != nil {
			t.Fatal(err)
		}
		c.SetDeadline(time.Now().Add(time.Second))
		t.Cleanup(func() { c.Close() })
		return c, bufio.NewReader(c)
	}
	call := func(c net.Conn, r *bufio.Reader, args ...string) resp.Frame {
		t.Helper()
		if _, err := c.Write(resp.Encode(args...)); err != nil {
			t.Fatal(err)
		}
		f, err := resp.Read(r)
		if err != nil || f.IsError() {
			t.Fatal(err, f)
		}
		return f
	}
	a, ar := connect()
	b, br := connect()
	call(a, ar, "MULTI")
	call(a, ar, "HSET", discarded, "f", "a")
	call(b, br, "MULTI")
	call(b, br, "HSET", committed, "f", "b")
	if f := call(b, br, "EXEC"); f.Kind != '*' || len(f.Items) != 1 {
		t.Fatal("other connection lost transaction")
	}
	// Closing an unexecuted transaction must not affect another pooled socket
	// or turn a fresh connection's read into a QUEUED reply.
	a.Close()
	c, cr := connect()
	if f := call(c, cr, "HGETALL", discarded); f.Kind != '*' || len(f.Items) != 0 {
		t.Fatal("disconnected transaction survived")
	}
	if f := call(c, cr, "HGETALL", committed); len(f.Items) != 2 || string(f.Items[1].Data) != "b" {
		t.Fatal("connection-local publication changed")
	}
	call(c, cr, "DEL", discarded, committed)
}

func TestRealLuaAndHandshakePassThrough(t *testing.T) {
	upstream := os.Getenv("REDIS_TEST_ADDR")
	if upstream == "" {
		t.Skip("set REDIS_TEST_ADDR for real Lua/handshake check")
	}
	hooks := scenario.NewScheduler("target", "")
	server, err := proxy.Start(context.Background(), "127.0.0.1:0", upstream, hooks)
	if err != nil {
		t.Fatal(err)
	}
	defer server.Close()
	testredis.Command(t, server.Addr(), "HELLO", "3")
	testredis.Command(t, server.Addr(), "CLIENT", "SETINFO", "LIB-NAME", "stalefill-validation")
	testredis.Command(t, server.Addr(), "CLIENT", "SETNAME", "stalefill-validation")
	testredis.Command(t, server.Addr(), "SELECT", "0")
	script := "return ARGV[1]"
	sha := testredis.Command(t, server.Addr(), "SCRIPT", "LOAD", script)
	for _, command := range []string{"EVAL", "EVALSHA"} {
		arg := script
		if command == "EVALSHA" {
			arg = string(sha.Data)
		}
		f := testredis.Command(t, server.Addr(), command, arg, "1", "lock:target", "target")
		if string(f.Data) != "target" {
			t.Fatal("Lua ARGV reply changed")
		}
	}
	_, problem, infra := hooks.Snapshot()
	if problem != "" || infra != "" {
		t.Fatal(problem, infra)
	}
	testredis.Command(t, server.Addr(), "EVAL", "return 1", "1", "target")
	_, problem, _ = hooks.Snapshot()
	if problem != "SF006" {
		t.Fatal("target Lua accepted", problem)
	}
	for _, e := range mustEvents(hooks) {
		if strings.Contains(e.Event, script) {
			t.Fatal("script leaked")
		}
	}
}

func TestRealMalformedLuaKeyCounts(t *testing.T) {
	upstream := os.Getenv("REDIS_TEST_ADDR")
	if upstream == "" {
		t.Skip("set REDIS_TEST_ADDR for real Lua declaration checks")
	}
	for _, count := range []string{"+1", "01", "-0"} {
		t.Run(count, func(t *testing.T) {
			hooks := scenario.NewScheduler("target", "")
			server, err := proxy.Start(context.Background(), "127.0.0.1:0", upstream, hooks)
			if err != nil {
				t.Fatal(err)
			}
			defer server.Close()
			conn, err := net.Dial("tcp", server.Addr())
			if err != nil {
				t.Fatal(err)
			}
			defer conn.Close()
			conn.SetDeadline(time.Now().Add(time.Second))
			if _, err := conn.Write(resp.Encode("EVAL", "return 1", count, "lock:target")); err != nil {
				t.Fatal(err)
			}
			reply, err := resp.Read(bufio.NewReader(conn))
			if err != nil || !reply.IsError() {
				t.Fatal("invalid Lua count was not rejected by Redis", reply, err)
			}
			_, problem, infra := hooks.Snapshot()
			if problem != "SF006" || infra != "" {
				t.Fatal("invalid declaration did not become unresolved", problem, infra)
			}
		})
	}
}
