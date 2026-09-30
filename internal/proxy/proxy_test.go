package proxy_test

import (
	"bufio"
	"context"
	"fmt"
	"net"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/0then0/stalefill/internal/proxy"
	"github.com/0then0/stalefill/internal/resp"
	"github.com/0then0/stalefill/internal/scenario"
	"github.com/0then0/stalefill/internal/testredis"
)

func TestPassThroughPipelinesAndConnections(t *testing.T) {
	upstream := testredis.Start(t)
	hooks := scenario.NewScheduler("target", "")
	s, e := proxy.Start(context.Background(), "127.0.0.1:0", upstream, hooks)
	if e != nil {
		t.Fatal(e)
	}
	defer s.Close()
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			c, e := net.Dial("tcp", s.Addr())
			if e != nil {
				t.Error(e)
				return
			}
			defer c.Close()
			c.SetDeadline(time.Now().Add(3 * time.Second))
			r := bufio.NewReader(c)
			frames := [][]byte{resp.Encode("PING"), resp.Encode("ECHO", "\x00\xff\r\n"), resp.Encode("HELLO", "3"), resp.Encode("MGET", "absent1", "absent2")}
			var all []byte
			for _, f := range frames {
				all = append(all, f...)
			}
			if _, e = c.Write(all); e != nil {
				t.Error(e)
				return
			}
			for j := range frames {
				f, e := resp.Read(r)
				if e != nil {
					t.Error(e)
					return
				}
				if j == 0 && string(f.Data) != "PONG" || j == 1 && string(f.Data) != "\x00\xff\r\n" || j == 2 && f.Kind != '%' || j == 3 && (len(f.Items) != 2 || !f.Items[0].Null) {
					t.Error("pipeline response mismatch")
				}
			}
		}()
	}
	wg.Wait()
}

func TestHandshakePassThrough(t *testing.T) {
	hooks := scenario.NewScheduler("target", "")
	s, err := proxy.Start(context.Background(), "127.0.0.1:0", testredis.Start(t), hooks)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	c, err := net.Dial("tcp", s.Addr())
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	c.SetDeadline(time.Now().Add(time.Second))
	r := bufio.NewReader(c)
	var pipeline []byte
	for _, args := range [][]string{{"AUTH", "SECRET_USERNAME", "SECRET_PASSWORD"}, {"SELECT", "1"}, {"CLIENT", "SETINFO", "LIB-NAME", "fixture"}, {"CLIENT", "SETNAME", "fixture"}, {"HELLO", "3"}} {
		pipeline = append(pipeline, resp.Encode(args...)...)
	}
	if _, err := c.Write(pipeline); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 5; i++ {
		f, err := resp.Read(r)
		if err != nil || f.IsError() {
			t.Fatal(err, f)
		}
		if i < 4 && string(f.Data) != "OK" || i == 4 && f.Kind != '%' {
			t.Fatal("handshake reply changed", f)
		}
	}
	events, problem, infra := hooks.Snapshot()
	if len(events) != 0 || problem != "" || infra != "" {
		t.Fatal("handshake affected schedule", events, problem, infra)
	}
}
func TestHeldConnectionDoesNotBlockOthersAndCloses(t *testing.T) {
	upstream := testredis.Start(t)
	hooks := scenario.NewScheduler("target", "")
	hooks.Arm()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s, e := proxy.Start(ctx, "127.0.0.1:0", upstream, hooks)
	if e != nil {
		t.Fatal(e)
	}
	defer s.Close()
	c, e := net.Dial("tcp", s.Addr())
	if e != nil {
		t.Fatal(e)
	}
	defer c.Close()
	c.SetDeadline(time.Now().Add(time.Second))
	r := bufio.NewReader(c)
	c.Write(resp.Encode("GET", "target"))
	if _, e = resp.Read(r); e != nil {
		t.Fatal(e)
	}
	c.Write(resp.Encode("SET", "target", "secret"))
	wait, stop := context.WithTimeout(context.Background(), time.Second)
	defer stop()
	if e = hooks.WaitHeld(wait); e != nil {
		t.Fatal(e)
	}
	if f := testredis.Command(t, s.Addr(), "PING"); string(f.Data) != "PONG" {
		t.Fatal("unrelated connection blocked")
	}
	cancel()
	done := make(chan struct{})
	go func() { s.Close(); close(done) }()
	select {
	case <-done:
	case <-wait.Done():
		t.Fatal("shutdown hung")
	}
	if _, e = resp.Read(r); e == nil {
		t.Fatal("held socket remained open")
	}
}

func TestHalfClosedPipelineDrains(t *testing.T) {
	s, e := proxy.Start(context.Background(), "127.0.0.1:0", testredis.Start(t), scenario.NewScheduler("x", ""))
	if e != nil {
		t.Fatal(e)
	}
	defer s.Close()
	c, e := net.DialTCP("tcp", nil, mustTCP(t, s.Addr()))
	if e != nil {
		t.Fatal(e)
	}
	defer c.Close()
	c.SetDeadline(time.Now().Add(time.Second))
	c.Write(append(resp.Encode("PING"), resp.Encode("ECHO", "final")...))
	c.CloseWrite()
	r := bufio.NewReader(c)
	f, e := resp.Read(r)
	if e != nil || string(f.Data) != "PONG" {
		t.Fatalf("first reply lost: %v", e)
	}
	f, e = resp.Read(r)
	if e != nil || string(f.Data) != "final" {
		t.Fatalf("last reply lost: %v", e)
	}
}
func mustTCP(t *testing.T, s string) *net.TCPAddr {
	t.Helper()
	a, e := net.ResolveTCPAddr("tcp", s)
	if e != nil {
		t.Fatal(e)
	}
	return a
}

func TestRealRedisProtocol(t *testing.T) {
	upstream := os.Getenv("REDIS_TEST_ADDR")
	if upstream == "" {
		t.Skip("set REDIS_TEST_ADDR for real RESP/pipeline verification")
	}
	s, e := proxy.Start(context.Background(), "127.0.0.1:0", upstream, scenario.NewScheduler("product:42", ""))
	if e != nil {
		t.Fatal(e)
	}
	defer s.Close()
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			c, e := net.Dial("tcp", s.Addr())
			if e != nil {
				t.Error(e)
				return
			}
			defer c.Close()
			c.SetDeadline(time.Now().Add(5 * time.Second))
			key := fmt.Sprintf("stalefill:protocol:%d", i)
			binary := "\x00\xff\r\n"
			commands := [][]byte{resp.Encode("SET", key, binary, "EX", "60"), resp.Encode("GET", key), resp.Encode("MGET", key, key+":absent"), resp.Encode("HELLO", "3"), resp.Encode("GET", key+":absent"), resp.Encode("EXPIRE", key, "60"), resp.Encode("PEXPIRE", key, "60000"), resp.Encode("SETEX", key, "60", binary), resp.Encode("PSETEX", key, "60000", binary), resp.Encode("ECHO", binary), resp.Encode("UNLINK", key)}
			var pipeline []byte
			for _, cmd := range commands {
				pipeline = append(pipeline, cmd...)
			}
			if _, e = c.Write(pipeline); e != nil {
				t.Error(e)
				return
			}
			r := bufio.NewReader(c)
			for j := range commands {
				f, e := resp.Read(r)
				if e != nil || f.IsError() {
					t.Errorf("response %d: %v %s", j, e, f.Data)
					return
				}
				switch j {
				case 1, 9:
					if string(f.Data) != binary {
						t.Error("binary payload changed")
					}
				case 2:
					if len(f.Items) != 2 || string(f.Items[0].Data) != binary || !f.Items[1].Null {
						t.Error("MGET response changed")
					}
				case 3:
					if f.Kind != '%' {
						t.Error("HELLO 3 map not forwarded")
					}
				case 4:
					if f.Kind != '_' || !f.Null {
						t.Error("RESP3 null not forwarded")
					}
				case 5, 6, 10:
					if f.Kind != ':' || string(f.Data) != "1" {
						t.Error("expiration/delete response changed")
					}
				}
			}
		}(i)
	}
	wg.Wait()
	testReplyModes(t, upstream)
}

func TestReplyModesRejectedWithoutBlockingPipeline(t *testing.T) {
	testReplyModes(t, testredis.Start(t))
}

func testReplyModes(t *testing.T, upstream string) {
	t.Helper()
	for _, mode := range []string{"OFF", "SKIP", "oFf", "sKiP"} {
		t.Run("reply_"+mode, func(t *testing.T) {
			hooks := scenario.NewScheduler("target", "")
			s, e := proxy.Start(context.Background(), "127.0.0.1:0", upstream, hooks)
			if e != nil {
				t.Fatal(e)
			}
			defer s.Close()
			c, e := net.Dial("tcp", s.Addr())
			if e != nil {
				t.Fatal(e)
			}
			defer c.Close()
			c.SetDeadline(time.Now().Add(time.Second))
			var pipeline []byte
			for _, args := range [][]string{{"CLIENT", "REPLY", mode}, {"PING"}, {"CLIENT", "REPLY", "ON"}, {"ECHO", "still-responsive"}} {
				pipeline = append(pipeline, resp.Encode(args...)...)
			}
			if _, e = c.Write(pipeline); e != nil {
				t.Fatal(e)
			}
			r := bufio.NewReader(c)
			for i, want := range []string{"ERR StaleFill does not support this Redis response mode", "PONG", "OK", "still-responsive"} {
				f, e := resp.Read(r)
				if e != nil || string(f.Data) != want || (i == 0 && !f.IsError()) {
					t.Fatalf("reply %d: %q %v", i, f.Data, e)
				}
			}
			_, problem, infra := hooks.Snapshot()
			if problem != "SF006" || infra != "" {
				t.Fatalf("unsupported mode classification: %s %s", problem, infra)
			}
		})
	}
}
