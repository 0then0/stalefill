// Package testredis is a bounded protocol fixture for unit tests, not the
// integration oracle. REDIS_TEST_ADDR enables tests against a real server.
package testredis

import (
	"bufio"
	"fmt"
	"net"
	"strconv"
	"strings"
	"sync"
	"testing"

	"github.com/0then0/stalefill/internal/resp"
)

func Start(t testing.TB) string {
	t.Helper()
	l, e := net.Listen("tcp", "127.0.0.1:0")
	if e != nil {
		t.Fatal(e)
	}
	var mu sync.Mutex
	values := map[string]string{}
	conns := map[net.Conn]bool{}
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			c, e := l.Accept()
			if e != nil {
				return
			}
			mu.Lock()
			conns[c] = true
			mu.Unlock()
			wg.Add(1)
			go func() {
				defer wg.Done()
				defer c.Close()
				defer func() { mu.Lock(); delete(conns, c); mu.Unlock() }()
				r := bufio.NewReader(c)
				for {
					f, e := resp.Read(r)
					if e != nil {
						return
					}
					name, args, e := f.Command()
					if e != nil {
						return
					}
					mu.Lock()
					var reply string
					switch name {
					case "PING":
						reply = "+PONG\r\n"
					case "AUTH", "CLIENT", "SELECT", "EXPIRE", "PEXPIRE":
						reply = "+OK\r\n"
					case "HELLO":
						reply = "%1\r\n+proto\r\n:3\r\n"
					case "ECHO":
						reply = bulk(string(args[0]))
					case "GET":
						v, ok := values[string(args[0])]
						if ok {
							reply = bulk(v)
						} else {
							reply = "$-1\r\n"
						}
					case "MGET":
						reply = fmt.Sprintf("*%d\r\n", len(args))
						for _, k := range args {
							v, ok := values[string(k)]
							if ok {
								reply += bulk(v)
							} else {
								reply += "$-1\r\n"
							}
						}
					case "SET", "SETEX", "PSETEX":
						i := 1
						if name != "SET" {
							i = 2
						}
						values[string(args[0])] = string(args[i])
						reply = "+OK\r\n"
					case "DEL", "UNLINK":
						n := 0
						for _, k := range args {
							if _, ok := values[string(k)]; ok {
								n++
								delete(values, string(k))
							}
						}
						reply = ":" + strconv.Itoa(n) + "\r\n"
					default:
						reply = "-ERR unknown command\r\n"
					}
					mu.Unlock()
					if _, e = c.Write([]byte(reply)); e != nil {
						return
					}
				}
			}()
		}
	}()
	t.Cleanup(func() {
		l.Close()
		mu.Lock()
		for c := range conns {
			c.Close()
		}
		mu.Unlock()
		wg.Wait()
	})
	return l.Addr().String()
}
func bulk(s string) string { return "$" + strconv.Itoa(len(s)) + "\r\n" + s + "\r\n" }
func Command(t testing.TB, addr string, args ...string) resp.Frame {
	t.Helper()
	c, e := net.Dial("tcp", addr)
	if e != nil {
		t.Fatal(e)
	}
	defer c.Close()
	if _, e = c.Write(resp.Encode(args...)); e != nil {
		t.Fatal(e)
	}
	f, e := resp.Read(bufio.NewReader(c))
	if e != nil {
		t.Fatal(e)
	}
	if f.IsError() {
		t.Fatal(strings.TrimSpace(string(f.Data)))
	}
	return f
}
