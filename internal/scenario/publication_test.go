package scenario

import (
	"context"
	"encoding/json"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/0then0/stalefill/internal/proxy"
	"github.com/0then0/stalefill/internal/resp"
)

func wire(name string, args ...string) proxy.Command {
	c := proxy.Command{Name: name, Connection: 1}
	for _, arg := range args {
		c.Args = append(c.Args, []byte(arg))
	}
	return c
}

func execCommand(commands ...proxy.Command) proxy.Command {
	return proxy.Command{Name: "EXEC", Connection: 1, InTransaction: true, Transaction: &proxy.Transaction{Commands: commands, Accepted: true}}
}

func TestHashMissRepresentations(t *testing.T) {
	for _, tc := range []struct {
		name string
		c    proxy.Command
		f    resp.Frame
		miss bool
	}{
		{"RESP2 empty", wire("HGETALL", "x"), resp.Frame{Kind: '*'}, true},
		{"RESP3 empty", wire("HGETALL", "x"), resp.Frame{Kind: '%'}, true},
		{"nonempty hash", wire("HGETALL", "x"), resp.Frame{Kind: '%', Items: []resp.Frame{{Kind: '$'}, {Kind: '$'}}}, false},
		{"null array is not HGETALL miss", wire("HGETALL", "x"), resp.Frame{Kind: '*', Null: true}, false},
		{"empty bulk is not HGETALL miss", wire("HGETALL", "x"), resp.Frame{Kind: '$'}, false},
		{"HGET null", wire("HGET", "x", "field"), resp.Frame{Kind: '_', Null: true}, true},
		{"HGET empty value", wire("HGET", "x", "field"), resp.Frame{Kind: '$'}, false},
		{"HMGET all absent", wire("HMGET", "x", "a", "b"), resp.Frame{Kind: '*', Items: []resp.Frame{{Kind: '$', Null: true}, {Kind: '_', Null: true}}}, true},
		{"HMGET partial hit", wire("HMGET", "x", "a", "b"), resp.Frame{Kind: '*', Items: []resp.Frame{{Kind: '$'}, {Kind: '$', Null: true}}}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := NewScheduler("x", "")
			s.Arm()
			s.After(tc.c, tc.f)
			if s.miss != tc.miss {
				t.Fatalf("miss=%v", s.miss)
			}
		})
	}
}

func TestPublicationShapes(t *testing.T) {
	hset := wire("HSET", "x", "field", "opaque")
	ttl := wire("EXPIRE", "x", "5")
	for _, tc := range []struct {
		name        string
		c           proxy.Command
		mode        string
		unsupported bool
	}{
		{"single hash", hset, "hash", false},
		{"hash TTL", execCommand(hset, ttl), "transactional_hash", false},
		{"replace hash", execCommand(wire("DEL", "x"), hset, ttl), "transactional_hash", false},
		{"multiple fields", execCommand(hset, hset, ttl), "transactional_hash", false},
		{"unrelated", execCommand(wire("HSET", "other", "f", "v")), "", false},
		{"mixed keys", execCommand(hset, wire("HSET", "other", "f", "v")), "", true},
		{"arbitrary mutation", execCommand(wire("INCR", "x"), wire("HSET", "other", "f", "v")), "", true},
		{"post write delete", execCommand(hset, wire("DEL", "x")), "", true},
		{"invalid TTL", execCommand(hset, wire("EXPIRE", "x", "0")), "", true},
		{"unknown program", execCommand(wire("CUSTOM", "other")), "", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := NewScheduler("x", "")
			p, unsupported := s.publication(tc.c)
			if p.Mode != tc.mode || unsupported != tc.unsupported {
				t.Fatalf("%+v unsupported=%v", p, unsupported)
			}
		})
	}
	s := NewScheduler("", "^product:[0-9]+$")
	_, unsupported := s.publication(execCommand(wire("HSET", "product:1", "f", "v"), wire("HSET", "product:2", "f", "v")))
	if !unsupported {
		t.Fatal("ambiguous regex transaction accepted")
	}
}

func TestTransactionalPublicationBarrier(t *testing.T) {
	s := NewScheduler("x", "")
	s.Arm()
	s.After(wire("HGETALL", "x"), resp.Frame{Kind: '*'})
	del := wire("DEL", "x")
	del.InTransaction = true
	s.Queued(del)
	s.After(del, resp.Frame{Kind: '+', Data: []byte("QUEUED")})
	if s.seenInvalidation || s.invalidated {
		t.Fatal("fill DEL counted as writer invalidation")
	}
	exec := execCommand(del, wire("HSET", "x", "field", "SECRET_VALUE"), wire("EXPIRE", "x", "5"))
	s.Queued(exec)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- s.Before(ctx, exec) }()
	if err := s.WaitHeld(ctx); err != nil {
		t.Fatal(err)
	}
	if s.Release() == nil {
		t.Fatal("released without writer")
	}
	s.Event("write_started")
	writer := wire("DEL", "x")
	writer.Connection = 2
	s.Queued(writer)
	s.After(writer, resp.Frame{Kind: ':', Data: []byte("0")})
	s.Event("write_completed")
	s.Event("authoritative_confirmed")
	if err := s.Release(); err != nil {
		t.Fatal(err)
	}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	s.After(exec, resp.Frame{Kind: '*', Items: []resp.Frame{{Kind: ':', Data: []byte("0")}, {Kind: ':', Data: []byte("1")}, {Kind: ':', Data: []byte("1")}}})
	if err := s.WaitApplied(ctx); err != nil {
		t.Fatal(err)
	}
	if s.FillMode() != "transactional_hash" {
		t.Fatal(s.FillMode())
	}
}

func TestPublicationAbortAndDiscard(t *testing.T) {
	for _, f := range []resp.Frame{{Kind: '*', Null: true}, {Kind: '_', Null: true}, {Kind: '-'}, {Kind: '*', Items: []resp.Frame{{Kind: '-'}}}, {Kind: '*'}} {
		s := NewScheduler("x", "")
		s.armed, s.held, s.released = true, true, true
		s.After(execCommand(wire("HSET", "x", "f", "v")), f)
		_, problem, _ := s.Snapshot()
		if s.applied || problem != "SF008" {
			t.Fatal("unsuccessful EXEC claimed applied", problem)
		}
	}
	s := NewScheduler("x", "")
	s.Arm()
	c := execCommand(wire("HSET", "x", "f", "v"))
	c.Name = "DISCARD"
	s.Queued(c)
	s.After(c, resp.Frame{Kind: '+', Data: []byte("OK")})
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if s.WaitHeld(ctx) == nil || ctx.Err() != nil {
		t.Fatal("DISCARD not resolved immediately")
	}
	_, p, _ := s.Snapshot()
	if p != "SF008" {
		t.Fatal(p)
	}
}

func TestLuaDeclaredKeysOnly(t *testing.T) {
	for _, name := range []string{"EVAL", "EVALSHA", "EVAL_RO", "EVALSHA_RO"} {
		for _, tc := range []struct {
			args        []string
			unsupported bool
		}{
			{[]string{"x", "1", "lock:x", "x"}, false},
			{[]string{"irrelevant", "0", "x"}, false},
			{[]string{"irrelevant", "1", "x"}, true},
			{[]string{"irrelevant", "2", "other", "x"}, true},
			{[]string{"irrelevant", "-1", "lock:x"}, true},
			{[]string{"irrelevant", "+1", "lock:x"}, true},
			{[]string{"irrelevant", "01", "lock:x"}, true},
			{[]string{"irrelevant", "-0", "lock:x"}, true},
			{[]string{"irrelevant", "", "lock:x"}, true},
			{[]string{"irrelevant", " 1", "lock:x"}, true},
			{[]string{"irrelevant", "9223372036854775808", "lock:x"}, true},
			{[]string{"irrelevant", "huge", "lock:x"}, true},
			{[]string{"irrelevant", "2", "lock:x"}, true},
		} {
			s := NewScheduler("x", "")
			c := wire(name, tc.args...)
			s.Queued(c)
			s.After(c, resp.Frame{Kind: '-', Data: []byte("NOSCRIPT")})
			_, p, infra := s.Snapshot()
			if infra != "" {
				t.Fatal("opaque Lua classified as infrastructure failure", infra)
			}
			if (p != "") != tc.unsupported {
				t.Fatalf("%s %v problem=%s", name, tc.args, p)
			}
		}
	}
}

func TestPublicationReportsExcludeHashAndLuaData(t *testing.T) {
	key := "SECRET_TARGET_KEY"
	s := NewScheduler(key, "")
	s.Queued(wire("EVAL", "SECRET_LUA_SOURCE", "1", "unrelated-lock", "SECRET_ARGV"))
	s.Arm()
	s.After(wire("HGETALL", key), resp.Frame{Kind: '%'})
	exec := execCommand(wire("HSET", key, "SECRET_FIELD", "SECRET_HASH_VALUE"), wire("EXPIRE", key, "5"))
	s.Queued(exec)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- s.Before(ctx, exec) }()
	if err := s.WaitHeld(ctx); err != nil {
		t.Fatal(err)
	}
	s.Event("write_started")
	del := wire("DEL", key)
	del.Connection = 2
	s.After(del, resp.Frame{Kind: ':', Data: []byte("0")})
	s.Event("write_completed")
	s.Event("authoritative_confirmed")
	if err := s.Release(); err != nil {
		t.Fatal(err)
	}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	s.After(exec, resp.Frame{Kind: '*', Items: []resp.Frame{{Kind: ':', Data: []byte("1")}, {Kind: ':', Data: []byte("1")}}})
	r := InitialReport(Config{})
	r.Events, _, _ = s.Snapshot()
	r.KeyHash, r.FillMode = s.KeyHash(), s.FillMode()
	encoded, err := json.Marshal(r)
	if err != nil {
		t.Fatal(err)
	}
	for _, secret := range []string{key, "SECRET_LUA_SOURCE", "SECRET_ARGV", "SECRET_FIELD", "SECRET_HASH_VALUE"} {
		if strings.Contains(string(encoded), secret) {
			t.Fatal("report leaked", secret)
		}
	}
}

func TestDuplicateEXECAndSameConnectionInvalidation(t *testing.T) {
	for _, duplicate := range []bool{false, true} {
		s := NewScheduler("x", "")
		s.Arm()
		exec := execCommand(wire("HSET", "x", "f", "v"))
		s.Queued(exec)
		if duplicate {
			exec.Connection = 2
			s.Queued(exec)
		} else {
			s.Queued(wire("DEL", "x"))
		}
		_, p, _ := s.Snapshot()
		want := "SF006"
		if duplicate {
			want = "SF009"
		}
		if p != want {
			t.Fatal(p)
		}
	}
}

func TestUnsupportedHashMutationsSelectOnlyTheirKey(t *testing.T) {
	for _, name := range []string{"HSETEX", "HGETDEL", "HGETEX", "HEXPIRE", "HPEXPIRE", "HEXPIREAT", "HPEXPIREAT", "HPERSIST"} {
		for _, selector := range []string{"exact", "regex"} {
			for _, target := range []bool{false, true} {
				t.Run(name+"/"+selector+"/target="+strconv.FormatBool(target), func(t *testing.T) {
					s := NewScheduler("target", "")
					if selector == "regex" {
						s = NewScheduler("", "^target$")
					}
					key := "unrelated"
					if target {
						key = "target"
					}
					// A field/value equal to the selector must not count as a key.
					c := wire(name, key, "FIELDS", "1", "target", "target")
					s.Queued(c)
					s.After(c, resp.Frame{Kind: ':', Data: []byte("1")})
					_, problem, infra := s.Snapshot()
					want := ""
					if target {
						want = "SF006"
					}
					if problem != want || infra != "" {
						t.Fatalf("problem=%q infra=%q, want %q", problem, infra, want)
					}
				})
			}
		}
	}
}
