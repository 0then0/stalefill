package scenario

import (
	"context"
	"testing"
	"time"

	"github.com/0then0/stalefill/internal/proxy"
	"github.com/0then0/stalefill/internal/resp"
)

func command(name, key string) proxy.Command {
	return proxy.Command{Name: name, Args: [][]byte{[]byte(key)}}
}
func TestBarrierOrdering(t *testing.T) {
	s := NewScheduler("x", "")
	s.Arm()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	wrong := command("SET", "other")
	s.Queued(wrong)
	if s.Before(ctx, wrong) != nil {
		t.Fatal("unrelated key held")
	}
	s.After(command("GET", "x"), resp.Frame{Kind: '$', Null: true})
	set := command("SET", "x")
	s.Queued(set)
	done := make(chan error, 1)
	go func() { done <- s.Before(ctx, set) }()
	if e := s.WaitHeld(ctx); e != nil {
		t.Fatal(e)
	}
	select {
	case <-done:
		t.Fatal("released before invalidation")
	default:
	}
	s.After(command("DEL", "other"), resp.Frame{Kind: ':'})
	s.mu.Lock()
	invalidated := s.invalidated
	s.mu.Unlock()
	if invalidated {
		t.Fatal("wrong key invalidated barrier")
	}
	s.Event("write_started")
	s.After(command("DEL", "x"), resp.Frame{Kind: ':'})
	if e := s.WaitInvalidation(ctx); e != nil {
		t.Fatal(e)
	}
	s.Event("write_completed")
	s.Event("authoritative_confirmed")
	if e := s.Release(); e != nil {
		t.Fatal(e)
	}
	if e := <-done; e != nil {
		t.Fatal(e)
	}
	s.After(set, resp.Frame{Kind: '+'})
	if e := s.WaitApplied(ctx); e != nil {
		t.Fatal(e)
	}
	events, _, _ := s.Snapshot()
	want := []string{"read_started", "cache_miss_observed", "stale_set_held", "write_started", "invalidation_applied", "write_completed", "authoritative_confirmed", "stale_set_released", "stale_set_completed"}
	for i, e := range events {
		if e.Seq != i+1 || e.Event != want[i] {
			t.Fatalf("event %d: %s", i, e.Event)
		}
	}
}
func TestMissingDuplicateUnexpectedAndCancellation(t *testing.T) {
	t.Run("timeout", func(t *testing.T) {
		s := NewScheduler("x", "")
		ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond)
		defer cancel()
		if s.WaitHeld(ctx) == nil {
			t.Fatal("missing SET passed")
		}
	})
	t.Run("duplicate", func(t *testing.T) {
		s := NewScheduler("x", "")
		s.Arm()
		s.Queued(command("SET", "x"))
		s.Queued(command("SETEX", "x"))
		_, p, _ := s.Snapshot()
		if p != "SF006" {
			t.Fatal(p)
		}
	})
	t.Run("opaque", func(t *testing.T) {
		s := NewScheduler("x", "")
		s.Queued(command("EVAL", "x"))
		_, p, _ := s.Snapshot()
		if p != "SF006" {
			t.Fatal(p)
		}
	})
	t.Run("fill before miss", func(t *testing.T) {
		s := NewScheduler("x", "")
		s.Arm()
		if s.Before(context.Background(), command("SET", "x")) == nil {
			t.Fatal("accepted missing miss")
		}
	})
	t.Run("cancel held", func(t *testing.T) {
		s := NewScheduler("x", "")
		s.Arm()
		s.After(command("GET", "x"), resp.Frame{Kind: '$', Null: true})
		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan error, 1)
		go func() { done <- s.Before(ctx, command("SET", "x")) }()
		wait, c := context.WithTimeout(context.Background(), time.Second)
		defer c()
		if e := s.WaitHeld(wait); e != nil {
			t.Fatal(e)
		}
		cancel()
		if <-done != context.Canceled {
			t.Fatal("held command did not cancel")
		}
	})
	t.Run("MGET selected miss", func(t *testing.T) {
		s := NewScheduler("x", "")
		s.Arm()
		s.After(proxy.Command{Name: "MGET", Args: [][]byte{[]byte("other"), []byte("x")}}, resp.Frame{Kind: '*', Items: []resp.Frame{{Kind: '$', Data: []byte("hit")}, {Kind: '$', Null: true}}})
		s.mu.Lock()
		defer s.mu.Unlock()
		if !s.miss {
			t.Fatal("MGET miss not detected")
		}
	})
	t.Run("regex ambiguity", func(t *testing.T) {
		s := NewScheduler("", `^product:[0-9]+$`)
		s.Arm()
		s.Queued(command("GET", "product:1"))
		s.Queued(command("GET", "product:2"))
		_, p, _ := s.Snapshot()
		if p != "SF006" {
			t.Fatal(p)
		}
	})
}

func TestSameConnectionInvalidationUnsupported(t *testing.T) {
	s := NewScheduler("x", "")
	s.Arm()
	fill := command("SET", "x")
	fill.Connection = 1
	s.Queued(fill)
	del := command("DEL", "x")
	del.Connection = 1
	s.Queued(del)
	_, p, _ := s.Snapshot()
	if p != "SF006" {
		t.Fatal("same-connection invalidation would deadlock", p)
	}
}

func TestCanceledScheduleWaitClassification(t *testing.T) {
	for _, fallback := range []string{"SF003", "SF005"} {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		r := InitialReport(Config{})
		classifySchedule(ctx, &r, NewScheduler("x", ""), fallback)
		if r.Outcome != InfrastructureError || len(r.Findings) != 1 || r.Findings[0].ID != "SF100" {
			t.Fatal(r)
		}
	}
}
