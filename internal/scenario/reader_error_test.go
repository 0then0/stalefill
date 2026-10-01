package scenario

import (
	"context"
	"testing"

	"github.com/0then0/stalefill/internal/resp"
)

func TestScheduleReasonSurvivesReaderError(t *testing.T) {
	s := NewScheduler("x", "")
	s.Arm()
	read := wire("GET", "x")
	read.Sequence = 1
	s.Queued(read)
	s.After(read, resp.Frame{Kind: '$', Null: true})
	first := wire("SET", "x", "opaque")
	first.Sequence = 2
	second := first
	second.Sequence = 3
	s.Queued(first)
	s.Queued(second)
	_, p, i := s.Snapshot()
	if p != "SF009" || i != "" {
		t.Fatal(p, i)
	}
	// Before refuses the ambiguous publication and closes its client socket.
	if err := s.Before(context.Background(), first); err == nil {
		t.Fatal("ambiguous fill accepted")
	}
	// The application converts that lost Redis reply into HTTP 502.
	s.ObserveReader(context.Background(), probeResult{status: 502, statusMismatch: true})
	r := InitialReport(Config{})
	classifySchedule(context.Background(), &r, s, "SF005")
	if r.Outcome != Unresolved || r.Findings[0].ID != "SF009" {
		t.Fatalf("lost schedule reason: %+v", r)
	}
}

func TestReaderFailureKeepsIndependentRedisError(t *testing.T) {
	s := NewScheduler("x", "")
	s.Arm()
	s.issueLocked("SF009")
	s.Error("Redis upstream connection failed")
	s.ObserveReader(context.Background(), probeResult{status: 502, statusMismatch: true})
	r := InitialReport(Config{})
	classifySchedule(context.Background(), &r, s, "SF005")
	if r.Outcome != InfrastructureError || r.Findings[0].ID != "SF100" {
		t.Fatal(r)
	}
}
