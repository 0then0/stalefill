package scenario

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"
	"time"

	"github.com/0then0/stalefill/internal/resp"
)

func TestPublicationLifetimes(t *testing.T) {
	for _, shape := range []string{"SET", "SETEX", "PSETEX", "HSET", "EXEC"} {
		for _, lifetime := range []string{"reader_first", "held_first", "synchronous"} {
			for _, connection := range []uint64{1, 2} {
				t.Run(shape+"/"+lifetime+"/connection="+strconv.FormatUint(connection, 10), func(t *testing.T) {
					s := NewScheduler("x", "")
					s.Arm()
					read := wire("GET", "x")
					miss := resp.Frame{Kind: '$', Null: true}
					if shape == "HSET" || shape == "EXEC" {
						read.Name, miss = "HGETALL", resp.Frame{Kind: '*'}
					}
					read.Sequence = 1
					s.Queued(read)
					s.After(read, miss)
					fill := wire(shape, "x", "opaque")
					if shape == "SETEX" || shape == "PSETEX" {
						fill = wire(shape, "x", "1000", "opaque")
					}
					if shape == "HSET" {
						fill = wire("HSET", "x", "field", "opaque")
					}
					if shape == "EXEC" {
						multi := wire("MULTI")
						multi.Connection = connection
						s.Queued(multi)
						fill = execCommand(wire("HSET", "x", "field", "opaque"))
					}
					fill.Connection, fill.Sequence = connection, 2
					ctx, cancel := context.WithTimeout(context.Background(), time.Second)
					defer cancel()
					readDone := make(chan probeResult, 1)
					type joined struct {
						old *probeResult
						err error
					}
					done := make(chan joined, 1)
					go func() { old, err := waitForPublication(ctx, s, readDone); done <- joined{old, err} }()
					if lifetime == "reader_first" {
						result := probeResult{status: 200, value: []byte(`1`)}
						s.ObserveReader(ctx, result)
						readDone <- result
						if err := s.wait(ctx, func() bool { return s.readerCompleted }); err != nil {
							t.Fatal(err)
						}
						select {
						case <-done:
							t.Fatal("HTTP completion ended publication lifetime")
						default:
						}
					}
					s.Queued(fill)
					forwarded := make(chan error, 1)
					go func() { forwarded <- s.Before(ctx, fill) }()
					if err := s.WaitHeld(ctx); err != nil {
						t.Fatal(err)
					}
					if lifetime == "held_first" {
						result := probeResult{status: 200, value: []byte(`1`)}
						s.ObserveReader(ctx, result)
						readDone <- result
					}
					result := <-done
					if result.err != nil {
						t.Fatal(result.err)
					}
					if lifetime == "reader_first" && (result.old == nil || string(result.old.value) != "1") {
						t.Fatal("lost early observation")
					}
					if lifetime == "synchronous" && result.old != nil {
						t.Fatal("required synchronous HTTP completion")
					}
					s.Event("write_started")
					writer := wire("DEL", "x")
					writer.Connection = 3
					s.Queued(writer)
					s.After(writer, resp.Frame{Kind: ':', Data: []byte("0")})
					s.Event("write_completed")
					s.Event("authoritative_confirmed")
					if err := s.Release(); err != nil {
						t.Fatal(err)
					}
					if err := <-forwarded; err != nil {
						t.Fatal(err)
					}
					reply := resp.Frame{Kind: '+'}
					if shape == "EXEC" {
						reply = resp.Frame{Kind: '*', Items: []resp.Frame{{Kind: ':', Data: []byte("1")}}}
					}
					s.After(fill, reply)
					if err := s.WaitApplied(ctx); err != nil {
						t.Fatal(err)
					}
					_, problem, infra := s.Snapshot()
					if problem != "" || infra != "" {
						t.Fatal(problem, infra)
					}
				})
			}
		}
	}
}

func TestEarlyReaderFailureAndPublicationTimeout(t *testing.T) {
	for _, tc := range []struct {
		name   string
		result probeResult
		failed bool
	}{
		{"transport", probeResult{err: errors.New("reader failed")}, true},
		{"status", probeResult{statusMismatch: true}, true},
		{"value", probeResult{mismatch: true}, true},
		{"missing_publication", probeResult{status: 200}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := NewScheduler("x", "")
			s.Arm()
			s.After(wire("GET", "x"), resp.Frame{Kind: '$', Null: true})
			ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
			defer cancel()
			readDone := make(chan probeResult, 1)
			s.ObserveReader(ctx, tc.result)
			readDone <- tc.result
			old, err := waitForPublication(ctx, s, readDone)
			if err == nil || (!tc.failed && old == nil) {
				t.Fatal("missing publication completed schedule")
			}
			if tc.failed && ctx.Err() != nil {
				t.Fatal("reader error hidden behind timeout")
			}
			if !tc.failed && ctx.Err() != context.DeadlineExceeded {
				t.Fatal("association window was not bounded")
			}
		})
	}
}

func TestConservativeAttribution(t *testing.T) {
	for _, reason := range []string{"prior_queued_fill", "prior_transaction", "wrong_shape", "second_read", "second_miss", "two_SET", "two_EXEC", "mixed_candidates"} {
		t.Run(reason, func(t *testing.T) {
			s := NewScheduler("x", "")
			fill := wire("SET", "x", "opaque")
			fill.Sequence = 2
			if reason == "prior_queued_fill" {
				s.Queued(fill)
			}
			if reason == "prior_transaction" {
				s.Queued(wire("MULTI"))
			}
			s.Arm()
			read := wire("GET", "x")
			read.Sequence = 1
			if reason == "two_EXEC" || reason == "prior_transaction" {
				read.Name = "HGET"
			}
			s.Queued(read)
			s.After(read, resp.Frame{Kind: '$', Null: true})
			switch reason {
			case "prior_queued_fill":
				_ = s.Before(context.Background(), fill)
			case "prior_transaction":
				fill = execCommand(wire("HSET", "x", "f", "opaque"))
				fill.Sequence = 3
				s.Queued(fill)
			case "wrong_shape":
				s.Queued(wire("HSET", "x", "f", "opaque"))
			case "second_read":
				read.Connection = 2
				s.Queued(read)
			case "second_miss":
				s.After(read, resp.Frame{Kind: '$', Null: true})
			default:
				if reason == "two_EXEC" {
					fill = execCommand(wire("HSET", "x", "f", "opaque"))
				}
				s.Queued(fill)
				fill.Connection = 2
				if reason == "mixed_candidates" {
					fill = execCommand(wire("HSET", "x", "f", "opaque"))
				}
				s.Queued(fill)
			}
			_, problem, _ := s.Snapshot()
			if problem != "SF009" {
				t.Fatal(problem)
			}
			if s.Release() == nil {
				t.Fatal("released ambiguous publication")
			}
		})
	}
}

func TestBaselinePublicationJoin(t *testing.T) {
	s := NewScheduler("x", "")
	s.Event("baseline_started")
	read := wire("GET", "x")
	read.Sequence = 1
	s.Queued(read)
	s.After(read, resp.Frame{Kind: '$', Null: true})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- s.WaitBaselinePublication(ctx) }()
	select {
	case <-done:
		t.Fatal("joined an unpublished baseline miss")
	default:
	}
	fill := wire("SET", "x", "opaque")
	fill.Sequence, fill.Connection = 2, 2
	s.Queued(fill)
	if err := s.Before(ctx, fill); err != nil {
		t.Fatal(err)
	}
	select {
	case <-done:
		t.Fatal("joined before upstream publication reply")
	default:
	}
	s.After(fill, resp.Frame{Kind: '+'})
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	// A duplicate from a completed baseline episode must never become race fill.
	s.Arm()
	read.Sequence = 3
	s.Queued(read)
	s.After(read, resp.Frame{Kind: '$', Null: true})
	if _, problem, infra := s.Snapshot(); problem != "" || infra != "" {
		t.Fatalf("new miss rejected before testing baseline fill: %s %s", problem, infra)
	}
	if err := s.Before(ctx, fill); err == nil {
		t.Fatal("accepted a baseline command as race publication")
	}
	events, problem, _ := s.Snapshot()
	if events[len(events)-1].Event != "publication_observed_before_miss" {
		t.Fatal(events)
	}
	if problem != "SF009" {
		t.Fatal(problem)
	}
}

func TestEarlyReaderFailureWhileWriterWaits(t *testing.T) {
	for _, tc := range []struct {
		name    string
		result  probeResult
		finding string
	}{
		{"transport", probeResult{err: errors.New("reader failed")}, "SF100"},
		{"status", probeResult{statusMismatch: true}, "SF100"},
		{"value", probeResult{mismatch: true}, "SF009"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := NewScheduler("x", "")
			s.Arm()
			s.After(wire("GET", "x"), resp.Frame{Kind: '$', Null: true})
			fill := wire("SET", "x", "opaque")
			s.Queued(fill)
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			blocked := make(chan error, 1)
			go func() { blocked <- s.Before(ctx, fill) }()
			if err := s.WaitHeld(ctx); err != nil {
				t.Fatal(err)
			}
			s.Event("write_started")
			h := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				s.ObserveReader(ctx, tc.result)
				<-r.Context().Done()
			}))
			defer h.Close()
			result := scheduleProbe(ctx, s, Probe{Method: "PUT", URL: h.URL, Status: 200})
			if result.err == nil || ctx.Err() != nil {
				t.Fatal("writer did not wake promptly on reader failure")
			}
			r := InitialReport(Config{})
			classifyProbe(&r, s)
			if r.Findings[0].ID != tc.finding {
				t.Fatal(r)
			}
			if s.Release() == nil {
				t.Fatal("released after reader failure")
			}
			cancel()
			if err := <-blocked; err != context.Canceled {
				t.Fatal(err)
			}
		})
	}
}

func TestDuplicateCandidateWhileHeld(t *testing.T) {
	s := NewScheduler("x", "")
	s.Arm()
	s.After(wire("GET", "x"), resp.Frame{Kind: '$', Null: true})
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	fill := wire("SET", "x", "opaque")
	s.Queued(fill)
	blocked := make(chan error, 1)
	go func() { blocked <- s.Before(ctx, fill) }()
	if err := s.WaitHeld(ctx); err != nil {
		t.Fatal(err)
	}
	fill.Connection = 2
	s.Queued(fill)
	if err := s.WaitHeld(ctx); err == nil {
		t.Fatal("selected one of two competing publications")
	}
	_, problem, _ := s.Snapshot()
	if problem != "SF009" || s.Release() == nil {
		t.Fatal(problem)
	}
	cancel()
	if err := <-blocked; err != context.Canceled {
		t.Fatal(err)
	}
}

func TestMissAfterReaderCompletionCannotAcquireAttribution(t *testing.T) {
	s := NewScheduler("x", "")
	s.Arm()
	s.ObserveReader(context.Background(), probeResult{status: 200})
	_, problem, _ := s.Snapshot()
	if problem != "SF005" {
		t.Fatal("reader without a preceding target miss was accepted", problem)
	}
	read := wire("GET", "x")
	read.Sequence = 1
	s.Queued(read)
	s.After(read, resp.Frame{Kind: '$', Null: true})
	if s.episode != nil {
		t.Fatal("unrelated later miss created reader episode")
	}
}

func TestBaselineNoPublicationContract(t *testing.T) {
	for _, publication := range []string{"none", "queued", "completed", "late"} {
		t.Run(publication, func(t *testing.T) {
			s := NewScheduler("x", "")
			s.Event("baseline_started")
			read := wire("GET", "x")
			read.Sequence = 1
			s.Queued(read)
			s.After(read, resp.Frame{Kind: '$', Null: true})
			fill := wire("SET", "x", "opaque")
			fill.Sequence = 2
			if publication == "queued" || publication == "completed" {
				s.Queued(fill)
				if publication == "completed" {
					if err := s.Before(context.Background(), fill); err != nil {
						t.Fatal(err)
					}
					s.After(fill, resp.Frame{Kind: '+'})
				}
			}
			err := s.CompleteBaselineWithoutPublication()
			if publication == "queued" || publication == "completed" {
				if err == nil {
					t.Fatal("accepted unexpected publication")
				}
			} else {
				if err != nil {
					t.Fatal(err)
				}
				if publication == "late" {
					s.Queued(fill)
				}
			}
			_, problem, infra := s.Snapshot()
			want := "SF009"
			if publication == "none" {
				want = ""
			}
			if problem != want || infra != "" {
				t.Fatal(problem, infra)
			}
		})
	}
}

func TestWriterReadPreservesPublicationConflicts(t *testing.T) {
	s := NewScheduler("x", "")
	s.Arm()
	read := wire("GET", "x")
	read.Sequence = 1
	s.Queued(read)
	s.After(read, resp.Frame{Kind: '$', Null: true})
	fill := wire("SET", "x", "opaque")
	fill.Sequence = 2
	s.Queued(fill)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- s.Before(ctx, fill) }()
	if err := s.WaitHeld(ctx); err != nil {
		t.Fatal(err)
	}
	s.Event("write_started")
	read.Connection = 2
	s.Queued(read)
	s.After(read, resp.Frame{Kind: '$', Null: true})
	if _, problem, infra := s.Snapshot(); problem != "" || infra != "" {
		t.Fatal(problem, infra)
	}
	fill.Connection = 2
	s.Queued(fill)
	if _, problem, _ := s.Snapshot(); problem != "SF009" {
		t.Fatal(problem)
	}
	cancel()
	<-done
}

func TestBaselineNoPublicationAfterCompletedEpisode(t *testing.T) {
	s := NewScheduler("x", "")
	s.Event("baseline_started")
	read := wire("GET", "x")
	s.Queued(read)
	s.After(read, resp.Frame{Kind: '$', Null: true})
	fill := wire("SET", "x", "opaque")
	s.Queued(fill)
	if err := s.Before(context.Background(), fill); err != nil {
		t.Fatal(err)
	}
	s.After(fill, resp.Frame{Kind: '+'})
	if err := s.WaitBaselinePublication(context.Background()); err != nil {
		t.Fatal(err)
	}
	s.After(read, resp.Frame{Kind: '$', Data: []byte("opaque")})
	if err := s.CompleteBaselineWithoutPublication(); err != nil {
		t.Fatal(err)
	}
}
