package scenario

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"regexp"
	"sync"
	"time"

	"github.com/0then0/stalefill/internal/proxy"
	"github.com/0then0/stalefill/internal/resp"
)

// Phase is the explicit state of the modeled schedule. Invalidation and the
// HTTP write can complete in either order; their join gates release.
type Phase string

const (
	PhaseStart               Phase = "START"
	PhaseBaseline            Phase = "BASELINE"
	PhasePrepared            Phase = "PREPARED"
	PhaseReading             Phase = "READ_STARTED"
	PhaseMiss                Phase = "CACHE_MISS_OBSERVED"
	PhaseHeld                Phase = "STALE_SET_HELD"
	PhaseWriting             Phase = "WRITE_STARTED"
	PhaseWaitingInvalidation Phase = "WAITING_INVALIDATION"
	PhaseReady               Phase = "WRITE_AND_INVALIDATION_COMPLETED"
	PhaseConfirmed           Phase = "AUTHORITATIVE_CONFIRMED"
	PhaseReleased            Phase = "STALE_SET_RELEASED"
	PhaseApplied             Phase = "STALE_SET_COMPLETED"
	PhaseReadCompleted       Phase = "READ_COMPLETED"
	PhaseVerify              Phase = "VERIFY"
)

type Event struct {
	State     Phase  `json:"state"`
	Seq       int    `json:"seq"`
	Event     string `json:"event"`
	ElapsedMS int64  `json:"elapsed_ms"`
	Command   string `json:"command,omitempty"`
}
type Scheduler struct {
	phase                                             Phase
	writeDone, confirmed                              bool
	fillConnection                                    uint64
	mu                                                sync.Mutex
	start                                             time.Time
	key, selected                                     string
	regex                                             *regexp.Regexp
	events                                            []Event
	changed                                           chan struct{}
	release                                           chan struct{}
	armed, miss, held, invalidated, released, applied bool
	sets                                              int
	seenRead, seenFill, seenInvalidation              bool
	problem, infra                                    string
}

func NewScheduler(key, pattern string) *Scheduler {
	s := &Scheduler{phase: PhaseStart, start: time.Now(), key: key, changed: make(chan struct{}), release: make(chan struct{})}
	if pattern != "" {
		s.regex = regexp.MustCompile(pattern)
	}
	return s
}
func (s *Scheduler) eventLocked(name, command string) {
	s.events = append(s.events, Event{State: s.phase, Seq: len(s.events) + 1, Event: name, ElapsedMS: time.Since(s.start).Milliseconds(), Command: command})
	close(s.changed)
	s.changed = make(chan struct{})
}
func (s *Scheduler) Event(name string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	switch name {
	case "baseline_started":
		s.phase = PhaseBaseline
	case "prepared":
		s.phase = PhasePrepared
	case "write_started":
		if s.phase != PhaseHeld {
			s.issueLocked("SF005")
			return
		}
		s.phase = PhaseWriting
	case "write_completed":
		s.writeDone = true
		s.phase = PhaseWaitingInvalidation
		if s.invalidated {
			s.phase = PhaseReady
		}
	case "authoritative_confirmed":
		if !s.writeDone || !s.invalidated {
			s.issueLocked("SF005")
			return
		}
		s.confirmed = true
		s.phase = PhaseConfirmed
	case "read_completed":
		s.phase = PhaseReadCompleted
	case "verify_started":
		s.phase = PhaseVerify
	case "verification_passed":
		s.phase = Phase(Pass)
	case "verification_stale":
		s.phase = Phase(Fail)
	case "verification_inconclusive":
		s.phase = Phase(Unresolved)
	}
	s.eventLocked(name, "")
}
func (s *Scheduler) match(key []byte) bool {
	return s.key != "" && string(key) == s.key || s.regex != nil && s.regex.Match(key)
}
func (s *Scheduler) keys(c proxy.Command) [][]byte {
	switch c.Name {
	case "GET", "SET", "SETEX", "PSETEX", "EXPIRE", "PEXPIRE":
		if len(c.Args) > 0 {
			return c.Args[:1]
		}
	case "MGET", "DEL", "UNLINK":
		return c.Args
	}
	return nil
}
func (s *Scheduler) target(c proxy.Command) bool {
	for _, k := range s.keys(c) {
		if s.match(k) {
			return true
		}
	}
	return false
}
func isFill(name string) bool   { return name == "SET" || name == "SETEX" || name == "PSETEX" }
func isDelete(name string) bool { return name == "DEL" || name == "UNLINK" }
func opaque(name string) bool {
	switch name {
	case "MULTI", "EXEC", "EVAL", "EVALSHA", "EVAL_RO", "EVALSHA_RO", "FCALL", "FCALL_RO":
		return true
	}
	return false
}
func (s *Scheduler) issueLocked(id string) {
	if s.problem == "" {
		s.problem = id
		s.phase = Phase(Unresolved)
		s.eventLocked("unsupported_or_ambiguous_schedule", "")
	}
}
func (s *Scheduler) Queued(c proxy.Command) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if opaque(c.Name) {
		s.issueLocked("SF006")
		return
	}
	if !s.armed || !s.target(c) {
		return
	}
	// Freeze the regex-selected key to one concrete key for this schedule.
	for _, k := range s.keys(c) {
		if s.match(k) {
			if s.selected == "" {
				s.selected = string(k)
			} else if s.selected != string(k) {
				s.issueLocked("SF006")
			}
		}
	}
	if isDelete(c.Name) && s.sets > 0 && !s.released && c.Connection != 0 && c.Connection == s.fillConnection {
		s.issueLocked("SF006")
	}
	if isFill(c.Name) && !s.released {
		s.sets++
		if s.sets == 1 {
			s.fillConnection = c.Connection
		}
		if s.sets > 1 {
			s.issueLocked("SF006")
		}
	}
}
func (s *Scheduler) Before(ctx context.Context, c proxy.Command) error {
	s.mu.Lock()
	if !s.armed || !s.target(c) || !isFill(c.Name) || s.released {
		s.mu.Unlock()
		return nil
	}
	if !s.miss {
		s.issueLocked("SF005")
		s.mu.Unlock()
		return errors.New("fill without observed miss")
	}
	if !s.held {
		s.held = true
		s.phase = PhaseHeld
		s.eventLocked("stale_set_held", c.Name)
	}
	release := s.release
	s.mu.Unlock()
	select {
	case <-release:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
func (s *Scheduler) After(c proxy.Command, f resp.Frame) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.target(c) {
		return
	}
	if f.IsError() {
		s.infra = "SF100"
		s.eventLocked("redis_command_error", c.Name)
		return
	}
	if c.Name == "GET" || c.Name == "MGET" {
		s.seenRead = true
		miss := c.Name == "GET" && f.Null
		if c.Name == "MGET" && f.Kind == '*' {
			for i, k := range c.Args {
				if s.match(k) && i < len(f.Items) && f.Items[i].Null {
					miss = true
				}
			}
		}
		if s.armed && miss && !s.miss {
			s.miss = true
			s.phase = PhaseMiss
			s.eventLocked("cache_miss_observed", c.Name)
		}
	}
	if isFill(c.Name) {
		s.seenFill = true
		if s.armed && s.held && s.released && !s.applied {
			s.applied = true
			s.phase = PhaseApplied
			s.eventLocked("stale_set_completed", c.Name)
		}
	}
	if isDelete(c.Name) && f.Kind == ':' {
		s.seenInvalidation = true
		if s.armed && s.held && !s.invalidated {
			if s.phase == PhaseHeld {
				s.issueLocked("SF006")
				return
			}
			s.invalidated = true
			if s.writeDone {
				s.phase = PhaseReady
			}
			s.eventLocked("invalidation_applied", c.Name)
		}
	}
}
func (s *Scheduler) Error(message string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.infra == "" {
		s.infra = "SF100"
		if message == "unsupported Redis response mode" || message == "unsupported unsolicited RESP3 push" {
			s.infra = ""
			s.issueLocked("SF006")
			return
		}
		s.eventLocked(message, "")
	}
}
func (s *Scheduler) Arm() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.armed = true
	s.phase = PhaseReading
	s.eventLocked("read_started", "")
}
func (s *Scheduler) Release() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.confirmed || !s.invalidated || !s.writeDone || s.problem != "" || s.infra != "" {
		return errors.New("release prerequisites absent")
	}
	if !s.released {
		s.released = true
		s.phase = PhaseReleased
		s.eventLocked("stale_set_released", "")
		close(s.release)
	}
	return nil
}
func (s *Scheduler) wait(ctx context.Context, condition func() bool) error {
	for {
		s.mu.Lock()
		if s.infra != "" || s.problem != "" {
			s.mu.Unlock()
			return errors.New("schedule error")
		}
		if condition() {
			s.mu.Unlock()
			return nil
		}
		ch := s.changed
		s.mu.Unlock()
		select {
		case <-ch:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}
func (s *Scheduler) WaitHeld(ctx context.Context) error {
	return s.wait(ctx, func() bool { return s.held })
}
func (s *Scheduler) WaitInvalidation(ctx context.Context) error {
	return s.wait(ctx, func() bool { return s.invalidated })
}
func (s *Scheduler) WaitApplied(ctx context.Context) error {
	return s.wait(ctx, func() bool { return s.applied })
}
func (s *Scheduler) Snapshot() ([]Event, string, string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]Event(nil), s.events...), s.problem, s.infra
}
func (s *Scheduler) BaselineTraffic() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.seenRead && s.seenFill && s.seenInvalidation
}
func (s *Scheduler) KeyHash() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	key := s.key
	if key == "" {
		key = s.selected
	}
	sum := sha256.Sum256([]byte(key))
	return hex.EncodeToString(sum[:])
}
