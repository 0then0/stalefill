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
	fillMode                                          string
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
	if isLua(c.Name) {
		_, valid := luaKeys(c)
		if !valid || s.target(c) {
			s.issueLocked("SF006")
		}
		return
	}
	if c.Name == "FCALL" || c.Name == "FCALL_RO" || c.Name == "FLUSHALL" || c.Name == "FLUSHDB" || c.Name == "SWAPDB" {
		s.issueLocked("SF006")
		return
	}
	pub, unsupported := s.publication(c)
	if !c.InTransaction && s.target(c) && !isRead(c.Name) && !isFill(c.Name) && !isDelete(c.Name) {
		switch c.Name {
		case "EXPIRE", "PEXPIRE", "TTL", "PTTL", "EXISTS", "WATCH":
		default:
			s.issueLocked("SF006")
		}
	}
	if unsupported && c.Name != "DISCARD" {
		s.issueLocked("SF006")
	}
	if !s.armed {
		return
	}
	keys := s.keys(c)
	if pub.Mode != "" {
		keys = [][]byte{pub.Key}
	}
	for _, k := range keys {
		if s.match(k) {
			if s.selected == "" {
				s.selected = string(k)
			} else if s.selected != string(k) {
				s.issueLocked("SF006")
			}
		}
	}
	// Queued transaction members have no publication or invalidation effect.
	if c.InTransaction && c.Name != "EXEC" {
		return
	}
	if isDelete(c.Name) && s.target(c) && s.sets > 0 && !s.released && c.Connection != 0 && c.Connection == s.fillConnection {
		s.issueLocked("SF006")
	}
	if pub.Mode != "" && !s.released {
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
	pub, unsupported := s.publication(c)
	if unsupported && c.Name != "DISCARD" {
		s.issueLocked("SF006")
	}
	// Non-atomic DEL/HSET publication has no supported group boundary.
	if s.armed && s.miss && !s.held && !c.InTransaction && isDelete(c.Name) && s.target(c) {
		s.issueLocked("SF006")
	}
	if !s.armed || pub.Mode == "" || s.released {
		s.mu.Unlock()
		return nil
	}
	if c.Name == "EXEC" && !c.Transaction.Accepted {
		s.issueLocked("SF008")
		s.mu.Unlock()
		return nil
	}
	if s.problem != "" || s.infra != "" {
		s.mu.Unlock()
		return errors.New("unsupported publication")
	}
	if !s.miss {
		s.issueLocked("SF005")
		s.mu.Unlock()
		return errors.New("fill without observed miss")
	}
	if !s.held {
		s.held = true
		s.fillMode = pub.Mode
		s.fillConnection = c.Connection
		s.phase = PhaseHeld
		if c.Name == "EXEC" {
			s.eventLocked("fill_transaction_identified", "EXEC")
			s.eventLocked("fill_exec_held", "EXEC")
		} else {
			s.eventLocked("stale_set_held", c.Name)
		}
	}
	release := s.release
	s.mu.Unlock()
	select {
	case <-release:
		if ctx.Err() != nil {
			return ctx.Err()
		}
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
func (s *Scheduler) After(c proxy.Command, f resp.Frame) {
	s.mu.Lock()
	defer s.mu.Unlock()
	// Opaque Lua has already been classified using declared KEYS. NOSCRIPT
	// on an unrelated lock is a normal client fallback, and target Lua stays
	// unsupported even if its reply is an error.
	if isLua(c.Name) {
		return
	}
	// Redis's QUEUED response is not an applied mutation or a cache miss.
	if c.InTransaction && c.Name != "EXEC" && c.Name != "DISCARD" {
		return
	}
	pub, unsupported := s.publication(c)
	if c.Name == "DISCARD" && unsupported {
		s.issueLocked("SF008")
		return
	}
	if unsupported {
		s.issueLocked("SF006")
		return
	}
	if pub.Mode == "" && !s.target(c) {
		return
	}
	if c.Name == "EXEC" && pub.Mode != "" && !publicationSucceeded(c, f) {
		s.issueLocked("SF008")
		return
	}
	if f.IsError() {
		s.infra = "SF100"
		s.eventLocked("redis_command_error", c.Name)
		return
	}
	if isRead(c.Name) {
		s.seenRead = true
		if s.armed && s.readMiss(c, f) && !s.miss {
			s.miss = true
			s.phase = PhaseMiss
			s.eventLocked("cache_miss_observed", c.Name)
		}
	}
	if pub.Mode != "" {
		s.seenFill = true
		if s.armed && s.held && s.released && !s.applied && c.Connection == s.fillConnection {
			s.applied = true
			s.phase = PhaseApplied
			if c.Name == "EXEC" {
				s.eventLocked("fill_exec_completed", c.Name)
			} else {
				s.eventLocked("stale_set_completed", c.Name)
			}
		}
	}
	if isDelete(c.Name) && f.Kind == ':' {
		s.seenInvalidation = true
		if s.armed && s.held && !s.invalidated {
			if s.phase == PhaseHeld || (c.Connection != 0 && c.Connection == s.fillConnection && !s.released) {
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
		if s.fillMode == "transactional_hash" {
			s.eventLocked("fill_exec_released", "EXEC")
		} else {
			s.eventLocked("stale_set_released", "")
		}
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

func (s *Scheduler) FillMode() string { s.mu.Lock(); defer s.mu.Unlock(); return s.fillMode }
