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
	State          Phase  `json:"state"`
	Seq            int    `json:"seq"`
	Event          string `json:"event"`
	ElapsedMS      int64  `json:"elapsed_ms"`
	Command        string `json:"command,omitempty"`
	Episode        uint64 `json:"miss_episode,omitempty"`
	MissConnection uint64 `json:"miss_connection,omitempty"`
	FillConnection uint64 `json:"fill_connection,omitempty"`
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
	baseline                                          bool
	episode                                           *MissEpisode
	nextEpisode                                       uint64
	candidates                                        map[commandRef]*MissEpisode
	transactionEpisodes                               map[uint64]*MissEpisode
	raceRead                                          commandRef
	raceReads                                         int
	readerCompleted                                   bool
}

func NewScheduler(key, pattern string) *Scheduler {
	s := &Scheduler{phase: PhaseStart, start: time.Now(), key: key, changed: make(chan struct{}), release: make(chan struct{}), candidates: make(map[commandRef]*MissEpisode), transactionEpisodes: make(map[uint64]*MissEpisode)}
	if pattern != "" {
		s.regex = regexp.MustCompile(pattern)
	}
	return s
}
func (s *Scheduler) eventLocked(name, command string) {
	e := Event{State: s.phase, Seq: len(s.events) + 1, Event: name, ElapsedMS: time.Since(s.start).Milliseconds(), Command: command}
	if s.episode != nil {
		e.Episode, e.MissConnection, e.FillConnection = s.episode.ID, s.episode.MissConnection, s.episode.Publication.connection
	}
	s.events = append(s.events, e)
	close(s.changed)
	s.changed = make(chan struct{})
}
func (s *Scheduler) Event(name string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	switch name {
	case "baseline_started":
		s.baseline = true
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

// ObserveReader records actual probe completion, independently of when the
// runner consumes its result. A failed early reader also wakes a waiting writer
// so that no publication is released on an invalid old observation.
func (s *Scheduler) ObserveReader(ctx context.Context, result probeResult) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.readerCompleted = true
	if s.episode != nil {
		s.episode.ReaderCompleted = true
	}
	if result.err == nil {
		// Completion is an observation, not a scheduling phase transition.
		if s.applied {
			s.phase = PhaseReadCompleted
		}
		s.eventLocked("read_completed", "")
	}
	if !s.released && ctx.Err() == nil {
		if result.err != nil || result.statusMismatch {
			if s.problem == "" {
				s.infra = "SF100"
			}
			s.eventLocked("reader_probe_failed", "")
		} else if result.mismatch {
			s.attributionProblemLocked("reader_old_observation_missing", "")
		} else if !s.miss && s.problem == "" {
			// Proxy After observes a miss before delivering its reply to the
			// application. A miss arriving after HTTP completion cannot belong
			// to this read, even if a later worker uses the same target key.
			s.problem = "SF005"
			s.phase = Phase(Unresolved)
			s.eventLocked("reader_completed_without_target_miss", "")
		}
	}
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
	if c.Name == "MULTI" && !c.InTransaction {
		s.transactionEpisodes[c.Connection] = s.episode
	}
	if c.Name == "EXEC" || c.Name == "DISCARD" {
		if pub.Mode != "" && c.Sequence != 0 && !s.released && s.transactionEpisodes[c.Connection] != s.episode {
			s.attributionProblemLocked("transaction_started_before_miss", c.Name)
		}
		delete(s.transactionEpisodes, c.Connection)
	}
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
	if !s.armed {
		if s.baseline && pub.Mode != "" {
			s.candidateLocked(c, pub)
		}
		return
	}
	if !c.InTransaction && isRead(c.Name) && s.target(c) && !s.held && !s.released {
		s.raceReads++
		if s.raceReads == 1 {
			s.raceRead = reference(c)
		} else {
			s.attributionProblemLocked("competing_target_read", c.Name)
		}
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
			s.attributionProblemLocked("multiple_publication_candidates", c.Name)
		} else if s.miss {
			s.candidateLocked(c, pub)
		}
	}
}
func (s *Scheduler) Before(ctx context.Context, c proxy.Command) error {
	s.mu.Lock()
	pub, unsupported := s.publication(c)
	if pub.Mode != "" && c.Sequence != 0 {
		e, ok := s.candidates[reference(c)]
		delete(s.candidates, reference(c))
		if !s.released && (!ok || e != s.episode) {
			s.attributionProblemLocked("publication_observed_before_miss", c.Name)
		}
	}
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
	if s.episode == nil || !s.episode.accepts(pub) {
		s.attributionProblemLocked("publication_without_matching_episode", c.Name)
		s.mu.Unlock()
		return errors.New("publication attribution failed")
	}
	if s.episode.Candidates == 0 {
		// Direct hook callers can omit Queued; the proxy always supplies it.
		s.candidateLocked(c, pub)
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
		if s.armed && !s.held && !s.released && c.Sequence != 0 && reference(c) != s.raceRead {
			s.attributionProblemLocked("read_observed_before_episode_window", c.Name)
			return
		}
		if (s.armed && !s.held && !s.released || s.baseline && !s.armed) && s.readMiss(c, f) {
			s.observeMissLocked(c)
		}
	}
	if pub.Mode != "" {
		s.seenFill = true
		if s.baseline && !s.armed && s.episode != nil && s.episode.accepts(pub) && s.episode.Publication == reference(c) {
			s.episode.Published = true
			s.eventLocked("baseline_publication_completed", c.Name)
		}
		if s.armed && s.held && s.released && !s.applied && s.episode != nil && reference(c) == s.episode.Publication {
			s.applied = true
			if s.episode != nil {
				s.episode.Published = true
			}
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
	if s.episode != nil && !s.episode.Published {
		s.attributionProblemLocked("prior_publication_pending", "")
		return
	}
	s.episode = nil
	s.baseline = false
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
