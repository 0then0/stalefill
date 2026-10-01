package scenario

import (
	"context"
	"errors"

	"github.com/0then0/stalefill/internal/proxy"
)

// MissEpisode outlives the HTTP operation that observed the old value. It
// associates one confirmed target miss with one supported publication boundary.
// This is bounded wire evidence within an isolated fixture, not an application
// request ID or proof of causality for arbitrary background workers.
type MissEpisode struct {
	ID              uint64
	Key             string
	ReadMode        string
	MissConnection  uint64
	Candidates      int
	Publication     commandRef
	Published       bool
	ReaderCompleted bool
}

type commandRef struct{ connection, sequence uint64 }

func reference(c proxy.Command) commandRef { return commandRef{c.Connection, c.Sequence} }

func readMode(c proxy.Command) string {
	if c.Name == "GET" || c.Name == "MGET" {
		return "string"
	}
	return "hash"
}

func (e *MissEpisode) accepts(pub FillPublication) bool {
	mode := pub.Mode
	if mode == "transactional_hash" {
		mode = "hash"
	}
	return e.Key == string(pub.Key) && e.ReadMode == mode
}

func (s *Scheduler) observeMissLocked(c proxy.Command) {
	if s.problem != "" || s.infra != "" {
		return
	}
	if s.episode != nil && (!s.episode.Published || s.armed) {
		s.attributionProblemLocked("competing_target_miss", c.Name)
		return
	}
	s.nextEpisode++
	s.episode = &MissEpisode{ID: s.nextEpisode, Key: s.selected, ReadMode: readMode(c), MissConnection: c.Connection, ReaderCompleted: s.readerCompleted}
	if s.episode.Key == "" {
		s.episode.Key = s.key
	}
	if s.armed {
		s.miss = true
		s.phase = PhaseMiss
		s.eventLocked("cache_miss_observed", c.Name)
	}
}

func (s *Scheduler) candidateLocked(c proxy.Command, pub FillPublication) {
	e := s.episode
	if e == nil || e.Published || !e.accepts(pub) {
		s.attributionProblemLocked("publication_without_matching_episode", c.Name)
		return
	}
	e.Candidates++
	if e.Candidates > 1 {
		s.attributionProblemLocked("multiple_publication_candidates", c.Name)
		return
	}
	e.Publication = reference(c)
	if c.Sequence != 0 {
		s.candidates[reference(c)] = e
	}
}

func (s *Scheduler) attributionProblemLocked(reason, command string) {
	if s.problem == "" {
		s.problem = "SF009"
		s.phase = Phase(Unresolved)
		s.eventLocked(reason, command)
	}
}

// WaitBaselinePublication joins observed miss/publication lifetimes before the
// next baseline mutation or prepare. No quiet-period sleep can establish this.
func (s *Scheduler) WaitBaselinePublication(ctx context.Context) error {
	return s.wait(ctx, func() bool {
		if s.episode != nil && !s.episode.Published {
			return false
		}
		s.episode = nil
		return true
	})
}

// CompleteBaselineWithoutPublication uses the fixture's explicit no-fill
// contract. HTTP completion alone cannot establish this for detached fills.
func (s *Scheduler) CompleteBaselineWithoutPublication() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.episode != nil && s.episode.Candidates != 0 {
		s.attributionProblemLocked("unexpected_baseline_publication", "")
	}
	if s.problem != "" || s.infra != "" {
		return errors.New("baseline publication contract failed")
	}
	s.episode = nil
	s.eventLocked("baseline_no_publication", "")
	return nil
}
