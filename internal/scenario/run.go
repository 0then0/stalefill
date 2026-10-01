package scenario

import (
	"bufio"
	"context"
	"errors"
	"net"
	"runtime"
	"strings"
	"time"

	"github.com/0then0/stalefill/internal/proxy"
	"github.com/0then0/stalefill/internal/resp"
)

const Version = "0.3.0"
const (
	Pass                = "PASS"
	Fail                = "FAIL"
	Unresolved          = "UNRESOLVED"
	InfrastructureError = "INFRASTRUCTURE_ERROR"
)

type Finding struct {
	ID      string `json:"id"`
	Message string `json:"message"`
}
type Report struct {
	Version     string            `json:"tool_version"`
	Scenario    string            `json:"scenario"`
	Outcome     string            `json:"outcome"`
	FillMode    string            `json:"fill_mode,omitempty"`
	KeyHash     string            `json:"key_sha256,omitempty"`
	Events      []Event           `json:"events"`
	Findings    []Finding         `json:"findings"`
	DurationMS  int64             `json:"duration_ms"`
	Environment map[string]string `json:"environment"`
}

func InitialReport(c Config) Report {
	name := c.Scenario.Type
	if name != "stale_fill_after_invalidation" && name != "negative_cache_resurrection" {
		name = "unknown"
	}
	return Report{Version: Version, Scenario: name, Outcome: InfrastructureError, Events: []Event{}, Findings: []Finding{}, Environment: map[string]string{"os": runtime.GOOS, "arch": runtime.GOARCH, "go": runtime.Version(), "protocol": "RESP2 / non-streaming RESP3"}}
}
func (r *Report) Set(outcome, id string) {
	r.Outcome = outcome
	if id != "" {
		r.Findings = append(r.Findings, Finding{ID: id, Message: findingMessage(id)})
	}
}
func ping(ctx context.Context, addr string) error {
	c, e := (&net.Dialer{}).DialContext(ctx, "tcp", addr)
	if e != nil {
		return errors.New("Redis unreachable")
	}
	defer c.Close()
	stop := context.AfterFunc(ctx, func() { c.Close() })
	defer stop()
	if _, e = c.Write(resp.Encode("PING")); e != nil {
		return errors.New("Redis PING write failed")
	}
	f, e := resp.Read(bufio.NewReader(c))
	if e == nil && f.Kind == '-' && strings.HasPrefix(string(f.Data), "NOAUTH") {
		return nil
	}
	if e != nil || f.Kind != '+' || string(f.Data) != "PONG" {
		return errors.New("Redis PING failed")
	}
	return nil
}

// Run starts the proxy before issuing application requests. Doctor runs only
// the configured sequential baseline; it never arms the command barrier.
func Run(parent context.Context, c Config, doctor bool) (report Report) {
	started := time.Now()
	report = InitialReport(c)
	if c.Validate() != nil {
		report.Set(InfrastructureError, "SF100")
		return
	}
	s := NewScheduler(c.Scenario.Key, c.Scenario.KeyRegex)
	defer func() {
		report.Events, _, _ = s.Snapshot()
		report.KeyHash = s.KeyHash()
		report.FillMode = s.FillMode()
		report.DurationMS = time.Since(started).Milliseconds()
	}()
	ctx, cancel := context.WithTimeout(parent, duration(c))
	defer cancel()
	server, e := proxy.Start(ctx, c.Redis.Listen, c.Redis.Upstream, s)
	if e != nil {
		s.Event("proxy_listen_failed")
		report.Set(InfrastructureError, "SF100")
		return
	}
	defer func() { cancel(); server.Close(); s.ReleaseOnCleanup() }()
	if e = ping(ctx, c.Redis.Upstream); e != nil {
		s.Event("redis_health_check_failed")
		report.Set(InfrastructureError, "SF100")
		return
	}
	s.Event("baseline_started")
	for _, step := range []struct {
		name string
		p    Probe
	}{
		{"prepare", c.Prepare}, {"read", c.Read}, {"write", c.Write}, {"authoritative", c.Authoritative}, {"verify", c.Verify},
	} {
		if e = baselineProbe(ctx, step.p); e != nil {
			s.Event("baseline_" + step.name + "_failed")
			report.Set(InfrastructureError, "SF100")
			return
		}
		if step.name == "read" || step.name == "verify" {
			if step.p.CachePublication == "none" {
				e = s.CompleteBaselineWithoutPublication()
			} else {
				e = s.WaitBaselinePublication(ctx)
			}
			if e != nil {
				s.Event("baseline_publication_not_joined")
				classifySchedule(parent, &report, s, "SF005")
				return
			}
		}
	}
	_, problem, infra := s.Snapshot()
	if infra != "" {
		report.Set(InfrastructureError, infra)
		return
	}
	if problem != "" {
		report.Set(Unresolved, problem)
		return
	}
	if !s.BaselineTraffic() {
		s.Event("baseline_target_traffic_missing")
		report.Set(Unresolved, "SF004")
		return
	}
	s.Event("baseline_completed")
	if doctor {
		report.Set(Pass, "")
		return
	}
	if e = baselineProbe(ctx, c.Prepare); e != nil {
		s.Event("prepare_failed")
		report.Set(InfrastructureError, "SF100")
		return
	}
	s.Event("prepared")
	s.Arm()
	readDone := make(chan probeResult, 1)
	readerFinished := make(chan struct{})
	go func() {
		defer close(readerFinished)
		result := probe(ctx, c.Read)
		s.ObserveReader(ctx, result)
		readDone <- result
	}()
	defer func() { cancel(); <-readerFinished }()
	old, e := waitForPublication(ctx, s, readDone)
	if e != nil {
		if parent.Err() != nil {
			report.Set(InfrastructureError, "SF100")
		} else if old != nil && (old.err != nil || old.statusMismatch) && ctx.Err() == nil {
			classifyProbe(&report, s)
		} else if old != nil && old.mismatch && ctx.Err() == nil {
			classifySchedule(parent, &report, s, "SF009")
		} else {
			classifySchedule(parent, &report, s, "SF005")
		}
		return
	}
	s.Event("write_started")
	w := scheduleProbe(ctx, s, c.Write)
	if w.err != nil || w.statusMismatch || w.mismatch {
		s.Event("write_probe_failed")
		classifyProbe(&report, s)
		return
	}
	s.Event("write_completed")
	if e = s.WaitInvalidation(ctx); e != nil {
		classifySchedule(parent, &report, s, "SF003")
		return
	}
	// Evidence of authoritative state is obtained while the old fill is held.
	a := probe(ctx, c.Authoritative)
	if a.err != nil || a.statusMismatch || a.mismatch {
		s.Event("authoritative_probe_failed")
		report.Set(InfrastructureError, "SF100")
		return
	}
	s.Event("authoritative_confirmed")
	if e = s.Release(); e != nil {
		classifySchedule(parent, &report, s, "SF005")
		return
	}
	if e = s.WaitApplied(ctx); e != nil {
		classifySchedule(parent, &report, s, "SF005")
		return
	}
	if old == nil {
		select {
		case result := <-readDone:
			old = &result
		case <-ctx.Done():
			report.Set(InfrastructureError, "SF100")
			return
		}
		// A protected read may refresh its response after the delayed fill.
		// The baseline defines the old value; final verification is the oracle.
		if old.err != nil || (old.statusMismatch && old.status != c.Verify.Status) {
			report.Set(InfrastructureError, "SF100")
			return
		}
	}
	s.Event("verify_started")
	v := probe(ctx, c.Verify)
	if v.err != nil || (v.statusMismatch && v.status != c.Read.Status) {
		s.Event("verification_probe_failed")
		report.Set(InfrastructureError, "SF100")
		return
	}
	_, problem, infra = s.Snapshot()
	if infra != "" {
		report.Set(InfrastructureError, infra)
		return
	}
	if problem != "" {
		report.Set(Unresolved, problem)
		return
	}
	if !v.statusMismatch && !v.mismatch {
		s.Event("verification_passed")
		report.Set(Pass, "")
		return
	}
	if v.status == c.Read.Status && equalJSON(v.value, c.Read.Assert.Equals) {
		s.Event("verification_stale")
		id := "SF001"
		if c.Scenario.Type == "negative_cache_resurrection" {
			id = "SF002"
		}
		report.Set(Fail, id)
	} else {
		s.Event("verification_inconclusive")
		report.Set(Unresolved, "SF007")
	}
	return
}

// waitForPublication joins two independent lifetimes. Only a successful old
// HTTP observation permits continued waiting after early reader completion.
// The invocation context bounds association; the deadline is never renewed.
func waitForPublication(ctx context.Context, s *Scheduler, readDone <-chan probeResult) (old *probeResult, err error) {
	waitCtx, stopWait := context.WithCancel(ctx)
	defer stopWait()
	heldDone := make(chan error, 1)
	go func() { heldDone <- s.WaitHeld(waitCtx) }()
	save := func(result probeResult) error {
		old = &result
		if result.err != nil || result.statusMismatch || result.mismatch {
			return errors.New("early reader observation failed")
		}
		return nil
	}
	readResults := readDone
	for {
		select {
		case result := <-readResults:
			readResults = nil
			if err = save(result); err != nil {
				stopWait()
				<-heldDone
				return
			}
		case err = <-heldDone:
			if err != nil {
				return
			}
			// Preserve an already completed reader regardless of select
			// arbitration when both observations are ready.
			select {
			case result := <-readResults:
				err = save(result)
			default:
			}
			return
		}
	}
}

func classifySchedule(parent context.Context, r *Report, s *Scheduler, fallback string) {
	if parent.Err() != nil {
		r.Set(InfrastructureError, "SF100")
		return
	}
	_, p, i := s.Snapshot()
	if i != "" {
		r.Set(InfrastructureError, i)
	} else if p != "" {
		r.Set(Unresolved, p)
	} else {
		r.Set(Unresolved, fallback)
	}
}
func classifyProbe(r *Report, s *Scheduler) {
	_, p, i := s.Snapshot()
	if p != "" && i == "" {
		r.Set(Unresolved, p)
	} else {
		r.Set(InfrastructureError, "SF100")
	}
}
func (s *Scheduler) ReleaseOnCleanup() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.released {
		s.released = true
		close(s.release)
	}
}

// Wake HTTP waits when a pipelined invalidation is trapped behind the held
// publication on the same connection. Otherwise the probe masks SF006 until
// the invocation timeout even though the unsupported schedule is known.
func scheduleProbe(ctx context.Context, s *Scheduler, p Probe) probeResult {
	probeCtx, cancel := context.WithCancel(ctx)
	done := make(chan struct{})
	go func() {
		defer close(done)
		s.wait(probeCtx, func() bool { return false })
		cancel()
	}()
	result := probe(probeCtx, p)
	cancel()
	<-done
	return result
}
