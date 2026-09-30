package proxy

import (
	"strings"

	"github.com/0then0/stalefill/internal/resp"
)

// Transaction is an immutable wire-order snapshot at EXEC or DISCARD. It is
// internal metadata, never a report payload. Accepted requires real OK/QUEUED
// replies, not merely a syntactically plausible command sequence.
type Transaction struct {
	Commands  []Command
	Truncated bool
	Accepted  bool
}

// Bound retained transaction frames as well as the ordinary request queue.
// Oversized programs still pass through, but cannot be scheduled safely.
const maxTransactionCommands = 256

type transactionTracker struct {
	active bool
	tx     Transaction
	bytes  int
}

func (t *transactionTracker) annotate(c *Command) {
	if c.Name == "MULTI" && !t.active {
		t.active = true
		t.tx = Transaction{}
		t.bytes = 0
		return
	}
	if !t.active {
		return
	}
	c.InTransaction = true
	if c.Name == "EXEC" || c.Name == "DISCARD" {
		snapshot := t.tx
		c.Transaction = &snapshot
		t.active = false
		t.tx = Transaction{}
		return
	}
	t.bytes += len(c.Frame.Raw)
	if len(t.tx.Commands) >= maxTransactionCommands || t.bytes > resp.MaxFrame {
		t.tx.Truncated = true
		t.tx.Commands = nil
	}
	if !t.tx.Truncated {
		t.tx.Commands = append(t.tx.Commands, *c)
	}
}

// Only the forwarding worker owns reply state. The reader can get ahead while
// pipelining, so its snapshots must not be changed by that worker.
type transactionReplies struct {
	active, failed bool
	queued         int
}

func (s *transactionReplies) before(c *Command) {
	// The upstream reply state, not the speculative reader, decides whether
	// this command can produce an applied mutation or a genuine read reply.
	c.InTransaction = s.active
	if c.Transaction != nil {
		tx := *c.Transaction
		tx.Accepted = s.active && !s.failed && !tx.Truncated && s.queued == len(tx.Commands)
		c.Transaction = &tx
	}
}

func (s *transactionReplies) after(c Command, f resp.Frame) {
	switch c.Name {
	case "MULTI":
		if !s.active && f.Kind == '+' && string(f.Data) == "OK" {
			s.active, s.failed, s.queued = true, false, 0
		} else if s.active {
			s.failed = true
		}
	case "EXEC", "DISCARD":
		// Arity errors leave Redis inside MULTI. EXECABORT clears it. Retain
		// failed state on other errors so later QUEUED writes are never treated
		// as standalone publications after a malformed boundary command.
		if !f.IsError() || c.Name == "EXEC" && strings.HasPrefix(string(f.Data), "EXECABORT") {
			*s = transactionReplies{}
		} else if s.active {
			s.failed = true
		}
	default:
		if s.active {
			if f.Kind == '+' && string(f.Data) == "QUEUED" {
				s.queued++
			} else {
				s.failed = true
			}
		}
	}
}
