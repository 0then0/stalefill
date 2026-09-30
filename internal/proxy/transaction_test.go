package proxy

import (
	"testing"

	"github.com/0then0/stalefill/internal/resp"
)

func TestTransactionTrackingAndReplies(t *testing.T) {
	var a, b transactionTracker
	var replies transactionReplies
	annotate := func(tr *transactionTracker, name string) Command {
		c := Command{Name: name, Frame: resp.Frame{Raw: resp.Encode(name)}}
		tr.annotate(&c)
		return c
	}
	multi := annotate(&a, "MULTI")
	replies.after(multi, resp.Frame{Kind: '+', Data: []byte("OK")})
	hset := annotate(&a, "HSET")
	if !hset.InTransaction || annotate(&b, "DEL").InTransaction {
		t.Fatal("transaction state shared across connections")
	}
	replies.after(hset, resp.Frame{Kind: '+', Data: []byte("QUEUED")})
	exec := annotate(&a, "EXEC")
	replies.before(&exec)
	if exec.Transaction == nil || !exec.Transaction.Accepted || len(exec.Transaction.Commands) != 1 {
		t.Fatal("missing accepted EXEC snapshot")
	}
	replies.after(exec, resp.Frame{Kind: '*'})
	if annotate(&a, "DEL").InTransaction || replies.active {
		t.Fatal("EXEC did not clear state")
	}
	annotate(&a, "MULTI")
	annotate(&a, "HSET")
	if annotate(&a, "DISCARD").Transaction == nil || annotate(&a, "HSET").InTransaction {
		t.Fatal("DISCARD did not clear state")
	}
}

func TestTransactionBoundsAndRejectedQueue(t *testing.T) {
	var tracker transactionTracker
	c := Command{Name: "MULTI"}
	tracker.annotate(&c)
	for i := 0; i <= maxTransactionCommands; i++ {
		c = Command{Name: "HSET"}
		tracker.annotate(&c)
	}
	c = Command{Name: "EXEC"}
	tracker.annotate(&c)
	if c.Transaction == nil || !c.Transaction.Truncated || len(c.Transaction.Commands) != 0 {
		t.Fatal("unbounded transaction retention")
	}
	var replies transactionReplies
	replies.after(Command{Name: "MULTI"}, resp.Frame{Kind: '+', Data: []byte("OK")})
	replies.after(Command{Name: "HSET"}, resp.Frame{Kind: '-', Data: []byte("ERR")})
	c = Command{Name: "EXEC", Transaction: &Transaction{Commands: []Command{{Name: "HSET"}}}}
	replies.before(&c)
	if c.Transaction.Accepted {
		t.Fatal("queue error accepted as publication")
	}
}

func TestMalformedBoundaryRetainsUpstreamState(t *testing.T) {
	for _, name := range []string{"EXEC", "DISCARD"} {
		var state transactionReplies
		state.after(Command{Name: "MULTI"}, resp.Frame{Kind: '+', Data: []byte("OK")})
		state.after(Command{Name: name}, resp.Frame{Kind: '-', Data: []byte("ERR wrong number of arguments")})
		write := Command{Name: "HSET"}
		state.before(&write)
		if !write.InTransaction || !state.failed {
			t.Fatal("malformed boundary turned QUEUED write into standalone publication")
		}
		state.after(Command{Name: "EXEC"}, resp.Frame{Kind: '-', Data: []byte("EXECABORT transaction discarded")})
		state.before(&write)
		if write.InTransaction {
			t.Fatal("EXECABORT did not clear state")
		}
	}
}
