package proxy

import (
	"context"
	"net"
	"testing"

	"github.com/0then0/stalefill/internal/resp"
)

type connectionErrorHooks struct{ errors []string }

func (*connectionErrorHooks) Queued(Command)                        {}
func (*connectionErrorHooks) Before(context.Context, Command) error { return nil }
func (*connectionErrorHooks) After(Command, resp.Frame)             {}
func (h *connectionErrorHooks) Error(message string)                { h.errors = append(h.errors, message) }

func TestUpstreamDialCancellationDoesNotReportInfrastructureError(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	address := listener.Addr().String()
	listener.Close()
	for _, canceled := range []bool{false, true} {
		t.Run(map[bool]string{false: "connection refused", true: "canceled cleanup"}[canceled], func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			if canceled {
				cancel()
			}
			hooks := &connectionErrorHooks{}
			s := &Server{ctx: ctx, upstream: address, hooks: hooks, conns: make(map[net.Conn]struct{})}
			client, peer := net.Pipe()
			defer peer.Close()
			s.wg.Add(1)
			s.serve(client)
			if canceled {
				if len(hooks.errors) != 0 {
					t.Fatalf("cleanup cancellation added infrastructure errors: %v", hooks.errors)
				}
			} else if len(hooks.errors) != 1 || hooks.errors[0] != "upstream connection failed" {
				t.Fatalf("genuine connection failure must remain visible: %v", hooks.errors)
			}
		})
	}
}
