// Package proxy preserves per-connection command order. A held command stops
// only its connection; other connections retain independent upstream sockets.
package proxy

import (
	"bufio"
	"context"
	"errors"
	"io"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/0then0/stalefill/internal/resp"
)

type Command struct {
	Connection    uint64
	Name          string
	Args          [][]byte
	Frame         resp.Frame
	InTransaction bool
	Transaction   *Transaction
}
type Hooks interface {
	Queued(Command)
	Before(context.Context, Command) error
	After(Command, resp.Frame)
	Error(string)
}

type Server struct {
	listener       net.Listener
	upstream       string
	hooks          Hooks
	ctx            context.Context
	cancel         context.CancelFunc
	wg             sync.WaitGroup
	mu             sync.Mutex
	conns          map[net.Conn]struct{}
	nextConnection atomic.Uint64
}

func Start(ctx context.Context, listen, upstream string, hooks Hooks) (*Server, error) {
	l, e := net.Listen("tcp", listen)
	if e != nil {
		return nil, e
	}
	ctx, cancel := context.WithCancel(ctx)
	s := &Server{listener: l, upstream: upstream, hooks: hooks, ctx: ctx, cancel: cancel, conns: make(map[net.Conn]struct{})}
	s.wg.Add(1)
	go s.accept()
	go func() { <-ctx.Done(); s.closeSockets() }()
	return s, nil
}
func (s *Server) Addr() string { return s.listener.Addr().String() }
func (s *Server) closeSockets() {
	s.listener.Close()
	s.mu.Lock()
	defer s.mu.Unlock()
	for c := range s.conns {
		c.Close()
	}
}
func (s *Server) Close()            { s.cancel(); s.closeSockets(); s.wg.Wait() }
func (s *Server) track(c net.Conn)  { s.mu.Lock(); s.conns[c] = struct{}{}; s.mu.Unlock() }
func (s *Server) forget(c net.Conn) { c.Close(); s.mu.Lock(); delete(s.conns, c); s.mu.Unlock() }
func (s *Server) accept() {
	defer s.wg.Done()
	for {
		c, e := s.listener.Accept()
		if e != nil {
			return
		}
		s.track(c)
		s.wg.Add(1)
		go s.serve(c)
	}
}

// Unsolicited replies need a different response multiplexer; refuse them rather
// than associating pushes or pub/sub messages with the next command response.
func unsupported(c Command) bool {
	switch c.Name {
	case "SUBSCRIBE", "PSUBSCRIBE", "SSUBSCRIBE", "MONITOR":
		return true
	}
	if c.Name != "CLIENT" || len(c.Args) == 0 {
		return false
	}
	if strings.EqualFold(string(c.Args[0]), "TRACKING") {
		return true
	}
	// OFF suppresses its own reply; SKIP suppresses the following reply. Either
	// breaks the worker's one-response-per-command contract. ON remains safe.
	return strings.EqualFold(string(c.Args[0]), "REPLY") && len(c.Args) > 1 &&
		(strings.EqualFold(string(c.Args[1]), "OFF") || strings.EqualFold(string(c.Args[1]), "SKIP"))
}
func (s *Server) serve(client net.Conn) {
	defer s.wg.Done()
	defer s.forget(client)
	ctx, cancel := context.WithCancel(s.ctx)
	defer cancel()
	up, e := (&net.Dialer{Timeout: 3 * time.Second}).DialContext(ctx, "tcp", s.upstream)
	if e != nil {
		s.hooks.Error("upstream connection failed")
		return
	}
	s.track(up)
	defer s.forget(up)
	// Cancellation closes blocked reads and writes, including a client that
	// disconnects while a SET is held.
	stopped := make(chan struct{})
	defer close(stopped)
	go func() {
		select {
		case <-ctx.Done():
			client.Close()
			up.Close()
		case <-stopped:
		}
	}()
	id := s.nextConnection.Add(1)
	queue := make(chan Command, 16)
	readerDone := make(chan struct{})
	go func() {
		defer close(readerDone)
		defer close(queue)
		r := bufio.NewReader(client)
		var tracker transactionTracker
		for {
			f, err := resp.Read(r)
			if err != nil {
				if errors.Is(err, io.EOF) {
					return
				}
				if ctx.Err() == nil {
					s.hooks.Error("client protocol or connection error")
				}
				cancel()
				return
			}
			name, args, err := f.Command()
			if err != nil {
				s.hooks.Error("invalid command frame")
				cancel()
				return
			}
			c := Command{Name: name, Args: args, Frame: f, Connection: id}
			tracker.annotate(&c)
			s.hooks.Queued(c)
			select {
			case queue <- c:
			case <-ctx.Done():
				return
			}
		}
	}()
	defer func() { cancel(); client.Close(); <-readerDone }()
	r := bufio.NewReader(up)
	var state transactionReplies
	for {
		var c Command
		select {
		case <-ctx.Done():
			return
		case v, ok := <-queue:
			if !ok {
				return
			}
			c = v
		}
		if unsupported(c) {
			s.hooks.Error("unsupported Redis response mode")
			if _, e = client.Write([]byte("-ERR StaleFill does not support this Redis response mode\r\n")); e != nil {
				return
			}
			continue
		}
		state.before(&c)
		if e = s.hooks.Before(ctx, c); e != nil {
			return
		}
		if _, e = up.Write(c.Frame.Raw); e != nil {
			if ctx.Err() == nil {
				s.hooks.Error("upstream write failed")
			}
			return
		}
		f, err := resp.Read(r)
		if err != nil {
			if ctx.Err() == nil {
				s.hooks.Error("upstream protocol or connection error")
			}
			return
		}
		// RESP3 attributes decorate the next reply; forward both as one response.
		for f.Kind == '|' {
			if _, e = client.Write(f.Raw); e != nil {
				return
			}
			f, err = resp.Read(r)
			if err != nil {
				s.hooks.Error("upstream protocol error")
				return
			}
		}
		if f.Kind == '>' {
			s.hooks.Error("unsupported unsolicited RESP3 push")
			return
		}
		state.after(c, f)
		s.hooks.After(c, f)
		if _, e = client.Write(f.Raw); e != nil {
			return
		}
	}
}
