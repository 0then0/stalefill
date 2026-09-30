// Package demo provides two cache protocols over an authoritative in-memory
// store. It deliberately contains no sleeps, proxy hooks, or race orchestration.
package demo

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"net"
	"net/http"
	"sync"
	"time"

	"github.com/0then0/stalefill/internal/resp"
)

type Value struct {
	Version int  `json:"version"`
	Exists  bool `json:"exists"`
}
type App struct {
	mu    sync.Mutex
	value Value
	Redis string
	Fixed bool
}

func New(redis string, fixed bool) *App {
	return &App{Redis: redis, Fixed: fixed, value: Value{Version: 1, Exists: true}}
}
func (a *App) snapshot() Value { a.mu.Lock(); defer a.mu.Unlock(); return a.value }
func (a *App) command(ctx context.Context, args ...string) (resp.Frame, error) {
	c, e := (&net.Dialer{Timeout: time.Second}).DialContext(ctx, "tcp", a.Redis)
	if e != nil {
		return resp.Frame{}, e
	}
	defer c.Close()
	stop := context.AfterFunc(ctx, func() { c.Close() })
	defer stop()
	if deadline, ok := ctx.Deadline(); ok {
		c.SetDeadline(deadline)
	} else {
		c.SetDeadline(time.Now().Add(30 * time.Second))
	}
	if _, e = c.Write(resp.Encode(args...)); e != nil {
		return resp.Frame{}, e
	}
	f, e := resp.Read(bufio.NewReader(c))
	if e == nil && f.IsError() {
		e = errors.New("Redis command failed")
	}
	return f, e
}
func (a *App) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	if r.URL.Path == "/authoritative/42" && r.Method == http.MethodGet {
		json.NewEncoder(w).Encode(a.snapshot())
		return
	}
	if r.URL.Path != "/items/42" {
		http.NotFound(w, r)
		return
	}
	switch r.Method {
	case http.MethodPut:
		var v struct {
			Version int   `json:"version"`
			Exists  *bool `json:"exists"`
		}
		d := json.NewDecoder(http.MaxBytesReader(w, r.Body, 4096))
		d.DisallowUnknownFields()
		if d.Decode(&v) != nil || v.Version < 1 {
			http.Error(w, "bad input", 400)
			return
		}
		exists := true
		if v.Exists != nil {
			exists = *v.Exists
		}
		a.mu.Lock()
		a.value = Value{Version: v.Version, Exists: exists}
		a.mu.Unlock()
		if _, e := a.command(r.Context(), "DEL", "product:42"); e != nil {
			http.Error(w, "cache error", 502)
			return
		}
		json.NewEncoder(w).Encode(a.snapshot())
	case http.MethodGet:
		f, e := a.command(r.Context(), "GET", "product:42")
		if e != nil {
			http.Error(w, "cache error", 502)
			return
		}
		var v Value
		hit := !f.Null
		if hit && json.Unmarshal(f.Data, &v) != nil {
			http.Error(w, "cache format error", 502)
			return
		}
		// Fixed mode validates every cached generation against the authoritative
		// snapshot. This demonstrates one mitigation, not a universal best pattern.
		if hit && a.Fixed && v != a.snapshot() {
			hit = false
		}
		if !hit {
			v = a.snapshot()
			b, _ := json.Marshal(v)
			if _, e = a.command(r.Context(), "SET", "product:42", string(b), "EX", "60"); e != nil {
				http.Error(w, "cache error", 502)
				return
			}
		}
		json.NewEncoder(w).Encode(v)
	default:
		w.WriteHeader(http.StatusMethodNotAllowed)
	}
}
