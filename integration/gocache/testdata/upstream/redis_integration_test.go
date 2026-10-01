package main

import (
	"context"
	"encoding/json"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/eko/gocache/lib/v4/cache"
	"github.com/eko/gocache/lib/v4/store"
	redisstore "github.com/eko/gocache/store/redis/v4"
	"github.com/redis/go-redis/v9"
)

// Only the first SET is gated, before socket I/O. All successful commands are
// executed by the pinned go-redis client against a disposable real Redis.
type publicationGate struct {
	entered, release chan struct{}
	once             sync.Once
	mu               sync.Mutex
	events           []string
}

func (h *publicationGate) DialHook(next redis.DialHook) redis.DialHook { return next }
func (h *publicationGate) ProcessPipelineHook(next redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return next
}
func (h *publicationGate) ProcessHook(next redis.ProcessHook) redis.ProcessHook {
	return func(ctx context.Context, cmd redis.Cmder) error {
		if cmd.Name() == "set" {
			h.once.Do(func() { close(h.entered); <-h.release })
		}
		err := next(ctx, cmd)
		if (cmd.Name() == "set" || cmd.Name() == "del") && err == nil {
			h.mu.Lock()
			h.events = append(h.events, cmd.Name()+"_completed")
			h.mu.Unlock()
		}
		return err
	}
}

func redisCheckWait[T any](t *testing.T, ch <-chan T, label string) T {
	t.Helper()
	select {
	case value := <-ch:
		return value
	case <-time.After(5 * time.Second):
		t.Fatalf("timeout waiting for %s", label)
		var zero T
		return zero
	}
}

// This is a library integration invariant, not a StaleFill verdict. The writer
// updates authoritative state and calls Delete; release is allowed while it is
// pending so serialized implementations can finish. There is no claim that the
// old StaleFill schedule completes, nor that Delete must return before Set.
func TestLoadableRedisPostDelete(t *testing.T) {
	addr := os.Getenv("REDIS_TEST_ADDR")
	if addr == "" {
		t.Skip("REDIS_TEST_ADDR must name a disposable local Redis")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	client := redis.NewClient(&redis.Options{Addr: addr, Protocol: 2,
		DialTimeout: 2 * time.Second, ReadTimeout: 2 * time.Second, WriteTimeout: 2 * time.Second})
	defer client.Close()
	const key = "stalefill:gocache:upstream-regression"
	if err := client.Del(ctx, key).Err(); err != nil {
		t.Fatal(err)
	}
	defer client.Del(context.Background(), key)
	hook := &publicationGate{entered: make(chan struct{}), release: make(chan struct{})}
	client.AddHook(hook)
	var authoritative atomic.Value
	authoritative.Store("V1")
	var loads atomic.Int32
	c := cache.NewLoadable[string](func(context.Context, any) (string, []store.Option, error) {
		loads.Add(1)
		return authoritative.Load().(string), nil, nil
	}, cache.New[string](redisstore.NewRedis(client)))
	var once sync.Once
	unblock := func() { once.Do(func() { close(hook.release) }) }
	writer := make(chan error, 1)
	var writerStarted bool
	closeCache := func() {
		done := make(chan error, 1)
		go func() { done <- c.Close() }()
		if err := redisCheckWait(t, done, "Close/drain"); err != nil {
			t.Fatal(err)
		}
	}
	defer func() {
		unblock()
		if writerStarted {
			if err := redisCheckWait(t, writer, "writer cleanup"); err != nil {
				t.Error(err)
			}
		}
		closeCache()
	}()
	if got, err := c.Get(ctx, key); err != nil || got != "V1" {
		t.Fatalf("reader = %q, %v", got, err)
	}
	redisCheckWait(t, hook.entered, "old background SET in flight")
	writeStarted := make(chan struct{})
	writerStarted = true
	go func() {
		authoritative.Store("V2")
		close(writeStarted)
		writer <- c.Delete(ctx, key)
	}()
	redisCheckWait(t, writeStarted, "authoritative update")
	if authoritative.Load().(string) != "V2" {
		t.Fatal("authoritative probe must observe V2")
	}
	unblock()
	deleteErr := redisCheckWait(t, writer, "Delete")
	writerStarted = false
	if deleteErr != nil {
		t.Fatal(deleteErr)
	}
	if got, err := c.Get(ctx, key); err != nil || got != "V2" {
		t.Fatalf("post-Delete reader = %q, %v; want V2", got, err)
	}
	closeCache()
	if got, err := client.Get(ctx, key).Result(); err != nil || got != "V2" {
		t.Fatalf("real Redis cache = %q, %v; want V2", got, err)
	}
	before := loads.Load()
	if got, err := c.Get(ctx, key); err != nil || got != "V2" {
		t.Fatalf("cache-backed reader = %q, %v", got, err)
	}
	if loads.Load() != before {
		t.Fatal("final Get must hit Redis without loading")
	}
	hook.mu.Lock()
	defer hook.mu.Unlock()
	if len(hook.events) != 3 || hook.events[0] != "set_completed" || hook.events[1] != "del_completed" || hook.events[2] != "set_completed" {
		t.Fatalf("unexpected Redis command order: %v", hook.events)
	}
	metadata, _ := json.Marshal(map[string]any{"commands": hook.events,
		"post_delete_value": "V2", "redis_value": "V2", "final_get_loaded": false,
		"dependency": "experimental local modified lib/v4.4.0", "stalefill_schedule": "not used"})
	t.Log(string(metadata))
}
