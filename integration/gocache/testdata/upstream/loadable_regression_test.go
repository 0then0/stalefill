package cache

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/eko/gocache/lib/v4/store"
)

// This fake applies writes immediately but holds the first publication for one
// key inside Set, before its effect. All assertions use public cache operations.
type resurrectionCache struct {
	mu          sync.Mutex
	values      map[any]string
	heldKey     any
	entered     chan struct{}
	release     chan struct{}
	holdOnce    sync.Once
	releaseOnce sync.Once
}

func newResurrectionCache(heldKey any) *resurrectionCache {
	return &resurrectionCache{values: make(map[any]string), heldKey: heldKey,
		entered: make(chan struct{}), release: make(chan struct{})}
}

func (s *resurrectionCache) unblock() { s.releaseOnce.Do(func() { close(s.release) }) }
func (s *resurrectionCache) Get(_ context.Context, key any) (string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	v, ok := s.values[key]
	if !ok {
		return "", store.NotFound{}
	}
	return v, nil
}
func (s *resurrectionCache) Set(ctx context.Context, key any, value string, _ ...store.Option) error {
	if key == s.heldKey {
		var hold bool
		s.holdOnce.Do(func() { hold = true; close(s.entered) })
		if hold {
			select {
			case <-s.release:
			case <-ctx.Done():
				return ctx.Err()
			}
		}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.values[key] = value
	return nil
}
func (s *resurrectionCache) Delete(_ context.Context, key any) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.values, key)
	return nil
}
func (s *resurrectionCache) Invalidate(context.Context, ...store.InvalidateOption) error {
	return fmt.Errorf("not used by this test")
}
func (s *resurrectionCache) Clear(context.Context) error { return fmt.Errorf("not used by this test") }
func (s *resurrectionCache) GetType() string             { return "controlled" }

func resurrectionWait[T any](t *testing.T, ch <-chan T, label string) T {
	t.Helper()
	select {
	case result := <-ch:
		return result
	case <-time.After(5 * time.Second):
		t.Fatalf("timeout waiting for %s", label)
		var zero T
		return zero
	}
}

func resurrectionClose(t *testing.T, c *LoadableCache[string]) {
	t.Helper()
	done := make(chan error, 1)
	go func() { done <- c.Close() }()
	if err := resurrectionWait(t, done, "Close/drain"); err != nil {
		t.Fatal(err)
	}
}

// Original: Delete completes before release. A serialized fix: Delete blocks
// until release. Wait reaches quiescence, not a wall-clock scheduling guess.
// Thus this checks the same post-Delete invariant without requiring an early
// Delete from a fix, or using a timeout to choose the operation order.
func TestLoadableDeleteDoesNotResurrectInFlightPublication(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx := context.Background()
		s := newResurrectionCache("item")
		authoritative := "V1"
		c := NewLoadable[string](func(context.Context, any) (string, []store.Option, error) {
			return authoritative, nil, nil
		}, s)
		defer func() { s.unblock(); resurrectionClose(t, c) }()
		if got, err := c.Get(ctx, "item"); err != nil || got != "V1" {
			t.Fatalf("initial Get = %q, %v", got, err)
		}
		resurrectionWait(t, s.entered, "in-flight Set")
		authoritative = "V2"
		deleted := make(chan error, 1)
		go func() { deleted <- c.Delete(ctx, "item") }()
		synctest.Wait()
		select {
		case err := <-deleted:
			if err != nil {
				t.Fatal(err)
			}
			t.Log("Delete completed while old Set was held")
		default:
			t.Log("Delete waits for old Set; release before waiting for completion")
			s.unblock()
			if err := resurrectionWait(t, deleted, "Delete"); err != nil {
				t.Fatal(err)
			}
		}
		s.unblock()
		synctest.Wait() // publication and temporary-cache cleanup finish
		if got, err := c.Get(ctx, "item"); err != nil || got != "V2" {
			t.Fatalf("post-Delete Get = %q, %v; want authoritative V2", got, err)
		}
	})
}

// Holding another key keeps item's publication queued. This is a distinct
// reproduced risk: a mutex around only in-flight Set cannot cancel the queue.
func TestLoadableMutationDoesNotResurrectQueuedPublication(t *testing.T) {
	for _, mutation := range []string{"Delete", "Set"} {
		t.Run(mutation, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				ctx := context.Background()
				s := newResurrectionCache("blocker")
				authoritative := "V1"
				c := NewLoadable[string](func(context.Context, any) (string, []store.Option, error) {
					return authoritative, nil, nil
				}, s)
				defer func() { s.unblock(); resurrectionClose(t, c) }()
				if _, err := c.Get(ctx, "blocker"); err != nil {
					t.Fatal(err)
				}
				resurrectionWait(t, s.entered, "blocking Set for unrelated key")
				if got, err := c.Get(ctx, "item"); err != nil || got != "V1" {
					t.Fatalf("Get = %q, %v", got, err)
				}
				authoritative = "V2"
				done := make(chan error, 1)
				go func() {
					if mutation == "Delete" {
						done <- c.Delete(ctx, "item")
					} else {
						done <- c.Set(ctx, "item", "V2")
					}
				}()
				// Mutation of item must progress while blocker's Set is held.
				if err := resurrectionWait(t, done, mutation+" of unrelated key"); err != nil {
					t.Fatal(err)
				}
				s.unblock()
				resurrectionClose(t, c) // drains the old queued publication
				if got, err := c.Get(ctx, "item"); err != nil || got != "V2" {
					t.Fatalf("post-%s Get = %q, %v; want V2", mutation, got, err)
				}
			})
		})
	}
}

// A load can capture V1 before Delete and enqueue it afterwards. Keeping only
// a generation at enqueue time would miss this already-confirmed neighbor.
func TestLoadableDeleteDoesNotPublishOverlappingLoad(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx := context.Background()
		s := newResurrectionCache(nil)
		authoritative := "V1"
		loaded, release := make(chan struct{}), make(chan struct{})
		var once sync.Once
		c := NewLoadable[string](func(context.Context, any) (string, []store.Option, error) {
			value := authoritative
			once.Do(func() { close(loaded); <-release })
			return value, nil, nil
		}, s)
		var releaseOnce sync.Once
		unblock := func() { releaseOnce.Do(func() { close(release) }) }
		read := make(chan error, 1)
		defer func() { unblock(); resurrectionWait(t, read, "overlapping reader cleanup"); resurrectionClose(t, c) }()
		go func() { _, err := c.Get(ctx, "item"); read <- err }()
		resurrectionWait(t, loaded, "loader snapshot")
		authoritative = "V2"
		if err := c.Delete(ctx, "item"); err != nil {
			t.Fatal(err)
		}
		unblock()
		synctest.Wait()
		resurrectionClose(t, c)
		if got, err := c.Get(ctx, "item"); err != nil || got != "V2" {
			t.Fatalf("post-Delete Get = %q, %v; want V2", got, err)
		}
	})
}
