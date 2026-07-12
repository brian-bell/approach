package router_test

import (
	"context"
	"fmt"
	"log/slog"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/brian-bell/approach/internal/router"
	"github.com/brian-bell/approach/internal/store"
)

// discardLogger keeps router noise out of test output while still
// exercising the logging paths.
func discardLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(discard{}, nil))
}

type discard struct{}

func (discard) Write(p []byte) (int, error) { return len(p), nil }

// qe builds a minimal queued event for dispatch tests.
func qe(id int64, threadKey string) store.QueuedEvent {
	return store.QueuedEvent{
		ID:        id,
		DedupKey:  fmt.Sprintf("discord:msg:%d", id),
		ThreadKey: threadKey,
		Kind:      "message",
		Trust:     "owner",
		Payload:   "{}",
		Status:    "received",
		Received:  1700000000,
	}
}

// collect returns a handler that appends each event's dedup key to a
// shared slice and signals on done, plus accessors for the recording.
func collect() (handler func(context.Context, store.QueuedEvent), keys func() []string, calls *atomic.Int64) {
	var mu sync.Mutex
	var got []string
	calls = new(atomic.Int64)
	handler = func(_ context.Context, ev store.QueuedEvent) {
		mu.Lock()
		got = append(got, ev.DedupKey)
		mu.Unlock()
		calls.Add(1)
	}
	keys = func() []string {
		mu.Lock()
		defer mu.Unlock()
		return append([]string(nil), got...)
	}
	return handler, keys, calls
}

// waitFor polls until cond is true or the deadline passes.
func waitFor(t *testing.T, cond func() bool, what string) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

// TestFIFOWithinThread: events on one thread run strictly in enqueue
// order, one at a time — the per-thread serialization §4.1 builds on.
func TestFIFOWithinThread(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var inFlight, maxInFlight atomic.Int64
	var mu sync.Mutex
	var got []string
	q := router.New(ctx, func(_ context.Context, ev store.QueuedEvent) {
		n := inFlight.Add(1)
		if m := maxInFlight.Load(); n > m {
			maxInFlight.Store(n)
		}
		time.Sleep(time.Millisecond) // widen any overlap window
		mu.Lock()
		got = append(got, ev.DedupKey)
		mu.Unlock()
		inFlight.Add(-1)
	}, discardLogger())

	const n = 20
	for i := int64(1); i <= n; i++ {
		q.Enqueue(qe(i, "discord:dm:a"))
	}
	q.Wait()

	if maxInFlight.Load() != 1 {
		t.Errorf("max concurrent handlers on one thread = %d, want 1 (§4.1 serialization)", maxInFlight.Load())
	}
	if int64(len(got)) != n {
		t.Fatalf("handled %d events, want %d", len(got), n)
	}
	for i := int64(1); i <= n; i++ {
		if got[i-1] != fmt.Sprintf("discord:msg:%d", i) {
			t.Fatalf("dispatch order %v is not FIFO", got)
		}
	}
}

// TestThreadsRunConcurrently: one thread's slow turn must never block
// another thread — queues are per-thread, not global (§4.1).
func TestThreadsRunConcurrently(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	bDone := make(chan struct{})
	q := router.New(ctx, func(_ context.Context, ev store.QueuedEvent) {
		switch ev.ThreadKey {
		case "discord:dm:a":
			// Thread a parks until thread b's event completes: if
			// dispatch were global-serial this would deadlock the test
			// (and trip its timeout).
			select {
			case <-bDone:
			case <-time.After(5 * time.Second):
				t.Error("thread a blocked thread b — dispatch is not per-thread")
			}
		case "discord:dm:b":
			close(bDone)
		}
	}, discardLogger())

	q.Enqueue(qe(1, "discord:dm:a"))
	q.Enqueue(qe(2, "discord:dm:b"))
	q.Wait()
}

// TestRebuildDispatchesUnprocessed: the in-memory queues are only an
// index over unprocessed event rows, rebuilt from the table on restart
// (§4.1) — completed history must not re-dispatch, and per-thread order
// is receipt (id) order.
func TestRebuildDispatchesUnprocessed(t *testing.T) {
	db, err := store.Open(filepath.Join(t.TempDir(), "state", "approach.db"))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() {
		if err := db.Close(); err != nil {
			t.Errorf("close: %v", err)
		}
	})
	bg := context.Background()

	for i, tk := range []string{"discord:dm:a", "discord:dm:a", "discord:dm:b"} {
		ev := store.Event{
			DedupKey:  fmt.Sprintf("discord:msg:%d", i+1),
			ThreadKey: tk,
			Kind:      "message",
			Trust:     "owner",
			Payload: fmt.Sprintf(
				`{"dedup_key":"discord:msg:%d","thread_key":"%s","kind":"message","trust":"owner"}`, i+1, tk),
			Received: int64(1700000000 + i),
		}
		if _, err := store.InsertEvent(bg, db, ev); err != nil {
			t.Fatalf("InsertEvent: %v", err)
		}
	}
	if _, err := db.Exec(`UPDATE events SET status = 'completed' WHERE dedup_key = 'discord:msg:1'`); err != nil {
		t.Fatalf("advance msg 1: %v", err)
	}

	ctx, cancel := context.WithCancel(bg)
	defer cancel()
	handler, keys, calls := collect()
	q := router.New(ctx, handler, discardLogger())
	if err := q.Rebuild(bg, db); err != nil {
		t.Fatalf("Rebuild: %v", err)
	}
	waitFor(t, func() bool { return calls.Load() == 2 }, "rebuild dispatch")
	q.Wait()

	got := keys()
	if len(got) != 2 {
		t.Fatalf("dispatched %v, want exactly msgs 2 and 3", got)
	}
	for _, k := range got {
		if k == "discord:msg:1" {
			t.Fatalf("completed event re-dispatched after restart: %v", got)
		}
	}
}

// TestEnqueueDuringProcessing: an event arriving while its thread is
// mid-turn appends behind the in-flight one — never dropped, never
// reordered.
func TestEnqueueDuringProcessing(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	started := make(chan struct{})
	release := make(chan struct{})
	var mu sync.Mutex
	var got []string
	q := router.New(ctx, func(_ context.Context, ev store.QueuedEvent) {
		if ev.ID == 1 {
			close(started)
			<-release
		}
		mu.Lock()
		got = append(got, ev.DedupKey)
		mu.Unlock()
	}, discardLogger())

	q.Enqueue(qe(1, "discord:dm:a"))
	<-started // thread a is mid-turn
	q.Enqueue(qe(2, "discord:dm:a"))
	close(release)
	q.Wait()

	mu.Lock()
	defer mu.Unlock()
	if len(got) != 2 || got[0] != "discord:msg:1" || got[1] != "discord:msg:2" {
		t.Fatalf("dispatch order %v, want [discord:msg:1 discord:msg:2]", got)
	}
}

// TestHandlerPanicDoesNotWedgeThread: a panicking turn is a loud log,
// not a wedged queue — the next event on the thread still runs (§4.6:
// every failure ends in a durable, visible state; a silently dead
// thread queue is the opposite).
func TestHandlerPanicDoesNotWedgeThread(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var handled atomic.Int64
	q := router.New(ctx, func(_ context.Context, ev store.QueuedEvent) {
		if ev.ID == 1 {
			panic("engine exploded")
		}
		handled.Add(1)
	}, discardLogger())

	q.Enqueue(qe(1, "discord:dm:a"))
	q.Enqueue(qe(2, "discord:dm:a"))
	q.Wait()

	if handled.Load() != 1 {
		t.Errorf("event after a panicking turn was not dispatched — thread queue wedged")
	}
}

// TestCancelStopsDispatch: after shutdown begins, queued-but-unstarted
// events stay in the table for the next restart's Rebuild — the router
// must not start new turns against a store that is about to close.
func TestCancelStopsDispatch(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())

	started := make(chan struct{})
	release := make(chan struct{})
	var handled atomic.Int64
	q := router.New(ctx, func(_ context.Context, ev store.QueuedEvent) {
		if ev.ID == 1 {
			close(started)
			<-release
		}
		handled.Add(1)
	}, discardLogger())

	q.Enqueue(qe(1, "discord:dm:a"))
	<-started
	q.Enqueue(qe(2, "discord:dm:a")) // queued behind the in-flight turn
	cancel()                         // drain begins while event 1 is mid-turn
	close(release)
	q.Wait()

	if handled.Load() != 1 {
		t.Errorf("handled %d events after cancel, want 1 (in-flight finishes, queued does not start)", handled.Load())
	}

	// Enqueue after shutdown must not dispatch either.
	q.Enqueue(qe(3, "discord:dm:b"))
	time.Sleep(10 * time.Millisecond)
	if handled.Load() != 1 {
		t.Errorf("event enqueued after cancel was dispatched")
	}
}
