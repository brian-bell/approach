package router_test

import (
	"context"
	"database/sql"
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

// mustOpen opens a store in a temp dir, closed via cleanup.
func mustOpen(t *testing.T) *sql.DB {
	t.Helper()
	db, err := store.Open(filepath.Join(t.TempDir(), "state", "approach.db"))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() {
		if err := db.Close(); err != nil {
			t.Errorf("close: %v", err)
		}
	})
	return db
}

// qe builds a minimal queued event for direct-Enqueue dispatch tests.
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

// storeEvent builds a valid insertable event (payload mirrors columns,
// as InsertEvent demands) for tests that go through the real table.
func storeEvent(n int64, threadKey string) store.Event {
	return store.Event{
		DedupKey:  fmt.Sprintf("discord:msg:%d", n),
		ThreadKey: threadKey,
		Kind:      "message",
		Trust:     "owner",
		Payload: fmt.Sprintf(
			`{"dedup_key":"discord:msg:%d","thread_key":"%s","kind":"message","trust":"owner"}`, n, threadKey),
		Received: 1700000000 + n,
	}
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

	db := mustOpen(t)
	var inFlight, maxInFlight atomic.Int64
	var mu sync.Mutex
	var got []string
	q := router.New(ctx, db, router.Options{
		Handler: func(_ context.Context, ev store.QueuedEvent) {
			n := inFlight.Add(1)
			if m := maxInFlight.Load(); n > m {
				maxInFlight.Store(n)
			}
			time.Sleep(time.Millisecond) // widen any overlap window
			mu.Lock()
			got = append(got, ev.DedupKey)
			mu.Unlock()
			inFlight.Add(-1)
		},
		Logger: discardLogger(),
	})

	const n = 20
	for i := int64(1); i <= n; i++ {
		if _, err := q.Persist(ctx, storeEvent(i, "discord:dm:a")); err != nil {
			t.Fatalf("Persist %d: %v", i, err)
		}
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

	db := mustOpen(t)
	bDone := make(chan struct{})
	q := router.New(ctx, db, router.Options{
		Handler: func(_ context.Context, ev store.QueuedEvent) {
			switch ev.ThreadKey {
			case "discord:dm:a":
				// Thread a parks until thread b's event completes: if
				// dispatch were global-serial this would deadlock the
				// test (and trip its timeout).
				select {
				case <-bDone:
				case <-time.After(5 * time.Second):
					t.Error("thread a blocked thread b — dispatch is not per-thread")
				}
			case "discord:dm:b":
				close(bDone)
			}
		},
		Logger: discardLogger(),
	})

	if _, err := q.Persist(ctx, storeEvent(1, "discord:dm:a")); err != nil {
		t.Fatalf("Persist: %v", err)
	}
	if _, err := q.Persist(ctx, storeEvent(2, "discord:dm:b")); err != nil {
		t.Fatalf("Persist: %v", err)
	}
	q.Wait()
}

// TestRebuildDispatchesUnprocessed: the in-memory queues are only an
// index over unprocessed event rows, rebuilt from the table on restart
// (§4.1) — completed history must not re-dispatch, and per-thread order
// is receipt (id) order.
func TestRebuildDispatchesUnprocessed(t *testing.T) {
	db := mustOpen(t)
	bg := context.Background()

	for i, tk := range []string{"discord:dm:a", "discord:dm:a", "discord:dm:b", "discord:dm:c"} {
		if _, _, err := store.InsertEvent(bg, db, storeEvent(int64(i+1), tk)); err != nil {
			t.Fatalf("InsertEvent: %v", err)
		}
	}
	if _, err := db.Exec(`UPDATE events SET status = 'completed' WHERE dedup_key = 'discord:msg:1'`); err != nil {
		t.Fatalf("advance msg 1: %v", err)
	}
	// msg 4 was mid-turn at the crash: §4.6 says interrupted, never rerun.
	if _, err := db.Exec(`UPDATE events SET status = 'processing' WHERE dedup_key = 'discord:msg:4'`); err != nil {
		t.Fatalf("mark msg 4 processing: %v", err)
	}

	ctx, cancel := context.WithCancel(bg)
	defer cancel()
	var mu sync.Mutex
	var got []string
	var calls atomic.Int64
	q := router.New(ctx, db, router.Options{
		Handler: func(_ context.Context, ev store.QueuedEvent) {
			mu.Lock()
			got = append(got, ev.DedupKey)
			mu.Unlock()
			calls.Add(1)
		},
		Logger: discardLogger(),
	})
	if err := q.Rebuild(bg); err != nil {
		t.Fatalf("Rebuild: %v", err)
	}
	waitFor(t, func() bool { return calls.Load() == 2 }, "rebuild dispatch")
	q.Wait()

	mu.Lock()
	defer mu.Unlock()
	if len(got) != 2 {
		t.Fatalf("dispatched %v, want exactly msgs 2 and 3", got)
	}
	for _, k := range got {
		if k == "discord:msg:1" {
			t.Fatalf("completed event re-dispatched after restart: %v", got)
		}
		if k == "discord:msg:4" {
			t.Fatalf("crash-interrupted processing event re-ran on restart — §4.6 forbids side-effect replay: %v", got)
		}
	}
	var status string
	if err := db.QueryRow(`SELECT status FROM events WHERE dedup_key = 'discord:msg:4'`).Scan(&status); err != nil {
		t.Fatalf("read back msg 4: %v", err)
	}
	if status != "interrupted" {
		t.Errorf("crash-interrupted event status = %q after Rebuild, want interrupted (§4.6)", status)
	}
}

// TestRebuildParksBeforeAnyDispatch: crash recovery completes before
// any new turn starts — a handler running while Rebuild is still
// parking crash-interrupted rows would interleave new work with
// recovery's rewrites, and a park FAILURE mid-scan would return a
// startup refusal while that handler races the store teardown. The
// received row here sorts BEFORE the processing row by id, so a
// single-pass rebuild would dispatch it first; the two-phase rebuild
// must have parked the later row before this handler observes it.
func TestRebuildParksBeforeAnyDispatch(t *testing.T) {
	db := mustOpen(t)
	bg := context.Background()

	for i, tk := range []string{"discord:dm:a", "discord:dm:b"} {
		if _, _, err := store.InsertEvent(bg, db, storeEvent(int64(i+1), tk)); err != nil {
			t.Fatalf("InsertEvent: %v", err)
		}
	}
	// id 2 (the LATER row) was mid-turn at the crash.
	if _, err := db.Exec(`UPDATE events SET status = 'processing' WHERE dedup_key = 'discord:msg:2'`); err != nil {
		t.Fatalf("mark msg 2 processing: %v", err)
	}

	ctx, cancel := context.WithCancel(bg)
	defer cancel()
	var crashRowStatus string
	var calls atomic.Int64
	q := router.New(ctx, db, router.Options{
		Handler: func(_ context.Context, ev store.QueuedEvent) {
			if err := db.QueryRow(`SELECT status FROM events WHERE dedup_key = 'discord:msg:2'`).Scan(&crashRowStatus); err != nil {
				t.Errorf("read crash row mid-turn: %v", err)
			}
			calls.Add(1)
		},
		Logger: discardLogger(),
	})
	if err := q.Rebuild(bg); err != nil {
		t.Fatalf("Rebuild: %v", err)
	}
	waitFor(t, func() bool { return calls.Load() == 1 }, "dispatch")
	q.Wait()

	if crashRowStatus != "interrupted" {
		t.Errorf("a turn ran while a crash-interrupted row was still %q — recovery must complete before any dispatch", crashRowStatus)
	}
}

// TestEnqueueDuringProcessing: an event arriving while its thread is
// mid-turn appends behind the in-flight one — never dropped, never
// reordered.
func TestEnqueueDuringProcessing(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	db := mustOpen(t)
	started := make(chan struct{})
	release := make(chan struct{})
	var mu sync.Mutex
	var got []string
	q := router.New(ctx, db, router.Options{
		Handler: func(_ context.Context, ev store.QueuedEvent) {
			if ev.ID == 1 {
				close(started)
				<-release
			}
			mu.Lock()
			got = append(got, ev.DedupKey)
			mu.Unlock()
		},
		Logger: discardLogger(),
	})

	if _, err := q.Persist(ctx, storeEvent(1, "discord:dm:a")); err != nil {
		t.Fatalf("Persist: %v", err)
	}
	<-started // thread a is mid-turn
	if _, err := q.Persist(ctx, storeEvent(2, "discord:dm:a")); err != nil {
		t.Fatalf("Persist: %v", err)
	}
	close(release)
	q.Wait()

	mu.Lock()
	defer mu.Unlock()
	if len(got) != 2 || got[0] != "discord:msg:1" || got[1] != "discord:msg:2" {
		t.Fatalf("dispatch order %v, want [discord:msg:1 discord:msg:2]", got)
	}
}

// TestPersistOrdersConcurrentIngest: receipt order IS dispatch order,
// even when ingests race on one thread (§4.1). Without the per-thread
// persist+enqueue lock, a goroutine descheduled between InsertEvent and
// Enqueue lets a younger row jump the queue — Rebuild would never
// produce that order, and the live daemon must not either.
func TestPersistOrdersConcurrentIngest(t *testing.T) {
	db := mustOpen(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var mu sync.Mutex
	var got []int64
	var calls atomic.Int64
	q := router.New(ctx, db, router.Options{
		Handler: func(_ context.Context, ev store.QueuedEvent) {
			mu.Lock()
			got = append(got, ev.ID)
			mu.Unlock()
			calls.Add(1)
		},
		Logger: discardLogger(),
	})

	const n = 30
	var wg sync.WaitGroup
	for i := int64(1); i <= n; i++ {
		wg.Add(1)
		go func(i int64) {
			defer wg.Done()
			inserted, err := q.Persist(ctx, storeEvent(i, "discord:dm:a"))
			if err != nil {
				t.Errorf("Persist %d: %v", i, err)
			}
			if !inserted {
				t.Errorf("Persist %d reported inserted=false on a fresh event", i)
			}
		}(i)
	}
	wg.Wait()
	waitFor(t, func() bool { return calls.Load() == n }, "all events dispatched")
	q.Wait()

	mu.Lock()
	defer mu.Unlock()
	for i := 1; i < len(got); i++ {
		if got[i] < got[i-1] {
			t.Fatalf("dispatch order by row id %v is not receipt order — id %d ran before %d", got, got[i], got[i-1])
		}
	}
}

// TestPersistDuplicateNotEnqueued: a collapsed duplicate must not
// double-dispatch (§4.1: duplicate delivery → one turn) — the original
// row is the only claimable copy.
func TestPersistDuplicateNotEnqueued(t *testing.T) {
	db := mustOpen(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var calls atomic.Int64
	q := router.New(ctx, db, router.Options{
		Handler: func(_ context.Context, ev store.QueuedEvent) { calls.Add(1) },
		Logger:  discardLogger(),
	})

	if inserted, err := q.Persist(ctx, storeEvent(1, "discord:dm:a")); err != nil || !inserted {
		t.Fatalf("first Persist: inserted=%v err=%v", inserted, err)
	}
	if inserted, err := q.Persist(ctx, storeEvent(1, "discord:dm:a")); err != nil || inserted {
		t.Fatalf("duplicate Persist: inserted=%v err=%v, want false, nil", inserted, err)
	}
	q.Wait()
	if calls.Load() != 1 {
		t.Errorf("dispatched %d turns for a duplicated delivery, want 1", calls.Load())
	}
}

// TestHandlerPanicParksEventAndContinues: a panicking turn must end in
// a durable, visible state — the event parks as interrupted (§4.6),
// because its side effects are unknowable and it is no longer in the
// RAM index — and the thread's next event still runs: one bad turn
// never silences a thread.
func TestHandlerPanicParksEventAndContinues(t *testing.T) {
	db := mustOpen(t)
	bg := context.Background()
	for i := int64(1); i <= 2; i++ {
		if _, _, err := store.InsertEvent(bg, db, storeEvent(i, "discord:dm:a")); err != nil {
			t.Fatalf("InsertEvent: %v", err)
		}
	}

	ctx, cancel := context.WithCancel(bg)
	defer cancel()
	var handled atomic.Int64
	q := router.New(ctx, db, router.Options{
		Handler: func(_ context.Context, ev store.QueuedEvent) {
			if ev.DedupKey == "discord:msg:1" {
				panic("engine exploded")
			}
			handled.Add(1)
		},
		Logger: discardLogger(),
		Now:    func() time.Time { return time.Unix(1700009999, 0) },
	})
	if err := q.Rebuild(bg); err != nil {
		t.Fatalf("Rebuild: %v", err)
	}
	q.Wait()

	if handled.Load() != 1 {
		t.Errorf("event after a panicking turn was not dispatched — thread queue wedged")
	}
	var status string
	var updated sql.NullInt64
	if err := db.QueryRow(`SELECT status, updated FROM events WHERE dedup_key = 'discord:msg:1'`).Scan(&status, &updated); err != nil {
		t.Fatalf("read back panicked event: %v", err)
	}
	if status != "interrupted" {
		t.Errorf("panicked turn's event status = %q, want interrupted (§4.6 — else it strands until restart)", status)
	}
	if !updated.Valid || updated.Int64 != 1700009999 {
		t.Errorf("parked event updated = %+v, want the injected clock's 1700009999", updated)
	}
}

// TestDispatchStampsProcessingBeforeHandler: the §4.1/§4.6 crash
// window — the durable row must read 'processing' BEFORE the handler
// runs, so a daemon that dies mid-turn parks the event on restart
// instead of replaying a half-finished turn's side effects.
func TestDispatchStampsProcessingBeforeHandler(t *testing.T) {
	db := mustOpen(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var statusDuringTurn string
	var calls atomic.Int64
	q := router.New(ctx, db, router.Options{
		Handler: func(_ context.Context, ev store.QueuedEvent) {
			// What would restart recovery see if we crashed RIGHT NOW?
			if err := db.QueryRow(`SELECT status FROM events WHERE id = ?`, ev.ID).Scan(&statusDuringTurn); err != nil {
				t.Errorf("read status mid-turn: %v", err)
			}
			calls.Add(1)
		},
		Logger: discardLogger(),
	})
	if _, err := q.Persist(ctx, storeEvent(1, "discord:dm:a")); err != nil {
		t.Fatalf("Persist: %v", err)
	}
	waitFor(t, func() bool { return calls.Load() == 1 }, "dispatch")
	q.Wait()

	if statusDuringTurn != "processing" {
		t.Errorf("row status during the turn = %q, want processing — a crash here would REPLAY the turn on restart (§4.6)", statusDuringTurn)
	}

	// The simulated crash: this handler never completed the row, so a
	// fresh router's Rebuild must park it, never re-dispatch it.
	ctx2, cancel2 := context.WithCancel(context.Background())
	defer cancel2()
	var replays atomic.Int64
	q2 := router.New(ctx2, db, router.Options{
		Handler: func(context.Context, store.QueuedEvent) { replays.Add(1) },
		Logger:  discardLogger(),
	})
	if err := q2.Rebuild(context.Background()); err != nil {
		t.Fatalf("Rebuild: %v", err)
	}
	q2.Wait()
	if replays.Load() != 0 {
		t.Errorf("crash-interrupted turn re-dispatched %d times on restart, want 0", replays.Load())
	}
	var status string
	if err := db.QueryRow(`SELECT status FROM events WHERE dedup_key = 'discord:msg:1'`).Scan(&status); err != nil {
		t.Fatalf("read back: %v", err)
	}
	if status != "interrupted" {
		t.Errorf("status after restart = %q, want interrupted", status)
	}
}

// TestCancelStopsDispatch: after shutdown begins, queued-but-unstarted
// events stay in the table for the next restart's Rebuild — the router
// must not start new turns against a store that is about to close.
func TestCancelStopsDispatch(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())

	db := mustOpen(t)
	started := make(chan struct{})
	release := make(chan struct{})
	var handled atomic.Int64
	q := router.New(ctx, db, router.Options{
		Handler: func(_ context.Context, ev store.QueuedEvent) {
			if ev.ID == 1 {
				close(started)
				<-release
			}
			handled.Add(1)
		},
		Logger: discardLogger(),
	})

	if _, err := q.Persist(ctx, storeEvent(1, "discord:dm:a")); err != nil {
		t.Fatalf("Persist: %v", err)
	}
	<-started
	if _, err := q.Persist(ctx, storeEvent(2, "discord:dm:a")); err != nil {
		t.Fatalf("Persist: %v", err)
	}
	cancel() // drain begins while event 1 is mid-turn
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
