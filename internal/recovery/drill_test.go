package recovery_test

import (
	"context"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/brian-bell/approach/internal/delivery"
	"github.com/brian-bell/approach/internal/recovery"
	"github.com/brian-bell/approach/internal/router"
	"github.com/brian-bell/approach/internal/store"
)

// discardLogger keeps router/recovery noise out of drill output while
// still exercising the logging paths.
func discardLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(discard{}, nil))
}

type discard struct{}

func (discard) Write(p []byte) (int, error) { return len(p), nil }

// waitFor polls until cond is true or the deadline passes — the drills
// compose the router, whose dispatch is asynchronous.
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

// TestDrillRateLimitStormAbsorbed is the §9 P1 drill for the §4.6
// retry rules under their design load: a provider rate-limit storm
// fails every thread's turn at once, twice, before clearing. The unit
// tests pin each transition; the drill composes the real router and
// the real recovery judgment into the daemon's turn shape and verifies
// the storm is ABSORBED — every event ends completed, on growing
// backoff, with zero dead letters, zero parks, and zero duplicate side
// effects — because a rate-limited turn provably did nothing (§6
// journal) and the budget (2) covers the storm.
func TestDrillRateLimitStormAbsorbed(t *testing.T) {
	db := openStore(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Per-event storm accounting, guarded: handlers run on per-thread
	// drain goroutines, concurrently across the three threads.
	var mu sync.Mutex
	dispatches := map[string]int{}
	backoffs := map[string][]time.Duration{}

	var q *router.Queues
	// The daemon's turn shape (§4.6): run the engine; on failure hand
	// the event to recovery with the router as the re-entry seam. The
	// fake engine is rate-limited on every thread's first two turns —
	// a storm, not a single flake.
	handler := func(ctx context.Context, ev store.QueuedEvent) {
		mu.Lock()
		dispatches[ev.DedupKey]++
		n := dispatches[ev.DedupKey]
		mu.Unlock()
		if n <= 2 {
			// Rate-limited: the turn died before any tool ran — the
			// journal stays empty, which is the §4.6 proof it is safe
			// to retry. After is synchronous so the drill owns time;
			// the requested delay is recorded as the retry rule's
			// observable output.
			out, err := recovery.HandleEngineFailure(ctx, db, q, ev, recovery.Options{
				Logger: discardLogger(),
				Now:    func() time.Time { return time.Unix(1700000100, 0) },
				After: func(d time.Duration, f func()) {
					mu.Lock()
					backoffs[ev.DedupKey] = append(backoffs[ev.DedupKey], d)
					mu.Unlock()
					f()
				},
			})
			if err != nil {
				t.Errorf("recovery for %s: %v", ev.DedupKey, err)
				return
			}
			if out != recovery.Retried {
				t.Errorf("outcome for %s attempt %d = %v, want Retried — a clean rate-limited turn is exactly what the budget is for", ev.DedupKey, n, out)
			}
			return
		}
		// The storm has cleared: the turn completes.
		if _, err := db.Exec(`UPDATE events SET status = 'completed' WHERE id = ?`, ev.ID); err != nil {
			t.Errorf("complete event: %v", err)
		}
	}
	q = router.New(ctx, db, router.Options{Handler: handler, Logger: discardLogger()})

	// Three threads hit the storm simultaneously.
	keys := make([]string, 0, 3)
	for _, thread := range []string{"discord:dm:1", "discord:dm:2", "discord:dm:3"} {
		ev := store.Event{
			DedupKey: "discord:msg:" + thread, ThreadKey: thread, Kind: "message", Trust: "owner",
			Payload:  `{"dedup_key":"discord:msg:` + thread + `","thread_key":"` + thread + `","kind":"message","trust":"owner"}`,
			Received: 1700000000,
		}
		if _, err := q.Persist(ctx, ev); err != nil {
			t.Fatalf("persist %s: %v", ev.DedupKey, err)
		}
		keys = append(keys, ev.DedupKey)
	}
	waitFor(t, func() bool {
		var done int
		if err := db.QueryRow(`SELECT count(*) FROM events WHERE status = 'completed'`).Scan(&done); err != nil {
			t.Fatalf("count completed: %v", err)
		}
		return done == len(keys)
	}, "every thread's turn to survive the storm")
	cancel()
	q.Wait()

	mu.Lock()
	defer mu.Unlock()
	for _, key := range keys {
		// Exactly three turns: two absorbed failures, one success — a
		// fourth would be a budget overrun, a second success a replay.
		if n := dispatches[key]; n != 3 {
			t.Errorf("%s dispatched %d times, want 3 (storm ×2 + success)", key, n)
		}
		// The schedule the §4.6 rules promise: growing backoff, so a
		// persistently limited provider is asked less often.
		want := []time.Duration{30 * time.Second, 2 * time.Minute}
		got := backoffs[key]
		if len(got) != len(want) {
			t.Fatalf("%s backoffs = %v, want %v", key, got, want)
		}
		for i := range want {
			if got[i] != want[i] {
				t.Errorf("%s backoff %d = %v, want %v — the schedule must grow through a storm", key, i+1, got[i], want[i])
			}
		}
		var status string
		var attempts int64
		if err := db.QueryRow(`SELECT status, attempts FROM events WHERE dedup_key = ?`, key).Scan(&status, &attempts); err != nil {
			t.Fatalf("read %s: %v", key, err)
		}
		if status != "completed" {
			t.Errorf("%s status = %q, want 'completed' — the storm must end absorbed, not escalated", key, status)
		}
		if attempts != 2 {
			t.Errorf("%s attempts = %d, want 2 — every retry spent exactly one budget unit", key, attempts)
		}
	}
	// Absorbed means QUIET: no dead letters, no parks, no §4.6 notices
	// for the owner to wake up to.
	var escalations int
	if err := db.QueryRow(
		`SELECT (SELECT count(*) FROM dead_letters) + (SELECT count(*) FROM deliveries) +
		        (SELECT count(*) FROM events WHERE status = 'interrupted')`,
	).Scan(&escalations); err != nil {
		t.Fatalf("count escalations: %v", err)
	}
	if escalations != 0 {
		t.Errorf("dead letters + notices + parks = %d after an in-budget storm, want 0 — absorption must not page a human", escalations)
	}
}

// TestDrillRateLimitStormOutboxAbsorbed is the outbound half of the
// same §9 P1 drill: the platform rate-limits the SEND leg. Every pump
// pass through the storm journals its attempt and leaves the row owed
// (§4.6 — never a silent drop, never a duplicate from a refusal);
// when the storm clears the message delivers exactly once and the ack
// advances the event to replied (§4.1).
func TestDrillRateLimitStormOutboxAbsorbed(t *testing.T) {
	db := openStore(t)
	ctx := context.Background()

	// A completed turn owing one reply — the write-before-send row.
	ev, _ := stageFailedTurn(t, db)
	if _, err := db.Exec(`UPDATE events SET status = 'completed' WHERE id = ?`, ev.ID); err != nil {
		t.Fatalf("complete event: %v", err)
	}
	id, inserted, err := store.InsertDelivery(ctx, db, store.Delivery{
		DeliveryKey: "reply:" + ev.DedupKey, EventID: ev.ID,
		Target: ev.ThreadKey, Payload: "the reply",
	})
	if err != nil || !inserted {
		t.Fatalf("compose reply: inserted=%v err=%v", inserted, err)
	}

	sender := &stormSender{refusals: 2}
	senders := map[string]delivery.Sender{"discord": sender}
	clock := func() time.Time { return time.Unix(1700000500, 0) }
	// Three pump passes: two inside the storm, one after it clears.
	for pass := 1; pass <= 3; pass++ {
		delivery.ResendUnacked(ctx, db, senders, discardLogger(), clock, nil)
	}

	if sender.delivered != 1 {
		t.Errorf("platform accepted %d sends, want 1 — refusals must not multiply the message", sender.delivered)
	}
	var status string
	var attempts int64
	var acked bool
	if err := db.QueryRow(
		`SELECT status, attempts, acked IS NOT NULL FROM deliveries WHERE id = ?`, id,
	).Scan(&status, &attempts, &acked); err != nil {
		t.Fatalf("read delivery: %v", err)
	}
	if status != "sent" || !acked {
		t.Errorf("delivery (status, acked) = (%q, %v), want ('sent', true) — the storm must end delivered", status, acked)
	}
	if attempts != 3 {
		t.Errorf("delivery attempts = %d, want 3 — every pass through the storm is journalled (§4.6)", attempts)
	}
	if got, _ := eventState(t, db, ev.ID); got != "replied" {
		t.Errorf("event status = %q, want 'replied' — the ack that ended the storm advances the event (§4.1)", got)
	}
}

// stormSender refuses its first N sends — a platform rate limit — then
// accepts. Single-goroutine use (ResendUnacked is a serial scan), so
// unguarded on purpose.
type stormSender struct {
	refusals  int
	delivered int
}

func (s *stormSender) Send(context.Context, string, string) (string, error) {
	if s.refusals > 0 {
		s.refusals--
		return "", errRateLimited
	}
	s.delivered++
	return "ack:1", nil
}

var errRateLimited = &rateLimitErr{}

type rateLimitErr struct{}

func (*rateLimitErr) Error() string { return "429: rate limited" }
