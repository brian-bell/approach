package recovery_test

import (
	"context"
	"database/sql"
	"log/slog"
	"path/filepath"
	"testing"
	"time"

	"github.com/brian-bell/approach/internal/recovery"
	"github.com/brian-bell/approach/internal/store"
)

// fakeEnqueuer records re-enqueued events.
type fakeEnqueuer struct{ enqueued []store.QueuedEvent }

func (f *fakeEnqueuer) Enqueue(ev store.QueuedEvent) { f.enqueued = append(f.enqueued, ev) }

// syncAfter runs the callback immediately and records the requested
// delay — tests own the clock, no real timers (§6 convention).
type syncAfter struct{ delays []time.Duration }

func (s *syncAfter) after(d time.Duration, f func()) {
	s.delays = append(s.delays, d)
	f()
}

func openStore(t *testing.T) *sql.DB {
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

// stageFailedTurn stands up the full mid-turn shape an engine failure
// leaves behind: a session on the thread, an event stamped
// 'processing'. Returns the queued-event view dispatch would hold.
func stageFailedTurn(t *testing.T, db *sql.DB) (store.QueuedEvent, string) {
	t.Helper()
	ctx := context.Background()
	sessionID := "11111111-1111-4111-8111-111111111111"
	if _, err := store.InsertSession(ctx, db, store.Session{
		ThreadKey: "discord:dm:123", SessionID: sessionID, Cwd: t.TempDir(),
		TrustFloor: "owner", CreatedAt: 1700000000, ActivationDeadline: 1700000600,
	}); err != nil {
		t.Fatalf("InsertSession: %v", err)
	}
	ev := store.Event{
		DedupKey: "discord:msg:1", ThreadKey: "discord:dm:123", Kind: "message", Trust: "owner",
		Payload:  `{"dedup_key":"discord:msg:1","thread_key":"discord:dm:123","kind":"message","trust":"owner"}`,
		Received: 1700000000,
	}
	id, _, err := store.InsertEvent(ctx, db, ev)
	if err != nil {
		t.Fatalf("InsertEvent: %v", err)
	}
	if err := store.MarkEventProcessing(ctx, db, id, 1700000050); err != nil {
		t.Fatalf("MarkEventProcessing: %v", err)
	}
	return store.QueuedEvent{
		ID: id, DedupKey: ev.DedupKey, ThreadKey: ev.ThreadKey, Kind: ev.Kind,
		Trust: ev.Trust, Payload: ev.Payload, Status: "processing", Received: ev.Received,
	}, sessionID
}

func journalAttempt(t *testing.T, db *sql.DB, sessionID string, eventID int64, state, idemKey string) {
	t.Helper()
	id, err := store.InsertToolAttempt(context.Background(), db, store.ToolAttempt{
		SessionID: sessionID, EventID: eventID, Tool: "mcp__discord__send",
		ArgsDigest: "sha256:aa", IdempotencyKey: idemKey, StartedAt: 1700000060,
	})
	if err != nil {
		t.Fatalf("InsertToolAttempt: %v", err)
	}
	if state != "started" {
		if err := store.CompleteToolAttempt(context.Background(), db, id, state, 1700000070); err != nil {
			t.Fatalf("CompleteToolAttempt: %v", err)
		}
	}
}

func eventState(t *testing.T, db *sql.DB, id int64) (status string, attempts int64) {
	t.Helper()
	if err := db.QueryRow(`SELECT status, attempts FROM events WHERE id = ?`, id).Scan(&status, &attempts); err != nil {
		t.Fatalf("read event: %v", err)
	}
	return status, attempts
}

func handle(t *testing.T, db *sql.DB, ev store.QueuedEvent, enq *fakeEnqueuer, after *syncAfter) recovery.Outcome {
	out, notified := handleN(t, db, ev, enq, after)
	_ = notified
	return out
}

func handleN(t *testing.T, db *sql.DB, ev store.QueuedEvent, enq *fakeEnqueuer, after *syncAfter) (recovery.Outcome, int) {
	t.Helper()
	notified := 0
	out, err := recovery.HandleEngineFailure(context.Background(), db, enq, ev, recovery.Options{
		Logger: slog.Default(),
		Now:    func() time.Time { return time.Unix(1700000100, 0) },
		After:  after.after,
		Notify: func() { notified++ },
	})
	if err != nil {
		t.Fatalf("HandleEngineFailure: %v", err)
	}
	return out, notified
}

// TestHandleEngineFailureRetriesCleanTurn: zero journalled attempts is
// the §4.6 proof the turn did nothing — the event requeues durably
// (received, budget spent) and re-enters its thread's queue after the
// first backoff step.
func TestHandleEngineFailureRetriesCleanTurn(t *testing.T) {
	db := openStore(t)
	ev, _ := stageFailedTurn(t, db)
	enq := &fakeEnqueuer{}
	after := &syncAfter{}

	if out := handle(t, db, ev, enq, after); out != recovery.Retried {
		t.Errorf("outcome = %v, want Retried", out)
	}
	status, attempts := eventState(t, db, ev.ID)
	if status != "received" || attempts != 1 {
		t.Errorf("(status, attempts) = (%q, %d), want ('received', 1)", status, attempts)
	}
	if len(enq.enqueued) != 1 || enq.enqueued[0].ID != ev.ID {
		t.Fatalf("enqueued = %+v, want the event exactly once", enq.enqueued)
	}
	if enq.enqueued[0].Status != "received" {
		t.Errorf("re-enqueued status = %q, want 'received' — dispatch stamps processing from there", enq.enqueued[0].Status)
	}
	if len(after.delays) != 1 || after.delays[0] != 30*time.Second {
		t.Errorf("backoff = %v, want [30s] for the first retry", after.delays)
	}
}

// TestHandleEngineFailureSecondRetryBacksOffLonger: the second spend
// of the budget waits the longer step — backoff must grow, §4.6.
func TestHandleEngineFailureSecondRetryBacksOffLonger(t *testing.T) {
	db := openStore(t)
	ev, _ := stageFailedTurn(t, db)
	if _, err := db.Exec(`UPDATE events SET attempts = 1 WHERE id = ?`, ev.ID); err != nil {
		t.Fatalf("spend one budget unit: %v", err)
	}
	enq := &fakeEnqueuer{}
	after := &syncAfter{}

	if out := handle(t, db, ev, enq, after); out != recovery.Retried {
		t.Errorf("outcome = %v, want Retried", out)
	}
	if _, attempts := eventState(t, db, ev.ID); attempts != 2 {
		t.Errorf("attempts = %d, want 2", attempts)
	}
	if len(after.delays) != 1 || after.delays[0] != 2*time.Minute {
		t.Errorf("backoff = %v, want [2m] for the second retry", after.delays)
	}
}

// TestHandleEngineFailureParksAmbiguousTurn: ANY journalled attempt
// without an idempotency_key makes the turn ambiguous — §4.6 counts
// even completed attempts (a done side effect would replay), and a
// started one may have lost its PostToolUse to the crash. Parked,
// never re-enqueued.
func TestHandleEngineFailureParksAmbiguousTurn(t *testing.T) {
	for _, state := range []string{"started", "done", "failed"} {
		t.Run(state, func(t *testing.T) {
			db := openStore(t)
			ev, sessionID := stageFailedTurn(t, db)
			journalAttempt(t, db, sessionID, ev.ID, state, "")
			enq := &fakeEnqueuer{}
			after := &syncAfter{}

			out, notified := handleN(t, db, ev, enq, after)
			if out != recovery.Parked {
				t.Errorf("outcome = %v, want Parked", out)
			}
			if notified != 1 {
				t.Errorf("Notify fired %d times, want 1 — the pump must wake for the notice NOW, not on the ticker", notified)
			}
			status, attempts := eventState(t, db, ev.ID)
			if status != "interrupted" || attempts != 0 {
				t.Errorf("(status, attempts) = (%q, %d), want ('interrupted', 0) — ambiguity spends no budget, it ends the automation", status, attempts)
			}
			if len(enq.enqueued) != 0 {
				t.Errorf("enqueued = %+v, want none", enq.enqueued)
			}
			assertSurfaced(t, db, ev)
		})
	}
}

// assertSurfaced checks the §4.6 park notice landed in the outbox,
// aimed at the originating thread.
func assertSurfaced(t *testing.T, db *sql.DB, ev store.QueuedEvent) {
	t.Helper()
	var target string
	if err := db.QueryRow(
		`SELECT target FROM deliveries WHERE delivery_key LIKE ?`, "interrupted:"+ev.DedupKey+":%",
	).Scan(&target); err != nil {
		t.Fatalf("§4.6 surface row missing for %s: %v — a park no one hears about is a silent drop", ev.DedupKey, err)
	}
	if target != ev.ThreadKey {
		t.Errorf("surface target = %q, want the originating thread %q", target, ev.ThreadKey)
	}
}

// TestHandleEngineFailureRetriesFullyKeyedTurn: when EVERY attempt
// carries an idempotency_key, a repeat is provably safe (§4.6) — the
// one exception to ambiguity.
func TestHandleEngineFailureRetriesFullyKeyedTurn(t *testing.T) {
	db := openStore(t)
	ev, sessionID := stageFailedTurn(t, db)
	journalAttempt(t, db, sessionID, ev.ID, "started", "send:msg:42")
	journalAttempt(t, db, sessionID, ev.ID, "done", "send:msg:43")
	enq := &fakeEnqueuer{}
	after := &syncAfter{}

	if out := handle(t, db, ev, enq, after); out != recovery.Retried {
		t.Errorf("outcome = %v, want Retried — every attempt is keyed", out)
	}
	if len(enq.enqueued) != 1 {
		t.Errorf("enqueued %d events, want 1", len(enq.enqueued))
	}
}

// TestHandleEngineFailureParksMixedKeyTurn: one keyed attempt does not
// launder an unkeyed sibling — safety is ALL attempts keyed, or none
// exist.
func TestHandleEngineFailureParksMixedKeyTurn(t *testing.T) {
	db := openStore(t)
	ev, sessionID := stageFailedTurn(t, db)
	journalAttempt(t, db, sessionID, ev.ID, "done", "send:msg:42")
	journalAttempt(t, db, sessionID, ev.ID, "started", "")
	enq := &fakeEnqueuer{}
	after := &syncAfter{}

	if out := handle(t, db, ev, enq, after); out != recovery.Parked {
		t.Errorf("outcome = %v, want Parked", out)
	}
	if len(enq.enqueued) != 0 {
		t.Errorf("enqueued = %+v, want none", enq.enqueued)
	}
}

// TestHandleEngineFailureParksExhaustedBudget: a clean turn whose
// budget is spent parks — the machine has given up and a human
// decides (§4.6; the dead_letters landing takes this branch over in
// x6n.3.6).
func TestHandleEngineFailureParksExhaustedBudget(t *testing.T) {
	db := openStore(t)
	ev, _ := stageFailedTurn(t, db)
	if _, err := db.Exec(`UPDATE events SET attempts = 2 WHERE id = ?`, ev.ID); err != nil {
		t.Fatalf("spend budget: %v", err)
	}
	enq := &fakeEnqueuer{}
	after := &syncAfter{}

	if out := handle(t, db, ev, enq, after); out != recovery.Parked {
		t.Errorf("outcome = %v, want Parked", out)
	}
	status, _ := eventState(t, db, ev.ID)
	if status != "interrupted" {
		t.Errorf("status = %q, want 'interrupted'", status)
	}
	if len(enq.enqueued) != 0 {
		t.Errorf("enqueued = %+v, want none", enq.enqueued)
	}
}

// TestHandleEngineFailureUnreadableJournalIsAnError: a journal that
// cannot be read IS ambiguity — the handler must not retry on missing
// evidence, and the error surfaces to the caller instead of a quiet
// verdict.
func TestHandleEngineFailureUnreadableJournalIsAnError(t *testing.T) {
	db := openStore(t)
	ev, _ := stageFailedTurn(t, db)
	if err := db.Close(); err != nil {
		t.Fatalf("close db: %v", err)
	}
	enq := &fakeEnqueuer{}
	after := &syncAfter{}

	_, err := recovery.HandleEngineFailure(context.Background(), db, enq, ev, recovery.Options{
		Logger: slog.Default(),
		Now:    func() time.Time { return time.Unix(1700000100, 0) },
		After:  after.after,
	})
	if err == nil {
		t.Fatal("HandleEngineFailure returned a verdict from an unreadable journal, want error")
	}
	if len(enq.enqueued) != 0 {
		t.Errorf("enqueued = %+v, want none — never retry on missing evidence", enq.enqueued)
	}
}
