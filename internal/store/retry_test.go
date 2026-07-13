package store_test

import (
	"context"
	"database/sql"
	"errors"
	"path/filepath"
	"testing"

	"github.com/brian-bell/approach/internal/store"
)

// insertProcessingEvent stages a row mid-turn: inserted, then stamped
// 'processing' the way dispatch does — the state an engine failure
// finds it in.
func insertProcessingEvent(t *testing.T, db *sql.DB) int64 {
	t.Helper()
	id, _, err := store.InsertEvent(context.Background(), db, testEvent())
	if err != nil {
		t.Fatalf("InsertEvent: %v", err)
	}
	if err := store.MarkEventProcessing(context.Background(), db, id, 1700000050); err != nil {
		t.Fatalf("MarkEventProcessing: %v", err)
	}
	return id
}

func eventRetryState(t *testing.T, db *sql.DB, id int64) (status string, attempts int64) {
	t.Helper()
	if err := db.QueryRow(
		`SELECT status, attempts FROM events WHERE id = ?`, id,
	).Scan(&status, &attempts); err != nil {
		t.Fatalf("read event %d: %v", id, err)
	}
	return status, attempts
}

// TestRequeueEventForRetry: the §4.6 auto-retry transition — a failed
// turn's event goes back to 'received' (durably owed a turn again)
// with the budget spent recorded, twice at most.
func TestRequeueEventForRetry(t *testing.T) {
	db := mustOpen(t, filepath.Join(t.TempDir(), "state", "approach.db"))
	ctx := context.Background()
	id := insertProcessingEvent(t, db)

	if err := store.RequeueEventForRetry(ctx, db, id, 1700000100); err != nil {
		t.Fatalf("first RequeueEventForRetry: %v", err)
	}
	status, attempts := eventRetryState(t, db, id)
	if status != "received" || attempts != 1 {
		t.Errorf("(status, attempts) = (%q, %d), want ('received', 1)", status, attempts)
	}

	// Second failed turn: re-stamp processing, retry again.
	if err := store.MarkEventProcessing(ctx, db, id, 1700000200); err != nil {
		t.Fatalf("re-stamp processing: %v", err)
	}
	if err := store.RequeueEventForRetry(ctx, db, id, 1700000300); err != nil {
		t.Fatalf("second RequeueEventForRetry: %v", err)
	}
	if status, attempts = eventRetryState(t, db, id); status != "received" || attempts != 2 {
		t.Errorf("(status, attempts) = (%q, %d), want ('received', 2)", status, attempts)
	}
}

// TestRequeueEventForRetryBudgetExhausted: the third failure finds the
// budget spent (§6: max 2) — the typed refusal tells the caller to
// park, and the row is left exactly as the failed turn left it.
func TestRequeueEventForRetryBudgetExhausted(t *testing.T) {
	db := mustOpen(t, filepath.Join(t.TempDir(), "state", "approach.db"))
	ctx := context.Background()
	id := insertProcessingEvent(t, db)
	if _, err := db.Exec(`UPDATE events SET attempts = 2 WHERE id = ?`, id); err != nil {
		t.Fatalf("spend budget: %v", err)
	}

	err := store.RequeueEventForRetry(ctx, db, id, 1700000100)
	if !errors.Is(err, store.ErrRetryBudgetExhausted) {
		t.Fatalf("err = %v, want ErrRetryBudgetExhausted", err)
	}
	status, attempts := eventRetryState(t, db, id)
	if status != "processing" || attempts != 2 {
		t.Errorf("(status, attempts) = (%q, %d), want ('processing', 2) untouched — parking is the CALLER's next move", status, attempts)
	}
}

// TestRequeueEventForRetryRefusesJournalledSideEffect: the §4.6
// judgment can go stale — a straggling PreToolUse hook can journal an
// attempt AFTER recovery read the journal but BEFORE the requeue
// lands. The transition itself re-checks atomically: an unkeyed
// attempt on the event refuses the requeue (typed, so the caller
// parks), while fully-keyed attempts still pass.
func TestRequeueEventForRetryRefusesJournalledSideEffect(t *testing.T) {
	db := mustOpen(t, filepath.Join(t.TempDir(), "state", "approach.db"))
	ctx := context.Background()

	if _, err := store.InsertSession(ctx, db, store.Session{
		ThreadKey: "discord:dm:123", SessionID: "11111111-1111-4111-8111-111111111111",
		Cwd: t.TempDir(), TrustFloor: "owner", CreatedAt: 1700000000, ActivationDeadline: 1700000600,
	}); err != nil {
		t.Fatalf("InsertSession: %v", err)
	}

	stage := func(t *testing.T, dedup, idemKey string) int64 {
		t.Helper()
		ev := testEvent()
		ev.DedupKey = dedup
		ev.Payload = `{"dedup_key":"` + dedup + `","thread_key":"discord:dm:123","kind":"message","trust":"owner"}`
		id, _, err := store.InsertEvent(ctx, db, ev)
		if err != nil {
			t.Fatalf("InsertEvent: %v", err)
		}
		if err := store.MarkEventProcessing(ctx, db, id, 1700000050); err != nil {
			t.Fatalf("MarkEventProcessing: %v", err)
		}
		if _, err := store.InsertToolAttempt(ctx, db, store.ToolAttempt{
			SessionID: "11111111-1111-4111-8111-111111111111", EventID: id,
			Tool: "mcp__discord__send", ArgsDigest: "sha256:aa",
			IdempotencyKey: idemKey, StartedAt: 1700000060,
		}); err != nil {
			t.Fatalf("InsertToolAttempt: %v", err)
		}
		return id
	}

	unkeyed := stage(t, "discord:msg:unkeyed", "")
	err := store.RequeueEventForRetry(ctx, db, unkeyed, 1700000100)
	if !errors.Is(err, store.ErrSideEffectingAttempt) {
		t.Fatalf("err = %v, want ErrSideEffectingAttempt — the transition must re-check the journal atomically", err)
	}
	status, attempts := eventRetryState(t, db, unkeyed)
	if status != "processing" || attempts != 0 {
		t.Errorf("(status, attempts) = (%q, %d), want ('processing', 0) untouched", status, attempts)
	}

	keyed := stage(t, "discord:msg:keyed", "send:msg:42")
	if err := store.RequeueEventForRetry(ctx, db, keyed, 1700000100); err != nil {
		t.Fatalf("RequeueEventForRetry(keyed): %v — fully-keyed attempts keep the §4.6 exception", err)
	}
}

// TestRequeueInterruptedEvent: the §4.6 human retry — an interrupted
// event returns to 'received', durably owed a turn again, at the
// thread's current tail. attempts is untouched: a human's "retry" is
// not the auto budget, and charging it would let two crash-parks
// exhaust the budget before any engine failure happened.
func TestRequeueInterruptedEvent(t *testing.T) {
	db := mustOpen(t, filepath.Join(t.TempDir(), "state", "approach.db"))
	ctx := context.Background()
	id := insertProcessingEvent(t, db)
	if err := store.ParkEvent(ctx, db, id, 1700000100); err != nil {
		t.Fatalf("ParkEvent: %v", err)
	}

	ev, err := store.RequeueInterruptedEvent(ctx, db, id, 1700000200)
	if err != nil {
		t.Fatalf("RequeueInterruptedEvent: %v", err)
	}
	if ev.ID != id || ev.DedupKey != testEvent().DedupKey || ev.ThreadKey != testEvent().ThreadKey || ev.Status != "received" {
		t.Errorf("returned event = %+v — must be the full row, status 'received', ready to enqueue", ev)
	}
	status, attempts := eventRetryState(t, db, id)
	if status != "received" || attempts != 0 {
		t.Errorf("(status, attempts) = (%q, %d), want ('received', 0)", status, attempts)
	}
}

// TestRequeueInterruptedEventGuards: only a parked event retries this
// way — anything else is a caller bug (or a stale command against a
// row the queue already owns again) and must fail loud, never
// double-enqueue.
func TestRequeueInterruptedEventGuards(t *testing.T) {
	db := mustOpen(t, filepath.Join(t.TempDir(), "state", "approach.db"))
	ctx := context.Background()

	id, _, err := store.InsertEvent(ctx, db, testEvent())
	if err != nil {
		t.Fatalf("InsertEvent: %v", err)
	}
	if _, err := store.RequeueInterruptedEvent(ctx, db, id, 1700000200); err == nil {
		t.Error("RequeueInterruptedEvent accepted a 'received' row, want error")
	}
	if _, err := store.RequeueInterruptedEvent(ctx, db, id+999, 1700000200); err == nil {
		t.Error("RequeueInterruptedEvent accepted an unknown id, want error")
	}
}

// TestRequeueEventForRetryWrongState: only a failed TURN retries, and
// turns run from 'processing' — requeueing a row in any other state is
// a caller bug that must fail loud, not silently reshuffle the queue.
func TestRequeueEventForRetryWrongState(t *testing.T) {
	db := mustOpen(t, filepath.Join(t.TempDir(), "state", "approach.db"))
	ctx := context.Background()

	id, _, err := store.InsertEvent(ctx, db, testEvent())
	if err != nil {
		t.Fatalf("InsertEvent: %v", err)
	}
	if err := store.RequeueEventForRetry(ctx, db, id, 1700000100); err == nil {
		t.Error("RequeueEventForRetry accepted a 'received' row, want error")
	} else if errors.Is(err, store.ErrRetryBudgetExhausted) {
		t.Error("wrong-state refusal reported as budget exhaustion — the two must not collapse")
	}
	if err := store.RequeueEventForRetry(ctx, db, id+999, 1700000100); err == nil {
		t.Error("RequeueEventForRetry accepted an unknown id, want error")
	}
}
