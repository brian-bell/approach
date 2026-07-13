package store_test

import (
	"context"
	"database/sql"
	"path/filepath"
	"testing"

	"github.com/brian-bell/approach/internal/store"
)

// stageDeadCandidate inserts an event and walks it to 'processing' —
// the state a retries-exhausted turn dies from.
func stageDeadCandidate(t *testing.T, db *sql.DB, dedup string) int64 {
	t.Helper()
	ctx := context.Background()
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
	return id
}

func deadLetterRow(t *testing.T, db *sql.DB, eventID int64) (reason string, entered, entries int64, acked sql.NullInt64, resolution sql.NullString) {
	t.Helper()
	if err := db.QueryRow(
		`SELECT reason, entered, entries, acked, resolution FROM dead_letters WHERE event_id = ?`, eventID,
	).Scan(&reason, &entered, &entries, &acked, &resolution); err != nil {
		t.Fatalf("read dead letter %d: %v", eventID, err)
	}
	return
}

// TestDeadLetterEvent: the §4.6 terminal landing — the machine has
// given up, so the event leaves the live lifecycle ('dead') and the
// dead_letters row records why, in one atomic step: a dead event with
// no record (or a record whose event still runs) would be a silent
// drop wearing a different hat.
func TestDeadLetterEvent(t *testing.T) {
	db := mustOpen(t, filepath.Join(t.TempDir(), "state", "approach.db"))
	ctx := context.Background()

	id := stageDeadCandidate(t, db, "discord:msg:doomed")
	if err := store.DeadLetterEvent(ctx, db, id, "retries-exhausted", 1700000100); err != nil {
		t.Fatalf("DeadLetterEvent: %v", err)
	}

	var evStatus string
	if err := db.QueryRow(`SELECT status FROM events WHERE id = ?`, id).Scan(&evStatus); err != nil {
		t.Fatalf("read event: %v", err)
	}
	if evStatus != "dead" {
		t.Errorf("event status = %q, want 'dead'", evStatus)
	}
	reason, entered, entries, acked, resolution := deadLetterRow(t, db, id)
	if reason != "retries-exhausted" || entered != 1700000100 || entries != 1 {
		t.Errorf("(reason, entered, entries) = (%q, %d, %d), want (retries-exhausted, 1700000100, 1)", reason, entered, entries)
	}
	if acked.Valid || resolution.Valid {
		t.Errorf("(acked, resolution) = (%v, %v), want NULL — a fresh death is unseen and unresolved", acked, resolution)
	}

	// An interrupted event whose surfacing terminally failed also dies.
	parked := stageDeadCandidate(t, db, "discord:msg:parked")
	if err := store.ParkEvent(ctx, db, parked, 1700000100); err != nil {
		t.Fatalf("ParkEvent: %v", err)
	}
	if err := store.DeadLetterEvent(ctx, db, parked, "surface-failed", 1700000200); err != nil {
		t.Fatalf("DeadLetterEvent(interrupted): %v", err)
	}
}

// TestDeadLetterEventGuards: only a live-but-failed event dies —
// 'received' would mean the queue still owns it (a dead row would be
// re-dispatched or silently skipped, both wrong), and the reason enum
// is closed.
func TestDeadLetterEventGuards(t *testing.T) {
	db := mustOpen(t, filepath.Join(t.TempDir(), "state", "approach.db"))
	ctx := context.Background()

	id, _, err := store.InsertEvent(ctx, db, testEvent())
	if err != nil {
		t.Fatalf("InsertEvent: %v", err)
	}
	if err := store.DeadLetterEvent(ctx, db, id, "retries-exhausted", 1700000100); err == nil {
		t.Error("DeadLetterEvent accepted a 'received' event, want error — the queue still owns it")
	}
	if err := store.DeadLetterEvent(ctx, db, id+999, "retries-exhausted", 1700000100); err == nil {
		t.Error("DeadLetterEvent accepted an unknown event, want error")
	}
	proc := stageDeadCandidate(t, db, "discord:msg:badreason")
	if err := store.DeadLetterEvent(ctx, db, proc, "bogus", 1700000100); err == nil {
		t.Error("DeadLetterEvent accepted a reason outside the closed enum, want error")
	}
}

// TestDeadLetterReentry: a requeued event can die AGAIN — the row
// re-enters (entries+1, fresh reason/entered, acked/resolution reset)
// so the second death is a new, unseen, unresolved item, never a
// leftover that looks already-handled.
func TestDeadLetterReentry(t *testing.T) {
	db := mustOpen(t, filepath.Join(t.TempDir(), "state", "approach.db"))
	ctx := context.Background()

	id := stageDeadCandidate(t, db, "discord:msg:doomed")
	if err := store.DeadLetterEvent(ctx, db, id, "retries-exhausted", 1700000100); err != nil {
		t.Fatalf("first death: %v", err)
	}
	if err := store.AckDeadLetter(ctx, db, id, 1700000150); err != nil {
		t.Fatalf("AckDeadLetter: %v", err)
	}
	if _, err := store.ResolveDeadLetterRequeue(ctx, db, id, 1700000200); err != nil {
		t.Fatalf("ResolveDeadLetterRequeue: %v", err)
	}
	// The retry fails terminally again.
	if err := store.MarkEventProcessing(ctx, db, id, 1700000250); err != nil {
		t.Fatalf("re-stamp processing: %v", err)
	}
	if err := store.DeadLetterEvent(ctx, db, id, "retries-exhausted", 1700000300); err != nil {
		t.Fatalf("second death: %v", err)
	}

	reason, entered, entries, acked, resolution := deadLetterRow(t, db, id)
	if entries != 2 {
		t.Errorf("entries = %d, want 2 — each death is a distinct episode", entries)
	}
	if reason != "retries-exhausted" || entered != 1700000300 {
		t.Errorf("(reason, entered) = (%q, %d), want the SECOND death's values", reason, entered)
	}
	if acked.Valid || resolution.Valid {
		t.Errorf("(acked, resolution) = (%v, %v), want NULL — the second death is unseen and unresolved", acked, resolution)
	}
}

// TestUnackedDeadLetters: the heartbeat re-surface hook (§4.6) — rows
// the owner has not seen and no one has resolved, carrying the event
// fields a checklist renders. Acked or resolved rows drop out.
func TestUnackedDeadLetters(t *testing.T) {
	db := mustOpen(t, filepath.Join(t.TempDir(), "state", "approach.db"))
	ctx := context.Background()

	fresh := stageDeadCandidate(t, db, "discord:msg:fresh")
	if err := store.DeadLetterEvent(ctx, db, fresh, "retries-exhausted", 1700000100); err != nil {
		t.Fatalf("DeadLetterEvent: %v", err)
	}
	seen := stageDeadCandidate(t, db, "discord:msg:seen")
	if err := store.DeadLetterEvent(ctx, db, seen, "malformed", 1700000100); err != nil {
		t.Fatalf("DeadLetterEvent: %v", err)
	}
	if err := store.AckDeadLetter(ctx, db, seen, 1700000200); err != nil {
		t.Fatalf("AckDeadLetter: %v", err)
	}

	rows, err := store.UnackedDeadLetters(ctx, db)
	if err != nil {
		t.Fatalf("UnackedDeadLetters: %v", err)
	}
	if len(rows) != 1 {
		t.Fatalf("got %d unacked rows, want 1 — acked rows must drop out", len(rows))
	}
	if rows[0].EventID != fresh || rows[0].DedupKey != "discord:msg:fresh" || rows[0].Reason != "retries-exhausted" {
		t.Errorf("row = %+v, want the fresh death with its event identity", rows[0])
	}

	if err := store.AckDeadLetter(ctx, db, fresh+999, 1700000200); err == nil {
		t.Error("AckDeadLetter accepted an unknown event, want error")
	}
}

// TestResolveDeadLetterRequeue: the manual drain's requeue half —
// resolution recorded and the event owed a turn again, atomically; a
// second resolution refuses (dead means a HUMAN decides, once).
func TestResolveDeadLetterRequeue(t *testing.T) {
	db := mustOpen(t, filepath.Join(t.TempDir(), "state", "approach.db"))
	ctx := context.Background()

	id := stageDeadCandidate(t, db, "discord:msg:doomed")
	if err := store.DeadLetterEvent(ctx, db, id, "retries-exhausted", 1700000100); err != nil {
		t.Fatalf("DeadLetterEvent: %v", err)
	}
	ev, err := store.ResolveDeadLetterRequeue(ctx, db, id, 1700000200)
	if err != nil {
		t.Fatalf("ResolveDeadLetterRequeue: %v", err)
	}
	if ev.ID != id || ev.Status != "received" || ev.DedupKey != "discord:msg:doomed" {
		t.Errorf("returned event = %+v, want the full row back in 'received'", ev)
	}
	_, _, _, _, resolution := deadLetterRow(t, db, id)
	if !resolution.Valid || resolution.String != "requeued" {
		t.Errorf("resolution = %v, want 'requeued'", resolution)
	}
	if _, err := store.ResolveDeadLetterRequeue(ctx, db, id, 1700000300); err == nil {
		t.Error("second resolution accepted, want error — a human decides once per death")
	}
}

// TestResolveDeadLetterDiscard: the discard half — terminal with a
// record: the event stays dead, the row says a human chose that.
func TestResolveDeadLetterDiscard(t *testing.T) {
	db := mustOpen(t, filepath.Join(t.TempDir(), "state", "approach.db"))
	ctx := context.Background()

	id := stageDeadCandidate(t, db, "discord:msg:doomed")
	if err := store.DeadLetterEvent(ctx, db, id, "unroutable", 1700000100); err != nil {
		t.Fatalf("DeadLetterEvent: %v", err)
	}
	if err := store.ResolveDeadLetterDiscard(ctx, db, id); err != nil {
		t.Fatalf("ResolveDeadLetterDiscard: %v", err)
	}
	var evStatus string
	if err := db.QueryRow(`SELECT status FROM events WHERE id = ?`, id).Scan(&evStatus); err != nil {
		t.Fatalf("read event: %v", err)
	}
	if evStatus != "dead" {
		t.Errorf("event status = %q, want 'dead' — discard is terminal", evStatus)
	}
	_, _, _, _, resolution := deadLetterRow(t, db, id)
	if !resolution.Valid || resolution.String != "discarded" {
		t.Errorf("resolution = %v, want 'discarded'", resolution)
	}
	if err := store.ResolveDeadLetterDiscard(ctx, db, id); err == nil {
		t.Error("second discard accepted, want error")
	}
	if err := store.ResolveDeadLetterDiscard(ctx, db, id+999); err == nil {
		t.Error("discard of an unknown event accepted, want error")
	}
}
