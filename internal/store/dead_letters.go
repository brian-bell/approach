package store

import (
	"context"
	"database/sql"
	"fmt"
)

// DeadLetterEvent is the §4.6 terminal landing: the machine has given
// up on this event — retry budget exhausted, malformed, unroutable, or
// its park's surfacing terminally failed — and a human decides from
// here. One transaction moves the event out of the live lifecycle
// ('dead') and records why in dead_letters: a dead event with no
// record, or a record whose event the queue still owns, would each be
// a silent drop wearing a different hat.
//
// Only a processing or interrupted event dies — those are the two
// states failures actually strand (a 'received' row is the queue's to
// dispatch, and completed/replied history is not a failure). Re-entry
// after a requeued death UPSERTs: fresh reason/entered, acked and
// resolution reset (the new death is unseen and unresolved), entries+1
// so the entry notice keys a fresh notification (same counter-not-
// timestamp rule as events.parks).
func DeadLetterEvent(ctx context.Context, db *sql.DB, eventID int64, reason string, now int64) error {
	// The enum is closed here as well as in schema, so a bad reason
	// names itself instead of surfacing as an opaque CHECK violation.
	switch reason {
	case "retries-exhausted", "malformed", "unroutable", "surface-failed":
	default:
		return fmt.Errorf("store: dead-letter event %d: reason %q is outside the closed enum (§6)", eventID, reason)
	}
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("store: dead-letter event %d: %w", eventID, err)
	}
	// Rollback after a successful Commit is a documented no-op — this
	// only sweeps the error paths.
	defer func() { _ = tx.Rollback() }()

	res, err := tx.ExecContext(ctx,
		`UPDATE events SET status = 'dead', updated = ?
		 WHERE id = ? AND status IN ('processing', 'interrupted')`,
		now, eventID,
	)
	if err != nil {
		return fmt.Errorf("store: dead-letter event %d: %w", eventID, err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return fmt.Errorf("store: dead-letter event %d: %w", eventID, err)
	}
	if n == 0 {
		var status string
		if err := tx.QueryRowContext(ctx,
			`SELECT status FROM events WHERE id = ?`, eventID,
		).Scan(&status); err != nil {
			return fmt.Errorf("store: dead-letter event %d: row missing: %w", eventID, err)
		}
		return fmt.Errorf("store: dead-letter event %d: status is %q — only a failed turn (processing) or a parked event (interrupted) dies", eventID, status)
	}

	if _, err := tx.ExecContext(ctx,
		`INSERT INTO dead_letters (event_id, reason, entered)
		 VALUES (?, ?, ?)
		 ON CONFLICT(event_id) DO UPDATE SET
		     reason = excluded.reason, entered = excluded.entered,
		     acked = NULL, resolution = NULL, entries = entries + 1`,
		eventID, reason, now,
	); err != nil {
		return fmt.Errorf("store: dead-letter event %d: %w", eventID, err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("store: dead-letter event %d: %w", eventID, err)
	}
	return nil
}

// AckDeadLetter records that the owner SAW the death (§4.6) — the
// heartbeat's re-surface scan drops the row. Guarded to unacked,
// unresolved rows: acking a resolved row is a stale command.
func AckDeadLetter(ctx context.Context, db *sql.DB, eventID int64, now int64) error {
	res, err := db.ExecContext(ctx,
		`UPDATE dead_letters SET acked = ?
		 WHERE event_id = ? AND acked IS NULL AND resolution IS NULL`,
		now, eventID,
	)
	if err != nil {
		return fmt.Errorf("store: ack dead letter %d: %w", eventID, err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return fmt.Errorf("store: ack dead letter %d: %w", eventID, err)
	}
	if n == 0 {
		return fmt.Errorf("store: ack dead letter %d: no unacked unresolved row", eventID)
	}
	return nil
}

// DeadLetter is one row of the §4.6 heartbeat re-surface scan, joined
// to the event identity a checklist line renders.
type DeadLetter struct {
	EventID   int64
	DedupKey  string
	ThreadKey string
	Reason    string
	Entered   int64
	Entries   int64
}

// UnackedDeadLetters is the heartbeat re-surface hook (§4.6): every
// death the owner has not seen and no one has resolved, oldest first.
// The no-re-nag guarantee is the §4.2 notifications ledger's job (a
// later epic); this query is the standing checklist item it reads.
func UnackedDeadLetters(ctx context.Context, db *sql.DB) ([]DeadLetter, error) {
	rows, err := db.QueryContext(ctx,
		`SELECT d.event_id, e.dedup_key, e.thread_key, d.reason, d.entered, d.entries
		 FROM dead_letters d JOIN events e ON e.id = d.event_id
		 WHERE d.acked IS NULL AND d.resolution IS NULL
		 ORDER BY d.entered, d.event_id`,
	)
	if err != nil {
		return nil, fmt.Errorf("store: scan unacked dead letters: %w", err)
	}
	// Read-only query: a Close error after full iteration has nothing
	// to add — rows.Err() below already surfaces any read failure.
	defer func() { _ = rows.Close() }()

	var out []DeadLetter
	for rows.Next() {
		var d DeadLetter
		if err := rows.Scan(&d.EventID, &d.DedupKey, &d.ThreadKey, &d.Reason, &d.Entered, &d.Entries); err != nil {
			return nil, fmt.Errorf("store: scan unacked dead letters: %w", err)
		}
		out = append(out, d)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("store: scan unacked dead letters: %w", err)
	}
	return out, nil
}

// ResolveDeadLetterRequeue is the manual drain's requeue half (§4.6):
// a human decided the event should run again. One transaction records
// the resolution and returns the event to 'received' — owed a turn —
// handing the full row back for the router's Readmit. Guarded to
// unresolved rows whose event is still 'dead': dead means a human
// decides ONCE per death.
func ResolveDeadLetterRequeue(ctx context.Context, db *sql.DB, eventID int64, now int64) (QueuedEvent, error) {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return QueuedEvent{}, fmt.Errorf("store: requeue dead letter %d: %w", eventID, err)
	}
	defer func() { _ = tx.Rollback() }()

	res, err := tx.ExecContext(ctx,
		`UPDATE dead_letters SET resolution = 'requeued'
		 WHERE event_id = ? AND resolution IS NULL`,
		eventID,
	)
	if err != nil {
		return QueuedEvent{}, fmt.Errorf("store: requeue dead letter %d: %w", eventID, err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return QueuedEvent{}, fmt.Errorf("store: requeue dead letter %d: %w", eventID, err)
	}
	if n == 0 {
		return QueuedEvent{}, fmt.Errorf("store: requeue dead letter %d: no unresolved row — a human decides once per death (§4.6)", eventID)
	}
	res, err = tx.ExecContext(ctx,
		`UPDATE events SET status = 'received', updated = ? WHERE id = ? AND status = 'dead'`,
		now, eventID,
	)
	if err != nil {
		return QueuedEvent{}, fmt.Errorf("store: requeue dead letter %d: %w", eventID, err)
	}
	n, err = res.RowsAffected()
	if err != nil {
		return QueuedEvent{}, fmt.Errorf("store: requeue dead letter %d: %w", eventID, err)
	}
	if n == 0 {
		return QueuedEvent{}, fmt.Errorf("store: requeue dead letter %d: event is not 'dead' — the record and the event disagree, refusing to guess", eventID)
	}

	var ev QueuedEvent
	if err := tx.QueryRowContext(ctx,
		`SELECT id, dedup_key, thread_key, kind, trust, payload, status, received
		 FROM events WHERE id = ?`, eventID,
	).Scan(&ev.ID, &ev.DedupKey, &ev.ThreadKey, &ev.Kind, &ev.Trust,
		&ev.Payload, &ev.Status, &ev.Received); err != nil {
		return QueuedEvent{}, fmt.Errorf("store: requeue dead letter %d: read back: %w", eventID, err)
	}
	if err := tx.Commit(); err != nil {
		return QueuedEvent{}, fmt.Errorf("store: requeue dead letter %d: %w", eventID, err)
	}
	return ev, nil
}

// ResolveDeadLetterDiscard is the manual drain's discard half (§4.6):
// terminal WITH a record — the event stays dead and the row says a
// human chose that. Guarded to unresolved rows.
func ResolveDeadLetterDiscard(ctx context.Context, db *sql.DB, eventID int64) error {
	res, err := db.ExecContext(ctx,
		`UPDATE dead_letters SET resolution = 'discarded'
		 WHERE event_id = ? AND resolution IS NULL`,
		eventID,
	)
	if err != nil {
		return fmt.Errorf("store: discard dead letter %d: %w", eventID, err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return fmt.Errorf("store: discard dead letter %d: %w", eventID, err)
	}
	if n == 0 {
		return fmt.Errorf("store: discard dead letter %d: no unresolved row — a human decides once per death (§4.6)", eventID)
	}
	return nil
}
