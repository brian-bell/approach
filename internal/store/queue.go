package store

import (
	"context"
	"database/sql"
	"fmt"
)

// QueuedEvent is one row of the §4.1 rebuild scan: everything dispatch
// needs, read in one query — a rebuild that re-queried per event would
// race fresh ingest writes landing between reads. Status rides along
// because restart recovery treats a crash-interrupted 'processing' row
// differently from a never-started 'received' one (§4.6).
type QueuedEvent struct {
	ID          int64
	DedupKey    string
	ThreadKey   string
	Kind        string
	Trust       string
	Payload     string
	Status      string
	Received    int64
	Correlation string // origin link (§6); "" = none — see Event.Correlation
}

// MarkEventProcessing is the pre-turn durability stamp (§4.1): the
// row leaves 'received' BEFORE the handler runs, so a daemon that
// dies mid-turn finds it 'processing' on restart and parks it as
// interrupted (§4.6) — never re-dispatches it as never-started work,
// which would replay a half-finished turn's side effects. Guarded:
// only a received row may start a turn; zero rows affected means the
// event was already claimed or advanced, and the caller must not run
// the handler.
func MarkEventProcessing(ctx context.Context, db *sql.DB, id int64, now int64) error {
	res, err := db.ExecContext(ctx,
		`UPDATE events SET status = 'processing', updated = ? WHERE id = ? AND status = 'received'`,
		now, id,
	)
	if err != nil {
		return fmt.Errorf("store: mark event %d processing: %w", id, err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return fmt.Errorf("store: mark event %d processing: %w", id, err)
	}
	if n == 0 {
		return fmt.Errorf("store: mark event %d processing: not in received state", id)
	}
	return nil
}

// MarkEventCompleted is the post-turn transition (§4.1): the turn ran
// to completion, so the row leaves the live queue — 'completed' is the
// rest state until (and unless) the reply leg's last platform ack
// advances it to 'replied' (AckDelivery). Guarded: turns run from
// 'processing', so completing any other status is a caller sequencing
// bug that must fail loud — silently re-completing history would hide
// a double dispatch.
func MarkEventCompleted(ctx context.Context, db *sql.DB, id int64, now int64) error {
	res, err := db.ExecContext(ctx,
		`UPDATE events SET status = 'completed', updated = ? WHERE id = ? AND status = 'processing'`,
		now, id,
	)
	if err != nil {
		return fmt.Errorf("store: mark event %d completed: %w", id, err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return fmt.Errorf("store: mark event %d completed: %w", id, err)
	}
	if n == 0 {
		return fmt.Errorf("store: mark event %d completed: not in processing state", id)
	}
	return nil
}

// MarkEventCompletedReconciled is the duplicate-compose completion
// transition. A prior life already wrote this event's reply rows, and
// restart recovery may have acked all of them while the event was
// parked interrupted. Complete and reconcile in ONE statement so the
// event lands directly at replied when every existing outbound row is
// already accepted; otherwise no future ack remains to advance a
// completed row (§4.1, §4.6). No delivery rows still means completed,
// and any unacked or failed row blocks replied just as AckDelivery
// does. The processing guard matches MarkEventCompleted.
func MarkEventCompletedReconciled(ctx context.Context, db *sql.DB, id int64, now int64) error {
	res, err := db.ExecContext(ctx,
		`UPDATE events
		 SET status = CASE
		     WHEN EXISTS (SELECT 1 FROM deliveries WHERE event_id = ?)
		      AND NOT EXISTS (SELECT 1 FROM deliveries WHERE event_id = ? AND acked IS NULL)
		     THEN 'replied' ELSE 'completed' END,
		     updated = ?
		 WHERE id = ? AND status = 'processing'`,
		id, id, now, id,
	)
	if err != nil {
		return fmt.Errorf("store: mark event %d completed with delivery reconciliation: %w", id, err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return fmt.Errorf("store: mark event %d completed with delivery reconciliation: %w", id, err)
	}
	if n == 0 {
		return fmt.Errorf("store: mark event %d completed with delivery reconciliation: not in processing state", id)
	}
	return nil
}

// SkipEvent records a deliberate refusal to process (§6 'skipped' —
// the §4.2 misfire coalescing state, and until the C9 policy hook
// lands, the trust admission gate's landing): the row leaves the live
// queue consumed on purpose — no turn ran, nothing is owed, and unlike
// a park or a dead letter nothing is surfaced, so a stranger's refused
// message cannot flood the owner with notices. Guarded to processing:
// only the handler that claimed an event may skip it, and skipping any
// other state would erase queue history.
func SkipEvent(ctx context.Context, db *sql.DB, id int64, now int64) error {
	res, err := db.ExecContext(ctx,
		`UPDATE events SET status = 'skipped', updated = ? WHERE id = ? AND status = 'processing'`,
		now, id,
	)
	if err != nil {
		return fmt.Errorf("store: skip event %d: %w", id, err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return fmt.Errorf("store: skip event %d: %w", id, err)
	}
	if n == 0 {
		return fmt.Errorf("store: skip event %d: not in processing state", id)
	}
	return nil
}

// scanQueuedEvent reads one queue-view row: the eight event columns
// plus the nullable correlation, in the canonical SELECT order every
// queue query uses — one scanner, so a new column cannot be carried by
// some views and silently dropped by others.
func scanQueuedEvent(row interface{ Scan(...any) error }) (QueuedEvent, error) {
	var ev QueuedEvent
	var correlation sql.NullString
	if err := row.Scan(&ev.ID, &ev.DedupKey, &ev.ThreadKey, &ev.Kind, &ev.Trust,
		&ev.Payload, &ev.Status, &ev.Received, &correlation); err != nil {
		return QueuedEvent{}, err
	}
	ev.Correlation = correlation.String
	return ev, nil
}

// UnprocessedEvents is the restart rebuild scan (§4.1): every row still
// owed a turn — status received or processing — in id order, which is
// receipt order. The in-memory per-thread queues are ONLY an index over
// these rows; this query is the single definition of what survives a
// restart. It deliberately matches the ev_queue partial index predicate
// — one definition of "unprocessed", in schema and here.
func UnprocessedEvents(ctx context.Context, db *sql.DB) ([]QueuedEvent, error) {
	rows, err := db.QueryContext(ctx,
		`SELECT id, dedup_key, thread_key, kind, trust, payload, status, received, correlation
		 FROM events
		 WHERE status IN ('received', 'processing')
		 ORDER BY id`,
	)
	if err != nil {
		return nil, fmt.Errorf("store: scan unprocessed events: %w", err)
	}
	// Read-only query: a Close error after full iteration has nothing
	// to add — rows.Err() below already surfaces any read failure.
	defer func() { _ = rows.Close() }()

	var out []QueuedEvent
	for rows.Next() {
		ev, err := scanQueuedEvent(rows)
		if err != nil {
			return nil, fmt.Errorf("store: scan unprocessed events: %w", err)
		}
		out = append(out, ev)
	}
	// A half-read queue rebuilt as whole would silently drop the tail
	// (§4.1: events are never silently dropped) — iteration errors are
	// rebuild failures, not short results.
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("store: scan unprocessed events: %w", err)
	}
	return out, nil
}
