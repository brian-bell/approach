package store

import (
	"context"
	"database/sql"
	"fmt"
)

// Delivery is one outbound message bound for the §6 deliveries outbox:
// the fields the composer stamps before the first send attempt.
// Lifecycle state (status, attempts, last_attempt, acked) is
// deliberately absent — the schema owns those defaults, and insert
// records intent to send, it never sends.
type Delivery struct {
	DeliveryKey string // delivery identity — a crash-retried compose collapses to one row
	EventID     int64  // originating turn's events.id; 0 = none (stored NULL — §4.2 notifies)
	Target      string // channel thread key to send to (§6 per-channel contract)
	Payload     string // the rendered text — persisted BEFORE the send attempt (§4.1)
}

// InsertDelivery is the write-before-send chokepoint (§4.1): persist
// the rendered payload before the FIRST send attempt, so a crash
// between compose and platform ack re-sends from this row instead of
// silently eating the reply. Validation happens before the db is
// touched and fails loud — a row with no target or no payload could
// never be honestly sent, and inserting it would park the outbox on a
// row no resend can drain.
//
// A duplicate delivery_key is a reported no-op, never an error: a
// crash-retried compose must collapse to the original row — including
// lifecycle state a send may already have advanced — so at-least-once
// duplication comes only from re-sending, never from two divergent
// payloads for one delivery. The conflict target is exactly
// delivery_key, so every OTHER constraint (CHECK, NOT NULL, the
// event_id foreign key) still fails loud. As with InsertEvent, the
// returned id is meaningful only when inserted is true.
//
// A replied event is SEALED: replied asserts the platform accepted
// every outbound row, so a composer binding a NEW row to it (chunk
// inserted after an earlier chunk's ack already advanced the event)
// is a sequencing bug refused loud — silently accepting the row would
// strand it under an event whose claim it falsifies. The seal rides
// the INSERT statement itself (a separate pre-check could pass just
// before a concurrent ack seals the event, landing the row anyway).
func InsertDelivery(ctx context.Context, db *sql.DB, d Delivery) (id int64, inserted bool, err error) {
	return insertDelivery(ctx, db, d)
}

// queryExecer is the slice of database/sql shared by *sql.DB and
// *sql.Tx that the delivery insert needs — the write plus the
// duplicate-vs-seal disambiguation read.
type queryExecer interface {
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
}

// insertDelivery is InsertDelivery's core over either a bare handle or
// a transaction — InsertDeliveries runs the same semantics per row
// inside one tx.
func insertDelivery(ctx context.Context, ex queryExecer, d Delivery) (id int64, inserted bool, err error) {
	if err := d.validate(); err != nil {
		return 0, false, fmt.Errorf("store: insert delivery: %w", err)
	}
	// 0 means "no originating event" and must land as NULL: event_id
	// is a foreign key, and a literal 0 would be refused (no such
	// events row) instead of recording a scheduler notify.
	var eventID any
	if d.EventID != 0 {
		eventID = d.EventID
	}
	res, err := ex.ExecContext(ctx,
		`INSERT INTO deliveries (delivery_key, event_id, target, payload)
		 SELECT ?, ?, ?, ?
		 WHERE NOT EXISTS (SELECT 1 FROM events WHERE id = ? AND status = 'replied')
		 ON CONFLICT(delivery_key) DO NOTHING`,
		d.DeliveryKey, eventID, d.Target, d.Payload, eventID,
	)
	if err != nil {
		return 0, false, fmt.Errorf("store: insert delivery %s: %w", d.DeliveryKey, err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return 0, false, fmt.Errorf("store: insert delivery %s: %w", d.DeliveryKey, err)
	}
	if n == 0 {
		// Zero rows is either the benign duplicate (row already exists —
		// first write wins, caller must not send) or the seal refusal
		// (no row exists and none may land). The two must not collapse:
		// one is normal at-least-once operation, the other a bug.
		var exists int
		if err := ex.QueryRowContext(ctx,
			`SELECT count(*) FROM deliveries WHERE delivery_key = ?`, d.DeliveryKey,
		).Scan(&exists); err != nil {
			return 0, false, fmt.Errorf("store: insert delivery %s: %w", d.DeliveryKey, err)
		}
		if exists == 0 {
			return 0, false, fmt.Errorf("store: insert delivery %s: event %d is already replied — a sealed event admits no new outbound rows (§4.1)", d.DeliveryKey, d.EventID)
		}
		return 0, false, nil
	}
	id, err = res.LastInsertId()
	if err != nil {
		return 0, false, fmt.Errorf("store: insert delivery %s: %w", d.DeliveryKey, err)
	}
	return id, true, nil
}

// InsertDeliveries composes one turn's whole reply atomically: every
// chunk row lands, or none do. The all-or-nothing shape is what makes
// a compose failure RECOVERABLE — a partial compose would leave a
// truncated head durably owed, and a later re-compose of the same turn
// would collide with the stale prefix's keys (first write wins) and
// deliver a mixed reply. With rollback, the caller can park the event
// (§4.6) knowing no fragment of the failed compose survives.
//
// A DUPLICATE key anywhere in the batch rolls the whole batch back
// too (composed=false, no error): a prior execution of this event
// already composed its reply — completely, because every compose is
// this same atomic batch — and committing only the fresh remainder
// would stitch a stale prefix from that execution onto this one's
// tail (a manual §4.6 retry may produce different text and a
// different chunk count). First write wins for the WHOLE reply: the
// caller leaves sending to the pump, which delivers the coherent
// prior compose. ids aligns with ds and is meaningful only when
// composed is true.
func InsertDeliveries(ctx context.Context, db *sql.DB, ds []Delivery) (ids []int64, composed bool, err error) {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return nil, false, fmt.Errorf("store: insert deliveries: %w", err)
	}
	// Rollback after a successful Commit is a documented no-op — this
	// only sweeps the error and duplicate-conflict paths.
	defer func() { _ = tx.Rollback() }()

	ids = make([]int64, 0, len(ds))
	for _, d := range ds {
		id, inserted, err := insertDelivery(ctx, tx, d)
		if err != nil {
			return nil, false, err
		}
		if !inserted {
			return nil, false, nil
		}
		ids = append(ids, id)
	}
	if err := tx.Commit(); err != nil {
		return nil, false, fmt.Errorf("store: insert deliveries: %w", err)
	}
	return ids, true, nil
}

// OwedDeliveriesBefore counts target's rows still owed a send — the
// resend-scan predicate (unacked, not failed) — with an id below
// before. The live-send path consults it: a direct send while an OLDER
// row is owed would deliver this thread's messages out of order
// (§4.1), so the turn defers to the pump, which sends in compose
// order.
func OwedDeliveriesBefore(ctx context.Context, db *sql.DB, target string, before int64) (int64, error) {
	var n int64
	err := db.QueryRowContext(ctx,
		`SELECT count(*) FROM deliveries
		 WHERE target = ? AND id < ? AND acked IS NULL AND status <> 'failed'`,
		target, before,
	).Scan(&n)
	if err != nil {
		return 0, fmt.Errorf("store: owed deliveries before %d for %s: %w", before, target, err)
	}
	return n, nil
}

// ResendableDelivery is one row of the §4.6 restart resend scan:
// everything a re-send needs, read in one query. EventID is 0 when the
// row has no originating event (§4.2 notifies); Attempts rides along
// because the §4.6 budget accounting reads it.
type ResendableDelivery struct {
	ID          int64
	DeliveryKey string
	EventID     int64
	Target      string
	Payload     string
	Attempts    int64
}

// ResendableDeliveries is the restart resend scan (§4.6): every row
// still owed a send — unacked and not terminally failed — in id order,
// which is compose order, so one thread's messages re-send in the
// order they were written. This query is the single runtime definition
// of "owed a send" and deliberately matches the deliveries_resend
// partial index predicate (0006) — one definition, in schema and here.
// Acked rows are delivered history; failed rows belong to the
// dead-letter flows, and re-sending one would resurrect a give-up the
// owner may already have been told about.
func ResendableDeliveries(ctx context.Context, db *sql.DB) ([]ResendableDelivery, error) {
	rows, err := db.QueryContext(ctx,
		`SELECT id, delivery_key, event_id, target, payload, attempts
		 FROM deliveries
		 WHERE acked IS NULL AND status <> 'failed'
		 ORDER BY id`,
	)
	if err != nil {
		return nil, fmt.Errorf("store: scan resendable deliveries: %w", err)
	}
	// Read-only query: a Close error after full iteration has nothing
	// to add — rows.Err() below already surfaces any read failure.
	defer func() { _ = rows.Close() }()

	var out []ResendableDelivery
	for rows.Next() {
		var d ResendableDelivery
		var eventID sql.NullInt64
		if err := rows.Scan(&d.ID, &d.DeliveryKey, &eventID, &d.Target, &d.Payload, &d.Attempts); err != nil {
			return nil, fmt.Errorf("store: scan resendable deliveries: %w", err)
		}
		d.EventID = eventID.Int64 // zero value when NULL — 0 means "no event", same as Delivery
		out = append(out, d)
	}
	// A half-read scan treated as whole would silently drop the tail's
	// re-sends — iteration errors are scan failures, not short results.
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("store: scan resendable deliveries: %w", err)
	}
	return out, nil
}

// MarkDeliveryAttempt journals one send attempt before its outcome is
// known — the §4.6 retry accounting reads attempts/last_attempt, so
// the stamp must precede the send the same way the processing stamp
// precedes a turn. Guarded to rows still owed a send (unacked,
// non-failed — the resend-scan predicate): stamping terminal or
// delivered history is a caller bug that must fail loud, not silently
// resurrect it.
func MarkDeliveryAttempt(ctx context.Context, db *sql.DB, id int64, now int64) error {
	res, err := db.ExecContext(ctx,
		`UPDATE deliveries SET attempts = attempts + 1, last_attempt = ?
		 WHERE id = ? AND acked IS NULL AND status <> 'failed'`,
		now, id,
	)
	if err != nil {
		return fmt.Errorf("store: mark delivery %d attempt: %w", id, err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return fmt.Errorf("store: mark delivery %d attempt: %w", id, err)
	}
	if n == 0 {
		return fmt.Errorf("store: mark delivery %d attempt: row is not owed a send (missing, failed, or already acked)", id)
	}
	return nil
}

// AckDelivery records the platform ack — the ONLY transition that may
// advance the originating event completed → replied (§4.1), and the
// two writes are one transaction: a delivery stamped sent whose event
// stayed completed would be re-replied by the queue, and an event
// replied without its ack recorded would strand the delivery in the
// resend scan. Neither half-state may ever be visible.
//
// One turn may emit several outbound rows for one event (§4.1 routes
// EVERY outbound message through the outbox; platform length limits
// chunk long replies), so replied rides the LAST sibling's ack: each
// ack is recorded as it arrives, and the event advances only when
// every sibling is acked. A terminally failed sibling blocks replied
// PERMANENTLY — a reply with an undelivered chunk is not replied, and
// the block must not depend on whether the failure or the last ack
// landed first: both orders converge on completed, and the event RESTS
// there — the honest terminal state for "turn done, reply not fully
// delivered". Deliberately NOT the events dead-letter flow: that
// drain's requeue re-runs the whole turn, and replaying a COMPLETED
// turn's side effects is the §4.6 hazard itself. What failed is the
// delivery row, and its terminal outcome belongs to the delivery-level
// drain that must land with the retry-budget flow (approach-bqmh) —
// retry re-sends the persisted payload, no engine involved.
//
// A duplicate ack is a no-op, not an error: at-least-once means
// duplicate sends, and each send's ack is normal operation — the
// first stamp wins. An ack against a 'failed' row IS an error: failed
// is terminal (budget exhausted, nothing in flight to ack). An ack
// whose bound event never reached 'completed' is a sequencing bug —
// the reply leg outran the turn — and fails loud with everything
// rolled back.
func AckDelivery(ctx context.Context, db *sql.DB, id int64, now int64) error {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("store: ack delivery %d: %w", id, err)
	}
	// Rollback after a successful Commit is a documented no-op — this
	// only sweeps the error paths, which return their own failure.
	defer func() { _ = tx.Rollback() }()

	// The guarded write is the transaction's FIRST statement, taking
	// the write lock upfront (same reason migrate.go spells BEGIN
	// IMMEDIATE): a deferred read-then-write upgrade under WAL fails
	// with SQLITE_BUSY_SNAPSHOT — which busy_timeout does not retry —
	// whenever another writer commits in between, and a "busy" ack
	// would strand a platform-accepted message in the resend scan.
	res, err := tx.ExecContext(ctx,
		`UPDATE deliveries SET status = 'sent', acked = ?
		 WHERE id = ? AND acked IS NULL AND status <> 'failed'`,
		now, id,
	)
	if err != nil {
		return fmt.Errorf("store: ack delivery %d: %w", id, err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return fmt.Errorf("store: ack delivery %d: %w", id, err)
	}
	if n == 0 {
		// The guard missed; read back to say why — "already acked"
		// (no-op), "failed" (error), and "missing" (error) must not
		// collapse into one message. Read-only from here on, so the
		// snapshot-upgrade hazard above cannot bite these paths.
		var status string
		var acked sql.NullInt64
		err := tx.QueryRowContext(ctx,
			`SELECT status, acked FROM deliveries WHERE id = ?`, id,
		).Scan(&status, &acked)
		if err != nil {
			return fmt.Errorf("store: ack delivery %d: row missing — nothing was owed a send: %w", id, err)
		}
		if acked.Valid {
			return nil // duplicate ack from a re-send — first stamp wins
		}
		return fmt.Errorf("store: ack delivery %d: row is terminally failed — no send remained to ack", id)
	}

	var eventID sql.NullInt64
	if err := tx.QueryRowContext(ctx,
		`SELECT event_id FROM deliveries WHERE id = ?`, id,
	).Scan(&eventID); err != nil {
		return fmt.Errorf("store: ack delivery %d: %w", id, err)
	}

	if eventID.Valid {
		// Advance completed → replied only when this ack settles the
		// event's LAST sibling: no row still unacked. Deliberately NOT
		// the resend-scan predicate — a failed row leaves the scan but
		// still blocks replied, because "replied" asserts the platform
		// accepted every outbound message.
		res, err := tx.ExecContext(ctx,
			`UPDATE events SET status = 'replied', updated = ?
			 WHERE id = ? AND status = 'completed'
			   AND NOT EXISTS (SELECT 1 FROM deliveries
			                   WHERE event_id = ? AND acked IS NULL)`,
			now, eventID.Int64, eventID.Int64,
		)
		if err != nil {
			return fmt.Errorf("store: ack delivery %d: advance event %d: %w", id, eventID.Int64, err)
		}
		n, err := res.RowsAffected()
		if err != nil {
			return fmt.Errorf("store: ack delivery %d: advance event %d: %w", id, eventID.Int64, err)
		}
		if n == 0 {
			// The hold is legitimate unless the event never finished
			// its turn: siblings still unacked or failed (stays
			// completed), already replied, or parked/dead — states the
			// §4.6 flows own, where the ack must still be RECORDED (the
			// platform accepted the message; rolling it back would
			// re-send forever). Only a pre-completion event refuses:
			// the reply leg outran the turn, a daemon sequencing bug.
			var evStatus string
			if err := tx.QueryRowContext(ctx,
				`SELECT status FROM events WHERE id = ?`, eventID.Int64,
			).Scan(&evStatus); err != nil {
				return fmt.Errorf("store: ack delivery %d: read event %d: %w", id, eventID.Int64, err)
			}
			if evStatus == "received" || evStatus == "processing" {
				return fmt.Errorf("store: ack delivery %d: event %d is %q — the reply leg outran the turn (§4.1)", id, eventID.Int64, evStatus)
			}
		}
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("store: ack delivery %d: %w", id, err)
	}
	return nil
}

// MarkDeliveryFailed is the terminal give-up (§4.6): the retry budget
// is exhausted and the row leaves the resend scan for the
// DELIVERY-level drain — surfacing, manual re-send, discard — that
// must land with the retry-budget flow that calls this (approach-bqmh;
// a terminal state never ships without its drain). Not the events
// dead-letter flow: the bound event's turn already ran, and only a
// re-SEND of the persisted payload is safe. Guarded to live unacked
// rows: flipping a delivered
// message to failed would un-deliver history, and re-entering failed
// must not read as a FRESH transition — the §4.6 surfacing (one DM on
// entry) keys off entering this state, so a repeat give-up reporting
// success would notify the owner twice for one failure. A row that
// isn't there to fail is a caller bug, not a quiet success.
func MarkDeliveryFailed(ctx context.Context, db *sql.DB, id int64) error {
	res, err := db.ExecContext(ctx,
		`UPDATE deliveries SET status = 'failed'
		 WHERE id = ? AND acked IS NULL AND status <> 'failed'`,
		id,
	)
	if err != nil {
		return fmt.Errorf("store: mark delivery %d failed: %w", id, err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return fmt.Errorf("store: mark delivery %d failed: %w", id, err)
	}
	if n == 0 {
		return fmt.Errorf("store: mark delivery %d failed: row missing, already acked, or already failed — the transition must be entered once", id)
	}
	return nil
}

// validate refuses a delivery the outbox could not honestly send.
func (d Delivery) validate() error {
	switch {
	case d.DeliveryKey == "":
		return fmt.Errorf("empty delivery_key — the delivery would have no identity (§6)")
	case d.Target == "":
		return fmt.Errorf("empty target — the delivery could never be addressed")
	case d.Payload == "":
		return fmt.Errorf("empty payload — write-before-send persists the rendered text, not a placeholder (§4.1)")
	case d.EventID < 0:
		return fmt.Errorf("event_id = %d, want 0 (none) or a positive events row id", d.EventID)
	}
	return nil
}
