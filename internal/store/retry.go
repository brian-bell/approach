package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
)

// maxEventRetries is the §4.6 / §6 auto-retry budget: at most 2
// retries of a turn that provably did nothing, then a human decides.
const maxEventRetries = 2

// ErrRetryBudgetExhausted is the typed refusal when an event has spent
// its §4.6 auto-retry budget — the caller's next move is parking the
// event where the surfacing flows find it, never a third quiet try.
var ErrRetryBudgetExhausted = errors.New("retry budget exhausted (§4.6: max 2)")

// ErrSideEffectingAttempt is the typed refusal when the journal holds
// an unkeyed attempt for the event AT REQUEUE TIME — the caller's
// earlier judgment went stale (a straggling PreToolUse write landed
// after it read the journal), and the caller's next move is parking.
var ErrSideEffectingAttempt = errors.New("unkeyed side-effecting attempt journalled — ambiguous, not retriable (§4.6)")

// RequeueEventForRetry is the §4.6 auto-retry transition: a failed
// turn's event returns to 'received' — durably owed a turn again —
// with one more unit of budget spent. Guarded twice in one statement:
// only a 'processing' row (turns run from processing; anything else is
// a caller bug) and only while budget remains. The write is the first
// statement (no read-then-write snapshot upgrade under WAL); the
// disambiguating read-back below is read-only.
//
// Judging WHETHER a retry is safe is the caller's job (the
// tool_attempts journal, internal/recovery) — but the judgment can go
// stale between its read and this write (a straggling PreToolUse from
// the killed child can journal an attempt in the gap), so the
// transition re-checks atomically: an unkeyed attempt on the event
// refuses the requeue in the same statement that would grant it.
// InsertToolAttempt closes the other half of the race (no journal
// writes once the event leaves 'processing'). The budget guard rides
// the same statement — even a buggy caller cannot retry a turn more
// than the §4.6 contract allows.
func RequeueEventForRetry(ctx context.Context, db *sql.DB, id int64, now int64) error {
	res, err := db.ExecContext(ctx,
		`UPDATE events SET status = 'received', attempts = attempts + 1, updated = ?
		 WHERE id = ? AND status = 'processing' AND attempts < ?
		   AND NOT EXISTS (SELECT 1 FROM tool_attempts
		                   WHERE event_id = ? AND idempotency_key IS NULL)`,
		now, id, maxEventRetries, id,
	)
	if err != nil {
		return fmt.Errorf("store: requeue event %d for retry: %w", id, err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return fmt.Errorf("store: requeue event %d for retry: %w", id, err)
	}
	if n == 0 {
		// The guard missed; say why — "budget spent" and "side effect
		// journalled" (the caller parks) and "wrong state / missing"
		// (a caller bug) demand different reactions and must not
		// collapse. Read-only from here — no snapshot-upgrade hazard.
		var status string
		var attempts int64
		err := db.QueryRowContext(ctx,
			`SELECT status, attempts FROM events WHERE id = ?`, id,
		).Scan(&status, &attempts)
		if err != nil {
			return fmt.Errorf("store: requeue event %d for retry: row missing: %w", id, err)
		}
		if status != "processing" {
			return fmt.Errorf("store: requeue event %d for retry: status is %q, not 'processing' — only a failed turn retries", id, status)
		}
		if attempts >= maxEventRetries {
			return fmt.Errorf("store: requeue event %d for retry: %w", id, ErrRetryBudgetExhausted)
		}
		return fmt.Errorf("store: requeue event %d for retry: %w", id, ErrSideEffectingAttempt)
	}
	return nil
}
