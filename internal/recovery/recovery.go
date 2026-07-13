// Package recovery owns the §4.6 engine-failure policy: retry only
// what provably did nothing; surface everything else. The turn wiring
// calls HandleEngineFailure when a turn ends in a non-zero exit,
// timeout, or rate limit — the verdict comes from the tool_attempts
// journal (§6), never from aggregate counts a crash can lose.
package recovery

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/brian-bell/approach/internal/store"
)

// Outcome is what became of the failed turn's event: re-queued for an
// automatic retry, or parked interrupted for a human.
type Outcome int

const (
	// Retried: the event is durably 'received' again and re-enters its
	// thread's queue after backoff.
	Retried Outcome = iota
	// Parked: the event is durably 'interrupted' — out-of-band, the
	// thread keeps flowing, the §4.6 surfacing flows own it now.
	Parked
)

// Enqueuer re-admits an event to its thread's in-memory queue —
// *router.Queues satisfies it. One method, so recovery never imports
// the router's world.
type Enqueuer interface {
	Enqueue(ev store.QueuedEvent)
}

// Options wires the injectables. Logger is required; Now defaults to
// time.Now; After defaults to time.AfterFunc-shaped scheduling — tests
// substitute a synchronous stub so no real timer runs.
type Options struct {
	Logger *slog.Logger
	Now    func() time.Time
	After  func(d time.Duration, f func())
}

// backoff is the §4.6 retry schedule by budget unit being spent
// (attempt 1 → 30s, attempt 2 → 2m). Growing, so a persistently
// failing engine is asked less and less often; rate-limit-aware
// tuning rides the turn wiring (x6n.8).
var backoff = [...]time.Duration{30 * time.Second, 2 * time.Minute}

// HandleEngineFailure applies the §4.6 rule to one failed turn.
//
// The journal is the only admissible evidence: zero attempts means the
// turn provably did nothing (auto-retry, budget permitting); ANY
// attempt — done and failed included, a completed side effect would
// replay on retry — makes the turn ambiguous unless EVERY attempt
// carries an idempotency_key that makes a repeat provably safe. A
// journal that cannot be READ is ambiguity itself: the error returns
// to the caller and nothing is retried on missing evidence.
//
// Retried events go durable-first (RequeueEventForRetry: 'received',
// budget spent) and re-enter the thread's queue at its CURRENT tail
// after backoff — a daemon that dies before the timer fires loses only
// the delay, because Rebuild re-indexes every 'received' row. Parked
// events go through ParkEvent (interrupted); budget exhaustion parks
// too, until the dead_letters landing (x6n.3.6) takes that branch
// over. Either way the event ends in a durable, human-visible state —
// never a silent drop.
func HandleEngineFailure(ctx context.Context, db *sql.DB, enq Enqueuer, ev store.QueuedEvent, opts Options) (Outcome, error) {
	now := opts.Now
	if now == nil {
		now = time.Now
	}
	after := opts.After
	if after == nil {
		after = func(d time.Duration, f func()) { time.AfterFunc(d, f) }
	}

	attempts, err := store.AttemptsForEvent(ctx, db, ev.ID)
	if err != nil {
		return 0, fmt.Errorf("recovery: judge event %s: journal unreadable — ambiguous, not retried: %w", ev.DedupKey, err)
	}
	if !safeToRetry(attempts) {
		return park(ctx, db, ev, opts.Logger, now,
			"side-effecting attempt without idempotency key — ambiguous, parked (§4.6)")
	}

	if err := store.RequeueEventForRetry(ctx, db, ev.ID, now().Unix()); err != nil {
		if errors.Is(err, store.ErrRetryBudgetExhausted) {
			return park(ctx, db, ev, opts.Logger, now,
				"retry budget exhausted — parked for a human (§4.6)")
		}
		if errors.Is(err, store.ErrSideEffectingAttempt) {
			// The judgment above went stale — a straggling PreToolUse
			// journalled an attempt in the gap. The transition's atomic
			// re-check caught it; ambiguity parks.
			return park(ctx, db, ev, opts.Logger, now,
				"attempt journalled during recovery — ambiguous, parked (§4.6)")
		}
		return 0, fmt.Errorf("recovery: requeue event %s: %w", ev.DedupKey, err)
	}

	// Budget unit just spent = the row's new attempts value; index the
	// schedule by the unit number (1-based → slice offset). A failed
	// read degrades to the LONGEST backoff — never to a skipped retry:
	// the requeue above already committed, so the retry must happen.
	delay := backoff[len(backoff)-1]
	var spent int64
	if err := db.QueryRowContext(ctx,
		`SELECT attempts FROM events WHERE id = ?`, ev.ID,
	).Scan(&spent); err != nil {
		opts.Logger.Warn("attempts read-back failed — using longest backoff",
			"dedup_key", ev.DedupKey, "error", err.Error())
	} else if spent >= 1 && int(spent) <= len(backoff) {
		delay = backoff[spent-1]
	}
	requeued := ev
	requeued.Status = "received"
	after(delay, func() { enq.Enqueue(requeued) })
	opts.Logger.Info("engine failure — clean turn requeued with backoff (§4.6)",
		"thread_key", ev.ThreadKey, "dedup_key", ev.DedupKey, "attempt", spent, "backoff", delay.String())
	return Retried, nil
}

// safeToRetry is the §4.6 judgment: no attempts, or every attempt
// idempotency-keyed. One unkeyed attempt in any state poisons the
// turn — a keyed sibling cannot launder it. The keyed exception rests
// on the verb contract pinned on ToolAttempt.IdempotencyKey: keys are
// deterministic per turn input, so the retried run re-derives the
// same key and the downstream service dedupes the repeat.
func safeToRetry(attempts []store.ToolAttempt) bool {
	for _, a := range attempts {
		if a.IdempotencyKey == "" {
			return false
		}
	}
	return true
}

// park moves the event to 'interrupted' and reports Parked. A park
// that cannot be written is an error, not a shrug: the event would
// otherwise strand in 'processing', invisible until the next restart.
func park(ctx context.Context, db *sql.DB, ev store.QueuedEvent, logger *slog.Logger, now func() time.Time, why string) (Outcome, error) {
	if err := store.ParkEvent(ctx, db, ev.ID, now().Unix()); err != nil {
		return 0, fmt.Errorf("recovery: park event %s: %w", ev.DedupKey, err)
	}
	logger.Warn("engine failure — event parked as interrupted (§4.6)",
		"thread_key", ev.ThreadKey, "dedup_key", ev.DedupKey, "reason", why)
	return Parked, nil
}
