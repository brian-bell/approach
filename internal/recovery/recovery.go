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

	"github.com/brian-bell/approach/internal/delivery"
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
	// Dead: the event is durably dead-lettered — the machine gave up
	// (§4.6) and the manual drain owns it now.
	Dead
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
	// Notify, when set, is called after a park's §4.6 notice lands in
	// the outbox — the daemon wires it to the outbox pump's kick so
	// the notice posts now instead of on the next ticker pass. Must be
	// non-blocking; nil means the pump's ticker is the delivery bound.
	Notify func()
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
		return park(ctx, db, ev, opts,
			"side-effecting attempt without idempotency key — ambiguous, parked (§4.6)")
	}

	if err := store.RequeueEventForRetry(ctx, db, ev.ID, now().Unix()); err != nil {
		if errors.Is(err, store.ErrRetryBudgetExhausted) {
			return deadLetter(ctx, db, ev, opts)
		}
		if errors.Is(err, store.ErrSideEffectingAttempt) {
			// The judgment above went stale — a straggling PreToolUse
			// journalled an attempt in the gap. The transition's atomic
			// re-check caught it; ambiguity parks.
			return park(ctx, db, ev, opts,
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

// deadLetter is the terminal landing for an exhausted budget (§4.6):
// event dead + dead_letters row atomically, owner-DM entry notice
// through the outbox, pump woken. If the dead-letter write itself
// fails the event FALLS BACK to a park — between states is the one
// place an event must never be, and interrupted is still durable,
// human-visible, and manually retriable.
func deadLetter(ctx context.Context, db *sql.DB, ev store.QueuedEvent, opts Options) (Outcome, error) {
	now := opts.Now
	if now == nil {
		now = time.Now
	}
	if err := store.DeadLetterEvent(ctx, db, ev.ID, "retries-exhausted", now().Unix()); err != nil {
		opts.Logger.Error("dead-letter write failed — falling back to park so the event stays visible (§4.6)",
			"dedup_key", ev.DedupKey, "error", err.Error())
		return park(ctx, db, ev, opts,
			"retry budget exhausted; dead-letter write failed — parked instead (§4.6)")
	}
	if err := delivery.SurfaceDeadLetter(ctx, db, ev); err != nil {
		opts.Logger.Error("dead-letter entry notice failed — the pump's sweep repairs it next pass (§4.6)",
			"dedup_key", ev.DedupKey, "error", err.Error())
	} else if opts.Notify != nil {
		opts.Notify()
	}
	opts.Logger.Warn("retry budget exhausted — event dead-lettered; a human decides (§4.6)",
		"thread_key", ev.ThreadKey, "dedup_key", ev.DedupKey)
	return Dead, nil
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

// park moves the event to 'interrupted', surfaces the §4.6 notice to
// the originating thread through the outbox, and reports Parked. A
// park that cannot be written is an error, not a shrug: the event
// would otherwise strand in 'processing', invisible until the next
// restart. A surface that cannot be written only logs — the park
// already committed, and §4.6 routes a failed surface to the
// dead-letter flow (x6n.3.6), never to a failed recovery.
func park(ctx context.Context, db *sql.DB, ev store.QueuedEvent, opts Options, why string) (Outcome, error) {
	now := opts.Now
	if now == nil {
		now = time.Now
	}
	if err := store.ParkEvent(ctx, db, ev.ID, now().Unix()); err != nil {
		return 0, fmt.Errorf("recovery: park event %s: %w", ev.DedupKey, err)
	}
	if err := delivery.SurfaceInterrupted(ctx, db, ev); err != nil {
		opts.Logger.Error("park notice write failed — the pump's unsurfaced-park sweep repairs it next pass (§4.6)",
			"dedup_key", ev.DedupKey, "error", err.Error())
	} else if opts.Notify != nil {
		// Wake the outbox pump so the notice posts now, not on the
		// next ticker pass.
		opts.Notify()
	}
	opts.Logger.Warn("engine failure — event parked as interrupted (§4.6)",
		"thread_key", ev.ThreadKey, "dedup_key", ev.DedupKey, "reason", why)
	return Parked, nil
}
