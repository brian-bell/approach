package delivery

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/brian-bell/approach/internal/store"
)

// SurfaceDeadLetter writes the §4.6 entry notification for a death:
// one owner-DM through the outbox per ENTRY, keyed
// "dead:<dedup_key>:<entries>" — the death-generation counter, so a
// re-entered death (requeued, died again) gets a fresh notice while
// same-death surfaces collapse (counter-not-timestamp, same rule as
// parks). Key AND payload derive from the dead_letters row inside the
// INSERT itself; a resolved row composes nothing. Unbound event_id,
// same rationale as the park notice.
//
// The target is the enrolled owner's DM, derived from the identities
// table. The "discord:dm:<native_id>" spelling is the discord
// adapter's thread-key contract (§6) — centralized here until a second
// adapter forces a per-channel registry. No enrolled owner is an
// ERROR, not a skip: the caller logs loud and the pump's sweep retries
// — a death notice with nowhere to go must never quietly evaporate.
func SurfaceDeadLetter(ctx context.Context, db *sql.DB, ev store.QueuedEvent) error {
	return SurfaceDeadLetterCoordinated(ctx, db, ev, nil)
}

// SurfaceDeadLetterCoordinated is SurfaceDeadLetter under the same
// per-target composition gate a live relay holds. Once its owner-DM
// target is known, the notice waits until any live reply has composed
// and sent, preserving durable and visible order (§4.1).
func SurfaceDeadLetterCoordinated(ctx context.Context, db *sql.DB, ev store.QueuedEvent, claims *InFlight) error {
	var nativeID string
	err := db.QueryRowContext(ctx,
		`SELECT native_id FROM identities
		 WHERE channel = 'discord' AND trust = 'owner'
		 ORDER BY native_id LIMIT 1`,
	).Scan(&nativeID)
	if err != nil {
		return fmt.Errorf("delivery: surface dead letter %s: no enrolled discord owner to notify (§4.6): %w", ev.DedupKey, err)
	}
	target := "discord:dm:" + nativeID
	release, err := claims.AcquireTarget(ctx, target)
	if err != nil {
		return fmt.Errorf("delivery: surface dead letter %s: acquire target %s: %w", ev.DedupKey, target, err)
	}
	defer release()

	_, err = db.ExecContext(ctx,
		`INSERT INTO deliveries (delivery_key, target, payload)
		 SELECT 'dead:' || e.dedup_key || ':' || d.entries, ?,
		        'An event could not be processed and was dead-lettered (event ' || e.dedup_key
		          || ', reason: ' || d.reason || ') — the machine gave up; you decide (§4.6). '
		          || 'Run ''approach dead requeue ' || d.event_id
		          || ''' or ''approach dead discard ' || d.event_id || '''.'
		 FROM dead_letters d JOIN events e ON e.id = d.event_id
		 WHERE d.event_id = ? AND d.resolution IS NULL
		 ON CONFLICT(delivery_key) DO NOTHING`,
		target, ev.ID,
	)
	if err != nil {
		return fmt.Errorf("delivery: surface dead letter %s: %w", ev.DedupKey, err)
	}
	return nil
}

// SurfaceInterrupted writes the §4.6 park notice into the outbox: the
// daemon posts to the originating thread what it was doing and offers
// the retry. It composes and PERSISTS only — write-before-send means
// the live pump or the restart resend delivers it, which is exactly
// what makes crash parks work: Rebuild parks before any adapter is up,
// and the notice waits durably until one is.
//
// The delivery key is deterministic PER EPISODE — "interrupted:" +
// dedup_key + ":" + the park generation counter (events.parks, which
// every park increments) — so a repeated surface of one park (crash
// between park and notice, the pump's sweep) collapses to one
// notification, while a re-park after a failed manual retry is a new
// generation and gets a fresh notice: the first notification must
// never make a later park silent. A counter, not a timestamp: two
// parks can share a second, and a collided key would silence the
// second episode. The key derives from the event row inside the
// INSERT statement itself, so it always matches the generation the
// park actually wrote. An event that is no longer interrupted
// composes nothing (no park to announce); like the duplicate, that is
// a quiet no-op.
//
// Callers treat an error as log-loud, never fatal: the park itself
// already committed, and the pump's unsurfaced-park sweep repairs a
// lost notice on its next pass.
func SurfaceInterrupted(ctx context.Context, db *sql.DB, ev store.QueuedEvent) error {
	return SurfaceInterruptedCoordinated(ctx, db, ev, nil)
}

// SurfaceInterruptedCoordinated is SurfaceInterrupted under the
// shared per-target composition gate. It cannot insert a notice into
// the middle of a live turn that has already been granted relay
// eligibility (§4.1).
func SurfaceInterruptedCoordinated(ctx context.Context, db *sql.DB, ev store.QueuedEvent, claims *InFlight) error {
	payload := fmt.Sprintf(
		"A turn on this thread was interrupted mid-run (event %s) — its side effects are unknown, so it was not re-run automatically. Reply with a retry request or run `approach retry %d` to re-queue it (§4.6).",
		ev.DedupKey, ev.ID,
	)
	// DELIBERATELY unbound (no event_id): AckDelivery gates a bound
	// row's ack on the event's reply lifecycle, and a retried event is
	// back in received/processing when the notice's ack lands — a
	// bound notice would roll its ack back and re-send on every pump
	// pass. The episode key carries the correlation instead.
	release, err := claims.AcquireTarget(ctx, ev.ThreadKey)
	if err != nil {
		return fmt.Errorf("delivery: surface interrupted event %s: acquire target %s: %w", ev.DedupKey, ev.ThreadKey, err)
	}
	defer release()
	_, err = db.ExecContext(ctx,
		`INSERT INTO deliveries (delivery_key, target, payload)
		 SELECT 'interrupted:' || e.dedup_key || ':' || e.parks, ?, ?
		 FROM events e WHERE e.id = ? AND e.status = 'interrupted'
		 ON CONFLICT(delivery_key) DO NOTHING`,
		ev.ThreadKey, payload, ev.ID,
	)
	if err != nil {
		return fmt.Errorf("delivery: surface interrupted event %s: %w", ev.DedupKey, err)
	}
	return nil
}
