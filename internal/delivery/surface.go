package delivery

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/brian-bell/approach/internal/store"
)

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
	payload := fmt.Sprintf(
		"A turn on this thread was interrupted mid-run (event %s) — its side effects are unknown, so it was not re-run automatically. Reply with a retry request or run `approach retry %d` to re-queue it (§4.6).",
		ev.DedupKey, ev.ID,
	)
	// DELIBERATELY unbound (no event_id): AckDelivery gates a bound
	// row's ack on the event's reply lifecycle, and a retried event is
	// back in received/processing when the notice's ack lands — a
	// bound notice would roll its ack back and re-send on every pump
	// pass. The episode key carries the correlation instead.
	_, err := db.ExecContext(ctx,
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
