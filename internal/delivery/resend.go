// Package delivery owns the outbound leg of the §4.1 reply contract:
// every outbound message is a deliveries outbox row written before the
// first send attempt, and this package drains what the store says is
// still owed. Today that is the §4.6 restart resend; the live-turn
// send path joins it with the epic's turn wiring.
package delivery

import (
	"context"
	"database/sql"
	"log/slog"
	"strings"
	"time"

	"github.com/brian-bell/approach/internal/store"
)

// Sender is one channel adapter's outbound surface (discord.Adapter
// satisfies it). Send delivers one message and returns the platform
// ack; an error is one attempt's failure — retry policy lives with the
// outbox, never the adapter.
type Sender interface {
	Send(ctx context.Context, target, payload string) (ack string, err error)
}

// ResendUnacked is the §4.6 restart resend: re-send every delivery row
// still owed a send from its persisted payload — at-least-once, so a
// crash after compose-but-before-ack duplicates a chat message rather
// than eating it. Rows process in compose (id) order, and a target
// whose send fails has its REMAINING rows skipped this pass: pressing
// on would deliver that thread's messages out of order, which is worse
// than late. Other targets keep flowing. Nothing here gives up on a
// row — failures stay pending for the next restart; budgets and
// dead-lettering are the §4.6 flows built on the attempts journal.
//
// Failures are per-row and never abort the pass (§4.6: events are
// never silently dropped — but a scan that stopped at the first bad
// row would silently strand the rest). Every skip is logged loud.
func ResendUnacked(ctx context.Context, db *sql.DB, senders map[string]Sender, logger *slog.Logger, now func() time.Time) {
	rows, err := store.ResendableDeliveries(ctx, db)
	if err != nil {
		logger.Error("resend scan failed — owed deliveries stay durable for the next restart", "error", err.Error())
		return
	}
	stopped := make(map[string]bool) // targets whose chain failed this pass
	for _, d := range rows {
		if ctx.Err() != nil {
			return // shutdown: everything left stays durably owed
		}
		if stopped[d.Target] {
			continue
		}
		sender, ok := senders[channelOf(d.Target)]
		if !ok {
			// No attempt stamp: no send was attempted, and consuming
			// §4.6 budget on an unroutable row would erode it before
			// the dead-letter flow that owns surfacing these exists.
			logger.Warn("delivery target has no live sender — row stays owed (§4.6)",
				"delivery_key", d.DeliveryKey, "target", d.Target)
			stopped[d.Target] = true
			continue
		}
		// The attempt is journalled BEFORE the send (§4.6: recovery
		// reasons from what provably started) — a row whose stamp
		// cannot be written must not send, or a crash loses the
		// attempt count the budget reads.
		if err := store.MarkDeliveryAttempt(ctx, db, d.ID, now().Unix()); err != nil {
			logger.Error("attempt stamp failed — send skipped, row stays owed",
				"delivery_key", d.DeliveryKey, "error", err.Error())
			stopped[d.Target] = true
			continue
		}
		ack, err := sender.Send(ctx, d.Target, d.Payload)
		if err != nil {
			logger.Warn("resend attempt refused — row stays owed for the next restart (§4.6)",
				"delivery_key", d.DeliveryKey, "target", d.Target, "error", err.Error())
			stopped[d.Target] = true
			continue
		}
		if err := store.AckDelivery(ctx, db, d.ID, now().Unix()); err != nil {
			// The platform accepted but the ack write failed: the row
			// stays owed and the NEXT pass re-sends — a duplicate
			// message, the §4.1 trade at-least-once accepts.
			logger.Error("platform accepted but ack write failed — row will re-send (duplicate accepted, §4.1)",
				"delivery_key", d.DeliveryKey, "ack", ack, "error", err.Error())
			stopped[d.Target] = true
			continue
		}
		logger.Info("owed delivery re-sent", "delivery_key", d.DeliveryKey, "target", d.Target, "ack", ack)
	}
}

// Pump keeps the outbox drained for the daemon's whole life: one
// drain at start (the §4.6 restart resend), then a drain per kick —
// the low-latency path for notices composed mid-life (a park's §4.6
// notice must post NOW, not on the next restart) — with a ticker as
// the safety net for any writer that forgets to kick. Kick sends
// should be non-blocking against a buffered channel; a kick that
// lands mid-drain coalesces into the next pass.
//
// Blocks until ctx is cancelled — run it on its own goroutine and
// wait for it before closing the store, exactly like the adapter.
func Pump(ctx context.Context, db *sql.DB, senders map[string]Sender, logger *slog.Logger, now func() time.Time, kick <-chan struct{}, interval time.Duration) {
	pass := func() {
		sweepUnsurfacedParks(ctx, db, logger)
		ResendUnacked(ctx, db, senders, logger, now)
	}
	pass()
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-kick:
		case <-ticker.C:
		}
		pass()
	}
}

// sweepUnsurfacedParks composes the missing §4.6 notice for every
// interrupted event that has none — the durable repair for a park
// whose surface write failed or whose daemon died between park and
// notice. Interrupted rows are outside every queue rescan by design,
// so without this sweep such a park is silent forever. Idempotent
// (deterministic notice key); failures log and wait for the next pass.
func sweepUnsurfacedParks(ctx context.Context, db *sql.DB, logger *slog.Logger) {
	events, err := store.UnsurfacedInterruptedEvents(ctx, db)
	if err != nil {
		logger.Error("unsurfaced-park sweep failed — parked events may be silent until the next pass", "error", err.Error())
		return
	}
	for _, ev := range events {
		if err := SurfaceInterrupted(ctx, db, ev); err != nil {
			logger.Error("re-surfacing parked event failed — next pass retries", "dedup_key", ev.DedupKey, "error", err.Error())
			continue
		}
		logger.Warn("parked event had no notice — composed by sweep (§4.6)", "dedup_key", ev.DedupKey)
	}
}

// channelOf extracts the channel segment of a §6 thread key — the
// prefix before the first ':' ("discord:dm:123" → "discord"). A key
// without a separator returns whole, matching no configured sender and
// failing toward "unroutable", never toward a wrong adapter.
func channelOf(target string) string {
	channel, _, _ := strings.Cut(target, ":")
	return channel
}
