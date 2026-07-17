package cli

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/brian-bell/approach/internal/adapter/discord"
	"github.com/brian-bell/approach/internal/delivery"
	"github.com/brian-bell/approach/internal/event"
	"github.com/brian-bell/approach/internal/recovery"
	"github.com/brian-bell/approach/internal/router"
	"github.com/brian-bell/approach/internal/session"
	"github.com/brian-bell/approach/internal/store"
	"github.com/brian-bell/approach/internal/trust"
)

// turnRunner is the handler's view of the session manager (C4): one
// blocking call that resolves the thread's session, runs the engine,
// and streams assistant text to the request's Output sink. An
// interface so dispatch tests drive the handler without a child
// process.
type turnRunner interface {
	Turn(ctx context.Context, req session.TurnRequest) error
}

// turnRelay is one turn's live streaming surface toward its thread —
// typing + partials while the engine runs, the final send at Finish,
// Posted (the §4.6 evidence bit: whether anything visible reached the
// platform), and Retract, which additionally removes the posted
// partial — for defer paths where the pump is guaranteed to deliver
// the same text again and a standing partial would duplicate it.
// *discord.Relay satisfies it; tests fake it.
type turnRelay interface {
	Push(delta string)
	FinishJournaled(beforeSend func(chunkIndex int) error) ([]string, error)
	Cancel()
	Retract()
	Posted() bool
}

// turnDeps wires productionTurn. Every field is a seam: the store
// handle, the session manager, the per-turn relay mint (nil, or
// returning nil, means no live adapter — replies then drain through
// the outbox pump), the router re-entry for §4.6 retries, the pump
// kick, the assistant cwd policy (§8), and the injectable clock/timer
// the recovery flows use.
type turnDeps struct {
	db      *sql.DB
	runner  turnRunner
	relay   func(ctx context.Context, threadKey string) turnRelay
	readmit func(ev store.QueuedEvent)
	notify  func()
	after   func(d time.Duration, f func())
	// inflight fences the pump off rows this handler's live send owns
	// (nil in tests without a pump — every method is nil-safe).
	inflight *delivery.InFlight
	cwd      string
	logger   *slog.Logger
	now      func() time.Time
}

// readmitFunc adapts the daemon's readmit closure to the recovery
// package's Readmitter seam.
type readmitFunc func(ev store.QueuedEvent)

func (f readmitFunc) Readmit(ev store.QueuedEvent) { f(ev) }

// productionTurn is the real router handler (§4.1): one queued event
// in, one engine turn out, every exit a durable event state. The
// router has already stamped the row 'processing'; this handler owns
// everything from there to completed/replied (success), received
// (§4.6 clean retry), interrupted (ambiguous failure), or dead
// (malformed / budget exhausted).
func productionTurn(d turnDeps) router.Handler {
	return func(ctx context.Context, ev store.QueuedEvent) {
		// ADMISSION GATE, owner-only until C9/C10 land (§7, fail
		// closed): with no PreToolUse policy hook and no sandbox, an
		// engine turn runs with the CLI's own defaults — owner-grade
		// capability. The matrix's untrusted column denies nearly
		// everything (send_same_thread included — an untrusted turn
		// could not even legally reply), and its known column needs the
		// ask/gate machinery to mean anything; neither is enforceable
		// yet, so a sub-owner event must not reach the engine at all.
		// The refusal is durable and quiet-to-the-channel: 'skipped'
		// (consumed on purpose, nothing owed) plus a loud journal line
		// — a park or dead letter would let any stranger's DM flood the
		// owner with §4.6 notices.
		if ev.Trust != "owner" {
			d.logger.Warn("event refused — only owner-stamped events run engine turns until the C9 policy gate lands (§7)",
				"thread_key", ev.ThreadKey, "dedup_key", ev.DedupKey, "trust", ev.Trust, "kind", ev.Kind)
			if err := store.SkipEvent(ctx, d.db, ev.ID, d.now().Unix()); err != nil {
				d.logger.Error("skip write failed — event stays processing until restart recovery",
					"dedup_key", ev.DedupKey, "error", err.Error())
			}
			return
		}

		// The payload is the §6 contract the turn replays from. One
		// that cannot parse can never run — no retry fixes bytes — so
		// it dead-letters as malformed rather than burning §4.6 budget.
		var pe event.Event
		if err := json.Unmarshal([]byte(ev.Payload), &pe); err != nil {
			d.logger.Error("event payload unparseable — dead-lettering as malformed (§4.6)",
				"dedup_key", ev.DedupKey, "error", err.Error())
			deadLetterMalformed(ctx, d, ev)
			return
		}

		// The relay is minted per turn and is pure UX until Finish. Its
		// target gate serializes the eligibility check + engine + durable
		// compose/send against EVERY recovery/dead-letter composer for the
		// same destination. Without that reservation, a notice could land
		// after this point but before our reply row, making an already-seen
		// partial jump the now-older notice (§4.1). Existing backlog still
		// suppresses the relay; a failed gate/check fails toward the ordered
		// pump. A missing adapter costs only the typing indicator.
		var relay turnRelay
		var releaseTarget func()
		releaseRelayTarget := func() {
			if releaseTarget != nil {
				releaseTarget()
				releaseTarget = nil
			}
		}
		defer releaseRelayTarget()
		if d.relay != nil {
			var err error
			releaseTarget, err = d.inflight.AcquireTarget(ctx, ev.ThreadKey)
			if err != nil {
				d.logger.Error("target reservation failed — suppressing live relay for ordered pump delivery",
					"dedup_key", ev.DedupKey, "error", err.Error())
			} else {
				owed, oerr := store.HasOwedDeliveries(ctx, d.db, ev.ThreadKey)
				if oerr != nil {
					d.logger.Error("backlog check failed — suppressing live relay for ordered pump delivery",
						"dedup_key", ev.DedupKey, "error", oerr.Error())
				} else if owed {
					d.logger.Info("delivery backlog exists — suppressing live relay for order (§4.1)",
						"dedup_key", ev.DedupKey)
				} else {
					relay = d.relay(ctx, ev.ThreadKey)
				}
			}
			if relay == nil {
				releaseRelayTarget()
			}
		}

		// The reply accumulates handler-side while the same deltas
		// stream to the relay: Finish sends the relay's own buffer, so
		// both must see identical text — one Output sink feeds both.
		// The sink runs on the engine's stdout goroutine; the builder
		// is read only after Turn returns (the engine joins that
		// goroutine before returning on every non-killed path, and a
		// killed turn never reaches the success read below).
		var reply strings.Builder
		output := func(delta string) {
			if reply.Len() > 0 {
				reply.WriteString("\n\n")
				if relay != nil {
					relay.Push("\n\n")
				}
			}
			reply.WriteString(delta)
			if relay != nil {
				relay.Push(delta)
			}
		}

		err := d.runner.Turn(ctx, session.TurnRequest{
			ThreadKey: ev.ThreadKey,
			// Fresh sessions pin the event's stamped trust as their
			// floor. Floor normalizes anything outside the participant
			// set — including the daemon-only system levels, which the
			// sessions CHECK refuses — fail-closed to untrusted (§6).
			TrustFloor: string(trust.Floor(trust.Level(ev.Trust))),
			// Assistant sessions spawn from the APPROACH_HOME root
			// (§8); worker cwd policy arrives with task events (§4.5).
			Cwd:    d.cwd,
			Kind:   ev.Kind,
			Prompt: pe.Text,
			Output: output,
		})
		if err != nil {
			if relay != nil {
				relay.Cancel()
			}
			// Recovery may compose a notice to this same target. The
			// live relay is over, so release before entering that shared
			// composer path rather than self-deadlocking on the gate.
			releaseRelayTarget()
			// Shutdown is not this event's failure (router contract):
			// the row stays 'processing' and the next boot parks it as
			// interrupted (§4.6) — recovery under a dead context could
			// neither read the journal nor write a park anyway.
			if ctx.Err() != nil {
				d.logger.Info("turn interrupted by shutdown — event stays processing for restart recovery (§4.6)",
					"dedup_key", ev.DedupKey)
				return
			}
			d.logger.Error("engine turn failed — applying §4.6 recovery",
				"thread_key", ev.ThreadKey, "dedup_key", ev.DedupKey, "error", err.Error())
			opts := recovery.Options{
				Logger:   d.logger,
				Now:      d.now,
				After:    d.after,
				Notify:   d.notify,
				InFlight: d.inflight,
			}
			// A relay that already POSTED a partial message put this
			// turn's side effect on the thread where everyone can see
			// it — the tool journal (recovery's evidence) knows nothing
			// about outbound partials, so the ambiguity is asserted
			// here: an auto-retry would leave the abandoned fragment
			// standing and answer a second time (§4.6 — retry only what
			// provably did nothing).
			if relay != nil && relay.Posted() {
				if _, perr := recovery.ParkAmbiguous(ctx, d.db, ev, opts,
					"partial reply already visible on the thread — a retry would answer twice (§4.6)"); perr != nil {
					d.logger.Error("park failed — event stranded in processing until restart recovery (§4.6)",
						"dedup_key", ev.DedupKey, "error", perr.Error())
				}
				return
			}
			if _, rerr := recovery.HandleEngineFailure(ctx, d.db, readmitFunc(d.readmit), ev, opts); rerr != nil {
				// The event stays 'processing' — invisible to the live
				// queue but parked by the next restart's Rebuild. Loud:
				// this is the one exit without a fresh durable state.
				d.logger.Error("recovery failed — event stranded in processing until restart recovery (§4.6)",
					"dedup_key", ev.DedupKey, "error", rerr.Error())
			}
			return
		}

		finishTurn(ctx, d, ev, reply.String(), relay, releaseRelayTarget)
	}
}

// deadLetterMalformed is the unparseable-payload landing: event dead +
// dead_letters row, owner notified through the outbox. A failed
// dead-letter write leaves the row processing — the next restart parks
// it, which is still durable and human-visible.
func deadLetterMalformed(ctx context.Context, d turnDeps, ev store.QueuedEvent) {
	if err := store.DeadLetterEvent(ctx, d.db, ev.ID, "malformed", d.now().Unix()); err != nil {
		d.logger.Error("dead-letter write failed — event stays processing until restart recovery",
			"dedup_key", ev.DedupKey, "error", err.Error())
		return
	}
	if err := delivery.SurfaceDeadLetterCoordinated(ctx, d.db, ev, d.inflight); err != nil {
		d.logger.Error("dead-letter entry notice failed — the pump's sweep repairs it next pass (§4.6)",
			"dedup_key", ev.DedupKey, "error", err.Error())
		return
	}
	d.notify()
}

// finishTurn is the success leg: compose the reply into the outbox
// (write-before-send, §4.1), complete the event, then attempt the live
// send through the relay, acking what the platform accepted. Every
// store write runs under WithoutCancel — the turn HAPPENED, and a
// shutdown landing now must not lose its completion or its composed
// reply (same rule as the session activation write).
func finishTurn(ctx context.Context, d turnDeps, ev store.QueuedEvent, text string, relay turnRelay, releaseTarget func()) {
	wctx := context.WithoutCancel(ctx)

	if text == "" {
		if relay != nil {
			relay.Cancel()
		}
		if err := store.MarkEventCompleted(wctx, d.db, ev.ID, d.now().Unix()); err != nil {
			d.logger.Error("completion stamp failed — event stays processing until restart recovery",
				"dedup_key", ev.DedupKey, "error", err.Error())
			return
		}
		d.logger.Info("turn completed with no reply text", "thread_key", ev.ThreadKey, "dedup_key", ev.DedupKey)
		return
	}

	// One outbox row per platform message, keyed reply:<dedup>:<chunk>.
	// ChunkMessage is THE chunker Relay.Finish sends by, so the acks it
	// returns align one-to-one with these rows. Deterministic keys make
	// a crash-repeated compose collapse to the first write (§4.1); a
	// collapsed duplicate means a prior life already owns this reply —
	// the direct send is skipped and the pump drains whatever is owed.
	chunks := discord.ChunkMessage(text)
	keys := make([]string, len(chunks))
	for i := range chunks {
		keys[i] = fmt.Sprintf("reply:%s:%d", ev.DedupKey, i)
	}
	// The claim lands BEFORE the first row does: from the instant a row
	// is durable, a pump pass (ticker, or another turn's kick) may scan
	// it — and a pump send racing the relay's would deliver it twice
	// from one living daemon. Released on every exit, and explicitly
	// before each pump kick so the kicked pass can actually drain what
	// this turn leaves owed.
	d.inflight.Claim(keys...)
	released := false
	release := func() {
		if !released {
			released = true
			d.inflight.Release(keys...)
		}
	}
	defer release()
	rows := make([]store.Delivery, len(chunks))
	for i, chunk := range chunks {
		rows[i] = store.Delivery{
			DeliveryKey: keys[i],
			EventID:     ev.ID,
			Target:      ev.ThreadKey,
			Payload:     chunk,
		}
	}
	// The whole reply composes in ONE transaction: all chunks or none.
	// A failed compose therefore leaves no truncated head owed, so the
	// event can PARK (§4.6) instead of completing with its reply lost —
	// interrupted is durable and human-visible, and a manual retry
	// re-runs the turn against a clean outbox (no stale prefix rows for
	// the fresh compose to collide with). Completing anyway would be a
	// silent drop wearing a success stamp: completed events are never
	// dispatched again, and nothing else would ever record the text.
	ids, composed, err := store.InsertDeliveries(wctx, d.db, rows)
	if err != nil {
		d.logger.Error("reply compose failed — parking the event so the reply is not silently lost (§4.6)",
			"dedup_key", ev.DedupKey, "error", err.Error())
		if relay != nil {
			relay.Cancel()
		}
		// ParkAmbiguous surfaces to this same target through the shared
		// composer gate. The live send is abandoned, so release before
		// entering recovery rather than waiting on our own reservation.
		releaseTarget()
		release()
		if _, perr := recovery.ParkAmbiguous(ctx, d.db, ev, recovery.Options{
			Logger: d.logger, Now: d.now, After: d.after, Notify: d.notify, InFlight: d.inflight,
		}, "turn ran but its reply could not be composed into the outbox (§4.6)"); perr != nil {
			d.logger.Error("park failed — event stranded in processing until restart recovery (§4.6)",
				"dedup_key", ev.DedupKey, "error", perr.Error())
		}
		return
	}
	// composed=false (no error) means a prior execution of this event
	// already composed its reply — the batch rolled back whole, and
	// sending the PRIOR compose is the pump's, in compose order. A
	// direct send is also refused while any OLDER row is still owed to
	// this target (a prior turn's unacked reply, a park notice): the
	// relay would deliver this thread's messages out of order (§4.1).
	// The fence protects only against rows composed AFTER ours — this
	// check is the other half. A failed check fails toward the pump's
	// ordering, never toward a maybe-out-of-order send.
	direct := relay != nil && composed
	if direct {
		owed, oerr := store.OwedDeliveriesBefore(wctx, d.db, ev.ThreadKey, ids[0])
		if oerr != nil {
			d.logger.Error("owed-order check failed — deferring the send to the pump",
				"dedup_key", ev.DedupKey, "error", oerr.Error())
			direct = false
		} else if owed > 0 {
			d.logger.Info("older deliveries still owed to this thread — deferring to the pump for order (§4.1)",
				"dedup_key", ev.DedupKey, "owed", owed)
			direct = false
		}
	}

	complete := store.MarkEventCompleted
	if !composed {
		// Duplicate compose means the prior reply rows may already have
		// been acked while this event was parked. Reconcile those acks in
		// the completion write; otherwise an all-acked event would rest at
		// completed forever because no future AckDelivery call remains.
		complete = store.MarkEventCompletedReconciled
	}
	if err := complete(wctx, d.db, ev.ID, d.now().Unix()); err != nil {
		// Should-never-happen store failure: the composed rows are owed
		// but must NOT reach the pump while the event sits pre-completion
		// — AckDelivery refuses an ack for a still-processing event
		// (reply leg outran the turn), so every pump pass would re-send
		// the same reply and roll its ack back: a duplicate per tick for
		// the rest of this life. The claims are deliberately KEPT (the
		// deferred release is disarmed): in-memory claims die with the
		// process, so the next boot parks the event (Rebuild) and the
		// pump delivers the reply exactly once, acks recording cleanly
		// against the parked row.
		d.logger.Error("completion stamp failed — reply fenced off the pump until restart recovery parks the event",
			"dedup_key", ev.DedupKey, "error", err.Error())
		released = true // disarm the deferred release; the fence outlives this turn on purpose
		if relay != nil {
			relay.Retract()
		}
		return
	}

	if !direct {
		// The pump will deliver this exact text from the composed rows
		// — a standing partial would show the reply twice, so it is
		// retracted, not just cancelled.
		if relay != nil {
			relay.Retract()
		}
		release()
		d.notify()
		return
	}

	// The relay owns the per-chunk platform loop, so it calls back at
	// the exact attempt boundary: immediately before each corresponding
	// edit/send. Later chunks stay unstamped when an earlier operation
	// fails or the process exits (§4.6 recovery must record only work
	// that provably started).
	acks, err := relay.FinishJournaled(func(chunkIndex int) error {
		if chunkIndex < 0 || chunkIndex >= len(ids) {
			return fmt.Errorf("relay requested attempt stamp for chunk %d of %d", chunkIndex, len(ids))
		}
		return store.MarkDeliveryAttempt(wctx, d.db, ids[chunkIndex], d.now().Unix())
	})
	for i := range acks {
		if i >= len(ids) {
			// More acks than rows would mean the relay chunked
			// differently than the composer — a drift bug worth a loud
			// record, though nothing durable is wrong (extra messages
			// were sent, all rows are settled).
			d.logger.Error("relay returned more acks than composed rows — chunker drift?",
				"dedup_key", ev.DedupKey, "acks", len(acks), "rows", len(ids))
			break
		}
		if aerr := store.AckDelivery(wctx, d.db, ids[i], d.now().Unix()); aerr != nil {
			d.logger.Error("platform accepted but ack write failed — row will re-send (duplicate accepted, §4.1)",
				"dedup_key", ev.DedupKey, "ack", acks[i], "error", aerr.Error())
		}
	}
	if err != nil || len(acks) < len(ids) {
		reason := "fewer acks than rows"
		if err != nil {
			reason = err.Error()
		}
		d.logger.Warn("reply partially delivered — remainder stays owed for the pump (§4.6)",
			"dedup_key", ev.DedupKey, "delivered", len(acks), "composed", len(ids), "error", reason)
		// Zero acks means NOTHING was delivered: the pump re-sends the
		// whole reply from chunk 0, and a standing partial would show
		// it twice — retract it. With at least one ack the first chunk
		// IS delivered content (possibly the partial edited into the
		// final message), and retracting would delete what the platform
		// already accepted.
		if len(acks) == 0 {
			relay.Retract()
		}
		release()
		d.notify()
		return
	}
	d.logger.Info("turn completed and reply delivered",
		"thread_key", ev.ThreadKey, "dedup_key", ev.DedupKey, "messages", len(acks))
}
