package discord

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"
	"unicode/utf8"
)

// Discord platform constants the relay paces itself against.
const (
	// messageCap is Discord's hard per-message content limit.
	messageCap = 2000
	// typingIntervalDefault refreshes the typing indicator, which the
	// platform expires after ~10s.
	typingIntervalDefault = 8 * time.Second
	// editIntervalDefault throttles partial-message edits well under
	// the ~5-per-5s per-channel edit bucket.
	editIntervalDefault = 1500 * time.Millisecond
	// partialMinDefault is how much text must accumulate before a
	// partial message is worth showing — a handful of characters
	// flickers more than it informs.
	partialMinDefault = 200
)

// Relay streams one turn's incremental engine output to a channel as
// typing + a progressively edited partial message, finishing with the
// complete text under Send's ack semantics (§4.1). One Relay per
// outbound turn; NewRelay to construct. Push/Finish/Cancel are
// goroutine-safe. Partial UX is best-effort — a failed partial send
// or edit never fails the turn; the FINAL delivery at Finish is the
// durable leg, and only its errors surface.
type Relay struct {
	a         *Adapter
	ctx       context.Context
	threadKey string

	typingInterval time.Duration
	editInterval   time.Duration
	partialMin     int

	mu           sync.Mutex
	buf          strings.Builder
	channelID    string // resolved on first need; "" until then
	partialID    string // the message being edited, once one exists
	posted       bool   // a partial message REACHED the platform — sticky (§4.6 evidence)
	partialAtCap bool   // partial shows the full cap; edits are no-ops now
	lastEdit     time.Time
	done         bool // Finish or Cancel happened
	typingStop   chan struct{}
	typingEnd    chan struct{}
}

// NewRelay opens a streaming turn toward threadKey. The context
// governs every platform call the relay makes; cancelling it (drain)
// silences the relay without needing Cancel.
func (a *Adapter) NewRelay(ctx context.Context, threadKey string) *Relay {
	return &Relay{
		a:              a,
		ctx:            ctx,
		threadKey:      threadKey,
		typingInterval: typingIntervalDefault,
		editInterval:   editIntervalDefault,
		partialMin:     partialMinDefault,
	}
}

// Push appends one delta of engine output. It may block briefly on a
// platform call (partial send/edit) — the stream-json reader tolerates
// that, and serializing here keeps message order trivially right.
// Platform failures inside Push are recorded as UX loss only, never
// surfaced: Finish re-delivers the full text regardless.
func (r *Relay) Push(delta string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.done || r.ctx.Err() != nil {
		// Drained: nothing may reach the platform. The delta is not
		// even buffered — Finish under this context refuses anyway,
		// and the outbox will re-deliver the full turn after restart.
		return
	}
	r.buf.WriteString(delta)

	if r.partialID == "" {
		// The threshold is a CHARACTER budget (like the platform cap),
		// not bytes — byte counting would trip it at half depth for
		// two-byte scripts.
		if utf8.RuneCountInString(r.buf.String()) < r.partialMin {
			r.ensureTypingLocked()
			return
		}
		// One throttle window for ALL partial traffic, failed creation
		// attempts included: retrying a failed send on every stream
		// delta is a REST storm against the same rate bucket the final
		// delivery needs.
		if !r.lastEdit.IsZero() && time.Since(r.lastEdit) < r.editInterval {
			return
		}
		// Crossing the threshold: replace the typing indicator with a
		// real partial message.
		if r.resolveChannelLocked() != nil {
			r.lastEdit = time.Now() // failed resolution is throttled traffic too
			return
		}
		msg, err := r.a.sendMessage(r.ctx, r.a.currentSession(), r.channelID, r.partialContentLocked())
		if err != nil || msg == nil || msg.ID == "" {
			r.invalidateChannelLocked()
			r.logPartialLoss("partial send", err)
			r.lastEdit = time.Now()
			return
		}
		r.partialID = msg.ID
		r.posted = true
		r.lastEdit = time.Now()
		r.stopTypingLocked()
		return
	}
	if r.partialAtCap || time.Since(r.lastEdit) < r.editInterval {
		return
	}
	if _, err := r.a.editMessage(r.ctx, r.a.currentSession(), r.channelID, r.partialID, r.partialContentLocked()); err != nil {
		r.invalidateChannelLocked()
		r.logPartialLoss("partial edit", err)
	}
	// Advance the window even on failure: hammering a failing edit
	// endpoint at push frequency is exactly what the throttle exists
	// to prevent.
	r.lastEdit = time.Now()
}

// invalidateChannelLocked drops the channel resolution in BOTH caches
// after a platform failure: the adapter's user→channel map (so the
// next turn re-resolves) and the relay's own copy (so THIS turn's
// later partials and its durable Finish leg re-resolve too, instead
// of riding a re-minted channel to the end of the turn). A cleared
// partialID goes with it — a partial message in a dead channel cannot
// be edited into the final delivery.
func (r *Relay) invalidateChannelLocked() {
	r.a.invalidateDMChannel(r.threadKey)
	if strings.HasPrefix(r.threadKey, "discord:dm:") {
		r.channelID = ""
		r.partialID = ""
		// The keepalive captured the now-dead channel id — typing
		// into it until the turn ends is pure waste; a later
		// below-threshold push restarts it against the re-resolved
		// channel.
		r.stopTypingLocked()
	}
}

// partialContentLocked is what the partial message may show: the
// first messageCap runes. An over-cap send or edit would be rejected
// by the platform exactly when a long answer needs the partial UX
// most; once the visible prefix stops changing (buffer past the cap)
// further edits are pointless and partialAtCap shuts them off — the
// overflow arrives as Finish chunks.
func (r *Relay) partialContentLocked() string {
	runes := []rune(r.buf.String())
	if len(runes) < messageCap {
		return string(runes)
	}
	r.partialAtCap = true
	return string(runes[:messageCap])
}

// Finish delivers the complete accumulated text and returns the
// platform acks — one per message the platform accepted, in order.
// The partial message (edited to final content) contributes its own
// ack; text past the 2000-char cap continues in fresh messages. On a
// platform error the acks already earned are returned WITH the error:
// the outbox must know both what landed and that the turn is not
// fully delivered.
func (r *Relay) Finish() ([]string, error) {
	return r.finish(nil)
}

// FinishJournaled is Finish with a durable-attempt boundary. It calls
// beforeSend immediately before every final edit or fresh send for a
// chunk, and aborts without touching the platform when that journal
// write fails. A failed edit followed by a fresh-send fallback invokes
// the callback twice for chunk 0 because two platform operations
// actually started (§4.6).
func (r *Relay) FinishJournaled(beforeSend func(chunkIndex int) error) ([]string, error) {
	if beforeSend == nil {
		return nil, fmt.Errorf("discord: relay for %s: nil attempt journal", r.threadKey)
	}
	return r.finish(beforeSend)
}

func (r *Relay) finish(beforeSend func(chunkIndex int) error) ([]string, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.done {
		return nil, fmt.Errorf("discord: relay for %s already finished or cancelled", r.threadKey)
	}
	r.done = true
	r.stopTypingLocked()

	// Cancellation is checked before the empty-turn shortcut: a
	// drained relay DROPS pushes, so an empty buffer here may mean
	// "everything was dropped", and a nil return would let the outbox
	// record a dead turn as silently delivered.
	if err := r.ctx.Err(); err != nil {
		return nil, fmt.Errorf("discord: relay for %s: drained before delivery: %w", r.threadKey, err)
	}
	text := r.buf.String()
	if text == "" && r.partialID == "" {
		return nil, nil
	}
	if err := r.resolveChannelLocked(); err != nil {
		return nil, fmt.Errorf("discord: relay for %s: %w", r.threadKey, err)
	}

	chunks := chunkRunes(text, messageCap)
	var acks []string
	rest := chunks
	restStart := 0
	if r.partialID != "" && len(chunks) > 0 {
		// The partial message becomes the final first chunk. If the
		// edit fails, fall through to a fresh send of the same chunk —
		// a stale partial plus a complete message beats a truncated
		// turn.
		if beforeSend != nil {
			if err := beforeSend(0); err != nil {
				return acks, fmt.Errorf("discord: relay finish to %s: journal chunk 0 before final edit: %w", r.threadKey, err)
			}
		}
		if _, err := r.a.editMessage(r.ctx, r.a.currentSession(), r.channelID, r.partialID, chunks[0]); err == nil {
			acks = append(acks, "discord:msg:"+r.partialID)
			rest = chunks[1:]
			restStart = 1
		} else {
			r.invalidateChannelLocked()
			r.logPartialLoss("final edit", err)
			// The channel may have been re-minted out from under the
			// partial — re-resolve so the fresh sends below reach the
			// live channel instead of riding the dead one.
			if rerr := r.resolveChannelLocked(); rerr != nil {
				return acks, fmt.Errorf("discord: relay finish to %s: %w", r.threadKey, rerr)
			}
		}
	}
	for i, chunk := range rest {
		chunkIndex := restStart + i
		if beforeSend != nil {
			if err := beforeSend(chunkIndex); err != nil {
				return acks, fmt.Errorf("discord: relay finish to %s: journal chunk %d before send: %w", r.threadKey, chunkIndex, err)
			}
		}
		msg, err := r.a.sendMessage(r.ctx, r.a.currentSession(), r.channelID, chunk)
		if err != nil {
			r.invalidateChannelLocked()
			return acks, fmt.Errorf("discord: relay finish to %s: %w", r.threadKey, err)
		}
		if msg == nil || msg.ID == "" {
			return acks, fmt.Errorf("discord: relay finish to %s: platform accepted without a message id — no honest ack to record", r.threadKey)
		}
		acks = append(acks, "discord:msg:"+msg.ID)
	}
	return acks, nil
}

// Posted reports whether this relay put anything VISIBLE on the
// platform — a partial message was created, whether or not it can
// still be edited. Sticky on purpose: channel invalidation clears the
// edit target (partialID), not the fact that a human saw text. The
// turn wiring consults this on engine failure — a visible partial is
// an outbound side effect the tool journal knows nothing about, and a
// turn that showed one must never silently auto-retry (§4.6).
func (r *Relay) Posted() bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.posted
}

// Cancel abandons the turn: typing stops, nothing further is sent.
// Idempotent; Finish after Cancel is refused.
func (r *Relay) Cancel() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.done = true
	r.stopTypingLocked()
}

// Retract is Cancel plus best-effort removal of the posted partial
// message — for the paths where the SAME text is guaranteed to arrive
// again through the outbox pump (a deferred send): leaving the partial
// standing would show the reply twice, once as an abandoned fragment.
// Best-effort on purpose: a failed delete only costs UX (the duplicate
// the caller was avoiding), never turn state — and Posted stays sticky
// either way, because a deleted message may still have been read
// (§4.6: retraction is not un-seeing).
func (r *Relay) Retract() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.done = true
	r.stopTypingLocked()
	if r.partialID == "" || r.channelID == "" {
		return
	}
	if err := r.a.deleteMessage(r.ctx, r.a.currentSession(), r.channelID, r.partialID); err != nil {
		r.logPartialLoss("partial retract", err)
	}
	r.partialID = ""
}

// resolveChannelLocked resolves and caches the platform channel for
// the thread key, returning the REAL cause on failure — Finish wraps
// it for the outbox's diagnosis; partial paths log and move on.
func (r *Relay) resolveChannelLocked() error {
	if r.channelID != "" {
		return nil
	}
	id, err := r.a.resolveSendChannel(r.ctx, r.threadKey)
	if err != nil {
		r.a.log.Warn("discord relay cannot resolve channel", "thread_key", r.threadKey, "error", err.Error())
		return err
	}
	r.channelID = id
	return nil
}

// ensureTypingLocked starts the keepalive goroutine once: the typing
// indicator expires after ~10s, so it refreshes every typingInterval
// until a partial message exists or the turn ends. Typing is pure
// cosmetics — failures are Debug noise, never turn state.
func (r *Relay) ensureTypingLocked() {
	if r.typingStop != nil {
		return
	}
	// Resolution here is platform traffic like any partial attempt —
	// a failing DM resolution must not retry once per engine delta
	// during an outage, so it shares the same throttle window.
	if !r.lastEdit.IsZero() && time.Since(r.lastEdit) < r.editInterval {
		return
	}
	if r.resolveChannelLocked() != nil {
		r.lastEdit = time.Now()
		return
	}
	r.typingStop = make(chan struct{})
	r.typingEnd = make(chan struct{})
	stop, end := r.typingStop, r.typingEnd
	channelID := r.channelID
	go func() {
		defer close(end)
		ticker := time.NewTicker(r.typingInterval)
		defer ticker.Stop()
		for {
			// Cancellation wins over an armed tick: a drained relay
			// must not spend one more platform call on cosmetics.
			if r.ctx.Err() != nil {
				return
			}
			if err := r.a.typing(r.ctx, r.a.currentSession(), channelID); err != nil {
				r.a.log.Debug("discord typing refresh failed", "thread_key", r.threadKey, "error", err.Error())
			}
			select {
			case <-stop:
				return
			case <-r.ctx.Done():
				return
			case <-ticker.C:
			}
		}
	}()
}

// stopTypingLocked stops the keepalive and waits it out, so no typing
// call can land after the relay has gone quiet.
func (r *Relay) stopTypingLocked() {
	if r.typingStop == nil {
		return
	}
	close(r.typingStop)
	<-r.typingEnd
	r.typingStop, r.typingEnd = nil, nil
}

func (r *Relay) logPartialLoss(stage string, err error) {
	detail := "platform accepted without a message id"
	if err != nil {
		detail = err.Error()
	}
	r.a.log.Warn("discord relay "+stage+" failed — partial UX degraded, final delivery unaffected",
		"thread_key", r.threadKey, "error", detail)
}

// ChunkMessage splits a reply into the exact per-message pieces this
// adapter will send for it — THE definition the outbox composer keys
// delivery rows off (§4.1: one row per outbound message), so the acks
// Relay.Finish returns align one-to-one with the rows the caller wrote.
// A second, drifting chunker would misalign acks and rows silently.
func ChunkMessage(text string) []string {
	return chunkRunes(text, messageCap)
}

// chunkRunes splits text into <=capRunes-rune pieces on rune
// boundaries — a byte split would corrupt multi-byte UTF-8 and
// Discord counts characters, not bytes.
func chunkRunes(text string, capRunes int) []string {
	if text == "" {
		return nil
	}
	runes := []rune(text)
	var chunks []string
	for len(runes) > 0 {
		n := min(len(runes), capRunes)
		chunks = append(chunks, string(runes[:n]))
		runes = runes[n:]
	}
	return chunks
}
