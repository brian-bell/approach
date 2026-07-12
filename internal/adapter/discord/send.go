package discord

import (
	"context"
	"fmt"
	"strings"

	"github.com/bwmarrin/discordgo"
)

// Send delivers one outbound message to a §6 discord thread key and
// returns the platform ack — the sent message's id, spelled
// discord:msg:<id> like every other message key this adapter mints,
// so a delivery ack correlates with future replies. The deliveries
// outbox (C4) advances an event to replied only on this ack (§4.1):
// a REST error propagates, and a "successful" response carrying no
// message id is an error too — an ack-less accept must never read as
// delivered.
//
// No retry lives here: the outbox owns at-least-once redelivery
// policy; Send reports one attempt's truth. Errors name the thread
// key, never the message content (§7 — the journal is not the
// platform).
func (a *Adapter) Send(ctx context.Context, threadKey, text string) (string, error) {
	if err := ctx.Err(); err != nil {
		return "", fmt.Errorf("discord: send to %s: %w", threadKey, err)
	}
	channelID, err := a.resolveSendChannel(ctx, threadKey)
	if err != nil {
		return "", err
	}
	msg, err := a.sendMessage(ctx, a.currentSession(), channelID, text)
	if err != nil {
		a.invalidateDMChannel(threadKey)
		return "", fmt.Errorf("discord: send to %s: %w", threadKey, err)
	}
	if msg == nil || msg.ID == "" {
		return "", fmt.Errorf("discord: send to %s: platform accepted without a message id — no honest ack to record", threadKey)
	}
	return "discord:msg:" + msg.ID, nil
}

// resolveSendChannel maps a §6 thread key back to the platform
// channel to post in. Only keys this adapter itself mints are
// accepted — discord:dm:<user_id> (resolved to the user's DM channel,
// cached after the first REST round-trip) and
// discord:thread:<thread_id> (used directly). Anything else is
// refused before any REST call: a mis-keyed delivery must fail, never
// land in a channel it was not addressed to.
func (a *Adapter) resolveSendChannel(ctx context.Context, threadKey string) (string, error) {
	if threadID, ok := strings.CutPrefix(threadKey, "discord:thread:"); ok && threadID != "" {
		return threadID, nil
	}
	userID, ok := strings.CutPrefix(threadKey, "discord:dm:")
	if !ok || userID == "" {
		return "", fmt.Errorf("discord: send: %q is not a discord thread key this adapter mints", threadKey)
	}

	a.dmGate.Lock()
	channelID, hit := a.dmChannels[userID]
	a.dmGate.Unlock()
	if hit {
		return channelID, nil
	}
	// UserChannelCreate is idempotent (it returns the existing DM
	// channel), but it is still a REST round-trip — cache on success
	// only, so a failed resolution is retried next send instead of
	// poisoning the map.
	ch, err := a.createDMChannel(ctx, a.currentSession(), userID)
	if err != nil {
		return "", fmt.Errorf("discord: send: resolving DM channel for %s: %w", threadKey, err)
	}
	if ch == nil || ch.ID == "" {
		return "", fmt.Errorf("discord: send: platform returned no DM channel for %s", threadKey)
	}
	a.dmGate.Lock()
	a.dmChannels[userID] = ch.ID
	a.dmGate.Unlock()
	return ch.ID, nil
}

// invalidateDMChannel drops the cached user→channel mapping behind a
// dm thread key after a send failure: the platform can re-mint a DM
// channel (the reason the §6 dm key holds the USER id), and a
// permanently cached stale id would wedge every retry — outbox resend
// or relayed turn alike — until restart. Invalidating on ANY error
// over-forgets on transients, which only costs the retry one
// idempotent UserChannelCreate. Every outbound path (Send, Relay)
// must route its send failures through here. No-op for non-DM keys.
func (a *Adapter) invalidateDMChannel(threadKey string) {
	userID, ok := strings.CutPrefix(threadKey, "discord:dm:")
	if !ok {
		return
	}
	a.dmGate.Lock()
	delete(a.dmChannels, userID)
	a.dmGate.Unlock()
}

// currentSession snapshots the session pointer under the gate: Send
// runs on the caller's goroutine while Run's goroutine may be
// rebuilding the session (resetSession after 4007/4009). REST calls
// only need the token, which every generation carries, so a snapshot
// taken just before a swap is still valid — the gate exists to make
// the read itself race-free, not to fence generations.
func (a *Adapter) currentSession() *discordgo.Session {
	a.sessionGate.Lock()
	defer a.sessionGate.Unlock()
	return a.session
}
