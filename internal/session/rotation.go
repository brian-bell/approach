package session

import (
	"context"
	"fmt"
	"time"

	"github.com/brian-bell/approach/internal/store"
)

// rotationCause names why an active session must rotate before its
// next turn (§3): "" means it doesn't. Idle is measured from the last
// completed turn; an active row that has somehow never been touched
// falls back to its creation instant — a session cannot be "fresh
// forever" just because its bookkeeping was lost.
func (m *Manager) rotationCause(live store.LiveSession) string {
	if live.Turns >= m.turnCap {
		return "turn_cap"
	}
	idleSince := live.LastSeen
	if idleSince == 0 {
		idleSince = live.CreatedAt
	}
	// Timestamps are floored whole seconds, so the computed idle span
	// overestimates the true one by up to a second (a turn at :00.9
	// stores :00; an event at :02.0 computes 2s idle when 1.1s truly
	// elapsed). Requiring one extra whole second beyond the TTL makes
	// "never rotate early" hold for ANY TTL, fractional included — and
	// an extra second of patience is invisible at the hours scale §3
	// intends these caps for.
	if time.Duration(m.now().Unix()-idleSince)*time.Second >= m.idleTTL+time.Second {
		return "idle_ttl"
	}
	return ""
}

// rotate retires the active session and mints its successor in one
// store transaction (§6: old→rotated + new→creating + rotated_to, all
// or nothing). Identity carries over from the DURABLE row — thread,
// trust floor, cwd; content seeding of the fresh session is the
// context assembler's job (C6), not ours.
func (m *Manager) rotate(ctx context.Context, live store.LiveSession, cause string) (store.LiveSession, error) {
	// Origin carries over too: a task:* worker's successor must keep
	// reporting to the thread that spawned the work (§4.5) — and the
	// schema-level validation rejects a worker row without one, which
	// would roll the whole rotation back.
	successor, err := m.mint(live.ThreadKey, live.TrustFloor, live.Cwd, live.Origin)
	if err != nil {
		return store.LiveSession{}, fmt.Errorf("session: rotate %s: %w", live.SessionID, err)
	}
	canonical, err := store.RotateSession(ctx, m.db, live.SessionID, successor)
	if err != nil {
		return store.LiveSession{}, fmt.Errorf("session: rotate %s: %w", live.SessionID, err)
	}
	m.logger.Info("session rotated", "thread_key", live.ThreadKey, "cause", cause,
		"old_session_id", live.SessionID, "new_session_id", successor.SessionID)
	return store.LiveSession{
		ThreadKey:          successor.ThreadKey,
		SessionID:          successor.SessionID,
		Status:             "creating",
		Cwd:                canonical,
		Origin:             successor.Origin,
		TrustFloor:         successor.TrustFloor,
		CreatedAt:          successor.CreatedAt,
		ActivationDeadline: successor.ActivationDeadline,
	}, nil
}

// RotateNow is the /new command path (§3): explicitly retire the
// thread's ACTIVE session and return its creating successor, whose
// first turn the next event will run. A thread with nothing active —
// empty, or a session still creating (it has no conversation to shed)
// — refuses loudly rather than minting sessions for effect.
func (m *Manager) RotateNow(ctx context.Context, threadKey string) (store.LiveSession, error) {
	live, ok, err := store.ResolveLiveSession(ctx, m.db, threadKey)
	if err != nil {
		return store.LiveSession{}, fmt.Errorf("session: rotate-now %s: %w", threadKey, err)
	}
	if !ok {
		return store.LiveSession{}, fmt.Errorf("session: rotate-now %s: no live session", threadKey)
	}
	// The store's ErrNotActive guard would catch a creating row too,
	// but refusing here avoids minting a successor UUID for a rotation
	// that cannot commit.
	if live.Status != "active" {
		return store.LiveSession{}, fmt.Errorf("session: rotate-now %s: session %s: %w", threadKey, live.SessionID, store.ErrNotActive)
	}
	return m.rotate(ctx, live, "new")
}
