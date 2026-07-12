package session

import (
	"context"
	"errors"
	"fmt"

	"github.com/brian-bell/approach/internal/store"
)

// ErrResumeFailed is the engine's report that a session's transcript
// is gone or unresumable (GC'd, corrupted — §4.6, §11): the resume
// itself cannot ever succeed, as opposed to a transient failure (rate
// limit, crash) that a later attempt might. The real engine (x6n.2.9)
// maps CLI behavior onto this sentinel; the §4.6 degradation below
// keys off it.
var ErrResumeFailed = errors.New("session transcript unresumable")

// resumeFailureNote is the §4.6 one-liner the degraded session's first
// reply must carry. Transcript loss degrades to amnesia-with-notes,
// never an error — facts in approach.db are the durable memory by
// design.
const resumeFailureNote = "lost this thread's conversation history — facts intact, starting fresh"

// isResumeFailure separates "this transcript will never resume" from
// "this attempt failed": only the former burns the session. ErrCwdGone
// (the §11 scope trap, asserted before the spawn) and ErrResumeFailed
// (the engine's own verdict) qualify; anything else propagates with
// the session intact for the event layer's §4.6 retry/interrupt logic.
func isResumeFailure(err error) bool {
	return errors.Is(err, ErrCwdGone) || errors.Is(err, ErrResumeFailed)
}

// degradeResumeFailed is the §4.6 resume-failure path: the old row is
// kept as resume_failed (a rotation cause — forensics, not deletion),
// a fresh session is minted in the same transaction, and the caller
// runs the pending event as the successor's first turn. The successor
// takes the CALLER's cwd — the config-current directory for this
// thread — not the recorded one: when the recorded cwd is the thing
// that died (§11), re-minting there would just fail again.
func (m *Manager) degradeResumeFailed(ctx context.Context, live store.LiveSession, cwd string) (store.LiveSession, error) {
	successor, err := m.mint(live.ThreadKey, live.TrustFloor, cwd, live.Origin)
	if err != nil {
		return store.LiveSession{}, fmt.Errorf("session: degrade %s: %w", live.SessionID, err)
	}
	canonical, err := store.ResumeFailSession(ctx, m.db, live.SessionID, successor)
	if err != nil {
		return store.LiveSession{}, fmt.Errorf("session: degrade %s: %w", live.SessionID, err)
	}
	m.logger.Warn("resume failed — session degraded to amnesia-with-notes (§4.6)",
		"thread_key", live.ThreadKey, "old_session_id", live.SessionID, "new_session_id", successor.SessionID)
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
