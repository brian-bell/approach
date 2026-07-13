package session

import (
	"context"
	"errors"
	"fmt"
	"os"

	"github.com/brian-bell/approach/internal/store"
)

// ErrCwdGone reports the §11 session-scope trap caught before it bit:
// the session's recorded cwd no longer exists (or is no longer a
// directory the daemon can stat). --resume lookups are scoped to the
// project dir, so spawning anyway would fail silently inside the
// engine; refusing here keeps the failure typed for the §4.6
// degradation flow (x6n.2.8) — resume_failed + fact-seeded fresh
// session + transparency note.
var ErrCwdGone = errors.New("session cwd no longer exists")

// Resume runs one turn of an ACTIVE session: claude -p --resume from
// the recorded sessions.cwd (§4.1, §6). Mirrors StartNew's discipline —
// the caller's snapshot only identifies the session; status and cwd
// are re-read from the row, the cwd is asserted on disk before the
// spawn (assert, don't assume — §11), and a dead context refuses
// rather than spawns. Engine failures propagate untyped here; telling
// transcript-gone from transient is x6n.2.8's classification. Bounding
// a hung resume (timeout kill) is the x6n.2.9 child-management remit.
func (m *Manager) Resume(ctx context.Context, live store.LiveSession) error {
	return m.resume(ctx, live, "")
}

// resume is Resume plus the event prompt the turn answers.
func (m *Manager) resume(ctx context.Context, live store.LiveSession, prompt string) error {
	current, ok, err := store.ResolveLiveSession(ctx, m.db, live.ThreadKey)
	if err != nil {
		return fmt.Errorf("session: resume %s: %w", live.SessionID, err)
	}
	if !ok || current.SessionID != live.SessionID || current.Status != "active" {
		return fmt.Errorf("session: resume %s refused — it is no longer %s's active session", live.SessionID, live.ThreadKey)
	}
	if err := assertCwd(current.SessionID, current.Cwd); err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("session: resume %s not started: %w", current.SessionID, err)
	}
	if err := m.engine.Resume(ctx, Spec{
		SessionID: current.SessionID,
		ThreadKey: current.ThreadKey,
		Cwd:       current.Cwd,
		Prompt:    prompt,
	}); err != nil {
		return fmt.Errorf("session: resume %s: %w", current.SessionID, err)
	}
	m.touch(ctx, current.SessionID)
	return nil
}

// assertCwd is the §11 pre-spawn assert: the recorded cwd must still
// exist as a directory. It is a stat, not a trust — the row's cwd was
// canonical and real at insert, but repos move and worktrees get
// pruned. Any stat failure — not just IsNotExist — reads as ErrCwdGone:
// a cwd the daemon cannot verify is one it must not spawn from (fail
// closed). Shared by resume, rotation, and /new so "cwd is gone" has
// exactly one definition.
func assertCwd(sessionID, cwd string) error {
	info, err := os.Stat(cwd)
	if err != nil {
		return fmt.Errorf("session: %s cwd %q: %v: %w", sessionID, cwd, err, ErrCwdGone)
	}
	if !info.IsDir() {
		return fmt.Errorf("session: %s recorded cwd %q is not a directory: %w", sessionID, cwd, ErrCwdGone)
	}
	return nil
}

// Turn is the unified per-event entry the queue handler calls: resolve
// the thread's session and run the right flow for its lifecycle state
// (§4.1). creating — fresh or a prior pin whose first turn failed
// transiently — owes a FIRST turn against the same pinned id (no
// transcript exists to resume; the expiry window, not the retry count,
// bounds how long the thread keeps trying). active resumes.
func (m *Manager) Turn(ctx context.Context, threadKey, trustFloor, cwd, prompt string) error {
	live, _, err := m.Ensure(ctx, threadKey, trustFloor, cwd)
	if err != nil {
		return fmt.Errorf("session: turn for %s: %w", threadKey, err)
	}
	// Cap checks come BEFORE the turn (§3): a session at its turn cap
	// or past its idle TTL rotates first, and this event lands on the
	// fresh successor — never one more turn on the retired transcript.
	if live.Status == "active" {
		if cause := m.rotationCause(live); cause != "" {
			// A rotation-due session whose recorded cwd has died cannot
			// rotate in place: the successor would inherit the dead cwd
			// and the insert's canonicalization would refuse it — every
			// later event repeating that failure is a wedged thread.
			// The transcript is unreachable anyway (--resume is
			// cwd-scoped), so this is the §4.6 resume-failure shape:
			// degrade to a successor at the caller's cwd.
			if cwdErr := assertCwd(live.SessionID, live.Cwd); cwdErr != nil {
				fresh, derr := m.degradeResumeFailed(ctx, live, cwd)
				if derr != nil {
					return fmt.Errorf("session: turn for %s: rotation-due cwd gone (%v) and degradation also failed: %w", threadKey, cwdErr, derr)
				}
				return m.startNew(ctx, fresh, resumeFailureNote, prompt)
			}
			live, err = m.rotate(ctx, live, cause)
			if err != nil {
				return fmt.Errorf("session: turn for %s: %w", threadKey, err)
			}
		}
	}
	switch live.Status {
	case "creating":
		// A creating row may be a §4.6 degradation successor whose
		// first turn failed transiently on an earlier event — the note
		// must survive that retry, so it is recovered from the durable
		// resume_failed predecessor link, never from in-memory context.
		note, nerr := m.degradationNote(ctx, live.SessionID)
		if nerr != nil {
			return fmt.Errorf("session: turn for %s: %w", threadKey, nerr)
		}
		return m.startNew(ctx, live, note, prompt)
	case "active":
		err := m.resume(ctx, live, prompt)
		if err == nil || !isResumeFailure(err) {
			return err
		}
		// The transcript is unrecoverable — degrade per §4.6 instead of
		// erroring: old row kept as resume_failed, and THIS event runs
		// as the fresh successor's first turn, its reply carrying the
		// transparency note. Amnesia-with-notes, never a hard failure.
		fresh, derr := m.degradeResumeFailed(ctx, live, cwd)
		if derr != nil {
			return fmt.Errorf("session: turn for %s: resume failed (%v) and degradation also failed: %w", threadKey, err, derr)
		}
		return m.startNew(ctx, fresh, resumeFailureNote, prompt)
	default:
		// ResolveLiveSession only returns the two live states; a third
		// here means the store contract broke — refuse loudly.
		return fmt.Errorf("session: turn for %s: live session %s in impossible state %q", threadKey, live.SessionID, live.Status)
	}
}
