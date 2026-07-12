// Package session owns the C4 session lifecycle (§4.1): pinning new
// sessions (the daemon mints the UUID and hands it to the engine via
// --session-id — never the reverse), the creating → active transition
// on a successful first turn, and the deadline expiry that retries a
// wedged creation fresh. The engine itself is an injected seam: child
// process management is x6n.2.9's remit, and tests drive these flows
// with a fake.
package session

import (
	"context"
	"crypto/rand"
	"database/sql"
	"fmt"
	"log/slog"
	"time"

	"github.com/brian-bell/approach/internal/store"
)

// Engine is the session manager's view of Claude Code. Start runs a
// session's FIRST turn — claude -p --session-id <pinned uuid> from
// spec.Cwd — and returns when the turn completes. Resume (--resume) is
// the x6n.2.6 flow and extends this seam there.
type Engine interface {
	Start(ctx context.Context, spec Spec) error
}

// Spec carries what a spawn needs. The SessionID is the daemon's
// pinned UUID (§4.1); Cwd is the session's recorded spawn dir (§6) —
// both come from the sessions row, never from engine output.
type Spec struct {
	SessionID string
	ThreadKey string
	Cwd       string
}

// Config wires a Manager. ActivationWindow bounds how long a session
// may sit in creating before the thread retries fresh (§4.1); Now is
// injectable so tests own the clock.
type Config struct {
	ActivationWindow time.Duration
	Logger           *slog.Logger
	Now              func() time.Time
}

// Manager drives session rows through their §4.1 lifecycle. One
// manager per daemon; its methods are called from per-thread queue
// goroutines, which serialize all calls for one thread_key — the
// concurrency assumption every flow below leans on (two racing
// Ensures for one thread cannot happen; the one_live_session index
// backstops even that, §6).
type Manager struct {
	db     *sql.DB
	engine Engine
	logger *slog.Logger
	now    func() time.Time
	window time.Duration
}

// NewManager builds a Manager over the store and engine seams.
func NewManager(db *sql.DB, engine Engine, cfg Config) *Manager {
	now := cfg.Now
	if now == nil {
		now = time.Now
	}
	return &Manager{
		db:     db,
		engine: engine,
		logger: cfg.Logger,
		now:    now,
		window: cfg.ActivationWindow,
	}
}

// Ensure resolves the thread's live session, minting one when the
// thread has none. fresh=true means the returned session is newly
// pinned and still owes its first engine turn (StartNew). Three cases
// (§4.1):
//
//   - live active, or live creating within its deadline → returned
//     as-is; never re-pin under a live row (one_live_session).
//   - live creating past its activation deadline → the spawn wedged:
//     mark it failed (kept for forensics), pin a fresh session.
//   - nothing live → pin a fresh session.
func (m *Manager) Ensure(ctx context.Context, threadKey, trustFloor, cwd string) (store.LiveSession, bool, error) {
	live, ok, err := store.ResolveLiveSession(ctx, m.db, threadKey)
	if err != nil {
		return store.LiveSession{}, false, fmt.Errorf("session: ensure %s: %w", threadKey, err)
	}
	if ok {
		if live.Status == "active" || m.now().Unix() < live.ActivationDeadline {
			return live, false, nil
		}
		// Creating past deadline: the engine never came up. Fail it and
		// fall through to a fresh pin. The guard makes a lost race loud
		// instead of clobbering an activation that landed after the
		// resolve above — impossible under per-thread serialization,
		// but this is a security-adjacent invariant, so belt-and-braces.
		if err := store.FailSession(ctx, m.db, live.SessionID); err != nil {
			return store.LiveSession{}, false, fmt.Errorf("session: expire creating %s: %w", live.SessionID, err)
		}
		m.logger.Warn("creating session passed its activation deadline — failed, retrying fresh (§4.1)",
			"thread_key", threadKey, "session_id", live.SessionID)
	}
	return m.pin(ctx, threadKey, trustFloor, cwd)
}

// pin mints the §4.1 new-session row: daemon-generated UUID, status
// creating (the schema default), activation deadline stamped in the
// same insert so a crash can never separate them.
func (m *Manager) pin(ctx context.Context, threadKey, trustFloor, cwd string) (store.LiveSession, bool, error) {
	id, err := newSessionID()
	if err != nil {
		return store.LiveSession{}, false, fmt.Errorf("session: pin %s: %w", threadKey, err)
	}
	now := m.now().Unix()
	s := store.Session{
		ThreadKey:          threadKey,
		SessionID:          id,
		Cwd:                cwd,
		TrustFloor:         trustFloor,
		CreatedAt:          now,
		ActivationDeadline: now + int64(m.window.Seconds()),
	}
	if err := store.InsertSession(ctx, m.db, s); err != nil {
		return store.LiveSession{}, false, fmt.Errorf("session: pin %s: %w", threadKey, err)
	}
	// Re-read rather than hand-assemble: the row is the truth, and the
	// store canonicalized the cwd on the way in (§6).
	live, ok, err := store.ResolveLiveSession(ctx, m.db, threadKey)
	if err != nil || !ok {
		return store.LiveSession{}, false, fmt.Errorf("session: pin %s: inserted row did not resolve (ok=%v): %w", threadKey, ok, err)
	}
	m.logger.Info("pinned new session", "thread_key", threadKey, "session_id", id)
	return live, true, nil
}

// StartNew runs the first engine turn for a freshly-pinned session and
// activates it on success (§4.1 creating → active). An engine failure
// leaves the row creating on purpose: the activation deadline — not
// one transient crash — decides when the thread gives up and retries
// fresh, so the failure is returned for the caller's §4.6 handling
// but the session keeps its window.
//
// The turn is BOUNDED by that same deadline: per-thread queues
// serialize behind this call, so a wedged first spawn would otherwise
// block its thread forever — no later event could even reach the
// Ensure that fails and replaces the expired row. The context deadline
// releases an engine that honors cancellation (force-kill of one that
// doesn't is the x6n.2.9 child-management remit); the activation guard
// below closes the other half — a turn that limps in after expiry must
// not activate a row Ensure is entitled to have failed already.
func (m *Manager) StartNew(ctx context.Context, live store.LiveSession) error {
	// Only the ENGINE runs under the deadline: the activation write
	// below is a local store update that must not be starved by a turn
	// that finished with seconds to spare.
	turnCtx, cancel := context.WithDeadline(ctx, time.Unix(live.ActivationDeadline, 0))
	defer cancel()
	if err := m.engine.Start(turnCtx, Spec{
		SessionID: live.SessionID,
		ThreadKey: live.ThreadKey,
		Cwd:       live.Cwd,
	}); err != nil {
		return fmt.Errorf("session: first turn for %s: %w", live.SessionID, err)
	}
	if m.now().Unix() >= live.ActivationDeadline {
		return fmt.Errorf("session: first turn for %s finished after its activation deadline — left creating for the §4.1 expiry retry", live.SessionID)
	}
	if err := store.ActivateSession(ctx, m.db, live.SessionID); err != nil {
		return fmt.Errorf("session: activate %s: %w", live.SessionID, err)
	}
	return nil
}

// newSessionID mints a version-4 UUID from crypto/rand. The daemon
// pins session identity (§4.1) — this is the mint. crypto/rand rather
// than math/rand is not paranoia: session ids appear in logs and
// filenames, and a guessable id is a needless capability.
func newSessionID() (string, error) {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", fmt.Errorf("mint session uuid: %w", err)
	}
	b[6] = (b[6] & 0x0f) | 0x40 // version 4
	b[8] = (b[8] & 0x3f) | 0x80 // RFC 4122 variant
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:16]), nil
}
