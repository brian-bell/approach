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
// spec.Cwd; Resume runs a later turn — claude -p --resume <id> from
// the same recorded cwd (§4.1). Both return when the turn completes.
// The real child-process implementation (spawn, timeout kill,
// --max-turns) is x6n.2.9.
type Engine interface {
	Start(ctx context.Context, spec Spec) error
	Resume(ctx context.Context, spec Spec) error
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
// injectable so tests own the clock. Every field has a safe default —
// a zero-value Config yields a working manager, never one that panics
// after mutating the store or mints rows the schema rejects.
type Config struct {
	ActivationWindow time.Duration // < 1s (incl. zero) → defaultActivationWindow
	IdleTTL          time.Duration // < 1s (incl. zero) → defaultIdleTTL; rotation trigger (§3)
	TurnCap          int64         // < 1 → defaultTurnCap; rotation trigger (§3)
	Logger           *slog.Logger  // nil → slog.Default()
	Now              func() time.Time
}

// defaultIdleTTL and defaultTurnCap mirror the config package's
// [sessions] defaults (§3) — a zero-value Config rotates on the same
// caps an unconfigured approach.toml would.
const (
	defaultIdleTTL = 4 * time.Hour
	defaultTurnCap = 50
)

// defaultActivationWindow is how long a creating session may wait for
// its first turn before the thread retries fresh (§4.1). Two minutes:
// generous against a slow model warm-up, short enough that a wedged
// spawn doesn't hold a thread hostage. Sub-second windows (including
// the zero value) are rejected in favor of this default — Seconds()
// truncation would stamp deadline == created_at, a born-expired row
// InsertSession refuses.
const defaultActivationWindow = 2 * time.Minute

// Manager drives session rows through their §4.1 lifecycle. One
// manager per daemon; its methods are called from per-thread queue
// goroutines, which serialize all calls for one thread_key — the
// concurrency assumption every flow below leans on (two racing
// Ensures for one thread cannot happen; the one_live_session index
// backstops even that, §6).
type Manager struct {
	db      *sql.DB
	engine  Engine
	logger  *slog.Logger
	now     func() time.Time
	window  time.Duration
	idleTTL time.Duration
	turnCap int64
}

// NewManager builds a Manager over the store and engine seams.
func NewManager(db *sql.DB, engine Engine, cfg Config) *Manager {
	now := cfg.Now
	if now == nil {
		now = time.Now
	}
	logger := cfg.Logger
	if logger == nil {
		logger = slog.Default()
	}
	window := cfg.ActivationWindow
	if window < time.Second {
		window = defaultActivationWindow
	}
	// Like ActivationWindow: sub-second values cannot be honored on
	// whole-second timestamps. config.Parse rejects them at load time
	// (fail loud); this clamp is the API-caller backstop, and a NONZERO
	// value being replaced is warned about, never silent — a zero value
	// simply means "unset, use the default".
	idleTTL := cfg.IdleTTL
	if idleTTL < time.Second {
		if idleTTL > 0 {
			logger.Warn("session idle TTL below 1s cannot be honored on whole-second timestamps — using the default",
				"configured", idleTTL.String(), "default", defaultIdleTTL.String())
		}
		idleTTL = defaultIdleTTL
	}
	turnCap := cfg.TurnCap
	if turnCap < 1 {
		turnCap = defaultTurnCap
	}
	return &Manager{
		db:      db,
		engine:  engine,
		logger:  logger,
		now:     now,
		window:  window,
		idleTTL: idleTTL,
		turnCap: turnCap,
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
	s, err := m.mint(threadKey, trustFloor, cwd, "")
	if err != nil {
		return store.LiveSession{}, false, fmt.Errorf("session: pin %s: %w", threadKey, err)
	}
	// The insert is the LAST fallible step: once it commits, this
	// method must report success — a read-back could fail (context
	// cancelled at shutdown) after the row committed, reporting a live
	// pin as unpinned and stranding the thread's first turn until the
	// deadline expiry. InsertSession returns the canonical cwd it
	// stored, so the row is reconstructed from what was written.
	canonical, err := store.InsertSession(ctx, m.db, s)
	if err != nil {
		return store.LiveSession{}, false, fmt.Errorf("session: pin %s: %w", threadKey, err)
	}
	m.logger.Info("pinned new session", "thread_key", threadKey, "session_id", s.SessionID)
	return store.LiveSession{
		ThreadKey:          threadKey,
		SessionID:          s.SessionID,
		Status:             "creating",
		Cwd:                canonical,
		TrustFloor:         trustFloor,
		CreatedAt:          s.CreatedAt,
		ActivationDeadline: s.ActivationDeadline,
	}, true, nil
}

// mint builds a fresh creating-session row: daemon-minted v4 UUID and
// a ceiled activation deadline. The deadline is CEILED to a whole Unix
// second: the schema stores seconds, and flooring a sub-second
// creation instant would shave up to a second off the window — at the
// 1s minimum, a session pinned at :00.999 would expire a millisecond
// later. Rounding up guarantees the row never expires before
// ActivationWindow has actually elapsed (§4.1).
func (m *Manager) mint(threadKey, trustFloor, cwd, origin string) (store.Session, error) {
	id, err := newSessionID()
	if err != nil {
		return store.Session{}, err
	}
	created := m.now()
	expiry := created.Add(m.window)
	deadline := expiry.Unix()
	if expiry.After(time.Unix(deadline, 0)) {
		deadline++
	}
	return store.Session{
		ThreadKey:          threadKey,
		SessionID:          id,
		Cwd:                cwd,
		Origin:             origin,
		TrustFloor:         trustFloor,
		CreatedAt:          created.Unix(),
		ActivationDeadline: deadline,
	}, nil
}

// touch records one completed turn's bookkeeping (§6) — the idle-TTL
// and turn-cap inputs. It runs under WithoutCancel (the turn HAPPENED;
// shutdown must not lose its count) and a failure is a loud log, not a
// turn failure: erroring a turn whose engine work succeeded would
// invite a replay of completed side effects (§4.6), which is strictly
// worse than a session outliving its caps by one turn.
func (m *Manager) touch(ctx context.Context, sessionID string) {
	if err := store.TouchSession(context.WithoutCancel(ctx), m.db, sessionID, m.now().Unix()); err != nil {
		m.logger.Error("turn bookkeeping failed — rotation caps may lag this session",
			"session_id", sessionID, "error", err.Error())
	}
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
	// The caller's snapshot identifies WHICH session; every durable
	// fact about it — status, deadline, cwd — is re-read from the row
	// before the spawn. Engine.Start is a side-effecting call, the one
	// class that must not lean on caller discipline: a stale snapshot
	// (duplicate call, replaced session) or a doctored one (later
	// deadline, different cwd) would otherwise run a whole unintended
	// agent turn — or run it outside the session's canonical directory
	// (§6) — before ActivateSession's guard could object.
	current, ok, err := store.ResolveLiveSession(ctx, m.db, live.ThreadKey)
	if err != nil {
		return fmt.Errorf("session: first turn for %s: %w", live.SessionID, err)
	}
	if !ok || current.SessionID != live.SessionID || current.Status != "creating" {
		return fmt.Errorf("session: first turn for %s refused — it is no longer %s's creating session", live.SessionID, live.ThreadKey)
	}
	// Refuse before spawning, not just after: queue delays can eat the
	// whole window between Ensure and here, and Engine has no contract
	// to be side-effect-free on an already-cancelled context — the only
	// spawn that provably does nothing is the one never started.
	// One clock read serves both the guard and the timeout below: a
	// second read could cross the deadline in between, and a zero
	// timeout is not a refusal — engines may do work under a cancelled
	// context. The remaining window is computed at full precision
	// against the whole-second persisted deadline: flooring `now`
	// would hand the engine up to an extra fractional second past the
	// instant Ensure already considers the row expired.
	remaining := time.Unix(current.ActivationDeadline, 0).Sub(m.now())
	if remaining <= 0 {
		return fmt.Errorf("session: first turn for %s not started — activation deadline already passed, row left for the §4.1 expiry retry", current.SessionID)
	}
	// Only the ENGINE runs under the deadline: the activation write
	// below is a local store update that must not be starved by a turn
	// that finished with seconds to spare. The bound is a TIMEOUT of
	// the window remaining on m.now()'s clock — not WithDeadline
	// against the wall clock — so the enforcement clock is the same
	// injectable one every other decision in this package uses
	// (positive by the pre-check above).
	turnCtx, cancel := context.WithTimeout(ctx, remaining)
	defer cancel()
	// A dead context is a refusal, not an input: shutdown may have
	// begun after the live-row query above, and Engine has no contract
	// to no-op on a cancelled context — starting no new turn once
	// shutdown begins is the router's lifetime promise (§4.1).
	if err := turnCtx.Err(); err != nil {
		return fmt.Errorf("session: first turn for %s not started: %w", current.SessionID, err)
	}
	if err := m.engine.Start(turnCtx, Spec{
		SessionID: current.SessionID,
		ThreadKey: current.ThreadKey,
		Cwd:       current.Cwd,
	}); err != nil {
		return fmt.Errorf("session: first turn for %s: %w", current.SessionID, err)
	}
	if m.now().Unix() >= current.ActivationDeadline {
		return fmt.Errorf("session: first turn for %s finished after its activation deadline — left creating for the §4.1 expiry retry", current.SessionID)
	}
	// The turn HAPPENED — its activation must not be lost to a SIGTERM
	// that landed as the child exited. The router waits for in-flight
	// handlers precisely so their durable writes finish; WithoutCancel
	// lets this one write complete while new work stays refused.
	if err := store.ActivateSession(context.WithoutCancel(ctx), m.db, current.SessionID); err != nil {
		return fmt.Errorf("session: activate %s: %w", current.SessionID, err)
	}
	m.touch(ctx, current.SessionID)
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
