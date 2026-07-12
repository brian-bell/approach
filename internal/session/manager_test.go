package session_test

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"path/filepath"
	"regexp"
	"testing"
	"time"

	"github.com/brian-bell/approach/internal/session"
	"github.com/brian-bell/approach/internal/store"
)

type discard struct{}

func (discard) Write(p []byte) (int, error) { return len(p), nil }

func discardLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(discard{}, nil))
}

func mustOpen(t *testing.T) *sql.DB {
	t.Helper()
	db, err := store.Open(filepath.Join(t.TempDir(), "state", "approach.db"))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() {
		if err := db.Close(); err != nil {
			t.Errorf("close: %v", err)
		}
	})
	return db
}

// fakeEngine records Start specs (and each context's liveness at
// entry) and returns scripted errors.
type fakeEngine struct {
	specs   []session.Spec
	ctxErrs []error // ctx.Err() observed at Start entry
	err     error
}

func (f *fakeEngine) Start(ctx context.Context, spec session.Spec) error {
	f.specs = append(f.specs, spec)
	f.ctxErrs = append(f.ctxErrs, ctx.Err())
	return f.err
}

// newManager builds a Manager over the given store with an injected
// clock and a 2-minute activation window.
func newManager(db *sql.DB, eng session.Engine, at int64) *session.Manager {
	return session.NewManager(db, eng, session.Config{
		ActivationWindow: 2 * time.Minute,
		Logger:           discardLogger(),
		Now:              func() time.Time { return time.Unix(at, 0) },
	})
}

// TestZeroValueConfigIsSafe: a Manager built from Config{} must mint
// working sessions — not panic on a nil logger after the insert, and
// not stamp deadline == created_at (a born-expired row the schema
// layer rejects).
func TestZeroValueConfigIsSafe(t *testing.T) {
	db := mustOpen(t)
	m := session.NewManager(db, &fakeEngine{}, session.Config{})

	live, fresh, err := m.Ensure(context.Background(), "discord:dm:a", "owner", t.TempDir())
	if err != nil {
		t.Fatalf("Ensure with zero-value Config: %v", err)
	}
	if !fresh {
		t.Error("Ensure reported fresh=false on an empty thread")
	}
	if live.ActivationDeadline <= live.CreatedAt {
		t.Errorf("deadline %d not after created_at %d — the default window did not apply", live.ActivationDeadline, live.CreatedAt)
	}
}

var uuidV4 = regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$`)

// TestEnsureMintsCreatingSession: a thread with no live session gets a
// daemon-pinned v4 UUID and a creating row whose activation deadline is
// now + window, written in the same insert (§4.1).
func TestEnsureMintsCreatingSession(t *testing.T) {
	db := mustOpen(t)
	m := newManager(db, &fakeEngine{}, 1700000000)
	cwd := t.TempDir()

	live, fresh, err := m.Ensure(context.Background(), "discord:dm:a", "owner", cwd)
	if err != nil {
		t.Fatalf("Ensure: %v", err)
	}
	if !fresh {
		t.Error("Ensure on an empty thread reported fresh=false")
	}
	if !uuidV4.MatchString(live.SessionID) {
		t.Errorf("pinned session id %q is not a v4 UUID", live.SessionID)
	}
	if live.Status != "creating" {
		t.Errorf("fresh session status = %q, want creating (§4.1)", live.Status)
	}
	if live.ActivationDeadline != 1700000000+120 {
		t.Errorf("activation_deadline = %d, want created_at + window = %d", live.ActivationDeadline, 1700000000+120)
	}

	// The row is durable, not just returned.
	got, ok, err := store.ResolveLiveSession(context.Background(), db, "discord:dm:a")
	if err != nil || !ok {
		t.Fatalf("ResolveLiveSession after Ensure: ok=%v err=%v", ok, err)
	}
	if got.SessionID != live.SessionID {
		t.Errorf("durable session id %q != returned %q", got.SessionID, live.SessionID)
	}
}

// TestEnsureReturnsExistingLive: a live session (creating within its
// deadline, or active) resolves as-is — Ensure must never double-pin.
func TestEnsureReturnsExistingLive(t *testing.T) {
	db := mustOpen(t)
	m := newManager(db, &fakeEngine{}, 1700000000)
	cwd := t.TempDir()
	ctx := context.Background()

	first, _, err := m.Ensure(ctx, "discord:dm:a", "owner", cwd)
	if err != nil {
		t.Fatalf("first Ensure: %v", err)
	}

	// Still creating, deadline not passed.
	again, fresh, err := m.Ensure(ctx, "discord:dm:a", "owner", cwd)
	if err != nil {
		t.Fatalf("second Ensure: %v", err)
	}
	if fresh || again.SessionID != first.SessionID {
		t.Errorf("Ensure re-pinned a live creating session: fresh=%v id=%q want %q", fresh, again.SessionID, first.SessionID)
	}

	// Active now.
	if err := store.ActivateSession(ctx, db, first.SessionID); err != nil {
		t.Fatalf("ActivateSession: %v", err)
	}
	again, fresh, err = m.Ensure(ctx, "discord:dm:a", "owner", cwd)
	if err != nil {
		t.Fatalf("third Ensure: %v", err)
	}
	if fresh || again.SessionID != first.SessionID || again.Status != "active" {
		t.Errorf("Ensure did not return the active session: fresh=%v %+v", fresh, again)
	}
}

// TestEnsureRetriesExpiredCreating: a creating row past its activation
// deadline is failed (kept for forensics) and a FRESH session is pinned
// — the §4.1 'creating past deadline ⇒ failed, retried fresh' rule.
func TestEnsureRetriesExpiredCreating(t *testing.T) {
	db := mustOpen(t)
	cwd := t.TempDir()
	ctx := context.Background()

	first, _, err := newManager(db, &fakeEngine{}, 1700000000).Ensure(ctx, "discord:dm:a", "owner", cwd)
	if err != nil {
		t.Fatalf("first Ensure: %v", err)
	}

	// Same thread, clock past the deadline.
	late := newManager(db, &fakeEngine{}, 1700000121)
	second, fresh, err := late.Ensure(ctx, "discord:dm:a", "owner", cwd)
	if err != nil {
		t.Fatalf("late Ensure: %v", err)
	}
	if !fresh {
		t.Error("late Ensure reported fresh=false over an expired creating row")
	}
	if second.SessionID == first.SessionID {
		t.Error("expired creating session was reused, want a fresh UUID (§4.1 retried fresh)")
	}

	// The old row is failed, kept, and no longer live.
	var status string
	if err := db.QueryRow(`SELECT status FROM sessions WHERE session_id = ?`, first.SessionID).Scan(&status); err != nil {
		t.Fatalf("read back expired session: %v", err)
	}
	if status != "failed" {
		t.Errorf("expired creating session status = %q, want failed", status)
	}
}

// TestStartNewActivatesOnSuccess: the engine's first turn runs with the
// PINNED id (the daemon chose it — §4.1, never engine output), and
// success flips creating → active.
func TestStartNewActivatesOnSuccess(t *testing.T) {
	db := mustOpen(t)
	eng := &fakeEngine{}
	m := newManager(db, eng, 1700000000)
	ctx := context.Background()

	live, _, err := m.Ensure(ctx, "discord:dm:a", "owner", t.TempDir())
	if err != nil {
		t.Fatalf("Ensure: %v", err)
	}
	if err := m.StartNew(ctx, live); err != nil {
		t.Fatalf("StartNew: %v", err)
	}

	if len(eng.specs) != 1 {
		t.Fatalf("engine started %d times, want 1", len(eng.specs))
	}
	spec := eng.specs[0]
	if spec.SessionID != live.SessionID {
		t.Errorf("engine got session id %q, want the pinned %q", spec.SessionID, live.SessionID)
	}
	if spec.Cwd != live.Cwd {
		t.Errorf("engine got cwd %q, want the recorded %q (§6)", spec.Cwd, live.Cwd)
	}
	// The turn context must be LIVE at spawn: the window bound follows
	// the injected clock, so a context-honoring engine actually runs.
	if eng.ctxErrs[0] != nil {
		t.Errorf("engine received an already-dead context (%v) inside the activation window", eng.ctxErrs[0])
	}

	got, ok, err := store.ResolveLiveSession(ctx, db, "discord:dm:a")
	if err != nil || !ok {
		t.Fatalf("resolve after StartNew: ok=%v err=%v", ok, err)
	}
	if got.Status != "active" {
		t.Errorf("session after successful first turn = %q, want active (§4.1)", got.Status)
	}
}

// TestStartNewEngineFailureLeavesCreating: a failed first turn leaves
// the row creating — the deadline, not this error, decides when the
// thread retries fresh, so a transient engine crash cannot burn the
// session before its window ends.
func TestStartNewEngineFailureLeavesCreating(t *testing.T) {
	db := mustOpen(t)
	eng := &fakeEngine{err: errors.New("engine exploded")}
	m := newManager(db, eng, 1700000000)
	ctx := context.Background()

	live, _, err := m.Ensure(ctx, "discord:dm:a", "owner", t.TempDir())
	if err != nil {
		t.Fatalf("Ensure: %v", err)
	}
	if err := m.StartNew(ctx, live); err == nil {
		t.Fatal("StartNew swallowed the engine failure")
	}

	got, ok, err := store.ResolveLiveSession(ctx, db, "discord:dm:a")
	if err != nil || !ok {
		t.Fatalf("resolve after failed StartNew: ok=%v err=%v", ok, err)
	}
	if got.Status != "creating" || got.SessionID != live.SessionID {
		t.Errorf("after engine failure: %+v, want the same creating row", got)
	}
}

// blockingEngine parks in Start until its context ends — the wedged
// first spawn the deadline bound exists for.
type blockingEngine struct{}

func (blockingEngine) Start(ctx context.Context, _ session.Spec) error {
	<-ctx.Done()
	return ctx.Err()
}

// TestStartNewBoundsFirstTurnByDeadline: the first turn runs under a
// timeout of the window remaining — a hung spawn must release the
// serialized thread queue instead of wedging it past the recovery
// window, and the row stays creating for the §4.1 expiry retry. The
// window is 1s so the blocking engine's cancellation fires in real
// time.
func TestStartNewBoundsFirstTurnByDeadline(t *testing.T) {
	db := mustOpen(t)
	m := session.NewManager(db, blockingEngine{}, session.Config{
		ActivationWindow: time.Second,
		Logger:           discardLogger(),
		Now:              func() time.Time { return time.Unix(1700000000, 0) },
	})
	ctx := context.Background()

	live, _, err := m.Ensure(ctx, "discord:dm:a", "owner", t.TempDir())
	if err != nil {
		t.Fatalf("Ensure: %v", err)
	}
	done := make(chan error, 1)
	go func() { done <- m.StartNew(ctx, live) }()
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("StartNew returned nil from a spawn that never completed")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("StartNew did not return — a wedged first turn blocks its thread queue forever")
	}

	got, ok, err := store.ResolveLiveSession(ctx, db, "discord:dm:a")
	if err != nil || !ok {
		t.Fatalf("resolve after bounded StartNew: ok=%v err=%v", ok, err)
	}
	if got.Status != "creating" {
		t.Errorf("session after deadline-bounded failure = %q, want creating (expiry retry owns it)", got.Status)
	}
}

// TestStartNewRefusesLateActivation: a first turn that completes after
// the activation deadline must not flip the row to active — Ensure is
// entitled to have failed it, and a late activation would fight that.
func TestStartNewRefusesLateActivation(t *testing.T) {
	db := mustOpen(t)
	eng := &fakeEngine{} // ignores ctx; "succeeds" no matter how late
	ctx := context.Background()

	live, _, err := newManager(db, eng, 1700000000).Ensure(ctx, "discord:dm:a", "owner", t.TempDir())
	if err != nil {
		t.Fatalf("Ensure: %v", err)
	}
	// Same session, but the queue delayed StartNew past the deadline.
	late := newManager(db, eng, 1700000121)
	if err := late.StartNew(ctx, live); err == nil {
		t.Fatal("StartNew activated a session after its activation deadline")
	}
	// The engine must never have spawned: past the window, the only
	// provably side-effect-free turn is the one never started.
	if len(eng.specs) != 0 {
		t.Errorf("engine invoked %d times after the deadline, want 0", len(eng.specs))
	}
	var status string
	if err := db.QueryRow(`SELECT status FROM sessions WHERE session_id = ?`, live.SessionID).Scan(&status); err != nil {
		t.Fatalf("read back: %v", err)
	}
	if status != "creating" {
		t.Errorf("late first turn left status %q, want creating", status)
	}
}

// TestStartNewRefusesStaleSnapshot: StartNew must not spawn for a
// session that is no longer the thread's creating row — a duplicate
// call (or one racing a replacement) would run a whole unintended
// agent turn before the activation guard could object.
func TestStartNewRefusesStaleSnapshot(t *testing.T) {
	db := mustOpen(t)
	eng := &fakeEngine{}
	m := newManager(db, eng, 1700000000)
	ctx := context.Background()

	live, _, err := m.Ensure(ctx, "discord:dm:a", "owner", t.TempDir())
	if err != nil {
		t.Fatalf("Ensure: %v", err)
	}
	if err := m.StartNew(ctx, live); err != nil {
		t.Fatalf("first StartNew: %v", err)
	}
	// Same snapshot again: the row is active now, not creating.
	if err := m.StartNew(ctx, live); err == nil {
		t.Fatal("duplicate StartNew spawned a second first turn")
	}
	if len(eng.specs) != 1 {
		t.Errorf("engine started %d times, want 1 — the stale snapshot must be refused before the spawn", len(eng.specs))
	}
}

// TestStartNewUsesDurableFieldsNotSnapshot: a doctored snapshot (same
// id, inflated deadline, foreign cwd) must not bypass the persisted
// row's window or move the spawn out of the recorded directory — the
// row, not the argument, is the truth (§6).
func TestStartNewUsesDurableFieldsNotSnapshot(t *testing.T) {
	db := mustOpen(t)
	eng := &fakeEngine{}
	ctx := context.Background()

	live, _, err := newManager(db, eng, 1700000000).Ensure(ctx, "discord:dm:a", "owner", t.TempDir())
	if err != nil {
		t.Fatalf("Ensure: %v", err)
	}
	doctored := live
	doctored.ActivationDeadline = 9700000000 // far future
	doctored.Cwd = "/somewhere/else"

	late := newManager(db, eng, 1700000121) // past the DURABLE deadline
	if err := late.StartNew(ctx, doctored); err == nil {
		t.Fatal("StartNew honored a snapshot deadline over the persisted one")
	}
	if len(eng.specs) != 0 {
		t.Fatalf("engine spawned %d times past the durable deadline, want 0", len(eng.specs))
	}

	// Within the window, the spawn uses the ROW's cwd, not the snapshot's.
	inWindow := newManager(db, eng, 1700000010)
	if err := inWindow.StartNew(ctx, doctored); err != nil {
		t.Fatalf("StartNew within window: %v", err)
	}
	if len(eng.specs) != 1 || eng.specs[0].Cwd != live.Cwd {
		t.Errorf("engine cwd = %q, want the recorded %q", eng.specs[0].Cwd, live.Cwd)
	}
}

// TestEnsurePinsUniqueIDs: every pin is a fresh UUID — collisions
// across threads would cross-wire transcripts.
func TestEnsurePinsUniqueIDs(t *testing.T) {
	db := mustOpen(t)
	m := newManager(db, &fakeEngine{}, 1700000000)
	ctx := context.Background()
	cwd := t.TempDir()

	seen := make(map[string]bool)
	for i := 0; i < 20; i++ {
		live, _, err := m.Ensure(ctx, fmt.Sprintf("discord:dm:%d", i), "owner", cwd)
		if err != nil {
			t.Fatalf("Ensure %d: %v", i, err)
		}
		if seen[live.SessionID] {
			t.Fatalf("duplicate pinned UUID %q", live.SessionID)
		}
		seen[live.SessionID] = true
	}
}
