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

// fakeEngine records Start specs and returns scripted errors.
type fakeEngine struct {
	specs []session.Spec
	err   error
}

func (f *fakeEngine) Start(_ context.Context, spec session.Spec) error {
	f.specs = append(f.specs, spec)
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
// context deadline derived from the row's activation_deadline — a hung
// spawn must release the serialized thread queue instead of wedging it
// past the recovery window, and the row stays creating for the §4.1
// expiry retry. (The pinned deadline here is far in the wall-clock
// past, so the derived context is already expired — the strongest form
// of "the engine outlived its window".)
func TestStartNewBoundsFirstTurnByDeadline(t *testing.T) {
	db := mustOpen(t)
	m := newManager(db, blockingEngine{}, 1700000000)
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
	// Same session, but the clock has passed the deadline by the time
	// the turn finishes.
	late := newManager(db, eng, 1700000121)
	if err := late.StartNew(ctx, live); err == nil {
		t.Fatal("StartNew activated a session after its activation deadline")
	}
	var status string
	if err := db.QueryRow(`SELECT status FROM sessions WHERE session_id = ?`, live.SessionID).Scan(&status); err != nil {
		t.Fatalf("read back: %v", err)
	}
	if status != "creating" {
		t.Errorf("late first turn left status %q, want creating", status)
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
