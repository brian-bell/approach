package session_test

import (
	"context"
	"errors"
	"os"
	"testing"

	"github.com/brian-bell/approach/internal/session"
	"github.com/brian-bell/approach/internal/store"
)

// resumeEngine extends fakeEngine with a scripted Resume.
type resumeEngine struct {
	fakeEngine
	resumes    []session.Spec
	resumeErr  error
	resumeCtxs []error // ctx.Err() at Resume entry
}

func (f *resumeEngine) Resume(ctx context.Context, spec session.Spec) error {
	f.resumes = append(f.resumes, spec)
	f.resumeCtxs = append(f.resumeCtxs, ctx.Err())
	return f.resumeErr
}

// TestResumeSpawnsFromRecordedCwd: the resume runs --resume with the
// session's recorded id, from its recorded cwd (§4.1, §6) — never from
// anything the caller supplies at turn time.
func TestResumeSpawnsFromRecordedCwd(t *testing.T) {
	db := mustOpen(t)
	eng := &resumeEngine{}
	m := newManager(db, eng, 1700000000)
	ctx := context.Background()

	live, _, err := m.Ensure(ctx, "discord:dm:a", "owner", t.TempDir())
	if err != nil {
		t.Fatalf("Ensure: %v", err)
	}
	if err := m.StartNew(ctx, live); err != nil {
		t.Fatalf("StartNew: %v", err)
	}
	active, _, err := store.ResolveLiveSession(ctx, db, "discord:dm:a")
	if err != nil {
		t.Fatalf("resolve: %v", err)
	}

	doctored := active
	doctored.Cwd = "/somewhere/else" // must be ignored: the row is the truth
	if err := m.Resume(ctx, doctored); err != nil {
		t.Fatalf("Resume: %v", err)
	}
	if len(eng.resumes) != 1 {
		t.Fatalf("engine resumed %d times, want 1", len(eng.resumes))
	}
	got := eng.resumes[0]
	if got.SessionID != active.SessionID || got.Cwd != active.Cwd {
		t.Errorf("resume spec = (%q, %q), want the recorded (%q, %q)", got.SessionID, got.Cwd, active.SessionID, active.Cwd)
	}
	if eng.resumeCtxs[0] != nil {
		t.Errorf("resume received a dead context: %v", eng.resumeCtxs[0])
	}
	if len(eng.specs) != 1 {
		t.Errorf("Start called %d times across the flow, want exactly the first turn", len(eng.specs))
	}
}

// TestResumeAssertsCwdBeforeSpawn: §11's session-scope trap — --resume
// is scoped to the project dir, so a moved or pruned cwd means the
// resume would silently fail. Assert, don't assume: the engine must
// never spawn when the recorded cwd is gone, and the error is typed so
// the §4.6 degradation (x6n.2.8) can key off it.
func TestResumeAssertsCwdBeforeSpawn(t *testing.T) {
	db := mustOpen(t)
	eng := &resumeEngine{}
	m := newManager(db, eng, 1700000000)
	ctx := context.Background()
	cwd := t.TempDir()

	live, _, err := m.Ensure(ctx, "discord:dm:a", "owner", cwd)
	if err != nil {
		t.Fatalf("Ensure: %v", err)
	}
	if err := m.StartNew(ctx, live); err != nil {
		t.Fatalf("StartNew: %v", err)
	}
	active, _, err := store.ResolveLiveSession(ctx, db, "discord:dm:a")
	if err != nil {
		t.Fatalf("resolve: %v", err)
	}

	if err := os.RemoveAll(active.Cwd); err != nil {
		t.Fatalf("remove cwd: %v", err)
	}
	err = m.Resume(ctx, active)
	if !errors.Is(err, session.ErrCwdGone) {
		t.Fatalf("Resume over a missing cwd = %v, want ErrCwdGone", err)
	}
	if len(eng.resumes) != 0 {
		t.Errorf("engine resumed %d times from a missing cwd, want 0 (assert, don't assume — §11)", len(eng.resumes))
	}
}

// TestResumeRefusesStaleSnapshot: like StartNew, Resume is a
// side-effecting spawn — it must re-verify the id is still the
// thread's ACTIVE session before running.
func TestResumeRefusesStaleSnapshot(t *testing.T) {
	db := mustOpen(t)
	eng := &resumeEngine{}
	m := newManager(db, eng, 1700000000)
	ctx := context.Background()

	live, _, err := m.Ensure(ctx, "discord:dm:a", "owner", t.TempDir())
	if err != nil {
		t.Fatalf("Ensure: %v", err)
	}
	// Still creating — a resume against it must refuse: there is no
	// transcript to resume yet.
	if err := m.Resume(ctx, live); err == nil {
		t.Fatal("Resume ran against a creating session")
	}
	if len(eng.resumes) != 0 {
		t.Errorf("engine resumed %d times against a creating row, want 0", len(eng.resumes))
	}
}

// TestResumeEngineErrorPropagates: an engine failure surfaces to the
// caller (the §4.6 classification is x6n.2.8's) and the session row is
// untouched — still active.
func TestResumeEngineErrorPropagates(t *testing.T) {
	db := mustOpen(t)
	eng := &resumeEngine{resumeErr: errors.New("transcript unreachable")}
	m := newManager(db, eng, 1700000000)
	ctx := context.Background()

	live, _, err := m.Ensure(ctx, "discord:dm:a", "owner", t.TempDir())
	if err != nil {
		t.Fatalf("Ensure: %v", err)
	}
	if err := m.StartNew(ctx, live); err != nil {
		t.Fatalf("StartNew: %v", err)
	}
	active, _, err := store.ResolveLiveSession(ctx, db, "discord:dm:a")
	if err != nil {
		t.Fatalf("resolve: %v", err)
	}
	if err := m.Resume(ctx, active); err == nil {
		t.Fatal("Resume swallowed the engine failure")
	}
	got, ok, err := store.ResolveLiveSession(ctx, db, "discord:dm:a")
	if err != nil || !ok || got.Status != "active" {
		t.Errorf("after failed resume: ok=%v err=%v status=%q, want the active row untouched", ok, err, got.Status)
	}
}

// TestTurnRoutesByLifecycle: the unified per-event entry — a thread
// with no session starts fresh; a creating thread retries its FIRST
// turn against the same pinned id; an active thread resumes (§4.1).
func TestTurnRoutesByLifecycle(t *testing.T) {
	db := mustOpen(t)
	eng := &resumeEngine{}
	m := newManager(db, eng, 1700000000)
	ctx := context.Background()
	cwd := t.TempDir()

	// Empty thread → first turn, session active.
	if err := m.Turn(ctx, "discord:dm:a", "owner", cwd, "hi"); err != nil {
		t.Fatalf("Turn (new): %v", err)
	}
	if len(eng.specs) != 1 || len(eng.resumes) != 0 {
		t.Fatalf("new thread: starts=%d resumes=%d, want 1, 0", len(eng.specs), len(eng.resumes))
	}
	first, _, err := store.ResolveLiveSession(ctx, db, "discord:dm:a")
	if err != nil {
		t.Fatalf("resolve: %v", err)
	}
	if first.Status != "active" {
		t.Fatalf("after first Turn status = %q, want active", first.Status)
	}

	// Active thread → resume, same session, no new Start.
	if err := m.Turn(ctx, "discord:dm:a", "owner", cwd, "hi"); err != nil {
		t.Fatalf("Turn (resume): %v", err)
	}
	if len(eng.specs) != 1 || len(eng.resumes) != 1 {
		t.Fatalf("active thread: starts=%d resumes=%d, want 1, 1", len(eng.specs), len(eng.resumes))
	}
	if eng.resumes[0].SessionID != first.SessionID {
		t.Errorf("resume hit %q, want the live %q", eng.resumes[0].SessionID, first.SessionID)
	}
}

// TestTurnRetriesCreatingFirstTurn: a creating row within its window
// (its first turn failed transiently) gets ANOTHER first turn against
// the SAME pinned id — never a resume (no transcript exists), never a
// fresh pin (the window isn't over).
func TestTurnRetriesCreatingFirstTurn(t *testing.T) {
	db := mustOpen(t)
	eng := &resumeEngine{}
	eng.err = errors.New("transient spawn failure")
	m := newManager(db, eng, 1700000000)
	ctx := context.Background()
	cwd := t.TempDir()

	if err := m.Turn(ctx, "discord:dm:a", "owner", cwd, "hi"); err == nil {
		t.Fatal("Turn swallowed the first-turn failure")
	}
	pinned, _, err := store.ResolveLiveSession(ctx, db, "discord:dm:a")
	if err != nil {
		t.Fatalf("resolve: %v", err)
	}
	if pinned.Status != "creating" {
		t.Fatalf("after failed first turn status = %q, want creating", pinned.Status)
	}

	// The engine recovers; the next event retries the same session.
	eng.err = nil
	if err := m.Turn(ctx, "discord:dm:a", "owner", cwd, "hi"); err != nil {
		t.Fatalf("Turn (retry): %v", err)
	}
	if len(eng.specs) != 2 || len(eng.resumes) != 0 {
		t.Fatalf("retry: starts=%d resumes=%d, want 2, 0", len(eng.specs), len(eng.resumes))
	}
	if eng.specs[1].SessionID != pinned.SessionID {
		t.Errorf("retry pinned %q, want the same %q", eng.specs[1].SessionID, pinned.SessionID)
	}
	got, _, err := store.ResolveLiveSession(ctx, db, "discord:dm:a")
	if err != nil {
		t.Fatalf("resolve after retry: %v", err)
	}
	if got.Status != "active" || got.SessionID != pinned.SessionID {
		t.Errorf("after retry: %+v, want the same session active", got)
	}
}
