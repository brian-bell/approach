package session_test

import (
	"context"
	"database/sql"
	"errors"
	"os"
	"testing"

	"github.com/brian-bell/approach/internal/session"
	"github.com/brian-bell/approach/internal/store"
)

// activeThread pins and activates a session via two Turns' worth of
// machinery: one Turn (first turn activates it).
func activeThread(t *testing.T, m *session.Manager, db *sql.DB, cwd string) store.LiveSession {
	t.Helper()
	if err := m.Turn(context.Background(), "discord:dm:a", "owner", cwd); err != nil {
		t.Fatalf("setup Turn: %v", err)
	}
	live, ok, err := store.ResolveLiveSession(context.Background(), db, "discord:dm:a")
	if err != nil || !ok || live.Status != "active" {
		t.Fatalf("setup resolve: ok=%v err=%v %+v", ok, err, live)
	}
	return live
}

// TestTurnDegradesOnResumeFailed: the §4.6 amnesia-with-notes path —
// the engine reports the transcript unresumable, the old row is kept
// as resume_failed with a successor link, and the SAME event runs as
// the fresh session's first turn carrying the one-line transparency
// note.
func TestTurnDegradesOnResumeFailed(t *testing.T) {
	db := mustOpen(t)
	eng := &resumeEngine{resumeErr: session.ErrResumeFailed}
	clock := int64(1700000000)
	m := rotationManager(db, eng, &clock)
	cwd := t.TempDir()

	old := activeThread(t, m, db, cwd)

	clock += 10
	if err := m.Turn(context.Background(), "discord:dm:a", "owner", cwd); err != nil {
		t.Fatalf("Turn over a dead transcript: %v (must degrade, never hard-error — §4.6)", err)
	}

	// Old row: resume_failed, kept, linked to the successor.
	var status string
	var rotatedTo sql.NullString
	if err := db.QueryRow(`SELECT status, rotated_to FROM sessions WHERE session_id = ?`, old.SessionID).Scan(&status, &rotatedTo); err != nil {
		t.Fatalf("read back old: %v", err)
	}
	if status != "resume_failed" {
		t.Errorf("old session status = %q, want resume_failed (rotation cause, kept for forensics)", status)
	}

	// Successor: active (its first turn ran), fresh id, linked from old.
	live, ok, err := store.ResolveLiveSession(context.Background(), db, "discord:dm:a")
	if err != nil || !ok {
		t.Fatalf("resolve successor: ok=%v err=%v", ok, err)
	}
	if live.SessionID == old.SessionID || live.Status != "active" {
		t.Errorf("successor = %+v, want a fresh active session", live)
	}
	if !rotatedTo.Valid || rotatedTo.String != live.SessionID {
		t.Errorf("old rotated_to = %+v, want %q", rotatedTo, live.SessionID)
	}

	// The engine ran: one resume attempt (failed), then the successor's
	// first turn carrying the §4.6 note; the old id never re-Started.
	if len(eng.resumes) != 1 || len(eng.specs) != 2 {
		t.Fatalf("resumes=%d starts=%d, want 1 and 2", len(eng.resumes), len(eng.specs))
	}
	degraded := eng.specs[1]
	if degraded.SessionID != live.SessionID {
		t.Errorf("degraded first turn ran session %q, want the successor %q", degraded.SessionID, live.SessionID)
	}
	if degraded.TransparencyNote == "" {
		t.Error("degraded first turn carries no transparency note (§4.6: the reply must say history was lost)")
	}
	// Ordinary turns carry no note.
	if eng.specs[0].TransparencyNote != "" {
		t.Errorf("ordinary first turn carried a note: %q", eng.specs[0].TransparencyNote)
	}
}

// TestTurnDegradesOnCwdGone: the recorded cwd vanished (§11) — the
// successor is minted at the CALLER's cwd, the config-current thread
// directory, because the recorded one is exactly what died.
func TestTurnDegradesOnCwdGone(t *testing.T) {
	db := mustOpen(t)
	eng := &resumeEngine{}
	clock := int64(1700000000)
	m := rotationManager(db, eng, &clock)
	oldCwd := t.TempDir()

	old := activeThread(t, m, db, oldCwd)
	if err := os.RemoveAll(old.Cwd); err != nil {
		t.Fatalf("remove recorded cwd: %v", err)
	}

	newCwd := t.TempDir()
	clock += 10
	if err := m.Turn(context.Background(), "discord:dm:a", "owner", newCwd); err != nil {
		t.Fatalf("Turn over a vanished cwd: %v (must degrade — §4.6, §11)", err)
	}

	live, ok, err := store.ResolveLiveSession(context.Background(), db, "discord:dm:a")
	if err != nil || !ok {
		t.Fatalf("resolve successor: ok=%v err=%v", ok, err)
	}
	if live.SessionID == old.SessionID || live.Status != "active" {
		t.Errorf("successor = %+v, want fresh and active", live)
	}
	if live.Cwd == old.Cwd {
		t.Error("successor kept the dead recorded cwd")
	}
	// No resume was attempted against the dead cwd's engine — the
	// assert refused before the spawn.
	if len(eng.resumes) != 0 {
		t.Errorf("engine resumed %d times from a vanished cwd, want 0", len(eng.resumes))
	}
	var status string
	if err := db.QueryRow(`SELECT status FROM sessions WHERE session_id = ?`, old.SessionID).Scan(&status); err != nil {
		t.Fatalf("read back old: %v", err)
	}
	if status != "resume_failed" {
		t.Errorf("old session status = %q, want resume_failed", status)
	}
}

// TestTurnDoesNotDegradeOnTransientError: an untyped engine failure is
// NOT transcript loss — the session stays active for the event layer's
// §4.6 retry/interrupt logic, and no session is burned.
func TestTurnDoesNotDegradeOnTransientError(t *testing.T) {
	db := mustOpen(t)
	eng := &resumeEngine{resumeErr: errors.New("rate limited")}
	clock := int64(1700000000)
	m := rotationManager(db, eng, &clock)
	cwd := t.TempDir()

	old := activeThread(t, m, db, cwd)

	clock += 10
	if err := m.Turn(context.Background(), "discord:dm:a", "owner", cwd); err == nil {
		t.Fatal("Turn swallowed a transient resume failure")
	}
	live, ok, err := store.ResolveLiveSession(context.Background(), db, "discord:dm:a")
	if err != nil || !ok {
		t.Fatalf("resolve: ok=%v err=%v", ok, err)
	}
	if live.SessionID != old.SessionID || live.Status != "active" {
		t.Errorf("after transient failure: %+v, want the original still active (no degradation)", live)
	}
	if len(eng.specs) != 1 {
		t.Errorf("starts=%d, want 1 — a transient failure must not mint sessions", len(eng.specs))
	}
}

// TestResumeFailSessionStore: the store primitive mirrors rotation —
// one transaction, old kept as resume_failed with the successor link,
// thread-bound guard.
func TestResumeFailSessionStore(t *testing.T) {
	db := mustOpen(t)
	ctx := context.Background()
	cwd := t.TempDir()

	old := store.Session{
		ThreadKey:          "discord:dm:a",
		SessionID:          "11111111-1111-4111-8111-111111111111",
		Cwd:                cwd,
		TrustFloor:         "owner",
		CreatedAt:          1700000000,
		ActivationDeadline: 1700000120,
	}
	if _, err := store.InsertSession(ctx, db, old); err != nil {
		t.Fatalf("insert: %v", err)
	}
	if err := store.ActivateSession(ctx, db, old.SessionID); err != nil {
		t.Fatalf("activate: %v", err)
	}
	successor := old
	successor.SessionID = "22222222-2222-4222-8222-222222222222"
	if _, err := store.ResumeFailSession(ctx, db, old.SessionID, successor); err != nil {
		t.Fatalf("ResumeFailSession: %v", err)
	}

	var status string
	var rotatedTo sql.NullString
	if err := db.QueryRow(`SELECT status, rotated_to FROM sessions WHERE session_id = ?`, old.SessionID).Scan(&status, &rotatedTo); err != nil {
		t.Fatalf("read back: %v", err)
	}
	if status != "resume_failed" || !rotatedTo.Valid || rotatedTo.String != successor.SessionID {
		t.Errorf("old = (%q, %+v), want resume_failed linked to %q", status, rotatedTo, successor.SessionID)
	}

	// Guard: only an active row degrades.
	if _, err := store.ResumeFailSession(ctx, db, old.SessionID, successor); !errors.Is(err, store.ErrNotActive) {
		t.Errorf("second ResumeFailSession = %v, want ErrNotActive", err)
	}
}
