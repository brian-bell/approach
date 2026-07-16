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
	if err := m.Turn(context.Background(), session.TurnRequest{ThreadKey: "discord:dm:a", TrustFloor: "owner", Cwd: cwd, Kind: "message", Prompt: "hi"}); err != nil {
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
	if err := m.Turn(context.Background(), session.TurnRequest{ThreadKey: "discord:dm:a", TrustFloor: "owner", Cwd: cwd, Kind: "message", Prompt: "hi"}); err != nil {
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
	if err := m.Turn(context.Background(), session.TurnRequest{ThreadKey: "discord:dm:a", TrustFloor: "owner", Cwd: newCwd, Kind: "message", Prompt: "hi"}); err != nil {
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
	if err := m.Turn(context.Background(), session.TurnRequest{ThreadKey: "discord:dm:a", TrustFloor: "owner", Cwd: cwd, Kind: "message", Prompt: "hi"}); err == nil {
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

// TestRotationOverMissingCwdDegrades: an active session due for
// rotation whose recorded cwd has vanished must NOT loop a failing
// rotation forever (the successor would inherit the dead cwd and the
// insert's canonicalization refuses it — every later event repeating
// the same failure is a wedged thread). The transcript is unreachable
// anyway (--resume is cwd-scoped), so this is the §4.6 resume-failure
// shape: degrade to a successor at the CALLER's cwd, note carried.
func TestRotationOverMissingCwdDegrades(t *testing.T) {
	db := mustOpen(t)
	clock := int64(1700000000)
	eng := &resumeEngine{}
	m := rotationManager(db, eng, &clock) // IdleTTL 1h
	oldCwd := t.TempDir()

	old := activeThread(t, m, db, oldCwd)
	if err := os.RemoveAll(old.Cwd); err != nil {
		t.Fatalf("remove recorded cwd: %v", err)
	}

	clock += 3601 // past the idle TTL — rotation is due
	newCwd := t.TempDir()
	if err := m.Turn(context.Background(), session.TurnRequest{ThreadKey: "discord:dm:a", TrustFloor: "owner", Cwd: newCwd, Kind: "message", Prompt: "hi"}); err != nil {
		t.Fatalf("Turn over a rotation-due session with a dead cwd: %v (must degrade, never wedge)", err)
	}

	live, ok, err := store.ResolveLiveSession(context.Background(), db, "discord:dm:a")
	if err != nil || !ok {
		t.Fatalf("resolve successor: ok=%v err=%v", ok, err)
	}
	if live.SessionID == old.SessionID || live.Status != "active" || live.Cwd == old.Cwd {
		t.Errorf("successor = %+v, want fresh, active, and at the caller's cwd", live)
	}
	var status string
	if err := db.QueryRow(`SELECT status FROM sessions WHERE session_id = ?`, old.SessionID).Scan(&status); err != nil {
		t.Fatalf("read back old: %v", err)
	}
	if status != "resume_failed" {
		t.Errorf("old session status = %q, want resume_failed (the transcript is cwd-scoped and unreachable)", status)
	}
	// The degradation note rides the successor's first turn.
	last := eng.specs[len(eng.specs)-1]
	if last.TransparencyNote == "" {
		t.Error("degraded-successor first turn carries no transparency note")
	}
}

// TestRotateNowRefusesMissingCwd: the /new primitive has no caller cwd
// to degrade to — a dead recorded cwd refuses with the typed error so
// the handler can route to degradation instead of committing a
// rotation whose successor cannot exist.
func TestRotateNowRefusesMissingCwd(t *testing.T) {
	db := mustOpen(t)
	clock := int64(1700000000)
	eng := &resumeEngine{}
	m := rotationManager(db, eng, &clock)
	cwd := t.TempDir()

	old := activeThread(t, m, db, cwd)
	if err := os.RemoveAll(old.Cwd); err != nil {
		t.Fatalf("remove recorded cwd: %v", err)
	}
	if _, err := m.RotateNow(context.Background(), "discord:dm:a"); !errors.Is(err, session.ErrCwdGone) {
		t.Errorf("RotateNow over a dead cwd = %v, want ErrCwdGone", err)
	}
	// The refused rotation left the old row alone — no half-state.
	live, ok, err := store.ResolveLiveSession(context.Background(), db, "discord:dm:a")
	if err != nil || !ok || live.SessionID != old.SessionID || live.Status != "active" {
		t.Errorf("after refused RotateNow: ok=%v %+v, want the original still active", ok, live)
	}
}

// TestDegradationNoteSurvivesRetry: if the degradation successor's
// FIRST turn fails transiently, the row stays creating — and the
// retry must still carry the §4.6 note: the user's first successful
// reply after transcript loss says so, no matter how many transient
// failures intervened. The state is recovered from the resume_failed
// predecessor link, not from in-memory context a crash could lose.
func TestDegradationNoteSurvivesRetry(t *testing.T) {
	db := mustOpen(t)
	eng := &resumeEngine{resumeErr: session.ErrResumeFailed}
	clock := int64(1700000000)
	m := rotationManager(db, eng, &clock)
	cwd := t.TempDir()

	activeThread(t, m, db, cwd)

	// Resume fails permanently; the degradation successor's first turn
	// ALSO fails, transiently.
	eng.err = errors.New("transient spawn failure")
	clock += 10
	if err := m.Turn(context.Background(), session.TurnRequest{ThreadKey: "discord:dm:a", TrustFloor: "owner", Cwd: cwd, Kind: "message", Prompt: "hi"}); err == nil {
		t.Fatal("Turn swallowed the successor's transient first-turn failure")
	}

	// Next event: engine recovered. The retry is an ordinary
	// creating-row first turn — but it must still carry the note.
	eng.err = nil
	eng.resumeErr = nil
	clock += 10
	if err := m.Turn(context.Background(), session.TurnRequest{ThreadKey: "discord:dm:a", TrustFloor: "owner", Cwd: cwd, Kind: "message", Prompt: "hi"}); err != nil {
		t.Fatalf("Turn (retry): %v", err)
	}
	last := eng.specs[len(eng.specs)-1]
	if last.TransparencyNote == "" {
		t.Error("retried degradation first turn lost the transparency note (§4.6)")
	}
	// And a session that was never degraded still carries none.
	if eng.specs[0].TransparencyNote != "" {
		t.Errorf("ordinary first turn carried a note: %q", eng.specs[0].TransparencyNote)
	}
}

// TestDegradationNoteSurvivesExpiry: the successor's first turns keep
// failing until its activation window closes; Ensure retires it as
// failed and mints a linked retry. The note must survive THAT chain
// too — resume_failed → failed → creating still owes the §4.6 line.
func TestDegradationNoteSurvivesExpiry(t *testing.T) {
	db := mustOpen(t)
	eng := &resumeEngine{resumeErr: session.ErrResumeFailed}
	clock := int64(1700000000)
	m := rotationManager(db, eng, &clock)
	cwd := t.TempDir()

	activeThread(t, m, db, cwd)

	// Degradation happens; the successor's first turn fails transiently.
	eng.err = errors.New("transient spawn failure")
	clock += 10
	if err := m.Turn(context.Background(), session.TurnRequest{ThreadKey: "discord:dm:a", TrustFloor: "owner", Cwd: cwd, Kind: "message", Prompt: "hi"}); err == nil {
		t.Fatal("Turn swallowed the successor's transient first-turn failure")
	}

	// The engine stays down past the successor's activation window, so
	// the next event expires it and mints a linked retry — which also
	// fails once.
	clock += 121
	if err := m.Turn(context.Background(), session.TurnRequest{ThreadKey: "discord:dm:a", TrustFloor: "owner", Cwd: cwd, Kind: "message", Prompt: "hi"}); err == nil {
		t.Fatal("Turn swallowed the expiry retry's failure")
	}

	// Engine recovers: the eventual first successful reply must still
	// say history was lost.
	eng.err = nil
	eng.resumeErr = nil
	clock += 10
	if err := m.Turn(context.Background(), session.TurnRequest{ThreadKey: "discord:dm:a", TrustFloor: "owner", Cwd: cwd, Kind: "message", Prompt: "hi"}); err != nil {
		t.Fatalf("Turn (recovered): %v", err)
	}
	last := eng.specs[len(eng.specs)-1]
	if last.TransparencyNote == "" {
		t.Error("transparency note lost across the expiry chain (resume_failed → failed → creating)")
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
