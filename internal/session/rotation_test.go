package session_test

import (
	"context"
	"database/sql"
	"errors"
	"testing"
	"time"

	"github.com/brian-bell/approach/internal/session"
	"github.com/brian-bell/approach/internal/store"
)

// rotationManager builds a Manager with tight rotation caps and a
// movable injected clock.
func rotationManager(db *sql.DB, eng session.Engine, clock *int64) *session.Manager {
	return session.NewManager(db, eng, session.Config{
		ActivationWindow: 2 * time.Minute,
		IdleTTL:          time.Hour,
		TurnCap:          3,
		Logger:           discardLogger(),
		Now:              func() time.Time { return time.Unix(*clock, 0) },
	})
}

// turnAndResolve runs one Turn and returns the thread's live session.
func turnAndResolve(t *testing.T, m *session.Manager, db *sql.DB, cwd string) store.LiveSession {
	const threadKey = "discord:dm:a"
	t.Helper()
	if err := m.Turn(context.Background(), threadKey, "owner", cwd); err != nil {
		t.Fatalf("Turn: %v", err)
	}
	live, ok, err := store.ResolveLiveSession(context.Background(), db, threadKey)
	if err != nil || !ok {
		t.Fatalf("resolve after Turn: ok=%v err=%v", ok, err)
	}
	return live
}

// TestTurnTouchesBookkeeping: every successful turn — first or resumed
// — advances last_seen and turns; rotation triggers read these (§6).
func TestTurnTouchesBookkeeping(t *testing.T) {
	db := mustOpen(t)
	clock := int64(1700000000)
	eng := &resumeEngine{}
	m := rotationManager(db, eng, &clock)
	cwd := t.TempDir()

	live := turnAndResolve(t, m, db, cwd)
	if live.Turns != 1 || live.LastSeen != 1700000000 {
		t.Errorf("after first turn: turns=%d last_seen=%d, want 1, 1700000000", live.Turns, live.LastSeen)
	}

	clock = 1700000500
	live = turnAndResolve(t, m, db, cwd)
	if live.Turns != 2 || live.LastSeen != 1700000500 {
		t.Errorf("after resume: turns=%d last_seen=%d, want 2, 1700000500", live.Turns, live.LastSeen)
	}
}

// TestTurnRotatesOnIdleTTL: an active session idle past the TTL is
// rotated before the event runs — the event lands on a fresh session
// whose first turn spawns with a NEW pinned id (§3, §6).
func TestTurnRotatesOnIdleTTL(t *testing.T) {
	db := mustOpen(t)
	clock := int64(1700000000)
	eng := &resumeEngine{}
	m := rotationManager(db, eng, &clock)
	cwd := t.TempDir()

	first := turnAndResolve(t, m, db, cwd)

	clock += 3601 // one hour TTL + 1s
	second := turnAndResolve(t, m, db, cwd)
	if second.SessionID == first.SessionID {
		t.Fatal("idle session was resumed, want rotation to a fresh session")
	}
	if len(eng.specs) != 2 || len(eng.resumes) != 0 {
		t.Errorf("starts=%d resumes=%d, want 2 starts (both sessions' first turns), 0 resumes", len(eng.specs), len(eng.resumes))
	}

	var status string
	var rotatedTo sql.NullString
	if err := db.QueryRow(`SELECT status, rotated_to FROM sessions WHERE session_id = ?`, first.SessionID).Scan(&status, &rotatedTo); err != nil {
		t.Fatalf("read back old: %v", err)
	}
	if status != "rotated" || !rotatedTo.Valid || rotatedTo.String != second.SessionID {
		t.Errorf("old session (%q, link %+v), want rotated → %q", status, rotatedTo, second.SessionID)
	}
	// The successor inherits the thread's floor and cwd from the
	// durable old row (fresh seeding is C6's; identity is ours).
	if second.TrustFloor != first.TrustFloor || second.Cwd != first.Cwd {
		t.Errorf("successor (floor %q, cwd %q), want inherited (%q, %q)", second.TrustFloor, second.Cwd, first.TrustFloor, first.Cwd)
	}
}

// TestTurnRotatesOnTurnCap: the cap counts completed turns; reaching
// it rotates before the next event runs.
func TestTurnRotatesOnTurnCap(t *testing.T) {
	db := mustOpen(t)
	clock := int64(1700000000)
	eng := &resumeEngine{}
	m := rotationManager(db, eng, &clock) // TurnCap: 3
	cwd := t.TempDir()

	first := turnAndResolve(t, m, db, cwd)
	for i := 0; i < 2; i++ {
		clock += 10
		turnAndResolve(t, m, db, cwd)
	}
	// 3 turns completed on the first session — the cap. Next event
	// must rotate.
	clock += 10
	second := turnAndResolve(t, m, db, cwd)
	if second.SessionID == first.SessionID {
		t.Fatal("capped session kept resuming, want rotation")
	}
	if second.Turns != 1 {
		t.Errorf("successor turns = %d, want 1 (its own first turn only)", second.Turns)
	}
	if len(eng.specs) != 2 || len(eng.resumes) != 2 {
		t.Errorf("starts=%d resumes=%d, want 2 and 2", len(eng.specs), len(eng.resumes))
	}
}

// TestRotateNow: the /new command path — explicit rotation of the
// active session, regardless of caps (§3).
func TestRotateNow(t *testing.T) {
	db := mustOpen(t)
	clock := int64(1700000000)
	eng := &resumeEngine{}
	m := rotationManager(db, eng, &clock)
	cwd := t.TempDir()

	first := turnAndResolve(t, m, db, cwd)
	successor, err := m.RotateNow(context.Background(), "discord:dm:a")
	if err != nil {
		t.Fatalf("RotateNow: %v", err)
	}
	if successor.SessionID == first.SessionID || successor.Status != "creating" {
		t.Errorf("RotateNow successor = %+v, want a fresh creating session", successor)
	}

	// Nothing active afterwards (successor is creating): /new again
	// refuses loudly rather than rotating a session that never spoke.
	if _, err := m.RotateNow(context.Background(), "discord:dm:a"); !errors.Is(err, store.ErrNotActive) {
		t.Errorf("RotateNow on a creating thread = %v, want ErrNotActive", err)
	}
	// And an empty thread refuses too.
	if _, err := m.RotateNow(context.Background(), "discord:dm:zzz"); err == nil {
		t.Error("RotateNow on an empty thread succeeded")
	}
}

// TestRotationPreservesPinDiscipline: the successor is born creating
// with a daemon-minted id and its own activation window — the next
// event's StartNew runs against it exactly like any fresh pin.
func TestRotationPreservesPinDiscipline(t *testing.T) {
	db := mustOpen(t)
	clock := int64(1700000000)
	eng := &resumeEngine{}
	m := rotationManager(db, eng, &clock)
	cwd := t.TempDir()

	turnAndResolve(t, m, db, cwd)
	successor, err := m.RotateNow(context.Background(), "discord:dm:a")
	if err != nil {
		t.Fatalf("RotateNow: %v", err)
	}
	if successor.ActivationDeadline != clock+120 {
		t.Errorf("successor deadline = %d, want now + window = %d", successor.ActivationDeadline, clock+120)
	}

	// Next event: first turn on the successor.
	live := turnAndResolve(t, m, db, cwd)
	if live.SessionID != successor.SessionID || live.Status != "active" {
		t.Errorf("after post-rotation turn: %+v, want the successor active", live)
	}
}
