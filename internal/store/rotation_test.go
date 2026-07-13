package store_test

import (
	"context"
	"database/sql"
	"errors"
	"path/filepath"
	"testing"

	"github.com/brian-bell/approach/internal/store"
)

// liveSeed inserts a creating session and optionally activates it.
func liveSeed(t *testing.T, db *sql.DB, cwd string, activate bool) string {
	const threadKey = "discord:dm:a"
	const id = "11111111-1111-4111-8111-111111111111"
	t.Helper()
	s := store.Session{
		ThreadKey:          threadKey,
		SessionID:          id,
		Cwd:                cwd,
		TrustFloor:         "owner",
		CreatedAt:          1700000000,
		ActivationDeadline: 1700000120,
	}
	if _, err := store.InsertSession(context.Background(), db, s); err != nil {
		t.Fatalf("seed session: %v", err)
	}
	if activate {
		if err := store.ActivateSession(context.Background(), db, id); err != nil {
			t.Fatalf("activate seed: %v", err)
		}
	}
	return id
}

// TestRotateSession: THE §6 rotation transaction — old active →
// rotated with a rotated_to link, successor born creating, same
// thread — with one_live_session intact throughout (bad interleaving
// is a constraint violation, not a race).
func TestRotateSession(t *testing.T) {
	db := mustOpen(t, filepath.Join(t.TempDir(), "state", "approach.db"))
	ctx := context.Background()
	cwd := t.TempDir()
	newID := "22222222-2222-4222-8222-222222222222"
	oldID := liveSeed(t, db, cwd, true)

	successor := store.Session{
		ThreadKey:          "discord:dm:a",
		SessionID:          newID,
		Cwd:                cwd,
		TrustFloor:         "owner",
		CreatedAt:          1700005000,
		ActivationDeadline: 1700005120,
	}
	if _, err := store.RotateSession(ctx, db, oldID, successor); err != nil {
		t.Fatalf("RotateSession: %v", err)
	}

	var status string
	var rotatedTo sql.NullString
	if err := db.QueryRow(`SELECT status, rotated_to FROM sessions WHERE session_id = ?`, oldID).Scan(&status, &rotatedTo); err != nil {
		t.Fatalf("read back old: %v", err)
	}
	if status != "rotated" {
		t.Errorf("old session status = %q, want rotated", status)
	}
	if !rotatedTo.Valid || rotatedTo.String != newID {
		t.Errorf("rotated_to = %+v, want the successor link %q (§6)", rotatedTo, newID)
	}

	live, ok, err := store.ResolveLiveSession(ctx, db, "discord:dm:a")
	if err != nil || !ok {
		t.Fatalf("resolve successor: ok=%v err=%v", ok, err)
	}
	if live.SessionID != newID || live.Status != "creating" {
		t.Errorf("live after rotation = %+v, want the creating successor", live)
	}
}

// TestRotateSessionGuards: only an ACTIVE session rotates — rotating a
// creating, already-rotated, or unknown id is a loud typed error and
// leaves no successor row behind.
func TestRotateSessionGuards(t *testing.T) {
	db := mustOpen(t, filepath.Join(t.TempDir(), "state", "approach.db"))
	ctx := context.Background()
	cwd := t.TempDir()
	liveSeed(t, db, cwd, false) // creating

	successor := store.Session{
		ThreadKey:          "discord:dm:b", // different thread so one_live_session isn't the failure
		SessionID:          "22222222-2222-4222-8222-222222222222",
		Cwd:                cwd,
		TrustFloor:         "owner",
		CreatedAt:          1700005000,
		ActivationDeadline: 1700005120,
	}
	for _, oldID := range []string{
		"11111111-1111-4111-8111-111111111111", // creating, not active
		"99999999-9999-4999-8999-999999999999", // unknown
	} {
		if _, err := store.RotateSession(ctx, db, oldID, successor); !errors.Is(err, store.ErrNotActive) {
			t.Errorf("RotateSession(%s) = %v, want ErrNotActive", oldID, err)
		}
	}
	var n int
	if err := db.QueryRow(`SELECT count(*) FROM sessions`).Scan(&n); err != nil {
		t.Fatalf("count: %v", err)
	}
	if n != 1 {
		t.Errorf("refused rotations left %d rows, want the original 1 — no successor may survive a rollback", n)
	}
}

// TestRotateSessionRefusesCrossThreadSuccessor: the successor must
// belong to the retired session's thread — otherwise the transaction
// would leave thread A with no live session and link its history into
// thread B's conversation.
func TestRotateSessionRefusesCrossThreadSuccessor(t *testing.T) {
	db := mustOpen(t, filepath.Join(t.TempDir(), "state", "approach.db"))
	ctx := context.Background()
	cwd := t.TempDir()
	oldID := liveSeed(t, db, cwd, true) // active on discord:dm:a

	crossThread := store.Session{
		ThreadKey:          "discord:dm:b",
		SessionID:          "22222222-2222-4222-8222-222222222222",
		Cwd:                cwd,
		TrustFloor:         "owner",
		CreatedAt:          1700005000,
		ActivationDeadline: 1700005120,
	}
	if _, err := store.RotateSession(ctx, db, oldID, crossThread); !errors.Is(err, store.ErrNotActive) {
		t.Fatalf("cross-thread rotation = %v, want ErrNotActive", err)
	}
	live, ok, err := store.ResolveLiveSession(ctx, db, "discord:dm:a")
	if err != nil || !ok || live.SessionID != oldID || live.Status != "active" {
		t.Errorf("after refused cross-thread rotation: ok=%v %+v, want the original still active", ok, live)
	}
}

// TestRotateSessionRollsBackAtomically: if the successor insert fails
// (unresolvable cwd), the old session must still be active — a
// half-rotation would leave the thread with no live session at all.
func TestRotateSessionRollsBackAtomically(t *testing.T) {
	db := mustOpen(t, filepath.Join(t.TempDir(), "state", "approach.db"))
	ctx := context.Background()
	oldID := liveSeed(t, db, t.TempDir(), true)

	bad := store.Session{
		ThreadKey:          "discord:dm:a",
		SessionID:          "22222222-2222-4222-8222-222222222222",
		Cwd:                filepath.Join(t.TempDir(), "does-not-exist"),
		TrustFloor:         "owner",
		CreatedAt:          1700005000,
		ActivationDeadline: 1700005120,
	}
	if _, err := store.RotateSession(ctx, db, oldID, bad); err == nil {
		t.Fatal("RotateSession accepted an unresolvable successor cwd")
	}
	live, ok, err := store.ResolveLiveSession(ctx, db, "discord:dm:a")
	if err != nil || !ok {
		t.Fatalf("resolve after rollback: ok=%v err=%v", ok, err)
	}
	if live.SessionID != oldID || live.Status != "active" {
		t.Errorf("after failed rotation: %+v, want the original still active", live)
	}
}

// TestTouchSession: the idle-TTL / turn-cap bookkeeping (§6) — turns
// counts from NULL, last_seen moves forward.
func TestTouchSession(t *testing.T) {
	db := mustOpen(t, filepath.Join(t.TempDir(), "state", "approach.db"))
	ctx := context.Background()
	id := liveSeed(t, db, t.TempDir(), true)

	for i, want := range []int64{1, 2} {
		if err := store.TouchSession(ctx, db, id, 1700001000+int64(i)); err != nil {
			t.Fatalf("TouchSession %d: %v", i, err)
		}
		var lastSeen, turns int64
		if err := db.QueryRow(`SELECT last_seen, turns FROM sessions WHERE session_id = ?`, id).Scan(&lastSeen, &turns); err != nil {
			t.Fatalf("read back: %v", err)
		}
		if turns != want || lastSeen != 1700001000+int64(i) {
			t.Errorf("after touch %d: turns=%d last_seen=%d, want %d, %d", i, turns, lastSeen, want, 1700001000+int64(i))
		}
	}

	// The live view carries both — rotation decisions read them.
	live, _, err := store.ResolveLiveSession(ctx, db, "discord:dm:a")
	if err != nil {
		t.Fatalf("resolve: %v", err)
	}
	if live.Turns != 2 || live.LastSeen != 1700001001 {
		t.Errorf("live view turns=%d last_seen=%d, want 2, 1700001001", live.Turns, live.LastSeen)
	}
}

// TestTouchSessionUnknown: touching a session that does not exist is a
// loud error — silent bookkeeping loss would break rotation triggers.
func TestTouchSessionUnknown(t *testing.T) {
	db := mustOpen(t, filepath.Join(t.TempDir(), "state", "approach.db"))
	if err := store.TouchSession(context.Background(), db, "99999999-9999-4999-8999-999999999999", 1700001000); err == nil {
		t.Error("TouchSession on an unknown id succeeded silently")
	}
}
