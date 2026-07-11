package store_test

import (
	"database/sql"
	"fmt"
	"path/filepath"
	"testing"
)

// insertSession inserts a minimal sessions row, returning the error.
func insertSession(db *sql.DB, threadKey, sessionID, status, cwd, trustFloor string) error {
	_, err := db.Exec(
		`INSERT INTO sessions (thread_key, session_id, status, cwd, trust_floor, created_at)
		 VALUES (?, ?, ?, ?, ?, 0)`,
		threadKey, sessionID, status, cwd, trustFloor,
	)
	return err
}

// TestSessionsTrustFloorColumn: every session carries a trust_floor —
// the least-trusted participant the thread can admit (§4.3, §7) — and
// the schema holds it to the closed participant set: retrieval and the
// policy matrix key off this column, so a junk or missing value must be
// unrepresentable, not a convention.
func TestSessionsTrustFloorColumn(t *testing.T) {
	db := mustOpen(t, filepath.Join(t.TempDir(), "state", "approach.db"))

	for i, floor := range []string{"owner", "known", "untrusted"} {
		if err := insertSession(db, fmt.Sprintf("discord:dm:%d", i), fmt.Sprintf("s%d", i), "active", "/home", floor); err != nil {
			t.Errorf("insert session with trust_floor %q: %v", floor, err)
		}
	}
	if err := insertSession(db, "discord:dm:x", "sx", "active", "/home", "system"); err == nil {
		t.Error("trust_floor 'system' accepted — the floor is a participant level, want CHECK violation")
	}
	if _, err := db.Exec(
		`INSERT INTO sessions (thread_key, session_id, status, cwd, created_at) VALUES ('discord:dm:y', 'sy', 'active', '/home', 0)`,
	); err == nil {
		t.Error("session without trust_floor accepted, want NOT NULL violation")
	}
}

// TestSessionsOneLiveSessionPerThread: THE router invariant is a schema
// constraint, not a convention (§6) — a second creating/active session
// on one thread is a unique violation, and rotation frees the slot.
func TestSessionsOneLiveSessionPerThread(t *testing.T) {
	db := mustOpen(t, filepath.Join(t.TempDir(), "state", "approach.db"))

	if err := insertSession(db, "discord:dm:1", "s1", "active", "/home", "owner"); err != nil {
		t.Fatalf("first live session: %v", err)
	}
	if err := insertSession(db, "discord:dm:1", "s2", "creating", "/home", "owner"); err == nil {
		t.Error("second live session on one thread accepted, want one_live_session violation")
	}
	if _, err := db.Exec(`UPDATE sessions SET status = 'rotated' WHERE session_id = 's1'`); err != nil {
		t.Fatalf("rotate s1: %v", err)
	}
	if err := insertSession(db, "discord:dm:1", "s2", "creating", "/home", "owner"); err != nil {
		t.Errorf("live session after rotation: %v — rotation must free the thread's slot", err)
	}
}

// TestSessionsOneWorkerPerRepo: at most one live task:* worker per repo
// path (§4.5, §7 X8) — also a schema constraint. Non-worker sessions
// sharing a cwd stay legal.
func TestSessionsOneWorkerPerRepo(t *testing.T) {
	db := mustOpen(t, filepath.Join(t.TempDir(), "state", "approach.db"))

	if err := insertSession(db, "task:approach-1", "w1", "active", "/repo", "owner"); err != nil {
		t.Fatalf("first worker: %v", err)
	}
	if err := insertSession(db, "task:approach-2", "w2", "creating", "/repo", "owner"); err == nil {
		t.Error("second live worker on one repo accepted, want one_worker_per_repo violation")
	}
	if err := insertSession(db, "discord:dm:1", "s1", "active", "/repo", "owner"); err != nil {
		t.Errorf("non-worker session sharing the repo cwd: %v — exclusivity is workers-only", err)
	}
}
