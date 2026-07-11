package store_test

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/brian-bell/approach/internal/store"
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

// TestSessionsSessionIDGloballyUnique: session_id is addressed ALONE by
// session_facts/approvals/log_entries (§6), so it must be unique across
// the whole table — two threads sharing an id would cross-wire facts
// and approvals between conversations.
func TestSessionsSessionIDGloballyUnique(t *testing.T) {
	db := mustOpen(t, filepath.Join(t.TempDir(), "state", "approach.db"))

	if err := insertSession(db, "discord:dm:1", "shared", "rotated", "/home", "owner"); err != nil {
		t.Fatalf("first session: %v", err)
	}
	// A different thread_key (so the composite PK passes) and a
	// non-live status (so one_live_session is out of the way): only
	// sessions_by_id stands between this insert and the table.
	if err := insertSession(db, "discord:dm:2", "shared", "rotated", "/home", "owner"); err == nil {
		t.Error("second session reusing a session_id accepted, want sessions_by_id violation")
	}
}

// testSession returns a valid session bound for InsertSession, rooted
// in a real directory (cwd must resolve).
func testSession(t *testing.T) store.Session {
	t.Helper()
	return store.Session{
		ThreadKey:          "discord:dm:123",
		SessionID:          "11111111-1111-1111-1111-111111111111",
		Cwd:                t.TempDir(),
		TrustFloor:         "owner",
		CreatedAt:          1700000000,
		ActivationDeadline: 1700000060,
	}
}

// TestInsertSessionPersistsCreating: every session is born 'creating'
// (§4.1) — the daemon pins the UUID and spawns; activation flips the
// status later. The schema default owns the birth state; InsertSession
// never lets a caller choose it.
func TestInsertSessionPersistsCreating(t *testing.T) {
	db := mustOpen(t, filepath.Join(t.TempDir(), "state", "approach.db"))

	s := testSession(t)
	s.Origin = "discord:dm:origin"
	if err := store.InsertSession(context.Background(), db, s); err != nil {
		t.Fatalf("InsertSession: %v", err)
	}

	var threadKey, status, cwd, origin, trustFloor string
	var createdAt, deadline int64
	if err := db.QueryRow(
		`SELECT thread_key, status, cwd, origin, trust_floor, created_at, activation_deadline
		 FROM sessions WHERE session_id = ?`,
		s.SessionID,
	).Scan(&threadKey, &status, &cwd, &origin, &trustFloor, &createdAt, &deadline); err != nil {
		t.Fatalf("read back session: %v", err)
	}
	if threadKey != s.ThreadKey || origin != s.Origin || trustFloor != s.TrustFloor || createdAt != s.CreatedAt {
		t.Errorf("session fields did not round-trip: got (%q, %q, %q, %d)", threadKey, origin, trustFloor, createdAt)
	}
	if deadline != s.ActivationDeadline {
		t.Errorf("activation_deadline = %d, want %d — a creating row without a deadline can wedge one_live_session forever (§4.1)", deadline, s.ActivationDeadline)
	}
	if status != "creating" {
		t.Errorf("status = %q at insert, want 'creating'", status)
	}
	want, err := filepath.EvalSymlinks(s.Cwd)
	if err != nil {
		t.Fatalf("canonicalize expected cwd: %v", err)
	}
	if cwd != want {
		t.Errorf("cwd stored as %q, want canonical %q", cwd, want)
	}
}

// TestInsertSessionFailsLoudOnInvalidFields: a session row the daemon
// cannot honestly act on — no thread, no id, a cwd that doesn't resolve
// — is refused, never stored. The daemon spawns the engine from
// sessions.cwd (§4.1, §6); a row pointing nowhere is a broken promise.
func TestInsertSessionFailsLoudOnInvalidFields(t *testing.T) {
	db := mustOpen(t, filepath.Join(t.TempDir(), "state", "approach.db"))

	cases := []struct {
		name   string
		mutate func(*testing.T, *store.Session)
	}{
		{"empty thread_key", func(_ *testing.T, s *store.Session) { s.ThreadKey = "" }},
		{"empty session_id", func(_ *testing.T, s *store.Session) { s.SessionID = "" }},
		{"empty cwd", func(_ *testing.T, s *store.Session) { s.Cwd = "" }},
		{"empty trust_floor", func(_ *testing.T, s *store.Session) { s.TrustFloor = "" }},
		{"zero created_at", func(_ *testing.T, s *store.Session) { s.CreatedAt = 0 }},
		{"zero activation_deadline", func(_ *testing.T, s *store.Session) { s.ActivationDeadline = 0 }},
		{"activation_deadline at created_at", func(_ *testing.T, s *store.Session) { s.ActivationDeadline = s.CreatedAt }},
		{"activation_deadline before created_at", func(_ *testing.T, s *store.Session) { s.ActivationDeadline = s.CreatedAt - 1 }},
		{"nonexistent cwd", func(_ *testing.T, s *store.Session) { s.Cwd = filepath.Join(s.Cwd, "gone") }},
		{"task worker without origin", func(_ *testing.T, s *store.Session) {
			s.ThreadKey = "task:approach-1"
			s.Origin = ""
		}},
		{"cwd is a regular file", func(t *testing.T, s *store.Session) {
			file := filepath.Join(s.Cwd, "file")
			if err := os.WriteFile(file, []byte("x"), 0o600); err != nil {
				t.Fatalf("write file: %v", err)
			}
			s.Cwd = file
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			s := testSession(t)
			tc.mutate(t, &s)
			if err := store.InsertSession(context.Background(), db, s); err == nil {
				t.Errorf("InsertSession accepted session with %s, want error", tc.name)
			}
		})
	}
	var n int
	if err := db.QueryRow(`SELECT count(*) FROM sessions`).Scan(&n); err != nil {
		t.Fatalf("count sessions: %v", err)
	}
	if n != 0 {
		t.Errorf("%d rows landed from invalid sessions, want 0", n)
	}
}

// TestInsertSessionCanonicalizesWorkerCwd: the §4.5/§7 X8 exclusivity
// drill (approach-8yp) — one_worker_per_repo compares cwd TEXT, so the
// invariant only holds if every spelling of a checkout collapses to one
// canonical path before insert. A symlink alias and a dot-dot spelling
// of the same repo must both collide with the live worker.
func TestInsertSessionCanonicalizesWorkerCwd(t *testing.T) {
	db := mustOpen(t, filepath.Join(t.TempDir(), "state", "approach.db"))

	base := t.TempDir()
	repo := filepath.Join(base, "repo")
	if err := os.Mkdir(repo, 0o700); err != nil {
		t.Fatalf("mkdir repo: %v", err)
	}
	link := filepath.Join(base, "repo-link")
	if err := os.Symlink(repo, link); err != nil {
		t.Fatalf("symlink repo: %v", err)
	}

	w1 := testSession(t)
	w1.ThreadKey = "task:approach-1"
	w1.SessionID = "w1"
	w1.Origin = "discord:dm:origin"
	w1.Cwd = repo
	if err := store.InsertSession(context.Background(), db, w1); err != nil {
		t.Fatalf("first worker: %v", err)
	}

	spellings := []struct{ name, cwd string }{
		{"symlink alias", link},
		// Built by concatenation, NOT filepath.Join — Join pre-cleans
		// the dots, and the drill is that InsertSession must.
		{"dot-dot spelling", base + string(filepath.Separator) + "repo-link" + string(filepath.Separator) + ".." + string(filepath.Separator) + "repo"},
		{"trailing slash", repo + string(filepath.Separator)},
	}
	for i, sp := range spellings {
		w := testSession(t)
		w.ThreadKey = fmt.Sprintf("task:approach-%d", i+2)
		w.SessionID = fmt.Sprintf("w%d", i+2)
		w.Origin = "discord:dm:origin"
		w.Cwd = sp.cwd
		if err := store.InsertSession(context.Background(), db, w); err == nil {
			t.Errorf("second live worker via %s %q accepted, want one_worker_per_repo violation", sp.name, sp.cwd)
		}
	}

	// A non-worker session on the same repo stays legal through the new
	// path — exclusivity is workers-only (§4.5).
	dm := testSession(t)
	dm.ThreadKey = "discord:dm:9"
	dm.SessionID = "s9"
	dm.Cwd = link
	if err := store.InsertSession(context.Background(), db, dm); err != nil {
		t.Errorf("non-worker session sharing the repo: %v — exclusivity is workers-only", err)
	}
}
