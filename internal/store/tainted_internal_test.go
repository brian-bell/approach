package store

import (
	"context"
	"io/fs"
	"testing"
	"testing/fstest"
)

// TestTaintedMigrationMarksExistingSessionsTainted: a session that
// predates the tainted column may already hold externally-authored
// content its flag never recorded, so upgrading must mark existing
// rows tainted, not assume them clean (fail safe) — while sessions
// created after the upgrade are born clean.
func TestTaintedMigrationMarksExistingSessionsTainted(t *testing.T) {
	db := openBare(t)
	ctx := context.Background()
	embedded, err := fs.Sub(embeddedMigrations, "migrations")
	if err != nil {
		t.Fatalf("embedded migrations: %v", err)
	}

	// Run the real embedded set truncated to just before 0004 — the
	// world an older binary left behind — and live a session in it.
	pre := fstest.MapFS{}
	for _, name := range []string{"0001_baseline.sql", "0002_identities.sql", "0003_sessions.sql"} {
		sql, err := fs.ReadFile(embedded, name)
		if err != nil {
			t.Fatalf("read embedded %s: %v", name, err)
		}
		pre[name] = &fstest.MapFile{Data: sql}
	}
	if err := migrate(ctx, db, pre); err != nil {
		t.Fatalf("migrate to pre-tainted schema: %v", err)
	}
	if _, err := db.Exec(
		`INSERT INTO sessions (thread_key, session_id, status, cwd, trust_floor, created_at)
		 VALUES ('discord:dm:1', 'old', 'active', '/home', 'owner', 0)`,
	); err != nil {
		t.Fatalf("insert pre-upgrade session: %v", err)
	}

	// Upgrade with the full embedded set.
	if err := Migrate(ctx, db); err != nil {
		t.Fatalf("migrate to current schema: %v", err)
	}

	var tainted bool
	if err := db.QueryRow(`SELECT tainted FROM sessions WHERE session_id = 'old'`).Scan(&tainted); err != nil {
		t.Fatalf("read pre-upgrade session taint: %v", err)
	}
	if !tainted {
		t.Error("pre-upgrade session is clean after adding the tainted column, want conservatively tainted")
	}

	if _, err := db.Exec(
		`INSERT INTO sessions (thread_key, session_id, status, cwd, trust_floor, created_at)
		 VALUES ('discord:dm:2', 'new', 'active', '/home', 'owner', 0)`,
	); err != nil {
		t.Fatalf("insert post-upgrade session: %v", err)
	}
	if err := db.QueryRow(`SELECT tainted FROM sessions WHERE session_id = 'new'`).Scan(&tainted); err != nil {
		t.Fatalf("read post-upgrade session taint: %v", err)
	}
	if tainted {
		t.Error("post-upgrade session born tainted, want clean (DEFAULT 0)")
	}
}
