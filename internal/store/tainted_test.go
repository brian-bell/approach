package store_test

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/brian-bell/approach/internal/store"
)

// TestSessionsTaintedColumn: sessions carry the sticky tainted flag
// (§6, §7) — born clean (DEFAULT 0) and held to 0|1 by the schema, so
// "read one column right" can never see a third state.
func TestSessionsTaintedColumn(t *testing.T) {
	db := mustOpen(t, filepath.Join(t.TempDir(), "state", "approach.db"))

	if err := insertSession(db, "discord:dm:1", "s1", "active", "/home", "owner"); err != nil {
		t.Fatalf("insert session: %v", err)
	}
	var tainted int
	if err := db.QueryRow(`SELECT tainted FROM sessions WHERE session_id = 's1'`).Scan(&tainted); err != nil {
		t.Fatalf("read tainted: %v", err)
	}
	if tainted != 0 {
		t.Errorf("new session tainted = %d, want 0 (born clean)", tainted)
	}
	if _, err := db.Exec(`UPDATE sessions SET tainted = 2 WHERE session_id = 's1'`); err == nil {
		t.Error("tainted = 2 accepted, want CHECK violation (flag is 0|1)")
	}
	if _, err := db.Exec(`UPDATE sessions SET tainted = NULL WHERE session_id = 's1'`); err == nil {
		t.Error("tainted = NULL accepted, want NOT NULL violation")
	}
}

// TestMarkSessionTainted: the set-on-ingest plumbing (§7). Marking is
// sticky and idempotent, and a taint that lands on no session is an
// ERROR — externally-authored content whose taint silently vanished
// would be a policy bypass, not a no-op.
func TestMarkSessionTainted(t *testing.T) {
	db := mustOpen(t, filepath.Join(t.TempDir(), "state", "approach.db"))
	ctx := context.Background()
	if err := insertSession(db, "discord:dm:1", "s1", "active", "/home", "owner"); err != nil {
		t.Fatalf("insert session: %v", err)
	}

	tainted, err := store.SessionTainted(ctx, db, "s1")
	if err != nil {
		t.Fatalf("SessionTainted before mark: %v", err)
	}
	if tainted {
		t.Error("session tainted before any ingest, want clean")
	}

	if err := store.MarkSessionTainted(ctx, db, "s1"); err != nil {
		t.Fatalf("MarkSessionTainted: %v", err)
	}
	if err := store.MarkSessionTainted(ctx, db, "s1"); err != nil {
		t.Errorf("second MarkSessionTainted: %v — marking must be idempotent", err)
	}
	tainted, err = store.SessionTainted(ctx, db, "s1")
	if err != nil {
		t.Fatalf("SessionTainted after mark: %v", err)
	}
	if !tainted {
		t.Error("session clean after MarkSessionTainted, want tainted")
	}

	if err := store.MarkSessionTainted(ctx, db, "no-such-session"); err == nil {
		t.Error("MarkSessionTainted on unknown session succeeded, want error — a lost taint is a policy bypass")
	}
	if _, err := store.SessionTainted(ctx, db, "no-such-session"); err == nil {
		t.Error("SessionTainted on unknown session succeeded, want error — absent must not read as clean")
	}
}
