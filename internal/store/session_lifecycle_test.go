package store_test

import (
	"context"
	"errors"
	"path/filepath"
	"testing"

	"github.com/brian-bell/approach/internal/store"
)

// TestResolveLiveSession: the router's thread_key → session lookup
// (§4.1). Only creating/active rows are live; rotated and failed
// history must never resolve.
func TestResolveLiveSession(t *testing.T) {
	db := mustOpen(t, filepath.Join(t.TempDir(), "state", "approach.db"))
	ctx := context.Background()
	cwd := t.TempDir()

	// No session yet: not found, not an error.
	if _, ok, err := store.ResolveLiveSession(ctx, db, "discord:dm:a"); err != nil || ok {
		t.Fatalf("ResolveLiveSession on empty store = ok=%v err=%v, want false, nil", ok, err)
	}

	s := store.Session{
		ThreadKey:          "discord:dm:a",
		SessionID:          "11111111-1111-4111-8111-111111111111",
		Cwd:                cwd,
		TrustFloor:         "owner",
		CreatedAt:          1700000000,
		ActivationDeadline: 1700000120,
	}
	if _, err := store.InsertSession(ctx, db, s); err != nil {
		t.Fatalf("InsertSession: %v", err)
	}

	live, ok, err := store.ResolveLiveSession(ctx, db, "discord:dm:a")
	if err != nil || !ok {
		t.Fatalf("ResolveLiveSession = ok=%v err=%v, want found", ok, err)
	}
	if live.SessionID != s.SessionID || live.Status != "creating" || live.TrustFloor != "owner" ||
		live.CreatedAt != s.CreatedAt || live.ActivationDeadline != s.ActivationDeadline {
		t.Errorf("live session did not round-trip: %+v", live)
	}
	// Cwd comes back canonicalized — the daemon spawns from THIS value.
	if live.Cwd == "" {
		t.Errorf("live session carries no cwd — the daemon could not spawn (§6)")
	}

	// A failed row is not live: the thread resolves to nothing again.
	if err := store.FailSession(ctx, db, s.SessionID); err != nil {
		t.Fatalf("FailSession: %v", err)
	}
	if _, ok, _ := store.ResolveLiveSession(ctx, db, "discord:dm:a"); ok {
		t.Error("failed session still resolves as live")
	}
}

// TestActivateSession: the §4.1 creating → active transition — guarded
// so only a creating row activates, and anything else fails loud (an
// activation racing a deadline-fail must not resurrect the session).
func TestActivateSession(t *testing.T) {
	db := mustOpen(t, filepath.Join(t.TempDir(), "state", "approach.db"))
	ctx := context.Background()

	s := store.Session{
		ThreadKey:          "discord:dm:a",
		SessionID:          "11111111-1111-4111-8111-111111111111",
		Cwd:                t.TempDir(),
		TrustFloor:         "owner",
		CreatedAt:          1700000000,
		ActivationDeadline: 1700000120,
	}
	if _, err := store.InsertSession(ctx, db, s); err != nil {
		t.Fatalf("InsertSession: %v", err)
	}
	if err := store.ActivateSession(ctx, db, s.SessionID); err != nil {
		t.Fatalf("ActivateSession: %v", err)
	}
	var status string
	if err := db.QueryRow(`SELECT status FROM sessions WHERE session_id = ?`, s.SessionID).Scan(&status); err != nil {
		t.Fatalf("read back: %v", err)
	}
	if status != "active" {
		t.Errorf("status after activate = %q, want active", status)
	}

	// Second activation: the row is no longer creating — loud error.
	if err := store.ActivateSession(ctx, db, s.SessionID); !errors.Is(err, store.ErrNotCreating) {
		t.Errorf("re-activate error = %v, want ErrNotCreating", err)
	}
	// Unknown session id: same refusal, never silence.
	if err := store.ActivateSession(ctx, db, "99999999-9999-4999-8999-999999999999"); !errors.Is(err, store.ErrNotCreating) {
		t.Errorf("activate unknown session error = %v, want ErrNotCreating", err)
	}
}

// TestFailSession mirrors the guard: only a creating row can be marked
// failed (§4.1 deadline expiry) — an active session must never be
// clobbered by a stale deadline check.
func TestFailSession(t *testing.T) {
	db := mustOpen(t, filepath.Join(t.TempDir(), "state", "approach.db"))
	ctx := context.Background()

	s := store.Session{
		ThreadKey:          "discord:dm:a",
		SessionID:          "11111111-1111-4111-8111-111111111111",
		Cwd:                t.TempDir(),
		TrustFloor:         "owner",
		CreatedAt:          1700000000,
		ActivationDeadline: 1700000120,
	}
	if _, err := store.InsertSession(ctx, db, s); err != nil {
		t.Fatalf("InsertSession: %v", err)
	}
	if err := store.ActivateSession(ctx, db, s.SessionID); err != nil {
		t.Fatalf("ActivateSession: %v", err)
	}
	if err := store.FailSession(ctx, db, s.SessionID); !errors.Is(err, store.ErrNotCreating) {
		t.Errorf("FailSession on active row = %v, want ErrNotCreating (the guard)", err)
	}
	var status string
	if err := db.QueryRow(`SELECT status FROM sessions WHERE session_id = ?`, s.SessionID).Scan(&status); err != nil {
		t.Fatalf("read back: %v", err)
	}
	if status != "active" {
		t.Errorf("active session clobbered to %q by FailSession", status)
	}
}
