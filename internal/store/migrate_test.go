package store_test

import (
	"context"
	"errors"
	"path/filepath"
	"testing"

	"github.com/brian-bell/approach/internal/store"
)

// TestOpenAppliesEmbeddedMigrations: Open is the daemon's startup path,
// so a freshly opened store is already migrated (§6) — nobody can hold
// an unmigrated handle.
func TestOpenAppliesEmbeddedMigrations(t *testing.T) {
	db := mustOpen(t, filepath.Join(t.TempDir(), "state", "approach.db"))

	var version int
	if err := db.QueryRow("PRAGMA user_version").Scan(&version); err != nil {
		t.Fatalf("read user_version: %v", err)
	}
	if version < 1 {
		t.Errorf("user_version = %d after Open, want >= 1 (embedded migrations applied)", version)
	}
}

// TestOpenRefusesNewerSchemaVersion: Open is the daemon's startup path,
// so a schema version beyond the embedded set — a db written by a newer
// binary — must refuse startup outright (downgrade protection, §6).
func TestOpenRefusesNewerSchemaVersion(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state", "approach.db")
	db := mustOpen(t, path)
	if _, err := db.Exec("PRAGMA user_version = 9999"); err != nil {
		t.Fatalf("set user_version: %v", err)
	}

	_, err := store.Open(path)
	if err == nil {
		t.Fatal("Open succeeded on schema version 9999, want refusal")
	}
	if !errors.Is(err, store.ErrSchemaTooNew) {
		t.Errorf("Open error %q is not store.ErrSchemaTooNew", err)
	}
}

// TestMigrateEmbeddedSetIsValid fails the build's tests if anyone adds
// a misnumbered or misnamed file to the embedded migrations directory.
func TestMigrateEmbeddedSetIsValid(t *testing.T) {
	db := mustOpen(t, filepath.Join(t.TempDir(), "state", "approach.db"))

	if err := store.Migrate(context.Background(), db); err != nil {
		t.Fatalf("Migrate on the embedded set: %v", err)
	}
}
