// Package store opens the approach.db SQLite state store with the §6
// posture applied and verified: WAL journaling, synchronous=NORMAL,
// busy_timeout, foreign keys on, state/ at 0700 and the db file at 0600.
package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net/url"
	"os"
	"path/filepath"

	_ "modernc.org/sqlite"
)

// Open opens (creating if needed) the SQLite database at path.
func Open(path string) (*sql.DB, error) {
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return nil, fmt.Errorf("store: %w", err)
	}
	if err := requireMode(dir, 0o700); err != nil {
		return nil, err
	}

	// Pre-create the db file so it exists at 0600 before SQLite touches
	// it: sql.Open is lazy, SQLite's own create mode is wider, and the
	// -wal/-shm sidecars inherit the main file's mode.
	f, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil && !os.IsExist(err) {
		return nil, fmt.Errorf("store: %w", err)
	}
	if err == nil {
		if err := f.Close(); err != nil {
			return nil, fmt.Errorf("store: %w", err)
		}
	}
	if err := requireMode(path, 0o600); err != nil {
		return nil, err
	}
	// Sidecars from an earlier run carry database pages and SQLite will
	// keep using them, so they get the same posture check as the main
	// file: -wal/-shm are live under WAL, and a -journal left by a tool
	// that opened the db in rollback mode is consumed during recovery.
	// Absent sidecars are fine — SQLite creates them inheriting the main
	// file's 0600.
	for _, suffix := range []string{"-wal", "-shm", "-journal"} {
		sidecar := path + suffix
		// Lstat, not Stat: a dangling symlink must be seen and refused,
		// not skipped as absent — SQLite writing through it would
		// create the target.
		if _, err := os.Lstat(sidecar); os.IsNotExist(err) {
			continue
		}
		if err := requireMode(sidecar, 0o600); err != nil {
			return nil, err
		}
	}

	// Pragmas ride the DSN because database/sql pools connections:
	// synchronous, busy_timeout, and foreign_keys are per-connection, so
	// a post-open Exec would configure one connection while the pool
	// hands out unconfigured ones. journal_mode=WAL persists in the file
	// but is harmless to re-assert per connection. Order matters:
	// busy_timeout must be set FIRST — the pragmas run in DSN order at
	// connection open, and re-asserting journal_mode needs a lock, so a
	// connection opened while another holds the write lock (e.g. during
	// a migration batch) would otherwise fail SQLITE_BUSY with no retry
	// budget.
	dsn := "file:" + url.PathEscape(path) +
		"?_pragma=busy_timeout(5000)" +
		"&_pragma=journal_mode(WAL)" +
		"&_pragma=synchronous(NORMAL)" +
		"&_pragma=foreign_keys(ON)"
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, fmt.Errorf("store: %w", err)
	}
	if err := verifyPragmas(db); err != nil {
		return nil, errors.Join(err, db.Close())
	}
	// Open is the daemon's startup path, so migrations apply here (§6):
	// a handle you can hold is a handle to a migrated store.
	if err := Migrate(context.Background(), db); err != nil {
		return nil, errors.Join(err, db.Close())
	}
	return db, nil
}

// verifyPragmas reads the §6 posture back rather than trusting the DSN:
// the _pragma syntax is driver-specific, and a typo there would
// otherwise degrade durability silently. It also forces a real
// connection, so Open fails loud on an unreadable database instead of
// deferring the error to the first query.
func verifyPragmas(db *sql.DB) error {
	for _, p := range []struct{ name, want string }{
		{"journal_mode", "wal"},
		{"synchronous", "1"}, // NORMAL
		{"busy_timeout", "5000"},
		{"foreign_keys", "1"},
	} {
		var got string
		if err := db.QueryRow("PRAGMA " + p.name).Scan(&got); err != nil {
			return fmt.Errorf("store: verify pragma %s: %w", p.name, err)
		}
		if got != p.want {
			return fmt.Errorf("store: pragma %s = %q, want %q", p.name, got, p.want)
		}
	}
	return nil
}

// requireMode refuses a symlink or a path whose permissions are wider
// than want. The store holds identities and approvals; auto-tightening
// would hide that the file sat exposed, so the caller is told to chmod
// instead. Symlinks are refused outright: a stat-follow check would
// approve the link's target while SQLite resolves the real database
// path and uses sidecars beside it, out of sight of these checks.
func requireMode(path string, want os.FileMode) error {
	info, err := os.Lstat(path)
	if err != nil {
		return fmt.Errorf("store: %w", err)
	}
	if info.Mode()&os.ModeSymlink != 0 {
		return fmt.Errorf("store: %s is a symlink — the state store refuses symlinked paths", path)
	}
	if got := info.Mode().Perm(); got != want {
		return fmt.Errorf("store: %s has mode %04o, want %04o — run: chmod %o %s", path, got, want, want, path)
	}
	return nil
}
