package store

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"embed"
	"errors"
	"fmt"
	"io/fs"
	"regexp"
	"sort"
	"strconv"
)

// The whole directory is embedded, not a *.sql glob: a glob would
// silently drop a misnamed migration (0002_users.sq, 0002_users.sql.bak)
// before loadMigrations could refuse it, shipping a binary without the
// schema it was meant to carry. Plain directory embedding still skips
// dot- and underscore-prefixed names, so OS/editor junk like .DS_Store
// cannot wedge the build; everything else misnamed fails loud.
//
//go:embed migrations
var embeddedMigrations embed.FS

// Migrate applies the schema migrations embedded in the binary to db:
// numbered NNNN_description.sql files, every pending one applied in a
// single transaction, with PRAGMA user_version tracking the schema
// version (§6). Open calls this, so an opened store is always migrated.
func Migrate(ctx context.Context, db *sql.DB) error {
	fsys, err := fs.Sub(embeddedMigrations, "migrations")
	if err != nil {
		return fmt.Errorf("store: embedded migrations: %w", err)
	}
	return migrate(ctx, db, fsys)
}

// migrationName is the required shape of a migration filename: a
// zero-padded number, an underscore, a description, and .sql.
var migrationName = regexp.MustCompile(`^(\d{4})_[^.]+\.sql$`)

// migration is one numbered SQL file, loaded and ready to apply.
type migration struct {
	number int
	name   string
	sql    string
}

// migrate applies the numbered migrations in fsys to db in order.
func migrate(ctx context.Context, db *sql.DB, fsys fs.FS) (err error) {
	migrations, err := loadMigrations(fsys)
	if err != nil {
		return err
	}
	// One dedicated connection for the whole batch: the transaction and
	// the PRAGMA reads must not scatter across the pool.
	conn, err := db.Conn(ctx)
	if err != nil {
		return fmt.Errorf("store: migrate: %w", err)
	}
	defer func() {
		if cerr := conn.Close(); cerr != nil {
			err = errors.Join(err, fmt.Errorf("store: release migration connection: %w", cerr))
		}
	}()

	// BEGIN IMMEDIATE, not a deferred database/sql tx: under WAL a
	// deferred read-then-write upgrade can fail with SQLITE_BUSY_SNAPSHOT,
	// which busy_timeout does not retry. Taking the write lock upfront
	// queues politely behind the busy_timeout Open sets — and the version
	// must be read INSIDE the lock, or a second migrator racing this one
	// reads a stale version and re-applies committed migrations.
	if _, err := conn.ExecContext(ctx, "BEGIN IMMEDIATE"); err != nil {
		return fmt.Errorf("store: begin migration transaction: %w", err)
	}
	if err := applyPending(ctx, conn, migrations); err != nil {
		return errors.Join(err, rollback(conn))
	}
	if _, err := conn.ExecContext(ctx, "COMMIT"); err != nil {
		return errors.Join(fmt.Errorf("store: commit migrations: %w", err), rollback(conn))
	}
	return nil
}

// rollback unwinds the open migration transaction on conn. It runs on
// a fresh context, not the caller's: the batch may be failing exactly
// because that context was canceled, and a skipped ROLLBACK would send
// the connection back to the pool still holding the write transaction —
// modernc's session reset does not roll back for us. If ROLLBACK itself
// fails the connection is poisoned so the pool discards it (closing a
// SQLite connection rolls back whatever it still holds).
func rollback(conn *sql.Conn) error {
	if _, err := conn.ExecContext(context.Background(), "ROLLBACK"); err != nil {
		_ = conn.Raw(func(any) error { return driver.ErrBadConn })
		return fmt.Errorf("store: rollback: %w", err)
	}
	return nil
}

// applyPending runs every migration newer than the schema version, and
// the version bump, on conn, which must hold the write lock. The bump
// rides the same transaction — user_version lives in the database
// header, so it commits or rolls back with the batch.
func applyPending(ctx context.Context, conn *sql.Conn, migrations []migration) error {
	var version int
	if err := conn.QueryRowContext(ctx, "PRAGMA user_version").Scan(&version); err != nil {
		return fmt.Errorf("store: read schema version: %w", err)
	}
	// A version beyond the embedded set means the db was written by a
	// newer binary; today that is a silent no-op — approach-1zr.1.5 turns
	// it into a hard refusal (downgrade protection, §6).
	applied := false
	for _, m := range migrations {
		if m.number <= version {
			continue
		}
		if _, err := conn.ExecContext(ctx, m.sql); err != nil {
			return fmt.Errorf("store: migration %s: %w", m.name, err)
		}
		if err := requireInTransaction(ctx, conn, m.name); err != nil {
			return err
		}
		applied = true
	}
	if !applied {
		return nil
	}
	last := migrations[len(migrations)-1].number
	if _, err := conn.ExecContext(ctx, fmt.Sprintf("PRAGMA user_version = %d", last)); err != nil {
		return fmt.Errorf("store: set schema version %d: %w", last, err)
	}
	return nil
}

// requireInTransaction verifies the batch transaction is still open
// after running a migration, by probing with BEGIN: SQLite refuses a
// nested BEGIN inside a transaction, so a probe that SUCCEEDS means the
// migration carried its own COMMIT or ROLLBACK and escaped the batch.
// The runner does not parse SQL (trigger bodies legitimately contain
// BEGIN...END), so escaped statements may already have persisted —
// failing loud here means the embedded-set test refuses such a
// migration at build time, before it ever ships.
func requireInTransaction(ctx context.Context, conn *sql.Conn, name string) error {
	if _, err := conn.ExecContext(ctx, "BEGIN"); err != nil {
		return nil // nested BEGIN refused — still inside the batch transaction
	}
	// The probe's transaction is left open deliberately: migrate's error
	// path rolls it back, leaving the connection clean.
	return fmt.Errorf("store: migration %s: closed the batch transaction — migrations must not contain COMMIT/ROLLBACK/BEGIN", name)
}

// loadMigrations reads every migration in fsys, ordered by number.
func loadMigrations(fsys fs.FS) ([]migration, error) {
	entries, err := fs.ReadDir(fsys, ".")
	if err != nil {
		return nil, fmt.Errorf("store: read migrations: %w", err)
	}
	var migrations []migration
	for _, entry := range entries {
		name := entry.Name()
		m := migrationName.FindStringSubmatch(name)
		if m == nil {
			return nil, fmt.Errorf("store: unexpected file %s in migrations: names must match NNNN_description.sql", name)
		}
		number, err := strconv.Atoi(m[1])
		if err != nil {
			return nil, fmt.Errorf("store: migration %s: bad number: %w", name, err)
		}
		contents, err := fs.ReadFile(fsys, name)
		if err != nil {
			return nil, fmt.Errorf("store: migration %s: %w", name, err)
		}
		migrations = append(migrations, migration{number: number, name: name, sql: string(contents)})
	}
	sort.Slice(migrations, func(i, j int) bool { return migrations[i].number < migrations[j].number })
	for i, m := range migrations {
		if i > 0 && m.number == migrations[i-1].number {
			return nil, fmt.Errorf("store: duplicate migration number %04d: %s and %s", m.number, migrations[i-1].name, m.name)
		}
		if m.number != i+1 {
			return nil, fmt.Errorf("store: migration %s: numbering must be contiguous from 0001, expected %04d here", m.name, i+1)
		}
	}
	return migrations, nil
}
