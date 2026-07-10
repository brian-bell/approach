package store

import (
	"context"
	"database/sql"
	"fmt"
	"path/filepath"
	"strings"
	"testing"
	"testing/fstest"
	"time"
)

// openBare opens a raw database for fixture-driven migration tests —
// not store.Open, which applies the real embedded migrations and would
// leave user_version competing with the fixtures' numbering. The §6
// pragmas migrate runs under in production are asserted here too.
func openBare(t *testing.T) *sql.DB {
	t.Helper()
	dsn := "file:" + filepath.Join(t.TempDir(), "fixture.db") +
		"?_pragma=busy_timeout(5000)" +
		"&_pragma=journal_mode(WAL)" +
		"&_pragma=foreign_keys(ON)"
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		t.Fatalf("open fixture db: %v", err)
	}
	t.Cleanup(func() {
		if err := db.Close(); err != nil {
			t.Errorf("close: %v", err)
		}
	})
	return db
}

// migrationFS builds an in-memory migration set from name → SQL.
func migrationFS(files map[string]string) fstest.MapFS {
	fsys := fstest.MapFS{}
	for name, sql := range files {
		fsys[name] = &fstest.MapFile{Data: []byte(sql)}
	}
	return fsys
}

// TestMigrateOrdersNumberedFiles: fs.FS walks lexically, but the
// contract is numeric order — 0002 references 0001's table, so any
// other order fails to apply.
func TestMigrateOrdersNumberedFiles(t *testing.T) {
	db := openBare(t)
	fsys := migrationFS(map[string]string{
		"0001_people.sql": `CREATE TABLE people (name TEXT PRIMARY KEY);
INSERT INTO people (name) VALUES ('ada');`,
		"0002_pets.sql": `CREATE TABLE pets (
    owner TEXT NOT NULL REFERENCES people(name)
);
INSERT INTO pets (owner) SELECT name FROM people;`,
	})

	if err := migrate(context.Background(), db, fsys); err != nil {
		t.Fatalf("migrate: %v", err)
	}

	var owner string
	if err := db.QueryRow(`SELECT owner FROM pets`).Scan(&owner); err != nil {
		t.Fatalf("select from pets: %v", err)
	}
	if owner != "ada" {
		t.Errorf("pets.owner = %q, want %q", owner, "ada")
	}
}

func TestMigrateRefusesGapInNumbering(t *testing.T) {
	db := openBare(t)
	fsys := migrationFS(map[string]string{
		"0001_a.sql": `CREATE TABLE a (id INTEGER PRIMARY KEY);`,
		"0003_c.sql": `CREATE TABLE c (id INTEGER PRIMARY KEY);`,
	})

	err := migrate(context.Background(), db, fsys)
	if err == nil {
		t.Fatal("migrate succeeded on a gapped set (0001, 0003), want refusal")
	}
	var count int
	if qerr := db.QueryRow(`SELECT count(*) FROM sqlite_schema WHERE name = 'a'`).Scan(&count); qerr != nil {
		t.Fatalf("inspect schema: %v", qerr)
	}
	if count != 0 {
		t.Errorf("migration 0001 was applied despite the gap; db must stay untouched")
	}
}

func TestMigrateRefusesDuplicateNumber(t *testing.T) {
	db := openBare(t)
	fsys := migrationFS(map[string]string{
		"0001_a.sql": `CREATE TABLE a (id INTEGER PRIMARY KEY);`,
		"0001_b.sql": `CREATE TABLE b (id INTEGER PRIMARY KEY);`,
	})

	err := migrate(context.Background(), db, fsys)
	if err == nil {
		t.Fatal("migrate succeeded with duplicate number 0001, want refusal")
	}
	for _, want := range []string{"duplicate", "0001_a.sql", "0001_b.sql"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error %q does not mention %q", err, want)
		}
	}
}

// TestMigrateRefusesUnexpectedFiles: the embedded set is the schema's
// source of truth — a misnamed file must fail loud, not be skipped or
// half-parsed.
func TestMigrateRefusesUnexpectedFiles(t *testing.T) {
	for _, bad := range []string{
		"01_short.sql",   // number not 4 digits
		"abcd_x.sql",     // no number
		"0001-dash.sql",  // no underscore separator
		"0001_x.sql.bak", // wrong extension
		"README.md",      // stray non-migration file
	} {
		t.Run(bad, func(t *testing.T) {
			db := openBare(t)
			fsys := migrationFS(map[string]string{
				bad: `CREATE TABLE bad (id INTEGER PRIMARY KEY);`,
			})

			err := migrate(context.Background(), db, fsys)
			if err == nil {
				t.Fatalf("migrate succeeded with file %q in the set, want refusal", bad)
			}
			if !strings.Contains(err.Error(), bad) {
				t.Errorf("error %q does not name the offending file %q", err, bad)
			}
		})
	}
}

func TestMigrateIsIdempotent(t *testing.T) {
	db := openBare(t)
	// Plain CREATE TABLE: if a second run re-executed anything, SQLite
	// would error with "table already exists".
	fsys := migrationFS(map[string]string{
		"0001_a.sql": `CREATE TABLE a (id INTEGER PRIMARY KEY);`,
	})

	for run := 1; run <= 2; run++ {
		if err := migrate(context.Background(), db, fsys); err != nil {
			t.Fatalf("migrate run %d: %v", run, err)
		}
	}
}

// TestMigrateAppliesOnlyPending: a db already at version 1 gets only
// 0002..0003. 0001 INSERTs a row, so a re-run would be visible as a
// second row even where CREATE TABLE IF NOT EXISTS would hide it.
func TestMigrateAppliesOnlyPending(t *testing.T) {
	db := openBare(t)
	files := map[string]string{
		"0001_a.sql": `CREATE TABLE a (id INTEGER PRIMARY KEY);
INSERT INTO a DEFAULT VALUES;`,
	}
	if err := migrate(context.Background(), db, migrationFS(files)); err != nil {
		t.Fatalf("first migrate: %v", err)
	}

	files["0002_b.sql"] = `CREATE TABLE b (id INTEGER PRIMARY KEY);`
	files["0003_c.sql"] = `CREATE TABLE c (id INTEGER PRIMARY KEY);`
	if err := migrate(context.Background(), db, migrationFS(files)); err != nil {
		t.Fatalf("second migrate: %v", err)
	}

	var rows int
	if err := db.QueryRow(`SELECT count(*) FROM a`).Scan(&rows); err != nil {
		t.Fatalf("count a: %v", err)
	}
	if rows != 1 {
		t.Errorf("table a has %d rows, want 1 — migration 0001 re-ran", rows)
	}
	if _, err := db.Exec(`SELECT 1 FROM b, c`); err != nil {
		t.Errorf("pending migrations 0002/0003 not applied: %v", err)
	}
	var version int
	if err := db.QueryRow("PRAGMA user_version").Scan(&version); err != nil {
		t.Fatalf("read user_version: %v", err)
	}
	if version != 3 {
		t.Errorf("user_version = %d, want 3", version)
	}
}

// TestMigrateRollsBackAllOnFailure: §6 — all pending migrations apply
// in ONE transaction. A failure in 0002 must also unwind 0001, leaving
// the db exactly as it was.
func TestMigrateRollsBackAllOnFailure(t *testing.T) {
	db := openBare(t)
	fsys := migrationFS(map[string]string{
		"0001_a.sql":      `CREATE TABLE a (id INTEGER PRIMARY KEY);`,
		"0002_broken.sql": `CREATE TABLE syntax error here;`,
	})

	err := migrate(context.Background(), db, fsys)
	if err == nil {
		t.Fatal("migrate succeeded with broken 0002, want failure")
	}
	if !strings.Contains(err.Error(), "0002_broken.sql") {
		t.Errorf("error %q does not name the failing file", err)
	}

	var version int
	if err := db.QueryRow("PRAGMA user_version").Scan(&version); err != nil {
		t.Fatalf("read user_version: %v", err)
	}
	if version != 0 {
		t.Errorf("user_version = %d after failed batch, want 0", version)
	}
	var count int
	if err := db.QueryRow(`SELECT count(*) FROM sqlite_schema WHERE name = 'a'`).Scan(&count); err != nil {
		t.Fatalf("inspect schema: %v", err)
	}
	if count != 0 {
		t.Errorf("migration 0001 survived the failed batch — the batch is not one transaction")
	}
}

func TestMigrateFailureLeavesStoreUsable(t *testing.T) {
	db := openBare(t)
	files := map[string]string{
		"0001_a.sql":      `CREATE TABLE a (id INTEGER PRIMARY KEY);`,
		"0002_broken.sql": `CREATE TABLE syntax error here;`,
	}
	if err := migrate(context.Background(), db, migrationFS(files)); err == nil {
		t.Fatal("migrate succeeded with broken 0002, want failure")
	}

	files["0002_broken.sql"] = `CREATE TABLE b (id INTEGER PRIMARY KEY);`
	if err := migrate(context.Background(), db, migrationFS(files)); err != nil {
		t.Fatalf("migrate after fixing 0002: %v", err)
	}

	var version int
	if err := db.QueryRow("PRAGMA user_version").Scan(&version); err != nil {
		t.Fatalf("read user_version: %v", err)
	}
	if version != 2 {
		t.Errorf("user_version = %d, want 2", version)
	}
}

// TestMigrateConcurrentCallers: two racing daemon starts (or daemon +
// admin CLI) must both come through — one applies, the other waits on
// the write lock and then sees nothing pending. The version read must
// therefore happen inside the transaction; run with -race.
func TestMigrateConcurrentCallers(t *testing.T) {
	fsys := migrationFS(map[string]string{
		"0001_a.sql": `CREATE TABLE a (id INTEGER PRIMARY KEY);
INSERT INTO a DEFAULT VALUES;`,
	})
	for round := 0; round < 5; round++ {
		dsn := "file:" + filepath.Join(t.TempDir(), "fixture.db") +
			"?_pragma=busy_timeout(5000)&_pragma=journal_mode(WAL)"
		errs := make(chan error, 2)
		start := make(chan struct{})
		for i := 0; i < 2; i++ {
			db, err := sql.Open("sqlite", dsn)
			if err != nil {
				t.Fatalf("open handle %d: %v", i, err)
			}
			t.Cleanup(func() {
				if err := db.Close(); err != nil {
					t.Errorf("close handle: %v", err)
				}
			})
			// Establish the connection (and the one-time WAL conversion)
			// serially: that is Open's job in production. The race under
			// test is migrate itself.
			if err := db.Ping(); err != nil {
				t.Fatalf("ping handle %d: %v", i, err)
			}
			go func() {
				<-start
				errs <- migrate(context.Background(), db, fsys)
			}()
		}
		close(start)
		for i := 0; i < 2; i++ {
			if err := <-errs; err != nil {
				t.Fatalf("round %d: concurrent migrate: %v", round, err)
			}
		}
	}
}

// TestMigrateCanceledContextReleasesWriteLock: a context canceled
// mid-batch must not park the write lock in the pool. DDL statements
// interrupted by cancellation do NOT auto-roll-back the transaction
// (SQLite only does that for INSERT/UPDATE/DELETE), and modernc's
// session reset does not roll back either — so the ROLLBACK must run
// detached from the caller's canceled context. The invariant holds in
// every interleaving: whatever migrate returns, the store takes writes
// afterwards.
func TestMigrateCanceledContextReleasesWriteLock(t *testing.T) {
	var sb strings.Builder
	for i := 0; i < 4000; i++ {
		fmt.Fprintf(&sb, "CREATE TABLE t%04d (id INTEGER PRIMARY KEY);\n", i)
	}
	fsys := migrationFS(map[string]string{"0001_many.sql": sb.String()})

	for _, delay := range []time.Duration{time.Millisecond, 5 * time.Millisecond, 20 * time.Millisecond} {
		db := openBare(t)
		ctx, cancel := context.WithTimeout(context.Background(), delay)
		_ = migrate(ctx, db, fsys) // outcome depends on timing; the lock invariant must not
		cancel()

		if _, err := db.ExecContext(context.Background(), `CREATE TABLE post_cancel (id INTEGER PRIMARY KEY)`); err != nil {
			t.Fatalf("delay %v: store refuses writes after canceled migrate — write lock leaked to the pool: %v", delay, err)
		}
	}
}

// TestMigrateCommitFailureRollsBack: COMMIT can fail (here: a reader's
// shared lock blocks the exclusive upgrade in rollback-journal mode).
// The failed batch must not park its open transaction in the pool —
// after the reader lets go, another connection must be able to take
// the write lock.
func TestMigrateCommitFailureRollsBack(t *testing.T) {
	path := filepath.Join(t.TempDir(), "fixture.db")
	dsn := "file:" + path + "?_pragma=busy_timeout(200)"
	open := func(name string) *sql.DB {
		db, err := sql.Open("sqlite", dsn)
		if err != nil {
			t.Fatalf("open %s: %v", name, err)
		}
		t.Cleanup(func() {
			if err := db.Close(); err != nil {
				t.Errorf("close %s: %v", name, err)
			}
		})
		return db
	}
	ctx := context.Background()

	// Reader holds a shared lock across the migrate run.
	reader := open("reader")
	rconn, err := reader.Conn(ctx)
	if err != nil {
		t.Fatalf("reader conn: %v", err)
	}
	if _, err := rconn.ExecContext(ctx, "BEGIN"); err != nil {
		t.Fatalf("reader begin: %v", err)
	}
	var n int
	if err := rconn.QueryRowContext(ctx, "SELECT count(*) FROM sqlite_schema").Scan(&n); err != nil {
		t.Fatalf("reader select: %v", err)
	}

	migrator := open("migrator")
	fsys := migrationFS(map[string]string{
		"0001_a.sql": `CREATE TABLE a (id INTEGER PRIMARY KEY);`,
	})
	if err := migrate(ctx, migrator, fsys); err == nil {
		t.Fatal("migrate committed despite the reader's lock, want COMMIT failure")
	}

	if _, err := rconn.ExecContext(ctx, "ROLLBACK"); err != nil {
		t.Fatalf("reader rollback: %v", err)
	}
	if err := rconn.Close(); err != nil {
		t.Fatalf("reader conn close: %v", err)
	}

	// The write lock must be free now: only the failed batch could
	// still be holding it.
	probe := open("probe")
	pconn, err := probe.Conn(ctx)
	if err != nil {
		t.Fatalf("probe conn: %v", err)
	}
	defer pconn.Close() //nolint:errcheck // probe conn, closed with its db
	if _, err := pconn.ExecContext(ctx, "BEGIN IMMEDIATE"); err != nil {
		t.Fatalf("failed migration batch leaked its write lock: %v", err)
	}
	if _, err := pconn.ExecContext(ctx, "ROLLBACK"); err != nil {
		t.Fatalf("probe rollback: %v", err)
	}
}

// TestMigrateRefusesTransactionEscape: a migration carrying its own
// COMMIT or ROLLBACK closes the runner's transaction and destroys the
// all-pending-in-one-transaction guarantee (§6). The runner cannot
// unwind statements that already committed, but it must fail loud
// naming the file — TestMigrateEmbeddedSetIsValid then keeps such a
// migration from ever shipping.
func TestMigrateRefusesTransactionEscape(t *testing.T) {
	for name, sql := range map[string]string{
		"commit": `CREATE TABLE a (id INTEGER PRIMARY KEY);
COMMIT;
CREATE TABLE b (id INTEGER PRIMARY KEY);`,
		"rollback": `CREATE TABLE a (id INTEGER PRIMARY KEY);
ROLLBACK;`,
	} {
		t.Run(name, func(t *testing.T) {
			db := openBare(t)
			fsys := migrationFS(map[string]string{"0001_escape.sql": sql})

			err := migrate(context.Background(), db, fsys)
			if err == nil {
				t.Fatal("migrate succeeded despite transaction control in the migration, want refusal")
			}
			if !strings.Contains(err.Error(), "0001_escape.sql") {
				t.Errorf("error %q does not name the offending file", err)
			}

			// The store must not be left wedged: the write lock is free
			// and user_version was not bumped past the failure.
			if _, err := db.Exec(`CREATE TABLE post_escape (id INTEGER PRIMARY KEY)`); err != nil {
				t.Errorf("store refuses writes after detected escape: %v", err)
			}
			var version int
			if err := db.QueryRow("PRAGMA user_version").Scan(&version); err != nil {
				t.Fatalf("read user_version: %v", err)
			}
			if version != 0 {
				t.Errorf("user_version = %d after failed batch, want 0", version)
			}
		})
	}
}

// TestMigrateAllowsTriggerBodies: CREATE TRIGGER bodies contain
// BEGIN...END — the escape guard must not misread them as transaction
// control (§6's FTS tables are trigger-synced, so real migrations will
// have these).
func TestMigrateAllowsTriggerBodies(t *testing.T) {
	db := openBare(t)
	fsys := migrationFS(map[string]string{
		"0001_trigger.sql": `CREATE TABLE logs (text TEXT NOT NULL);
CREATE TABLE log_meta (n INTEGER);
INSERT INTO log_meta (n) VALUES (0);
CREATE TRIGGER logs_count AFTER INSERT ON logs
BEGIN
    UPDATE log_meta SET n = n + 1;
END;`,
	})

	if err := migrate(context.Background(), db, fsys); err != nil {
		t.Fatalf("migrate with trigger body: %v", err)
	}
	if _, err := db.Exec(`INSERT INTO logs (text) VALUES ('x')`); err != nil {
		t.Fatalf("insert: %v", err)
	}
	var n int
	if err := db.QueryRow(`SELECT n FROM log_meta`).Scan(&n); err != nil {
		t.Fatalf("select: %v", err)
	}
	if n != 1 {
		t.Errorf("trigger fired %d times, want 1", n)
	}
}
