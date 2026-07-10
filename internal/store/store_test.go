package store_test

import (
	"context"
	"database/sql"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/brian-bell/approach/internal/store"
)

// mustOpen opens the store at path, failing the test on error and
// closing the handle at cleanup. sql.DB.Close is idempotent, so tests
// that close explicitly mid-test can still use this.
func mustOpen(t *testing.T, path string) *sql.DB {
	t.Helper()
	db, err := store.Open(path)
	if err != nil {
		t.Fatalf("Open(%q): %v", path, err)
	}
	t.Cleanup(func() {
		if err := db.Close(); err != nil {
			t.Errorf("close: %v", err)
		}
	})
	return db
}

func TestOpenCreatesRestrictedDirAndFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state", "approach.db")

	mustOpen(t, path)

	dirInfo, err := os.Stat(filepath.Dir(path))
	if err != nil {
		t.Fatalf("stat state dir: %v", err)
	}
	if mode := dirInfo.Mode().Perm(); mode != 0o700 {
		t.Errorf("state dir mode = %04o, want 0700", mode)
	}

	fileInfo, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat db file: %v", err)
	}
	if mode := fileInfo.Mode().Perm(); mode != 0o600 {
		t.Errorf("db file mode = %04o, want 0600", mode)
	}
}

func TestOpenAppliesPragmas(t *testing.T) {
	db := mustOpen(t, filepath.Join(t.TempDir(), "state", "approach.db"))

	assertPragmas(t, db.QueryRow)
}

// TestOpenAppliesPragmasOnEveryPooledConnection guards the DSN-vs-Exec
// trap: synchronous, busy_timeout, and foreign_keys are per-connection,
// so they must hold on every connection the pool creates, not just the
// first one configured.
func TestOpenAppliesPragmasOnEveryPooledConnection(t *testing.T) {
	db := mustOpen(t, filepath.Join(t.TempDir(), "state", "approach.db"))

	ctx := context.Background()
	conns := make(map[string]*sql.Conn)
	for _, name := range []string{"first", "second"} {
		conn, err := db.Conn(ctx)
		if err != nil {
			t.Fatalf("%s conn: %v", name, err)
		}
		t.Cleanup(func() {
			if err := conn.Close(); err != nil {
				t.Errorf("close %s conn: %v", name, err)
			}
		})
		conns[name] = conn
	}

	for name, conn := range conns {
		t.Run(name, func(t *testing.T) {
			assertPragmas(t, func(query string, args ...any) *sql.Row {
				return conn.QueryRowContext(ctx, query, args...)
			})
		})
	}
}

// assertPragmas checks the §6 posture on whatever connection queryRow
// reads from. synchronous and the flags report numerically; journal_mode
// reports the string "wal".
func assertPragmas(t *testing.T, queryRow func(query string, args ...any) *sql.Row) {
	t.Helper()
	for pragma, want := range map[string]string{
		"journal_mode": "wal",
		"synchronous":  "1",
		"busy_timeout": "5000",
		"foreign_keys": "1",
	} {
		var got string
		if err := queryRow("PRAGMA " + pragma).Scan(&got); err != nil {
			t.Errorf("PRAGMA %s: %v", pragma, err)
			continue
		}
		if got != want {
			t.Errorf("PRAGMA %s = %q, want %q", pragma, got, want)
		}
	}
}

func TestOpenRefusesLooseDirPermissions(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "state")
	if err := os.Mkdir(dir, 0o755); err != nil {
		t.Fatal(err)
	}

	_, err := store.Open(filepath.Join(dir, "approach.db"))
	if err == nil {
		t.Fatalf("Open succeeded on a 0755 state dir, want refusal")
	}
	for _, want := range []string{dir, "0700"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error %q does not mention %q", err, want)
		}
	}
}

func TestOpenRefusesLooseFilePermissions(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "state")
	if err := os.Mkdir(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, "approach.db")
	if err := os.WriteFile(path, nil, 0o644); err != nil {
		t.Fatal(err)
	}

	_, err := store.Open(path)
	if err == nil {
		t.Fatalf("Open succeeded on a 0644 db file, want refusal")
	}
	for _, want := range []string{path, "0600"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error %q does not mention %q", err, want)
		}
	}
}

func TestOpenExistingDatabase(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state", "approach.db")

	db := mustOpen(t, path)
	if _, err := db.Exec(`CREATE TABLE t (v TEXT NOT NULL); INSERT INTO t (v) VALUES ('persisted')`); err != nil {
		t.Fatalf("seed: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	db = mustOpen(t, path)

	var got string
	if err := db.QueryRow(`SELECT v FROM t`).Scan(&got); err != nil {
		t.Fatalf("select: %v", err)
	}
	if got != "persisted" {
		t.Fatalf("got %q, want %q", got, "persisted")
	}
	assertPragmas(t, db.QueryRow)
}

func TestOpenRoundTrip(t *testing.T) {
	db := mustOpen(t, filepath.Join(t.TempDir(), "state", "approach.db"))

	if _, err := db.Exec(`CREATE TABLE t (v TEXT NOT NULL)`); err != nil {
		t.Fatalf("create table: %v", err)
	}
	if _, err := db.Exec(`INSERT INTO t (v) VALUES ('hello')`); err != nil {
		t.Fatalf("insert: %v", err)
	}
	var got string
	if err := db.QueryRow(`SELECT v FROM t`).Scan(&got); err != nil {
		t.Fatalf("select: %v", err)
	}
	if got != "hello" {
		t.Fatalf("round trip: got %q, want %q", got, "hello")
	}
}
