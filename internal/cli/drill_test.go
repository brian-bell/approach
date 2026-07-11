package cli_test

import (
	"encoding/json"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/brian-bell/approach/internal/cli"
	"github.com/brian-bell/approach/internal/store"
)

// TestDrillDaemonRefusesNewerSchema is the §9 PS drill for downgrade
// protection (§6): a database written by a newer binary must stop THE
// DAEMON — not just the store layer — completely. No socket, nothing
// listening, nonzero exit, and a journal record an operator can act
// on. The store-layer refusal itself is unit-tested with
// approach-1zr.1.5; this drives the real startup path end to end.
func TestDrillDaemonRefusesNewerSchema(t *testing.T) {
	dir, err := os.MkdirTemp("", "cli")
	if err != nil {
		t.Fatalf("make temp dir: %v", err)
	}
	t.Cleanup(func() {
		if err := os.RemoveAll(dir); err != nil {
			t.Errorf("remove temp dir: %v", err)
		}
	})
	state := filepath.Join(dir, "state")
	socket := filepath.Join(state, "approach.sock")

	// A database from the future: migrated by this binary, then
	// stamped with a schema version far beyond what it knows.
	db, err := store.Open(filepath.Join(state, "approach.db"))
	if err != nil {
		t.Fatalf("seed store: %v", err)
	}
	if _, err := db.Exec("PRAGMA user_version = 9999"); err != nil {
		t.Fatalf("stamp future schema version: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close seed store: %v", err)
	}

	// The daemon must refuse — and refuse promptly, not hang serving.
	var out, errW strings.Builder
	done := make(chan int, 1)
	go func() { done <- cli.Run([]string{"daemon", "--state", state}, &out, &errW) }()
	var code int
	select {
	case code = <-done:
	case <-time.After(30 * time.Second):
		t.Fatal("daemon still running against a newer schema, want refusal")
	}
	if code != 1 {
		t.Errorf("daemon exit = %d, want 1", code)
	}

	// The refusal must be total: no readiness line, no socket file,
	// nothing to dial.
	if strings.Contains(out.String(), "listening") {
		t.Errorf("stdout %q claims readiness despite refusal", out.String())
	}
	if _, err := os.Lstat(socket); !os.IsNotExist(err) {
		t.Errorf("socket file exists after refusal (Lstat err = %v)", err)
	}
	var statusOut, statusErr strings.Builder
	if code := cli.Run([]string{"status", "--socket", socket}, &statusOut, &statusErr); code == 0 {
		t.Error("status succeeded after refused startup — something is listening")
	}

	// The journal must carry an actionable diagnosis: the version
	// found, the newest version this binary supports, and that it is a
	// downgrade refusal. All three, or an operator cannot tell which
	// side to fix.
	supported := regexp.MustCompile(`knows up to \d+`)
	var diagnosed bool
	for _, line := range strings.Split(strings.TrimSpace(errW.String()), "\n") {
		var rec map[string]any
		if json.Unmarshal([]byte(line), &rec) != nil {
			continue
		}
		msg, _ := rec["error"].(string)
		if strings.Contains(msg, "9999") &&
			strings.Contains(msg, "newer than this binary") &&
			supported.MatchString(msg) {
			diagnosed = true
		}
	}
	if !diagnosed {
		t.Errorf("journal %q lacks a structured record naming version 9999, the supported version, and the downgrade refusal", errW.String())
	}

	// Nothing may have touched the future schema: the drill leaves the
	// database exactly as the newer binary wrote it.
	db, err = store.Open(filepath.Join(state, "approach.db"))
	if err == nil {
		_ = db.Close()
		t.Fatal("store.Open succeeded against the future schema during verification")
	}
	raw, err := os.ReadFile(filepath.Join(state, "approach.db"))
	if err != nil {
		t.Fatalf("read db file: %v", err)
	}
	// user_version lives at offset 60 in the SQLite header, big-endian.
	if len(raw) < 64 {
		t.Fatalf("db file too short: %d bytes", len(raw))
	}
	got := int(raw[60])<<24 | int(raw[61])<<16 | int(raw[62])<<8 | int(raw[63])
	if got != 9999 {
		t.Errorf("db header user_version = %d after refusals, want 9999 untouched", got)
	}
}
