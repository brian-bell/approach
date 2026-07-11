package cli_test

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/brian-bell/approach/internal/cli"
	"github.com/brian-bell/approach/internal/store"
)

// minimalConfig is a valid approach.toml enrolling one owner (§6).
const minimalConfig = `
[models]
heartbeat = "claude-haiku-4-5"
message   = "claude-sonnet-5"

[channels.discord]
auth = "strong"

[[identity]]
channel   = "discord"
native_id = "42"
trust     = "owner"
owner_id  = "brian"
label     = "Brian"
`

// startDaemon runs approach daemon with args in a goroutine and waits
// for the admin socket, returning the exit-code channel and stderr.
func startDaemon(t *testing.T, socket string, args ...string) (<-chan int, *strings.Builder) {
	t.Helper()
	var out, errW strings.Builder
	done := make(chan int, 1)
	go func() {
		done <- cli.Run(append([]string{"daemon"}, args...), &out, &errW)
	}()
	waitDialable(t, socket)
	return done, &errW
}

// drainDaemon drains via socket and waits for the daemon to exit 0.
func drainDaemon(t *testing.T, socket string, done <-chan int, daemonErr *strings.Builder) {
	t.Helper()
	var out, errW strings.Builder
	if code := cli.Run([]string{"drain", "--socket", socket}, &out, &errW); code != 0 {
		t.Fatalf("drain exit = %d, stderr %q, want 0", code, errW.String())
	}
	select {
	case code := <-done:
		if code != 0 {
			t.Fatalf("daemon exit after drain = %d, stderr %q, want 0", code, daemonErr.String())
		}
	case <-time.After(10 * time.Second):
		t.Fatal("daemon did not exit after drain")
	}
}

// shortTempDir returns a temp dir short enough for sun_path socket
// limits (~104 bytes on darwin), removed at cleanup.
func shortTempDir(t *testing.T) string {
	t.Helper()
	dir, err := os.MkdirTemp("", "cli")
	if err != nil {
		t.Fatalf("make temp dir: %v", err)
	}
	t.Cleanup(func() {
		if err := os.RemoveAll(dir); err != nil {
			t.Errorf("remove temp dir: %v", err)
		}
	})
	return dir
}

// TestDaemonSeedsIdentitiesFromConfig: startup loads approach.toml and
// syncs the identities table to it (§6) — the enrolled rows are in the
// store once the daemon is up.
func TestDaemonSeedsIdentitiesFromConfig(t *testing.T) {
	dir := shortTempDir(t)
	state := filepath.Join(dir, "state")
	socket := filepath.Join(state, "approach.sock")
	configPath := filepath.Join(dir, "approach.toml")
	if err := os.WriteFile(configPath, []byte(minimalConfig), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}

	done, daemonErr := startDaemon(t, socket, "--state", state, "--config", configPath)
	drainDaemon(t, socket, done, daemonErr)

	db, err := store.Open(filepath.Join(state, "approach.db"))
	if err != nil {
		t.Fatalf("open store after daemon exit: %v", err)
	}
	defer db.Close()
	var trust, ownerID string
	err = db.QueryRow(
		`SELECT trust, owner_id FROM identities WHERE channel = 'discord' AND native_id = '42'`,
	).Scan(&trust, &ownerID)
	if err != nil {
		t.Fatalf("read seeded identity: %v", err)
	}
	if trust != "owner" || ownerID != "brian" {
		t.Errorf("seeded identity = (%s, %s), want (owner, brian)", trust, ownerID)
	}
}

// TestDaemonSeedsFromDefaultedConfigPath: with no --config, the daemon
// reads approach.toml beside the state directory — including when
// --state is spelled with a trailing separator, which must not shift
// the derived sibling (a misderivation would silently boot with zero
// identities, revoking the enrolled set).
func TestDaemonSeedsFromDefaultedConfigPath(t *testing.T) {
	dir := shortTempDir(t)
	state := filepath.Join(dir, "state")
	socket := filepath.Join(state, "approach.sock")
	if err := os.WriteFile(filepath.Join(dir, "approach.toml"), []byte(minimalConfig), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}

	done, daemonErr := startDaemon(t, socket, "--state", state+string(os.PathSeparator))
	drainDaemon(t, socket, done, daemonErr)

	db, err := store.Open(filepath.Join(state, "approach.db"))
	if err != nil {
		t.Fatalf("open store after daemon exit: %v", err)
	}
	defer db.Close()
	var count int
	if err := db.QueryRow(`SELECT COUNT(*) FROM identities`).Scan(&count); err != nil {
		t.Fatalf("count identities: %v", err)
	}
	if count != 1 {
		t.Errorf("identities rows = %d, want 1 (seeded from the defaulted approach.toml)", count)
	}
}

// TestDaemonRefusesExplicitBadConfig: approach.toml is security-load-
// bearing, so an explicit --config that is missing or invalid refuses
// startup — exit 1, no readiness line, a structured error record.
func TestDaemonRefusesExplicitBadConfig(t *testing.T) {
	for _, tc := range []struct {
		name    string
		content string // "" means do not create the file
	}{
		{"missing file", ""},
		{"invalid file", "models = \"not a table\"\n"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := shortTempDir(t)
			state := filepath.Join(dir, "state")
			configPath := filepath.Join(dir, "approach.toml")
			if tc.content != "" {
				if err := os.WriteFile(configPath, []byte(tc.content), 0o600); err != nil {
					t.Fatalf("write config: %v", err)
				}
			}

			var out, errW strings.Builder
			if code := cli.Run([]string{"daemon", "--state", state, "--config", configPath}, &out, &errW); code != 1 {
				t.Errorf("daemon exit = %d, want 1", code)
			}
			if strings.Contains(out.String(), "listening") {
				t.Errorf("stdout = %q — printed the readiness line despite refusing startup", out.String())
			}
			if !strings.Contains(errW.String(), "config") {
				t.Errorf("stderr = %q, want a config error record", errW.String())
			}
		})
	}
}

// TestDaemonWithoutConfigWarnsAndServes: no approach.toml beside the
// state dir is a bootable posture — zero enrolled identities means every
// sender is untrusted (deny-by-default, §6) — but it must be loud: a
// structured warning record, and an empty identities table.
func TestDaemonWithoutConfigWarnsAndServes(t *testing.T) {
	dir := shortTempDir(t)
	state := filepath.Join(dir, "state")
	socket := filepath.Join(state, "approach.sock")

	done, daemonErr := startDaemon(t, socket, "--state", state)
	drainDaemon(t, socket, done, daemonErr)

	var warned bool
	for _, line := range strings.Split(strings.TrimSpace(daemonErr.String()), "\n") {
		if line == "" {
			continue
		}
		var rec map[string]any
		if err := json.Unmarshal([]byte(line), &rec); err != nil {
			t.Errorf("daemon stderr line %q is not a JSON log record: %v", line, err)
			continue
		}
		if rec["level"] == "WARN" && strings.Contains(line, "approach.toml") {
			warned = true
		}
	}
	if !warned {
		t.Errorf("daemon stderr %q has no warning about the missing approach.toml", daemonErr.String())
	}

	db, err := store.Open(filepath.Join(state, "approach.db"))
	if err != nil {
		t.Fatalf("open store after daemon exit: %v", err)
	}
	defer db.Close()
	var count int
	if err := db.QueryRow(`SELECT COUNT(*) FROM identities`).Scan(&count); err != nil {
		t.Fatalf("count identities: %v", err)
	}
	if count != 0 {
		t.Errorf("identities rows = %d without a config, want 0 (deny-by-default)", count)
	}
}
