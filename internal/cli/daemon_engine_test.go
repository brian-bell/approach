package cli_test

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/brian-bell/approach/internal/cli"
)

// pinFixture writes a fake claude binary reporting the given version
// and an approach.toml pinning wantPin against it, returning the state
// dir to boot from.
func pinFixture(t *testing.T, reports, wantPin string) (state string) {
	t.Helper()
	dir, err := os.MkdirTemp("", "cli") // sun_path cap — see daemon_test.go
	if err != nil {
		t.Fatalf("make temp dir: %v", err)
	}
	t.Cleanup(func() {
		if err := os.RemoveAll(dir); err != nil {
			t.Errorf("remove temp dir: %v", err)
		}
	})
	bin := filepath.Join(dir, "claude")
	if err := os.WriteFile(bin, []byte(fmt.Sprintf("#!/bin/sh\nprintf '%%s\\n' %q\n", reports)), 0o755); err != nil {
		t.Fatalf("write fake cli: %v", err)
	}
	cfgBody := fmt.Sprintf(`[models]
message = "claude-sonnet-5"
heartbeat = "claude-haiku-4-5"

[engine]
bin = %q
version = %q
`, bin, wantPin)
	if err := os.WriteFile(filepath.Join(dir, "approach.toml"), []byte(cfgBody), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}
	return filepath.Join(dir, "state")
}

// TestDaemonRefusesEngineVersionDrift: the §2 pin at work — a deployed
// CLI that does not match [engine].version refuses startup with the
// unrecoverable exit BEFORE the store exists (a restart cannot fix
// drift, and the drifted binary must not migrate anything).
func TestDaemonRefusesEngineVersionDrift(t *testing.T) {
	state := pinFixture(t, "2.2.0 (Claude Code)", "2.1.199")

	var out, errW strings.Builder
	code := cli.Run([]string{"daemon", "--state", state, "--config", filepath.Join(filepath.Dir(state), "approach.toml")}, &out, &errW)
	if code != 3 {
		t.Fatalf("daemon exit = %d, want 3 (unrecoverable); stderr: %s", code, errW.String())
	}
	if !strings.Contains(errW.String(), "version drift") {
		t.Errorf("stderr %q does not name the version drift", errW.String())
	}
	if _, err := os.Stat(filepath.Join(state, "approach.db")); !os.IsNotExist(err) {
		t.Errorf("refused startup still touched the store (err=%v) — the drift check must run pre-store", err)
	}
}

// TestDaemonBootsWithMatchingEnginePin: a matching pin boots normally
// and drains clean.
func TestDaemonBootsWithMatchingEnginePin(t *testing.T) {
	state := pinFixture(t, "2.1.199 (Claude Code)", "2.1.199")
	socket := filepath.Join(state, "approach.sock")

	var daemonOut, daemonErr strings.Builder
	done := make(chan int, 1)
	go func() {
		done <- cli.Run([]string{"daemon", "--state", state, "--config", filepath.Join(filepath.Dir(state), "approach.toml")}, &daemonOut, &daemonErr)
	}()
	waitDialable(t, socket)

	var out, errW strings.Builder
	if code := cli.Run([]string{"drain", "--socket", socket}, &out, &errW); code != 0 {
		t.Fatalf("drain exit = %d, stderr %q", code, errW.String())
	}
	select {
	case code := <-done:
		if code != 0 {
			t.Fatalf("daemon exit = %d, stderr %q", code, daemonErr.String())
		}
	case <-time.After(10 * time.Second):
		t.Fatal("daemon did not exit after drain")
	}
	if !strings.Contains(daemonErr.String(), "engine pin verified") {
		t.Errorf("journal does not record the verified pin: %s", daemonErr.String())
	}
}
