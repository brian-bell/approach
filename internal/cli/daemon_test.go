package cli_test

import (
	"encoding/json"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/brian-bell/approach/internal/cli"
)

// TestDaemonAdminRoundTrip drives the real startup path end to end:
// approach daemon serves the admin socket over an opened store, the
// poke/status/drain subcommands reach it, and drain shuts it down
// with exit 0.
func TestDaemonAdminRoundTrip(t *testing.T) {
	// Not t.TempDir: sun_path caps socket paths (~104 bytes on darwin)
	// and test-name-derived paths blow the limit.
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

	var daemonOut, daemonErr strings.Builder
	daemonDone := make(chan int, 1)
	go func() {
		daemonDone <- cli.Run([]string{"daemon", "--state", state}, &daemonOut, &daemonErr)
	}()
	waitDialable(t, socket)

	var out, errW strings.Builder
	if code := cli.Run([]string{"poke", "--socket", socket}, &out, &errW); code != 0 {
		t.Errorf("poke exit = %d, stderr %q, want 0", code, errW.String())
	}
	if !strings.Contains(out.String(), "ok") {
		t.Errorf("poke stdout = %q, want ok", out.String())
	}

	out.Reset()
	errW.Reset()
	if code := cli.Run([]string{"status", "--socket", socket}, &out, &errW); code != 0 {
		t.Errorf("status exit = %d, stderr %q, want 0", code, errW.String())
	}
	if !strings.Contains(out.String(), "schema_version") {
		t.Errorf("status stdout = %q, want a schema_version field", out.String())
	}
	// The poke above must be observable — an acknowledged-but-dropped
	// wake signal would let a dead timer path go unnoticed until M1
	// wires the event router onto this hook.
	if !strings.Contains(out.String(), `"pokes":1`) {
		t.Errorf("status stdout = %q, want pokes count 1 after one poke", out.String())
	}

	out.Reset()
	errW.Reset()
	if code := cli.Run([]string{"drain", "--socket", socket}, &out, &errW); code != 0 {
		t.Errorf("drain exit = %d, stderr %q, want 0", code, errW.String())
	}
	select {
	case code := <-daemonDone:
		if code != 0 {
			t.Errorf("daemon exit after drain = %d, stderr %q, want 0", code, daemonErr.String())
		}
	case <-time.After(10 * time.Second):
		t.Fatal("daemon did not exit after drain")
	}

	// The daemon opened the store on the way up (§6: a running daemon
	// is a migrated daemon).
	if _, err := os.Stat(filepath.Join(state, "approach.db")); err != nil {
		t.Errorf("daemon did not create the state store: %v", err)
	}

	// stderr is the systemd journal: the daemon's lifecycle must land
	// there as structured JSON records, not prose.
	var readyLogged bool
	for _, line := range strings.Split(strings.TrimSpace(daemonErr.String()), "\n") {
		if line == "" {
			continue
		}
		var rec map[string]any
		if err := json.Unmarshal([]byte(line), &rec); err != nil {
			t.Errorf("daemon stderr line %q is not a JSON log record: %v", line, err)
			continue
		}
		if rec["msg"] == "ready" && rec["socket"] == socket {
			readyLogged = true
		}
	}
	if !readyLogged {
		t.Errorf("daemon stderr %q has no ready record naming the socket", daemonErr.String())
	}
}

// TestAdminCommandsWithoutDaemon: the client verbs fail with exit 1
// and a useful message when nothing is listening.
func TestAdminCommandsWithoutDaemon(t *testing.T) {
	dir, err := os.MkdirTemp("", "cli")
	if err != nil {
		t.Fatalf("make temp dir: %v", err)
	}
	t.Cleanup(func() {
		if err := os.RemoveAll(dir); err != nil {
			t.Errorf("remove temp dir: %v", err)
		}
	})
	socket := filepath.Join(dir, "approach.sock")

	var out, errW strings.Builder
	if code := cli.Run([]string{"status", "--socket", socket}, &out, &errW); code != 1 {
		t.Errorf("status with no daemon exit = %d, want 1", code)
	}
	if !strings.Contains(errW.String(), socket) {
		t.Errorf("stderr %q does not name the socket path", errW.String())
	}
}

// TestSecondDaemonRefusedBeforeTouchingStore: a second daemon start
// against a live one must fail without printing the listening line —
// launchers treat that line as readiness — and must be refused before
// it opens (and potentially migrates) the live daemon's store.
func TestSecondDaemonRefusedBeforeTouchingStore(t *testing.T) {
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

	var daemonOut, daemonErr strings.Builder
	daemonDone := make(chan int, 1)
	go func() {
		daemonDone <- cli.Run([]string{"daemon", "--state", state}, &daemonOut, &daemonErr)
	}()
	waitDialable(t, socket)

	var out, errW strings.Builder
	if code := cli.Run([]string{"daemon", "--state", state}, &out, &errW); code != 1 {
		t.Errorf("second daemon exit = %d, want 1", code)
	}
	if !strings.Contains(errW.String(), "already running") {
		t.Errorf("second daemon stderr = %q, want already-running refusal", errW.String())
	}
	if strings.Contains(out.String(), "listening") {
		t.Errorf("second daemon stdout = %q — printed the readiness line without ever binding", out.String())
	}

	var drainOut, drainErr strings.Builder
	if code := cli.Run([]string{"drain", "--socket", socket}, &drainOut, &drainErr); code != 0 {
		t.Errorf("drain exit = %d, stderr %q, want 0", code, drainErr.String())
	}
	select {
	case code := <-daemonDone:
		if code != 0 {
			t.Errorf("first daemon exit = %d, stderr %q, want 0", code, daemonErr.String())
		}
	case <-time.After(10 * time.Second):
		t.Fatal("first daemon did not exit after drain")
	}
}

// TestAdminVerbRejectsLeftoverArguments: flag parsing stops at the
// first positional argument, so "drain typo --socket <path>" would
// silently fall back to the DEFAULT socket and drain the wrong daemon.
// Leftovers are a usage error, refused before anything is contacted.
func TestAdminVerbRejectsLeftoverArguments(t *testing.T) {
	for _, args := range [][]string{
		{"drain", "typo", "--socket", "/nonexistent.sock"},
		{"status", "extra"},
		{"daemon", "extra", "--state", "/nonexistent"},
	} {
		var out, errW strings.Builder
		if code := cli.Run(args, &out, &errW); code != 2 {
			t.Errorf("Run(%v) exit = %d, want 2 (usage error)", args, code)
		}
		if !strings.Contains(errW.String(), "unexpected argument") {
			t.Errorf("Run(%v) stderr = %q, want unexpected-argument diagnostic", args, errW.String())
		}
	}
}

// waitDialable blocks until the socket accepts connections.
func waitDialable(t *testing.T, path string) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		conn, err := net.Dial("unix", path)
		if err == nil {
			if err := conn.Close(); err != nil {
				t.Fatalf("close probe connection: %v", err)
			}
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("socket %s never became dialable", path)
}
