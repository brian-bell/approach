package cli

import (
	"context"
	"database/sql"
	"errors"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/brian-bell/approach/internal/admin"
	"github.com/brian-bell/approach/internal/config"
)

// fakeRunner stands in for the discord adapter's Run loop. Internal
// (package cli) because it replaces the newDiscordRunner seam.
type fakeRunner struct {
	run      func(context.Context) error
	started  atomic.Bool
	returned atomic.Bool
}

func (f *fakeRunner) Run(ctx context.Context) error {
	f.started.Store(true)
	defer f.returned.Store(true)
	return f.run(ctx)
}

// stubRunner swaps the production adapter constructor for one
// returning r, restored at cleanup. Daemon tests run sequentially, so
// the package-level seam is safe to swap.
func stubRunner(t *testing.T, r adapterRunner) {
	t.Helper()
	prev := newDiscordRunner
	newDiscordRunner = func(*config.Config, *sql.DB, *slog.Logger) (adapterRunner, error) {
		return r, nil
	}
	t.Cleanup(func() { newDiscordRunner = prev })
}

// discordDaemonDir lays out a state dir + config with a readable
// discord token so runDaemon reaches the adapter-start path. The
// socket lives under os.MkdirTemp (not t.TempDir) to stay inside the
// unix-socket path length limit.
func discordDaemonDir(t *testing.T) (state, configPath string) {
	t.Helper()
	dir, err := os.MkdirTemp("", "approach-*")
	if err != nil {
		t.Fatalf("MkdirTemp: %v", err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dir) })
	tokenPath := filepath.Join(dir, "token")
	if err := os.WriteFile(tokenPath, []byte("test-token\n"), 0o600); err != nil {
		t.Fatalf("write token: %v", err)
	}
	cfg := `
[models]
message = "sonnet"
heartbeat = "haiku"

[channels.discord]
auth = "strong"
token_file = "` + tokenPath + `"
`
	configPath = filepath.Join(dir, "approach.toml")
	if err := os.WriteFile(configPath, []byte(cfg), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}
	return filepath.Join(dir, "state"), configPath
}

// awaitReady polls until the daemon's readiness line appears — the
// admin socket is bound after that point.
func awaitReady(t *testing.T, out *syncBuilder) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if strings.Contains(out.String(), "listening") {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("daemon never printed the readiness line; stdout=%q", out.String())
}

// syncBuilder is a strings.Builder safe to read while the daemon
// goroutine writes it.
type syncBuilder struct {
	mu sync.Mutex
	b  strings.Builder
}

func (s *syncBuilder) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.Write(p)
}

func (s *syncBuilder) String() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.String()
}

// TestDaemonRunsAdapterAndWaitsOnDrain: with a configured discord
// channel the daemon starts the adapter's Run loop, and a drain
// cancels it AND waits for it to return before the daemon exits — the
// handler must never race a closing store (§4.1: the insert path has
// to be dead before the db is).
func TestDaemonRunsAdapterAndWaitsOnDrain(t *testing.T) {
	runner := &fakeRunner{run: func(ctx context.Context) error {
		<-ctx.Done()
		return ctx.Err()
	}}
	stubRunner(t, runner)
	state, configPath := discordDaemonDir(t)

	var out syncBuilder
	var errW syncBuilder
	exit := make(chan int, 1)
	go func() { exit <- Run([]string{"daemon", "--state", state, "--config", configPath}, &out, &errW) }()
	awaitReady(t, &out)

	if !runner.started.Load() {
		t.Fatal("daemon is serving but the discord adapter was never started")
	}
	if _, err := admin.Request(filepath.Join(state, "approach.sock"), "drain"); err != nil {
		t.Fatalf("drain: %v", err)
	}
	select {
	case code := <-exit:
		if code != 0 {
			t.Errorf("daemon exit = %d, want 0 on drain; stderr=%q", code, errW.String())
		}
	case <-time.After(10 * time.Second):
		t.Fatal("daemon did not exit after drain")
	}
	if !runner.returned.Load() {
		t.Error("daemon exited before the adapter's Run returned — the ingest path could still be writing to a closed store")
	}
}

// TestDaemonAdapterTerminalErrorExits3: a terminal gateway refusal
// (bad credential, refused intents) after startup must take the
// daemon down with exitUnrecoverable — systemd restart cannot mint a
// working credential, and a daemon that keeps serving with a dead
// channel is quiet degradation (§8).
func TestDaemonAdapterTerminalErrorExits3(t *testing.T) {
	runner := &fakeRunner{run: func(context.Context) error {
		return errors.New("gateway refused the connection — credential or intents problem")
	}}
	stubRunner(t, runner)
	state, configPath := discordDaemonDir(t)

	var out, errW syncBuilder
	exit := make(chan int, 1)
	go func() { exit <- Run([]string{"daemon", "--state", state, "--config", configPath}, &out, &errW) }()

	select {
	case code := <-exit:
		if code != exitUnrecoverable {
			t.Errorf("daemon exit = %d, want %d for a terminal adapter failure", code, exitUnrecoverable)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("daemon kept serving after the adapter died terminally")
	}
	if !strings.Contains(errW.String(), "discord") {
		t.Errorf("stderr %q does not surface the adapter failure", errW.String())
	}
}
