package engine_test

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/brian-bell/approach/internal/engine"
	"github.com/brian-bell/approach/internal/session"
)

type discard struct{}

func (discard) Write(p []byte) (int, error) { return len(p), nil }

func discardLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(discard{}, nil))
}

// fakeCLI writes a shell script standing in for the pinned claude
// binary: it records argv, cwd, and stdin into the given dir, then
// behaves per the script body appended after the recording preamble.
func fakeCLI(t *testing.T, dir, body string) string {
	t.Helper()
	script := fmt.Sprintf(`#!/bin/sh
printf '%%s\n' "$@" > %q/argv
pwd -P > %q/cwd
cat > %q/stdin
%s
`, dir, dir, dir, body)
	bin := filepath.Join(dir, "claude")
	if err := os.WriteFile(bin, []byte(script), 0o755); err != nil {
		t.Fatalf("write fake cli: %v", err)
	}
	return bin
}

func slurp(t *testing.T, path string) string {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	return string(b)
}

func newEngine(t *testing.T, bin string, timeout time.Duration) *engine.Engine {
	t.Helper()
	e, err := engine.New(engine.Config{
		Bin:         bin,
		Model:       "claude-sonnet-5",
		MaxTurns:    25,
		TurnTimeout: timeout,
		Logger:      discardLogger(),
	})
	if err != nil {
		t.Fatalf("engine.New: %v", err)
	}
	return e
}

func spec(cwd string) session.Spec {
	return session.Spec{
		SessionID: "11111111-1111-4111-8111-111111111111",
		ThreadKey: "discord:dm:a",
		Cwd:       cwd,
		Prompt:    "hello there",
	}
}

// TestStartInvocationFormPinned: THE pin test (§7: pin the flags, test
// the pin). A bare `claude -p` without the model pin and --max-turns
// is exactly the drift this asserts against — the §11 runaway controls
// live in these flags.
func TestStartInvocationFormPinned(t *testing.T) {
	dir := t.TempDir()
	bin := fakeCLI(t, dir, "exit 0")
	e := newEngine(t, bin, 30*time.Second)
	cwd := t.TempDir()

	s := spec(cwd)
	if err := e.Start(context.Background(), s); err != nil {
		t.Fatalf("Start: %v", err)
	}
	wantArgv := strings.Join([]string{
		"-p", "--model", "claude-sonnet-5", "--max-turns", "25", "--session-id", s.SessionID,
	}, "\n") + "\n"
	if got := slurp(t, filepath.Join(dir, "argv")); got != wantArgv {
		t.Errorf("pinned invocation drifted:\ngot  %q\nwant %q", got, wantArgv)
	}
	// The child runs from the SESSION's recorded cwd (§4.1, §6).
	gotCwd := strings.TrimSpace(slurp(t, filepath.Join(dir, "cwd")))
	resolved, err := filepath.EvalSymlinks(cwd)
	if err != nil {
		t.Fatalf("resolve cwd: %v", err)
	}
	if gotCwd != resolved {
		t.Errorf("child cwd = %q, want the session cwd %q", gotCwd, resolved)
	}
	// The prompt arrives on stdin, not argv — message content must not
	// leak into process listings.
	if got := slurp(t, filepath.Join(dir, "stdin")); got != "hello there" {
		t.Errorf("stdin = %q, want the prompt", got)
	}
}

// TestResumeInvocationFormPinned: the resume leg differs only in
// --resume <id> — same pinned safety flags.
func TestResumeInvocationFormPinned(t *testing.T) {
	dir := t.TempDir()
	bin := fakeCLI(t, dir, "exit 0")
	e := newEngine(t, bin, 30*time.Second)
	s := spec(t.TempDir())

	if err := e.Resume(context.Background(), s); err != nil {
		t.Fatalf("Resume: %v", err)
	}
	wantArgv := strings.Join([]string{
		"-p", "--model", "claude-sonnet-5", "--max-turns", "25", "--resume", s.SessionID,
	}, "\n") + "\n"
	if got := slurp(t, filepath.Join(dir, "argv")); got != wantArgv {
		t.Errorf("pinned resume invocation drifted:\ngot  %q\nwant %q", got, wantArgv)
	}
}

// TestTimeoutKillsChild: a wedged child is killed at the turn timeout
// (§11: timeouts kill the child, then §4.6 reasons about the turn) —
// the call returns promptly with a timeout-flavored error, not after
// the child's own schedule.
func TestTimeoutKillsChild(t *testing.T) {
	dir := t.TempDir()
	bin := fakeCLI(t, dir, "sleep 30\nexit 0")
	e := newEngine(t, bin, 300*time.Millisecond)

	begin := time.Now()
	err := e.Start(context.Background(), spec(t.TempDir()))
	elapsed := time.Since(begin)
	if err == nil {
		t.Fatal("Start returned nil from a turn that outlived its timeout")
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("timeout error = %v, want context.DeadlineExceeded in the chain", err)
	}
	if elapsed > 10*time.Second {
		t.Errorf("Start took %v to enforce a 300ms timeout — the kill did not happen", elapsed)
	}
	// A timeout is not transcript loss.
	if errors.Is(err, session.ErrResumeFailed) {
		t.Error("timeout mapped to ErrResumeFailed — it would burn the session on a slow turn")
	}
}

// TestResumeFailedMapping: the CLI's missing-transcript refusal maps
// to session.ErrResumeFailed so the §4.6 degradation triggers; any
// other failure stays transient.
func TestResumeFailedMapping(t *testing.T) {
	cases := []struct {
		name   string
		body   string
		failed bool
	}{
		{"no conversation found", `echo "No conversation found with session ID: 1111" >&2; exit 1`, true},
		{"case variant", `echo "error: no conversation found with session id 1111" >&2; exit 1`, true},
		{"transient failure", `echo "rate limited, retry later" >&2; exit 1`, false},
		{"other exit", `echo "boom" >&2; exit 7`, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			bin := fakeCLI(t, dir, tc.body)
			e := newEngine(t, bin, 30*time.Second)
			err := e.Resume(context.Background(), spec(t.TempDir()))
			if err == nil {
				t.Fatal("Resume returned nil from a failing CLI")
			}
			if got := errors.Is(err, session.ErrResumeFailed); got != tc.failed {
				t.Errorf("ErrResumeFailed = %v, want %v (err: %v)", got, tc.failed, err)
			}
		})
	}
}

// TestTransparencyNotePrependedToPrompt: the §4.6 degradation note
// precedes the event text on stdin so the model can speak it.
func TestTransparencyNotePrependedToPrompt(t *testing.T) {
	dir := t.TempDir()
	bin := fakeCLI(t, dir, "exit 0")
	e := newEngine(t, bin, 30*time.Second)

	s := spec(t.TempDir())
	s.TransparencyNote = "lost this thread's conversation history — facts intact, starting fresh"
	if err := e.Start(context.Background(), s); err != nil {
		t.Fatalf("Start: %v", err)
	}
	got := slurp(t, filepath.Join(dir, "stdin"))
	if !strings.HasPrefix(got, "[system note: "+s.TransparencyNote+"]") {
		t.Errorf("stdin %q does not lead with the transparency note", got)
	}
	if !strings.Contains(got, "hello there") {
		t.Errorf("stdin %q lost the event prompt", got)
	}
}

// TestConfigValidation: every field is load-bearing (§8 model pin,
// §11 runaway controls) — New refuses a config missing any of them.
func TestConfigValidation(t *testing.T) {
	valid := engine.Config{
		Bin: "/usr/local/bin/claude", Model: "claude-sonnet-5",
		MaxTurns: 25, TurnTimeout: time.Minute, Logger: discardLogger(),
	}
	cases := []struct {
		name   string
		mutate func(*engine.Config)
	}{
		{"missing bin", func(c *engine.Config) { c.Bin = "" }},
		{"missing model", func(c *engine.Config) { c.Model = "" }},
		{"zero max turns", func(c *engine.Config) { c.MaxTurns = 0 }},
		{"negative max turns", func(c *engine.Config) { c.MaxTurns = -1 }},
		{"zero timeout", func(c *engine.Config) { c.TurnTimeout = 0 }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cfg := valid
			tc.mutate(&cfg)
			if _, err := engine.New(cfg); err == nil {
				t.Errorf("New accepted a config with %s", tc.name)
			}
		})
	}
	if _, err := engine.New(valid); err != nil {
		t.Errorf("New rejected a valid config: %v", err)
	}
}
