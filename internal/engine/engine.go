// Package engine runs Claude Code turns as child processes — the real
// implementation of session.Engine. The invocation form is PINNED
// (§7): every spawn carries the model pin (§8 — the CLI's own default
// is settings-derived, never rely on it) and --max-turns, and every
// turn runs under a wall-clock timeout that kills the child (§11's
// runaway controls). A bare `claude -p` must be unrepresentable here;
// the pin is enforced by test.
package engine

import (
	"bytes"
	"context"
	"fmt"
	"log/slog"
	"os/exec"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/brian-bell/approach/internal/session"
)

// Config pins the engine invocation. Every field is load-bearing and
// required — a missing model would silently fall back to the CLI's
// settings-derived default (§8), and missing caps would ship the §11
// runaway shape — so New fails loud instead of defaulting.
type Config struct {
	Bin         string        // pinned claude binary path (version pin is x6n.2.10)
	Model       string        // pinned per event kind in approach.toml (§8)
	MaxTurns    int           // --max-turns per spawn (§11)
	TurnTimeout time.Duration // wall-clock kill for one turn (§11)
	Logger      *slog.Logger
}

// Engine spawns claude -p children. One engine per daemon; safe for
// concurrent use (it holds no per-turn state).
type Engine struct {
	bin      string
	model    string
	maxTurns int
	timeout  time.Duration
	logger   *slog.Logger
}

// New validates the pin. Fail-loud: an engine with a hole in its
// safety flags must never construct.
func New(cfg Config) (*Engine, error) {
	switch {
	case cfg.Bin == "":
		return nil, fmt.Errorf("engine: no binary path — the invocation must be pinned, never $PATH luck (§7)")
	case cfg.Model == "":
		return nil, fmt.Errorf("engine: no model — the CLI default is settings-derived, pin it (§8)")
	case cfg.MaxTurns < 1:
		return nil, fmt.Errorf("engine: max turns %d — the §11 runaway cap must be positive", cfg.MaxTurns)
	case cfg.TurnTimeout <= 0:
		return nil, fmt.Errorf("engine: turn timeout %v — a turn without a wall clock is the §11 runaway shape", cfg.TurnTimeout)
	}
	logger := cfg.Logger
	if logger == nil {
		logger = slog.Default()
	}
	return &Engine{
		bin:      cfg.Bin,
		model:    cfg.Model,
		maxTurns: cfg.MaxTurns,
		timeout:  cfg.TurnTimeout,
		logger:   logger,
	}, nil
}

// Start runs a session's first turn: claude -p --session-id <pinned
// uuid>, from the session's recorded cwd (§4.1).
func (e *Engine) Start(ctx context.Context, spec session.Spec) error {
	return e.run(ctx, spec, "--session-id", spec.SessionID)
}

// Resume runs a later turn: claude -p --resume <id>, same cwd (§4.1).
// A missing transcript maps to session.ErrResumeFailed so the §4.6
// degradation can trigger.
func (e *Engine) Resume(ctx context.Context, spec session.Spec) error {
	return e.run(ctx, spec, "--resume", spec.SessionID)
}

// waitDelay is the grace between the timeout's SIGTERM and the hard
// kill of a child that ignores it — long enough for the CLI to flush
// its transcript, short enough that a wedged child cannot hold the
// thread queue meaningfully past its timeout.
const waitDelay = 10 * time.Second

func (e *Engine) run(ctx context.Context, spec session.Spec, sessionFlag, sessionID string) error {
	ctx, cancel := context.WithTimeout(ctx, e.timeout)
	defer cancel()

	// THE pinned form (§7 — tested, see TestStartInvocationFormPinned):
	// -p, model pin, turn cap, then exactly one session flag. The
	// prompt travels on stdin: argv is world-readable in process
	// listings, and message content must not leak there (§7).
	args := []string{
		"-p",
		"--model", e.model,
		"--max-turns", strconv.Itoa(e.maxTurns),
		sessionFlag, sessionID,
	}
	cmd := exec.CommandContext(ctx, e.bin, args...)
	cmd.Dir = spec.Cwd
	cmd.Stdin = strings.NewReader(promptText(spec))
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	// The child leads its own process group: the CLI spawns tool
	// children of its own, and a timeout that kills only the leader
	// leaves grandchildren running — holding this thread's pipes open
	// (wedging the queue past its own timeout) and burning the box
	// (§11). Teardown must take the whole tree.
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	// Timeout kill (§11): TERM the group first so the CLI can flush
	// its transcript; WaitDelay hard-kills a leader that ignores it,
	// and the group SIGKILL sweep below catches ignoring grandchildren.
	cmd.Cancel = func() error {
		return syscall.Kill(-cmd.Process.Pid, syscall.SIGTERM)
	}
	cmd.WaitDelay = waitDelay

	begin := time.Now()
	err := cmd.Run()
	if cmd.Process != nil && ctx.Err() != nil {
		// Straggler sweep after a killed turn: anything in the group
		// that outlived TERM + WaitDelay dies now. ESRCH (group already
		// gone — the normal case) is fine.
		_ = syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)
	}
	if err == nil {
		e.logger.Info("engine turn completed",
			"session_id", sessionID, "duration_ms", time.Since(begin).Milliseconds())
		return nil
	}
	if ctx.Err() != nil {
		// The turn hit its wall clock (or the daemon's shutdown): the
		// child was killed. NOT transcript loss — the session survives;
		// the event layer owns the §4.6 disposition.
		return fmt.Errorf("engine: turn for %s killed after %v: %w", sessionID, time.Since(begin).Round(time.Millisecond), ctx.Err())
	}
	if isTranscriptGone(stderr.String()) {
		return fmt.Errorf("engine: %s: %s: %w", sessionID, excerpt(stderr.String()), session.ErrResumeFailed)
	}
	return fmt.Errorf("engine: turn for %s: %v: %s", sessionID, err, excerpt(stderr.String()))
}

// promptText assembles stdin: the §4.6 transparency note (when a
// degradation successor carries one) leads, framed as a system note so
// the model relays it, then the event text.
func promptText(spec session.Spec) string {
	if spec.TransparencyNote == "" {
		return spec.Prompt
	}
	return "[system note: " + spec.TransparencyNote + "]\n\n" + spec.Prompt
}

// isTranscriptGone recognizes the CLI's missing-transcript refusal.
// The phrase is version-sensitive — the CLI version pin (x6n.2.10)
// is what makes string-matching it defensible; the pinned-version
// drill re-verifies it on every bump.
func isTranscriptGone(stderr string) bool {
	return strings.Contains(strings.ToLower(stderr), "no conversation found")
}

// excerpt bounds stderr for error messages: enough to diagnose, not
// enough to relay a whole transcript into logs (stderr is engine
// diagnostics, but bound it anyway — fail contained).
func excerpt(s string) string {
	s = strings.TrimSpace(s)
	if len(s) > 500 {
		s = s[:500] + "…"
	}
	if s == "" {
		return "(no stderr)"
	}
	return s
}

// Interface conformance: the daemon hands this to session.NewManager.
var _ session.Engine = (*Engine)(nil)
