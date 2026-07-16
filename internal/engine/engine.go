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
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/brian-bell/approach/internal/session"
	"github.com/brian-bell/approach/internal/store"
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
	// RecordTurn receives every turn's C11 observability row (§6) —
	// the daemon wires it to store.InsertTurn. Required: the turns
	// table feeds the §7 cost alarm, and an engine that burns tokens
	// invisibly is the quiet degradation that alarm exists to prevent.
	// A returned error is logged loudly but never fails the turn —
	// erroring a turn whose engine work succeeded would invite a §4.6
	// replay of completed side effects.
	RecordTurn func(context.Context, store.Turn) error
	Logger     *slog.Logger
}

// Engine spawns claude -p children. One engine per daemon; safe for
// concurrent use (it holds no per-turn state).
type Engine struct {
	bin        string
	model      string
	maxTurns   int
	timeout    time.Duration
	recordTurn func(context.Context, store.Turn) error
	logger     *slog.Logger
}

// New validates the pin. Fail-loud: an engine with a hole in its
// safety flags must never construct.
func New(cfg Config) (*Engine, error) {
	switch {
	case cfg.Bin == "":
		return nil, fmt.Errorf("engine: no binary path — the invocation must be pinned, never $PATH luck (§7)")
	case !filepath.IsAbs(cfg.Bin):
		// A relative name resolves through the daemon's PATH at spawn
		// time — a changed or attacker-influenced PATH would swap the
		// engine out from under the pin. Absolute or nothing.
		return nil, fmt.Errorf("engine: binary path %q is not absolute — a PATH-resolved engine is not pinned (§7)", cfg.Bin)
	case cfg.Model == "":
		return nil, fmt.Errorf("engine: no model — the CLI default is settings-derived, pin it (§8)")
	case cfg.MaxTurns < 1:
		return nil, fmt.Errorf("engine: max turns %d — the §11 runaway cap must be positive", cfg.MaxTurns)
	case cfg.TurnTimeout <= 0:
		return nil, fmt.Errorf("engine: turn timeout %v — a turn without a wall clock is the §11 runaway shape", cfg.TurnTimeout)
	case cfg.RecordTurn == nil:
		return nil, fmt.Errorf("engine: no turn recorder — an engine that burns tokens invisibly defeats the §7 cost alarm (C11)")
	}
	logger := cfg.Logger
	if logger == nil {
		logger = slog.Default()
	}
	return &Engine{
		bin:        cfg.Bin,
		model:      cfg.Model,
		maxTurns:   cfg.MaxTurns,
		timeout:    cfg.TurnTimeout,
		recordTurn: cfg.RecordTurn,
		logger:     logger,
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
	// -p, model pin, turn cap, the stream-json output the C11 turn
	// record parses (--verbose is the CLI's own requirement for
	// stream-json under -p), then exactly one session flag. The prompt
	// travels on stdin: argv is world-readable in process listings, and
	// message content must not leak there (§7).
	args := []string{
		"-p",
		"--model", e.model,
		"--max-turns", strconv.Itoa(e.maxTurns),
		"--output-format", "stream-json", "--verbose",
		sessionFlag, sessionID,
	}
	cmd := exec.CommandContext(ctx, e.bin, args...)
	cmd.Dir = spec.Cwd
	cmd.Stdin = strings.NewReader(promptText(spec))
	// stdout carries the stream-json event feed; the collector keeps
	// the C11 score (model, tool calls, result usage) as it streams —
	// and forwards assistant text to the turn's Output sink (the reply
	// relay) — bounded against a child that floods it (§11).
	stats := &turnStats{output: spec.Output}
	cmd.Stdout = stats
	// Bounded: stderr is diagnostics, and an unbounded buffer hands a
	// misbehaving (or prompt-injected) child a path to exhaust daemon
	// memory long before its timeout — the cap is enforced at write
	// time, not after the fact.
	stderr := &boundedBuffer{limit: stderrCap}
	cmd.Stderr = stderr
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
		return termGroup(cmd.Process.Pid)
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
	// The C11 record happens on EVERY exit path — a killed or failed
	// turn still burned tokens and must be visible to the §7 spend
	// query. WithoutCancel: the turn happened; a shutdown that killed
	// it must not also lose its record (same rule as the ack write).
	e.record(context.WithoutCancel(ctx), spec, stats.snapshot(), begin, err, ctx.Err())
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

// record builds and hands off one turn's C11 row (§6). Outcome is the
// closed §6 enum: timeout when the wall clock killed the child, error
// for any other failure (including a result event that reports one),
// ok otherwise — 'denied' arrives with the C9 policy gate, not here.
// Usage is known only when the result event arrived; a killed child's
// tokens are NOBODY's to invent, so they stay unknown and land NULL.
// A recorder failure is a loud log, never a turn failure: erroring a
// turn whose engine work succeeded would invite a §4.6 replay of
// completed side effects.
func (e *Engine) record(ctx context.Context, spec session.Spec, snap statsSnapshot, begin time.Time, runErr, ctxErr error) {
	turn := store.Turn{
		SessionID:  spec.SessionID,
		TS:         time.Now().Unix(),
		Kind:       spec.Kind,
		Model:      snap.model,
		ToolCalls:  snap.toolCalls,
		DurationMS: time.Since(begin).Milliseconds(),
		Outcome:    "ok",
	}
	if snap.resultSeen {
		// The CLI's own accounting beats the engine's wall clock, and
		// an errored result still burned real tokens — usage stays
		// known so the spend query counts it (§7).
		turn.UsageKnown = true
		turn.InputTokens = snap.inputTokens
		turn.OutputTokens = snap.outputTokens
		turn.CostUSD = snap.costUSD
		turn.DurationMS = snap.durationMS
		if !snap.resultOK {
			turn.Outcome = "error"
		}
	}
	switch {
	case errors.Is(ctxErr, context.DeadlineExceeded):
		turn.Outcome = "timeout"
	case ctxErr != nil:
		// Shutdown cancellation killed the child — not a wall-clock
		// timeout, and the closed enum has no 'interrupted': the event
		// layer owns that disposition (§4.6); here it is an error.
		turn.Outcome = "error"
	case runErr != nil:
		turn.Outcome = "error"
	}
	if err := e.recordTurn(ctx, turn); err != nil {
		e.logger.Error("turn record failed — the §7 spend query is missing this turn",
			"session_id", spec.SessionID, "outcome", turn.Outcome, "error", err.Error())
	}
}

// termGroup TERMs a spawn's process group, translating ESRCH into
// os.ErrProcessDone: cancellation can race a clean exit, and the
// exec.Cmd.Cancel contract records the command as FAILED if Cancel
// returns any other error after a successful run — a completed turn
// must never be misfiled as a failure by its own teardown.
func termGroup(pid int) error {
	err := syscall.Kill(-pid, syscall.SIGTERM)
	if errors.Is(err, syscall.ESRCH) {
		return os.ErrProcessDone
	}
	return err
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

// stderrCap bounds how much child stderr the daemon retains: plenty
// for the CLI's error lines (the transcript-gone match needs only
// one), nothing a hostile writer can grow.
const stderrCap = 64 * 1024

// boundedBuffer keeps the first limit bytes and drops (but counts as
// accepted) the rest — a Write error here would kill the child's
// stderr pipe mid-turn.
type boundedBuffer struct {
	buf       bytes.Buffer
	limit     int
	truncated bool
}

func (b *boundedBuffer) Write(p []byte) (int, error) {
	if room := b.limit - b.buf.Len(); room > 0 {
		if len(p) > room {
			b.buf.Write(p[:room])
			b.truncated = true
		} else {
			b.buf.Write(p)
		}
	} else if len(p) > 0 {
		b.truncated = true
	}
	return len(p), nil
}

func (b *boundedBuffer) String() string {
	if b.truncated {
		return b.buf.String() + "…(stderr truncated)"
	}
	return b.buf.String()
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
