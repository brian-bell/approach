package engine_test

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/brian-bell/approach/internal/engine"
	"github.com/brian-bell/approach/internal/store"
)

// recorder captures the turns the engine records (C11) — the seam the
// daemon wires to store.InsertTurn.
type recorder struct {
	mu    sync.Mutex
	turns []store.Turn
	fail  error
}

func (r *recorder) record(_ context.Context, t store.Turn) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.turns = append(r.turns, t)
	return r.fail
}

func (r *recorder) one(t *testing.T) store.Turn {
	t.Helper()
	r.mu.Lock()
	defer r.mu.Unlock()
	if len(r.turns) != 1 {
		t.Fatalf("recorded %d turns, want exactly 1: %+v", len(r.turns), r.turns)
	}
	return r.turns[0]
}

func newRecordingEngine(t *testing.T, bin string, timeout time.Duration) (*engine.Engine, *recorder) {
	t.Helper()
	rec := &recorder{}
	e, err := engine.New(engine.Config{
		Bin:         bin,
		Model:       "claude-sonnet-5",
		MaxTurns:    25,
		TurnTimeout: timeout,
		RecordTurn:  rec.record,
		Logger:      discardLogger(),
	})
	if err != nil {
		t.Fatalf("engine.New: %v", err)
	}
	return e, rec
}

// stream is a realistic pinned-CLI stream-json transcript: init names
// the served model, assistant events carry tool_use blocks, and the
// result event carries the C11 numbers (§6).
const stream = `echo '{"type":"system","subtype":"init","model":"claude-sonnet-5-20260115","tools":["Bash"]}'
echo 'not json at all — the parser must skim past it'
echo '{"type":"assistant","message":{"content":[{"type":"text","text":"looking"},{"type":"tool_use","name":"Bash","input":{}}]}}'
echo '{"type":"user","message":{"content":[{"type":"tool_result","content":"ok"}]}}'
echo '{"type":"assistant","message":{"content":[{"type":"tool_use","name":"Read","input":{}},{"type":"tool_use","name":"Edit","input":{}}]}}'
echo '{"type":"result","subtype":"success","is_error":false,"duration_ms":5150,"num_turns":3,"total_cost_usd":0.0421,"usage":{"input_tokens":1200,"output_tokens":340}}'
`

// TestTurnRecordedFromResultEvent: the C11 contract — a completed turn
// lands with model (from init), tokens and cost (from the result
// event), tool calls (counted as they streamed), duration, and outcome
// (§6). Garbled and unknown lines are skimmed, never fatal:
// observability parsing must not fail a turn that succeeded.
func TestTurnRecordedFromResultEvent(t *testing.T) {
	dir := t.TempDir()
	bin := fakeCLI(t, dir, stream+"exit 0")
	e, rec := newRecordingEngine(t, bin, 30*time.Second)

	s := spec(t.TempDir())
	s.Kind = "message"
	if err := e.Start(context.Background(), s); err != nil {
		t.Fatalf("Start: %v", err)
	}

	turn := rec.one(t)
	if turn.SessionID != s.SessionID || turn.Kind != "message" {
		t.Errorf("(session, kind) = (%q, %q), want (%q, %q)", turn.SessionID, turn.Kind, s.SessionID, "message")
	}
	if turn.Model != "claude-sonnet-5-20260115" {
		t.Errorf("model = %q, want the SERVED model from the init event", turn.Model)
	}
	if !turn.UsageKnown || turn.InputTokens != 1200 || turn.OutputTokens != 340 || turn.CostUSD != 0.0421 {
		t.Errorf("usage = (known=%v, %d, %d, %v), want (true, 1200, 340, 0.0421)",
			turn.UsageKnown, turn.InputTokens, turn.OutputTokens, turn.CostUSD)
	}
	if turn.ToolCalls != 3 {
		t.Errorf("tool_calls = %d, want 3 tool_use blocks counted across assistant events", turn.ToolCalls)
	}
	if turn.DurationMS != 5150 {
		t.Errorf("duration_ms = %d, want the result event's 5150", turn.DurationMS)
	}
	if turn.Outcome != "ok" {
		t.Errorf("outcome = %q, want ok", turn.Outcome)
	}
	if turn.TS <= 0 {
		t.Errorf("ts = %d, want a positive unix timestamp", turn.TS)
	}
}

// TestTurnRecordedOnErrorResult: a turn that ends in an error result
// still burned tokens — usage stays known so the §7 spend query counts
// it; only the outcome says error.
func TestTurnRecordedOnErrorResult(t *testing.T) {
	dir := t.TempDir()
	body := `echo '{"type":"system","subtype":"init","model":"claude-sonnet-5-20260115"}'
echo '{"type":"result","subtype":"error_max_turns","is_error":true,"duration_ms":900,"total_cost_usd":0.9,"usage":{"input_tokens":50000,"output_tokens":9000}}'
exit 1`
	bin := fakeCLI(t, dir, body)
	e, rec := newRecordingEngine(t, bin, 30*time.Second)

	if err := e.Start(context.Background(), spec(t.TempDir())); err == nil {
		t.Fatal("Start returned nil from a failing CLI")
	}
	turn := rec.one(t)
	if turn.Outcome != "error" {
		t.Errorf("outcome = %q, want error", turn.Outcome)
	}
	if !turn.UsageKnown || turn.CostUSD != 0.9 || turn.InputTokens != 50000 {
		t.Errorf("usage = (known=%v, %d, %v) — an errored turn's burn must still count in the spend query (§7)",
			turn.UsageKnown, turn.InputTokens, turn.CostUSD)
	}
}

// TestTurnRecordedOnTimeout: a child killed at its wall clock never
// sent a result event — outcome timeout, usage UNKNOWN (never a
// fabricated zero), tool calls counted from what did stream, duration
// from the engine's own clock (§6, §11).
func TestTurnRecordedOnTimeout(t *testing.T) {
	dir := t.TempDir()
	body := `echo '{"type":"system","subtype":"init","model":"claude-sonnet-5-20260115"}'
echo '{"type":"assistant","message":{"content":[{"type":"tool_use","name":"Bash","input":{}}]}}'
sleep 30
exit 0`
	bin := fakeCLI(t, dir, body)
	e, rec := newRecordingEngine(t, bin, 300*time.Millisecond)

	err := e.Start(context.Background(), spec(t.TempDir()))
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Start = %v, want a timeout", err)
	}
	turn := rec.one(t)
	if turn.Outcome != "timeout" {
		t.Errorf("outcome = %q, want timeout", turn.Outcome)
	}
	if turn.UsageKnown {
		t.Errorf("usage known on a killed turn — tokens nobody knows must land NULL, not zero (§7)")
	}
	if turn.ToolCalls != 1 {
		t.Errorf("tool_calls = %d, want the 1 that streamed before the kill", turn.ToolCalls)
	}
	if turn.Model != "claude-sonnet-5-20260115" {
		t.Errorf("model = %q, want the init event's model — it streamed before the kill", turn.Model)
	}
	if turn.DurationMS <= 0 {
		t.Errorf("duration_ms = %d, want the engine's measured wall clock", turn.DurationMS)
	}
}

// TestTurnRecordedOnFailureWithoutResult: a child that dies with no
// result event records an error turn with unknown usage — the turn
// happened and must be visible, even when the CLI told us nothing.
func TestTurnRecordedOnFailureWithoutResult(t *testing.T) {
	dir := t.TempDir()
	bin := fakeCLI(t, dir, `echo "boom" >&2; exit 7`)
	e, rec := newRecordingEngine(t, bin, 30*time.Second)

	if err := e.Start(context.Background(), spec(t.TempDir())); err == nil {
		t.Fatal("Start returned nil from a failing CLI")
	}
	turn := rec.one(t)
	if turn.Outcome != "error" || turn.UsageKnown {
		t.Errorf("(outcome, usageKnown) = (%q, %v), want (error, false)", turn.Outcome, turn.UsageKnown)
	}
}

// TestOverlongLineSkimmedNotFatal: one absurdly long stdout line (a
// giant tool input) must neither grow daemon memory without bound nor
// desync the parser — the line is dropped, and the result event after
// it still lands (fail contained, §11).
func TestOverlongLineSkimmedNotFatal(t *testing.T) {
	dir := t.TempDir()
	body := `printf '{"type":"assistant","padding":"'
head -c 8388608 /dev/zero | tr '\0' 'a'
printf '"}\n'
` + stream + "exit 0"
	bin := fakeCLI(t, dir, body)
	e, rec := newRecordingEngine(t, bin, 30*time.Second)

	if err := e.Start(context.Background(), spec(t.TempDir())); err != nil {
		t.Fatalf("Start: %v", err)
	}
	turn := rec.one(t)
	if !turn.UsageKnown || turn.CostUSD != 0.0421 || turn.ToolCalls != 3 {
		t.Errorf("turn after an overlong line = (known=%v, cost=%v, tools=%d), want the stream's real numbers",
			turn.UsageKnown, turn.CostUSD, turn.ToolCalls)
	}
}

// TestRecordFailureDoesNotFailTheTurn: the observability write is
// bookkeeping — a failed record is a loud log, never a turn failure
// that would invite a §4.6 replay of completed side effects.
func TestRecordFailureDoesNotFailTheTurn(t *testing.T) {
	dir := t.TempDir()
	bin := fakeCLI(t, dir, stream+"exit 0")
	e, rec := newRecordingEngine(t, bin, 30*time.Second)
	rec.fail = fmt.Errorf("db locked")

	if err := e.Start(context.Background(), spec(t.TempDir())); err != nil {
		t.Errorf("Start = %v — a record failure must not fail a turn whose engine work succeeded", err)
	}
}
