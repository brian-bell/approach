package engine_test

import (
	"context"
	"strings"
	"sync"
	"testing"
	"time"
)

// deltaSink collects Spec.Output calls under a mutex — the engine
// invokes the sink from exec's stdout copy goroutine.
type deltaSink struct {
	mu     sync.Mutex
	deltas []string
}

func (d *deltaSink) push(delta string) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.deltas = append(d.deltas, delta)
}

func (d *deltaSink) all() []string {
	d.mu.Lock()
	defer d.mu.Unlock()
	return append([]string(nil), d.deltas...)
}

// TestOutputSinkReceivesAssistantText: the reply-relay seam — each
// assistant MESSAGE's text reaches Spec.Output as one call, its blocks
// joined verbatim (injecting anything between adjacent blocks would
// alter the model's own text; the message boundary is the only
// separator the sink may infer). Tool_use blocks, user events, and the
// result event carry no reply text and must not reach the sink.
func TestOutputSinkReceivesAssistantText(t *testing.T) {
	dir := t.TempDir()
	body := `echo '{"type":"system","subtype":"init","model":"claude-sonnet-5-20260115"}'
echo '{"type":"assistant","message":{"content":[{"type":"text","text":"looking at it"},{"type":"tool_use","name":"Bash","input":{}}]}}'
echo '{"type":"user","message":{"content":[{"type":"tool_result","content":"tool text must not relay"}]}}'
echo '{"type":"assistant","message":{"content":[{"type":"text","text":"first half, "},{"type":"text","text":"second half"}]}}'
echo '{"type":"result","subtype":"success","is_error":false,"duration_ms":10,"total_cost_usd":0.01,"usage":{"input_tokens":1,"output_tokens":2},"result":"result text must not double-relay"}'
exit 0`
	bin := fakeCLI(t, dir, body)
	e, _ := newRecordingEngine(t, bin, 30*time.Second)

	sink := &deltaSink{}
	s := spec(t.TempDir())
	s.Output = sink.push
	if err := e.Start(context.Background(), s); err != nil {
		t.Fatalf("Start: %v", err)
	}

	got := sink.all()
	want := []string{"looking at it", "first half, second half"}
	if strings.Join(got, "|") != strings.Join(want, "|") {
		t.Errorf("output deltas = %q, want %q", got, want)
	}
}

// TestOutputSinkEmptyTextSkipped: an empty text block carries nothing
// to relay — pushing it would only churn the relay's throttle windows.
func TestOutputSinkEmptyTextSkipped(t *testing.T) {
	dir := t.TempDir()
	body := `echo '{"type":"assistant","message":{"content":[{"type":"text","text":""}]}}'
echo '{"type":"result","subtype":"success","is_error":false,"duration_ms":10,"total_cost_usd":0.01,"usage":{"input_tokens":1,"output_tokens":2}}'
exit 0`
	bin := fakeCLI(t, dir, body)
	e, _ := newRecordingEngine(t, bin, 30*time.Second)

	sink := &deltaSink{}
	s := spec(t.TempDir())
	s.Output = sink.push
	if err := e.Start(context.Background(), s); err != nil {
		t.Fatalf("Start: %v", err)
	}
	if got := sink.all(); len(got) != 0 {
		t.Errorf("output deltas = %q, want none for an empty text block", got)
	}
}

// TestOutputSinkNilIsDiscard: a nil Output must not panic — turns
// outside a channel context (nothing to relay to) simply discard.
func TestOutputSinkNilIsDiscard(t *testing.T) {
	dir := t.TempDir()
	body := `echo '{"type":"assistant","message":{"content":[{"type":"text","text":"hello"}]}}'
echo '{"type":"result","subtype":"success","is_error":false,"duration_ms":10,"total_cost_usd":0.01,"usage":{"input_tokens":1,"output_tokens":2}}'
exit 0`
	bin := fakeCLI(t, dir, body)
	e, _ := newRecordingEngine(t, bin, 30*time.Second)
	if err := e.Start(context.Background(), spec(t.TempDir())); err != nil {
		t.Fatalf("Start with nil Output: %v", err)
	}
}
