// stream.go parses the pinned CLI's --output-format stream-json stdout
// into the C11 turn record (§6) — the served model from the init event,
// tool_use blocks counted as they stream, the result event's usage —
// and feeds assistant text to the turn's reply sink (Spec.Output, the
// §4.1 live relay). Parsing is fail-contained by design — a garbled line, an
// unknown event type, or a line past the size cap is skimmed, never a
// turn failure: observability must not fail a turn that succeeded, and
// a hostile child must not grow daemon memory through its own stdout
// (§11, same posture as the stderr bound).
package engine

import (
	"bytes"
	"encoding/json"
	"strings"
	"sync"
)

// lineCap bounds one buffered stdout line. Result and init events are
// a few KB; assistant events carry whole tool inputs (a Write of a
// large file rides in its tool_use block), so the cap is generous —
// but it is a hard ceiling: a line past it is dropped to the next
// newline, costing at worst an undercounted tool_calls, never memory.
const lineCap = 4 * 1024 * 1024

// streamEvent is the slice of the CLI's stream-json schema this
// collector consumes: the C11 score (init model, tool_use counts, the
// result usage) plus the assistant text blocks the reply relay
// carries. Everything else in an event is deliberately ignored.
type streamEvent struct {
	Type    string `json:"type"`
	Subtype string `json:"subtype"`
	Model   string `json:"model"` // system/init: the SERVED model
	Message struct {
		Content []struct {
			Type string `json:"type"`
			Text string `json:"text"` // text blocks: the reply the relay carries
		} `json:"content"`
	} `json:"message"`
	IsError    bool    `json:"is_error"`
	DurationMS int64   `json:"duration_ms"`
	CostUSD    float64 `json:"total_cost_usd"`
	Usage      struct {
		InputTokens  int64 `json:"input_tokens"`
		OutputTokens int64 `json:"output_tokens"`
	} `json:"usage"`
}

// turnStats collects the C11 numbers as the child streams. It is the
// engine's cmd.Stdout: Write runs on exec's copy goroutine, and a
// killed turn's Wait can return while that goroutine drains its last
// read — the mutex makes the post-Wait snapshot safe under that race.
type turnStats struct {
	mu         sync.Mutex
	buf        bytes.Buffer // current partial line
	discarding bool         // inside a line past lineCap — drop to next newline

	// output is the turn's Spec.Output sink (nil = discard): each
	// assistant message's text streams to it as it parses — the
	// reply-relay feed. Called under mu from the copy goroutine; the
	// Spec contract makes prompt return the sink's job.
	output func(delta string)

	model        string
	toolCalls    int64
	resultSeen   bool
	resultOK     bool
	durationMS   int64
	costUSD      float64
	inputTokens  int64
	outputTokens int64
}

// statsSnapshot is what run() reads after the child exits.
type statsSnapshot struct {
	model        string
	toolCalls    int64
	resultSeen   bool
	resultOK     bool
	durationMS   int64
	costUSD      float64
	inputTokens  int64
	outputTokens int64
}

func (s *turnStats) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	accepted := len(p)
	for len(p) > 0 {
		nl := bytes.IndexByte(p, '\n')
		if nl < 0 {
			if !s.discarding {
				if s.buf.Len()+len(p) > lineCap {
					s.discarding = true
					s.buf.Reset()
				} else {
					s.buf.Write(p)
				}
			}
			break
		}
		if s.discarding {
			// The overlong line ends here; resync on the next one.
			s.discarding = false
		} else if s.buf.Len()+nl > lineCap {
			s.buf.Reset()
		} else {
			s.buf.Write(p[:nl])
			s.consumeLine()
		}
		p = p[nl+1:]
	}
	// Always accept everything: a Write error would kill the child's
	// stdout pipe mid-turn (same contract as boundedBuffer).
	return accepted, nil
}

// consumeLine parses one complete stdout line and resets the buffer.
// Non-JSON and unknown shapes are skimmed silently — the CLI is
// entitled to grow new event types under the version pin's watch.
func (s *turnStats) consumeLine() {
	line := s.buf.Bytes()
	defer s.buf.Reset()
	var ev streamEvent
	if err := json.Unmarshal(line, &ev); err != nil {
		return
	}
	switch ev.Type {
	case "system":
		if ev.Subtype == "init" && ev.Model != "" {
			s.model = ev.Model
		}
	case "assistant":
		// One Output call per assistant MESSAGE, its text blocks joined
		// verbatim: adjacent blocks are one utterance, and injecting
		// anything between them would alter the model's own text. The
		// message boundary is the only separator the sink may infer —
		// it is carried by the granularity of the calls themselves.
		var text strings.Builder
		for _, block := range ev.Message.Content {
			switch block.Type {
			case "tool_use":
				s.toolCalls++
			case "text":
				text.WriteString(block.Text)
			}
		}
		// An empty message carries nothing to relay and would only
		// churn the relay's throttle windows.
		if s.output != nil && text.Len() > 0 {
			s.output(text.String())
		}
	case "result":
		s.resultSeen = true
		s.resultOK = !ev.IsError && ev.Subtype == "success"
		s.durationMS = ev.DurationMS
		s.costUSD = ev.CostUSD
		s.inputTokens = ev.Usage.InputTokens
		s.outputTokens = ev.Usage.OutputTokens
	}
}

// snapshot reads the collected stats after the child exited.
func (s *turnStats) snapshot() statsSnapshot {
	s.mu.Lock()
	defer s.mu.Unlock()
	return statsSnapshot{
		model:        s.model,
		toolCalls:    s.toolCalls,
		resultSeen:   s.resultSeen,
		resultOK:     s.resultOK,
		durationMS:   s.durationMS,
		costUSD:      s.costUSD,
		inputTokens:  s.inputTokens,
		outputTokens: s.outputTokens,
	}
}
