package store

import (
	"context"
	"database/sql"
	"fmt"
)

// Turn is one row of the C11 observability table (§6): what one engine
// turn cost and how it ended, parsed from the CLI's stream-json result
// events. It feeds the §7 daily-spend query (the heartbeat's cost
// alarm), the §4.3 token-bloat check, and P4 tuning data.
type Turn struct {
	SessionID string // the turn's session (§6) — must exist, FK-enforced
	TS        int64  // unix seconds when the turn ended
	Kind      string // event kind that drove the turn; "" = none (stored NULL)
	Model     string // served model from the stream's init event; "" = unknown (stored NULL)
	// InputTokens, OutputTokens, and CostUSD come from the result
	// event's usage and are meaningful only when UsageKnown is true —
	// a child killed before its result event has usage NOBODY knows,
	// and storing zero would read as a free turn in the spend query.
	InputTokens  int64
	OutputTokens int64
	CostUSD      float64
	ToolCalls    int64  // tool_use blocks observed on the stream (known even for a killed turn)
	DurationMS   int64  // result event's duration when it arrived, else engine wall clock
	Outcome      string // ok | error | denied | timeout (§6 closed enum)
	UsageKnown   bool   // false → input_tokens, output_tokens, cost_usd land NULL
}

// turnKinds mirrors the events table's closed kind enum (§6): a turn's
// kind is the kind of the event that drove it, so the same spellings —
// and only those — are legal here.
var turnKinds = map[string]bool{
	"message": true, "heartbeat": true, "webhook": true,
	"cron": true, "approval": true, "task": true,
}

// turnOutcomes is the §6 outcome enum, closed here as well as in
// schema so a bad outcome names itself instead of surfacing as an
// opaque CHECK violation.
var turnOutcomes = map[string]bool{
	"ok": true, "error": true, "denied": true, "timeout": true,
}

// InsertTurn records one engine turn (§6 C11). Validation fails loud
// before the db is touched: a malformed row would poison the §7 spend
// query silently — the exact quiet degradation the cost alarm exists
// to prevent. Unknown usage lands NULL, never zero (absence is
// absence, same rule as tool_attempts' idempotency_key).
func InsertTurn(ctx context.Context, db *sql.DB, t Turn) (id int64, err error) {
	if err := t.validate(); err != nil {
		return 0, fmt.Errorf("store: insert turn: %w", err)
	}
	// "" means "absent" and must land as NULL — an empty-string kind
	// would escape the closed enum, and an empty model would read as a
	// model named "".
	var kind, model any
	if t.Kind != "" {
		kind = t.Kind
	}
	if t.Model != "" {
		model = t.Model
	}
	var inTok, outTok, cost any
	if t.UsageKnown {
		inTok, outTok, cost = t.InputTokens, t.OutputTokens, t.CostUSD
	}
	res, err := db.ExecContext(ctx,
		`INSERT INTO turns (session_id, ts, kind, model, input_tokens, output_tokens, cost_usd, tool_calls, duration_ms, outcome)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		t.SessionID, t.TS, kind, model, inTok, outTok, cost, t.ToolCalls, t.DurationMS, t.Outcome,
	)
	if err != nil {
		return 0, fmt.Errorf("store: insert turn for session %s: %w", t.SessionID, err)
	}
	id, err = res.LastInsertId()
	if err != nil {
		return 0, fmt.Errorf("store: insert turn for session %s: %w", t.SessionID, err)
	}
	return id, nil
}

// validate refuses a turn the §7 spend query could not reason from.
func (t Turn) validate() error {
	switch {
	case t.SessionID == "":
		return fmt.Errorf("empty session_id — the turn would have no provenance (§6)")
	case t.TS <= 0:
		return fmt.Errorf("ts = %d, want a positive unix timestamp", t.TS)
	case t.Kind != "" && !turnKinds[t.Kind]:
		return fmt.Errorf("kind %q is not an event kind — the enum is closed (§6)", t.Kind)
	case !turnOutcomes[t.Outcome]:
		return fmt.Errorf("outcome %q is not ok|error|denied|timeout — the enum is closed (§6)", t.Outcome)
	case t.InputTokens < 0 || t.OutputTokens < 0:
		return fmt.Errorf("tokens (%d, %d) negative", t.InputTokens, t.OutputTokens)
	case t.CostUSD < 0:
		return fmt.Errorf("cost_usd %v negative", t.CostUSD)
	case t.ToolCalls < 0:
		return fmt.Errorf("tool_calls = %d negative", t.ToolCalls)
	case t.DurationMS < 0:
		return fmt.Errorf("duration_ms = %d negative", t.DurationMS)
	}
	return nil
}
