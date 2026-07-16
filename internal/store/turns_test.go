package store_test

import (
	"context"
	"database/sql"
	"math"
	"path/filepath"
	"strings"
	"testing"

	"github.com/brian-bell/approach/internal/store"
)

// testTurn is a fully-known turn: a result event arrived, so usage is
// populated (§6 C11).
func testTurn(sessionID string) store.Turn {
	return store.Turn{
		SessionID:    sessionID,
		TS:           1700000200,
		Kind:         "message",
		Model:        "claude-sonnet-5",
		InputTokens:  1200,
		OutputTokens: 340,
		CostUSD:      0.0421,
		ToolCalls:    3,
		DurationMS:   5150,
		Outcome:      "ok",
		UsageKnown:   true,
	}
}

// TestTurnsTableAndTSIndexExist: the C11 observability substrate —
// the turns table and the timestamp index the §7 daily-spend query
// scans — must exist on a fresh store.
func TestTurnsTableAndTSIndexExist(t *testing.T) {
	db := mustOpen(t, filepath.Join(t.TempDir(), "state", "approach.db"))

	for _, obj := range []struct{ typ, name string }{
		{"table", "turns"},
		{"index", "turns_by_ts"},
	} {
		var n int
		if err := db.QueryRow(
			`SELECT count(*) FROM sqlite_master WHERE type = ? AND name = ?`,
			obj.typ, obj.name,
		).Scan(&n); err != nil {
			t.Fatalf("query sqlite_master for %s %s: %v", obj.typ, obj.name, err)
		}
		if n != 1 {
			t.Errorf("%s %s: found %d in sqlite_master, want 1", obj.typ, obj.name, n)
		}
	}
}

// TestInsertTurnRoundTrip: one completed turn lands with every C11
// field the cost alarm and tuning queries read (§6).
func TestInsertTurnRoundTrip(t *testing.T) {
	db := mustOpen(t, filepath.Join(t.TempDir(), "state", "approach.db"))
	sessionID := seedSession(t, db)

	turn := testTurn(sessionID)
	id, err := store.InsertTurn(context.Background(), db, turn)
	if err != nil {
		t.Fatalf("InsertTurn: %v", err)
	}
	if id <= 0 {
		t.Errorf("id = %d, want a positive row id", id)
	}

	var (
		gotSession, kind, model, outcome string
		ts, toolCalls, durationMS        int64
		inTok, outTok                    sql.NullInt64
		cost                             sql.NullFloat64
	)
	if err := db.QueryRow(
		`SELECT session_id, ts, kind, model, input_tokens, output_tokens, cost_usd, tool_calls, duration_ms, outcome
		 FROM turns WHERE id = ?`, id,
	).Scan(&gotSession, &ts, &kind, &model, &inTok, &outTok, &cost, &toolCalls, &durationMS, &outcome); err != nil {
		t.Fatalf("read back turn: %v", err)
	}
	if gotSession != turn.SessionID || ts != turn.TS || kind != turn.Kind || model != turn.Model {
		t.Errorf("identity fields did not round-trip: got (%q, %d, %q, %q)", gotSession, ts, kind, model)
	}
	if !inTok.Valid || inTok.Int64 != turn.InputTokens || !outTok.Valid || outTok.Int64 != turn.OutputTokens {
		t.Errorf("tokens = (%v, %v), want (%d, %d)", inTok, outTok, turn.InputTokens, turn.OutputTokens)
	}
	if !cost.Valid || cost.Float64 != turn.CostUSD {
		t.Errorf("cost_usd = %v, want %v", cost, turn.CostUSD)
	}
	if toolCalls != turn.ToolCalls || durationMS != turn.DurationMS || outcome != turn.Outcome {
		t.Errorf("(tool_calls, duration_ms, outcome) = (%d, %d, %q), want (%d, %d, %q)",
			toolCalls, durationMS, outcome, turn.ToolCalls, turn.DurationMS, turn.Outcome)
	}
}

// TestInsertTurnUnknownUsageStoresNull: a turn whose child died before
// its result event (timeout, kill) has no usage to report — tokens and
// cost land NULL, never a fabricated zero that would read as "free
// turn" in the §7 spend query. Empty kind and model land NULL too:
// absence is absence, same rule as tool_attempts.
func TestInsertTurnUnknownUsageStoresNull(t *testing.T) {
	db := mustOpen(t, filepath.Join(t.TempDir(), "state", "approach.db"))
	sessionID := seedSession(t, db)

	id, err := store.InsertTurn(context.Background(), db, store.Turn{
		SessionID:  sessionID,
		TS:         1700000300,
		ToolCalls:  1,
		DurationMS: 30000,
		Outcome:    "timeout",
	})
	if err != nil {
		t.Fatalf("InsertTurn: %v", err)
	}

	var (
		kind, model   sql.NullString
		inTok, outTok sql.NullInt64
		cost          sql.NullFloat64
	)
	if err := db.QueryRow(
		`SELECT kind, model, input_tokens, output_tokens, cost_usd FROM turns WHERE id = ?`, id,
	).Scan(&kind, &model, &inTok, &outTok, &cost); err != nil {
		t.Fatalf("read back turn: %v", err)
	}
	if kind.Valid || model.Valid {
		t.Errorf("(kind, model) = (%v, %v), want NULL for absent values", kind, model)
	}
	if inTok.Valid || outTok.Valid || cost.Valid {
		t.Errorf("usage = (%v, %v, %v), want NULL — unknown usage must not read as a free turn (§7)", inTok, outTok, cost)
	}
}

// TestInsertTurnValidation: a row the cost alarm could not reason from
// is refused before the db is touched — closed enums, positive
// timestamps, non-negative counters (§6).
func TestInsertTurnValidation(t *testing.T) {
	db := mustOpen(t, filepath.Join(t.TempDir(), "state", "approach.db"))
	sessionID := seedSession(t, db)

	cases := []struct {
		name   string
		mutate func(*store.Turn)
	}{
		{"empty session_id", func(tn *store.Turn) { tn.SessionID = "" }},
		{"zero ts", func(tn *store.Turn) { tn.TS = 0 }},
		{"unknown kind", func(tn *store.Turn) { tn.Kind = "sms" }},
		{"unknown outcome", func(tn *store.Turn) { tn.Outcome = "success" }},
		{"empty outcome", func(tn *store.Turn) { tn.Outcome = "" }},
		{"negative input tokens", func(tn *store.Turn) { tn.InputTokens = -1 }},
		{"negative output tokens", func(tn *store.Turn) { tn.OutputTokens = -1 }},
		{"negative cost", func(tn *store.Turn) { tn.CostUSD = -0.01 }},
		{"negative tool calls", func(tn *store.Turn) { tn.ToolCalls = -1 }},
		{"negative duration", func(tn *store.Turn) { tn.DurationMS = -1 }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			turn := testTurn(sessionID)
			tc.mutate(&turn)
			if _, err := store.InsertTurn(context.Background(), db, turn); err == nil {
				t.Errorf("InsertTurn accepted a turn with %s", tc.name)
			}
		})
	}
}

// TestInsertTurnEveryOutcome: the §6 enum is exactly ok | error |
// denied | timeout — all four insert cleanly (denied arrives with the
// C9 policy gate; the schema must already hold it).
func TestInsertTurnEveryOutcome(t *testing.T) {
	db := mustOpen(t, filepath.Join(t.TempDir(), "state", "approach.db"))
	sessionID := seedSession(t, db)

	for _, outcome := range []string{"ok", "error", "denied", "timeout"} {
		turn := testTurn(sessionID)
		turn.Outcome = outcome
		if _, err := store.InsertTurn(context.Background(), db, turn); err != nil {
			t.Errorf("InsertTurn(outcome=%q): %v", outcome, err)
		}
	}
}

// TestInsertTurnRequiresKnownSession: turns reference sessions the
// daemon actually created — a row from a session that never existed is
// a bug surfacing loudly, not observability data (§6 provenance rule).
func TestInsertTurnRequiresKnownSession(t *testing.T) {
	db := mustOpen(t, filepath.Join(t.TempDir(), "state", "approach.db"))

	turn := testTurn("99999999-9999-4999-8999-999999999999")
	if _, err := store.InsertTurn(context.Background(), db, turn); err == nil {
		t.Error("InsertTurn accepted a turn for a session the daemon never created")
	} else if !strings.Contains(err.Error(), "turn") {
		t.Errorf("error %q does not identify the failing insert", err)
	}
}

// seedTurn inserts one turn with the given ts/cost — cost < 0 means
// usage unknown (a killed child's NULL row).
func seedTurn(t *testing.T, db *sql.DB, sessionID string, ts int64, costUSD float64) {
	t.Helper()
	turn := store.Turn{
		SessionID: sessionID, TS: ts, Kind: "message", Model: "claude-sonnet-5",
		ToolCalls: 1, DurationMS: 100, Outcome: "ok",
	}
	if costUSD >= 0 {
		turn.UsageKnown = true
		turn.CostUSD = costUSD
		turn.InputTokens = 10
		turn.OutputTokens = 10
	} else {
		turn.Outcome = "timeout"
	}
	if _, err := store.InsertTurn(context.Background(), db, turn); err != nil {
		t.Fatalf("InsertTurn(ts=%d): %v", ts, err)
	}
}

// TestDailySpend: the §7 heartbeat checklist query — cost summed over
// a half-open [since, until) window, with unknown-usage turns counted
// separately so the total reads as the LOWER BOUND it is, never as a
// complete answer it cannot be.
func TestDailySpend(t *testing.T) {
	db := mustOpen(t, filepath.Join(t.TempDir(), "state", "approach.db"))
	sessionID := seedSession(t, db)

	const midnight = int64(1700000000)
	seedTurn(t, db, sessionID, midnight-1, 99.0)   // yesterday — outside
	seedTurn(t, db, sessionID, midnight, 0.25)     // window start, inclusive
	seedTurn(t, db, sessionID, midnight+100, 0.50) // inside
	seedTurn(t, db, sessionID, midnight+200, -1)   // inside, usage unknown
	seedTurn(t, db, sessionID, midnight+86400, 77) // next window, exclusive

	got, err := store.DailySpend(context.Background(), db, midnight, midnight+86400)
	if err != nil {
		t.Fatalf("DailySpend: %v", err)
	}
	if got.KnownUSD != 0.75 {
		t.Errorf("KnownUSD = %v, want 0.75 — window edges are [since, until)", got.KnownUSD)
	}
	if got.Turns != 3 {
		t.Errorf("Turns = %d, want 3", got.Turns)
	}
	if got.UnknownTurns != 1 {
		t.Errorf("UnknownTurns = %d, want 1 — unknown burn must stay visible (§7)", got.UnknownTurns)
	}
}

// TestDailySpendEmptyWindow: no turns is zeros, not an error — a
// fresh store's checklist reads $0, and NULL SUM must not scan-fail.
func TestDailySpendEmptyWindow(t *testing.T) {
	db := mustOpen(t, filepath.Join(t.TempDir(), "state", "approach.db"))

	got, err := store.DailySpend(context.Background(), db, 0, 1)
	if err != nil {
		t.Fatalf("DailySpend on empty store: %v", err)
	}
	if got.KnownUSD != 0 || got.Turns != 0 || got.UnknownTurns != 0 {
		t.Errorf("empty window = %+v, want zeros", got)
	}
}

// TestDailySpendRefusesInvertedWindow: a window that ends before it
// starts is a caller bug answered loudly, not an empty $0 that would
// read as "no burn today".
func TestDailySpendRefusesInvertedWindow(t *testing.T) {
	db := mustOpen(t, filepath.Join(t.TempDir(), "state", "approach.db"))

	if _, err := store.DailySpend(context.Background(), db, 100, 100); err == nil {
		t.Error("DailySpend accepted an empty [since, until) window")
	}
	if _, err := store.DailySpend(context.Background(), db, 200, 100); err == nil {
		t.Error("DailySpend accepted an inverted window")
	}
}

// TestInsertTurnRejectsNonFiniteCost: a NaN cost would slip past the
// "< 0" check (NaN compares false to everything) and then poison the
// §7 spend SUM — one bad row and every daily total reads NaN, which
// also breaks the status verb's JSON. Same for infinities.
func TestInsertTurnRejectsNonFiniteCost(t *testing.T) {
	db := mustOpen(t, filepath.Join(t.TempDir(), "state", "approach.db"))
	sessionID := seedSession(t, db)

	for _, bad := range []float64{math.NaN(), math.Inf(1), math.Inf(-1)} {
		turn := testTurn(sessionID)
		turn.CostUSD = bad
		if _, err := store.InsertTurn(context.Background(), db, turn); err == nil {
			t.Errorf("InsertTurn accepted cost_usd = %v", bad)
		}
	}
}
