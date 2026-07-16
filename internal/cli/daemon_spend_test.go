package cli

import (
	"context"
	"database/sql"
	"path/filepath"
	"testing"
	"time"

	"github.com/brian-bell/approach/internal/store"
)

// openSpendStore opens a fresh store seeded with one session turns can
// bind to.
func openSpendStore(t *testing.T) (*sql.DB, string) {
	t.Helper()
	db, err := store.Open(filepath.Join(t.TempDir(), "state", "approach.db"))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() {
		if err := db.Close(); err != nil {
			t.Errorf("close: %v", err)
		}
	})
	s := store.Session{
		ThreadKey:          "discord:dm:spend",
		SessionID:          "22222222-2222-4222-8222-222222222222",
		Cwd:                t.TempDir(),
		TrustFloor:         "owner",
		CreatedAt:          1,
		ActivationDeadline: 2,
	}
	if _, err := store.InsertSession(context.Background(), db, s); err != nil {
		t.Fatalf("InsertSession: %v", err)
	}
	return db, s.SessionID
}

func insertSpendTurn(t *testing.T, db *sql.DB, sessionID string, ts int64, cost float64, known bool) {
	t.Helper()
	turn := store.Turn{
		SessionID: sessionID, TS: ts, ToolCalls: 0, DurationMS: 1,
		Outcome: "ok", UsageKnown: known, CostUSD: cost,
	}
	if !known {
		turn.Outcome = "timeout"
		turn.CostUSD = 0
	}
	if _, err := store.InsertTurn(context.Background(), db, turn); err != nil {
		t.Fatalf("InsertTurn: %v", err)
	}
}

// TestSpendStatusFields: the status verb carries the §7 daily-spend
// checklist — today's known burn (since LOCAL midnight), the turn
// count, and the unknown-usage count that marks the total as a lower
// bound. This is the C11 feed the heartbeat checklist reads once M3
// lands; until then status is where a human sees the burn.
func TestSpendStatusFields(t *testing.T) {
	db, sessionID := openSpendStore(t)
	now := time.Date(2026, 7, 16, 15, 0, 0, 0, time.Local)
	midnight := time.Date(2026, 7, 16, 0, 0, 0, 0, time.Local)

	insertSpendTurn(t, db, sessionID, midnight.Unix()-1, 99, true) // yesterday
	insertSpendTurn(t, db, sessionID, midnight.Unix()+3600, 1.25, true)
	insertSpendTurn(t, db, sessionID, now.Unix()-60, 0.50, true)
	insertSpendTurn(t, db, sessionID, now.Unix()-30, 0, false) // killed child

	fields := map[string]any{}
	spendStatus(db, 0, now, fields)
	if got := fields["spend_today_usd"]; got != 1.75 {
		t.Errorf("spend_today_usd = %v, want 1.75 (yesterday excluded)", got)
	}
	if got := fields["spend_today_turns"]; got != int64(3) {
		t.Errorf("spend_today_turns = %v, want 3", got)
	}
	if got := fields["spend_today_unknown_usage"]; got != int64(1) {
		t.Errorf("spend_today_unknown_usage = %v, want 1 — unknown burn stays visible (§7)", got)
	}
	if _, ok := fields["spend_alarm"]; ok {
		t.Error("spend_alarm present with no threshold configured — a disabled alarm must not report ok")
	}
}

// TestSpendStatusAlarm: with a threshold configured the alarm states
// itself — ok under it, over at/above it (§7 anomalous-burn alarm).
func TestSpendStatusAlarm(t *testing.T) {
	db, sessionID := openSpendStore(t)
	now := time.Date(2026, 7, 16, 15, 0, 0, 0, time.Local)
	insertSpendTurn(t, db, sessionID, now.Unix()-60, 30, true)

	fields := map[string]any{}
	spendStatus(db, 50, now, fields)
	if got := fields["spend_alarm"]; got != "ok" {
		t.Errorf("spend_alarm = %v, want ok under a $50 threshold", got)
	}
	if got := fields["spend_alarm_usd"]; got != 50.0 {
		t.Errorf("spend_alarm_usd = %v, want the configured 50", got)
	}

	fields = map[string]any{}
	spendStatus(db, 25, now, fields)
	if got := fields["spend_alarm"]; got != "OVER" {
		t.Errorf("spend_alarm = %v, want OVER at $30 burn against a $25 threshold", got)
	}
}

// TestSpendStatusQueryFailureIsLoud: a broken spend query must not
// vanish from status — the checklist reports the failure instead of
// silently reading as $0 (§7: quiet degradation is the enemy).
func TestSpendStatusQueryFailureIsLoud(t *testing.T) {
	db, _ := openSpendStore(t)
	if _, err := db.Exec(`DROP TABLE turns`); err != nil {
		t.Fatalf("drop turns: %v", err)
	}

	fields := map[string]any{}
	spendStatus(db, 50, time.Now(), fields)
	if _, ok := fields["spend_error"]; !ok {
		t.Error("spend_error missing — a failed spend query must be visible, not a silent $0")
	}
	if _, ok := fields["spend_today_usd"]; ok {
		t.Error("spend_today_usd present despite a failed query — a made-up number is worse than none")
	}
}
