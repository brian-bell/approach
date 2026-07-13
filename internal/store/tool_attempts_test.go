package store_test

import (
	"context"
	"database/sql"
	"path/filepath"
	"testing"

	"github.com/brian-bell/approach/internal/store"
)

// seedSession inserts a live session the journal rows can bind to —
// tool_attempts.session_id is a foreign key, and the tests exercise it
// against real rows, not disabled constraints.
func seedSession(t *testing.T, db *sql.DB) string {
	t.Helper()
	s := store.Session{
		ThreadKey:          "discord:dm:123",
		SessionID:          "11111111-1111-4111-8111-111111111111",
		Cwd:                t.TempDir(),
		TrustFloor:         "owner",
		CreatedAt:          1700000000,
		ActivationDeadline: 1700000600,
	}
	if _, err := store.InsertSession(context.Background(), db, s); err != nil {
		t.Fatalf("InsertSession: %v", err)
	}
	return s.SessionID
}

func testAttempt(sessionID string) store.ToolAttempt {
	return store.ToolAttempt{
		SessionID:  sessionID,
		Tool:       "mcp__discord__send",
		ArgsDigest: "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		StartedAt:  1700000100,
	}
}

// TestToolAttemptsTableAndEventIndexExist: the journal is the §4.6
// recovery substrate — it proves what STARTED, not aggregate counts a
// crash can lose — and the per-event index carries the retry
// question. Both must exist on a fresh store.
func TestToolAttemptsTableAndEventIndexExist(t *testing.T) {
	db := mustOpen(t, filepath.Join(t.TempDir(), "state", "approach.db"))

	for _, obj := range []struct{ typ, name string }{
		{"table", "tool_attempts"},
		{"index", "ta_by_event"},
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

// TestInsertToolAttemptJournalsBeforeTheCall: the PreToolUse write
// (§4.1) — the row lands in 'started' with no completion state, and
// an absent idempotency_key stays NULL (its absence is load-bearing:
// no key means an ambiguous attempt is NEVER auto-retried, §4.6).
func TestInsertToolAttemptJournalsBeforeTheCall(t *testing.T) {
	db := mustOpen(t, filepath.Join(t.TempDir(), "state", "approach.db"))
	sessionID := seedSession(t, db)

	a := testAttempt(sessionID)
	id, err := store.InsertToolAttempt(context.Background(), db, a)
	if err != nil {
		t.Fatalf("InsertToolAttempt: %v", err)
	}
	if id <= 0 {
		t.Errorf("id = %d, want a positive row id", id)
	}

	var (
		gotSession, tool, digest, state string
		eventID                         sql.NullInt64
		idemKey                         sql.NullString
		startedAt                       int64
		endedAt                         sql.NullInt64
	)
	if err := db.QueryRow(
		`SELECT session_id, event_id, tool, args_digest, idempotency_key, state, started_at, ended_at
		 FROM tool_attempts WHERE id = ?`, id,
	).Scan(&gotSession, &eventID, &tool, &digest, &idemKey, &state, &startedAt, &endedAt); err != nil {
		t.Fatalf("read back attempt: %v", err)
	}
	if gotSession != a.SessionID || tool != a.Tool || digest != a.ArgsDigest || startedAt != a.StartedAt {
		t.Errorf("attempt did not round-trip: got (%q, %q, %q, %d)", gotSession, tool, digest, startedAt)
	}
	if eventID.Valid {
		t.Errorf("event_id = %d, want NULL for an unbound attempt", eventID.Int64)
	}
	if idemKey.Valid {
		t.Errorf("idempotency_key = %q, want NULL — an invented key would authorize a §4.6 retry the verb never promised", idemKey.String)
	}
	if state != "started" {
		t.Errorf("state = %q, want 'started' — the journal records what began, never an outcome it can't know yet", state)
	}
	if endedAt.Valid {
		t.Errorf("ended_at = %v, want NULL before completion", endedAt)
	}
}

// TestInsertToolAttemptValidation: a row the §4.6 recovery could not
// reason from is refused before the db is touched.
func TestInsertToolAttemptValidation(t *testing.T) {
	db := mustOpen(t, filepath.Join(t.TempDir(), "state", "approach.db"))
	sessionID := seedSession(t, db)

	cases := []struct {
		name   string
		mutate func(*store.ToolAttempt)
	}{
		{"empty session_id", func(a *store.ToolAttempt) { a.SessionID = "" }},
		{"empty tool", func(a *store.ToolAttempt) { a.Tool = "" }},
		{"empty args_digest", func(a *store.ToolAttempt) { a.ArgsDigest = "" }},
		{"zero started_at", func(a *store.ToolAttempt) { a.StartedAt = 0 }},
		{"negative event_id", func(a *store.ToolAttempt) { a.EventID = -1 }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			a := testAttempt(sessionID)
			tc.mutate(&a)
			if _, err := store.InsertToolAttempt(context.Background(), db, a); err == nil {
				t.Fatal("InsertToolAttempt accepted a row recovery could not reason from, want error")
			}
		})
	}

	var n int
	if err := db.QueryRow(`SELECT count(*) FROM tool_attempts`).Scan(&n); err != nil {
		t.Fatalf("count attempts: %v", err)
	}
	if n != 0 {
		t.Errorf("found %d rows after refused inserts, want 0", n)
	}
}

// TestInsertToolAttemptForeignKeys: an attempt claiming a session the
// daemon never created, or a turn that never entered the queue, is a
// bug the schema refuses (foreign_keys=ON) — the journal must never
// carry provenance recovery can't resolve. EventID 0 means "no queued
// event" and stores NULL.
func TestInsertToolAttemptForeignKeys(t *testing.T) {
	db := mustOpen(t, filepath.Join(t.TempDir(), "state", "approach.db"))
	sessionID := seedSession(t, db)
	ctx := context.Background()

	unknownSession := testAttempt("99999999-9999-4999-8999-999999999999")
	if _, err := store.InsertToolAttempt(ctx, db, unknownSession); err == nil {
		t.Error("InsertToolAttempt accepted an unknown session_id, want a foreign-key refusal")
	}

	dangling := testAttempt(sessionID)
	dangling.EventID = 12345
	if _, err := store.InsertToolAttempt(ctx, db, dangling); err == nil {
		t.Error("InsertToolAttempt accepted a dangling event_id, want a foreign-key refusal")
	}

	evID, _, err := store.InsertEvent(ctx, db, testEvent())
	if err != nil {
		t.Fatalf("InsertEvent: %v", err)
	}
	if err := store.MarkEventProcessing(ctx, db, evID, 1700000050); err != nil {
		t.Fatalf("MarkEventProcessing: %v", err)
	}
	bound := testAttempt(sessionID)
	bound.EventID = evID
	bound.IdempotencyKey = "send:msg:42"
	id, err := store.InsertToolAttempt(ctx, db, bound)
	if err != nil {
		t.Fatalf("InsertToolAttempt with real event: %v", err)
	}
	var gotEvent int64
	var gotKey string
	if err := db.QueryRow(
		`SELECT event_id, idempotency_key FROM tool_attempts WHERE id = ?`, id,
	).Scan(&gotEvent, &gotKey); err != nil {
		t.Fatalf("read back bound attempt: %v", err)
	}
	if gotEvent != evID || gotKey != bound.IdempotencyKey {
		t.Errorf("(event_id, idempotency_key) = (%d, %q), want (%d, %q)", gotEvent, gotKey, evID, bound.IdempotencyKey)
	}
}

// TestInsertToolAttemptRefusesEventNotMidTurn: a bound event must
// still be 'processing' — PreToolUse only fires during a live turn, so
// a journal write landing after recovery already requeued or parked
// the event is a straggler from a killed child, and accepting it would
// re-open the §4.6 race the requeue's atomic re-check just closed.
func TestInsertToolAttemptRefusesEventNotMidTurn(t *testing.T) {
	db := mustOpen(t, filepath.Join(t.TempDir(), "state", "approach.db"))
	sessionID := seedSession(t, db)
	ctx := context.Background()

	evID, _, err := store.InsertEvent(ctx, db, testEvent())
	if err != nil {
		t.Fatalf("InsertEvent: %v", err)
	}
	// Deliberately NOT stamped 'processing'.
	a := testAttempt(sessionID)
	a.EventID = evID
	if _, err := store.InsertToolAttempt(ctx, db, a); err == nil {
		t.Error("InsertToolAttempt accepted a bound event outside 'processing', want error — no turn is running for it")
	}
	var n int
	if err := db.QueryRow(`SELECT count(*) FROM tool_attempts`).Scan(&n); err != nil {
		t.Fatalf("count attempts: %v", err)
	}
	if n != 0 {
		t.Errorf("found %d rows, want 0", n)
	}
}

// TestInsertToolAttemptRefusesCrossThreadBinding: existence is not
// provenance — a session and an event that both exist but belong to
// different threads must not combine into one journal row, because
// §4.6 recovery reads attempts PER EVENT: a side effect filed under
// the wrong turn makes the real turn look side-effect-free (unsafe
// auto-retry) and interrupts an innocent one.
func TestInsertToolAttemptRefusesCrossThreadBinding(t *testing.T) {
	db := mustOpen(t, filepath.Join(t.TempDir(), "state", "approach.db"))
	sessionID := seedSession(t, db) // thread discord:dm:123
	ctx := context.Background()

	foreign := testEvent()
	foreign.DedupKey = "discord:msg:foreign"
	foreign.ThreadKey = "discord:dm:456"
	foreign.Payload = `{"dedup_key":"discord:msg:foreign","thread_key":"discord:dm:456","kind":"message","trust":"owner"}`
	foreignID, _, err := store.InsertEvent(ctx, db, foreign)
	if err != nil {
		t.Fatalf("InsertEvent: %v", err)
	}

	a := testAttempt(sessionID)
	a.EventID = foreignID
	if _, err := store.InsertToolAttempt(ctx, db, a); err == nil {
		t.Error("InsertToolAttempt bound a session to another thread's event, want error — provenance must resolve to ONE turn")
	}
	var n int
	if err := db.QueryRow(`SELECT count(*) FROM tool_attempts`).Scan(&n); err != nil {
		t.Fatalf("count attempts: %v", err)
	}
	if n != 0 {
		t.Errorf("found %d rows, want 0 — the refused binding must not land", n)
	}
}

// TestCompleteToolAttempt: the PostToolUse flip (§4.1) — started →
// done|failed with the end stamped; each outcome is a legal flip.
func TestCompleteToolAttempt(t *testing.T) {
	db := mustOpen(t, filepath.Join(t.TempDir(), "state", "approach.db"))
	sessionID := seedSession(t, db)
	ctx := context.Background()

	for _, outcome := range []string{"done", "failed"} {
		t.Run(outcome, func(t *testing.T) {
			id, err := store.InsertToolAttempt(ctx, db, testAttempt(sessionID))
			if err != nil {
				t.Fatalf("InsertToolAttempt: %v", err)
			}
			if err := store.CompleteToolAttempt(ctx, db, id, outcome, 1700000200); err != nil {
				t.Fatalf("CompleteToolAttempt(%s): %v", outcome, err)
			}
			var state string
			var endedAt sql.NullInt64
			if err := db.QueryRow(
				`SELECT state, ended_at FROM tool_attempts WHERE id = ?`, id,
			).Scan(&state, &endedAt); err != nil {
				t.Fatalf("read back: %v", err)
			}
			if state != outcome {
				t.Errorf("state = %q, want %q", state, outcome)
			}
			if !endedAt.Valid || endedAt.Int64 != 1700000200 {
				t.Errorf("ended_at = %v, want 1700000200", endedAt)
			}
		})
	}
}

// TestCompleteToolAttemptGuards: journalled history must stand — a
// completed attempt never flips again (a 'failed' rewritten to 'done'
// would authorize a §4.6 judgment on falsified evidence), a missing id
// is a caller bug, and the outcome enum is closed before the db.
func TestCompleteToolAttemptGuards(t *testing.T) {
	db := mustOpen(t, filepath.Join(t.TempDir(), "state", "approach.db"))
	sessionID := seedSession(t, db)
	ctx := context.Background()

	id, err := store.InsertToolAttempt(ctx, db, testAttempt(sessionID))
	if err != nil {
		t.Fatalf("InsertToolAttempt: %v", err)
	}
	if err := store.CompleteToolAttempt(ctx, db, id, "bogus", 1700000200); err == nil {
		t.Error("CompleteToolAttempt accepted an outcome outside the closed enum, want error")
	}
	if err := store.CompleteToolAttempt(ctx, db, id, "failed", 1700000200); err != nil {
		t.Fatalf("CompleteToolAttempt: %v", err)
	}
	if err := store.CompleteToolAttempt(ctx, db, id, "done", 1700000300); err == nil {
		t.Error("CompleteToolAttempt rewrote a completed attempt, want error — history must stand")
	}
	var state string
	if err := db.QueryRow(`SELECT state FROM tool_attempts WHERE id = ?`, id).Scan(&state); err != nil {
		t.Fatalf("read back: %v", err)
	}
	if state != "failed" {
		t.Errorf("state = %q, want the original 'failed'", state)
	}
	if err := store.CompleteToolAttempt(ctx, db, id+999, "done", 1700000200); err == nil {
		t.Error("CompleteToolAttempt accepted an unknown id, want error")
	}
}

// TestAttemptsForEvent: the §4.6 retry question, answered per turn —
// every journalled attempt for the event in id order, carrying the
// state and idempotency_key the retry logic reasons from. An event
// with no attempts answers empty (provably side-effect-free → safe to
// retry); rows for OTHER events never leak in.
func TestAttemptsForEvent(t *testing.T) {
	db := mustOpen(t, filepath.Join(t.TempDir(), "state", "approach.db"))
	sessionID := seedSession(t, db)
	ctx := context.Background()

	evID, _, err := store.InsertEvent(ctx, db, testEvent())
	if err != nil {
		t.Fatalf("InsertEvent: %v", err)
	}
	if err := store.MarkEventProcessing(ctx, db, evID, 1700000050); err != nil {
		t.Fatalf("MarkEventProcessing: %v", err)
	}
	other := testEvent()
	other.DedupKey = "discord:msg:other"
	other.Payload = `{"dedup_key":"discord:msg:other","thread_key":"discord:dm:123","kind":"message","trust":"owner"}`
	otherID, _, err := store.InsertEvent(ctx, db, other)
	if err != nil {
		t.Fatalf("InsertEvent(other): %v", err)
	}
	if err := store.MarkEventProcessing(ctx, db, otherID, 1700000050); err != nil {
		t.Fatalf("MarkEventProcessing(other): %v", err)
	}

	first := testAttempt(sessionID)
	first.EventID = evID
	firstID, err := store.InsertToolAttempt(ctx, db, first)
	if err != nil {
		t.Fatalf("InsertToolAttempt: %v", err)
	}
	if err := store.CompleteToolAttempt(ctx, db, firstID, "done", 1700000200); err != nil {
		t.Fatalf("CompleteToolAttempt: %v", err)
	}
	second := testAttempt(sessionID)
	second.EventID = evID
	second.IdempotencyKey = "send:msg:42"
	if _, err := store.InsertToolAttempt(ctx, db, second); err != nil {
		t.Fatalf("InsertToolAttempt: %v", err)
	}
	leak := testAttempt(sessionID)
	leak.EventID = otherID
	if _, err := store.InsertToolAttempt(ctx, db, leak); err != nil {
		t.Fatalf("InsertToolAttempt(leak): %v", err)
	}

	got, err := store.AttemptsForEvent(ctx, db, evID)
	if err != nil {
		t.Fatalf("AttemptsForEvent: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("got %d attempts, want 2 — other events' rows must never leak in", len(got))
	}
	if got[0].State != "done" || got[0].Tool != first.Tool || got[0].IdempotencyKey != "" {
		t.Errorf("attempt 0 = %+v, want the completed keyless attempt first (id order)", got[0])
	}
	if got[1].State != "started" || got[1].IdempotencyKey != "send:msg:42" {
		t.Errorf("attempt 1 = %+v, want the ambiguous keyed attempt", got[1])
	}

	empty, err := store.AttemptsForEvent(ctx, db, otherID+999)
	if err != nil {
		t.Fatalf("AttemptsForEvent(none): %v", err)
	}
	if len(empty) != 0 {
		t.Errorf("got %d attempts for an eventless id, want 0", len(empty))
	}
}
