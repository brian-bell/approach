package store

import (
	"context"
	"database/sql"
	"fmt"
)

// ToolAttempt is one row of the §6 per-side-effecting-call journal:
// what the PreToolUse gate stamps before the call runs. Lifecycle
// state (state, ended_at) is deliberately absent — the schema owns
// 'started', and the journal records what began, never an outcome it
// cannot know yet.
type ToolAttempt struct {
	SessionID      string // the turn's session (§6) — must exist, FK-enforced
	EventID        int64  // the queued event whose turn made the call; 0 = none (stored NULL)
	Tool           string // canonical tool name
	ArgsDigest     string // digest of normalized args — display strings are not identity (§4.4)
	IdempotencyKey string // "" = none (stored NULL) — absence is load-bearing: no key, no §4.6 retry.
	// A verb that stamps a key PROMISES the key is deterministic
	// w.r.t. the turn's inputs (and honored by the downstream
	// service): a retried turn re-derives the SAME key, so the repeat
	// dedupes instead of doubling. A randomly minted key would make
	// the §4.6 retry exception a lie — enforcement lives with the
	// daemon verbs that stamp keys (M2+), not this journal.
	StartedAt int64 // unix seconds at PreToolUse
	// State and ended_at are read-side fields, populated by queries.
	State   string
	EndedAt int64
}

// InsertToolAttempt is the PreToolUse write (§4.1): journal the call
// BEFORE it runs, so a crash that loses the PostToolUse still leaves
// proof the call started — the exact evidence §4.6 recovery reasons
// from. Validation fails loud before the db is touched: a row without
// session, tool, or digest is provenance recovery cannot resolve, and
// inserting it would let an ambiguous side effect hide.
//
// Existence is not provenance: when the attempt binds an event, the
// session and event must share a thread_key — §4.6 reads attempts PER
// EVENT, so a side effect filed under another thread's turn makes the
// real turn look side-effect-free (unsafe auto-retry) and interrupts
// an innocent one. The event must also still be 'processing':
// PreToolUse only fires during a live turn, so a write landing after
// recovery already requeued or parked the event is a straggler from a
// killed child — accepting it would re-open the race the requeue's
// atomic journal re-check (RequeueEventForRetry) closes from its
// side. Both checks ride the INSERT statement itself; a separate
// pre-check could pass just before a racing write changes what it
// proved.
func InsertToolAttempt(ctx context.Context, db *sql.DB, a ToolAttempt) (id int64, err error) {
	if err := a.validate(); err != nil {
		return 0, fmt.Errorf("store: insert tool attempt: %w", err)
	}
	// 0 / "" mean "absent" and must land as NULL: event_id is a
	// foreign key, and an empty-string idempotency_key would read as
	// a (vacuous) retry authorization instead of its absence.
	var eventID any
	if a.EventID != 0 {
		eventID = a.EventID
	}
	var idemKey any
	if a.IdempotencyKey != "" {
		idemKey = a.IdempotencyKey
	}
	res, err := db.ExecContext(ctx,
		`INSERT INTO tool_attempts (session_id, event_id, tool, args_digest, idempotency_key, started_at)
		 SELECT ?, ?, ?, ?, ?, ?
		 WHERE ? IS NULL
		    OR EXISTS (SELECT 1 FROM sessions s JOIN events e ON e.thread_key = s.thread_key
		               WHERE s.session_id = ? AND e.id = ? AND e.status = 'processing')`,
		a.SessionID, eventID, a.Tool, a.ArgsDigest, idemKey, a.StartedAt,
		eventID, a.SessionID, eventID,
	)
	if err != nil {
		return 0, fmt.Errorf("store: insert tool attempt %s: %w", a.Tool, err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return 0, fmt.Errorf("store: insert tool attempt %s: %w", a.Tool, err)
	}
	if n == 0 {
		return 0, fmt.Errorf("store: insert tool attempt %s: session %s and event %d do not resolve to one live turn — thread mismatch or the event is no longer mid-turn (§4.6)", a.Tool, a.SessionID, a.EventID)
	}
	id, err = res.LastInsertId()
	if err != nil {
		return 0, fmt.Errorf("store: insert tool attempt %s: %w", a.Tool, err)
	}
	return id, nil
}

// CompleteToolAttempt is the PostToolUse flip (§4.1): started →
// done|failed, end stamped. Guarded to 'started' rows only — a
// completed attempt never flips again, because §4.6 judges retries on
// this history and a 'failed' rewritten to 'done' (or the reverse)
// would judge on falsified evidence. Zero rows affected is a caller
// bug (missing id or double completion), never a quiet success.
func CompleteToolAttempt(ctx context.Context, db *sql.DB, id int64, outcome string, endedAt int64) error {
	// The enum is closed here as well as in schema: a bad outcome must
	// name itself, not surface as an opaque CHECK violation.
	if outcome != "done" && outcome != "failed" {
		return fmt.Errorf("store: complete tool attempt %d: outcome %q is not done|failed — the enum is closed (§6)", id, outcome)
	}
	res, err := db.ExecContext(ctx,
		`UPDATE tool_attempts SET state = ?, ended_at = ? WHERE id = ? AND state = 'started'`,
		outcome, endedAt, id,
	)
	if err != nil {
		return fmt.Errorf("store: complete tool attempt %d: %w", id, err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return fmt.Errorf("store: complete tool attempt %d: %w", id, err)
	}
	if n == 0 {
		return fmt.Errorf("store: complete tool attempt %d: no started row — missing id or already completed, and journalled history must stand", id)
	}
	return nil
}

// AttemptsForEvent answers the §4.6 retry question for one turn:
// every journalled attempt for the event, in id (call) order, carrying
// the state and idempotency_key the retry logic reasons from. Empty
// means provably side-effect-free — the only state that authorizes a
// plain auto-retry.
func AttemptsForEvent(ctx context.Context, db *sql.DB, eventID int64) ([]ToolAttempt, error) {
	rows, err := db.QueryContext(ctx,
		`SELECT session_id, event_id, tool, args_digest, idempotency_key, state, started_at, ended_at
		 FROM tool_attempts
		 WHERE event_id = ?
		 ORDER BY id`,
		eventID,
	)
	if err != nil {
		return nil, fmt.Errorf("store: attempts for event %d: %w", eventID, err)
	}
	// Read-only query: a Close error after full iteration has nothing
	// to add — rows.Err() below already surfaces any read failure.
	defer func() { _ = rows.Close() }()

	var out []ToolAttempt
	for rows.Next() {
		var a ToolAttempt
		var evID, endedAt sql.NullInt64
		var idemKey sql.NullString
		if err := rows.Scan(&a.SessionID, &evID, &a.Tool, &a.ArgsDigest, &idemKey, &a.State, &a.StartedAt, &endedAt); err != nil {
			return nil, fmt.Errorf("store: attempts for event %d: %w", eventID, err)
		}
		a.EventID = evID.Int64
		a.IdempotencyKey = idemKey.String
		a.EndedAt = endedAt.Int64
		out = append(out, a)
	}
	// A half-read journal treated as whole would hide an attempt from
	// the retry judgment — exactly the ambiguity the journal exists to
	// remove. Iteration errors are scan failures, not short results.
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("store: attempts for event %d: %w", eventID, err)
	}
	return out, nil
}

// validate refuses an attempt the §4.6 recovery could not reason from.
func (a ToolAttempt) validate() error {
	switch {
	case a.SessionID == "":
		return fmt.Errorf("empty session_id — the attempt would have no provenance (§6)")
	case a.Tool == "":
		return fmt.Errorf("empty tool")
	case a.ArgsDigest == "":
		return fmt.Errorf("empty args_digest — the call would have no identity to match against (§4.4, §6)")
	case a.StartedAt <= 0:
		return fmt.Errorf("started_at = %d, want a positive unix timestamp", a.StartedAt)
	case a.EventID < 0:
		return fmt.Errorf("event_id = %d, want 0 (none) or a positive events row id", a.EventID)
	}
	return nil
}
