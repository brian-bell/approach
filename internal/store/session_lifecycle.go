package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
)

// ErrNotCreating reports a guarded session transition that found the
// row not in 'creating' — already activated, failed, rotated, or never
// inserted. Callers must treat it as a real signal, not noise: an
// activation racing a deadline-fail (or vice versa) must lose loudly,
// never resurrect or clobber the winner's state.
var ErrNotCreating = errors.New("session is not in creating state")

// LiveSession is the router's view of a thread's one live session
// (§4.1): everything the spawn/resume decision needs, read in one
// query. Status distinguishes the two live states — active resumes,
// creating means a first turn is owed (or overdue, per the deadline).
type LiveSession struct {
	ThreadKey          string
	SessionID          string
	Status             string // creating | active — the only live states
	Cwd                string // canonicalized at insert; the spawn dir (§6)
	TrustFloor         string
	CreatedAt          int64
	ActivationDeadline int64 // meaningful while creating (§4.1)
}

// ResolveLiveSession finds the thread's live session. At most one row
// can match — one_live_session is a schema invariant (§6) — so a
// multi-row state is impossible by construction, not by this query's
// LIMIT. Absence is (zero, false, nil): a new thread is normal, not an
// error; a query failure is an error and must never read as "no
// session" (a fail-open would double-pin under one_live_session and
// crash the insert instead).
func ResolveLiveSession(ctx context.Context, db *sql.DB, threadKey string) (LiveSession, bool, error) {
	var s LiveSession
	var deadline sql.NullInt64
	err := db.QueryRowContext(ctx,
		`SELECT thread_key, session_id, status, cwd, trust_floor, created_at, activation_deadline
		 FROM sessions
		 WHERE thread_key = ? AND status IN ('creating', 'active')`,
		threadKey,
	).Scan(&s.ThreadKey, &s.SessionID, &s.Status, &s.Cwd, &s.TrustFloor, &s.CreatedAt, &deadline)
	if errors.Is(err, sql.ErrNoRows) {
		return LiveSession{}, false, nil
	}
	if err != nil {
		return LiveSession{}, false, fmt.Errorf("store: resolve live session %s: %w", threadKey, err)
	}
	s.ActivationDeadline = deadline.Int64
	return s, true, nil
}

// ActivateSession is the §4.1 creating → active transition, taken when
// the pinned session's first engine turn succeeds. Guarded: any state
// but creating returns ErrNotCreating — activating a row a deadline
// sweep already failed would resurrect a session whose retry may
// already be live under one_live_session.
func ActivateSession(ctx context.Context, db *sql.DB, sessionID string) error {
	return transitionCreating(ctx, db, sessionID, "active")
}

// FailSession is the §4.1 expiry transition: a creating row past its
// activation_deadline is marked failed so the thread can retry fresh.
// Guarded like ActivateSession — a stale deadline check must never
// clobber a session that activated in the meantime.
func FailSession(ctx context.Context, db *sql.DB, sessionID string) error {
	return transitionCreating(ctx, db, sessionID, "failed")
}

// transitionCreating moves one session out of 'creating', enforcing
// that creating is the only state these transitions leave. RowsAffected
// is the guard's verdict: 0 means the row was not creating (or does not
// exist) and the caller lost a race it must hear about.
func transitionCreating(ctx context.Context, db *sql.DB, sessionID, to string) error {
	res, err := db.ExecContext(ctx,
		`UPDATE sessions SET status = ? WHERE session_id = ? AND status = 'creating'`,
		to, sessionID,
	)
	if err != nil {
		return fmt.Errorf("store: session %s → %s: %w", sessionID, to, err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return fmt.Errorf("store: session %s → %s: %w", sessionID, to, err)
	}
	if n == 0 {
		return fmt.Errorf("store: session %s → %s: %w", sessionID, to, ErrNotCreating)
	}
	return nil
}
