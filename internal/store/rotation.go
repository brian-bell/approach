package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
)

// ErrNotActive reports a guarded rotation that found the old row not
// 'active' — creating, already rotated, failed, or missing. Loud for
// the same reason as ErrNotCreating: a rotation racing another cause
// (/new vs turn cap) must lose visibly, never double-rotate.
var ErrNotActive = errors.New("session is not active")

// RotateSession is THE §6 rotation transaction: old → rotated with a
// rotated_to link, successor born creating — all or nothing. A
// half-rotation would strand the thread with no live session (old
// demoted, successor missing) or two (both live under
// one_live_session, which the schema refuses anyway).
//
// Step order is forced by the schema's immediate constraints:
// demoting the old row FIRST frees one_live_session for the successor
// insert; the rotated_to link is written LAST because its foreign key
// needs the successor row to exist. All three inside one transaction,
// so every intermediate state is invisible.
func RotateSession(ctx context.Context, db *sql.DB, oldSessionID string, successor Session) (canonicalCwdStored string, err error) {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return "", fmt.Errorf("store: rotate session %s: %w", oldSessionID, err)
	}
	// Rollback after Commit is a documented no-op; on any early return
	// it restores the old session's live status.
	defer func() { _ = tx.Rollback() }()

	res, err := tx.ExecContext(ctx,
		`UPDATE sessions SET status = 'rotated' WHERE session_id = ? AND status = 'active'`,
		oldSessionID,
	)
	if err != nil {
		return "", fmt.Errorf("store: rotate session %s: %w", oldSessionID, err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return "", fmt.Errorf("store: rotate session %s: %w", oldSessionID, err)
	}
	if n == 0 {
		return "", fmt.Errorf("store: rotate session %s: %w", oldSessionID, ErrNotActive)
	}

	cwd, err := insertSession(ctx, tx, successor)
	if err != nil {
		return "", fmt.Errorf("store: rotate session %s: successor: %w", oldSessionID, err)
	}

	if _, err := tx.ExecContext(ctx,
		`UPDATE sessions SET rotated_to = ? WHERE session_id = ?`,
		successor.SessionID, oldSessionID,
	); err != nil {
		return "", fmt.Errorf("store: rotate session %s: link successor: %w", oldSessionID, err)
	}

	if err := tx.Commit(); err != nil {
		return "", fmt.Errorf("store: rotate session %s: %w", oldSessionID, err)
	}
	return cwd, nil
}

// TouchSession records one completed turn (§6): last_seen moves to
// now, turns increments from its NULL-as-zero start. These two columns
// are the idle-TTL and turn-cap rotation inputs — losing a touch
// silently would let a session outlive its caps, so an unknown id is
// an error, not a no-op.
func TouchSession(ctx context.Context, db *sql.DB, sessionID string, now int64) error {
	res, err := db.ExecContext(ctx,
		`UPDATE sessions SET last_seen = ?, turns = COALESCE(turns, 0) + 1 WHERE session_id = ?`,
		now, sessionID,
	)
	if err != nil {
		return fmt.Errorf("store: touch session %s: %w", sessionID, err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return fmt.Errorf("store: touch session %s: %w", sessionID, err)
	}
	if n == 0 {
		return fmt.Errorf("store: touch session %s: no such session", sessionID)
	}
	return nil
}
