package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
)

// MarkSessionTainted sets the session's sticky tainted flag (§6, §7):
// call it the moment externally-authored content is ingested. Marking
// is idempotent, there is no unmark — the flag clears only when C4
// rotation retires the whole row — and an unknown session is an ERROR:
// a taint that lands on no session would silently skip the mutating-
// verb lattice shift, a policy bypass rather than a no-op.
func MarkSessionTainted(ctx context.Context, db *sql.DB, sessionID string) error {
	res, err := db.ExecContext(ctx, `UPDATE sessions SET tainted = 1 WHERE session_id = ?`, sessionID)
	if err != nil {
		return fmt.Errorf("store: mark session %s tainted: %w", sessionID, err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return fmt.Errorf("store: mark session %s tainted: %w", sessionID, err)
	}
	if n == 0 {
		return fmt.Errorf("store: mark session %s tainted: no such session — the taint would be lost", sessionID)
	}
	return nil
}

// SessionTainted reads the session's tainted flag — the one column
// mutating verbs read right (§6). An unknown session is an ERROR, never
// false: absent must not read as clean.
func SessionTainted(ctx context.Context, db *sql.DB, sessionID string) (bool, error) {
	var tainted bool
	err := db.QueryRowContext(ctx, `SELECT tainted FROM sessions WHERE session_id = ?`, sessionID).Scan(&tainted)
	if errors.Is(err, sql.ErrNoRows) {
		return false, fmt.Errorf("store: session %s tainted: no such session", sessionID)
	}
	if err != nil {
		return false, fmt.Errorf("store: session %s tainted: %w", sessionID, err)
	}
	return tainted, nil
}
