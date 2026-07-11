package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
)

// Identity is one hand-enrolled row for the §6 identities table. It
// mirrors config.Identity without importing it — the store stays a
// schema-and-SQL package, and the caller (the daemon) does the mapping.
type Identity struct {
	Channel  string
	NativeID string
	Trust    string // owner | known — untrusted is the absence of a row
	OwnerID  string // canonical principal; exactly owner rows carry it (§4.4)
	Label    string
}

// SeedIdentities syncs the identities table to ids, in one transaction.
// A full sync, not an upsert: approach.toml is the source of truth, so a
// row dropped from the config is revoked here at the next startup —
// upsert-only seeding would leave a removed person enrolled forever (§6).
func SeedIdentities(ctx context.Context, db *sql.DB, ids []Identity) (err error) {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("store: seed identities: %w", err)
	}
	defer func() {
		if err != nil {
			if rerr := tx.Rollback(); rerr != nil && !errors.Is(rerr, sql.ErrTxDone) {
				err = errors.Join(err, fmt.Errorf("store: seed identities: rollback: %w", rerr))
			}
		}
	}()
	if _, err := tx.ExecContext(ctx, "DELETE FROM identities"); err != nil {
		return fmt.Errorf("store: seed identities: clear: %w", err)
	}
	for _, id := range ids {
		// Empty owner_id and label become NULL so the schema's
		// owner-rows-carry-owner_id CHECK sees the same shape config
		// validation approved.
		if _, err := tx.ExecContext(ctx,
			`INSERT INTO identities (channel, native_id, trust, owner_id, label)
			 VALUES (?, ?, ?, NULLIF(?, ''), NULLIF(?, ''))`,
			id.Channel, id.NativeID, id.Trust, id.OwnerID, id.Label,
		); err != nil {
			return fmt.Errorf("store: seed identity %s/%s: %w", id.Channel, id.NativeID, err)
		}
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("store: seed identities: commit: %w", err)
	}
	return nil
}
