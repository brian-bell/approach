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

// ResolveOwnerID resolves a sender to the canonical principal behind it
// (§6): the owner_id shared by ALL of the owner's surfaces, which
// cross-surface approval matches on — an owner_id match, never a channel
// match (§4.4). ok is false when the sender has no principal: unmapped
// (untrusted, deny-by-default) or enrolled below owner — known people
// cannot satisfy an approval. A query failure returns err with ok false,
// so a broken store fails closed instead of reading as "not the owner".
func ResolveOwnerID(ctx context.Context, db *sql.DB, channel, nativeID string) (ownerID string, ok bool, err error) {
	// The 0002 CHECK makes owner rows with a NULL owner_id (and non-owner
	// rows with one) unrepresentable, so scanning owner_id alone decides.
	err = db.QueryRowContext(ctx,
		`SELECT owner_id FROM identities
		 WHERE channel = ? AND native_id = ? AND trust = 'owner'`,
		channel, nativeID,
	).Scan(&ownerID)
	if errors.Is(err, sql.ErrNoRows) {
		return "", false, nil
	}
	if err != nil {
		return "", false, fmt.Errorf("store: resolve owner_id for %s/%s: %w", channel, nativeID, err)
	}
	return ownerID, true, nil
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
