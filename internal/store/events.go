package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
)

// Event is one inbound event bound for the §6 durable queue: the fields
// the adapter stamps at receipt. Lifecycle state (status, attempts,
// updated, correlation) is deliberately absent — the schema owns those
// defaults, and receipt records an event, it never processes one.
type Event struct {
	DedupKey  string // per-kind identity contract (§6) — see 0005_events.sql
	ThreadKey string // per-channel contract (§6); the queue claim key (§4.1)
	Kind      string // message | heartbeat | webhook | cron | approval | task
	Trust     string // stamped at ingest (§6); includes daemon-only system levels
	Payload   string // full normalized event JSON
	Received  int64  // unix seconds at receipt — the adapter owns the clock
}

// InsertEvent is the single write-on-receipt chokepoint (§4.1): persist
// the event row before ANY processing. Gateway channels don't redeliver,
// so receipt is the last moment durability is free — a row that fails to
// land here is an error the adapter must surface, never a silent drop.
// Validation happens before the db is touched and fails loud: a blank
// identity or key would either violate schema anyway or, worse, insert a
// row the queue can never claim correctly.
func InsertEvent(ctx context.Context, db *sql.DB, ev Event) error {
	if err := ev.validate(); err != nil {
		return fmt.Errorf("store: insert event: %w", err)
	}
	if _, err := db.ExecContext(ctx,
		`INSERT INTO events (dedup_key, thread_key, kind, trust, payload, received)
		 VALUES (?, ?, ?, ?, ?, ?)`,
		ev.DedupKey, ev.ThreadKey, ev.Kind, ev.Trust, ev.Payload, ev.Received,
	); err != nil {
		return fmt.Errorf("store: insert event %s: %w", ev.DedupKey, err)
	}
	return nil
}

// validate refuses an event the queue could not honestly carry. Enum
// membership (kind, trust) is the schema CHECK's job — one closed list,
// not two drifting ones.
func (ev Event) validate() error {
	switch {
	case ev.DedupKey == "":
		return fmt.Errorf("empty dedup_key — the event would have no identity (§6)")
	case ev.ThreadKey == "":
		return fmt.Errorf("empty thread_key — the event could never be claimed (§4.1)")
	case ev.Kind == "":
		return fmt.Errorf("empty kind")
	case ev.Trust == "":
		return fmt.Errorf("empty trust — ingest must stamp a level, ambiguity is untrusted, not blank (§6)")
	case ev.Received <= 0:
		return fmt.Errorf("received = %d, want a positive unix timestamp", ev.Received)
	}
	return ev.validatePayload()
}

// validatePayload holds the payload column to what replay needs. The
// full normalized-event type is adapter-owned (§6, C1) and isn't
// re-validated field-by-field here — but the four fields mirrored in
// this row's columns must be present and AGREE with them: the queue is
// claimed by the columns and replayed from the payload, so a divergent
// payload (wrong thread, laundered trust) would misroute or re-trust
// the event long after the adapter bug that produced it.
func (ev Event) validatePayload() error {
	var p struct {
		DedupKey  string `json:"dedup_key"`
		ThreadKey string `json:"thread_key"`
		Kind      string `json:"kind"`
		Trust     string `json:"trust"`
	}
	// Unmarshal into a struct rejects non-objects (null decodes but
	// leaves every field blank, failing the agreement checks below) and
	// trailing garbage; unknown fields pass through — the contract has
	// more fields than the store mirrors.
	if err := json.Unmarshal([]byte(ev.Payload), &p); err != nil {
		return fmt.Errorf("payload is not a normalized event JSON object: %w", err)
	}
	for _, f := range []struct{ name, payload, column string }{
		{"dedup_key", p.DedupKey, ev.DedupKey},
		{"thread_key", p.ThreadKey, ev.ThreadKey},
		{"kind", p.Kind, ev.Kind},
		{"trust", p.Trust, ev.Trust},
	} {
		if f.payload != f.column {
			return fmt.Errorf("payload %s %q disagrees with event %s %q", f.name, f.payload, f.name, f.column)
		}
	}
	return nil
}
