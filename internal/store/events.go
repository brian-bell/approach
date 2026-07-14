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
	// Correlation links a follow-up event the DAEMON mints (an
	// approval round-trip, §4.4; retry forensics, §4.6) to its origin
	// event's dedup_key — a durable identity, unlike row ids. "" means
	// no link (stored NULL). Deliberately outside the payload-
	// agreement validation: adapters never stamp it, and a link
	// arriving in inbound content must never be trusted as one.
	Correlation string
}

// InsertEvent is the single write-on-receipt chokepoint (§4.1): persist
// the event row before ANY processing. Gateway channels don't redeliver,
// so receipt is the last moment durability is free — a row that fails to
// land here is an error the adapter must surface, never a silent drop.
// Validation happens before the db is touched and fails loud: a blank
// identity or key would either violate schema anyway or, worse, insert a
// row the queue can never claim correctly.
//
// A duplicate dedup_key is a reported no-op, never an error (§6: dup
// insert = no-op): redelivery must collapse to one turn (§4.1), and the
// first write wins — the original row, including lifecycle state the
// queue has already advanced, is untouched. inserted=false is the
// caller's signal not to enqueue; a caller that drops it still cannot
// double-process, because only one row exists to claim. The conflict
// target is exactly dedup_key, so every OTHER constraint (CHECK, NOT
// NULL) still fails loud.
//
// The returned id is the new row's id — receipt order itself (§4.1),
// which the router's per-thread FIFO keys on. It is meaningful only
// when inserted is true: a collapsed duplicate returns 0, never the
// original row's id, so a buggy caller cannot re-enqueue an event the
// queue already carries.
func InsertEvent(ctx context.Context, db *sql.DB, ev Event) (id int64, inserted bool, err error) {
	if err := ev.validate(); err != nil {
		return 0, false, fmt.Errorf("store: insert event: %w", err)
	}
	// "" means "no link" and must land as NULL — an empty-string
	// correlation would group every unlinked event into one bogus
	// round-trip.
	var correlation any
	if ev.Correlation != "" {
		correlation = ev.Correlation
	}
	res, err := db.ExecContext(ctx,
		`INSERT INTO events (dedup_key, thread_key, kind, trust, payload, received, correlation)
		 VALUES (?, ?, ?, ?, ?, ?, ?)
		 ON CONFLICT(dedup_key) DO NOTHING`,
		ev.DedupKey, ev.ThreadKey, ev.Kind, ev.Trust, ev.Payload, ev.Received, correlation,
	)
	if err != nil {
		return 0, false, fmt.Errorf("store: insert event %s: %w", ev.DedupKey, err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return 0, false, fmt.Errorf("store: insert event %s: %w", ev.DedupKey, err)
	}
	if n == 0 {
		return 0, false, nil
	}
	id, err = res.LastInsertId()
	if err != nil {
		return 0, false, fmt.Errorf("store: insert event %s: %w", ev.DedupKey, err)
	}
	return id, true, nil
}

// CorrelatedEvents is the round-trip/forensics lookup (§6): every
// event stamped with this origin link, in id (receipt) order. An
// unknown link answers empty — absence of follow-ups is a normal
// state, not an error. Unindexed on purpose: correlation reads are
// per-approval or forensic, never the hot path; if M2's approval flow
// proves otherwise an index is one migration away.
func CorrelatedEvents(ctx context.Context, db *sql.DB, correlation string) ([]QueuedEvent, error) {
	if correlation == "" {
		return nil, fmt.Errorf("store: correlated events: empty link — NULL rows are unlinked, not a group (§6)")
	}
	rows, err := db.QueryContext(ctx,
		`SELECT id, dedup_key, thread_key, kind, trust, payload, status, received, correlation
		 FROM events WHERE correlation = ? ORDER BY id`,
		correlation,
	)
	if err != nil {
		return nil, fmt.Errorf("store: correlated events %s: %w", correlation, err)
	}
	// Read-only query: a Close error after full iteration has nothing
	// to add — rows.Err() below already surfaces any read failure.
	defer func() { _ = rows.Close() }()

	var out []QueuedEvent
	for rows.Next() {
		ev, err := scanQueuedEvent(rows)
		if err != nil {
			return nil, fmt.Errorf("store: correlated events %s: %w", correlation, err)
		}
		out = append(out, ev)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("store: correlated events %s: %w", correlation, err)
	}
	return out, nil
}

// ParkEvent durably parks a queue row as interrupted (§4.6): the turn
// failed in a way whose side effects are unknowable (handler panic,
// crash-adjacent), so it must neither auto-retry nor silently vanish
// from the live queue — interrupted is out-of-band, human-visible
// state. The guard clause only advances rows still owed a turn: a
// handler that already moved the row (completed, dead) wins, and
// re-parking finished history would resurrect it as visible failure.
func ParkEvent(ctx context.Context, db *sql.DB, id int64, now int64) error {
	// parks increments per park: each park is a distinct episode, and
	// the §4.6 notice key (interrupted:<dedup_key>:<parks>) must never
	// let an earlier episode's notice suppress a later one — seconds
	// collide, a monotonic counter cannot.
	_, err := db.ExecContext(ctx,
		`UPDATE events SET status = 'interrupted', updated = ?, parks = parks + 1
		 WHERE id = ? AND status IN ('received', 'processing')`,
		now, id,
	)
	if err != nil {
		return fmt.Errorf("store: park event %d: %w", id, err)
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
	case ev.Correlation == ev.DedupKey:
		return fmt.Errorf("correlation equals dedup_key %q — an event cannot be its own origin (§6)", ev.DedupKey)
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
