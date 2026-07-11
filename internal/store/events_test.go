package store_test

import (
	"context"
	"database/sql"
	"path/filepath"
	"testing"

	"github.com/brian-bell/approach/internal/store"
)

// testEvent returns a valid §6 event, ready to insert: the payload
// carries the normalized-event fields mirrored in the row's columns.
func testEvent() store.Event {
	return store.Event{
		DedupKey:  "discord:msg:9871",
		ThreadKey: "discord:dm:123",
		Kind:      "message",
		Trust:     "owner",
		Payload:   `{"dedup_key":"discord:msg:9871","thread_key":"discord:dm:123","kind":"message","trust":"owner","text":"hi"}`,
		Received:  1700000000,
	}
}

// TestEventsTableAndQueueIndexExist: the events table is THE durable
// queue (§4.1, §6) — written on receipt, before any processing — and
// ev_queue is the partial index the per-thread queue scan rides on. A
// freshly opened store must already carry both: durability cannot
// depend on a later setup step.
func TestEventsTableAndQueueIndexExist(t *testing.T) {
	db := mustOpen(t, filepath.Join(t.TempDir(), "state", "approach.db"))

	for _, obj := range []struct{ typ, name string }{
		{"table", "events"},
		{"index", "ev_queue"},
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

// TestInsertEventPersistsOnReceipt: the write-on-receipt chokepoint
// (§4.1) — the row lands exactly as stamped, with the schema owning the
// lifecycle defaults (status 'received', zero attempts, no updated /
// correlation yet): receipt records, it never processes.
func TestInsertEventPersistsOnReceipt(t *testing.T) {
	db := mustOpen(t, filepath.Join(t.TempDir(), "state", "approach.db"))

	ev := testEvent()
	if err := store.InsertEvent(context.Background(), db, ev); err != nil {
		t.Fatalf("InsertEvent: %v", err)
	}

	var (
		threadKey, kind, trust, payload, status string
		attempts, received                      int64
		updated, correlation                    sql.NullString
	)
	if err := db.QueryRow(
		`SELECT thread_key, kind, trust, payload, status, attempts, received, updated, correlation
		 FROM events WHERE dedup_key = ?`, ev.DedupKey,
	).Scan(&threadKey, &kind, &trust, &payload, &status, &attempts, &received, &updated, &correlation); err != nil {
		t.Fatalf("read back event: %v", err)
	}
	if threadKey != ev.ThreadKey || kind != ev.Kind || trust != ev.Trust || payload != ev.Payload || received != ev.Received {
		t.Errorf("event fields did not round-trip: got (%q, %q, %q, %q, %d)", threadKey, kind, trust, payload, received)
	}
	if status != "received" {
		t.Errorf("status = %q on receipt, want 'received'", status)
	}
	if attempts != 0 {
		t.Errorf("attempts = %d on receipt, want 0", attempts)
	}
	if updated.Valid || correlation.Valid {
		t.Errorf("updated/correlation set on receipt (%v, %v), want NULL", updated, correlation)
	}
}

// TestInsertEventClosedEnums: kind and trust are closed sets (§6) — a
// value outside them is a schema CHECK violation surfacing as an error,
// never a row. Enums are closed; ambiguity fails loud.
func TestInsertEventClosedEnums(t *testing.T) {
	db := mustOpen(t, filepath.Join(t.TempDir(), "state", "approach.db"))

	bad := testEvent()
	bad.Kind = "carrier-pigeon"
	if err := store.InsertEvent(context.Background(), db, bad); err == nil {
		t.Error("kind 'carrier-pigeon' accepted, want CHECK violation")
	}
	bad = testEvent()
	bad.Trust = "root" // not even 'system' spelling drift may pass
	if err := store.InsertEvent(context.Background(), db, bad); err == nil {
		t.Error("trust 'root' accepted, want CHECK violation")
	}
	var n int
	if err := db.QueryRow(`SELECT count(*) FROM events`).Scan(&n); err != nil {
		t.Fatalf("count events: %v", err)
	}
	if n != 0 {
		t.Errorf("%d rows landed from rejected inserts, want 0", n)
	}
}

// TestInsertEventFailsLoudOnInvalidFields: a blank identity, key, or
// stamp is refused before the db is touched (§4.1, §6) — a row with no
// dedup identity or no claim key would be unclaimable/undedupable, a
// silent queue corruption rather than a durability win.
func TestInsertEventFailsLoudOnInvalidFields(t *testing.T) {
	db := mustOpen(t, filepath.Join(t.TempDir(), "state", "approach.db"))

	cases := []struct {
		name   string
		mutate func(*store.Event)
	}{
		{"empty dedup_key", func(ev *store.Event) { ev.DedupKey = "" }},
		{"empty thread_key", func(ev *store.Event) { ev.ThreadKey = "" }},
		{"empty kind", func(ev *store.Event) { ev.Kind = "" }},
		{"empty trust", func(ev *store.Event) { ev.Trust = "" }},
		{"non-JSON payload", func(ev *store.Event) { ev.Payload = "not json" }},
		{"empty payload", func(ev *store.Event) { ev.Payload = "" }},
		{"JSON null payload", func(ev *store.Event) { ev.Payload = "null" }},
		{"JSON array payload", func(ev *store.Event) { ev.Payload = "[]" }},
		{"JSON scalar payload", func(ev *store.Event) { ev.Payload = `"hi"` }},
		{"payload with trailing garbage", func(ev *store.Event) { ev.Payload += "}" }},
		{"payload missing contract fields", func(ev *store.Event) { ev.Payload = `{"text":"hi"}` }},
		{"payload thread_key disagrees with column", func(ev *store.Event) {
			ev.Payload = `{"dedup_key":"discord:msg:9871","thread_key":"discord:dm:OTHER","kind":"message","trust":"owner"}`
		}},
		{"payload trust disagrees with column", func(ev *store.Event) {
			ev.Payload = `{"dedup_key":"discord:msg:9871","thread_key":"discord:dm:123","kind":"message","trust":"untrusted"}`
		}},
		{"zero received", func(ev *store.Event) { ev.Received = 0 }},
		{"negative received", func(ev *store.Event) { ev.Received = -1 }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ev := testEvent()
			tc.mutate(&ev)
			if err := store.InsertEvent(context.Background(), db, ev); err == nil {
				t.Errorf("InsertEvent accepted event with %s, want error", tc.name)
			}
		})
	}
	var n int
	if err := db.QueryRow(`SELECT count(*) FROM events`).Scan(&n); err != nil {
		t.Fatalf("count events: %v", err)
	}
	if n != 0 {
		t.Errorf("%d rows landed from invalid events, want 0", n)
	}
}

// TestInsertEventDuplicateDedupKey: dedup_key is the event's identity
// (§6) — a second insert with the same key must leave exactly one row.
// Until approach-x6n.2.2 lands the dup-insert=no-op contract, the
// duplicate surfaces as a loud UNIQUE violation.
func TestInsertEventDuplicateDedupKey(t *testing.T) {
	db := mustOpen(t, filepath.Join(t.TempDir(), "state", "approach.db"))

	ev := testEvent()
	if err := store.InsertEvent(context.Background(), db, ev); err != nil {
		t.Fatalf("first InsertEvent: %v", err)
	}
	dup := ev
	dup.Payload = `{"dedup_key":"discord:msg:9871","thread_key":"discord:dm:123","kind":"message","trust":"owner","text":"redelivered"}`
	if err := store.InsertEvent(context.Background(), db, dup); err == nil {
		t.Error("duplicate dedup_key accepted, want UNIQUE violation (no-op semantics land in x6n.2.2)")
	}
	var n int
	if err := db.QueryRow(`SELECT count(*) FROM events WHERE dedup_key = ?`, ev.DedupKey).Scan(&n); err != nil {
		t.Fatalf("count events: %v", err)
	}
	if n != 1 {
		t.Errorf("%d rows for dedup_key %q, want exactly 1", n, ev.DedupKey)
	}
}
