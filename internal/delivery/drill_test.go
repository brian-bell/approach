package delivery_test

import (
	"context"
	"log/slog"
	"path/filepath"
	"testing"

	"github.com/brian-bell/approach/internal/delivery"
	"github.com/brian-bell/approach/internal/store"
)

// TestDrillCrashAfterComposeBeforeAckResends is the §4.1 drill: the
// daemon composes a turn's reply — write-before-send puts the rendered
// payload in the outbox — and dies before any platform ack lands. The
// next life's restart resend must deliver every owed row FROM THE
// PERSISTED PAYLOAD, in compose order, and settle the event to
// 'replied'. Both crash positions are drilled at once: a chunk that
// never reached the platform (re-send is the only copy) and a chunk
// the platform accepted whose ack write died (re-send duplicates — the
// trade at-least-once accepts, §4.1 — rather than eats the reply).
func TestDrillCrashAfterComposeBeforeAckResends(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "state", "approach.db")
	ctx := context.Background()

	// Life 1 — the daemon that dies. One turn ran to completion and
	// composed a two-chunk reply (platform length limits chunk long
	// replies; both rows bind the one event).
	db1, err := store.Open(dbPath)
	if err != nil {
		t.Fatalf("open life-1 store: %v", err)
	}
	evID, _, err := store.InsertEvent(ctx, db1, store.Event{
		DedupKey: "discord:msg:1", ThreadKey: "discord:dm:123", Kind: "message", Trust: "owner",
		Payload:  `{"dedup_key":"discord:msg:1","thread_key":"discord:dm:123","kind":"message","trust":"owner"}`,
		Received: 1700000000,
	})
	if err != nil {
		t.Fatalf("insert event: %v", err)
	}
	if err := store.MarkEventProcessing(ctx, db1, evID, 1700000010); err != nil {
		t.Fatalf("mark processing: %v", err)
	}
	if _, err := db1.Exec(`UPDATE events SET status = 'completed' WHERE id = ?`, evID); err != nil {
		t.Fatalf("complete event: %v", err)
	}
	compose := func(key, payload string) int64 {
		t.Helper()
		id, inserted, err := store.InsertDelivery(ctx, db1, store.Delivery{
			DeliveryKey: key, EventID: evID, Target: "discord:dm:123", Payload: payload,
		})
		if err != nil || !inserted {
			t.Fatalf("compose %s: inserted=%v err=%v", key, inserted, err)
		}
		return id
	}
	chunk1 := compose("reply:discord:msg:1:0", "reply chunk one")
	chunk2 := compose("reply:discord:msg:1:1", "reply chunk two")
	// Chunk one WAS handed to the platform — the attempt stamp precedes
	// the send (§4.6) — but the daemon died before the ack write.
	// Chunk two never got that far: composed, zero attempts.
	if err := store.MarkDeliveryAttempt(ctx, db1, chunk1, 1700000020); err != nil {
		t.Fatalf("stamp chunk-1 attempt: %v", err)
	}
	// The crash: no acks, no more writes. (Closing flushes nothing
	// extra — every write above already committed, which is the whole
	// point of write-before-send.)
	if err := db1.Close(); err != nil {
		t.Fatalf("close life-1 store: %v", err)
	}

	// Life 2 — restart. The pump's first pass is the §4.6 resend.
	db2, err := store.Open(dbPath)
	if err != nil {
		t.Fatalf("open life-2 store: %v", err)
	}
	t.Cleanup(func() {
		if err := db2.Close(); err != nil {
			t.Errorf("close life-2 store: %v", err)
		}
	})
	sender := &fakeSender{}
	senders := map[string]delivery.Sender{"discord": sender}
	delivery.ResendUnacked(ctx, db2, senders, slog.Default(), testClock())

	// Both chunks re-sent from their persisted payloads, in compose
	// order — chunk one's re-send is the accepted duplicate, chunk
	// two's is the only copy that will ever exist.
	want := []sentCall{
		{"discord:dm:123", "reply chunk one"},
		{"discord:dm:123", "reply chunk two"},
	}
	if len(sender.sent) != len(want) {
		t.Fatalf("resend sent %d messages %v, want %d", len(sender.sent), sender.sent, len(want))
	}
	for i := range want {
		if sender.sent[i] != want[i] {
			t.Errorf("send %d = %+v, want %+v — the resend replays the PERSISTED payload, in compose order (§4.1)", i, sender.sent[i], want[i])
		}
	}
	for _, tc := range []struct {
		id       int64
		name     string
		attempts int64
	}{
		{chunk1, "chunk 1 (sent, ack lost)", 2},
		{chunk2, "chunk 2 (never sent)", 1},
	} {
		status, attempts, acked := deliveryState(t, db2, tc.id)
		if status != "sent" || !acked.Valid {
			t.Errorf("%s (status, acked) = (%q, %v), want ('sent', set)", tc.name, status, acked)
		}
		if attempts != tc.attempts {
			t.Errorf("%s attempts = %d, want %d — the pre-crash stamp survives into the budget (§4.6)", tc.name, attempts, tc.attempts)
		}
	}
	// The LAST sibling's ack advances the event: the reply is now
	// fully delivered, so the turn is settled history (§4.1).
	var evStatus string
	if err := db2.QueryRow(`SELECT status FROM events WHERE id = ?`, evID).Scan(&evStatus); err != nil {
		t.Fatalf("read event: %v", err)
	}
	if evStatus != "replied" {
		t.Errorf("event status = %q, want 'replied' — completed → replied rides the final ack (§4.1)", evStatus)
	}

	// Steady state: another pass (and by extension every later
	// restart) owes nothing — acked history never re-sends.
	delivery.ResendUnacked(ctx, db2, senders, slog.Default(), testClock())
	if n := len(sender.sent); n != len(want) {
		t.Errorf("a second pass sent %d more messages, want 0 — the ack ended this delivery's story", n-len(want))
	}
}
