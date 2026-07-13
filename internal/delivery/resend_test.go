package delivery_test

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"path/filepath"
	"testing"
	"time"

	"github.com/brian-bell/approach/internal/delivery"
	"github.com/brian-bell/approach/internal/store"
)

// fakeSender records sends and fails on command, per target.
type fakeSender struct {
	sent []sentCall
	fail map[string]bool // target → refuse every send
}

type sentCall struct{ target, payload string }

func (f *fakeSender) Send(_ context.Context, target, payload string) (string, error) {
	if f.fail[target] {
		return "", fmt.Errorf("fake: platform refused %s", target)
	}
	f.sent = append(f.sent, sentCall{target, payload})
	return "fake:msg:" + fmt.Sprint(len(f.sent)), nil
}

func openStore(t *testing.T) *sql.DB {
	t.Helper()
	db, err := store.Open(filepath.Join(t.TempDir(), "state", "approach.db"))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() {
		if err := db.Close(); err != nil {
			t.Errorf("close: %v", err)
		}
	})
	return db
}

func insertPending(t *testing.T, db *sql.DB, key, target, payload string) int64 {
	t.Helper()
	id, inserted, err := store.InsertDelivery(context.Background(), db, store.Delivery{
		DeliveryKey: key, Target: target, Payload: payload,
	})
	if err != nil || !inserted {
		t.Fatalf("InsertDelivery(%s): inserted=%v err=%v", key, inserted, err)
	}
	return id
}

func deliveryState(t *testing.T, db *sql.DB, id int64) (status string, attempts int64, acked sql.NullInt64) {
	t.Helper()
	if err := db.QueryRow(
		`SELECT status, attempts, acked FROM deliveries WHERE id = ?`, id,
	).Scan(&status, &attempts, &acked); err != nil {
		t.Fatalf("read delivery %d: %v", id, err)
	}
	return status, attempts, acked
}

func testClock() func() time.Time {
	return func() time.Time { return time.Unix(1700000500, 0) }
}

// TestResendUnackedSendsPersistedPayloadsInComposeOrder: the §4.6
// recovery half of write-before-send — every row still owed a send
// re-sends from the PERSISTED payload, in compose (id) order, and ends
// sent+acked with the attempt journalled.
func TestResendUnackedSendsPersistedPayloadsInComposeOrder(t *testing.T) {
	db := openStore(t)
	first := insertPending(t, db, "reply:1", "discord:dm:123", "first message")
	second := insertPending(t, db, "reply:2", "discord:dm:123", "second message")

	sender := &fakeSender{}
	delivery.ResendUnacked(context.Background(), db,
		map[string]delivery.Sender{"discord": sender}, slog.Default(), testClock())

	want := []sentCall{
		{"discord:dm:123", "first message"},
		{"discord:dm:123", "second message"},
	}
	if len(sender.sent) != len(want) {
		t.Fatalf("sent %d messages, want %d", len(sender.sent), len(want))
	}
	for i := range want {
		if sender.sent[i] != want[i] {
			t.Errorf("send %d = %+v, want %+v — resend must replay the persisted payload in compose order", i, sender.sent[i], want[i])
		}
	}
	for _, id := range []int64{first, second} {
		status, attempts, acked := deliveryState(t, db, id)
		if status != "sent" || !acked.Valid {
			t.Errorf("row %d (status, acked) = (%q, %v), want ('sent', set)", id, status, acked)
		}
		if attempts != 1 {
			t.Errorf("row %d attempts = %d, want 1 — the attempt is journalled", id, attempts)
		}
	}
}

// TestResendUnackedFailureLeavesRowPending: a refused send costs
// nothing durable — the attempt is journalled (stamp precedes outcome,
// §4.6) and the row stays pending for the next restart. This bead
// never gives up on a row; budgets and dead letters are later flows.
func TestResendUnackedFailureLeavesRowPending(t *testing.T) {
	db := openStore(t)
	id := insertPending(t, db, "reply:1", "discord:dm:123", "hello")

	sender := &fakeSender{fail: map[string]bool{"discord:dm:123": true}}
	delivery.ResendUnacked(context.Background(), db,
		map[string]delivery.Sender{"discord": sender}, slog.Default(), testClock())

	status, attempts, acked := deliveryState(t, db, id)
	if status != "pending" || acked.Valid {
		t.Errorf("(status, acked) = (%q, %v), want ('pending', NULL) — the row stays durably owed", status, acked)
	}
	if attempts != 1 {
		t.Errorf("attempts = %d, want 1 — the failed attempt is still journalled", attempts)
	}
}

// TestResendUnackedFailedTargetStopsItsChain: one thread's messages
// must not reorder — if a target's earlier row fails, its later rows
// are NOT attempted this boot (sending row 2 after row 1 failed would
// deliver a conversation out of order); other targets keep flowing.
func TestResendUnackedFailedTargetStopsItsChain(t *testing.T) {
	db := openStore(t)
	blockedFirst := insertPending(t, db, "reply:1", "discord:dm:blocked", "first")
	otherTarget := insertPending(t, db, "reply:2", "discord:dm:ok", "independent")
	blockedSecond := insertPending(t, db, "reply:3", "discord:dm:blocked", "second")

	sender := &fakeSender{fail: map[string]bool{"discord:dm:blocked": true}}
	delivery.ResendUnacked(context.Background(), db,
		map[string]delivery.Sender{"discord": sender}, slog.Default(), testClock())

	if len(sender.sent) != 1 || sender.sent[0].target != "discord:dm:ok" {
		t.Fatalf("sent = %+v, want exactly the independent target's row", sender.sent)
	}
	if status, _, _ := deliveryState(t, db, otherTarget); status != "sent" {
		t.Errorf("independent target status = %q, want 'sent' — one target's failure must not silence the rest", status)
	}
	_, firstAttempts, _ := deliveryState(t, db, blockedFirst)
	if firstAttempts != 1 {
		t.Errorf("blocked row 1 attempts = %d, want 1", firstAttempts)
	}
	_, secondAttempts, _ := deliveryState(t, db, blockedSecond)
	if secondAttempts != 0 {
		t.Errorf("blocked row 2 attempts = %d, want 0 — no attempt after its target's chain stopped (order would invert)", secondAttempts)
	}
}

// TestResendUnackedUnroutableChannelIsSkipped: a target whose channel
// has no live sender is left untouched — no attempt is journalled
// because no send was attempted; surfacing unroutable rows is the
// dead-letter flow's job (§4.6), and quietly consuming their budget
// here would erode it before that flow exists.
func TestResendUnackedUnroutableChannelIsSkipped(t *testing.T) {
	db := openStore(t)
	id := insertPending(t, db, "reply:1", "slack:dm:U1", "hello")

	sender := &fakeSender{}
	delivery.ResendUnacked(context.Background(), db,
		map[string]delivery.Sender{"discord": sender}, slog.Default(), testClock())

	if len(sender.sent) != 0 {
		t.Fatalf("sent = %+v, want none", sender.sent)
	}
	status, attempts, acked := deliveryState(t, db, id)
	if status != "pending" || attempts != 0 || acked.Valid {
		t.Errorf("(status, attempts, acked) = (%q, %d, %v), want ('pending', 0, NULL) — untouched", status, attempts, acked)
	}
}

// TestResendUnackedAdvancesBoundEvent: the resend leg uses the same
// ack transition as the live leg — a re-sent reply whose event was
// left 'completed' by the crashed turn advances it to 'replied' (§4.1)
// through AckDelivery.
func TestResendUnackedAdvancesBoundEvent(t *testing.T) {
	db := openStore(t)
	ctx := context.Background()

	evID, _, err := store.InsertEvent(ctx, db, store.Event{
		DedupKey: "discord:msg:1", ThreadKey: "discord:dm:123", Kind: "message", Trust: "owner",
		Payload:  `{"dedup_key":"discord:msg:1","thread_key":"discord:dm:123","kind":"message","trust":"owner"}`,
		Received: 1700000000,
	})
	if err != nil {
		t.Fatalf("InsertEvent: %v", err)
	}
	if _, err := db.Exec(`UPDATE events SET status = 'completed' WHERE id = ?`, evID); err != nil {
		t.Fatalf("advance event: %v", err)
	}
	if _, _, err := store.InsertDelivery(ctx, db, store.Delivery{
		DeliveryKey: "reply:1", EventID: evID, Target: "discord:dm:123", Payload: "hello",
	}); err != nil {
		t.Fatalf("InsertDelivery: %v", err)
	}

	delivery.ResendUnacked(ctx, db,
		map[string]delivery.Sender{"discord": &fakeSender{}}, slog.Default(), testClock())

	var evStatus string
	if err := db.QueryRow(`SELECT status FROM events WHERE id = ?`, evID).Scan(&evStatus); err != nil {
		t.Fatalf("read event: %v", err)
	}
	if evStatus != "replied" {
		t.Errorf("event status = %q, want 'replied' — the resend ack completes the §4.1 contract", evStatus)
	}
}
