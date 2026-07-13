package store_test

import (
	"context"
	"database/sql"
	"path/filepath"
	"testing"

	"github.com/brian-bell/approach/internal/store"
)

// testDelivery returns a valid outbound delivery, ready to insert:
// a reply on a discord DM thread with no originating event bound yet
// (callers bind one where the test needs the replied transition).
func testDelivery() store.Delivery {
	return store.Delivery{
		DeliveryKey: "reply:discord:msg:9871",
		Target:      "discord:dm:123",
		Payload:     "the rendered reply text",
	}
}

// TestDeliveriesTableAndResendIndexExist: deliveries is the generalized
// outbound outbox (§4.1, §6) — EVERY outbound message routes through it
// — and deliveries_resend is the partial index over the §4.6 restart
// resend predicate (unacked, non-failed). A freshly opened store must
// already carry both: write-before-send cannot depend on a later setup
// step.
func TestDeliveriesTableAndResendIndexExist(t *testing.T) {
	db := mustOpen(t, filepath.Join(t.TempDir(), "state", "approach.db"))

	for _, obj := range []struct{ typ, name string }{
		{"table", "deliveries"},
		{"index", "deliveries_resend"},
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

// TestInsertDeliveryPersistsBeforeSend: the write-before-send
// chokepoint (§4.1) — the rendered payload lands durably with the
// schema owning the lifecycle defaults (status 'pending', zero
// attempts, no last_attempt, unacked): insert records intent, it never
// sends.
func TestInsertDeliveryPersistsBeforeSend(t *testing.T) {
	db := mustOpen(t, filepath.Join(t.TempDir(), "state", "approach.db"))

	d := testDelivery()
	id, inserted, err := store.InsertDelivery(context.Background(), db, d)
	if err != nil {
		t.Fatalf("InsertDelivery: %v", err)
	}
	if !inserted {
		t.Error("InsertDelivery reported inserted=false on a fresh delivery, want true")
	}
	if id <= 0 {
		t.Errorf("InsertDelivery returned id %d, want a positive row id", id)
	}

	var (
		target, payload, status string
		attempts                int64
		eventID, lastAttempt    sql.NullInt64
		acked                   sql.NullInt64
	)
	if err := db.QueryRow(
		`SELECT event_id, target, payload, status, attempts, last_attempt, acked
		 FROM deliveries WHERE delivery_key = ?`, d.DeliveryKey,
	).Scan(&eventID, &target, &payload, &status, &attempts, &lastAttempt, &acked); err != nil {
		t.Fatalf("read back delivery: %v", err)
	}
	if target != d.Target || payload != d.Payload {
		t.Errorf("delivery fields did not round-trip: got (%q, %q)", target, payload)
	}
	if eventID.Valid {
		t.Errorf("event_id = %d, want NULL for an unbound delivery", eventID.Int64)
	}
	if status != "pending" {
		t.Errorf("status = %q, want the schema default 'pending'", status)
	}
	if attempts != 0 {
		t.Errorf("attempts = %d, want 0 before any send attempt", attempts)
	}
	if lastAttempt.Valid || acked.Valid {
		t.Errorf("last_attempt/acked already set (%v, %v), want NULL — insert must not read as an attempt or an ack", lastAttempt, acked)
	}
}

// TestInsertDeliveryDuplicateKeyIsNoOp: a crash-retried compose must
// collapse to the original row (§4.1) — the first write wins,
// including lifecycle state a send may already have advanced, and
// inserted=false tells the caller not to send what may already be
// acked. A divergent payload under the same key must NOT replace the
// persisted one: at-least-once duplication comes from re-sending,
// never from re-composing.
func TestInsertDeliveryDuplicateKeyIsNoOp(t *testing.T) {
	db := mustOpen(t, filepath.Join(t.TempDir(), "state", "approach.db"))
	ctx := context.Background()

	d := testDelivery()
	if _, _, err := store.InsertDelivery(ctx, db, d); err != nil {
		t.Fatalf("first InsertDelivery: %v", err)
	}
	// Advance lifecycle state the duplicate must not disturb.
	if _, err := db.Exec(`UPDATE deliveries SET attempts = 1, last_attempt = 1700000100 WHERE delivery_key = ?`, d.DeliveryKey); err != nil {
		t.Fatalf("advance original row: %v", err)
	}

	dup := d
	dup.Payload = "a DIVERGENT re-render that must not land"
	id, inserted, err := store.InsertDelivery(ctx, db, dup)
	if err != nil {
		t.Fatalf("duplicate InsertDelivery: %v", err)
	}
	if inserted {
		t.Error("duplicate InsertDelivery reported inserted=true, want false")
	}
	if id != 0 {
		t.Errorf("duplicate InsertDelivery returned id %d, want 0 — never the original's id", id)
	}

	var payload string
	var attempts int64
	if err := db.QueryRow(
		`SELECT payload, attempts FROM deliveries WHERE delivery_key = ?`, d.DeliveryKey,
	).Scan(&payload, &attempts); err != nil {
		t.Fatalf("read back original: %v", err)
	}
	if payload != d.Payload {
		t.Errorf("payload = %q, want the ORIGINAL %q — first write wins", payload, d.Payload)
	}
	if attempts != 1 {
		t.Errorf("attempts = %d, want 1 — lifecycle state must survive a duplicate insert", attempts)
	}
}

// TestInsertDeliveryValidation: a delivery the outbox could not
// honestly send is refused before the db is touched (fail loud, §4.1)
// — a keyless, unaddressed, or empty row would park the outbox on
// something no resend can drain.
func TestInsertDeliveryValidation(t *testing.T) {
	db := mustOpen(t, filepath.Join(t.TempDir(), "state", "approach.db"))

	cases := []struct {
		name   string
		mutate func(*store.Delivery)
	}{
		{"empty delivery_key", func(d *store.Delivery) { d.DeliveryKey = "" }},
		{"empty target", func(d *store.Delivery) { d.Target = "" }},
		{"empty payload", func(d *store.Delivery) { d.Payload = "" }},
		{"negative event_id", func(d *store.Delivery) { d.EventID = -1 }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			d := testDelivery()
			tc.mutate(&d)
			_, inserted, err := store.InsertDelivery(context.Background(), db, d)
			if err == nil {
				t.Fatal("InsertDelivery accepted an unsendable delivery, want error")
			}
			if inserted {
				t.Error("InsertDelivery reported inserted=true alongside an error")
			}
		})
	}

	var n int
	if err := db.QueryRow(`SELECT count(*) FROM deliveries`).Scan(&n); err != nil {
		t.Fatalf("count deliveries: %v", err)
	}
	if n != 0 {
		t.Errorf("found %d rows after refused inserts, want 0 — validation must precede the write", n)
	}
}

// TestInsertDeliveryEventBinding: event_id links the delivery to its
// originating turn (§6). A real event id round-trips; a dangling one
// is refused by the foreign key (store posture: foreign_keys=ON) —
// an outbox row claiming an origin that does not exist would make the
// completed → replied transition unverifiable.
func TestInsertDeliveryEventBinding(t *testing.T) {
	db := mustOpen(t, filepath.Join(t.TempDir(), "state", "approach.db"))
	ctx := context.Background()

	evID, _, err := store.InsertEvent(ctx, db, testEvent())
	if err != nil {
		t.Fatalf("InsertEvent: %v", err)
	}

	d := testDelivery()
	d.EventID = evID
	if _, _, err := store.InsertDelivery(ctx, db, d); err != nil {
		t.Fatalf("InsertDelivery with real event: %v", err)
	}
	var got int64
	if err := db.QueryRow(
		`SELECT event_id FROM deliveries WHERE delivery_key = ?`, d.DeliveryKey,
	).Scan(&got); err != nil {
		t.Fatalf("read back event_id: %v", err)
	}
	if got != evID {
		t.Errorf("event_id = %d, want %d", got, evID)
	}

	dangling := testDelivery()
	dangling.DeliveryKey = "reply:discord:msg:none"
	dangling.EventID = evID + 999
	if _, _, err := store.InsertDelivery(ctx, db, dangling); err == nil {
		t.Error("InsertDelivery accepted a dangling event_id, want a foreign-key refusal")
	}
}

// TestInsertDeliveryRepliedEventIsSealed: replied is terminal — it
// asserts the platform accepted EVERY outbound row (§4.1), so a
// composer inserting another chunk after an ack already sealed the
// event is a sequencing bug that must fail loud: silently accepting
// the row would strand it under an event whose "all accepted" claim
// it falsifies. A duplicate key against a sealed event stays the
// benign no-op — the original row is the one already accounted for.
func TestInsertDeliveryRepliedEventIsSealed(t *testing.T) {
	db := mustOpen(t, filepath.Join(t.TempDir(), "state", "approach.db"))
	ctx := context.Background()

	evID := insertCompletedEvent(t, db)
	first := testDelivery()
	first.EventID = evID
	firstID := insertTestDelivery(t, db, first)
	if err := store.AckDelivery(ctx, db, firstID, 1700000500); err != nil {
		t.Fatalf("AckDelivery: %v", err)
	}
	if got := eventStatus(t, db, evID); got != "replied" {
		t.Fatalf("event status = %q, want 'replied' — the seal under test", got)
	}

	late := testDelivery()
	late.DeliveryKey = "reply:discord:msg:9871:late"
	late.EventID = evID
	if _, _, err := store.InsertDelivery(ctx, db, late); err == nil {
		t.Error("InsertDelivery bound a new row to a replied event, want error — the seal must be mechanical")
	}
	var n int
	if err := db.QueryRow(`SELECT count(*) FROM deliveries WHERE delivery_key = ?`, late.DeliveryKey).Scan(&n); err != nil {
		t.Fatalf("count late rows: %v", err)
	}
	if n != 0 {
		t.Errorf("found %d late rows, want 0 — the refused insert must not land", n)
	}

	dup := first
	dup.Payload = "divergent"
	id, inserted, err := store.InsertDelivery(ctx, db, dup)
	if err != nil || inserted || id != 0 {
		t.Errorf("duplicate against a sealed event: (id, inserted, err) = (%d, %v, %v), want (0, false, nil) — still the benign no-op", id, inserted, err)
	}
}

// insertTestDelivery inserts d and returns its row id.
func insertTestDelivery(t *testing.T, db *sql.DB, d store.Delivery) int64 {
	t.Helper()
	id, inserted, err := store.InsertDelivery(context.Background(), db, d)
	if err != nil || !inserted {
		t.Fatalf("InsertDelivery(%s): inserted=%v err=%v", d.DeliveryKey, inserted, err)
	}
	return id
}

// insertCompletedEvent inserts a test event and advances it to
// 'completed' — the state a turn handler leaves it in after the engine
// finishes but before the platform ack (§4.1). Direct SQL because the
// completion transition belongs to the turn wiring, not this bead.
func insertCompletedEvent(t *testing.T, db *sql.DB) int64 {
	t.Helper()
	id, _, err := store.InsertEvent(context.Background(), db, testEvent())
	if err != nil {
		t.Fatalf("InsertEvent: %v", err)
	}
	if _, err := db.Exec(`UPDATE events SET status = 'completed' WHERE id = ?`, id); err != nil {
		t.Fatalf("advance event to completed: %v", err)
	}
	return id
}

// deliveryState reads back a delivery row's lifecycle columns.
func deliveryState(t *testing.T, db *sql.DB, id int64) (status string, attempts int64, lastAttempt, acked sql.NullInt64) {
	t.Helper()
	if err := db.QueryRow(
		`SELECT status, attempts, last_attempt, acked FROM deliveries WHERE id = ?`, id,
	).Scan(&status, &attempts, &lastAttempt, &acked); err != nil {
		t.Fatalf("read delivery %d: %v", id, err)
	}
	return status, attempts, lastAttempt, acked
}

// eventStatus reads back an event row's status.
func eventStatus(t *testing.T, db *sql.DB, id int64) string {
	t.Helper()
	var s string
	if err := db.QueryRow(`SELECT status FROM events WHERE id = ?`, id).Scan(&s); err != nil {
		t.Fatalf("read event %d: %v", id, err)
	}
	return s
}

// TestResendableDeliveries: the §4.6 restart resend scan — every row
// still owed a send (unacked, non-failed), in id (compose) order, so
// one thread's messages re-send in the order they were composed. This
// query is the single runtime definition of "owed a send" and must
// match the deliveries_resend index predicate exactly: acked rows are
// delivered history, failed rows left for the dead-letter flows.
func TestResendableDeliveries(t *testing.T) {
	db := mustOpen(t, filepath.Join(t.TempDir(), "state", "approach.db"))
	ctx := context.Background()

	pending := testDelivery()
	pending.DeliveryKey = "reply:pending"
	pendingID := insertTestDelivery(t, db, pending)

	ackedRow := testDelivery()
	ackedRow.DeliveryKey = "reply:acked"
	ackedID := insertTestDelivery(t, db, ackedRow)
	if err := store.AckDelivery(ctx, db, ackedID, 1700000500); err != nil {
		t.Fatalf("AckDelivery: %v", err)
	}

	failedRow := testDelivery()
	failedRow.DeliveryKey = "reply:failed"
	failedID := insertTestDelivery(t, db, failedRow)
	if err := store.MarkDeliveryFailed(ctx, db, failedID); err != nil {
		t.Fatalf("MarkDeliveryFailed: %v", err)
	}

	attempted := testDelivery()
	attempted.DeliveryKey = "reply:attempted"
	attemptedID := insertTestDelivery(t, db, attempted)
	if err := store.MarkDeliveryAttempt(ctx, db, attemptedID, 1700000600); err != nil {
		t.Fatalf("MarkDeliveryAttempt: %v", err)
	}

	rows, err := store.ResendableDeliveries(ctx, db)
	if err != nil {
		t.Fatalf("ResendableDeliveries: %v", err)
	}
	if len(rows) != 2 {
		t.Fatalf("got %d resendable rows, want 2 (pending + attempted; never acked or failed)", len(rows))
	}
	if rows[0].ID != pendingID || rows[1].ID != attemptedID {
		t.Errorf("resend order = (%d, %d), want compose order (%d, %d)", rows[0].ID, rows[1].ID, pendingID, attemptedID)
	}
	if rows[0].DeliveryKey != pending.DeliveryKey || rows[0].Target != pending.Target || rows[0].Payload != pending.Payload {
		t.Errorf("row 0 did not round-trip: got (%q, %q, %q)", rows[0].DeliveryKey, rows[0].Target, rows[0].Payload)
	}
	if rows[1].Attempts != 1 {
		t.Errorf("attempted row Attempts = %d, want 1 — the §4.6 budget accounting rides this field", rows[1].Attempts)
	}
}

// TestMarkDeliveryAttempt: each send attempt is journalled before its
// outcome is known (§4.6 retry accounting reads attempts/last_attempt)
// — and only rows still owed a send may attempt: stamping an acked or
// failed row is a caller bug that must fail loud, not silently
// resurrect terminal history.
func TestMarkDeliveryAttempt(t *testing.T) {
	db := mustOpen(t, filepath.Join(t.TempDir(), "state", "approach.db"))
	ctx := context.Background()

	id := insertTestDelivery(t, db, testDelivery())
	if err := store.MarkDeliveryAttempt(ctx, db, id, 1700000100); err != nil {
		t.Fatalf("MarkDeliveryAttempt: %v", err)
	}
	if err := store.MarkDeliveryAttempt(ctx, db, id, 1700000200); err != nil {
		t.Fatalf("second MarkDeliveryAttempt: %v", err)
	}
	status, attempts, lastAttempt, _ := deliveryState(t, db, id)
	if status != "pending" {
		t.Errorf("status = %q, want 'pending' — an attempt is not an outcome", status)
	}
	if attempts != 2 {
		t.Errorf("attempts = %d, want 2", attempts)
	}
	if !lastAttempt.Valid || lastAttempt.Int64 != 1700000200 {
		t.Errorf("last_attempt = %v, want 1700000200", lastAttempt)
	}

	for _, tc := range []struct {
		name    string
		advance string
	}{
		{"failed row", `UPDATE deliveries SET status = 'failed' WHERE id = ?`},
		{"acked row", `UPDATE deliveries SET status = 'sent', acked = 1700000300 WHERE id = ?`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			id := insertTestDelivery(t, db, store.Delivery{
				DeliveryKey: "reply:" + tc.name, Target: "discord:dm:123", Payload: "x",
			})
			if _, err := db.Exec(tc.advance, id); err != nil {
				t.Fatalf("advance row: %v", err)
			}
			if err := store.MarkDeliveryAttempt(context.Background(), db, id, 1700000400); err == nil {
				t.Error("MarkDeliveryAttempt accepted a row no longer owed a send, want error")
			}
		})
	}
}

// TestAckDeliveryAdvancesEventToReplied: the §4.1 delivery contract —
// the event advances to 'replied' ONLY when the platform ack arrives,
// and the delivery's sent+acked stamp and the event transition are one
// atomic step: neither state may exist without the other.
func TestAckDeliveryAdvancesEventToReplied(t *testing.T) {
	db := mustOpen(t, filepath.Join(t.TempDir(), "state", "approach.db"))
	ctx := context.Background()

	evID := insertCompletedEvent(t, db)
	d := testDelivery()
	d.EventID = evID
	id := insertTestDelivery(t, db, d)

	if err := store.AckDelivery(ctx, db, id, 1700000500); err != nil {
		t.Fatalf("AckDelivery: %v", err)
	}
	status, _, _, acked := deliveryState(t, db, id)
	if status != "sent" {
		t.Errorf("status = %q, want 'sent'", status)
	}
	if !acked.Valid || acked.Int64 != 1700000500 {
		t.Errorf("acked = %v, want 1700000500", acked)
	}
	if got := eventStatus(t, db, evID); got != "replied" {
		t.Errorf("event status = %q, want 'replied' — completed → replied rides the ack (§4.1)", got)
	}
}

// TestAckDeliveryDuplicateIsNoOp: at-least-once means duplicate sends,
// and duplicate sends mean duplicate acks — the second ack is normal
// operation, not an error, and must not re-stamp what the first
// already recorded.
func TestAckDeliveryDuplicateIsNoOp(t *testing.T) {
	db := mustOpen(t, filepath.Join(t.TempDir(), "state", "approach.db"))
	ctx := context.Background()

	evID := insertCompletedEvent(t, db)
	d := testDelivery()
	d.EventID = evID
	id := insertTestDelivery(t, db, d)

	if err := store.AckDelivery(ctx, db, id, 1700000500); err != nil {
		t.Fatalf("first AckDelivery: %v", err)
	}
	if err := store.AckDelivery(ctx, db, id, 1700000900); err != nil {
		t.Fatalf("duplicate AckDelivery: %v — a re-send's ack is normal, not an error", err)
	}
	_, _, _, acked := deliveryState(t, db, id)
	if !acked.Valid || acked.Int64 != 1700000500 {
		t.Errorf("acked = %v, want the FIRST ack's 1700000500 — the original stamp wins", acked)
	}
	if got := eventStatus(t, db, evID); got != "replied" {
		t.Errorf("event status = %q, want 'replied'", got)
	}
}

// TestAckDeliveryWithoutEvent: a pure scheduler notify (§4.2) has no
// originating turn — the ack stamps the delivery and touches no event.
func TestAckDeliveryWithoutEvent(t *testing.T) {
	db := mustOpen(t, filepath.Join(t.TempDir(), "state", "approach.db"))

	id := insertTestDelivery(t, db, testDelivery())
	if err := store.AckDelivery(context.Background(), db, id, 1700000500); err != nil {
		t.Fatalf("AckDelivery: %v", err)
	}
	status, _, _, acked := deliveryState(t, db, id)
	if status != "sent" || !acked.Valid {
		t.Errorf("(status, acked) = (%q, %v), want ('sent', set)", status, acked)
	}
}

// TestAckDeliveryEventNotCompletedRollsBack: an ack whose bound event
// is not yet 'completed' is a sequencing bug (the reply leg ran before
// the turn finished) — it must fail loud AND leave the delivery
// unacked: a half-advanced state (delivery sent, event stuck in
// processing) would satisfy neither the resend scan nor the queue.
func TestAckDeliveryEventNotCompletedRollsBack(t *testing.T) {
	db := mustOpen(t, filepath.Join(t.TempDir(), "state", "approach.db"))
	ctx := context.Background()

	evID, _, err := store.InsertEvent(ctx, db, testEvent())
	if err != nil {
		t.Fatalf("InsertEvent: %v", err)
	}
	if _, err := db.Exec(`UPDATE events SET status = 'processing' WHERE id = ?`, evID); err != nil {
		t.Fatalf("advance event to processing: %v", err)
	}
	d := testDelivery()
	d.EventID = evID
	id := insertTestDelivery(t, db, d)

	if err := store.AckDelivery(ctx, db, id, 1700000500); err == nil {
		t.Fatal("AckDelivery accepted an ack for a turn still processing, want error")
	}
	status, _, _, acked := deliveryState(t, db, id)
	if status != "pending" || acked.Valid {
		t.Errorf("(status, acked) = (%q, %v), want ('pending', NULL) — the refused ack must roll back atomically", status, acked)
	}
	if got := eventStatus(t, db, evID); got != "processing" {
		t.Errorf("event status = %q, want 'processing' untouched", got)
	}
}

// TestAckDeliverySiblingsAdvanceEventOnlyWhenAllAcked: one turn may
// emit several outbound messages (§4.1 routes EVERY outbound message
// through the outbox; platform length limits chunk long replies), so
// completed → replied must ride the LAST sibling's ack, not the first
// — and an earlier sibling's ack must be recorded, not refused, or the
// row would sit in the resend scan re-sending forever.
func TestAckDeliverySiblingsAdvanceEventOnlyWhenAllAcked(t *testing.T) {
	db := mustOpen(t, filepath.Join(t.TempDir(), "state", "approach.db"))
	ctx := context.Background()

	evID := insertCompletedEvent(t, db)
	first := testDelivery()
	first.DeliveryKey = "reply:discord:msg:9871:part1"
	first.EventID = evID
	firstID := insertTestDelivery(t, db, first)
	second := testDelivery()
	second.DeliveryKey = "reply:discord:msg:9871:part2"
	second.EventID = evID
	secondID := insertTestDelivery(t, db, second)

	if err := store.AckDelivery(ctx, db, firstID, 1700000500); err != nil {
		t.Fatalf("first sibling AckDelivery: %v", err)
	}
	status, _, _, acked := deliveryState(t, db, firstID)
	if status != "sent" || !acked.Valid {
		t.Errorf("first sibling (status, acked) = (%q, %v), want ('sent', set) — an early ack must be recorded", status, acked)
	}
	if got := eventStatus(t, db, evID); got != "completed" {
		t.Errorf("event status = %q after first of two acks, want 'completed' — replied means ALL outbound accepted", got)
	}

	if err := store.AckDelivery(ctx, db, secondID, 1700000600); err != nil {
		t.Fatalf("second sibling AckDelivery: %v", err)
	}
	if got := eventStatus(t, db, evID); got != "replied" {
		t.Errorf("event status = %q after the last ack, want 'replied'", got)
	}
}

// TestAckDeliveryFailedSiblingBlocksReplied: a reply with an
// undelivered chunk is NOT replied — 'replied' asserts the platform
// accepted every outbound row (§4.1), so a terminally failed sibling
// blocks the transition permanently and the event's state must not
// depend on whether the failure or the ack landed first: both orders
// converge on 'completed', where the §4.6 dead-letter flow owns the
// event's terminal outcome.
func TestAckDeliveryFailedSiblingBlocksReplied(t *testing.T) {
	for _, order := range []string{"fail then ack", "ack then fail"} {
		t.Run(order, func(t *testing.T) {
			db := mustOpen(t, filepath.Join(t.TempDir(), "state", "approach.db"))
			ctx := context.Background()

			evID := insertCompletedEvent(t, db)
			doomed := testDelivery()
			doomed.DeliveryKey = "reply:discord:msg:9871:part1"
			doomed.EventID = evID
			doomedID := insertTestDelivery(t, db, doomed)
			ok := testDelivery()
			ok.DeliveryKey = "reply:discord:msg:9871:part2"
			ok.EventID = evID
			okID := insertTestDelivery(t, db, ok)

			steps := []func() error{
				func() error { return store.MarkDeliveryFailed(ctx, db, doomedID) },
				func() error { return store.AckDelivery(ctx, db, okID, 1700000500) },
			}
			if order == "ack then fail" {
				steps[0], steps[1] = steps[1], steps[0]
			}
			for i, step := range steps {
				if err := step(); err != nil {
					t.Fatalf("step %d (%s): %v", i, order, err)
				}
			}

			if got := eventStatus(t, db, evID); got != "completed" {
				t.Errorf("event status = %q, want 'completed' — a failed sibling blocks replied in EITHER order", got)
			}
			status, _, _, acked := deliveryState(t, db, okID)
			if status != "sent" || !acked.Valid {
				t.Errorf("delivered sibling (status, acked) = (%q, %v), want ('sent', set) — the ack itself must still be recorded", status, acked)
			}
		})
	}
}

// TestAckDeliveryParkedEventRecordsAckWithoutAdvance: a late ack can
// land after the event left the live path (§4.6 parked it interrupted,
// or the dead-letter flow marked it dead). The platform DID accept the
// message — refusing the ack would roll it back into the resend scan
// and re-send forever — so the ack is recorded and the event, whose
// state those flows own, is left untouched. Only a PRE-completion
// event (the reply leg outran the turn) refuses.
func TestAckDeliveryParkedEventRecordsAckWithoutAdvance(t *testing.T) {
	db := mustOpen(t, filepath.Join(t.TempDir(), "state", "approach.db"))
	ctx := context.Background()

	evID, _, err := store.InsertEvent(ctx, db, testEvent())
	if err != nil {
		t.Fatalf("InsertEvent: %v", err)
	}
	if _, err := db.Exec(`UPDATE events SET status = 'interrupted' WHERE id = ?`, evID); err != nil {
		t.Fatalf("park event: %v", err)
	}
	d := testDelivery()
	d.EventID = evID
	id := insertTestDelivery(t, db, d)

	if err := store.AckDelivery(ctx, db, id, 1700000500); err != nil {
		t.Fatalf("AckDelivery on a parked event: %v — a rolled-back ack would resend forever", err)
	}
	status, _, _, acked := deliveryState(t, db, id)
	if status != "sent" || !acked.Valid {
		t.Errorf("(status, acked) = (%q, %v), want ('sent', set)", status, acked)
	}
	if got := eventStatus(t, db, evID); got != "interrupted" {
		t.Errorf("event status = %q, want 'interrupted' untouched — parked state is the §4.6 flows' to advance", got)
	}
}

// TestAckDeliveryFailedRowIsError: 'failed' is terminal — the retry
// budget is exhausted and no send remains in flight, so an ack against
// a failed row is a caller bug, never a state to record.
func TestAckDeliveryFailedRowIsError(t *testing.T) {
	db := mustOpen(t, filepath.Join(t.TempDir(), "state", "approach.db"))

	id := insertTestDelivery(t, db, testDelivery())
	if _, err := db.Exec(`UPDATE deliveries SET status = 'failed' WHERE id = ?`, id); err != nil {
		t.Fatalf("fail row: %v", err)
	}
	if err := store.AckDelivery(context.Background(), db, id, 1700000500); err == nil {
		t.Error("AckDelivery accepted a terminal failed row, want error")
	}
}

// TestMarkDeliveryFailed: terminal give-up (§4.6 — retry budget
// exhausted; dead-letter surfacing is the epic's later flow). Acked
// rows refuse: flipping a delivered message to failed would un-deliver
// history.
func TestMarkDeliveryFailed(t *testing.T) {
	db := mustOpen(t, filepath.Join(t.TempDir(), "state", "approach.db"))
	ctx := context.Background()

	id := insertTestDelivery(t, db, testDelivery())
	if err := store.MarkDeliveryFailed(ctx, db, id); err != nil {
		t.Fatalf("MarkDeliveryFailed: %v", err)
	}
	status, _, _, _ := deliveryState(t, db, id)
	if status != "failed" {
		t.Errorf("status = %q, want 'failed'", status)
	}
	if err := store.MarkDeliveryFailed(ctx, db, id); err == nil {
		t.Error("repeat MarkDeliveryFailed reported a fresh transition, want error — dead-letter surfacing keys off ENTERING failed (§4.6), and a re-entered row would surface twice")
	}

	acked := testDelivery()
	acked.DeliveryKey = "reply:discord:msg:acked"
	ackedID := insertTestDelivery(t, db, acked)
	if err := store.AckDelivery(ctx, db, ackedID, 1700000500); err != nil {
		t.Fatalf("AckDelivery: %v", err)
	}
	if err := store.MarkDeliveryFailed(ctx, db, ackedID); err == nil {
		t.Error("MarkDeliveryFailed accepted an acked row, want error — delivered history must stand")
	}
}
