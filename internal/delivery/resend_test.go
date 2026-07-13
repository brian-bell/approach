package delivery_test

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/brian-bell/approach/internal/delivery"
	"github.com/brian-bell/approach/internal/store"
)

// fakeSender records sends and fails on command, per target. Guarded:
// the pump test reads counts from the test goroutine while the pump
// goroutine sends.
type fakeSender struct {
	mu   sync.Mutex
	sent []sentCall
	fail map[string]bool // target → refuse every send
}

type sentCall struct{ target, payload string }

func (f *fakeSender) Send(_ context.Context, target, payload string) (string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.fail[target] {
		return "", fmt.Errorf("fake: platform refused %s", target)
	}
	f.sent = append(f.sent, sentCall{target, payload})
	return "fake:msg:" + fmt.Sprint(len(f.sent)), nil
}

func (f *fakeSender) count() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.sent)
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

// TestSurfaceInterrupted: the §4.6 park notice — an outbox row aimed
// at the originating thread, keyed deterministically so a crash-
// retried park collapses to ONE notification, bound to the event for
// correlation, payload naming what was interrupted and offering the
// retry. Write-before-send delivers it even when the park happened
// before the adapter was up (crash parks at boot).
func TestSurfaceInterrupted(t *testing.T) {
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
	if err := store.MarkEventProcessing(ctx, db, evID, 1700000050); err != nil {
		t.Fatalf("MarkEventProcessing: %v", err)
	}
	if err := store.ParkEvent(ctx, db, evID, 1700000100); err != nil {
		t.Fatalf("ParkEvent: %v", err)
	}

	ev := store.QueuedEvent{ID: evID, DedupKey: "discord:msg:1", ThreadKey: "discord:dm:123"}
	if err := delivery.SurfaceInterrupted(ctx, db, ev); err != nil {
		t.Fatalf("SurfaceInterrupted: %v", err)
	}

	var key, target, payload string
	var boundEvent sql.NullInt64
	if err := db.QueryRow(
		`SELECT delivery_key, target, payload, event_id FROM deliveries`,
	).Scan(&key, &target, &payload, &boundEvent); err != nil {
		t.Fatalf("read surface row: %v", err)
	}
	if key != "interrupted:discord:msg:1:1" {
		t.Errorf("delivery_key = %q, want interrupted:<dedup_key>:<park generation> — deterministic within the episode, fresh per re-park", key)
	}
	if target != "discord:dm:123" {
		t.Errorf("target = %q, want the originating thread", target)
	}
	// DELIBERATELY unbound: a bound notice whose event is retried
	// (received/processing again) before the ack lands would roll the
	// ack back on every pump pass — re-sending the notice forever. The
	// deterministic key carries the correlation instead.
	if boundEvent.Valid {
		t.Errorf("event_id = %d, want NULL — the notice's ack must never gate on the event's reply state", boundEvent.Int64)
	}
	if !strings.Contains(payload, "interrupted") || !strings.Contains(payload, "retry") {
		t.Errorf("payload %q must say what happened and offer the retry (§4.6)", payload)
	}

	// A repeated surface of the SAME episode (crash between park and
	// surface, then the sweep) must not re-notify.
	if err := delivery.SurfaceInterrupted(ctx, db, ev); err != nil {
		t.Fatalf("duplicate SurfaceInterrupted: %v", err)
	}
	var n int
	if err := db.QueryRow(`SELECT count(*) FROM deliveries`).Scan(&n); err != nil {
		t.Fatalf("count: %v", err)
	}
	if n != 1 {
		t.Errorf("found %d surface rows, want 1 — same-episode surfaces collapse", n)
	}
}

// TestSurfaceInterruptedFreshNoticePerEpisode: a manual retry that
// fails again is a NEW episode — the first notice must not suppress
// the second, or the owner's failed retry parks silently forever.
// And a surface racing a retry (event no longer interrupted) composes
// nothing: there is no park to announce.
func TestSurfaceInterruptedFreshNoticePerEpisode(t *testing.T) {
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
	ev := store.QueuedEvent{ID: evID, DedupKey: "discord:msg:1", ThreadKey: "discord:dm:123"}

	// Episode 1: park + notice.
	if err := store.MarkEventProcessing(ctx, db, evID, 1700000050); err != nil {
		t.Fatalf("MarkEventProcessing: %v", err)
	}
	if err := store.ParkEvent(ctx, db, evID, 1700000100); err != nil {
		t.Fatalf("ParkEvent: %v", err)
	}
	if err := delivery.SurfaceInterrupted(ctx, db, ev); err != nil {
		t.Fatalf("SurfaceInterrupted: %v", err)
	}

	// The owner retries; the surface racing AFTER the retry composes
	// nothing new.
	if _, err := store.RequeueInterruptedEvent(ctx, db, evID, 1700000200); err != nil {
		t.Fatalf("RequeueInterruptedEvent: %v", err)
	}
	if err := delivery.SurfaceInterrupted(ctx, db, ev); err != nil {
		t.Fatalf("SurfaceInterrupted on a retried event: %v", err)
	}
	var n int
	if err := db.QueryRow(`SELECT count(*) FROM deliveries`).Scan(&n); err != nil {
		t.Fatalf("count: %v", err)
	}
	if n != 1 {
		t.Fatalf("found %d notices after a surface against a retried event, want 1 — no park, no notice", n)
	}

	// Episode 2: the retry fails and re-parks — the owner MUST hear it.
	if err := store.MarkEventProcessing(ctx, db, evID, 1700000250); err != nil {
		t.Fatalf("MarkEventProcessing: %v", err)
	}
	if err := store.ParkEvent(ctx, db, evID, 1700000300); err != nil {
		t.Fatalf("re-park: %v", err)
	}
	if err := delivery.SurfaceInterrupted(ctx, db, ev); err != nil {
		t.Fatalf("SurfaceInterrupted (episode 2): %v", err)
	}
	if err := db.QueryRow(`SELECT count(*) FROM deliveries`).Scan(&n); err != nil {
		t.Fatalf("count: %v", err)
	}
	if n != 2 {
		t.Errorf("found %d notices after a re-park, want 2 — a failed retry must not park silently", n)
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

// TestPumpDrainsNewRowsWhileRunning: the outbox is not a boot-only
// artifact — a §4.6 notice composed while the daemon is up (an engine
// failure parked a turn) must send NOW, not on the next restart. The
// pump drains once at start (the restart resend), then again on every
// kick.
func TestPumpDrainsNewRowsWhileRunning(t *testing.T) {
	db := openStore(t)
	boot := insertPending(t, db, "reply:boot", "discord:dm:123", "owed from the last life")

	sender := &fakeSender{}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	kick := make(chan struct{}, 1)
	done := make(chan struct{})
	go func() {
		defer close(done)
		delivery.Pump(ctx, db, map[string]delivery.Sender{"discord": sender},
			slog.Default(), testClock(), kick, time.Hour) // ticker out of the picture — kicks drive this test
	}()

	waitForSends := func(n int) {
		t.Helper()
		deadline := time.Now().Add(5 * time.Second)
		for time.Now().Before(deadline) {
			if sender.count() >= n {
				return
			}
			time.Sleep(5 * time.Millisecond)
		}
		t.Fatalf("pump never reached %d sends (have %d)", n, sender.count())
	}
	waitForSends(1) // the initial drain — the restart resend

	live := insertPending(t, db, "reply:live", "discord:dm:123", "composed mid-life")
	kick <- struct{}{}
	waitForSends(2)

	cancel()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("pump did not exit on context cancel")
	}
	for _, id := range []int64{boot, live} {
		status, _, acked := deliveryState(t, db, id)
		if status != "sent" || !acked.Valid {
			t.Errorf("row %d (status, acked) = (%q, %v), want ('sent', set)", id, status, acked)
		}
	}
}

// TestPumpSurfacesUnsurfacedInterruptedEvents: a park whose notice
// write failed (transient store error, crash between park and surface)
// must not stay silent forever — interrupted rows are out of the
// queue's rescans, so the pump itself sweeps for interrupted events
// with no notice row and composes the missing notice, idempotently
// (the deterministic key makes a re-sweep a no-op).
func TestPumpSurfacesUnsurfacedInterruptedEvents(t *testing.T) {
	db := openStore(t)
	ctx := context.Background()

	evID, _, err := store.InsertEvent(ctx, db, store.Event{
		DedupKey: "discord:msg:lost", ThreadKey: "discord:dm:123", Kind: "message", Trust: "owner",
		Payload:  `{"dedup_key":"discord:msg:lost","thread_key":"discord:dm:123","kind":"message","trust":"owner"}`,
		Received: 1700000000,
	})
	if err != nil {
		t.Fatalf("InsertEvent: %v", err)
	}
	// Parked, but the notice never landed — the exact loss mode.
	if err := store.MarkEventProcessing(ctx, db, evID, 1700000050); err != nil {
		t.Fatalf("MarkEventProcessing: %v", err)
	}
	if err := store.ParkEvent(ctx, db, evID, 1700000100); err != nil {
		t.Fatalf("park: %v", err)
	}

	sender := &fakeSender{}
	pumpCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	kick := make(chan struct{}, 1)
	done := make(chan struct{})
	go func() {
		defer close(done)
		delivery.Pump(pumpCtx, db, map[string]delivery.Sender{"discord": sender},
			slog.Default(), testClock(), kick, time.Hour)
	}()

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) && sender.count() == 0 {
		time.Sleep(5 * time.Millisecond)
	}
	cancel()
	<-done

	if n := sender.count(); n != 1 {
		t.Fatalf("pump sent %d messages, want 1 — the swept notice", n)
	}
	var status string
	var acked sql.NullInt64
	if err := db.QueryRow(
		`SELECT status, acked FROM deliveries WHERE delivery_key LIKE 'interrupted:discord:msg:lost:%'`,
	).Scan(&status, &acked); err != nil {
		t.Fatalf("swept notice row: %v", err)
	}
	if status != "sent" || !acked.Valid {
		t.Errorf("(status, acked) = (%q, %v), want ('sent', set)", status, acked)
	}
}

// seedOwnerIdentity enrolls a discord owner so dead-letter notices
// have a DM target to derive.
func seedOwnerIdentity(t *testing.T, db *sql.DB) {
	t.Helper()
	if _, err := db.Exec(
		`INSERT INTO identities (channel, native_id, trust, owner_id) VALUES ('discord', '999', 'owner', 'brian')`,
	); err != nil {
		t.Fatalf("seed owner identity: %v", err)
	}
}

// stageDeadLetter walks an event to 'dead' with its dead_letters row.
func stageDeadLetter(t *testing.T, db *sql.DB, dedup string) store.QueuedEvent {
	t.Helper()
	ctx := context.Background()
	id, _, err := store.InsertEvent(ctx, db, store.Event{
		DedupKey: dedup, ThreadKey: "discord:dm:123", Kind: "message", Trust: "owner",
		Payload:  `{"dedup_key":"` + dedup + `","thread_key":"discord:dm:123","kind":"message","trust":"owner"}`,
		Received: 1700000000,
	})
	if err != nil {
		t.Fatalf("InsertEvent: %v", err)
	}
	if err := store.MarkEventProcessing(ctx, db, id, 1700000050); err != nil {
		t.Fatalf("MarkEventProcessing: %v", err)
	}
	if err := store.DeadLetterEvent(ctx, db, id, "retries-exhausted", 1700000100); err != nil {
		t.Fatalf("DeadLetterEvent: %v", err)
	}
	return store.QueuedEvent{ID: id, DedupKey: dedup, ThreadKey: "discord:dm:123"}
}

// TestSurfaceDeadLetter: the §4.6 entry notification — one owner-DM
// through the outbox per DEATH, keyed dead:<dedup>:<entries> (fresh
// notice per re-entry, same-death duplicates collapse), naming the
// reason and the manual drain commands. Target derives from the
// enrolled owner identity; no owner enrolled is an error the sweep
// retries, never a silent skip.
func TestSurfaceDeadLetter(t *testing.T) {
	db := openStore(t)
	ctx := context.Background()

	ev := stageDeadLetter(t, db, "discord:msg:doomed")
	if err := delivery.SurfaceDeadLetter(ctx, db, ev); err == nil {
		t.Fatal("SurfaceDeadLetter with no enrolled owner succeeded, want error — a notice with nowhere to go must say so")
	}
	seedOwnerIdentity(t, db)
	if err := delivery.SurfaceDeadLetter(ctx, db, ev); err != nil {
		t.Fatalf("SurfaceDeadLetter: %v", err)
	}

	var key, target, payload string
	if err := db.QueryRow(
		`SELECT delivery_key, target, payload FROM deliveries`,
	).Scan(&key, &target, &payload); err != nil {
		t.Fatalf("read notice: %v", err)
	}
	if key != "dead:discord:msg:doomed:1" {
		t.Errorf("delivery_key = %q, want dead:<dedup>:<entries>", key)
	}
	if target != "discord:dm:999" {
		t.Errorf("target = %q, want the enrolled owner's DM", target)
	}
	if !strings.Contains(payload, "retries-exhausted") || !strings.Contains(payload, "dead requeue") {
		t.Errorf("payload %q must name the reason and the drain commands", payload)
	}

	// Same death: collapse.
	if err := delivery.SurfaceDeadLetter(ctx, db, ev); err != nil {
		t.Fatalf("duplicate SurfaceDeadLetter: %v", err)
	}
	var n int
	if err := db.QueryRow(`SELECT count(*) FROM deliveries`).Scan(&n); err != nil {
		t.Fatalf("count: %v", err)
	}
	if n != 1 {
		t.Fatalf("found %d notices, want 1 — same-death surfaces collapse", n)
	}

	// Second death after a requeue: fresh notice.
	if _, err := store.ResolveDeadLetterRequeue(ctx, db, ev.ID, 1700000200); err != nil {
		t.Fatalf("requeue: %v", err)
	}
	if err := store.MarkEventProcessing(ctx, db, ev.ID, 1700000250); err != nil {
		t.Fatalf("re-stamp: %v", err)
	}
	if err := store.DeadLetterEvent(ctx, db, ev.ID, "retries-exhausted", 1700000300); err != nil {
		t.Fatalf("second death: %v", err)
	}
	if err := delivery.SurfaceDeadLetter(ctx, db, ev); err != nil {
		t.Fatalf("SurfaceDeadLetter (death 2): %v", err)
	}
	if err := db.QueryRow(`SELECT count(*) FROM deliveries`).Scan(&n); err != nil {
		t.Fatalf("count: %v", err)
	}
	if n != 2 {
		t.Errorf("found %d notices after a second death, want 2 — a re-death must not be silenced", n)
	}
}

// TestPumpSurfacesUnsurfacedDeadLetters: same self-repair as parks — a
// death whose entry notice never landed is composed by the pump's
// sweep, so no dead letter waits for a heartbeat that doesn't exist
// yet to become visible.
func TestPumpSurfacesUnsurfacedDeadLetters(t *testing.T) {
	db := openStore(t)
	seedOwnerIdentity(t, db)
	stageDeadLetter(t, db, "discord:msg:silent")

	sender := &fakeSender{}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan struct{})
	go func() {
		defer close(done)
		delivery.Pump(ctx, db, map[string]delivery.Sender{"discord": sender},
			slog.Default(), testClock(), make(chan struct{}), time.Hour)
	}()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) && sender.count() == 0 {
		time.Sleep(5 * time.Millisecond)
	}
	cancel()
	<-done

	if n := sender.count(); n != 1 {
		t.Fatalf("pump sent %d messages, want the swept dead-letter notice", n)
	}
	var status string
	if err := db.QueryRow(
		`SELECT status FROM deliveries WHERE delivery_key = 'dead:discord:msg:silent:1'`,
	).Scan(&status); err != nil {
		t.Fatalf("swept notice: %v", err)
	}
	if status != "sent" {
		t.Errorf("status = %q, want 'sent'", status)
	}
}
