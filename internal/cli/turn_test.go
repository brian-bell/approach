package cli

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/brian-bell/approach/internal/delivery"
	"github.com/brian-bell/approach/internal/session"
	"github.com/brian-bell/approach/internal/store"
)

func discardLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// fakeTurnRunner stands in for the session manager: it records the
// request, streams the scripted deltas through the request's Output
// sink, and returns the scripted error.
type fakeTurnRunner struct {
	mu     sync.Mutex
	reqs   []session.TurnRequest
	deltas []string
	err    error
	hook   func() // runs inside Turn — fault injection between dispatch and finish
}

func (f *fakeTurnRunner) Turn(_ context.Context, req session.TurnRequest) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.reqs = append(f.reqs, req)
	if req.Output != nil {
		for _, d := range f.deltas {
			req.Output(d)
		}
	}
	if f.hook != nil {
		f.hook()
	}
	return f.err
}

// fakeTurnRelay records the streaming traffic one turn sent it.
type fakeTurnRelay struct {
	mu        sync.Mutex
	pushes    []string
	finished  bool
	cancelled bool
	retracted bool
	posted    bool
	acks      []string
	finishErr error
}

func (r *fakeTurnRelay) Posted() bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.posted
}

func (r *fakeTurnRelay) Retract() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.retracted = true
}

func (r *fakeTurnRelay) Push(delta string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.pushes = append(r.pushes, delta)
}

func (r *fakeTurnRelay) FinishJournaled(beforeSend func(chunkIndex int) error) ([]string, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.finished = true
	attempts := len(r.acks)
	if r.finishErr != nil {
		attempts++ // the operation after the accepted prefix started and failed
	}
	for i := 0; i < attempts; i++ {
		if err := beforeSend(i); err != nil {
			accepted := min(i, len(r.acks))
			return r.acks[:accepted], err
		}
	}
	return r.acks, r.finishErr
}

func (r *fakeTurnRelay) Cancel() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.cancelled = true
}

// dispatchFixture wires a production handler over a real store with
// every seam faked, and stages one processing event — exactly the
// state the router hands a handler.
type dispatchFixture struct {
	db      *sql.DB
	runner  *fakeTurnRunner
	relay   *fakeTurnRelay
	notify  int
	readmit []store.QueuedEvent
	deps    turnDeps
	ev      store.QueuedEvent
}

func newDispatchFixture(t *testing.T, runner *fakeTurnRunner, relay *fakeTurnRelay) *dispatchFixture {
	t.Helper()
	f := &dispatchFixture{db: testStore(t), runner: runner, relay: relay}
	f.deps = turnDeps{
		db:     f.db,
		runner: runner,
		notify: func() { f.notify++ },
		readmit: func(ev store.QueuedEvent) {
			f.readmit = append(f.readmit, ev)
		},
		after:  func(_ time.Duration, fn func()) { fn() },
		cwd:    t.TempDir(),
		logger: discardLogger(),
		now:    func() time.Time { return time.Unix(1700000100, 0) },
	}
	if relay != nil {
		f.deps.relay = func(_ context.Context, _ string) turnRelay { return relay }
	}
	f.ev = stageProcessingEvent(t, f.db, "discord:msg:1", "hi there")
	return f
}

// stageProcessingEvent inserts one message event and stamps it
// processing — the state the router guarantees a handler sees.
func stageProcessingEvent(t *testing.T, db *sql.DB, dedup, text string) store.QueuedEvent {
	t.Helper()
	payload := fmt.Sprintf(
		`{"dedup_key":%q,"thread_key":"discord:dm:a","kind":"message","trust":"owner","text":%q}`,
		dedup, text)
	id, inserted, err := store.InsertEvent(context.Background(), db, store.Event{
		DedupKey:  dedup,
		ThreadKey: "discord:dm:a",
		Kind:      "message",
		Trust:     "owner",
		Payload:   payload,
		Received:  1700000000,
	})
	if err != nil || !inserted {
		t.Fatalf("InsertEvent: inserted=%v err=%v", inserted, err)
	}
	if err := store.MarkEventProcessing(context.Background(), db, id, 1700000050); err != nil {
		t.Fatalf("MarkEventProcessing: %v", err)
	}
	return store.QueuedEvent{
		ID: id, DedupKey: dedup, ThreadKey: "discord:dm:a", Kind: "message",
		Trust: "owner", Payload: payload, Status: "processing", Received: 1700000000,
	}
}

func eventStatus(t *testing.T, db *sql.DB, id int64) string {
	t.Helper()
	var status string
	if err := db.QueryRow(`SELECT status FROM events WHERE id = ?`, id).Scan(&status); err != nil {
		t.Fatalf("read event status: %v", err)
	}
	return status
}

type deliveryRow struct {
	Key     string
	Payload string
	Acked   bool
	Att     int64
}

func deliveryRows(t *testing.T, db *sql.DB, eventID int64) []deliveryRow {
	t.Helper()
	rows, err := db.Query(
		`SELECT delivery_key, payload, acked IS NOT NULL, attempts FROM deliveries WHERE event_id = ? ORDER BY id`, eventID)
	if err != nil {
		t.Fatalf("scan deliveries: %v", err)
	}
	defer func() { _ = rows.Close() }()
	var out []deliveryRow
	for rows.Next() {
		var d deliveryRow
		if err := rows.Scan(&d.Key, &d.Payload, &d.Acked, &d.Att); err != nil {
			t.Fatalf("scan delivery: %v", err)
		}
		out = append(out, d)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("scan deliveries: %v", err)
	}
	return out
}

// TestProductionTurnDeliversReply: the happy path end to end — the
// turn's streamed text reaches the relay (with paragraph separators
// between deltas), the full reply is composed into the outbox BEFORE
// the send (write-before-send, §4.1), the event completes, and the
// relay's acks advance it to replied.
func TestProductionTurnDeliversReply(t *testing.T) {
	runner := &fakeTurnRunner{deltas: []string{"hello", "world"}}
	relay := &fakeTurnRelay{acks: []string{"discord:msg:900"}}
	f := newDispatchFixture(t, runner, relay)

	productionTurn(f.deps)(context.Background(), f.ev)

	// The runner saw the event translated per the §6 contract.
	if len(runner.reqs) != 1 {
		t.Fatalf("runner ran %d turns, want 1", len(runner.reqs))
	}
	req := runner.reqs[0]
	if req.ThreadKey != "discord:dm:a" || req.TrustFloor != "owner" ||
		req.Cwd != f.deps.cwd || req.Kind != "message" || req.Prompt != "hi there" {
		t.Errorf("turn request = %+v, want event fields threaded through", req)
	}

	// The relay streamed the deltas with a separator between them.
	if got := strings.Join(relay.pushes, "|"); got != "hello|\n\n|world" {
		t.Errorf("relay pushes = %q, want hello|\\n\\n|world", got)
	}
	if !relay.finished || relay.cancelled {
		t.Errorf("relay finished=%v cancelled=%v, want finished, not cancelled", relay.finished, relay.cancelled)
	}

	rows := deliveryRows(t, f.db, f.ev.ID)
	if len(rows) != 1 {
		t.Fatalf("deliveries = %+v, want exactly 1", rows)
	}
	if rows[0].Key != "reply:discord:msg:1:0" || rows[0].Payload != "hello\n\nworld" {
		t.Errorf("delivery = %+v, want reply:discord:msg:1:0 carrying the full text", rows[0])
	}
	if !rows[0].Acked || rows[0].Att != 1 {
		t.Errorf("delivery acked=%v attempts=%d, want acked after exactly one stamped attempt", rows[0].Acked, rows[0].Att)
	}
	if got := eventStatus(t, f.db, f.ev.ID); got != "replied" {
		t.Errorf("event status = %q, want replied (§4.1: ack advances completed → replied)", got)
	}
}

// TestProductionTurnChunksLongReply: a reply past the platform cap
// composes one outbox row per chunk, and each returned ack settles its
// own row — acks and rows must align one-to-one.
func TestProductionTurnChunksLongReply(t *testing.T) {
	long := strings.Repeat("x", 2000) + "tail"
	runner := &fakeTurnRunner{deltas: []string{long}}
	relay := &fakeTurnRelay{acks: []string{"discord:msg:900", "discord:msg:901"}}
	f := newDispatchFixture(t, runner, relay)

	productionTurn(f.deps)(context.Background(), f.ev)

	rows := deliveryRows(t, f.db, f.ev.ID)
	if len(rows) != 2 {
		t.Fatalf("deliveries = %d rows, want 2 chunks", len(rows))
	}
	if rows[0].Payload != strings.Repeat("x", 2000) || rows[1].Payload != "tail" {
		t.Errorf("chunk payloads wrong: %q / %q", rows[0].Payload[:10]+"…", rows[1].Payload)
	}
	if !rows[0].Acked || !rows[1].Acked {
		t.Errorf("acked = %v/%v, want both", rows[0].Acked, rows[1].Acked)
	}
	if got := eventStatus(t, f.db, f.ev.ID); got != "replied" {
		t.Errorf("event status = %q, want replied", got)
	}
}

// TestProductionTurnPartialFinishLeavesRemainderOwed: the relay
// delivered only the first chunk before failing — its ack settles its
// row, the rest stays durably owed for the pump, and the event RESTS
// at completed (replied asserts every sibling acked).
func TestProductionTurnPartialFinishLeavesRemainderOwed(t *testing.T) {
	long := strings.Repeat("x", 2000) + "tail"
	runner := &fakeTurnRunner{deltas: []string{long}}
	relay := &fakeTurnRelay{acks: []string{"discord:msg:900"}, finishErr: errors.New("platform hiccup")}
	f := newDispatchFixture(t, runner, relay)

	productionTurn(f.deps)(context.Background(), f.ev)

	rows := deliveryRows(t, f.db, f.ev.ID)
	if len(rows) != 2 {
		t.Fatalf("deliveries = %d rows, want 2", len(rows))
	}
	if !rows[0].Acked || rows[1].Acked {
		t.Errorf("acked = %v/%v, want first settled, second owed", rows[0].Acked, rows[1].Acked)
	}
	if got := eventStatus(t, f.db, f.ev.ID); got != "completed" {
		t.Errorf("event status = %q, want completed while a chunk is owed", got)
	}
	if f.notify == 0 {
		t.Error("pump not kicked — the owed remainder would wait for the ticker")
	}
	if relay.retracted {
		t.Error("relay retracted after a delivered first chunk — that would delete platform-accepted content")
	}
}

// TestProductionTurnZeroAckFinishRetractsPartial: Finish delivered
// NOTHING — every composed row is owed and the pump will re-send the
// whole reply from chunk 0, so a posted partial must come down or the
// text shows twice. (Contrast: with any ack, chunk 0 is delivered
// content and must stand — see the partial-finish test above.)
func TestProductionTurnZeroAckFinishRetractsPartial(t *testing.T) {
	runner := &fakeTurnRunner{deltas: []string{"hello"}}
	relay := &fakeTurnRelay{finishErr: errors.New("platform down")}
	f := newDispatchFixture(t, runner, relay)

	productionTurn(f.deps)(context.Background(), f.ev)

	if !relay.retracted {
		t.Error("relay not retracted after a zero-ack Finish — the pump's re-send would duplicate the partial")
	}
	rows := deliveryRows(t, f.db, f.ev.ID)
	if len(rows) != 1 || rows[0].Acked {
		t.Fatalf("deliveries = %+v, want 1 owed row for the pump", rows)
	}
	if got := eventStatus(t, f.db, f.ev.ID); got != "completed" {
		t.Errorf("event status = %q, want completed", got)
	}
	if f.notify == 0 {
		t.Error("pump not kicked for the owed reply")
	}
}

// TestProductionTurnFailedFirstChunkDoesNotStampLaterChunks: a
// multi-chunk direct reply whose first platform operation fails has
// started exactly one delivery attempt. Later chunks were never
// touched, so their recovery journal and retry budget must remain
// pristine (§4.6).
func TestProductionTurnFailedFirstChunkDoesNotStampLaterChunks(t *testing.T) {
	long := strings.Repeat("x", 2000) + "tail"
	runner := &fakeTurnRunner{deltas: []string{long}}
	relay := &fakeTurnRelay{finishErr: errors.New("platform down")}
	f := newDispatchFixture(t, runner, relay)

	productionTurn(f.deps)(context.Background(), f.ev)

	rows := deliveryRows(t, f.db, f.ev.ID)
	if len(rows) != 2 {
		t.Fatalf("deliveries = %+v, want two chunks", rows)
	}
	if rows[0].Att != 1 || rows[1].Att != 0 {
		t.Errorf("attempts = %d/%d, want 1/0 — only the first platform operation started", rows[0].Att, rows[1].Att)
	}
}

// TestProductionTurnEmptyReplyCompletes: a turn that produced no text
// completes the event with nothing composed — completed is the rest
// state, and faking a reply would be dishonest.
func TestProductionTurnEmptyReplyCompletes(t *testing.T) {
	runner := &fakeTurnRunner{}
	relay := &fakeTurnRelay{}
	f := newDispatchFixture(t, runner, relay)

	productionTurn(f.deps)(context.Background(), f.ev)

	if rows := deliveryRows(t, f.db, f.ev.ID); len(rows) != 0 {
		t.Errorf("deliveries = %+v, want none for an empty reply", rows)
	}
	if got := eventStatus(t, f.db, f.ev.ID); got != "completed" {
		t.Errorf("event status = %q, want completed", got)
	}
	if relay.finished {
		t.Error("relay finished an empty turn — nothing should have been sent")
	}
	if !relay.cancelled {
		t.Error("relay not cancelled — its typing indicator would outlive the turn")
	}
}

// TestProductionTurnNoRelayLeavesOwed: no adapter is up (or none ever
// will be) — the reply still composes durably and the pump is kicked;
// nothing is silently dropped (§4.1).
func TestProductionTurnNoRelayLeavesOwed(t *testing.T) {
	runner := &fakeTurnRunner{deltas: []string{"hello"}}
	f := newDispatchFixture(t, runner, nil)

	productionTurn(f.deps)(context.Background(), f.ev)

	rows := deliveryRows(t, f.db, f.ev.ID)
	if len(rows) != 1 || rows[0].Acked {
		t.Fatalf("deliveries = %+v, want 1 owed row", rows)
	}
	if got := eventStatus(t, f.db, f.ev.ID); got != "completed" {
		t.Errorf("event status = %q, want completed (reply owed, not acked)", got)
	}
	if f.notify == 0 {
		t.Error("pump not kicked for the owed reply")
	}
}

// TestProductionTurnEngineFailureRequeues: a failed turn with a clean
// journal takes the §4.6 auto-retry — durably received again, budget
// spent, readmitted after backoff — and the relay is cancelled, never
// finished.
func TestProductionTurnEngineFailureRequeues(t *testing.T) {
	runner := &fakeTurnRunner{err: errors.New("engine: turn for x killed after 10m")}
	relay := &fakeTurnRelay{}
	f := newDispatchFixture(t, runner, relay)

	productionTurn(f.deps)(context.Background(), f.ev)

	if got := eventStatus(t, f.db, f.ev.ID); got != "received" {
		t.Fatalf("event status = %q, want received (§4.6 clean retry)", got)
	}
	var attempts int64
	if err := f.db.QueryRow(`SELECT attempts FROM events WHERE id = ?`, f.ev.ID).Scan(&attempts); err != nil {
		t.Fatalf("read attempts: %v", err)
	}
	if attempts != 1 {
		t.Errorf("attempts = %d, want 1 budget unit spent", attempts)
	}
	if len(f.readmit) != 1 || f.readmit[0].ID != f.ev.ID {
		t.Errorf("readmitted %+v, want the event back in its thread queue", f.readmit)
	}
	if relay.finished || !relay.cancelled {
		t.Errorf("relay finished=%v cancelled=%v, want cancelled only", relay.finished, relay.cancelled)
	}
	if rows := deliveryRows(t, f.db, f.ev.ID); len(rows) != 0 {
		t.Errorf("deliveries = %+v, want none from a failed turn", rows)
	}
}

// TestProductionTurnShutdownLeavesProcessing: cancellation is "shut
// down gracefully", not this event's failure (router contract) — no
// recovery runs, the row stays processing, and the next boot parks it
// as interrupted (§4.6).
func TestProductionTurnShutdownLeavesProcessing(t *testing.T) {
	runner := &fakeTurnRunner{err: context.Canceled}
	relay := &fakeTurnRelay{}
	f := newDispatchFixture(t, runner, relay)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	productionTurn(f.deps)(ctx, f.ev)

	if got := eventStatus(t, f.db, f.ev.ID); got != "processing" {
		t.Errorf("event status = %q, want processing (restart recovery owns it)", got)
	}
	var attempts int64
	if err := f.db.QueryRow(`SELECT attempts FROM events WHERE id = ?`, f.ev.ID).Scan(&attempts); err != nil {
		t.Fatalf("read attempts: %v", err)
	}
	if attempts != 0 {
		t.Errorf("attempts = %d — shutdown must not spend §4.6 retry budget", attempts)
	}
}

// TestProductionTurnMalformedPayloadDeadLetters: a payload that cannot
// parse can never run — no retry can fix it, so it dead-letters with
// reason malformed and the owner is notified (§4.6).
func TestProductionTurnMalformedPayloadDeadLetters(t *testing.T) {
	runner := &fakeTurnRunner{}
	f := newDispatchFixture(t, runner, nil)
	seedTestIdentities(t, f.db) // the death notice needs an enrolled owner
	if _, err := f.db.Exec(`UPDATE events SET payload = 'not json' WHERE id = ?`, f.ev.ID); err != nil {
		t.Fatalf("corrupt payload: %v", err)
	}
	f.ev.Payload = "not json"

	productionTurn(f.deps)(context.Background(), f.ev)

	if len(runner.reqs) != 0 {
		t.Error("a malformed event must never reach the engine")
	}
	if got := eventStatus(t, f.db, f.ev.ID); got != "dead" {
		t.Fatalf("event status = %q, want dead", got)
	}
	var reason string
	if err := f.db.QueryRow(`SELECT reason FROM dead_letters WHERE event_id = ?`, f.ev.ID).Scan(&reason); err != nil {
		t.Fatalf("read dead letter: %v", err)
	}
	if reason != "malformed" {
		t.Errorf("dead letter reason = %q, want malformed", reason)
	}
	var notices int
	if err := f.db.QueryRow(`SELECT count(*) FROM deliveries WHERE delivery_key LIKE 'dead:%'`).Scan(&notices); err != nil {
		t.Fatalf("count notices: %v", err)
	}
	if notices != 1 || f.notify == 0 {
		t.Errorf("death notice count=%d notify=%d, want the owner told now", notices, f.notify)
	}
}

// TestProductionTurnDefersToPumpWhenOlderRowOwed: an OLDER delivery is
// still owed to this thread (a prior turn's unsent reply, a park
// notice) — even a live partial would jump the queue and become
// visible out of order (§4.1). The relay therefore stays disabled for
// the whole turn; the reply composes durably for the ordered pump.
func TestProductionTurnDefersToPumpWhenOlderRowOwed(t *testing.T) {
	runner := &fakeTurnRunner{deltas: []string{"hello"}}
	relay := &fakeTurnRelay{acks: []string{"discord:msg:900"}}
	f := newDispatchFixture(t, runner, relay)
	if _, inserted, err := store.InsertDelivery(context.Background(), f.db, store.Delivery{
		DeliveryKey: "notice:older", Target: "discord:dm:a", Payload: "an older owed message",
	}); err != nil || !inserted {
		t.Fatalf("stage older owed row: inserted=%v err=%v", inserted, err)
	}

	productionTurn(f.deps)(context.Background(), f.ev)

	if len(relay.pushes) != 0 || relay.finished || relay.retracted {
		t.Errorf("relay pushes=%q finished=%v retracted=%v — backlog must suppress live output before it becomes visible",
			relay.pushes, relay.finished, relay.retracted)
	}
	rows := deliveryRows(t, f.db, f.ev.ID)
	if len(rows) != 1 || rows[0].Acked || rows[0].Att != 0 {
		t.Fatalf("reply rows = %+v, want 1 composed row left for the pump", rows)
	}
	if got := eventStatus(t, f.db, f.ev.ID); got != "completed" {
		t.Errorf("event status = %q, want completed", got)
	}
	if f.notify == 0 {
		t.Error("pump not kicked — both owed rows would wait for the ticker")
	}
}

// TestProductionTurnReservesRelayTargetThroughComposition: after the
// relay passes its backlog check, cross-thread recovery may target the
// same owner DM. The live turn holds the shared target gate through
// reply composition/send so that notice waits and receives a later
// outbox id; visible partial order and durable order cannot diverge.
func TestProductionTurnReservesRelayTargetThroughComposition(t *testing.T) {
	runner := &fakeTurnRunner{deltas: []string{"hello"}}
	relay := &fakeTurnRelay{acks: []string{"discord:msg:900"}}
	f := newDispatchFixture(t, runner, relay)
	f.deps.inflight = delivery.NewInFlight()
	noticeEv := stageProcessingEvent(t, f.db, "discord:msg:notice", "other turn")
	if err := store.ParkEvent(context.Background(), f.db, noticeEv.ID, 1700000060); err != nil {
		t.Fatalf("ParkEvent notice source: %v", err)
	}
	surfaceDone := make(chan error, 1)
	runner.hook = func() {
		go func() {
			surfaceDone <- delivery.SurfaceInterruptedCoordinated(context.Background(), f.db, noticeEv, f.deps.inflight)
		}()
		time.Sleep(20 * time.Millisecond)
		var notices int
		if err := f.db.QueryRow(`SELECT count(*) FROM deliveries WHERE delivery_key LIKE 'interrupted:discord:msg:notice:%'`).Scan(&notices); err != nil {
			t.Errorf("count concurrent notices: %v", err)
		} else if notices != 0 {
			t.Errorf("notice composed during live turn: %d rows — target reservation did not span the engine", notices)
		}
	}

	productionTurn(f.deps)(context.Background(), f.ev)
	select {
	case err := <-surfaceDone:
		if err != nil {
			t.Fatalf("SurfaceInterruptedCoordinated: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("concurrent notice did not compose after live turn released target")
	}
	var replyID, noticeID int64
	if err := f.db.QueryRow(`SELECT id FROM deliveries WHERE delivery_key = 'reply:discord:msg:1:0'`).Scan(&replyID); err != nil {
		t.Fatalf("read reply id: %v", err)
	}
	if err := f.db.QueryRow(`SELECT id FROM deliveries WHERE delivery_key LIKE 'interrupted:discord:msg:notice:%'`).Scan(&noticeID); err != nil {
		t.Fatalf("read notice id: %v", err)
	}
	if replyID >= noticeID {
		t.Errorf("delivery order reply=%d notice=%d, want live reply composed first", replyID, noticeID)
	}
}

// TestProductionTurnComposeFailureParks: the turn ran but its reply
// could not be composed into the outbox — completing anyway would be a
// silent drop wearing a success stamp (completed events never dispatch
// again). The event parks instead: durable, human-visible, retriable
// against a clean outbox (§4.6).
func TestProductionTurnComposeFailureParks(t *testing.T) {
	runner := &fakeTurnRunner{deltas: []string{"a reply that cannot land"}}
	relay := &fakeTurnRelay{}
	f := newDispatchFixture(t, runner, relay)
	// Blunt fault injection: every deliveries write fails from here on.
	if _, err := f.db.Exec(`DROP TABLE deliveries`); err != nil {
		t.Fatalf("drop deliveries: %v", err)
	}

	productionTurn(f.deps)(context.Background(), f.ev)

	if got := eventStatus(t, f.db, f.ev.ID); got != "interrupted" {
		t.Fatalf("event status = %q, want interrupted — the reply must not be silently lost", got)
	}
	if len(f.readmit) != 0 {
		t.Errorf("readmitted %+v — a parked event must never auto-retry", f.readmit)
	}
	if len(relay.pushes) != 0 || relay.finished || relay.cancelled || relay.retracted {
		t.Errorf("relay changed after backlog check failed: %+v — failure must suppress live output", relay)
	}
}

// TestProductionTurnCompletionStampFailureKeepsRowsFenced: the
// completion stamp failed after the reply composed — the rows must
// stay fenced off the pump for the rest of this life, because
// AckDelivery refuses acks for a pre-completion event and every pump
// pass would otherwise re-send the same reply and roll the ack back
// (one visible duplicate per tick). Restart recovery owns it from
// here: in-memory claims die with the process, the next boot parks the
// event, and the pump then delivers exactly once.
func TestProductionTurnCompletionStampFailureKeepsRowsFenced(t *testing.T) {
	runner := &fakeTurnRunner{deltas: []string{"hello"}}
	relay := &fakeTurnRelay{acks: []string{"discord:msg:900"}}
	f := newDispatchFixture(t, runner, relay)
	inflight := delivery.NewInFlight()
	f.deps.inflight = inflight
	// Fault injection: the event leaves 'processing' mid-turn (as a
	// store failure would), so MarkEventCompleted's guard refuses.
	runner.hook = func() {
		if _, err := f.db.Exec(`UPDATE events SET status = 'interrupted' WHERE id = ?`, f.ev.ID); err != nil {
			t.Errorf("fault injection: %v", err)
		}
	}

	productionTurn(f.deps)(context.Background(), f.ev)

	if !inflight.Held("reply:discord:msg:1:0") {
		t.Error("claim released while the event never completed — the pump would duplicate the reply every pass")
	}
	if relay.finished || !relay.retracted {
		t.Errorf("relay finished=%v retracted=%v, want retracted only — nothing may send this life", relay.finished, relay.retracted)
	}
	rows := deliveryRows(t, f.db, f.ev.ID)
	if len(rows) != 1 || rows[0].Acked || rows[0].Att != 0 {
		t.Fatalf("deliveries = %+v, want 1 untouched owed row for the next life", rows)
	}
}

// TestProductionTurnRefusesSubOwnerEvents: the pre-C9 admission gate
// (§7, fail closed) — with no policy hook and no sandbox, an engine
// turn carries owner-grade capability, so a sub-owner event must never
// reach the engine. It lands 'skipped': consumed on purpose, no turn,
// no reply, and no owner-facing notice a stranger could flood.
func TestProductionTurnRefusesSubOwnerEvents(t *testing.T) {
	for _, tc := range []string{"untrusted", "known"} {
		runner := &fakeTurnRunner{deltas: []string{"must never be seen"}}
		relay := &fakeTurnRelay{}
		f := newDispatchFixture(t, runner, relay)
		if _, err := f.db.Exec(`UPDATE events SET trust = ? WHERE id = ?`, tc, f.ev.ID); err != nil {
			t.Fatalf("restamp trust: %v", err)
		}
		f.ev.Trust = tc

		productionTurn(f.deps)(context.Background(), f.ev)

		if len(runner.reqs) != 0 {
			t.Errorf("%s: a sub-owner event reached the engine (§7 fail closed)", tc)
		}
		if got := eventStatus(t, f.db, f.ev.ID); got != "skipped" {
			t.Errorf("%s: event status = %q, want skipped", tc, got)
		}
		if rows := deliveryRows(t, f.db, f.ev.ID); len(rows) != 0 {
			t.Errorf("%s: deliveries = %+v, want none", tc, rows)
		}
		var notices int
		if err := f.db.QueryRow(`SELECT count(*) FROM deliveries`).Scan(&notices); err != nil {
			t.Fatalf("count deliveries: %v", err)
		}
		if notices != 0 {
			t.Errorf("%s: %d notices composed — a refused stranger must not generate owner-facing traffic", tc, notices)
		}
	}
}

// TestProductionTurnFailureAfterVisiblePartialParks: the engine failed
// AFTER the relay put a partial message on the thread — a visible
// outbound side effect the tool journal knows nothing about. The event
// must park (interrupted + §4.6 notice), never auto-retry: a retry
// would leave the abandoned fragment standing and answer twice.
func TestProductionTurnFailureAfterVisiblePartialParks(t *testing.T) {
	runner := &fakeTurnRunner{deltas: []string{"half an ans"}, err: errors.New("engine: turn killed")}
	relay := &fakeTurnRelay{posted: true}
	f := newDispatchFixture(t, runner, relay)

	productionTurn(f.deps)(context.Background(), f.ev)

	if got := eventStatus(t, f.db, f.ev.ID); got != "interrupted" {
		t.Fatalf("event status = %q, want interrupted (§4.6 park on visible partial)", got)
	}
	var attempts int64
	if err := f.db.QueryRow(`SELECT attempts FROM events WHERE id = ?`, f.ev.ID).Scan(&attempts); err != nil {
		t.Fatalf("read attempts: %v", err)
	}
	if attempts != 0 {
		t.Errorf("attempts = %d — a park must not spend §4.6 retry budget", attempts)
	}
	if len(f.readmit) != 0 {
		t.Errorf("readmitted %+v — a parked event must never auto-retry", f.readmit)
	}
	var notices int
	if err := f.db.QueryRow(`SELECT count(*) FROM deliveries WHERE delivery_key LIKE 'interrupted:%'`).Scan(&notices); err != nil {
		t.Fatalf("count notices: %v", err)
	}
	if notices != 1 || f.notify == 0 {
		t.Errorf("park notice count=%d notify=%d, want the thread told now (§4.6)", notices, f.notify)
	}
	if relay.finished || !relay.cancelled {
		t.Errorf("relay finished=%v cancelled=%v, want cancelled only", relay.finished, relay.cancelled)
	}
}

// TestProductionTurnClaimsFenceThePump: while the direct send is in
// flight its delivery keys are claimed — a concurrent pump pass must
// skip them (no duplicate send) — and released by the time the handler
// returns, so the pump can drain anything left owed.
func TestProductionTurnClaimsFenceThePump(t *testing.T) {
	runner := &fakeTurnRunner{deltas: []string{"hello"}}
	relay := &fakeTurnRelay{acks: []string{"discord:msg:900"}}
	f := newDispatchFixture(t, runner, relay)
	inflight := delivery.NewInFlight()
	f.deps.inflight = inflight

	var heldDuringFinish bool
	f.deps.relay = func(_ context.Context, _ string) turnRelay {
		return &hookedRelay{fakeTurnRelay: relay, onFinish: func() {
			heldDuringFinish = inflight.Held("reply:discord:msg:1:0")
		}}
	}

	productionTurn(f.deps)(context.Background(), f.ev)

	if !heldDuringFinish {
		t.Error("delivery key not claimed during Finish — a pump pass could send it concurrently")
	}
	if inflight.Held("reply:discord:msg:1:0") {
		t.Error("claim not released after the handler returned — the pump could never drain an owed remainder")
	}
	if got := eventStatus(t, f.db, f.ev.ID); got != "replied" {
		t.Errorf("event status = %q, want replied", got)
	}
}

// hookedRelay wraps the fake to observe state at the moment of Finish.
type hookedRelay struct {
	*fakeTurnRelay
	onFinish func()
}

func (h *hookedRelay) FinishJournaled(beforeSend func(chunkIndex int) error) ([]string, error) {
	h.onFinish()
	return h.fakeTurnRelay.FinishJournaled(beforeSend)
}

// TestProductionTurnDuplicateComposeDefersToPump: a prior life already
// composed this reply (crash between compose and completion) — that
// owed row suppresses the relay before the turn, the first write wins,
// and the pump owns whatever is still owed.
func TestProductionTurnDuplicateComposeDefersToPump(t *testing.T) {
	runner := &fakeTurnRunner{deltas: []string{"hello"}}
	relay := &fakeTurnRelay{acks: []string{"discord:msg:900"}}
	f := newDispatchFixture(t, runner, relay)
	if _, inserted, err := store.InsertDelivery(context.Background(), f.db, store.Delivery{
		DeliveryKey: "reply:discord:msg:1:0",
		EventID:     f.ev.ID,
		Target:      "discord:dm:a",
		Payload:     "an earlier life's words",
	}); err != nil || !inserted {
		t.Fatalf("pre-compose: inserted=%v err=%v", inserted, err)
	}

	productionTurn(f.deps)(context.Background(), f.ev)

	rows := deliveryRows(t, f.db, f.ev.ID)
	if len(rows) != 1 || rows[0].Payload != "an earlier life's words" {
		t.Fatalf("deliveries = %+v, want the original row untouched", rows)
	}
	if rows[0].Acked {
		t.Error("row acked without a send — the pump owns it")
	}
	if len(relay.pushes) != 0 || relay.finished || relay.cancelled || relay.retracted {
		t.Errorf("relay changed despite pre-existing owed reply: %+v — backlog must suppress it before output", relay)
	}
	if got := eventStatus(t, f.db, f.ev.ID); got != "completed" {
		t.Errorf("event status = %q, want completed", got)
	}
	if f.notify == 0 {
		t.Error("pump not kicked for the owed reply")
	}
}

// TestProductionTurnDuplicateComposeReconcilesAckedRows covers the
// crash-after-compose path after restart recovery has already sent and
// acked the persisted reply while the event was parked. A human retry
// may run the turn again, but duplicate composition must reconcile the
// prior acks after completion; no unacked row remains to call
// AckDelivery and advance the event later.
func TestProductionTurnDuplicateComposeReconcilesAckedRows(t *testing.T) {
	runner := &fakeTurnRunner{deltas: []string{"hello"}}
	relay := &fakeTurnRelay{acks: []string{"discord:msg:900"}}
	f := newDispatchFixture(t, runner, relay)
	id, inserted, err := store.InsertDelivery(context.Background(), f.db, store.Delivery{
		DeliveryKey: "reply:discord:msg:1:0",
		EventID:     f.ev.ID,
		Target:      "discord:dm:a",
		Payload:     "an earlier life's words",
	})
	if err != nil || !inserted {
		t.Fatalf("pre-compose: inserted=%v err=%v", inserted, err)
	}
	if err := store.ParkEvent(context.Background(), f.db, f.ev.ID, 1700000060); err != nil {
		t.Fatalf("ParkEvent: %v", err)
	}
	if err := store.AckDelivery(context.Background(), f.db, id, 1700000070); err != nil {
		t.Fatalf("restart-pump AckDelivery: %v", err)
	}
	requeued, err := store.RequeueInterruptedEvent(context.Background(), f.db, f.ev.ID, 1700000080)
	if err != nil {
		t.Fatalf("RequeueInterruptedEvent: %v", err)
	}
	if err := store.MarkEventProcessing(context.Background(), f.db, f.ev.ID, 1700000090); err != nil {
		t.Fatalf("MarkEventProcessing: %v", err)
	}
	f.ev = requeued
	f.ev.Status = "processing"

	productionTurn(f.deps)(context.Background(), f.ev)

	if got := eventStatus(t, f.db, f.ev.ID); got != "replied" {
		t.Errorf("event status = %q, want replied — all existing reply rows were already acked", got)
	}
	if len(relay.pushes) != 0 || relay.finished || relay.cancelled || relay.retracted {
		t.Errorf("relay changed despite prior acknowledged reply: %+v — a manual retry must not expose duplicate text", relay)
	}
}
