package session_test

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/brian-bell/approach/internal/delivery"
	"github.com/brian-bell/approach/internal/router"
	"github.com/brian-bell/approach/internal/session"
	"github.com/brian-bell/approach/internal/store"
)

// These are the §9 P1 fault drills for the session lifecycle's two
// standing promises: one live session per thread no matter how ingest
// races (§6 one_live_session), and a conversation that survives hours
// of silence plus a daemon restart (the P1 exit criterion). The unit
// tests pin each flow under the per-thread serialization the router
// guarantees; the drills attack the promises from outside it.

// TestDrillSimultaneousFirstMessagesOneSession is the §9 P1 drill for
// the one_live_session backstop: two (here four) first-messages land
// on a new thread SIMULTANEOUSLY — no router serialization, the exact
// interleaving §4.1's queues exist to prevent — and still at most one
// session row is minted, because the partial unique index holds where
// caller discipline didn't. A loser is a loud constraint error or a
// resolve of the winner's row; never a second live session.
func TestDrillSimultaneousFirstMessagesOneSession(t *testing.T) {
	db := mustOpen(t)
	ctx := context.Background()
	cwd := t.TempDir()
	eng := &fakeEngine{}
	m := newManager(db, eng, 1700000000)

	const racers = 4
	type outcome struct {
		live  store.LiveSession
		fresh bool
		err   error
	}
	results := make([]outcome, racers)
	start := make(chan struct{})
	var wg sync.WaitGroup
	for i := range racers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			live, fresh, err := m.Ensure(ctx, "discord:dm:7", "owner", cwd)
			results[i] = outcome{live, fresh, err}
		}()
	}
	close(start)
	wg.Wait()

	// THE invariant: however the race interleaved, the schema admits
	// one live row. Everything below is diagnosis; this is the drill.
	var live int
	if err := db.QueryRow(
		`SELECT count(*) FROM sessions WHERE thread_key = 'discord:dm:7' AND status IN ('creating', 'active')`,
	).Scan(&live); err != nil {
		t.Fatalf("count live sessions: %v", err)
	}
	if live != 1 {
		t.Fatalf("live sessions = %d after %d simultaneous first-messages, want 1 — one_live_session must hold (§6)", live, racers)
	}

	// The thread is not wedged: at least one racer got a usable
	// session, every success agrees on WHICH session, and exactly one
	// racer minted it (fresh) — a second fresh success would be a
	// second insert the index somehow admitted.
	var winner store.LiveSession
	successes, freshes := 0, 0
	for i, r := range results {
		if r.err != nil {
			t.Logf("racer %d refused loud (as a loser must be): %v", i, r.err)
			continue
		}
		successes++
		if r.fresh {
			freshes++
			winner = r.live
		} else if winner.SessionID == "" {
			winner = r.live
		}
		if winner.SessionID != "" && r.live.SessionID != winner.SessionID {
			t.Errorf("racer %d resolved session %s, another got %s — two live identities for one thread", i, r.live.SessionID, winner.SessionID)
		}
	}
	if successes == 0 {
		t.Fatal("every racer errored — the race must leave the thread a usable session, not a wedge")
	}
	if freshes > 1 {
		t.Errorf("%d racers report a fresh mint, want at most 1 — the index admitted a second insert", freshes)
	}

	// The surviving row is real, not a torn artifact: its first turn
	// runs and activates it, still under one live row.
	if err := m.StartNew(ctx, winner); err != nil {
		t.Fatalf("StartNew on the surviving session: %v", err)
	}
	var status string
	if err := db.QueryRow(
		`SELECT status FROM sessions WHERE session_id = ?`, winner.SessionID,
	).Scan(&status); err != nil {
		t.Fatalf("read winner: %v", err)
	}
	if status != "active" {
		t.Errorf("winner status = %q after first turn, want 'active' (§4.1)", status)
	}
}

// drillEngine records Start and Resume specs behind a mutex — drill
// handlers run on router drain goroutines while the test goroutine
// reads the record.
type drillEngine struct {
	mu      sync.Mutex
	starts  []session.Spec
	resumes []session.Spec
}

func (e *drillEngine) Start(_ context.Context, spec session.Spec) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.starts = append(e.starts, spec)
	return nil
}

func (e *drillEngine) Resume(_ context.Context, spec session.Spec) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.resumes = append(e.resumes, spec)
	return nil
}

func (e *drillEngine) record() (starts, resumes []session.Spec) {
	e.mu.Lock()
	defer e.mu.Unlock()
	return append([]session.Spec(nil), e.starts...), append([]session.Spec(nil), e.resumes...)
}

// drillSender records acked sends for the outbox leg of the composed
// turn — same shape as the delivery package's fake, local so the drill
// owns what it asserts against.
type drillSender struct {
	mu   sync.Mutex
	sent []string
}

func (s *drillSender) Send(_ context.Context, _, payload string) (string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.sent = append(s.sent, payload)
	return fmt.Sprintf("ack:%d", len(s.sent)), nil
}

// TestDrillThreadContinuesAfterHoursAndRestart is the P1 exit
// criterion (§9) as a drill: message one runs a first turn and gets
// its reply; the daemon shuts down; three idle hours pass; a NEW
// daemon process-equivalent (fresh store handle, fresh router, fresh
// manager, restart resend pass) receives message two — and the
// conversation CONTINUES: the same pinned session id resumes from the
// same recorded cwd (§4.1), the reply flows through the outbox, and
// the thread still holds exactly one live session. Every life composes
// the real seams the way the daemon wires them; only the engine and
// the platform are fakes.
func TestDrillThreadContinuesAfterHoursAndRestart(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "state", "approach.db")
	cwd := filepath.Join(dir, "repo")
	if err := os.MkdirAll(cwd, 0o755); err != nil {
		t.Fatalf("make repo dir: %v", err)
	}
	const threadKey = "discord:dm:42"

	// The drills own the clock (§6 convention): life 2 starts three
	// hours later — inside the 4h idle TTL, so continuity means RESUME,
	// not rotation.
	var clock atomic.Int64
	clock.Store(1700000000)
	now := func() time.Time { return time.Unix(clock.Load(), 0) }

	open := func() *sql.DB {
		t.Helper()
		db, err := store.Open(dbPath)
		if err != nil {
			t.Fatalf("open store: %v", err)
		}
		return db
	}
	// runTurn is the epic-1.4 turn shape the daemon will wire: resolve
	// the session and run the engine (Turn), then the completion
	// transition and the write-before-send reply compose (§4.1).
	runTurn := func(db *sql.DB, m *session.Manager) router.Handler {
		return func(ctx context.Context, ev store.QueuedEvent) {
			if err := m.Turn(ctx, ev.ThreadKey, "owner", cwd, "message", ev.DedupKey); err != nil {
				t.Errorf("turn for %s: %v", ev.DedupKey, err)
				return
			}
			if _, err := db.Exec(`UPDATE events SET status = 'completed' WHERE id = ?`, ev.ID); err != nil {
				t.Errorf("complete event: %v", err)
				return
			}
			if _, _, err := store.InsertDelivery(ctx, db, store.Delivery{
				DeliveryKey: "reply:" + ev.DedupKey, EventID: ev.ID,
				Target: ev.ThreadKey, Payload: "re: " + ev.DedupKey,
			}); err != nil {
				t.Errorf("compose reply: %v", err)
			}
		}
	}
	eventStatus := func(db *sql.DB, dedup string) string {
		t.Helper()
		var status string
		if err := db.QueryRow(`SELECT status FROM events WHERE dedup_key = ?`, dedup).Scan(&status); err != nil {
			t.Fatalf("read event %s: %v", dedup, err)
		}
		return status
	}
	// One life = rebuild, restart-resend pass, one message end to end.
	life := func(db *sql.DB, eng session.Engine, msg store.Event) *drillSender {
		t.Helper()
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		m := session.NewManager(db, eng, session.Config{Logger: discardLogger(), Now: now})
		q := router.New(ctx, db, router.Options{Handler: runTurn(db, m), Logger: discardLogger()})
		if err := q.Rebuild(ctx); err != nil {
			t.Fatalf("rebuild: %v", err)
		}
		sender := &drillSender{}
		senders := map[string]delivery.Sender{"discord": sender}
		// The pump's start pass — the §4.6 restart resend. Life 1 owes
		// nothing; a later life must drain whatever an earlier crash
		// left, and here proves nothing is double-sent.
		delivery.ResendUnacked(ctx, db, senders, discardLogger(), now)
		if _, err := q.Persist(ctx, msg); err != nil {
			t.Fatalf("persist %s: %v", msg.DedupKey, err)
		}
		// The composed reply row is the turn's LAST write — waiting on
		// it (not on 'completed') keeps the resend pass below from
		// racing the compose.
		waitForDrill(t, func() bool {
			var n int
			if err := db.QueryRow(
				`SELECT count(*) FROM deliveries WHERE delivery_key = ?`, "reply:"+msg.DedupKey,
			).Scan(&n); err != nil {
				t.Fatalf("count composed replies: %v", err)
			}
			return n == 1
		}, msg.DedupKey+" turn to complete and compose its reply")
		// The reply leg: send from the outbox, ack advances the event
		// completed → replied (§4.1).
		delivery.ResendUnacked(ctx, db, senders, discardLogger(), now)
		if got := eventStatus(db, msg.DedupKey); got != "replied" {
			t.Errorf("event %s status = %q, want 'replied' — the reply rides the ack (§4.1)", msg.DedupKey, got)
		}
		// Orderly shutdown, exactly like the daemon: stop dispatch,
		// wait for in-flight turns, only then may the caller close.
		cancel()
		q.Wait()
		return sender
	}
	message := func(n int64) store.Event {
		return store.Event{
			DedupKey: fmt.Sprintf("discord:msg:%d", n), ThreadKey: threadKey,
			Kind: "message", Trust: "owner",
			Payload: fmt.Sprintf(
				`{"dedup_key":"discord:msg:%d","thread_key":"%s","kind":"message","trust":"owner"}`, n, threadKey),
			Received: clock.Load(),
		}
	}

	// Life 1: first contact — a session is pinned, its first turn runs,
	// the reply lands.
	db1 := open()
	eng1 := &drillEngine{}
	sender1 := life(db1, eng1, message(1))
	starts, resumes := eng1.record()
	if len(starts) != 1 || len(resumes) != 0 {
		t.Fatalf("life 1 engine calls = %d starts / %d resumes, want 1/0 — first contact pins and starts fresh (§4.1)", len(starts), len(resumes))
	}
	pinned := starts[0]
	if len(sender1.sent) != 1 || sender1.sent[0] != "re: discord:msg:1" {
		t.Errorf("life 1 sent %v, want the one composed reply", sender1.sent)
	}
	if err := db1.Close(); err != nil {
		t.Fatalf("close life-1 store: %v", err)
	}

	// Hours pass. The daemon restarts as a new process over the same
	// state directory.
	clock.Add(3 * 60 * 60)

	// Life 2: message two continues the SAME conversation.
	db2 := open()
	t.Cleanup(func() {
		if err := db2.Close(); err != nil {
			t.Errorf("close life-2 store: %v", err)
		}
	})
	eng2 := &drillEngine{}
	sender2 := life(db2, eng2, message(2))
	starts, resumes = eng2.record()
	if len(starts) != 0 || len(resumes) != 1 {
		t.Fatalf("life 2 engine calls = %d starts / %d resumes, want 0/1 — continuity means resume, never a fresh transcript (§4.1)", len(starts), len(resumes))
	}
	resumed := resumes[0]
	if resumed.SessionID != pinned.SessionID {
		t.Errorf("life 2 resumed session %s, life 1 pinned %s — the thread lost its identity across the restart", resumed.SessionID, pinned.SessionID)
	}
	if resumed.Cwd != pinned.Cwd {
		t.Errorf("life 2 resumed from %q, life 1 spawned from %q — --resume is cwd-scoped and must use the recorded dir (§6)", resumed.Cwd, pinned.Cwd)
	}
	if resumed.Prompt != "discord:msg:2" {
		t.Errorf("life 2 resume prompt = %q, want the new event's — hours-old context must not leak into the wrong turn", resumed.Prompt)
	}
	// Exactly one reply per life: the restart resend pass in life 2
	// found nothing owed from life 1.
	if len(sender2.sent) != 1 || sender2.sent[0] != "re: discord:msg:2" {
		t.Errorf("life 2 sent %v, want only message 2's reply — an acked delivery must never re-send", sender2.sent)
	}

	// Continuity bookkeeping: one live session, two turns on it,
	// last_seen at the resume.
	var liveCount int
	if err := db2.QueryRow(
		`SELECT count(*) FROM sessions WHERE thread_key = ? AND status IN ('creating', 'active')`, threadKey,
	).Scan(&liveCount); err != nil {
		t.Fatalf("count live sessions: %v", err)
	}
	if liveCount != 1 {
		t.Errorf("live sessions = %d after two lives, want 1 (§6 one_live_session)", liveCount)
	}
	var turns, lastSeen int64
	if err := db2.QueryRow(
		`SELECT turns, last_seen FROM sessions WHERE session_id = ?`, pinned.SessionID,
	).Scan(&turns, &lastSeen); err != nil {
		t.Fatalf("read session bookkeeping: %v", err)
	}
	if turns != 2 {
		t.Errorf("session turns = %d, want 2 — one per life (§6)", turns)
	}
	if lastSeen != clock.Load() {
		t.Errorf("last_seen = %d, want %d (the resume's clock) — rotation caps key off this", lastSeen, clock.Load())
	}
}

// waitForDrill polls until cond is true or the deadline passes — the
// session package's unit tests are synchronous, so the router-composed
// drills carry their own waiter.
func waitForDrill(t *testing.T, cond func() bool, what string) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}
