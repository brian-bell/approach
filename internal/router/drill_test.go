package router_test

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/brian-bell/approach/internal/delivery"
	"github.com/brian-bell/approach/internal/router"
	"github.com/brian-bell/approach/internal/store"
)

// These are the §9 P1 fault drills for the queue's crash contract. The
// unit tests beside them pin each transition in isolation; the drills
// pin the COMPOSED promise — a real SIGKILL'd process, a real second
// life over the same database file — because the failure they guard
// against (a replayed side effect, a doubled turn) lives in the seams
// between transitions, not inside any one of them.

// drillKill9Env carries the state dir from the drill to its re-exec'd
// child; its presence is what turns the child test on.
const drillKill9Env = "APPROACH_DRILL_KILL9_STATE"

// drillKill9Session is the session the child's mid-turn side effect
// journals under — the parent asserts against the same id.
const drillKill9Session = "d3111111-1111-4111-8111-111111111111"

// TestDrillKillNineMidTurn is the §9 P1 drill: kill -9 the process
// mid-turn — after a side-effecting call provably started (§6 journal),
// before the turn completed — and verify the next life parks the event
// as interrupted with NO side-effect replay (§4.6). The kill is a real
// SIGKILL of a real process: no deferred cleanup runs, no transaction
// gets to finish politely, which is exactly the state a power cut or
// OOM kill leaves behind and exactly what an in-process simulation
// cannot prove.
func TestDrillKillNineMidTurn(t *testing.T) {
	if os.Getenv(drillKill9Env) != "" {
		t.Skip("re-exec guard: the child runs TestDrillKillNineMidTurnChild only")
	}
	dir := t.TempDir()

	// Life 1: the child process ingests one event through the real
	// router, journals an unkeyed tool attempt (a side effect with no
	// retry authorization, §4.6), drops a marker file, and hangs
	// mid-turn until the SIGKILL below.
	child := exec.Command(os.Args[0], "-test.run=TestDrillKillNineMidTurnChild$", "-test.v")
	child.Env = append(os.Environ(), drillKill9Env+"="+dir)
	var childOut lockedBuffer
	child.Stdout = &childOut
	child.Stderr = &childOut
	if err := child.Start(); err != nil {
		t.Fatalf("start child: %v", err)
	}
	// The kill must happen even if an assertion below fails first — a
	// leaked child holds the test binary's exit hostage.
	defer func() { _ = child.Process.Kill(); _ = child.Wait() }()

	// A generous deadline of its own: the child is a whole re-exec'd
	// test process (binary init, store open, migrations) — slower than
	// anything waitFor's shared 5s budget was sized for.
	marker := filepath.Join(dir, "mid-turn")
	midTurn := false
	for deadline := time.Now().Add(30 * time.Second); time.Now().Before(deadline); {
		if _, err := os.Stat(marker); err == nil {
			midTurn = true
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	if !midTurn {
		t.Fatalf("child never reached mid-turn; output:\n%s", childOut.String())
	}
	if err := child.Process.Kill(); err != nil {
		t.Fatalf("kill -9 child: %v", err)
	}
	_ = child.Wait() // reap; a SIGKILL death is the expected "failure"

	// Life 2: a fresh daemon over the same database. Rebuild must find
	// the crash-interrupted row and park it — never hand it to a
	// handler, whose re-run would replay the journalled side effect.
	db, err := store.Open(filepath.Join(dir, "state", "approach.db"))
	if err != nil {
		t.Fatalf("reopen store after kill: %v", err)
	}
	t.Cleanup(func() {
		if err := db.Close(); err != nil {
			t.Errorf("close: %v", err)
		}
	})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var replayed atomic.Int64
	var parked []store.QueuedEvent
	q := router.New(ctx, db, router.Options{
		Handler: func(context.Context, store.QueuedEvent) { replayed.Add(1) },
		Logger:  discardLogger(),
		// Wired like the daemon: the park surfaces to the originating
		// thread through the outbox (§4.6).
		OnPark: func(ctx context.Context, ev store.QueuedEvent) error {
			parked = append(parked, ev)
			return delivery.SurfaceInterrupted(ctx, db, ev)
		},
	})
	if err := q.Rebuild(ctx); err != nil {
		t.Fatalf("rebuild after kill: %v", err)
	}
	q.Wait()

	// The event is durably interrupted — out-of-band, human-visible,
	// never auto-rerun (§4.6).
	var status string
	var attempts, parks int64
	if err := db.QueryRow(
		`SELECT status, attempts, parks FROM events WHERE dedup_key = 'discord:msg:1'`,
	).Scan(&status, &attempts, &parks); err != nil {
		t.Fatalf("read event after rebuild: %v", err)
	}
	if status != "interrupted" {
		t.Errorf("event status = %q after kill -9 mid-turn, want 'interrupted' (§4.6)", status)
	}
	if parks != 1 {
		t.Errorf("parks = %d, want 1 — one crash, one park episode", parks)
	}
	if attempts != 0 {
		t.Errorf("retry budget attempts = %d, want 0 — a crash park is not an auto-retry", attempts)
	}

	// NO replay: no handler ran, and the journal still holds exactly
	// the one attempt the killed turn provably started — the evidence
	// §4.6 recovery reasons from, not a side effect run twice.
	if n := replayed.Load(); n != 0 {
		t.Errorf("handler ran %d times in life 2, want 0 — an interrupted turn's side effects are unknowable, it must never auto-rerun (§4.6)", n)
	}
	var journalled int
	if err := db.QueryRow(
		`SELECT count(*) FROM tool_attempts WHERE session_id = ?`, drillKill9Session,
	).Scan(&journalled); err != nil {
		t.Fatalf("count journalled attempts: %v", err)
	}
	if journalled != 1 {
		t.Errorf("tool_attempts rows = %d, want exactly the 1 the killed turn journalled — more is a replay, fewer is lost evidence (§6)", journalled)
	}

	// The park was HEARD: surfaced once to the originating thread, with
	// the §4.6 notice durably in the outbox (write-before-send).
	if len(parked) != 1 || parked[0].DedupKey != "discord:msg:1" {
		t.Errorf("OnPark saw %v, want exactly the killed event", parked)
	}
	var notices int
	if err := db.QueryRow(
		`SELECT count(*) FROM deliveries WHERE delivery_key = 'interrupted:discord:msg:1:1'`,
	).Scan(&notices); err != nil {
		t.Fatalf("count park notices: %v", err)
	}
	if notices != 1 {
		t.Errorf("park notice rows = %d, want 1 — the park must be visible to a human (§4.6)", notices)
	}

	// The auto-retry door is closed even to a buggy caller: the store
	// transition refuses to requeue a parked row.
	if err := store.RequeueEventForRetry(ctx, db, parked[0].ID, time.Now().Unix()); err == nil {
		t.Error("RequeueEventForRetry accepted an interrupted event — only the §4.6 human retry may requeue a park")
	}
}

// TestDrillKillNineMidTurnChild is TestDrillKillNineMidTurn's life 1 —
// runs only under the drill's re-exec (env set), skips in a normal
// `go test` pass. It never returns: the parent SIGKILLs it mid-turn.
func TestDrillKillNineMidTurnChild(t *testing.T) {
	dir := os.Getenv(drillKill9Env)
	if dir == "" {
		t.Skip("helper process for TestDrillKillNineMidTurn")
	}
	// Deliberately no Close cleanup anywhere here: this process dies by
	// SIGKILL, and tidy teardown would falsify the crash being drilled.
	db, err := store.Open(filepath.Join(dir, "state", "approach.db"))
	if err != nil {
		t.Fatalf("child: open store: %v", err)
	}
	ctx := context.Background()
	// The turn's session — tool_attempts binds session and event to one
	// live turn (§6), so the mid-turn journal write needs the real row.
	if _, err := store.InsertSession(ctx, db, store.Session{
		ThreadKey: "discord:dm:123", SessionID: drillKill9Session, Cwd: dir,
		TrustFloor: "owner", CreatedAt: 1700000000, ActivationDeadline: 1700000600,
	}); err != nil {
		t.Fatalf("child: insert session: %v", err)
	}

	q := router.New(ctx, db, router.Options{
		Handler: func(ctx context.Context, ev store.QueuedEvent) {
			// The side effect PROVABLY STARTED: PreToolUse journals
			// before the call runs (§4.1), unkeyed — no retry
			// authorization exists for it (§4.6).
			if _, err := store.InsertToolAttempt(ctx, db, store.ToolAttempt{
				SessionID: drillKill9Session, EventID: ev.ID,
				Tool: "mcp__discord__send", ArgsDigest: "sha256:aa",
				StartedAt: 1700000060,
			}); err != nil {
				t.Errorf("child: journal attempt: %v", err)
				return
			}
			// Only now is the process in the state the drill kills: row
			// 'processing', side effect journalled, turn unfinished.
			if err := os.WriteFile(filepath.Join(dir, "mid-turn"), []byte("x"), 0o600); err != nil {
				t.Errorf("child: write marker: %v", err)
				return
			}
			select {} // hang mid-turn until the SIGKILL
		},
		Logger: discardLogger(),
	})
	if _, err := q.Persist(ctx, storeEvent(1, "discord:dm:123")); err != nil {
		t.Fatalf("child: persist event: %v", err)
	}
	select {} // never return — the parent owns this process's death
}

// lockedBuffer is a goroutine-safe capture of the child's output — the
// child process writes concurrently with the parent's failure paths
// reading it for diagnostics.
type lockedBuffer struct {
	mu  sync.Mutex
	buf []byte
}

func (b *lockedBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.buf = append(b.buf, p...)
	return len(p), nil
}

func (b *lockedBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return string(b.buf)
}

// TestDrillDuplicateChannelDeliveryOneTurn is the §9 P1 drill for the
// §4.1 dedup contract: the same channel message delivered many times —
// concurrently, after the turn completed, and again after a restart —
// runs exactly ONE turn, because the dedup_key unique insert collapses
// every copy onto the first row. Gateway redelivery is real (reconnect
// replays, §6), so this is the drill that keeps "the bot answered me
// twice" from ever shipping.
func TestDrillDuplicateChannelDeliveryOneTurn(t *testing.T) {
	db := mustOpen(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var turns atomic.Int64
	handler := func(_ context.Context, ev store.QueuedEvent) {
		turns.Add(1)
		// The future turn wiring's completion transition, as the
		// deliveries tests stage it — the drill needs completed history
		// to prove a late duplicate cannot resurrect it.
		if _, err := db.Exec(`UPDATE events SET status = 'completed' WHERE id = ?`, ev.ID); err != nil {
			t.Errorf("complete event: %v", err)
		}
	}
	q := router.New(ctx, db, router.Options{Handler: handler, Logger: discardLogger()})

	// A redelivery burst: the gateway hands the SAME message to the
	// ingest path from several goroutines at once.
	ev := storeEvent(1, "discord:dm:9")
	const burst = 8
	var wg sync.WaitGroup
	var insertedCount atomic.Int64
	start := make(chan struct{})
	for range burst {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			inserted, err := q.Persist(ctx, ev)
			if err != nil {
				t.Errorf("Persist duplicate: %v", err)
			}
			if inserted {
				insertedCount.Add(1)
			}
		}()
	}
	close(start)
	wg.Wait()
	if n := insertedCount.Load(); n != 1 {
		t.Errorf("%d of %d concurrent duplicates reported inserted, want exactly 1 — first write wins (§6)", n, burst)
	}
	waitFor(t, func() bool { return turns.Load() == 1 }, "the one turn to run")

	// A straggler duplicate arriving AFTER the turn completed: still a
	// collapsed no-op — completed history is untouched, nothing enqueued.
	if inserted, err := q.Persist(ctx, ev); err != nil || inserted {
		t.Errorf("late duplicate Persist = (inserted=%v, err=%v), want a quiet no-op", inserted, err)
	}

	// Restart: the duplicate arrives again in the NEXT life. Rebuild
	// must not re-index completed history, and the fresh insert must
	// still collapse.
	cancel()
	q.Wait()
	ctx2, cancel2 := context.WithCancel(context.Background())
	defer cancel2()
	q2 := router.New(ctx2, db, router.Options{Handler: handler, Logger: discardLogger()})
	if err := q2.Rebuild(ctx2); err != nil {
		t.Fatalf("rebuild: %v", err)
	}
	if inserted, err := q2.Persist(ctx2, ev); err != nil || inserted {
		t.Errorf("post-restart duplicate Persist = (inserted=%v, err=%v), want a quiet no-op", inserted, err)
	}
	q2.Wait()

	// One turn, one row, ever — across the burst, the straggler, and
	// the restart.
	if n := turns.Load(); n != 1 {
		t.Errorf("turns = %d across burst + straggler + restart, want 1 (§4.1: duplicate delivery → one turn)", n)
	}
	var rows int
	if err := db.QueryRow(`SELECT count(*) FROM events`).Scan(&rows); err != nil {
		t.Fatalf("count events: %v", err)
	}
	if rows != 1 {
		t.Errorf("events rows = %d, want 1 — every duplicate collapsed onto the first write", rows)
	}
	var status string
	if err := db.QueryRow(`SELECT status FROM events WHERE dedup_key = ?`, ev.DedupKey).Scan(&status); err != nil {
		t.Fatalf("read event status: %v", err)
	}
	if status != "completed" {
		t.Errorf("event status = %q, want 'completed' — duplicates must not disturb lifecycle state the queue already advanced", status)
	}
}
