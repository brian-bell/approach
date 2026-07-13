// Package router owns the §4.1 per-thread queues: an in-memory FIFO
// index over unprocessed rows of the durable events table, claimed by
// thread_key. The table is the truth — these queues are ONLY an index
// over it, rebuilt from UnprocessedEvents on restart — so losing the
// process never loses an event, only its position in RAM. Dispatch is
// strictly serial within a thread (a turn must finish before the next
// begins) and freely concurrent across threads; session resolution
// happens inside the handler, AFTER the claim, so two simultaneous
// first-messages to a new thread serialize here and the one_live_session
// index (§6) is a backstop, not the lock.
package router

import (
	"context"
	"database/sql"
	"fmt"
	"hash/fnv"
	"log/slog"
	"runtime/debug"
	"sync"
	"time"

	"github.com/brian-bell/approach/internal/store"
)

// Handler processes one queued event — the turn: resolve the session,
// run the engine, advance the event row. Called serially per thread_key,
// concurrently across threads. The context is the router's lifetime
// context: a handler should treat its cancellation as "shut down
// gracefully", not as this event's failure.
type Handler func(context.Context, store.QueuedEvent)

// Options configures New. Handler and Logger are required; Now defaults
// to time.Now — injectable so tests own the clock (§6 convention).
type Options struct {
	Handler Handler
	Logger  *slog.Logger
	Now     func() time.Time
	// OnPark, when set, is told about each crash-interrupted event
	// Rebuild parks — the seam where the daemon surfaces the §4.6
	// notice to the originating thread. It runs AFTER the park
	// committed; an error degrades to a logged loss, never a failed
	// Rebuild (the park is durable, and refusing to boot over a failed
	// notification would invert §4.6 — the dead-letter flow owns a
	// surface that stays failed).
	OnPark func(context.Context, store.QueuedEvent) error
}

// Queues is the per-thread dispatch index. Zero value is not usable —
// New wires the store handle, lifetime context, handler, and logger.
type Queues struct {
	ctx     context.Context
	db      *sql.DB
	handler Handler
	logger  *slog.Logger
	now     func() time.Time
	onPark  func(context.Context, store.QueuedEvent) error

	mu      sync.Mutex
	pending map[string][]store.QueuedEvent // per-thread FIFO, receipt (id) order
	claimed map[string]bool                // threads with a live drain goroutine
	ingest  [ingestShards]sync.Mutex       // sharded persist+enqueue serialization
	wg      sync.WaitGroup                 // one per live drain goroutine
}

// ingestShards bounds the persist+enqueue lock table. Thread keys are
// externally sourced (any stranger's DM mints one), so a lock PER key
// would grow resident memory for the daemon's lifetime — an avoidable
// exhaustion surface. Sharding keeps the §4.1 ordering guarantee (one
// thread always hashes to one lock) at a fixed size; the cost is two
// distinct threads occasionally serializing their inserts, which
// SQLite's single-writer lock forces anyway.
const ingestShards = 64

// New builds the queue index over db's events table. ctx bounds every
// dispatch: once it is cancelled no NEW turn starts — in-flight
// handlers finish (see Wait) and everything still queued stays in the
// events table for the next restart's Rebuild.
func New(ctx context.Context, db *sql.DB, opts Options) *Queues {
	now := opts.Now
	if now == nil {
		now = time.Now
	}
	return &Queues{
		ctx:     ctx,
		db:      db,
		handler: opts.Handler,
		logger:  opts.Logger,
		now:     now,
		onPark:  opts.OnPark,
		pending: make(map[string][]store.QueuedEvent),
		claimed: make(map[string]bool),
	}
}

// Persist is the ingest chokepoint for a live daemon: write the event
// row (§4.1: durability before anything), then index it — atomically
// per thread. The per-thread lock exists because receipt order IS
// dispatch order: without it, two concurrent ingests on one thread can
// insert rows 1,2 but enqueue 2,1 if the first goroutine is descheduled
// between insert and enqueue — a FIFO violation Rebuild would never
// have produced. A collapsed duplicate (inserted=false) is not
// enqueued: the original row is either already indexed or already
// processed, and re-enqueueing it would double-dispatch (§4.1: dup
// delivery → one turn).
func (q *Queues) Persist(ctx context.Context, ev store.Event) (inserted bool, err error) {
	lock := q.ingestLock(ev.ThreadKey)
	lock.Lock()
	defer lock.Unlock()
	id, inserted, err := store.InsertEvent(ctx, q.db, ev)
	if err != nil || !inserted {
		return inserted, err
	}
	q.Enqueue(store.QueuedEvent{
		ID:          id,
		DedupKey:    ev.DedupKey,
		ThreadKey:   ev.ThreadKey,
		Kind:        ev.Kind,
		Trust:       ev.Trust,
		Payload:     ev.Payload,
		Status:      "received",
		Received:    ev.Received,
		Correlation: ev.Correlation,
	})
	return true, nil
}

// ingestLock returns the persist+enqueue mutex for a thread key: its
// shard by FNV-1a hash. Same key, same lock — the ordering invariant —
// with a table whose size never depends on how many strangers opened
// a thread.
func (q *Queues) ingestLock(threadKey string) *sync.Mutex {
	h := fnv.New32a()
	h.Write([]byte(threadKey)) // never errors (hash.Hash contract)
	return &q.ingest[h.Sum32()%ingestShards]
}

// Enqueue appends the event to its thread's FIFO and claims the thread
// — spawns its drain goroutine — if no claim is live. Callers own the
// FIFO contract: events for one thread must be enqueued in receipt (id)
// order. Rebuild satisfies it by scanning in id order before ingest is
// live; concurrent ingest must go through Persist, whose per-thread
// lock makes insert+enqueue atomic. After shutdown begins this is a
// no-op: the row is already durable, and starting a turn against a
// store that is about to close would lose the turn's writes, not the
// event.
func (q *Queues) Enqueue(ev store.QueuedEvent) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.ctx.Err() != nil {
		return
	}
	q.pending[ev.ThreadKey] = append(q.pending[ev.ThreadKey], ev)
	if !q.claimed[ev.ThreadKey] {
		q.claimed[ev.ThreadKey] = true
		q.wg.Add(1)
		go q.drain(ev.ThreadKey)
	}
}

// Readmit re-enters an event the store has already durably requeued
// (a §4.6 retry: interrupted → received, or an engine-failure retry)
// at its thread's CURRENT tail. It takes the same per-thread ingest
// lock as Persist, so a readmission never interleaves inside another
// ingest's insert+enqueue critical section. This is the sanctioned
// exception to Enqueue's receipt-order contract: a retried event's old
// id lands behind newer arrivals live (the tail NOW), while a restart
// would replay it by id order — an accepted asymmetry, per-thread
// serialization (not cross-arrival order) is the §4.1 guarantee.
func (q *Queues) Readmit(ev store.QueuedEvent) {
	lock := q.ingestLock(ev.ThreadKey)
	lock.Lock()
	defer lock.Unlock()
	q.Enqueue(ev)
}

// Rebuild reloads the index from the durable queue (§4.1: rebuilt from
// the table on restart). UnprocessedEvents returns rows in id (receipt)
// order, so per-thread FIFO order is reconstructed exactly. Call before
// ingest goes live: Rebuild enqueues directly, relying on the scan's
// ordering rather than the per-thread ingest lock.
//
// A row found in 'processing' was mid-turn when the daemon died. Its
// side effects are mechanically unknowable (the email may or may not
// have gone out), so it parks as interrupted — NEVER auto-rerun (§4.6)
// — out-of-band, so the thread's queue keeps flowing. Surfacing the
// parked turn to its originating thread with a retry offer is the §4.6
// delivery flow's job (epic 1.3); the park here is the durable state it
// reads from. A park that cannot be written is a startup refusal: booting
// anyway would either replay the turn later or lose it from view.
func (q *Queues) Rebuild(ctx context.Context) error {
	rows, err := store.UnprocessedEvents(ctx, q.db)
	if err != nil {
		return fmt.Errorf("router: rebuild queues: %w", err)
	}
	// Two phases, strictly ordered: EVERY crash-interrupted row parks
	// before ANY handler can start. Enqueue spawns drain goroutines
	// immediately, so interleaving the two would (a) run new turns
	// while recovery is still rewriting history, and (b) on a park
	// failure, return this startup refusal while an already-spawned
	// handler races the daemon's store teardown — Rebuild's error path
	// must leave nothing running.
	for _, ev := range rows {
		if ev.Status != "processing" {
			continue
		}
		if err := store.ParkEvent(ctx, q.db, ev.ID, q.now().Unix()); err != nil {
			return fmt.Errorf("router: rebuild queues: park crash-interrupted event: %w", err)
		}
		q.logger.Warn("crash-interrupted event parked as interrupted — never auto-rerun (§4.6)",
			"thread_key", ev.ThreadKey, "dedup_key", ev.DedupKey)
		if q.onPark != nil {
			if err := q.onPark(ctx, ev); err != nil {
				q.logger.Error("park surfacing failed — the park is durable, the notice is not (§4.6)",
					"dedup_key", ev.DedupKey, "error", err.Error())
			}
		}
	}
	for _, ev := range rows {
		if ev.Status == "received" {
			q.Enqueue(ev)
		}
	}
	return nil
}

// Wait blocks until every live drain goroutine has exited — the daemon
// calls this after cancelling the router context and quiescing ingest,
// so no turn is mid-write when the store closes. Producers must be
// stopped first: Wait concurrent with a live Enqueue is a misuse.
func (q *Queues) Wait() {
	q.wg.Wait()
}

// drain is the thread's claim: one goroutine per thread_key, dispatching
// its FIFO until empty or shutdown, then releasing the claim. The
// release and the empty-check happen under one lock acquisition, so an
// Enqueue landing "just after" either sees the claim and appends, or
// sees no claim and spawns the successor — a wakeup is never lost.
func (q *Queues) drain(threadKey string) {
	defer q.wg.Done()
	for {
		q.mu.Lock()
		if q.ctx.Err() != nil || len(q.pending[threadKey]) == 0 {
			// Anything still pending on shutdown stays in the events
			// table (status received/processing) — the next restart's
			// Rebuild re-indexes it. Dropping the RAM copy is safe by
			// construction.
			delete(q.claimed, threadKey)
			delete(q.pending, threadKey)
			q.mu.Unlock()
			return
		}
		ev := q.pending[threadKey][0]
		q.pending[threadKey] = q.pending[threadKey][1:]
		q.mu.Unlock()
		q.dispatch(ev)
	}
}

// dispatch runs one turn. A handler panic must end in a durable,
// human-visible state, not a log line the queue forgets (§4.6): the
// event was already removed from the RAM index, so without a store
// write it would strand — row still 'received'/'processing', never
// rescheduled until a daemon restart. So a panicking turn parks its
// event as interrupted: never auto-retried (the panic's side effects
// are unknowable), but durably out-of-band where the §4.6 surfacing
// flows find it. The thread's queue itself continues — one bad turn
// must not silence a thread.
func (q *Queues) dispatch(ev store.QueuedEvent) {
	// Quiet refusal on shutdown: the row is durable and still
	// 'received'; the next boot's Rebuild re-indexes it.
	if q.ctx.Err() != nil {
		return
	}
	// The processing stamp precedes the handler (§4.1, §4.6): from
	// this write until the turn's completion transition, a crash reads
	// as interrupted — the only honest state for a turn whose side
	// effects are unknowable. If the stamp cannot be written the turn
	// must NOT run: an unstamped turn is exactly the replay hazard,
	// and the untouched row simply waits for a later restart
	// (at-least-once, provably side-effect-free so far).
	if err := store.MarkEventProcessing(q.ctx, q.db, ev.ID, q.now().Unix()); err != nil {
		q.logger.Error("pre-turn processing stamp failed — turn not started, event stays durably queued",
			"thread_key", ev.ThreadKey, "dedup_key", ev.DedupKey, "error", err.Error())
		return
	}
	defer func() {
		if p := recover(); p != nil {
			q.logger.Error("turn handler panicked — parking event as interrupted (§4.6)",
				"thread_key", ev.ThreadKey, "dedup_key", ev.DedupKey,
				"panic", fmt.Sprint(p), "stack", string(debug.Stack()))
			// Background context: the panic may be unwinding during
			// shutdown, and parking is exactly the write that must not
			// be skipped then.
			if err := store.ParkEvent(context.Background(), q.db, ev.ID, q.now().Unix()); err != nil {
				q.logger.Error("park after panic failed — event stranded until restart recovery",
					"dedup_key", ev.DedupKey, "error", err.Error())
			} else if q.onPark != nil {
				// The park must be HEARD (§4.6): Rebuild skips rows
				// already interrupted, so a notice skipped here is
				// skipped forever — this is the only chance to write it.
				if err := q.onPark(context.Background(), ev); err != nil {
					q.logger.Error("park surfacing failed — the park is durable, the notice is not (§4.6)",
						"dedup_key", ev.DedupKey, "error", err.Error())
				}
			}
		}
	}()
	q.handler(q.ctx, ev)
}
