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
}

// Queues is the per-thread dispatch index. Zero value is not usable —
// New wires the store handle, lifetime context, handler, and logger.
type Queues struct {
	ctx     context.Context
	db      *sql.DB
	handler Handler
	logger  *slog.Logger
	now     func() time.Time

	mu      sync.Mutex
	pending map[string][]store.QueuedEvent // per-thread FIFO, receipt (id) order
	claimed map[string]bool                // threads with a live drain goroutine
	ingest  map[string]*sync.Mutex         // per-thread persist+enqueue serialization
	wg      sync.WaitGroup                 // one per live drain goroutine
}

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
		pending: make(map[string][]store.QueuedEvent),
		claimed: make(map[string]bool),
		ingest:  make(map[string]*sync.Mutex),
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
		ID:        id,
		DedupKey:  ev.DedupKey,
		ThreadKey: ev.ThreadKey,
		Kind:      ev.Kind,
		Trust:     ev.Trust,
		Payload:   ev.Payload,
		Status:    "received",
		Received:  ev.Received,
	})
	return true, nil
}

// ingestLock returns the per-thread persist+enqueue mutex, creating it
// on first use. Locks accumulate one per thread_key ever seen — bounded
// by the thread population, and a lock is a few words; an eviction
// scheme would risk two goroutines holding "the" lock for one thread.
func (q *Queues) ingestLock(threadKey string) *sync.Mutex {
	q.mu.Lock()
	defer q.mu.Unlock()
	l, ok := q.ingest[threadKey]
	if !ok {
		l = &sync.Mutex{}
		q.ingest[threadKey] = l
	}
	return l
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

// Rebuild reloads the index from the durable queue (§4.1: rebuilt from
// the table on restart). UnprocessedEvents returns rows in id (receipt)
// order, so per-thread FIFO order is reconstructed exactly. Call before
// ingest goes live: Rebuild enqueues directly, relying on the scan's
// ordering rather than the per-thread ingest lock.
func (q *Queues) Rebuild(ctx context.Context) error {
	rows, err := store.UnprocessedEvents(ctx, q.db)
	if err != nil {
		return fmt.Errorf("router: rebuild queues: %w", err)
	}
	for _, ev := range rows {
		q.Enqueue(ev)
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
			}
		}
	}()
	q.handler(q.ctx, ev)
}
