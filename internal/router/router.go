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

	"github.com/brian-bell/approach/internal/store"
)

// Handler processes one queued event — the turn: resolve the session,
// run the engine, advance the event row. Called serially per thread_key,
// concurrently across threads. The context is the router's lifetime
// context: a handler should treat its cancellation as "shut down
// gracefully", not as this event's failure.
type Handler func(context.Context, store.QueuedEvent)

// Queues is the per-thread dispatch index. Zero value is not usable —
// New wires the lifetime context, handler, and logger.
type Queues struct {
	ctx     context.Context
	handler Handler
	logger  *slog.Logger

	mu      sync.Mutex
	pending map[string][]store.QueuedEvent // per-thread FIFO, enqueue order
	claimed map[string]bool                // threads with a live drain goroutine
	wg      sync.WaitGroup                 // one per live drain goroutine
}

// New builds the queue index. ctx bounds every dispatch: once it is
// cancelled no NEW turn starts — in-flight handlers finish (see Wait)
// and everything still queued stays in the events table for the next
// restart's Rebuild.
func New(ctx context.Context, handler Handler, logger *slog.Logger) *Queues {
	return &Queues{
		ctx:     ctx,
		handler: handler,
		logger:  logger,
		pending: make(map[string][]store.QueuedEvent),
		claimed: make(map[string]bool),
	}
}

// Enqueue appends the event to its thread's FIFO and claims the thread
// — spawns its drain goroutine — if no claim is live. Called by ingest
// after the row is durably inserted (§4.1: persist before anything),
// and by Rebuild on restart. After shutdown begins this is a no-op:
// the row is already durable, and starting a turn against a store
// that is about to close would lose the turn's writes, not the event.
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
// order, so per-thread FIFO order is reconstructed exactly.
func (q *Queues) Rebuild(ctx context.Context, db *sql.DB) error {
	rows, err := store.UnprocessedEvents(ctx, db)
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

// dispatch runs one turn, converting a handler panic into a loud log
// instead of a dead thread queue: the daemon must survive a bad turn
// (§4.6 — every failure ends visible; a wedged queue that silently
// ignores a thread forever is the failure mode this forecloses). The
// event row keeps whatever status the handler reached — recovery
// reasons from the table, not from this goroutine's fate.
func (q *Queues) dispatch(ev store.QueuedEvent) {
	defer func() {
		if p := recover(); p != nil {
			q.logger.Error("turn handler panicked — thread queue continues",
				"thread_key", ev.ThreadKey, "dedup_key", ev.DedupKey,
				"panic", fmt.Sprint(p), "stack", string(debug.Stack()))
		}
	}()
	q.handler(q.ctx, ev)
}
