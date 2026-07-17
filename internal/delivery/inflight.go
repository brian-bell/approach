package delivery

import (
	"context"
	"fmt"
	"sync"
)

// InFlight coordinates live delivery in two dimensions: delivery-key
// claims exclude rows from the pump while a relay sends them, and
// per-target gates serialize relay eligibility/composition with
// recovery notices aimed at the same destination. At-least-once
// (§4.1) accepts duplicates across a CRASH — it must not manufacture
// them or reorder visible output between goroutines of one living
// daemon. In-memory on purpose: both fences protect only concurrent
// work in this process; the next life recovers from durable rows.
//
// A nil *InFlight holds nothing and never fails — callers without a
// live-send path (tests, the restart-only resend) just pass nil.
type InFlight struct {
	mu      sync.Mutex
	keys    map[string]struct{}
	targets map[string]*targetGate
}

type targetGate struct {
	token chan struct{}
	refs  int
}

// NewInFlight builds an empty claim table.
func NewInFlight() *InFlight {
	return &InFlight{
		keys:    make(map[string]struct{}),
		targets: make(map[string]*targetGate),
	}
}

// AcquireTarget serializes delivery composition for one destination.
// A live relay holds this gate from its backlog check through durable
// reply composition and the direct send; recovery/dead-letter notices
// acquire the same gate before inserting. That closes the otherwise
// unavoidable check-then-compose race where a newer visible partial
// could jump a notice inserted during the engine turn (§4.1).
//
// Waiting honors ctx. The returned release is idempotent. A nil
// *InFlight is an unlocked seam for tests and restart-only callers.
func (f *InFlight) AcquireTarget(ctx context.Context, target string) (release func(), err error) {
	if f == nil {
		return func() {}, nil
	}
	if target == "" {
		return nil, fmt.Errorf("delivery: acquire empty target")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	f.mu.Lock()
	gate := f.targets[target]
	if gate == nil {
		gate = &targetGate{token: make(chan struct{}, 1)}
		gate.token <- struct{}{}
		f.targets[target] = gate
	}
	gate.refs++
	f.mu.Unlock()

	select {
	case <-ctx.Done():
		f.releaseTargetRef(target, gate)
		return nil, ctx.Err()
	case <-gate.token:
		var once sync.Once
		return func() {
			once.Do(func() {
				gate.token <- struct{}{}
				f.releaseTargetRef(target, gate)
			})
		}, nil
	}
}

func (f *InFlight) releaseTargetRef(target string, gate *targetGate) {
	f.mu.Lock()
	defer f.mu.Unlock()
	gate.refs--
	if gate.refs == 0 && f.targets[target] == gate {
		delete(f.targets, target)
	}
}

// Claim marks keys as owned by a live send. No-op on nil.
func (f *InFlight) Claim(keys ...string) {
	if f == nil {
		return
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, k := range keys {
		f.keys[k] = struct{}{}
	}
}

// Release returns keys to the pump. No-op on nil; releasing an
// unclaimed key is a no-op too — Release runs on every exit path of a
// live send, double-release included.
func (f *InFlight) Release(keys ...string) {
	if f == nil {
		return
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, k := range keys {
		delete(f.keys, k)
	}
}

// Held reports whether a live send owns the key. Always false on nil.
func (f *InFlight) Held(key string) bool {
	if f == nil {
		return false
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	_, ok := f.keys[key]
	return ok
}
