package delivery

import "sync"

// InFlight is the live-send claim table: the delivery keys a turn's
// relay currently owns, excluded from the pump's resend scan so a
// ticker or kick pass cannot pick the same row up mid-send and deliver
// it twice. At-least-once (§4.1) accepts duplicates across a CRASH —
// it must not manufacture them between two goroutines of one living
// daemon. In-memory on purpose: a claim's whole job is to fence the
// pump off a send that is happening right now, and if the daemon dies
// mid-claim the next life's pump re-sends from the durable row —
// exactly the crash contract.
//
// A nil *InFlight holds nothing and never fails — callers without a
// live-send path (tests, the restart-only resend) just pass nil.
type InFlight struct {
	mu   sync.Mutex
	keys map[string]struct{}
}

// NewInFlight builds an empty claim table.
func NewInFlight() *InFlight {
	return &InFlight{keys: make(map[string]struct{})}
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
