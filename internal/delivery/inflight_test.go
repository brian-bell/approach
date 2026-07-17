package delivery

import (
	"context"
	"errors"
	"testing"
	"time"
)

// TestInFlightTargetGate serializes every composer for one target but
// leaves cancellation live while a caller waits. A live relay holds
// this gate from eligibility through reply composition; notices use
// the same gate so neither can acquire an older row mid-turn (§4.1).
func TestInFlightTargetGate(t *testing.T) {
	claims := NewInFlight()
	release, err := claims.AcquireTarget(context.Background(), "discord:dm:owner")
	if err != nil {
		t.Fatalf("first AcquireTarget: %v", err)
	}

	acquired := make(chan struct{})
	done := make(chan struct{})
	go func() {
		defer close(done)
		unlock, err := claims.AcquireTarget(context.Background(), "discord:dm:owner")
		if err != nil {
			t.Errorf("second AcquireTarget: %v", err)
			return
		}
		close(acquired)
		unlock()
	}()
	select {
	case <-acquired:
		t.Fatal("second composer acquired a target still held by the first")
	case <-time.After(20 * time.Millisecond):
	}
	release()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("second composer did not acquire after release")
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := claims.AcquireTarget(ctx, "discord:dm:owner"); !errors.Is(err, context.Canceled) {
		t.Errorf("AcquireTarget with cancelled context = %v, want context.Canceled", err)
	}
}
