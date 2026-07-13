package cli

import (
	"context"
	"database/sql"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/brian-bell/approach/internal/admin"
	"github.com/brian-bell/approach/internal/store"
)

// sendingRunner is a fakeRunner that also satisfies delivery.Sender —
// the shape of the real discord adapter.
type sendingRunner struct {
	fakeRunner
	mu   sync.Mutex
	sent []string // payloads, in send order
}

func (s *sendingRunner) Send(_ context.Context, _, payload string) (string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.sent = append(s.sent, payload)
	return "fake:msg:1", nil
}

func (s *sendingRunner) sentPayloads() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]string(nil), s.sent...)
}

// seedPendingDelivery creates the state store before the daemon boots
// and plants one unacked delivery row — the §4.6 crash artifact the
// restart must drain.
func seedPendingDelivery(t *testing.T, state, target, payload string) {
	t.Helper()
	db, err := store.Open(filepath.Join(state, "approach.db"))
	if err != nil {
		t.Fatalf("pre-seed open: %v", err)
	}
	defer func() {
		if err := db.Close(); err != nil {
			t.Fatalf("pre-seed close: %v", err)
		}
	}()
	if _, _, err := store.InsertDelivery(context.Background(), db, store.Delivery{
		DeliveryKey: "reply:seeded", Target: target, Payload: payload,
	}); err != nil {
		t.Fatalf("pre-seed delivery: %v", err)
	}
}

// readSeededDelivery returns the seeded row's status and acked flag
// after the daemon has shut down and released the store.
func readSeededDelivery(t *testing.T, state string) (status string, acked bool) {
	t.Helper()
	db, err := store.Open(filepath.Join(state, "approach.db"))
	if err != nil {
		t.Fatalf("post-run open: %v", err)
	}
	defer func() {
		if err := db.Close(); err != nil {
			t.Fatalf("post-run close: %v", err)
		}
	}()
	var ackedTS sql.NullInt64
	if err := db.QueryRow(
		`SELECT status, acked FROM deliveries WHERE delivery_key = 'reply:seeded'`,
	).Scan(&status, &ackedTS); err != nil {
		t.Fatalf("read seeded delivery: %v", err)
	}
	return status, ackedTS.Valid
}

// TestDaemonResendsOwedDeliveriesOnBoot: the §4.6 restart contract —
// an unacked delivery row left by the previous life re-sends from its
// persisted payload once the adapter is up, and the ack lands durably.
func TestDaemonResendsOwedDeliveriesOnBoot(t *testing.T) {
	runner := &sendingRunner{fakeRunner: fakeRunner{run: func(ctx context.Context) error {
		<-ctx.Done()
		return ctx.Err()
	}}}
	stubRunner(t, runner)
	state, configPath := discordDaemonDir(t)
	seedPendingDelivery(t, state, "discord:dm:123", "the reply the crash ate")

	var out, errW syncBuilder
	exit := make(chan int, 1)
	go func() { exit <- Run([]string{"daemon", "--state", state, "--config", configPath}, &out, &errW) }()
	awaitReady(t, &out)

	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if len(runner.sentPayloads()) > 0 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if got := runner.sentPayloads(); len(got) != 1 || got[0] != "the reply the crash ate" {
		t.Fatalf("resent payloads = %q, want exactly the persisted payload", got)
	}

	if _, err := admin.Request(filepath.Join(state, "approach.sock"), "drain"); err != nil {
		t.Fatalf("drain: %v", err)
	}
	select {
	case code := <-exit:
		if code != 0 {
			t.Fatalf("daemon exit = %d, want 0; stderr=%q", code, errW.String())
		}
	case <-time.After(10 * time.Second):
		t.Fatal("daemon did not exit after drain")
	}

	status, acked := readSeededDelivery(t, state)
	if status != "sent" || !acked {
		t.Errorf("seeded row (status, acked) = (%q, %v), want ('sent', true) — the resend's ack must land durably", status, acked)
	}
}

// TestDaemonWithoutSenderLeavesDeliveriesOwed: an adapter that cannot
// send (the seam's plain fakeRunner — and honestly, a future adapter
// wired before its outbound path) must leave owed rows untouched and
// say so loudly: silence here would read as "nothing owed" when the
// outbox disagrees.
func TestDaemonWithoutSenderLeavesDeliveriesOwed(t *testing.T) {
	runner := &fakeRunner{run: func(ctx context.Context) error {
		<-ctx.Done()
		return ctx.Err()
	}}
	stubRunner(t, runner)
	state, configPath := discordDaemonDir(t)
	seedPendingDelivery(t, state, "discord:dm:123", "still owed")

	var out, errW syncBuilder
	exit := make(chan int, 1)
	go func() { exit <- Run([]string{"daemon", "--state", state, "--config", configPath}, &out, &errW) }()
	awaitReady(t, &out)

	if _, err := admin.Request(filepath.Join(state, "approach.sock"), "drain"); err != nil {
		t.Fatalf("drain: %v", err)
	}
	select {
	case code := <-exit:
		if code != 0 {
			t.Fatalf("daemon exit = %d, want 0; stderr=%q", code, errW.String())
		}
	case <-time.After(10 * time.Second):
		t.Fatal("daemon did not exit after drain")
	}

	status, acked := readSeededDelivery(t, state)
	if status != "pending" || acked {
		t.Errorf("seeded row (status, acked) = (%q, %v), want ('pending', false) — no sender, no send", status, acked)
	}
	if !strings.Contains(errW.String(), "resend") {
		t.Errorf("stderr %q never mentions the skipped resend — owed rows must not be silent", errW.String())
	}
}
