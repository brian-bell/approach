package cli_test

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/brian-bell/approach/internal/cli"
	"github.com/brian-bell/approach/internal/store"
)

// dispatchFixtureDirs builds an APPROACH_HOME-shaped layout with a
// fake claude that answers the version probe AND plays one scripted
// stream-json turn, plus an approach.toml pinning it. Returns the
// home dir and the state dir under it.
func dispatchFixtureDirs(t *testing.T) (home, state string) {
	t.Helper()
	home, err := os.MkdirTemp("", "cli") // sun_path cap — see daemon_test.go
	if err != nil {
		t.Fatalf("make temp dir: %v", err)
	}
	t.Cleanup(func() {
		if err := os.RemoveAll(home); err != nil {
			t.Errorf("remove temp dir: %v", err)
		}
	})
	bin := filepath.Join(home, "claude")
	script := `#!/bin/sh
case "$1" in
  --version) echo "2.1.199 (Claude Code)"; exit 0;;
esac
cat > /dev/null
echo '{"type":"system","subtype":"init","model":"claude-sonnet-5-20260115"}'
echo '{"type":"assistant","message":{"content":[{"type":"text","text":"pong from the engine"}]}}'
echo '{"type":"result","subtype":"success","is_error":false,"duration_ms":42,"total_cost_usd":0.003,"usage":{"input_tokens":10,"output_tokens":5}}'
exit 0
`
	if err := os.WriteFile(bin, []byte(script), 0o755); err != nil {
		t.Fatalf("write fake cli: %v", err)
	}
	cfgBody := fmt.Sprintf(`[models]
message = "claude-sonnet-5"
heartbeat = "claude-haiku-4-5"

[engine]
bin = %q
version = "2.1.199"
hooks = ["SessionStart", "Stop"]
`, bin)
	if err := os.WriteFile(filepath.Join(home, "approach.toml"), []byte(cfgBody), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}
	return home, filepath.Join(home, "state")
}

// TestDaemonDispatchesRealTurn is the approach-x6n.11 acceptance drill:
// a daemon with [engine] pinned finds a queued discord message on
// boot, runs a REAL engine turn for it (the fake CLI plays one
// stream-json conversation), records the C11 turns row, composes the
// reply into the outbox, and advances the event out of processing —
// nothing left for a human to unstick.
func TestDaemonDispatchesRealTurn(t *testing.T) {
	home, state := dispatchFixtureDirs(t)
	socket := filepath.Join(state, "approach.sock")

	// Stage one received event BEFORE the daemon boots — Rebuild must
	// dispatch it exactly like a live arrival (§4.1: the table is the
	// truth; the daemon that persisted an event need not be the one
	// that runs its turn).
	{
		db, err := store.Open(filepath.Join(state, "approach.db"))
		if err != nil {
			t.Fatalf("pre-stage store: %v", err)
		}
		payload := `{"dedup_key":"discord:msg:42","thread_key":"discord:dm:owner1","kind":"message","trust":"owner","text":"ping"}`
		if _, inserted, err := store.InsertEvent(context.Background(), db, store.Event{
			DedupKey:  "discord:msg:42",
			ThreadKey: "discord:dm:owner1",
			Kind:      "message",
			Trust:     "owner",
			Payload:   payload,
			Received:  time.Now().Unix(),
		}); err != nil || !inserted {
			t.Fatalf("stage event: inserted=%v err=%v", inserted, err)
		}
		if err := db.Close(); err != nil {
			t.Fatalf("close staging store: %v", err)
		}
	}

	var daemonOut, daemonErr strings.Builder
	done := make(chan int, 1)
	go func() {
		done <- cli.Run([]string{"daemon", "--state", state, "--config", filepath.Join(home, "approach.toml")}, &daemonOut, &daemonErr)
	}()
	waitDialable(t, socket)

	// The turn is asynchronous; the C11 spend fields on status are the
	// observable truth of its completion (§7) — poll them rather than
	// racing the drain against the dispatch.
	deadline := time.Now().Add(10 * time.Second)
	for {
		var out, errW strings.Builder
		if code := cli.Run([]string{"status", "--socket", socket}, &out, &errW); code != 0 {
			t.Fatalf("status exit = %d, stderr %q", code, errW.String())
		}
		if strings.Contains(out.String(), `"spend_today_turns":1`) {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("turn never landed in the spend fields; status: %s\njournal: %s", out.String(), daemonErr.String())
		}
		time.Sleep(20 * time.Millisecond)
	}

	var out, errW strings.Builder
	if code := cli.Run([]string{"drain", "--socket", socket}, &out, &errW); code != 0 {
		t.Fatalf("drain exit = %d, stderr %q", code, errW.String())
	}
	select {
	case code := <-done:
		if code != 0 {
			t.Fatalf("daemon exit = %d, stderr %q", code, daemonErr.String())
		}
	case <-time.After(10 * time.Second):
		t.Fatal("daemon did not exit after drain")
	}
	if !strings.Contains(daemonErr.String(), "turn dispatch wired") {
		t.Errorf("journal does not record the wired dispatch: %s", daemonErr.String())
	}

	db, err := store.Open(filepath.Join(state, "approach.db"))
	if err != nil {
		t.Fatalf("reopen store: %v", err)
	}
	defer func() { _ = db.Close() }()

	// The event left processing: completed, with the reply owed in the
	// outbox (no discord adapter is up, so the pump could not send it —
	// owed-but-durable is the honest state, never a silent drop).
	var status string
	if err := db.QueryRow(`SELECT status FROM events WHERE dedup_key = 'discord:msg:42'`).Scan(&status); err != nil {
		t.Fatalf("read event: %v", err)
	}
	if status != "completed" {
		t.Errorf("event status = %q, want completed", status)
	}
	var target, payload string
	var acked bool
	if err := db.QueryRow(
		`SELECT target, payload, acked IS NOT NULL FROM deliveries WHERE delivery_key = 'reply:discord:msg:42:0'`,
	).Scan(&target, &payload, &acked); err != nil {
		t.Fatalf("read reply delivery: %v", err)
	}
	if target != "discord:dm:owner1" || payload != "pong from the engine" || acked {
		t.Errorf("reply delivery = (%q, %q, acked=%v), want the engine's text owed to the thread", target, payload, acked)
	}

	// The C11 turns row landed through store.InsertTurn (§6) with the
	// stream's numbers, attributed to the driving event's kind.
	var kind, model, outcome string
	var cost float64
	if err := db.QueryRow(`SELECT kind, model, outcome, cost_usd FROM turns`).Scan(&kind, &model, &outcome, &cost); err != nil {
		t.Fatalf("read turns row: %v", err)
	}
	if kind != "message" || model != "claude-sonnet-5-20260115" || outcome != "ok" || cost != 0.003 {
		t.Errorf("turns row = (%q, %q, %q, %v), want (message, claude-sonnet-5-20260115, ok, 0.003)", kind, model, outcome, cost)
	}

	// The session pinned for the thread activated on its first turn,
	// spawned from the APPROACH_HOME root (§8 cwd policy).
	var sessStatus, cwd string
	if err := db.QueryRow(`SELECT status, cwd FROM sessions WHERE thread_key = 'discord:dm:owner1'`).Scan(&sessStatus, &cwd); err != nil {
		t.Fatalf("read session: %v", err)
	}
	wantCwd, err := filepath.EvalSymlinks(home)
	if err != nil {
		t.Fatalf("resolve home: %v", err)
	}
	if sessStatus != "active" || cwd != wantCwd {
		t.Errorf("session = (%q, %q), want (active, %q)", sessStatus, cwd, wantCwd)
	}
}

// TestDaemonWithoutEngineStaysDormant: the other half of the
// acceptance criteria — no [engine] means no dispatch: the event stays
// durably queued (processing under the placeholder), no turns row, no
// reply, and the journal says loudly why.
func TestDaemonWithoutEngineStaysDormant(t *testing.T) {
	home, err := os.MkdirTemp("", "cli") // sun_path cap
	if err != nil {
		t.Fatalf("make temp dir: %v", err)
	}
	t.Cleanup(func() {
		if err := os.RemoveAll(home); err != nil {
			t.Errorf("remove temp dir: %v", err)
		}
	})
	cfgBody := `[models]
message = "claude-sonnet-5"
heartbeat = "claude-haiku-4-5"
`
	if err := os.WriteFile(filepath.Join(home, "approach.toml"), []byte(cfgBody), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}
	state := filepath.Join(home, "state")
	socket := filepath.Join(state, "approach.sock")

	{
		db, err := store.Open(filepath.Join(state, "approach.db"))
		if err != nil {
			t.Fatalf("pre-stage store: %v", err)
		}
		payload := `{"dedup_key":"discord:msg:7","thread_key":"discord:dm:owner1","kind":"message","trust":"owner","text":"ping"}`
		if _, inserted, err := store.InsertEvent(context.Background(), db, store.Event{
			DedupKey: "discord:msg:7", ThreadKey: "discord:dm:owner1", Kind: "message",
			Trust: "owner", Payload: payload, Received: time.Now().Unix(),
		}); err != nil || !inserted {
			t.Fatalf("stage event: inserted=%v err=%v", inserted, err)
		}
		if err := db.Close(); err != nil {
			t.Fatalf("close staging store: %v", err)
		}
	}

	var daemonOut, daemonErr strings.Builder
	done := make(chan int, 1)
	go func() {
		done <- cli.Run([]string{"daemon", "--state", state, "--config", filepath.Join(home, "approach.toml")}, &daemonOut, &daemonErr)
	}()
	waitDialable(t, socket)

	var out, errW strings.Builder
	if code := cli.Run([]string{"drain", "--socket", socket}, &out, &errW); code != 0 {
		t.Fatalf("drain exit = %d, stderr %q", code, errW.String())
	}
	select {
	case code := <-done:
		if code != 0 {
			t.Fatalf("daemon exit = %d, stderr %q", code, daemonErr.String())
		}
	case <-time.After(10 * time.Second):
		t.Fatal("daemon did not exit after drain")
	}

	if !strings.Contains(daemonErr.String(), "no [engine] section") {
		t.Errorf("journal does not warn about the dormant engine: %s", daemonErr.String())
	}
	if strings.Contains(daemonErr.String(), "turn dispatch wired") {
		t.Error("dispatch wired without an [engine] section")
	}

	db, err := store.Open(filepath.Join(state, "approach.db"))
	if err != nil {
		t.Fatalf("reopen store: %v", err)
	}
	defer func() { _ = db.Close() }()
	var turns int
	if err := db.QueryRow(`SELECT count(*) FROM turns`).Scan(&turns); err != nil {
		t.Fatalf("count turns: %v", err)
	}
	if turns != 0 {
		t.Errorf("turns = %d, want 0 — a dormant daemon must not spawn engines", turns)
	}
	var status string
	if err := db.QueryRow(`SELECT status FROM events WHERE dedup_key = 'discord:msg:7'`).Scan(&status); err != nil {
		t.Fatalf("read event: %v", err)
	}
	if status != "processing" {
		t.Errorf("event status = %q, want processing (durably queued; the next boot parks it, §4.6)", status)
	}
}
