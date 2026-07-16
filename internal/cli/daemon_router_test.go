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

// TestDaemonStartupRebuildParksCrashInterrupted drives the §4.1/§4.6
// restart path through the real daemon: a row left 'processing' by a
// crash parks as interrupted at startup — never auto-rerun — while a
// queued 'received' row is dispatched with the pre-turn processing
// stamp (the M1 scaffold performs no completion, so it stays
// 'processing' for the NEXT restart to park).
func TestDaemonStartupRebuildParksCrashInterrupted(t *testing.T) {
	dir, err := os.MkdirTemp("", "cli")
	if err != nil {
		t.Fatalf("make temp dir: %v", err)
	}
	t.Cleanup(func() {
		if err := os.RemoveAll(dir); err != nil {
			t.Errorf("remove temp dir: %v", err)
		}
	})
	state := filepath.Join(dir, "state")
	socket := filepath.Join(state, "approach.sock")

	// Seed the store the daemon will reopen: one crash-interrupted
	// turn, one still-queued event.
	db, err := store.Open(filepath.Join(state, "approach.db"))
	if err != nil {
		t.Fatalf("seed store: %v", err)
	}
	ctx := context.Background()
	for i, status := range []string{"processing", "received"} {
		ev := store.Event{
			DedupKey:  fmt.Sprintf("discord:msg:%d", i+1),
			ThreadKey: "discord:dm:a",
			Kind:      "message",
			Trust:     "owner",
			Payload: fmt.Sprintf(
				`{"dedup_key":"discord:msg:%d","thread_key":"discord:dm:a","kind":"message","trust":"owner"}`, i+1),
			Received: int64(1700000000 + i),
		}
		id, _, err := store.InsertEvent(ctx, db, ev)
		if err != nil {
			t.Fatalf("seed event %d: %v", i+1, err)
		}
		if _, err := db.Exec(`UPDATE events SET status = ? WHERE id = ?`, status, id); err != nil {
			t.Fatalf("set status: %v", err)
		}
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close seed store: %v", err)
	}

	var daemonOut, daemonErr strings.Builder
	daemonDone := make(chan int, 1)
	go func() {
		daemonDone <- cli.Run([]string{"daemon", "--state", state}, &daemonOut, &daemonErr)
	}()
	waitDialable(t, socket)

	// The queued row's dispatch runs on its thread's queue goroutine,
	// and dispatch quietly REFUSES once shutdown begins (leaving the
	// row 'received' for the next boot — correct router behavior, not
	// what this test probes). An immediate drain can win that race on
	// a slow runner, so wait for the pre-turn stamp — the §4.1
	// contract under test — before draining. WAL allows this second
	// reader alongside the daemon's own connection.
	pollDB, err := store.Open(filepath.Join(state, "approach.db"))
	if err != nil {
		t.Fatalf("open poll store: %v", err)
	}
	deadline := time.Now().Add(10 * time.Second)
	for {
		var status string
		err := pollDB.QueryRow(`SELECT status FROM events WHERE dedup_key = 'discord:msg:2'`).Scan(&status)
		if err == nil && status == "processing" {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("discord:msg:2 never reached 'processing' (last: %q, err: %v)", status, err)
		}
		time.Sleep(10 * time.Millisecond)
	}
	if err := pollDB.Close(); err != nil {
		t.Fatalf("close poll store: %v", err)
	}

	var out, errW strings.Builder
	if code := cli.Run([]string{"drain", "--socket", socket}, &out, &errW); code != 0 {
		t.Fatalf("drain exit = %d, stderr %q", code, errW.String())
	}
	select {
	case code := <-daemonDone:
		if code != 0 {
			t.Fatalf("daemon exit = %d, stderr %q", code, daemonErr.String())
		}
	case <-time.After(10 * time.Second):
		t.Fatal("daemon did not exit after drain")
	}

	db, err = store.Open(filepath.Join(state, "approach.db"))
	if err != nil {
		t.Fatalf("reopen store: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	for dedup, want := range map[string]string{
		"discord:msg:1": "interrupted", // crash mid-turn → parked (§4.6)
		"discord:msg:2": "processing",  // dispatched with the pre-turn stamp; completion is epic 1.3
	} {
		var got string
		if err := db.QueryRow(`SELECT status FROM events WHERE dedup_key = ?`, dedup).Scan(&got); err != nil {
			t.Fatalf("read back %s: %v", dedup, err)
		}
		if got != want {
			t.Errorf("%s status after daemon restart = %q, want %q", dedup, got, want)
		}
	}
}
