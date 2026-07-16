package store_test

import (
	"context"
	"fmt"
	"path/filepath"
	"testing"

	"github.com/brian-bell/approach/internal/store"
)

// TestUnprocessedEventsOrderAndFilter: the queue rebuild query (§4.1)
// returns only rows still owed a turn — status received or processing —
// in id (receipt) order, ready to be indexed per thread on restart.
func TestUnprocessedEventsOrderAndFilter(t *testing.T) {
	db := mustOpen(t, filepath.Join(t.TempDir(), "state", "approach.db"))
	ctx := context.Background()

	// Three threads, five events, inserted in a deliberate interleave.
	for i, tk := range []string{"discord:dm:a", "discord:dm:b", "discord:dm:a", "discord:dm:c", "discord:dm:b"} {
		ev := store.Event{
			DedupKey:  fmt.Sprintf("discord:msg:%d", i+1),
			ThreadKey: tk,
			Kind:      "message",
			Trust:     "owner",
			Payload: fmt.Sprintf(
				`{"dedup_key":"discord:msg:%d","thread_key":"%s","kind":"message","trust":"owner"}`, i+1, tk),
			Received: int64(1700000000 + i),
		}
		if _, _, err := store.InsertEvent(ctx, db, ev); err != nil {
			t.Fatalf("InsertEvent %d: %v", i+1, err)
		}
	}
	// Advance two rows out of the queue: completed history and a dead
	// letter must never be re-dispatched after a restart.
	for dedup, status := range map[string]string{
		"discord:msg:2": "completed",
		"discord:msg:4": "dead",
	} {
		if _, err := db.Exec(`UPDATE events SET status = ? WHERE dedup_key = ?`, status, dedup); err != nil {
			t.Fatalf("advance %s: %v", dedup, err)
		}
	}
	// A processing row (crash mid-turn) IS still unprocessed — restart
	// recovery owns its disposition (§4.6), so the rebuild must see it.
	if _, err := db.Exec(`UPDATE events SET status = 'processing' WHERE dedup_key = 'discord:msg:3'`); err != nil {
		t.Fatalf("mark processing: %v", err)
	}

	rows, err := store.UnprocessedEvents(ctx, db)
	if err != nil {
		t.Fatalf("UnprocessedEvents: %v", err)
	}
	var got []string
	for _, r := range rows {
		got = append(got, r.DedupKey)
	}
	want := []string{"discord:msg:1", "discord:msg:3", "discord:msg:5"}
	if len(got) != len(want) {
		t.Fatalf("UnprocessedEvents returned %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("UnprocessedEvents returned %v, want %v (id order = receipt order)", got, want)
		}
	}
	// The row carries everything dispatch needs — a rebuild that had to
	// re-query per event would race fresh ingest writes.
	first := rows[0]
	if first.ID <= 0 || first.ThreadKey != "discord:dm:a" || first.Kind != "message" ||
		first.Trust != "owner" || first.Status != "received" || first.Payload == "" || first.Received != 1700000000 {
		t.Errorf("row did not round-trip: %+v", first)
	}
	if rows[1].Status != "processing" {
		t.Errorf("crash-interrupted row status = %q, want processing preserved for recovery (§4.6)", rows[1].Status)
	}
}

// TestParkEventGuards: parking only advances rows still owed a turn —
// a row the handler already finished (completed, dead) must not be
// resurrected as visible failure, while received/processing rows park
// as interrupted (§4.6).
func TestParkEventGuards(t *testing.T) {
	db := mustOpen(t, filepath.Join(t.TempDir(), "state", "approach.db"))
	ctx := context.Background()

	for i, status := range []string{"received", "processing", "completed"} {
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
			t.Fatalf("InsertEvent %d: %v", i+1, err)
		}
		if _, err := db.Exec(`UPDATE events SET status = ? WHERE id = ?`, status, id); err != nil {
			t.Fatalf("set status %s: %v", status, err)
		}
		if err := store.ParkEvent(ctx, db, id, 1700009999); err != nil {
			t.Fatalf("ParkEvent(%s): %v", status, err)
		}
		var got string
		if err := db.QueryRow(`SELECT status FROM events WHERE id = ?`, id).Scan(&got); err != nil {
			t.Fatalf("read back: %v", err)
		}
		want := "interrupted"
		if status == "completed" {
			want = "completed"
		}
		if got != want {
			t.Errorf("ParkEvent over %s row: status = %q, want %q", status, got, want)
		}
	}
}

// TestMarkEventCompleted: the post-turn transition (§4.1) — only a
// processing row completes (a turn runs from processing; anything else
// is a caller bug), and the guard failing is an error, never a silent
// re-completion of history.
func TestMarkEventCompleted(t *testing.T) {
	db := mustOpen(t, filepath.Join(t.TempDir(), "state", "approach.db"))
	ctx := context.Background()

	for i, tc := range []struct {
		status  string
		wantErr bool
	}{
		{"processing", false},
		{"received", true},
		{"completed", true},
		{"interrupted", true},
	} {
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
			t.Fatalf("InsertEvent %d: %v", i+1, err)
		}
		if _, err := db.Exec(`UPDATE events SET status = ? WHERE id = ?`, tc.status, id); err != nil {
			t.Fatalf("set status %s: %v", tc.status, err)
		}
		err = store.MarkEventCompleted(ctx, db, id, 1700009999)
		if tc.wantErr {
			if err == nil {
				t.Errorf("MarkEventCompleted over %s row: want error, got nil", tc.status)
			}
			continue
		}
		if err != nil {
			t.Fatalf("MarkEventCompleted over %s row: %v", tc.status, err)
		}
		var got string
		var updated int64
		if err := db.QueryRow(`SELECT status, updated FROM events WHERE id = ?`, id).Scan(&got, &updated); err != nil {
			t.Fatalf("read back: %v", err)
		}
		if got != "completed" || updated != 1700009999 {
			t.Errorf("MarkEventCompleted: status = %q updated = %d, want completed / 1700009999", got, updated)
		}
	}

	// A missing row is a caller bug, not a quiet success.
	if err := store.MarkEventCompleted(ctx, db, 9999, 1700009999); err == nil {
		t.Error("MarkEventCompleted on a missing row: want error, got nil")
	}
}

// TestSkipEvent: the deliberate-refusal transition — a claimed
// (processing) event the daemon chose not to run leaves the queue as
// 'skipped'; any other state refuses loud (skipping unclaimed or
// finished history would erase queue truth).
func TestSkipEvent(t *testing.T) {
	db := mustOpen(t, filepath.Join(t.TempDir(), "state", "approach.db"))
	ctx := context.Background()

	for i, tc := range []struct {
		status  string
		wantErr bool
	}{
		{"processing", false},
		{"received", true},
		{"completed", true},
	} {
		ev := store.Event{
			DedupKey:  fmt.Sprintf("discord:msg:s%d", i+1),
			ThreadKey: "discord:dm:a",
			Kind:      "message",
			Trust:     "untrusted",
			Payload: fmt.Sprintf(
				`{"dedup_key":"discord:msg:s%d","thread_key":"discord:dm:a","kind":"message","trust":"untrusted"}`, i+1),
			Received: int64(1700000000 + i),
		}
		id, _, err := store.InsertEvent(ctx, db, ev)
		if err != nil {
			t.Fatalf("InsertEvent %d: %v", i+1, err)
		}
		if _, err := db.Exec(`UPDATE events SET status = ? WHERE id = ?`, tc.status, id); err != nil {
			t.Fatalf("set status %s: %v", tc.status, err)
		}
		err = store.SkipEvent(ctx, db, id, 1700009999)
		if tc.wantErr {
			if err == nil {
				t.Errorf("SkipEvent over %s row: want error, got nil", tc.status)
			}
			continue
		}
		if err != nil {
			t.Fatalf("SkipEvent over %s row: %v", tc.status, err)
		}
		var got string
		if err := db.QueryRow(`SELECT status FROM events WHERE id = ?`, id).Scan(&got); err != nil {
			t.Fatalf("read back: %v", err)
		}
		if got != "skipped" {
			t.Errorf("SkipEvent: status = %q, want skipped", got)
		}
	}
}

// TestUnprocessedEventsEmpty: an empty (or fully-drained) queue rebuilds
// to nothing — no error, no phantom rows.
func TestUnprocessedEventsEmpty(t *testing.T) {
	db := mustOpen(t, filepath.Join(t.TempDir(), "state", "approach.db"))
	rows, err := store.UnprocessedEvents(context.Background(), db)
	if err != nil {
		t.Fatalf("UnprocessedEvents on empty store: %v", err)
	}
	if len(rows) != 0 {
		t.Errorf("got %d rows from an empty store, want 0", len(rows))
	}
}
