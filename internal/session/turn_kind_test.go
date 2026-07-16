package session_test

import (
	"context"
	"testing"
)

// TestTurnThreadsEventKindToEngine: the event kind that drove a turn
// reaches the engine's Spec on both lifecycle paths — it is what the
// C11 turns row records (§6), so cost can be attributed per entry
// point. First turn and resume alike.
func TestTurnThreadsEventKindToEngine(t *testing.T) {
	db := mustOpen(t)
	eng := &resumeEngine{}
	m := newManager(db, eng, 1700000000)
	ctx := context.Background()
	cwd := t.TempDir()

	// Fresh thread: the event's turn is the session's FIRST turn.
	if err := m.Turn(ctx, "discord:dm:a", "owner", cwd, "message", "hi"); err != nil {
		t.Fatalf("Turn (first): %v", err)
	}
	if len(eng.specs) != 1 {
		t.Fatalf("engine started %d times, want 1", len(eng.specs))
	}
	if got := eng.specs[0].Kind; got != "message" {
		t.Errorf("first-turn spec kind = %q, want %q", got, "message")
	}

	// Active session: the next event resumes — kind still travels.
	if err := m.Turn(ctx, "discord:dm:a", "owner", cwd, "cron", "nudge"); err != nil {
		t.Fatalf("Turn (resume): %v", err)
	}
	if len(eng.resumes) != 1 {
		t.Fatalf("engine resumed %d times, want 1", len(eng.resumes))
	}
	if got := eng.resumes[0].Kind; got != "cron" {
		t.Errorf("resume spec kind = %q, want %q", got, "cron")
	}
}
