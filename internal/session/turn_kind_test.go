package session_test

import (
	"context"
	"testing"

	"github.com/brian-bell/approach/internal/session"
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
	if err := m.Turn(ctx, session.TurnRequest{ThreadKey: "discord:dm:a", TrustFloor: "owner", Cwd: cwd, Kind: "message", Prompt: "hi"}); err != nil {
		t.Fatalf("Turn (first): %v", err)
	}
	if len(eng.specs) != 1 {
		t.Fatalf("engine started %d times, want 1", len(eng.specs))
	}
	if got := eng.specs[0].Kind; got != "message" {
		t.Errorf("first-turn spec kind = %q, want %q", got, "message")
	}

	// Active session: the next event resumes — kind still travels.
	if err := m.Turn(ctx, session.TurnRequest{ThreadKey: "discord:dm:a", TrustFloor: "owner", Cwd: cwd, Kind: "cron", Prompt: "nudge"}); err != nil {
		t.Fatalf("Turn (resume): %v", err)
	}
	if len(eng.resumes) != 1 {
		t.Fatalf("engine resumed %d times, want 1", len(eng.resumes))
	}
	if got := eng.resumes[0].Kind; got != "cron" {
		t.Errorf("resume spec kind = %q, want %q", got, "cron")
	}
}

// TestTurnThreadsOutputSinkToEngine: the reply sink reaches the
// engine's Spec on both lifecycle paths — without it a real turn's
// answer has nowhere to go (§4.1 reply relay).
func TestTurnThreadsOutputSinkToEngine(t *testing.T) {
	db := mustOpen(t)
	eng := &resumeEngine{}
	m := newManager(db, eng, 1700000000)
	ctx := context.Background()
	cwd := t.TempDir()

	var got []string
	sink := func(delta string) { got = append(got, delta) }

	if err := m.Turn(ctx, session.TurnRequest{ThreadKey: "discord:dm:a", TrustFloor: "owner", Cwd: cwd, Kind: "message", Prompt: "hi", Output: sink}); err != nil {
		t.Fatalf("Turn (first): %v", err)
	}
	if len(eng.specs) != 1 || eng.specs[0].Output == nil {
		t.Fatal("first-turn spec carries no Output sink")
	}
	eng.specs[0].Output("from start")

	if err := m.Turn(ctx, session.TurnRequest{ThreadKey: "discord:dm:a", TrustFloor: "owner", Cwd: cwd, Kind: "message", Prompt: "again", Output: sink}); err != nil {
		t.Fatalf("Turn (resume): %v", err)
	}
	if len(eng.resumes) != 1 || eng.resumes[0].Output == nil {
		t.Fatal("resume spec carries no Output sink")
	}
	eng.resumes[0].Output("from resume")

	if len(got) != 2 || got[0] != "from start" || got[1] != "from resume" {
		t.Errorf("sink received %q, want [from start, from resume]", got)
	}
}
