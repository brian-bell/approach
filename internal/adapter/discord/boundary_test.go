package discord

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bwmarrin/discordgo"
)

// TestDMClassification: a GuildID=="" message is only a 1:1 DM if the
// channel says so. Group DMs share the empty GuildID but are multi-
// party — keyed as discord:dm:<author> they would masquerade as the
// author's PRIVATE thread and replies would leak to the other
// participants (§4.3 confidentiality) — so they are dropped at the
// boundary. An unclassifiable private channel falls back to one REST
// fetch (cached into state); a fetch that fails drops the message —
// fail closed, loudly (§7).
func TestDMClassification(t *testing.T) {
	newFixture := func(t *testing.T) (*Adapter, *discordgo.Session, *[]*discordgo.MessageCreate) {
		t.Helper()
		var got []*discordgo.MessageCreate
		a, err := New(canaryToken, func(m *discordgo.MessageCreate) { got = append(got, m) }, discardLogger())
		if err != nil {
			t.Fatalf("New: %v", err)
		}
		s := a.session
		s.State.User = &discordgo.User{ID: "bot-self"}
		return a, s, &got
	}
	dm := func(channel string) *discordgo.MessageCreate {
		return &discordgo.MessageCreate{Message: &discordgo.Message{
			ID: "m1", Author: &discordgo.User{ID: "human"}, ChannelID: channel,
		}}
	}

	t.Run("cached 1:1 DM delivered", func(t *testing.T) {
		a, s, got := newFixture(t)
		if err := s.State.ChannelAdd(&discordgo.Channel{ID: "dm-1", Type: discordgo.ChannelTypeDM}); err != nil {
			t.Fatal(err)
		}
		a.onMessageCreate(s, dm("dm-1"))
		if len(*got) != 1 {
			t.Errorf("delivered %d, want 1 — a known 1:1 DM must pass", len(*got))
		}
	})

	t.Run("group DM dropped", func(t *testing.T) {
		a, s, got := newFixture(t)
		if err := s.State.ChannelAdd(&discordgo.Channel{ID: "gdm-1", Type: discordgo.ChannelTypeGroupDM}); err != nil {
			t.Fatal(err)
		}
		a.onMessageCreate(s, dm("gdm-1"))
		if len(*got) != 0 {
			t.Error("a group DM crossed the boundary — its thread key would impersonate a private 1:1 thread")
		}
	})

	t.Run("unknown channel resolved by one cached fetch", func(t *testing.T) {
		a, s, got := newFixture(t)
		var fetches int
		a.fetchChannel = func(_ *discordgo.Session, id string) (*discordgo.Channel, error) {
			fetches++
			return &discordgo.Channel{ID: id, Type: discordgo.ChannelTypeDM}, nil
		}
		a.onMessageCreate(s, dm("dm-2"))
		a.onMessageCreate(s, dm("dm-2"))
		if len(*got) != 2 {
			t.Errorf("delivered %d, want 2 — a REST-classified DM must pass", len(*got))
		}
		if fetches != 1 {
			t.Errorf("fetches = %d, want 1 — the classification must cache into state", fetches)
		}
	})

	t.Run("unknown channel with failed fetch dropped", func(t *testing.T) {
		a, s, got := newFixture(t)
		a.fetchChannel = func(*discordgo.Session, string) (*discordgo.Channel, error) {
			return nil, errors.New("rest down")
		}
		a.onMessageCreate(s, dm("dm-3"))
		if len(*got) != 0 {
			t.Error("an unclassifiable private channel crossed the boundary — must fail closed (§7)")
		}
	})

	t.Run("session reset does not clobber the fetch seam", func(t *testing.T) {
		// fetchChannel is read from the handler goroutine while Run's
		// goroutine rebuilds sessions (resetSession after 4007/4009) —
		// discordgo's close path does not join handlers, so a write in
		// adopt would be a data race. The field must be write-once.
		a, _, _ := newFixture(t)
		marker := errors.New("test seam")
		a.fetchChannel = func(*discordgo.Session, string) (*discordgo.Channel, error) {
			return nil, marker
		}
		if err := a.reset(); err != nil {
			t.Fatalf("reset: %v", err)
		}
		if _, err := a.fetchChannel(a.session, "any"); !errors.Is(err, marker) {
			t.Error("resetSession replaced fetchChannel — a handler racing the rebuild reads a torn seam")
		}
	})
}

// TestRunJoinsInFlightHandlers: discordgo's Close stops the listener
// but never joins it — a message decoded before the close can still be
// mid-handler when Run's loop exits. Run must wait for that handler:
// the daemon closes the store right after Run returns, and an insert
// racing a closing db loses the event (§4.1).
func TestRunJoinsInFlightHandlers(t *testing.T) {
	entered := make(chan struct{})
	release := make(chan struct{})
	var calls atomic.Int32
	a, err := New(canaryToken, func(*discordgo.MessageCreate) {
		if calls.Add(1) == 1 {
			close(entered)
			<-release
		}
	}, discardLogger())
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	a.open = func() error { return nil }
	a.closeFn = func() error { return nil }
	a.session.State.User = &discordgo.User{ID: "bot-self"}
	if err := a.session.State.ChannelAdd(&discordgo.Channel{ID: "dm-1", Type: discordgo.ChannelTypeDM}); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	runDone := make(chan error, 1)
	go func() { runDone <- a.Run(ctx) }()

	// Simulate the listen goroutine dispatching mid-shutdown.
	go a.onMessageCreate(a.session, &discordgo.MessageCreate{Message: &discordgo.Message{
		ID: "m1", Author: &discordgo.User{ID: "human"}, ChannelID: "dm-1",
	}})
	<-entered
	cancel()

	select {
	case <-runDone:
		t.Fatal("Run returned while a handler was still executing — the daemon would close the store under it")
	case <-time.After(100 * time.Millisecond):
	}
	close(release)
	select {
	case <-runDone:
	case <-time.After(5 * time.Second):
		t.Fatal("Run never returned after the last handler finished")
	}

	// After Run has returned the adapter is unsupervised: a straggler
	// dispatch must be dropped (loudly), never handed to a handler
	// whose store may already be closed.
	a.onMessageCreate(a.session, &discordgo.MessageCreate{Message: &discordgo.Message{
		ID: "m2", Author: &discordgo.User{ID: "human"}, ChannelID: "dm-1",
	}})
	if calls.Load() != 1 {
		t.Error("a message dispatched after Run returned reached the handler")
	}
}
