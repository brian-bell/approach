package discord

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/bwmarrin/discordgo"
)

// sendFixture wires the REST seams to fakes and records calls.
type sendFixture struct {
	a        *Adapter
	sent     []struct{ channel, content string }
	dmCreate []string
}

func newSendFixture(t *testing.T) *sendFixture {
	t.Helper()
	f := &sendFixture{a: newTestAdapter(t)}
	f.a.sendMessage = func(_ context.Context, _ *discordgo.Session, channelID, content string) (*discordgo.Message, error) {
		f.sent = append(f.sent, struct{ channel, content string }{channelID, content})
		return &discordgo.Message{ID: "out-1"}, nil
	}
	f.a.createDMChannel = func(_ context.Context, _ *discordgo.Session, userID string) (*discordgo.Channel, error) {
		f.dmCreate = append(f.dmCreate, userID)
		return &discordgo.Channel{ID: "dmchan-" + userID, Type: discordgo.ChannelTypeDM}, nil
	}
	return f
}

// TestSendToThread: a thread key sends straight to the thread's
// channel id and returns the platform ack in the shared
// discord:msg:<id> spelling — the same key family as inbound
// dedup/reply keys, so a delivery ack correlates with future replies.
func TestSendToThread(t *testing.T) {
	f := newSendFixture(t)
	ack, err := f.a.Send(context.Background(), "discord:thread:th-9", "hello")
	if err != nil {
		t.Fatalf("Send: %v", err)
	}
	if ack != "discord:msg:out-1" {
		t.Errorf("ack = %q, want discord:msg:out-1", ack)
	}
	if len(f.sent) != 1 || f.sent[0].channel != "th-9" || f.sent[0].content != "hello" {
		t.Errorf("sent = %+v, want one send to th-9", f.sent)
	}
	if len(f.dmCreate) != 0 {
		t.Errorf("created a DM channel for a thread send: %v", f.dmCreate)
	}
}

// TestSendToDM: a dm key resolves the user's DM channel first — the §6
// thread key holds the USER id (durable conversation identity), not
// the platform's channel id.
func TestSendToDM(t *testing.T) {
	f := newSendFixture(t)
	ack, err := f.a.Send(context.Background(), "discord:dm:user-7", "hi")
	if err != nil {
		t.Fatalf("Send: %v", err)
	}
	if ack != "discord:msg:out-1" {
		t.Errorf("ack = %q, want discord:msg:out-1", ack)
	}
	if len(f.dmCreate) != 1 || f.dmCreate[0] != "user-7" {
		t.Errorf("dm creates = %v, want [user-7]", f.dmCreate)
	}
	if len(f.sent) != 1 || f.sent[0].channel != "dmchan-user-7" {
		t.Errorf("sent = %+v, want one send to dmchan-user-7", f.sent)
	}
}

// TestSendDMChannelCached: the user→channel resolution costs one REST
// round-trip total, not one per message — the mapping is stable and
// the send path sits under Discord's rate limits.
func TestSendDMChannelCached(t *testing.T) {
	f := newSendFixture(t)
	for range 3 {
		if _, err := f.a.Send(context.Background(), "discord:dm:user-7", "x"); err != nil {
			t.Fatalf("Send: %v", err)
		}
	}
	if len(f.dmCreate) != 1 {
		t.Errorf("dm creates = %d, want 1 — resolution must cache", len(f.dmCreate))
	}
}

// TestSendRefusesForeignKeys: a key the discord adapter does not own,
// or one with no id, is refused before any REST call — a mis-keyed
// delivery must fail, never land in a wrong channel.
func TestSendRefusesForeignKeys(t *testing.T) {
	for _, key := range []string{
		"slack:C1:169.1",
		"discord:dm:",
		"discord:thread:",
		"discord:msg:123",
		"task:approach-1",
		"",
	} {
		t.Run("key "+key, func(t *testing.T) {
			f := newSendFixture(t)
			_, err := f.a.Send(context.Background(), key, "x")
			if err == nil {
				t.Fatalf("Send accepted foreign key %q", key)
			}
			if !strings.Contains(err.Error(), "thread key") {
				t.Errorf("error %q does not name the key problem", err)
			}
			if len(f.sent) != 0 || len(f.dmCreate) != 0 {
				t.Error("a refused key still reached a REST seam")
			}
		})
	}
}

// TestSendSurfacesRESTErrorAndAcklessAccept: the outbox advances an
// event to replied only on ack (§4.1) — a send error must propagate,
// and a "successful" response carrying no message id is an error too,
// never an empty ack the caller might record as delivered.
func TestSendSurfacesRESTErrorAndAcklessAccept(t *testing.T) {
	f := newSendFixture(t)
	restErr := errors.New("rest 502")
	f.a.sendMessage = func(context.Context, *discordgo.Session, string, string) (*discordgo.Message, error) {
		return nil, restErr
	}
	if _, err := f.a.Send(context.Background(), "discord:thread:t1", "x"); !errors.Is(err, restErr) {
		t.Errorf("Send error = %v, want the REST error surfaced", err)
	}

	f.a.sendMessage = func(context.Context, *discordgo.Session, string, string) (*discordgo.Message, error) {
		return &discordgo.Message{}, nil
	}
	if _, err := f.a.Send(context.Background(), "discord:thread:t1", "x"); err == nil {
		t.Error("Send returned nil error for an ack-less accept — the caller would mark replied on nothing")
	}
}

// TestSendErrorOmitsContent: message content is externally-visible
// only on the platform — an error (which lands in the journal) must
// never carry it (§7).
func TestSendErrorOmitsContent(t *testing.T) {
	f := newSendFixture(t)
	f.a.sendMessage = func(context.Context, *discordgo.Session, string, string) (*discordgo.Message, error) {
		return nil, errors.New("boom")
	}
	_, err := f.a.Send(context.Background(), "discord:thread:t1", "s3cret-reply")
	if err == nil {
		t.Fatal("want error")
	}
	if strings.Contains(err.Error(), "s3cret") {
		t.Errorf("error %q leaked message content", err)
	}
}

// TestSendHonorsCancelledContext: a drain must not queue more REST
// work — a cancelled context refuses before any call.
func TestSendHonorsCancelledContext(t *testing.T) {
	f := newSendFixture(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := f.a.Send(ctx, "discord:thread:t1", "x"); err == nil {
		t.Error("Send proceeded under a cancelled context")
	}
	if len(f.sent) != 0 {
		t.Error("a cancelled Send still reached the REST seam")
	}
}

// TestSendStaleDMChannelReResolved: the platform may re-mint a DM
// channel (the reason the §6 dm key holds the USER id, not the
// channel id) — a send failure through a cached channel must drop the
// cache entry so the outbox's next retry re-resolves instead of
// re-sending into the dead channel forever.
func TestSendStaleDMChannelReResolved(t *testing.T) {
	f := newSendFixture(t)
	generation := 0
	f.a.createDMChannel = func(_ context.Context, _ *discordgo.Session, userID string) (*discordgo.Channel, error) {
		generation++
		return &discordgo.Channel{ID: fmt.Sprintf("dmchan-gen%d", generation)}, nil
	}
	f.a.sendMessage = func(_ context.Context, _ *discordgo.Session, channelID, _ string) (*discordgo.Message, error) {
		if channelID == "dmchan-gen1" {
			return nil, errors.New("HTTP 404 Not Found, {\"code\": 10003}") // unknown channel
		}
		return &discordgo.Message{ID: "out-2"}, nil
	}

	if _, err := f.a.Send(context.Background(), "discord:dm:u1", "x"); err == nil {
		t.Fatal("send into the stale channel should fail")
	}
	ack, err := f.a.Send(context.Background(), "discord:dm:u1", "x")
	if err != nil {
		t.Fatalf("retry after invalidation: %v", err)
	}
	if ack != "discord:msg:out-2" {
		t.Errorf("ack = %q, want discord:msg:out-2", ack)
	}
	if generation != 2 {
		t.Errorf("createDMChannel generations = %d, want 2 — the stale entry must be dropped", generation)
	}
}

// TestSendFailedDMResolutionNotCached: a failed channel resolution
// must not poison the cache — the next attempt retries the lookup.
func TestSendFailedDMResolutionNotCached(t *testing.T) {
	f := newSendFixture(t)
	calls := 0
	f.a.createDMChannel = func(_ context.Context, _ *discordgo.Session, userID string) (*discordgo.Channel, error) {
		calls++
		if calls == 1 {
			return nil, errors.New("rest down")
		}
		return &discordgo.Channel{ID: "dmchan-" + userID}, nil
	}
	if _, err := f.a.Send(context.Background(), "discord:dm:u1", "x"); err == nil {
		t.Fatal("first Send should fail with the resolution error")
	}
	if _, err := f.a.Send(context.Background(), "discord:dm:u1", "x"); err != nil {
		t.Fatalf("second Send: %v", err)
	}
	if calls != 2 {
		t.Errorf("createDMChannel calls = %d, want 2 (failure retried, success cached)", calls)
	}
}
