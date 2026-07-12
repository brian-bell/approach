package discord

import (
	"strings"
	"testing"

	"github.com/bwmarrin/discordgo"
)

func msg(id, channelID, guildID, authorID, content string) *discordgo.MessageCreate {
	m := &discordgo.MessageCreate{Message: &discordgo.Message{
		ID:        id,
		ChannelID: channelID,
		GuildID:   guildID,
		Content:   content,
	}}
	if authorID != "" {
		m.Author = &discordgo.User{ID: authorID}
	}
	return m
}

// TestNormalizeDM: a direct message stamps the full §6 shape — the
// thread key is the USER (a DM channel id can be re-minted; the user
// id is the durable conversation identity per the §6 contract
// discord:dm:<user_id>).
func TestNormalizeDM(t *testing.T) {
	ev, err := Normalize(msg("9871", "chan1", "", "123", "hello"))
	if err != nil {
		t.Fatalf("Normalize: %v", err)
	}
	if ev.Channel != "discord" {
		t.Errorf("channel = %q, want discord", ev.Channel)
	}
	if ev.ThreadKey != "discord:dm:123" {
		t.Errorf("thread_key = %q, want discord:dm:123 (§6 per-channel contract)", ev.ThreadKey)
	}
	if ev.DedupKey != "discord:msg:9871" {
		t.Errorf("dedup_key = %q, want discord:msg:9871 (§6 message identity)", ev.DedupKey)
	}
	if ev.Sender != "123" {
		t.Errorf("sender = %q, want the native user id 123 (identities lookup keys off it)", ev.Sender)
	}
	if ev.Kind != "message" {
		t.Errorf("kind = %q, want message", ev.Kind)
	}
	if ev.Text != "hello" {
		t.Errorf("text = %q, want hello", ev.Text)
	}
	// Normalize's trust is the fail-closed baseline the ingest handler
	// upgrades after the identities lookup — from here it is always
	// untrusted, never blank (§6).
	if ev.Trust != "untrusted" {
		t.Errorf("trust = %q, want untrusted (fail closed until the identities lookup lands)", ev.Trust)
	}
	if ev.OwnerID != nil {
		t.Errorf("owner_id = %v, want nil before the identities lookup", *ev.OwnerID)
	}
	if ev.ReplyTo != nil {
		t.Errorf("reply_to = %v, want nil without a message reference", *ev.ReplyTo)
	}
	if ev.Occurrence != nil {
		t.Errorf("occurrence = %v, want nil for kind=message", *ev.Occurrence)
	}
}

// TestNormalizeThread: a guild thread message keys the conversation by
// the thread's channel id — discord:thread:<thread_id> (§6).
func TestNormalizeThread(t *testing.T) {
	ev, err := Normalize(msg("555", "thread9", "guild1", "123", "in thread"))
	if err != nil {
		t.Fatalf("Normalize: %v", err)
	}
	if ev.ThreadKey != "discord:thread:thread9" {
		t.Errorf("thread_key = %q, want discord:thread:thread9", ev.ThreadKey)
	}
	if ev.DedupKey != "discord:msg:555" {
		t.Errorf("dedup_key = %q, want discord:msg:555", ev.DedupKey)
	}
}

// TestNormalizeReplyTo: a message reference becomes reply_to in the
// same dedup-key spelling, so the router can correlate without
// knowing Discord's reference shape.
func TestNormalizeReplyTo(t *testing.T) {
	m := msg("2", "chan1", "", "123", "replying")
	m.MessageReference = &discordgo.MessageReference{MessageID: "1"}
	ev, err := Normalize(m)
	if err != nil {
		t.Fatalf("Normalize: %v", err)
	}
	if ev.ReplyTo == nil || *ev.ReplyTo != "discord:msg:1" {
		t.Errorf("reply_to = %v, want discord:msg:1", ev.ReplyTo)
	}

	// A reference with no message id (channel-only references exist in
	// the API) is not a reply — null, never "discord:msg:".
	m = msg("3", "chan1", "", "123", "not a reply")
	m.MessageReference = &discordgo.MessageReference{}
	ev, err = Normalize(m)
	if err != nil {
		t.Fatalf("Normalize: %v", err)
	}
	if ev.ReplyTo != nil {
		t.Errorf("reply_to = %q, want nil for a reference without a message id", *ev.ReplyTo)
	}
}

// TestNormalizeRefusals: a message that cannot carry a §6 identity is
// refused with an error naming what's missing — the handler drops it
// loudly. A blank identity would either fail the store's validation
// anyway or, worse, collide every such message into one dedup_key.
func TestNormalizeRefusals(t *testing.T) {
	cases := []struct {
		name string
		m    *discordgo.MessageCreate
		want string // substring the error must carry
	}{
		{"nil message", &discordgo.MessageCreate{}, "message"},
		{"nil author", msg("1", "chan1", "", "", "x"), "author"},
		{"blank author id", func() *discordgo.MessageCreate {
			m := msg("1", "chan1", "", "", "x")
			m.Author = &discordgo.User{}
			return m
		}(), "author"},
		{"blank message id", msg("", "chan1", "", "123", "x"), "message id"},
		{"blank channel id on thread", msg("1", "", "guild1", "123", "x"), "channel id"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := Normalize(tc.m)
			if err == nil {
				t.Fatal("Normalize accepted a message with no honest §6 identity")
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Errorf("error %q does not name the missing field (%q)", err, tc.want)
			}
		})
	}
}

// TestNormalizeAttachments: platform attachments ride the event as
// externally-authored DATA — url, filename, content type and size
// pass through verbatim (never dereferenced at ingest; egress policy
// is C9/C10 territory), and an entry with no URL is skipped: content
// that cannot be fetched is noise, but its siblings and the message
// survive.
func TestNormalizeAttachments(t *testing.T) {
	m := msg("9", "chan1", "", "123", "see attached")
	m.Attachments = []*discordgo.MessageAttachment{
		{URL: "https://cdn.discordapp.com/a.pdf", Filename: "a.pdf", ContentType: "application/pdf", Size: 12345},
		{URL: "", Filename: "ghost.bin"}, // unfetchable — skipped
		{URL: "https://cdn.discordapp.com/b.png", Filename: "../evil.png", ContentType: "image/png", Size: 99},
	}
	ev, err := Normalize(m)
	if err != nil {
		t.Fatalf("Normalize: %v", err)
	}
	if len(ev.Attachments) != 2 {
		t.Fatalf("attachments = %d, want 2 (blank-URL entry skipped)", len(ev.Attachments))
	}
	a := ev.Attachments[0]
	if a.URL != "https://cdn.discordapp.com/a.pdf" || a.Filename != "a.pdf" ||
		a.ContentType != "application/pdf" || a.Size != 12345 {
		t.Errorf("attachment[0] = %+v, want verbatim platform fields", a)
	}
	// Filename is display-only data and passes through verbatim, path
	// separators included — sanitizing is the CONSUMER's job at point
	// of use, and mangling here would hide what the sender actually
	// supplied from the audit trail.
	if ev.Attachments[1].Filename != "../evil.png" {
		t.Errorf("attachment[1].Filename = %q, want the verbatim externally-authored name", ev.Attachments[1].Filename)
	}
}

// TestNormalizeAttachmentOnlyMessage: an attachment with no text is a
// valid message — refusing it would drop real traffic (screenshots).
func TestNormalizeAttachmentOnlyMessage(t *testing.T) {
	m := msg("10", "chan1", "", "123", "")
	m.Attachments = []*discordgo.MessageAttachment{{URL: "https://cdn.discordapp.com/x.png", Filename: "x.png"}}
	ev, err := Normalize(m)
	if err != nil {
		t.Fatalf("Normalize refused an attachment-only message: %v", err)
	}
	if len(ev.Attachments) != 1 || ev.Text != "" {
		t.Errorf("event = text %q + %d attachments, want empty text + 1 attachment", ev.Text, len(ev.Attachments))
	}
}

// TestNormalizeBotSender: other bots' messages ARE normalized — the
// trust model contains them (unmapped sender ⇒ untrusted, §6) and the
// adapter stays thin; whether an untrusted event earns a turn is
// router policy, not connection policy.
func TestNormalizeBotSender(t *testing.T) {
	m := msg("7", "chan1", "", "botuser", "beep")
	m.Author.Bot = true
	ev, err := Normalize(m)
	if err != nil {
		t.Fatalf("Normalize refused a bot-authored message: %v", err)
	}
	if ev.Trust != "untrusted" {
		t.Errorf("trust = %q, want untrusted", ev.Trust)
	}
}
