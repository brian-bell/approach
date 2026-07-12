package discord

import (
	"errors"

	"github.com/brian-bell/approach/internal/event"
	"github.com/bwmarrin/discordgo"
)

// Normalize converts one inbound Discord message into the §6 event
// contract. It is a pure function of the message: no I/O, no clock —
// receipt time and persistence belong to the handler that calls it.
//
// Key contract (§6):
//   - thread_key: discord:dm:<user_id> for DMs — the USER is the
//     durable conversation identity, not the DM channel id, which the
//     platform can re-mint; discord:thread:<thread_id> for guild
//     threads (the connection layer guarantees DM-or-thread, §3 C1).
//   - dedup_key: discord:msg:<message_id> — channel + native message
//     id, the §6 message identity; duplicate gateway delivery
//     collapses on it (§4.1).
//   - sender: the native user id — exactly what the identities table
//     keys off (§6). Label/owner resolution is the x6n.1.3 slice.
//   - trust: untrusted, unconditionally, until the identities lookup
//     lands (x6n.1.3) — ambiguity is untrusted, never blank (§6).
//     Bot-authored messages normalize like any other sender: the
//     trust model contains them, and whether an untrusted event earns
//     a turn is router policy, not adapter policy.
//
// A message that cannot carry an honest identity — no author, no
// message id, no channel id on the thread path — is refused with an
// error naming the gap: a blank field would either fail the store's
// validation anyway or, worse, collide every such message into one
// dedup_key. The caller drops refused messages loudly, never silently.
func Normalize(m *discordgo.MessageCreate) (event.Event, error) {
	if m == nil || m.Message == nil {
		return event.Event{}, errors.New("discord: normalize: nil message")
	}
	if m.Author == nil || m.Author.ID == "" {
		return event.Event{}, errors.New("discord: normalize: message has no author id — cannot stamp a sender identity")
	}
	if m.ID == "" {
		return event.Event{}, errors.New("discord: normalize: blank message id — the event would have no dedup identity (§6)")
	}
	threadKey := "discord:dm:" + m.Author.ID
	if m.GuildID != "" {
		if m.ChannelID == "" {
			return event.Event{}, errors.New("discord: normalize: guild message with blank channel id — no thread identity (§6)")
		}
		threadKey = "discord:thread:" + m.ChannelID
	}
	ev := event.Event{
		Channel:   "discord",
		ThreadKey: threadKey,
		DedupKey:  "discord:msg:" + m.ID,
		Sender:    m.Author.ID,
		Trust:     "untrusted",
		Kind:      "message",
		Text:      m.Content,
	}
	if ref := m.MessageReference; ref != nil && ref.MessageID != "" {
		reply := "discord:msg:" + ref.MessageID
		ev.ReplyTo = &reply
	}
	return ev, nil
}
