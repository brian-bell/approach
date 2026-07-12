package cli

import (
	"context"
	"database/sql"
	"encoding/json"
	"log/slog"
	"time"

	"github.com/brian-bell/approach/internal/adapter/discord"
	"github.com/brian-bell/approach/internal/event"
	"github.com/brian-bell/approach/internal/store"
	"github.com/brian-bell/approach/internal/trust"
	"github.com/bwmarrin/discordgo"
)

// discordIngest is the write-on-receipt path (§4.1): normalize the
// inbound message into the §6 contract, stamp trust from the
// identities lookup, and persist BEFORE any processing — the gateway
// does not redeliver, so this insert is the last moment durability is
// free. Every exit is loud: a refused message WARNs, a failed insert
// ERRORs (that log line is the only trace the message ever existed),
// a duplicate collapses quietly (§4.1: dup delivery → one turn).
// Message content is never logged on any path — it is externally-
// authored data and the journal is not the event store (§7).
//
// auth is the channel's configured auth grade ("strong"/"weak"),
// re-applied as a clamp at stamping time (§7: the identities table
// can drift past config validation). The clock is injected: receipt
// time is the adapter's to stamp (§6), and the tests own it.
func discordIngest(db *sql.DB, auth string, logger *slog.Logger, now func() time.Time) discord.MessageHandler {
	return func(m *discordgo.MessageCreate) {
		ev, err := discord.Normalize(m)
		if err != nil {
			logger.Warn("discord message dropped — cannot carry a §6 identity", "error", err.Error())
			return
		}
		stampEvent(&ev, db, auth, logger)
		payload, err := json.Marshal(ev)
		if err != nil {
			// Unreachable for a struct of strings, but an unlogged
			// impossible-error is how a future field makes drops silent.
			logger.Error("discord event payload marshal failed", "dedup_key", ev.DedupKey, "error", err.Error())
			return
		}
		inserted, err := store.InsertEvent(context.Background(), db, store.Event{
			DedupKey:  ev.DedupKey,
			ThreadKey: ev.ThreadKey,
			Kind:      ev.Kind,
			Trust:     ev.Trust,
			Payload:   string(payload),
			Received:  now().Unix(),
		})
		if err != nil {
			logger.Error("discord event insert failed — message lost, gateway will not redeliver",
				"dedup_key", ev.DedupKey, "thread_key", ev.ThreadKey, "error", err.Error())
			return
		}
		if !inserted {
			logger.Debug("duplicate discord delivery collapsed", "dedup_key", ev.DedupKey)
		}
	}
}

// stampEvent upgrades the event's fail-closed untrusted baseline with
// the §6 identities lookup, clamped by the channel's auth grade. A
// lookup failure keeps the baseline and logs loudly — ambiguity is
// untrusted, never a dropped message: recording the event at bottom
// trust beats losing it, and a broken identities read must not read
// as "clean" (§6).
//
// owner_id rides only a stamp that survived the clamp at owner: the
// approval principal must never be carried by a downgraded event —
// §4.4 matches approvals on owner_id AND a strong-auth channel, and
// stamping both facts from one decision keeps that invariant local.
func stampEvent(ev *event.Event, db *sql.DB, auth string, logger *slog.Logger) {
	stamped, err := store.ResolveStamped(context.Background(), db, ev.Channel, ev.Sender, auth)
	if err != nil {
		logger.Error("identities lookup failed — event stamped untrusted", "thread_key", ev.ThreadKey, "error", err.Error())
	}
	ev.Trust = string(stamped.Trust)
	if stamped.Trust != trust.Owner {
		return
	}
	ownerID, ok, err := store.ResolveOwnerID(context.Background(), db, ev.Channel, ev.Sender)
	if err != nil {
		logger.Error("owner_id lookup failed — event carries no approval principal", "thread_key", ev.ThreadKey, "error", err.Error())
		return
	}
	if ok {
		ev.OwnerID = &ownerID
	}
}
