package cli

import (
	"context"
	"database/sql"
	"encoding/json"
	"log/slog"
	"time"

	"github.com/brian-bell/approach/internal/adapter/discord"
	"github.com/brian-bell/approach/internal/store"
	"github.com/bwmarrin/discordgo"
)

// discordIngest is the write-on-receipt path (§4.1): normalize the
// inbound message into the §6 contract and persist it BEFORE any
// processing — the gateway does not redeliver, so this insert is the
// last moment durability is free. Every exit is loud: a refused
// message WARNs, a failed insert ERRORs (that log line is the only
// trace the message ever existed), a duplicate collapses quietly
// (§4.1: dup delivery → one turn). Message content is never logged on
// any path — it is externally-authored data and the journal is not
// the event store (§7).
//
// The clock is injected: receipt time is the adapter's to stamp (§6),
// and the tests own it.
func discordIngest(db *sql.DB, logger *slog.Logger, now func() time.Time) discord.MessageHandler {
	return func(m *discordgo.MessageCreate) {
		ev, err := discord.Normalize(m)
		if err != nil {
			logger.Warn("discord message dropped — cannot carry a §6 identity", "error", err.Error())
			return
		}
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
