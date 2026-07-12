package cli

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"log/slog"
	"path/filepath"
	"testing"
	"time"

	"github.com/brian-bell/approach/internal/store"
	"github.com/bwmarrin/discordgo"
)

func testStore(t *testing.T) *sql.DB {
	t.Helper()
	db, err := store.Open(filepath.Join(t.TempDir(), "state", "approach.db"))
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return db
}

func inbound(id, content string) *discordgo.MessageCreate {
	return &discordgo.MessageCreate{Message: &discordgo.Message{
		ID:        id,
		ChannelID: "chan1",
		Content:   content,
		Author:    &discordgo.User{ID: "123"},
	}}
}

// TestDiscordIngestPersistsEvent: one inbound message becomes one
// events row with the §6 mirrored columns — and the payload passes the
// store's payload-agreement validation, which pins Normalize's output
// against InsertEvent's contract (the two halves cannot drift apart
// without this test failing).
func TestDiscordIngestPersistsEvent(t *testing.T) {
	sdb := testStore(t)
	now := time.Unix(1700000000, 0)
	handle := discordIngest(sdb, "strong", slog.New(slog.NewTextHandler(&bytes.Buffer{}, nil)), func() time.Time { return now })

	handle(inbound("9871", "hello"))

	var dedup, thread, kind, trust string
	var received int64
	err := sdb.QueryRow(`SELECT dedup_key, thread_key, kind, trust, received FROM events`).
		Scan(&dedup, &thread, &kind, &trust, &received)
	if err != nil {
		t.Fatalf("read back event row: %v", err)
	}
	if dedup != "discord:msg:9871" || thread != "discord:dm:123" || kind != "message" || trust != "untrusted" {
		t.Errorf("row = (%s, %s, %s, %s), want the §6 stamped identity", dedup, thread, kind, trust)
	}
	if received != now.Unix() {
		t.Errorf("received = %d, want %d — the adapter owns the receipt clock", received, now.Unix())
	}
}

// TestDiscordIngestDuplicateCollapses is the §4.1 drill: duplicate
// channel delivery → one row, quietly. The gateway can redeliver on
// reconnect; two rows would mean two turns for one message.
func TestDiscordIngestDuplicateCollapses(t *testing.T) {
	sdb := testStore(t)
	var log bytes.Buffer
	handle := discordIngest(sdb, "strong", slog.New(slog.NewTextHandler(&log, nil)), time.Now)

	handle(inbound("1", "once"))
	handle(inbound("1", "once"))

	var n int
	if err := sdb.QueryRow(`SELECT COUNT(*) FROM events`).Scan(&n); err != nil {
		t.Fatalf("count events: %v", err)
	}
	if n != 1 {
		t.Errorf("events rows = %d, want 1 — duplicate delivery must collapse (§4.1)", n)
	}
	if bytes.Contains(log.Bytes(), []byte("ERROR")) {
		t.Errorf("duplicate delivery logged an error — it is a normal no-op:\n%s", log.String())
	}
}

// TestDiscordIngestRefusedMessageIsLoudDrop: a message Normalize
// refuses (no author) is dropped with a WARN — never silently, and
// never a crash.
func TestDiscordIngestRefusedMessageIsLoudDrop(t *testing.T) {
	sdb := testStore(t)
	var log bytes.Buffer
	handle := discordIngest(sdb, "strong", slog.New(slog.NewTextHandler(&log, nil)), time.Now)

	m := inbound("1", "x")
	m.Author = nil
	handle(m)

	var n int
	if err := sdb.QueryRow(`SELECT COUNT(*) FROM events`).Scan(&n); err != nil {
		t.Fatalf("count events: %v", err)
	}
	if n != 0 {
		t.Errorf("events rows = %d, want 0 for a refused message", n)
	}
	if !bytes.Contains(log.Bytes(), []byte("WARN")) {
		t.Errorf("refused message left no WARN in the log — silent drop:\n%s", log.String())
	}
}

// seedTestIdentities enrolls one owner and one known sender, the §6
// lookup fixtures for the stamping tests.
func seedTestIdentities(t *testing.T, db *sql.DB) {
	t.Helper()
	err := store.SeedIdentities(context.Background(), db, []store.Identity{
		{Channel: "discord", NativeID: "owner-1", Trust: "owner", OwnerID: "brian", Label: "Brian"},
		{Channel: "discord", NativeID: "known-1", Trust: "known", Label: "Friend"},
	})
	if err != nil {
		t.Fatalf("seed identities: %v", err)
	}
}

func inboundFrom(authorID string) *discordgo.MessageCreate {
	m := inbound("1", "hi")
	m.Author.ID = authorID
	return m
}

// payloadOf reads back the single event row's payload.
func payloadOf(t *testing.T, db *sql.DB) (trustCol string, payload map[string]any) {
	t.Helper()
	var raw string
	if err := db.QueryRow(`SELECT trust, payload FROM events`).Scan(&trustCol, &raw); err != nil {
		t.Fatalf("read back event: %v", err)
	}
	if err := json.Unmarshal([]byte(raw), &payload); err != nil {
		t.Fatalf("payload is not JSON: %v", err)
	}
	return trustCol, payload
}

// TestDiscordIngestStampsOwner: an enrolled owner's message carries
// trust=owner and the canonical owner_id in the payload — the §6
// identities lookup, stamped at ingest so the queue replays with the
// trust the adapter saw, never re-derived later.
func TestDiscordIngestStampsOwner(t *testing.T) {
	sdb := testStore(t)
	seedTestIdentities(t, sdb)
	handle := discordIngest(sdb, "strong", slog.New(slog.NewTextHandler(&bytes.Buffer{}, nil)), time.Now)

	handle(inboundFrom("owner-1"))

	trustCol, payload := payloadOf(t, sdb)
	if trustCol != "owner" {
		t.Errorf("trust column = %q, want owner", trustCol)
	}
	if payload["trust"] != "owner" || payload["owner_id"] != "brian" {
		t.Errorf("payload trust/owner_id = %v/%v, want owner/brian (§4.4 approval matching needs this)", payload["trust"], payload["owner_id"])
	}
}

// TestDiscordIngestStampsKnown: a known sender stamps known, and never
// carries an owner_id — only owner rows hold the approval principal.
func TestDiscordIngestStampsKnown(t *testing.T) {
	sdb := testStore(t)
	seedTestIdentities(t, sdb)
	handle := discordIngest(sdb, "strong", slog.New(slog.NewTextHandler(&bytes.Buffer{}, nil)), time.Now)

	handle(inboundFrom("known-1"))

	trustCol, payload := payloadOf(t, sdb)
	if trustCol != "known" {
		t.Errorf("trust column = %q, want known", trustCol)
	}
	if payload["owner_id"] != nil {
		t.Errorf("payload owner_id = %v, want null for a known sender", payload["owner_id"])
	}
}

// TestDiscordIngestUnmappedSenderIsUntrusted is the §6 deny-by-default
// drill: no identities row means untrusted — a valid verdict, not an
// error, and never a dropped message.
func TestDiscordIngestUnmappedSenderIsUntrusted(t *testing.T) {
	sdb := testStore(t)
	seedTestIdentities(t, sdb)
	var log bytes.Buffer
	handle := discordIngest(sdb, "strong", slog.New(slog.NewTextHandler(&log, nil)), time.Now)

	handle(inboundFrom("stranger-9"))

	trustCol, payload := payloadOf(t, sdb)
	if trustCol != "untrusted" {
		t.Errorf("trust column = %q, want untrusted (unmapped sender, §6)", trustCol)
	}
	if payload["owner_id"] != nil {
		t.Errorf("payload owner_id = %v, want null", payload["owner_id"])
	}
	if bytes.Contains(log.Bytes(), []byte("ERROR")) {
		t.Errorf("an unmapped sender is not an error:\n%s", log.String())
	}
}

// TestDiscordIngestWeakAuthClamps: even an enrolled OWNER row cannot
// stamp owner through a weak-auth channel — the clamp is re-enforced
// at ingest (§7: the identities table can drift past config
// validation), and a clamped stamp carries no owner_id: the approval
// principal must never ride a spoofable surface (§4.4).
func TestDiscordIngestWeakAuthClamps(t *testing.T) {
	sdb := testStore(t)
	seedTestIdentities(t, sdb)
	handle := discordIngest(sdb, "weak", slog.New(slog.NewTextHandler(&bytes.Buffer{}, nil)), time.Now)

	handle(inboundFrom("owner-1"))

	trustCol, payload := payloadOf(t, sdb)
	if trustCol != "known" {
		t.Errorf("trust column = %q, want known (owner clamped by weak auth)", trustCol)
	}
	if payload["owner_id"] != nil {
		t.Errorf("payload owner_id = %v, want null on a clamped stamp", payload["owner_id"])
	}
}

// TestDiscordIngestPersistsAttachments: an attachment survives the
// write-on-receipt path intact — the payload is the §6 replay source,
// and the router's taint decision (trust.IngestAttachment) keys off
// this array being faithful.
func TestDiscordIngestPersistsAttachments(t *testing.T) {
	sdb := testStore(t)
	handle := discordIngest(sdb, "strong", slog.New(slog.NewTextHandler(&bytes.Buffer{}, nil)), time.Now)

	m := inbound("1", "see attached")
	m.Attachments = []*discordgo.MessageAttachment{
		{URL: "https://cdn.discordapp.com/a.pdf", Filename: "a.pdf", ContentType: "application/pdf", Size: 12345},
	}
	handle(m)

	_, payload := payloadOf(t, sdb)
	atts, ok := payload["attachments"].([]any)
	if !ok || len(atts) != 1 {
		t.Fatalf("payload attachments = %v, want one entry", payload["attachments"])
	}
	att, _ := atts[0].(map[string]any)
	if att["url"] != "https://cdn.discordapp.com/a.pdf" || att["filename"] != "a.pdf" ||
		att["content_type"] != "application/pdf" || att["size"] != float64(12345) {
		t.Errorf("attachment round-tripped as %v, want verbatim platform fields", att)
	}
}

// TestDiscordIngestInsertFailureIsLoud: a store that cannot accept the
// write (closed here — the nearest reachable stand-in for disk-full or
// corruption) must surface an ERROR. The gateway does not redeliver;
// this log line is the only trace the message ever existed.
func TestDiscordIngestInsertFailureIsLoud(t *testing.T) {
	sdb := testStore(t)
	var log bytes.Buffer
	handle := discordIngest(sdb, "strong", slog.New(slog.NewTextHandler(&log, nil)), time.Now)
	_ = sdb.Close()

	handle(inbound("1", "attacker-authored s3cret"))

	if !bytes.Contains(log.Bytes(), []byte("ERROR")) {
		t.Errorf("failed insert left no ERROR in the log — the drop would be invisible:\n%s", log.String())
	}
	// The message content is externally-authored data and must not
	// leak into the journal on the error path.
	if bytes.Contains(log.Bytes(), []byte("s3cret")) {
		t.Errorf("error log leaked message content:\n%s", log.String())
	}
}
