package cli

import (
	"bytes"
	"database/sql"
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
	handle := discordIngest(sdb, slog.New(slog.NewTextHandler(&bytes.Buffer{}, nil)), func() time.Time { return now })

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
	handle := discordIngest(sdb, slog.New(slog.NewTextHandler(&log, nil)), time.Now)

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
	handle := discordIngest(sdb, slog.New(slog.NewTextHandler(&log, nil)), time.Now)

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

// TestDiscordIngestInsertFailureIsLoud: a store that cannot accept the
// write (closed here — the nearest reachable stand-in for disk-full or
// corruption) must surface an ERROR. The gateway does not redeliver;
// this log line is the only trace the message ever existed.
func TestDiscordIngestInsertFailureIsLoud(t *testing.T) {
	sdb := testStore(t)
	var log bytes.Buffer
	handle := discordIngest(sdb, slog.New(slog.NewTextHandler(&log, nil)), time.Now)
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
