package store_test

import (
	"context"
	"database/sql"
	"path/filepath"
	"strings"
	"testing"

	"github.com/brian-bell/approach/internal/config"
	"github.com/brian-bell/approach/internal/store"
	"github.com/brian-bell/approach/internal/trust"
)

// seedFromTOML drives the real enrollment path end to end: config.Parse
// over an approach.toml, the daemon's config→store field mapping
// (mirrors seedIdentities in internal/cli/daemon.go), and the real
// store.SeedIdentities sync — so the drills below probe the table as an
// actual boot would have populated it, not a hand-built fixture.
func seedFromTOML(t *testing.T, db *sql.DB, toml string) *config.Config {
	t.Helper()
	cfg, err := config.Parse(strings.NewReader(toml))
	if err != nil {
		t.Fatalf("parse config: %v", err)
	}
	ids := make([]store.Identity, len(cfg.Identities))
	for i, id := range cfg.Identities {
		ids[i] = store.Identity{
			Channel:  id.Channel,
			NativeID: id.NativeID,
			Trust:    id.Trust,
			OwnerID:  id.OwnerID,
			Label:    id.Label,
		}
	}
	if err := store.SeedIdentities(context.Background(), db, ids); err != nil {
		t.Fatalf("seed identities: %v", err)
	}
	return cfg
}

// drillConfig enrolls an owner on a strong channel and a known person
// on a weak one — the smallest realistic §6 registry, so a miss below
// is a miss against real neighbors, not against an empty table.
const drillConfig = `
[models]
message = "claude-sonnet-5"
heartbeat = "claude-haiku-4-5"

[channels.discord]
auth = "strong"

[channels.slack]
auth = "strong"

[channels.sms]
auth = "weak"

[[identity]]
channel = "discord"
native_id = "42"
trust = "owner"
owner_id = "brian"
label = "Brian"

[[identity]]
channel = "slack"
native_id = "U7AB"
trust = "known"
label = "Dana"

[[identity]]
channel = "sms"
native_id = "+15550100"
trust = "known"
label = "Dana (SMS)"
`

// TestDrillUnmappedSenderResolvesUntrusted is the §9 PS drill for the
// §6 deny-by-default rule: a sender with no identities row resolves
// untrusted — through EVERY resolution surface the router's stamping
// path (M1) will answer from, probed with the attacker-shaped inputs a
// real channel delivers: near-misses of enrolled senders, not just
// random ids. The unit tests pin each surface in isolation; this drill
// pins that the composed, config-seeded system holds the line.
func TestDrillUnmappedSenderResolvesUntrusted(t *testing.T) {
	db := mustOpen(t, filepath.Join(t.TempDir(), "state", "approach.db"))
	ctx := context.Background()
	cfg := seedFromTOML(t, db, drillConfig)

	for _, tc := range []struct {
		name, channel, nativeID string
	}{
		{"unknown id on an enrolled channel", "discord", "999"},
		// Cross-channel replay: the owner's discord id arriving over
		// sms is a DIFFERENT identity — (channel, native_id) is the
		// key, so enrollment never transfers across channels.
		{"enrolled id on the wrong channel", "sms", "42"},
		// The lookup is exact-match (§6): platform native ids are
		// case-sensitive, so a case variant of Dana's letter-bearing
		// Slack id is a stranger.
		{"case variant of an enrolled id", "slack", "u7ab"},
		{"whitespace-padded enrolled id", "discord", " 42"},
		{"empty native_id", "discord", ""},
		{"channel absent from config entirely", "carrier-pigeon", "42"},
	} {
		level, err := store.ResolveTrust(ctx, db, tc.channel, tc.nativeID)
		if err != nil {
			t.Fatalf("%s: ResolveTrust: %v", tc.name, err)
		}
		if level != trust.Untrusted {
			t.Errorf("%s: resolved %q, want untrusted deny-by-default", tc.name, level)
		}

		ownerID, ok, err := store.ResolveOwnerID(ctx, db, tc.channel, tc.nativeID)
		if err != nil {
			t.Fatalf("%s: ResolveOwnerID: %v", tc.name, err)
		}
		if ok || ownerID != "" {
			t.Errorf("%s: resolved principal %q, want none — an unmapped sender must never satisfy an approval", tc.name, ownerID)
		}

		// The stamped decision the router will act on: unmapped stays
		// untrusted through the channel clamp too. cfg.Channels misses
		// yield the zero Channel (auth ""), which Stamp fails closed.
		stamped, err := store.ResolveStamped(ctx, db, tc.channel, tc.nativeID, cfg.Channels[tc.channel].Auth)
		if err != nil {
			t.Fatalf("%s: ResolveStamped: %v", tc.name, err)
		}
		if stamped.Trust != trust.Untrusted {
			t.Errorf("%s: stamped %q, want untrusted", tc.name, stamped.Trust)
		}
	}
}

// TestDrillZeroConfigTrustsNobody: the daemon's no-approach.toml boot
// syncs the table to empty (internal/cli seedIdentities with a nil
// config) — and an unconfigured daemon must trust nobody, even a
// sender spelling the owner's exact id (§6).
func TestDrillZeroConfigTrustsNobody(t *testing.T) {
	db := mustOpen(t, filepath.Join(t.TempDir(), "state", "approach.db"))
	ctx := context.Background()
	if err := store.SeedIdentities(ctx, db, nil); err != nil {
		t.Fatalf("seed with nil identities: %v", err)
	}

	level, err := store.ResolveTrust(ctx, db, "discord", "42")
	if err != nil {
		t.Fatalf("ResolveTrust: %v", err)
	}
	if level != trust.Untrusted {
		t.Errorf("owner-shaped sender resolved %q on a zero-config boot, want untrusted", level)
	}
	if _, ok, err := store.ResolveOwnerID(ctx, db, "discord", "42"); err != nil || ok {
		t.Errorf("owner-shaped sender resolved a principal on a zero-config boot (ok=%v, err=%v), want none", ok, err)
	}
}
