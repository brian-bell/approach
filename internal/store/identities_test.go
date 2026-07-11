package store_test

import (
	"context"
	"database/sql"
	"path/filepath"
	"testing"

	"github.com/brian-bell/approach/internal/store"
	"github.com/brian-bell/approach/internal/trust"
)

// TestOpenCreatesIdentitiesTable: the §6 identities table — the root of
// every §7 trust decision — arrives with the embedded migrations, keyed
// by (channel, native_id).
func TestOpenCreatesIdentitiesTable(t *testing.T) {
	db := mustOpen(t, filepath.Join(t.TempDir(), "state", "approach.db"))

	if _, err := db.Exec(
		`INSERT INTO identities (channel, native_id, trust, owner_id, label)
		 VALUES ('discord', '42', 'owner', 'brian', 'Brian')`,
	); err != nil {
		t.Fatalf("insert into identities: %v", err)
	}

	// The primary key is (channel, native_id): the same native id on
	// another channel is a different identity, but re-enrolling the same
	// pair must conflict.
	if _, err := db.Exec(
		`INSERT INTO identities (channel, native_id, trust) VALUES ('slack', '42', 'known')`,
	); err != nil {
		t.Errorf("same native_id on another channel should be a distinct row: %v", err)
	}
	if _, err := db.Exec(
		`INSERT INTO identities (channel, native_id, trust, owner_id) VALUES ('discord', '42', 'owner', 'brian')`,
	); err == nil {
		t.Error("duplicate (channel, native_id) insert succeeded, want primary-key conflict")
	}
}

// TestIdentitiesSchemaConstraints: config validation (§6) is mirrored in
// the schema — the table is the root of §7, so its invariants are CHECK
// constraints, not conventions. Untrusted is the absence of a row and
// exactly owner rows carry the canonical owner_id (§4.4).
func TestIdentitiesSchemaConstraints(t *testing.T) {
	db := mustOpen(t, filepath.Join(t.TempDir(), "state", "approach.db"))

	for _, tc := range []struct {
		name string
		sql  string
	}{
		{
			"untrusted is never enrolled",
			`INSERT INTO identities (channel, native_id, trust) VALUES ('discord', '1', 'untrusted')`,
		},
		{
			"owner row without owner_id",
			`INSERT INTO identities (channel, native_id, trust) VALUES ('discord', '2', 'owner')`,
		},
		{
			"known row with owner_id",
			`INSERT INTO identities (channel, native_id, trust, owner_id) VALUES ('discord', '3', 'known', 'brian')`,
		},
		{
			// Empty is not a principal: '' is non-NULL, so a naive CHECK
			// admits it, and every such row would match every other empty
			// owner_id in cross-surface approval (§4.4).
			"owner row with empty owner_id",
			`INSERT INTO identities (channel, native_id, trust, owner_id) VALUES ('discord', '4', 'owner', '')`,
		},
		{
			// The two valid states are exactly (owner, non-empty) and
			// (known, NULL) — an empty string on a known row is neither.
			"known row with empty owner_id",
			`INSERT INTO identities (channel, native_id, trust, owner_id) VALUES ('discord', '5', 'known', '')`,
		},
	} {
		if _, err := db.Exec(tc.sql); err == nil {
			t.Errorf("%s: insert succeeded, want CHECK violation", tc.name)
		}
	}
}

// readIdentities returns every row keyed "channel/native_id" →
// "trust/owner_id/label" (NULLs rendered empty) for seed assertions.
func readIdentities(t *testing.T, db *sql.DB) map[string]string {
	t.Helper()
	rows, err := db.Query(`SELECT channel, native_id, trust, COALESCE(owner_id, ''), COALESCE(label, '') FROM identities`)
	if err != nil {
		t.Fatalf("query identities: %v", err)
	}
	defer func() {
		if err := rows.Close(); err != nil {
			t.Errorf("close identities rows: %v", err)
		}
	}()
	got := make(map[string]string)
	for rows.Next() {
		var channel, nativeID, trust, ownerID, label string
		if err := rows.Scan(&channel, &nativeID, &trust, &ownerID, &label); err != nil {
			t.Fatalf("scan identities: %v", err)
		}
		got[channel+"/"+nativeID] = trust + "/" + ownerID + "/" + label
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate identities: %v", err)
	}
	return got
}

// TestSeedIdentitiesInsertsConfiguredRows: seeding writes each enrolled
// (channel, native_id) with its trust, owner_id, and label (§6).
func TestSeedIdentitiesInsertsConfiguredRows(t *testing.T) {
	db := mustOpen(t, filepath.Join(t.TempDir(), "state", "approach.db"))

	err := store.SeedIdentities(context.Background(), db, []store.Identity{
		{Channel: "discord", NativeID: "42", Trust: "owner", OwnerID: "brian", Label: "Brian"},
		{Channel: "sms", NativeID: "+15555550100", Trust: "known", Label: "Brian (SMS)"},
	})
	if err != nil {
		t.Fatalf("SeedIdentities: %v", err)
	}

	want := map[string]string{
		"discord/42":       "owner/brian/Brian",
		"sms/+15555550100": "known//Brian (SMS)",
	}
	got := readIdentities(t, db)
	if len(got) != len(want) {
		t.Errorf("identities rows = %v, want %v", got, want)
	}
	for key, row := range want {
		if got[key] != row {
			t.Errorf("identities[%s] = %q, want %q", key, got[key], row)
		}
	}
}

// TestSeedIdentitiesSyncsToConfig: approach.toml is the source of truth,
// so a re-seed is a full sync — rows dropped from the config are REVOKED,
// changed rows are updated, and an empty set empties the table. Upsert-only
// seeding would leave a removed person enrolled forever (§6).
func TestSeedIdentitiesSyncsToConfig(t *testing.T) {
	db := mustOpen(t, filepath.Join(t.TempDir(), "state", "approach.db"))
	ctx := context.Background()

	first := []store.Identity{
		{Channel: "discord", NativeID: "42", Trust: "owner", OwnerID: "brian", Label: "Brian"},
		{Channel: "discord", NativeID: "77", Trust: "known", Label: "Guest"},
	}
	if err := store.SeedIdentities(ctx, db, first); err != nil {
		t.Fatalf("first SeedIdentities: %v", err)
	}

	// Guest removed (revoked), Brian's label changed.
	second := []store.Identity{
		{Channel: "discord", NativeID: "42", Trust: "owner", OwnerID: "brian", Label: "Brian B."},
	}
	if err := store.SeedIdentities(ctx, db, second); err != nil {
		t.Fatalf("second SeedIdentities: %v", err)
	}

	got := readIdentities(t, db)
	want := map[string]string{"discord/42": "owner/brian/Brian B."}
	if len(got) != 1 || got["discord/42"] != want["discord/42"] {
		t.Errorf("after re-seed identities = %v, want %v", got, want)
	}

	if err := store.SeedIdentities(ctx, db, nil); err != nil {
		t.Fatalf("empty SeedIdentities: %v", err)
	}
	if got := readIdentities(t, db); len(got) != 0 {
		t.Errorf("after empty re-seed identities = %v, want no rows", got)
	}
}

// TestResolveOwnerID: the canonical principal behind a sender (§6).
// Cross-surface approval (§4.4) matches on the resolved owner_id, so all
// of the owner's surfaces resolve to the SAME principal, and neither an
// unmapped sender nor an enrolled known person resolves to any — only
// owner rows carry a principal, and a miss must read as "no principal",
// never as an error.
func TestResolveOwnerID(t *testing.T) {
	db := mustOpen(t, filepath.Join(t.TempDir(), "state", "approach.db"))
	ctx := context.Background()
	if err := store.SeedIdentities(ctx, db, []store.Identity{
		{Channel: "discord", NativeID: "42", Trust: "owner", OwnerID: "brian"},
		{Channel: "slack", NativeID: "U9", Trust: "owner", OwnerID: "brian"},
		{Channel: "discord", NativeID: "77", Trust: "owner", OwnerID: "alice"},
		{Channel: "discord", NativeID: "99", Trust: "known", Label: "Guest"},
	}); err != nil {
		t.Fatalf("SeedIdentities: %v", err)
	}

	resolve := func(channel, nativeID string) (string, bool) {
		t.Helper()
		ownerID, ok, err := store.ResolveOwnerID(ctx, db, channel, nativeID)
		if err != nil {
			t.Fatalf("ResolveOwnerID(%s, %s): %v", channel, nativeID, err)
		}
		return ownerID, ok
	}

	// The owner's surfaces unify on one principal.
	discordOwner, ok := resolve("discord", "42")
	if !ok || discordOwner != "brian" {
		t.Errorf("discord/42 = (%q, %v), want (brian, true)", discordOwner, ok)
	}
	slackOwner, ok := resolve("slack", "U9")
	if !ok || slackOwner != discordOwner {
		t.Errorf("slack/U9 = (%q, %v), want the same principal as discord/42 (%q)", slackOwner, ok, discordOwner)
	}

	// A different owner row is a different principal.
	if ownerID, ok := resolve("discord", "77"); !ok || ownerID == discordOwner {
		t.Errorf("discord/77 = (%q, %v), want a distinct principal", ownerID, ok)
	}

	// Known people and unmapped senders have no principal — and neither
	// is an error: a miss reads as "cannot approve", not a failure.
	if ownerID, ok := resolve("discord", "99"); ok {
		t.Errorf("known row resolved principal %q, want none", ownerID)
	}
	if ownerID, ok := resolve("discord", "unmapped"); ok {
		t.Errorf("unmapped sender resolved principal %q, want none", ownerID)
	}
}

// TestResolveTrust: the §6 lookup every gate reduces to — (channel,
// native_id) -> trust level, and a miss IS untrusted, deny-by-default:
// an unknown Discord DM is untrusted, never inferred toward anything
// else.
func TestResolveTrust(t *testing.T) {
	db := mustOpen(t, filepath.Join(t.TempDir(), "state", "approach.db"))
	ctx := context.Background()

	level, err := store.ResolveTrust(ctx, db, "discord", "999")
	if err != nil {
		t.Fatalf("ResolveTrust on unmapped sender: %v", err)
	}
	if level != trust.Untrusted {
		t.Errorf("unmapped sender resolved %q, want untrusted deny-by-default", level)
	}
}

// TestResolveTrustEnrolled: hits stamp the row's enrolled level, and
// the lookup is exact-match — platform native IDs are case-sensitive,
// so a case-mismatched id is a different (unmapped) sender.
func TestResolveTrustEnrolled(t *testing.T) {
	db := mustOpen(t, filepath.Join(t.TempDir(), "state", "approach.db"))
	ctx := context.Background()
	if _, err := db.Exec(
		`INSERT INTO identities (channel, native_id, trust, owner_id) VALUES ('discord', '42', 'owner', 'brian');
		 INSERT INTO identities (channel, native_id, trust) VALUES ('slack', 'U7', 'known')`,
	); err != nil {
		t.Fatalf("enroll identities: %v", err)
	}

	for _, tc := range []struct {
		name, channel, nativeID string
		want                    trust.Level
	}{
		{"owner row", "discord", "42", trust.Owner},
		{"known row", "slack", "U7", trust.Known},
		{"case-mismatched native_id is a different sender", "slack", "u7", trust.Untrusted},
		{"same native_id on an unenrolled channel", "sms", "42", trust.Untrusted},
	} {
		level, err := store.ResolveTrust(ctx, db, tc.channel, tc.nativeID)
		if err != nil {
			t.Fatalf("%s: %v", tc.name, err)
		}
		if level != tc.want {
			t.Errorf("%s: resolved %q, want %q", tc.name, level, tc.want)
		}
	}
}

// TestResolveTrustFailsClosed: a broken store and a drifted row are
// errors carrying Untrusted — never a silent allow, and never a level a
// caller that drops the error could act on.
func TestResolveTrustFailsClosed(t *testing.T) {
	ctx := context.Background()

	db := mustOpen(t, filepath.Join(t.TempDir(), "state", "approach.db"))
	if err := db.Close(); err != nil {
		t.Fatalf("close db: %v", err)
	}
	level, err := store.ResolveTrust(ctx, db, "discord", "42")
	if err == nil {
		t.Error("ResolveTrust on a closed store returned no error, want fail-closed error")
	}
	if level != trust.Untrusted {
		t.Errorf("failed lookup resolved %q, want untrusted", level)
	}

	// Drift the table past the 0002 CHECK — exactly what the Parse
	// defense exists for: the identities table is the root of §7, and a
	// manually edited row must not coerce into some level.
	db = mustOpen(t, filepath.Join(t.TempDir(), "state", "approach.db"))
	if _, err := db.Exec(`PRAGMA ignore_check_constraints = ON`); err != nil {
		t.Fatalf("disable check constraints: %v", err)
	}
	if _, err := db.Exec(
		`INSERT INTO identities (channel, native_id, trust) VALUES ('discord', '13', 'admin')`,
	); err != nil {
		t.Fatalf("insert drifted row: %v", err)
	}
	level, err = store.ResolveTrust(ctx, db, "discord", "13")
	if err == nil {
		t.Error("drifted trust value resolved without error, want fail-closed error")
	}
	if level != trust.Untrusted {
		t.Errorf("drifted row resolved %q, want untrusted", level)
	}
}

// TestResolveTrustRowInvariants: the Parse defense alone is not enough —
// the 0002 CHECK also binds trust to the owner_id shape, and a drifted
// row violating THOSE invariants must fail closed too: a malformed
// owner row must never stamp elevated trust, and a stored 'untrusted'
// row is drift by definition (absence of a row is what untrusted means).
func TestResolveTrustRowInvariants(t *testing.T) {
	ctx := context.Background()
	for _, tc := range []struct {
		name, insert, nativeID string
	}{
		{
			"owner row without owner_id",
			`INSERT INTO identities (channel, native_id, trust) VALUES ('discord', '13', 'owner')`,
			"13",
		},
		{
			"owner row with empty owner_id",
			`INSERT INTO identities (channel, native_id, trust, owner_id) VALUES ('discord', '14', 'owner', '')`,
			"14",
		},
		{
			"known row carrying owner_id",
			`INSERT INTO identities (channel, native_id, trust, owner_id) VALUES ('discord', '15', 'known', 'brian')`,
			"15",
		},
		{
			"stored untrusted row",
			`INSERT INTO identities (channel, native_id, trust) VALUES ('discord', '16', 'untrusted')`,
			"16",
		},
	} {
		db := mustOpen(t, filepath.Join(t.TempDir(), "state", "approach.db"))
		if _, err := db.Exec(`PRAGMA ignore_check_constraints = ON`); err != nil {
			t.Fatalf("%s: disable check constraints: %v", tc.name, err)
		}
		if _, err := db.Exec(tc.insert); err != nil {
			t.Fatalf("%s: insert drifted row: %v", tc.name, err)
		}
		level, err := store.ResolveTrust(ctx, db, "discord", tc.nativeID)
		if err == nil {
			t.Errorf("%s: resolved without error, want fail-closed error", tc.name)
		}
		if level != trust.Untrusted {
			t.Errorf("%s: resolved %q, want untrusted", tc.name, level)
		}
	}
}
