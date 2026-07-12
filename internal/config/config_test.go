package config_test

import (
	"strings"
	"testing"
	"time"

	"github.com/brian-bell/approach/internal/config"
)

// validModels satisfies the required [models] keys for tests exercising
// other sections.
const validModels = "[models]\nmessage = \"m\"\nheartbeat = \"h\"\n"

func TestChannels_AuthEnum(t *testing.T) {
	cases := []struct {
		name, auth string
		wantErr    bool
	}{
		{"strong ok", "strong", false},
		{"weak ok", "weak", false},
		{"unknown rejected", "medium", true},
		{"empty rejected", "", true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			toml := validModels + "[channels.discord]\nauth = \"" + tc.auth + "\"\n"
			_, err := config.Parse(strings.NewReader(toml))
			if tc.wantErr && err == nil {
				t.Fatalf("auth=%q: want error, got nil", tc.auth)
			}
			if !tc.wantErr && err != nil {
				t.Fatalf("auth=%q: unexpected error: %v", tc.auth, err)
			}
		})
	}
}

func TestChannel_DerivedTrustCaps(t *testing.T) {
	c, err := config.Parse(strings.NewReader(validModels + `
[channels.discord]
auth = "strong"
[channels.sms]
auth = "weak"
`))
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	discord, sms := c.Channels["discord"], c.Channels["sms"]
	if got := discord.MaxTrust(); got != "owner" {
		t.Errorf("strong channel MaxTrust = %q, want owner", got)
	}
	if !discord.MayApprove() {
		t.Error("strong channel should be able to satisfy approvals")
	}
	if got := sms.MaxTrust(); got != "known" {
		t.Errorf("weak channel MaxTrust = %q, want known", got)
	}
	if sms.MayApprove() {
		t.Error("weak channel must never satisfy an approval (§6)")
	}
	// §6: weak channels are clamped read-only, not just capped at known —
	// a spoofable sender must never reach the known column's mutating verbs.
	if discord.ReadOnly() {
		t.Error("strong channel should not be read-only")
	}
	if !sms.ReadOnly() {
		t.Error("weak channel must be read-only (§6)")
	}
}

// validChannels defines one strong and one weak channel for identity tests.
const validChannels = "[channels.discord]\nauth = \"strong\"\n[channels.sms]\nauth = \"weak\"\n"

func TestChannels_TokenFile(t *testing.T) {
	cases := []struct {
		name, toml string
		wantErr    bool
	}{
		{
			"token_file on a strong channel",
			"[channels.discord]\nauth = \"strong\"\ntoken_file = \"/run/credentials/approach/discord\"\n",
			false,
		},
		{
			"token_file on a weak channel rejected",
			"[channels.sms]\nauth = \"weak\"\ntoken_file = \"/run/credentials/approach/sms\"\n",
			true,
		},
		{
			"strong channel without token_file still valid",
			"[channels.discord]\nauth = \"strong\"\n",
			false,
		},
		{
			// A typo'd or not-yet-supported channel must not swallow a
			// credential path silently — the daemon only wires gateway
			// adapters it implements, and an ignored token_file leaves
			// the operator believing a gateway is live.
			"token_file on a channel with no gateway adapter rejected",
			"[channels.disocrd]\nauth = \"strong\"\ntoken_file = \"/run/credentials/approach/discord\"\n",
			true,
		},
		{
			// An explicitly empty token_file decodes to the same zero
			// value as an omitted key, which would downgrade a configured
			// credential mistake to the dormant-channel warning instead
			// of a startup refusal.
			"explicitly empty token_file rejected",
			"[channels.discord]\nauth = \"strong\"\ntoken_file = \"\"\n",
			true,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := config.Parse(strings.NewReader(validModels + tc.toml))
			if tc.wantErr && err == nil {
				t.Fatal("want error, got nil")
			}
			if !tc.wantErr && err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
		})
	}
}

func TestIdentities_ValidRowsParse(t *testing.T) {
	c, err := config.Parse(strings.NewReader(validModels + validChannels + `
[[identity]]
channel   = "discord"
native_id = "123456"
trust     = "owner"
owner_id  = "brian"
label     = "Brian"

[[identity]]
channel   = "discord"
native_id = "789"
trust     = "known"
`))
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if len(c.Identities) != 2 {
		t.Fatalf("got %d identities, want 2", len(c.Identities))
	}
	owner := c.Identities[0]
	if owner.Channel != "discord" || owner.NativeID != "123456" ||
		owner.Trust != "owner" || owner.OwnerID != "brian" || owner.Label != "Brian" {
		t.Errorf("owner row mismatch: %+v", owner)
	}
	if c.Identities[1].Trust != "known" {
		t.Errorf("known row mismatch: %+v", c.Identities[1])
	}
}

func TestIdentities_Validation(t *testing.T) {
	cases := []struct {
		name, rows, wantErr string
	}{
		{
			"trust enum closed",
			"[[identity]]\nchannel = \"discord\"\nnative_id = \"1\"\ntrust = \"untrusted\"",
			"trust",
		},
		{
			"owner requires owner_id",
			"[[identity]]\nchannel = \"discord\"\nnative_id = \"1\"\ntrust = \"owner\"",
			"owner_id",
		},
		{
			"undefined channel",
			"[[identity]]\nchannel = \"telegram\"\nnative_id = \"1\"\ntrust = \"known\"",
			"telegram",
		},
		{
			"duplicate (channel, native_id)",
			"[[identity]]\nchannel = \"discord\"\nnative_id = \"1\"\ntrust = \"known\"\n" +
				"[[identity]]\nchannel = \"discord\"\nnative_id = \"1\"\ntrust = \"known\"",
			"duplicate",
		},
		{
			"owner_id on known row",
			"[[identity]]\nchannel = \"discord\"\nnative_id = \"1\"\ntrust = \"known\"\nowner_id = \"brian\"",
			"owner_id",
		},
		{
			"empty native_id",
			"[[identity]]\nchannel = \"discord\"\nnative_id = \"\"\ntrust = \"known\"",
			"native_id",
		},
		{
			"owner on weak channel",
			"[[identity]]\nchannel = \"sms\"\nnative_id = \"+15551234\"\ntrust = \"owner\"\nowner_id = \"brian\"",
			"weak",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := config.Parse(strings.NewReader(validModels + validChannels + tc.rows + "\n"))
			if err == nil {
				t.Fatal("want validation error, got nil")
			}
			if !strings.Contains(err.Error(), tc.wantErr) {
				t.Errorf("error should mention %q, got: %v", tc.wantErr, err)
			}
		})
	}
}

func TestSessions_DurationParsing(t *testing.T) {
	c, err := config.Parse(strings.NewReader(validModels + `
[sessions]
idle_ttl = "90m"
turn_cap = 25
`))
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if got := c.Sessions.IdleTTL.Duration(); got != 90*time.Minute {
		t.Errorf("idle_ttl = %v, want 90m", got)
	}
	if c.Sessions.TurnCap != 25 {
		t.Errorf("turn_cap = %d, want 25", c.Sessions.TurnCap)
	}

	_, err = config.Parse(strings.NewReader(validModels + "[sessions]\nidle_ttl = \"soon\"\n"))
	if err == nil {
		t.Fatal("garbage duration: want error, got nil")
	}
}

func TestSessions_Defaults(t *testing.T) {
	c, err := config.Parse(strings.NewReader(validModels))
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if got := c.Sessions.IdleTTL.Duration(); got != 4*time.Hour {
		t.Errorf("default idle_ttl = %v, want 4h", got)
	}
	if c.Sessions.TurnCap != 50 {
		t.Errorf("default turn_cap = %d, want 50", c.Sessions.TurnCap)
	}
}

func TestSessions_Bounds(t *testing.T) {
	cases := []struct {
		name, sessions, wantErr string
	}{
		{"negative idle_ttl", "[sessions]\nidle_ttl = \"-1h\"\n", "idle_ttl"},
		{"sub-second idle_ttl", "[sessions]\nidle_ttl = \"500ms\"\n", "idle_ttl"},
		{"zero turn_cap", "[sessions]\nturn_cap = 0\n", "turn_cap"},
		{"negative turn_cap", "[sessions]\nturn_cap = -5\n", "turn_cap"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := config.Parse(strings.NewReader(validModels + tc.sessions))
			if err == nil {
				t.Fatal("want validation error, got nil")
			}
			if !strings.Contains(err.Error(), tc.wantErr) {
				t.Errorf("error should mention %q, got: %v", tc.wantErr, err)
			}
		})
	}
}

func TestPolicy_DefaultMatrixMatchesSpec(t *testing.T) {
	c, err := config.Parse(strings.NewReader(validModels))
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	// Spot-check load-bearing cells of the §7 table.
	checks := []struct {
		capability, column, want string
	}{
		{"bash", "untrusted", "deny"},
		{"bash", "owner", "allow"},
		{"host_exec", "owner", "ask"},
		{"host_exec", "known", "deny"},
		{"memory_write", "known", "gate"},
		{"read", "untrusted", "sandbox"},
		{"git_push", "owner", "ask"},
		{"task_graph_write", "untrusted", "deny"},
	}
	for _, ck := range checks {
		row, ok := c.Policy.Matrix[ck.capability]
		if !ok {
			t.Errorf("default matrix missing capability %q", ck.capability)
			continue
		}
		if got := row.Action(ck.column); got != ck.want {
			t.Errorf("%s/%s = %q, want %q", ck.capability, ck.column, got, ck.want)
		}
	}
}

func TestPolicy_Overrides(t *testing.T) {
	c, err := config.Parse(strings.NewReader(validModels + `
[policy.matrix.web_fetch]
owner = "ask"
known = "deny"
`))
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	row := c.Policy.Matrix["web_fetch"]
	if got := row.Action("owner"); got != "ask" {
		t.Errorf("override owner = %q, want ask", got)
	}
	// The override omitted untrusted: deny-by-default, NOT the default
	// table's "sandbox".
	if got := row.Action("untrusted"); got != "deny" {
		t.Errorf("omitted column = %q, want deny", got)
	}
	// Untouched capabilities keep their defaults.
	if got := c.Policy.Matrix["bash"].Action("owner"); got != "allow" {
		t.Errorf("untouched capability changed: bash/owner = %q", got)
	}
}

func TestPolicy_Validation(t *testing.T) {
	cases := []struct {
		name, policy, wantErr string
	}{
		{"invalid action", "[policy.matrix.bash]\nowner = \"permit\"\n", "permit"},
		{"unknown capability", "[policy.matrix.bassh]\nowner = \"allow\"\n", "bassh"},
		// An explicitly configured empty action is a mistake, not a deny:
		// only an OMITTED column defaults to deny.
		{"explicit empty action", "[policy.matrix.bash]\nowner = \"\"\n", "bash.owner"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := config.Parse(strings.NewReader(validModels + tc.policy))
			if err == nil {
				t.Fatal("want validation error, got nil")
			}
			if !strings.Contains(err.Error(), tc.wantErr) {
				t.Errorf("error should mention %q, got: %v", tc.wantErr, err)
			}
		})
	}
}

func TestParse_MultipleErrorsReported(t *testing.T) {
	// Three independent defects: missing message model, bad channel auth,
	// zero turn_cap. All three must surface in one Parse call.
	_, err := config.Parse(strings.NewReader(`
[models]
heartbeat = "h"
[channels.discord]
auth = "medium"
[sessions]
turn_cap = 0
`))
	if err == nil {
		t.Fatal("want validation errors, got nil")
	}
	for _, want := range []string{"models.message", "channels.discord.auth", "turn_cap"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("aggregated error should mention %q, got: %v", want, err)
		}
	}
}

func TestParse_UnknownKeysAggregateWithValidation(t *testing.T) {
	// A typo'd section AND a semantic defect must both surface in one
	// Parse call — unknown keys don't short-circuit validation.
	_, err := config.Parse(strings.NewReader(`
[modles]
message = "m"
[channels.discord]
auth = "medium"
`))
	if err == nil {
		t.Fatal("want errors, got nil")
	}
	for _, want := range []string{"modles", "models.message", "channels.discord.auth"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("aggregated error should mention %q, got: %v", want, err)
		}
	}
}

// TestExampleConfigIsValid loads the committed example file, so the
// documented example can never drift from what the loader accepts.
func TestExampleConfigIsValid(t *testing.T) {
	c, err := config.Load("../../docs/approach.toml.example")
	if err != nil {
		t.Fatalf("docs/approach.toml.example must load cleanly: %v", err)
	}
	if c.Models.Message == "" {
		t.Error("example should pin the interactive model")
	}
	if len(c.Identities) == 0 {
		t.Error("example should enroll at least one identity")
	}
}

func TestParse_MalformedTOML(t *testing.T) {
	_, err := config.Parse(strings.NewReader("[models\nmessage = "))
	if err == nil {
		t.Fatal("Parse of malformed TOML: want error, got nil")
	}
}

func TestParse_UnknownKeys(t *testing.T) {
	cases := []struct {
		name, toml, wantKey string
	}{
		{"top-level typo", "[modles]\nmessage = \"m\"", "modles"},
		{"nested typo", "[channels.discord]\nauthh = \"strong\"", "authh"},
		{"unknown event kind", "[models]\nwebhookk = \"x\"", "webhookk"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := config.Parse(strings.NewReader(tc.toml))
			if err == nil {
				t.Fatal("want unknown-key error, got nil")
			}
			if !strings.Contains(err.Error(), tc.wantKey) {
				t.Errorf("error should name the unknown key %q, got: %v", tc.wantKey, err)
			}
		})
	}
}

func TestModels_ValidTableParses(t *testing.T) {
	c, err := config.Parse(strings.NewReader(`
[models]
heartbeat = "claude-haiku-4-5"
message   = "claude-sonnet-5"
heavy     = "claude-opus-4-8"
critique  = "codex"
`))
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	m := c.Models
	if m.Heartbeat != "claude-haiku-4-5" || m.Message != "claude-sonnet-5" ||
		m.Heavy != "claude-opus-4-8" || m.Critique != "codex" {
		t.Errorf("models mismatch: %+v", m)
	}
}

func TestModels_RequiredKinds(t *testing.T) {
	cases := []struct {
		name, toml, wantErr string
	}{
		{"no models section", "", "models.message"},
		{"missing message", "[models]\nheartbeat = \"h\"", "models.message"},
		{"missing heartbeat", "[models]\nmessage = \"m\"", "models.heartbeat"},
		{"empty message", "[models]\nmessage = \"\"\nheartbeat = \"h\"", "models.message"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := config.Parse(strings.NewReader(tc.toml))
			if err == nil {
				t.Fatal("want validation error, got nil")
			}
			if !strings.Contains(err.Error(), tc.wantErr) {
				t.Errorf("error should mention %q, got: %v", tc.wantErr, err)
			}
		})
	}
}

func TestLoad_MissingFile(t *testing.T) {
	_, err := config.Load("/nonexistent/approach.toml")
	if err == nil {
		t.Fatal("Load on a missing file: want error, got nil")
	}
	if !strings.Contains(err.Error(), "/nonexistent/approach.toml") {
		t.Errorf("error should name the path, got: %v", err)
	}
}
