// Package config loads and validates approach.toml — the single file
// holding model routing, channel bindings, identities, session TTLs, and
// the policy matrix (spec §6, §8). The file is security-load-bearing, so
// the loader fails loud: unknown keys are errors, enums are closed, and
// missing policy values default to deny.
package config

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/BurntSushi/toml"
)

// Config is the validated content of approach.toml.
type Config struct {
	Models     Models             `toml:"models"`
	Channels   map[string]Channel `toml:"channels"`
	Identities []Identity         `toml:"identity"`
	Sessions   Sessions           `toml:"sessions"`
	Engine     *Engine            `toml:"engine"`
	Policy     Policy             `toml:"policy"`
}

// Engine is the §2 pin block: which Claude Code binary the daemon
// spawns, WHICH VERSION it expects (the hook lifecycle is
// version-sensitive — a silent CLI upgrade changes the enforcement
// substrate, so the daemon verifies this pin at startup and refuses
// on drift), the §11 runaway caps, and the enrolled hook set as
// config — the deterministic-lifecycle events the harness relies on,
// declared here so drift is reviewable in one place. A pointer:
// absence means a dormant, engine-less daemon (bootable; adapters and
// queues still run), which validate skips entirely.
type Engine struct {
	Bin         string   `toml:"bin"`          // absolute path; a PATH-resolved engine is not pinned (§7)
	Version     string   `toml:"version"`      // exact-match verified at startup (§2)
	MaxTurns    int      `toml:"max_turns"`    // --max-turns per spawn (§11); default 25
	TurnTimeout Duration `toml:"turn_timeout"` // wall-clock kill per turn (§11); default 10m
	Hooks       []string `toml:"hooks"`        // enrolled hook set (§2) — consumed by C8/C9 enrollment
}

// Policy is the §7 capability × trust matrix. STUB in this milestone:
// parsed, defaulted, and validated here; enforcement is C9's PreToolUse
// hook, built in a later epic. After Parse, Matrix always holds the full
// effective table (defaults merged with any [policy.matrix.*] overrides).
type Policy struct {
	Matrix map[string]PolicyRow `toml:"matrix"`
}

// PolicyRow maps the three adapter-stampable trust columns to an action.
// Fixed fields make a typo'd column an unknown-key error at decode time.
type PolicyRow struct {
	Owner     string `toml:"owner"`
	Known     string `toml:"known"`
	Untrusted string `toml:"untrusted"`
}

// Action returns the action for a trust column; an empty cell reads as
// deny — deny-by-default is a property of the type, not a convention.
func (r PolicyRow) Action(column string) string {
	var a string
	switch column {
	case "owner":
		a = r.Owner
	case "known":
		a = r.Known
	case "untrusted":
		a = r.Untrusted
	}
	if a == "" {
		return "deny"
	}
	return a
}

// mergeMatrix overlays validated user overrides on the default table.
// An override replaces its capability's whole row; columns it leaves
// empty read as deny via Action, not as the default table's cell.
func mergeMatrix(overrides map[string]PolicyRow) map[string]PolicyRow {
	m := defaultMatrix()
	for capability, row := range overrides {
		m[capability] = row
	}
	return m
}

// defaultMatrix encodes the §7 policy table. Capabilities form a closed
// set: an override for a capability not listed here is a validation error.
func defaultMatrix() map[string]PolicyRow {
	return map[string]PolicyRow{
		"read":               {Owner: "allow", Known: "allow", Untrusted: "sandbox"},
		"schedule":           {Owner: "allow", Known: "gate", Untrusted: "deny"},
		"web_fetch":          {Owner: "allow", Known: "allow", Untrusted: "sandbox"},
		"bash":               {Owner: "allow", Known: "allow", Untrusted: "deny"},
		"edit":               {Owner: "allow", Known: "ask", Untrusted: "deny"},
		"host_exec":          {Owner: "ask", Known: "deny", Untrusted: "deny"},
		"git_push":           {Owner: "ask", Known: "deny", Untrusted: "deny"},
		"send_same_thread":   {Owner: "allow", Known: "allow", Untrusted: "deny"},
		"send_other_surface": {Owner: "ask", Known: "deny", Untrusted: "deny"},
		"memory_write":       {Owner: "allow", Known: "gate", Untrusted: "gate"},
		"codex":              {Owner: "allow", Known: "ask", Untrusted: "deny"},
		"task_graph_write":   {Owner: "allow", Known: "gate", Untrusted: "deny"},
	}
}

// Sessions holds the C4 rotation inputs (§4.1): a thread's session is
// rotated after IdleTTL of inactivity or TurnCap turns.
type Sessions struct {
	IdleTTL Duration `toml:"idle_ttl"`
	TurnCap int      `toml:"turn_cap"`
}

// Duration decodes a TOML string like "4h" or "90m" via time.ParseDuration.
type Duration time.Duration

// UnmarshalText implements encoding.TextUnmarshaler for TOML decoding.
func (d *Duration) UnmarshalText(text []byte) error {
	parsed, err := time.ParseDuration(string(text))
	if err != nil {
		return err
	}
	*d = Duration(parsed)
	return nil
}

// Duration returns the value as a time.Duration.
func (d Duration) Duration() time.Duration { return time.Duration(d) }

// Identity is a hand-enrolled sender mapping that seeds the identities
// table (§6): (channel, native_id) → trust. No row means untrusted —
// deny-by-default — so only owner and known are ever configured.
type Identity struct {
	Channel  string `toml:"channel"`
	NativeID string `toml:"native_id"`
	Trust    string `toml:"trust"`
	OwnerID  string `toml:"owner_id"`
	Label    string `toml:"label"`
}

// Channel binds a chat surface and grades its sender authentication (§6):
// platform- or network-authenticated channels are "strong"; spoofable ones
// (sms, email) are "weak" and clamped to at most known trust, never able
// to satisfy an approval. trust.Stamp encodes the same strong/weak rules
// for the runtime stamping path — keep the two in sync.
type Channel struct {
	Auth string `toml:"auth"`
	// TokenFile is the path to the platform credential for gateway
	// channels (discord, slack), readable at daemon startup — a plain
	// file so systemd LoadCredential can supply it. The secret itself
	// never lives in approach.toml: the config is checked into dotfiles
	// and read more widely than a credential should travel (§7).
	TokenFile string `toml:"token_file"`
}

// MaxTrust is the highest trust an identity on this channel can be
// stamped with: weak channels are clamped to at most "known" (§6).
func (ch Channel) MaxTrust() string {
	if ch.Auth == "weak" {
		return "known"
	}
	return "owner"
}

// MayApprove reports whether an approval event arriving on this channel
// can satisfy a pending approval — never true for weak channels (§4.4).
func (ch Channel) MayApprove() bool {
	return ch.Auth == "strong"
}

// ReadOnly reports whether sessions entered from this channel are
// restricted to non-mutating verbs. Weak channels are clamped read-only
// (§6), not just capped at known trust: without this, a spoofable SMS
// sender enrolled as known would reach the known column's mutating
// actions. C9 must consult this alongside the stamped trust when
// answering from the policy matrix.
func (ch Channel) ReadOnly() bool {
	return ch.Auth == "weak"
}

// Models routes event kinds to engine models (§8). Message and Heartbeat
// are required — the interactive model is pinned here deliberately, never
// settings-derived. Fixed fields make a typo'd event kind an unknown-key
// error at decode time.
type Models struct {
	Heartbeat string `toml:"heartbeat"`
	Message   string `toml:"message"`
	Heavy     string `toml:"heavy"`
	Critique  string `toml:"critique"`
}

// Load reads and validates the config file at path.
func Load(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("config: %w", err)
	}
	return Parse(bytes.NewReader(data))
}

// Parse reads and validates a config from r.
func Parse(r io.Reader) (*Config, error) {
	c := Config{
		Sessions: Sessions{
			IdleTTL: Duration(4 * time.Hour),
			TurnCap: 50,
		},
	}
	md, err := toml.NewDecoder(r).Decode(&c)
	if err != nil {
		return nil, fmt.Errorf("config: %w", err)
	}
	var errs []error
	if undec := md.Undecoded(); len(undec) > 0 {
		keys := make([]string, len(undec))
		for i, k := range undec {
			keys[i] = k.String()
		}
		errs = append(errs, fmt.Errorf("config: unknown keys: %s", strings.Join(keys, ", ")))
	}
	// Engine caps default only when OMITTED — an explicit zero is a
	// validation error, not a silent 25/10m. Needs decode metadata to
	// tell the two apart, so it lives here rather than in validate.
	if c.Engine != nil {
		if !md.IsDefined("engine", "max_turns") {
			c.Engine.MaxTurns = 25
		}
		if !md.IsDefined("engine", "turn_timeout") {
			c.Engine.TurnTimeout = Duration(10 * time.Minute)
		}
	}
	// An explicitly configured empty action would silently normalize to
	// deny; only an omitted column may mean deny. Needs decode metadata
	// to tell the two apart, so it lives here rather than in validate.
	for capability, row := range c.Policy.Matrix {
		for column, raw := range map[string]string{"owner": row.Owner, "known": row.Known, "untrusted": row.Untrusted} {
			if raw == "" && md.IsDefined("policy", "matrix", capability, column) {
				errs = append(errs, fmt.Errorf("config: policy.matrix.%s.%s: empty action — omit the key to mean deny, or set one of allow|ask|gate|deny|sandbox", capability, column))
			}
		}
	}
	// Same presence-vs-omission rule for token_file: an explicitly empty
	// path would decode like an omitted key and downgrade a configured
	// credential mistake to the dormant-channel warning instead of a
	// startup refusal.
	for name, ch := range c.Channels {
		if ch.TokenFile == "" && md.IsDefined("channels", name, "token_file") {
			errs = append(errs, fmt.Errorf("config: channels.%s.token_file: empty path — omit the key for a dormant channel, or point it at the credential file", name))
		}
	}
	errs = append(errs, c.validate())
	if err := errors.Join(errs...); err != nil {
		return nil, err
	}
	c.Policy.Matrix = mergeMatrix(c.Policy.Matrix)
	return &c, nil
}

// failFunc records one validation problem.
type failFunc func(format string, args ...any)

// validate collects every problem in the config so a user fixes the file
// in one pass, not one error per daemon restart.
func (c *Config) validate() error {
	var errs []error
	fail := func(format string, args ...any) {
		errs = append(errs, fmt.Errorf("config: "+format, args...))
	}

	c.validateModels(fail)
	c.validateChannels(fail)
	c.validateSessions(fail)
	c.validateEngine(fail)
	c.validatePolicy(fail)
	c.validateIdentities(fail)

	return errors.Join(errs...)
}

func (c *Config) validateModels(fail failFunc) {
	if c.Models.Message == "" {
		fail("models.message is required (pin the interactive model — §8)")
	}
	if c.Models.Heartbeat == "" {
		fail("models.heartbeat is required (§8)")
	}
}

// gatewayChannels is the closed set of channels the daemon implements
// a gateway adapter for — the only places a token_file means anything.
// Grows with each adapter (slack arrives with the C1 contract's second
// implementation, approach-cb5.3.1).
var gatewayChannels = map[string]bool{"discord": true}

func (c *Config) validateChannels(fail failFunc) {
	for name, ch := range c.Channels {
		if ch.Auth != "strong" && ch.Auth != "weak" {
			fail("channels.%s.auth must be %q or %q, got %q", name, "strong", "weak", ch.Auth)
		}
		// token_file is only honored on channels the daemon has a
		// gateway adapter for — a closed set, so a typo'd channel name
		// or a not-yet-supported platform fails HERE instead of
		// silently ignoring the credential and leaving the operator
		// believing a gateway is live (closed config, §6). Weak
		// channels (sms, email) never qualify: no gateway, no token.
		if ch.TokenFile != "" && !gatewayChannels[name] {
			fail("channels.%s.token_file: no gateway adapter exists for channel %q — supported: discord", name, name)
		}
		if ch.Auth == "weak" && ch.TokenFile != "" {
			fail("channels.%s.token_file: weak-auth channels have no gateway credential — remove the key", name)
		}
	}
}

func (c *Config) validateSessions(fail failFunc) {
	// The floor is 1s, not merely positive: session timestamps are
	// whole Unix seconds (§6), so a sub-second TTL cannot be honored —
	// and silently rounding an explicit "500ms" to anything else would
	// swap the operator's retention policy without a word (fail loud).
	if c.Sessions.IdleTTL < Duration(time.Second) {
		fail("sessions.idle_ttl must be at least 1s (timestamps are whole seconds), got %v", c.Sessions.IdleTTL.Duration())
	}
	if c.Sessions.TurnCap < 1 {
		fail("sessions.turn_cap must be >= 1, got %d", c.Sessions.TurnCap)
	}
}

func (c *Config) validateEngine(fail failFunc) {
	e := c.Engine
	if e == nil {
		return
	}
	if e.Bin == "" {
		fail("engine.bin is required — the invocation must be pinned to a path (§7)")
	} else if !filepath.IsAbs(e.Bin) {
		fail("engine.bin %q is not absolute — a PATH-resolved engine is not pinned (§7)", e.Bin)
	}
	if e.Version == "" {
		fail("engine.version is required — the hook lifecycle is version-sensitive; pin it (§2)")
	}
	if e.MaxTurns < 1 {
		fail("engine.max_turns must be >= 1, got %d (§11 runaway cap)", e.MaxTurns)
	}
	if e.TurnTimeout < Duration(time.Second) {
		fail("engine.turn_timeout must be at least 1s, got %v (§11 — a turn without a wall clock is the runaway shape)", e.TurnTimeout.Duration())
	}
	// The enrolled set must be DECLARED, not defaulted or empty: hooks
	// are the enforcement and reflection substrate (§2), and an engine
	// pinned with zero hooks is the un-enforced shape §7 forbids —
	// omitting the key must not quietly produce it.
	if len(e.Hooks) == 0 {
		fail("engine.hooks must declare the enrolled hook set — an engine with no hooks has no enforcement substrate (§2, §7)")
	}
	seen := make(map[string]bool)
	for i, h := range e.Hooks {
		if h == "" {
			fail("engine.hooks[%d]: empty hook name", i)
			continue
		}
		if seen[h] {
			fail("engine.hooks[%d]: duplicate hook %q — the enrolled set is a set", i, h)
		}
		seen[h] = true
	}
}

func (c *Config) validatePolicy(fail failFunc) {
	defaults := defaultMatrix()
	for capability, row := range c.Policy.Matrix {
		if _, ok := defaults[capability]; !ok {
			fail("policy.matrix.%s: unknown capability — the §7 table is a closed set", capability)
		}
		for _, column := range []string{"owner", "known", "untrusted"} {
			switch row.Action(column) {
			case "allow", "ask", "gate", "deny", "sandbox":
			default:
				fail("policy.matrix.%s.%s: invalid action %q (allow|ask|gate|deny|sandbox)", capability, column, row.Action(column))
			}
		}
	}
}

func (c *Config) validateIdentities(fail failFunc) {
	seen := make(map[[2]string]bool)
	for i, id := range c.Identities {
		where := fmt.Sprintf("identity[%d] (%s/%s)", i, id.Channel, id.NativeID)
		if id.NativeID == "" {
			fail("%s: native_id is required", where)
		}
		if id.Trust != "owner" && id.Trust != "known" {
			fail("%s: trust must be %q or %q, got %q — untrusted is the absence of a row, never enrolled", where, "owner", "known", id.Trust)
		}
		if id.Trust == "owner" && id.OwnerID == "" {
			fail("%s: owner rows require owner_id — cross-surface approval matches on it (§4.4)", where)
		}
		if id.Trust != "owner" && id.OwnerID != "" {
			fail("%s: owner_id is only valid on owner rows — approval authorization matches on it, so a non-owner row must not carry it (§4.4, §6)", where)
		}
		ch, ok := c.Channels[id.Channel]
		if !ok {
			fail("%s: channel %q is not defined under [channels]", where, id.Channel)
		} else if id.Trust == "owner" && ch.Auth == "weak" {
			fail("%s: owner trust on a weak-auth channel would be clamped to known at runtime (§6) — enroll the owner on a strong channel instead", where)
		}
		key := [2]string{id.Channel, id.NativeID}
		if seen[key] {
			fail("%s: duplicate (channel, native_id) — mirrors the identities table primary key", where)
		}
		seen[key] = true
	}
}
