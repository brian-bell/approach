package config_test

import (
	"strings"
	"testing"
	"time"

	"github.com/brian-bell/approach/internal/config"
)

const validEngine = `
[engine]
bin = "/usr/local/bin/claude"
version = "2.1.199"
hooks = ["Stop"]
`

// engineNoHooks is the base for cases that supply their own hooks key.
const engineNoHooks = `
[engine]
bin = "/usr/local/bin/claude"
version = "2.1.199"
`

// TestEngineSection: the §2 pins as config — binary path, version,
// runaway caps, and the enrolled hook set.
func TestEngineSection(t *testing.T) {
	c, err := config.Parse(strings.NewReader(validModels + `
[engine]
bin = "/opt/claude/bin/claude"
version = "2.1.199"
max_turns = 40
turn_timeout = "5m"
hooks = ["SessionStart", "UserPromptSubmit", "PreToolUse", "PostToolUse", "Stop", "SessionEnd"]
`))
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	e := c.Engine
	if e == nil {
		t.Fatal("engine section did not parse")
	}
	if e.Bin != "/opt/claude/bin/claude" || e.Version != "2.1.199" || e.MaxTurns != 40 {
		t.Errorf("engine = %+v, want the configured pin", e)
	}
	if e.TurnTimeout.Duration() != 5*time.Minute {
		t.Errorf("turn_timeout = %v, want 5m", e.TurnTimeout.Duration())
	}
	if len(e.Hooks) != 6 || e.Hooks[0] != "SessionStart" {
		t.Errorf("hooks = %v, want the enrolled set in order", e.Hooks)
	}
}

// TestEngineDefaults: max_turns and turn_timeout have safe defaults;
// bin and version never do — a defaulted pin is not a pin.
func TestEngineDefaults(t *testing.T) {
	c, err := config.Parse(strings.NewReader(validModels + validEngine))
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if c.Engine.MaxTurns != 25 {
		t.Errorf("default max_turns = %d, want 25", c.Engine.MaxTurns)
	}
	if c.Engine.TurnTimeout.Duration() != 10*time.Minute {
		t.Errorf("default turn_timeout = %v, want 10m", c.Engine.TurnTimeout.Duration())
	}
}

// TestEngineAbsent: no [engine] section is a bootable, dormant posture
// — the daemon runs (adapters, queues) without an engine pin until one
// is deployed.
func TestEngineAbsent(t *testing.T) {
	c, err := config.Parse(strings.NewReader(validModels))
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if c.Engine != nil {
		t.Errorf("absent engine section parsed as %+v, want nil", c.Engine)
	}
}

// TestEngineValidation: the pin is security-load-bearing — every hole
// fails loud at load time.
func TestEngineValidation(t *testing.T) {
	cases := []struct {
		name, engine, wantErr string
	}{
		{"missing bin", "[engine]\nversion = \"2.1.199\"\nhooks = [\"Stop\"]\n", "engine.bin"},
		{"relative bin", "[engine]\nbin = \"claude\"\nversion = \"2.1.199\"\nhooks = [\"Stop\"]\n", "engine.bin"},
		{"missing version", "[engine]\nbin = \"/usr/local/bin/claude\"\nhooks = [\"Stop\"]\n", "engine.version"},
		{"zero max_turns", validEngine + "max_turns = 0\n", "engine.max_turns"},
		{"negative max_turns", validEngine + "max_turns = -3\n", "engine.max_turns"},
		{"sub-second turn_timeout", validEngine + "turn_timeout = \"500ms\"\n", "engine.turn_timeout"},
		{"missing hooks (no enforcement substrate)", engineNoHooks, "engine.hooks"},
		{"empty hooks list", engineNoHooks + "hooks = []\n", "engine.hooks"},
		{"duplicate hook", engineNoHooks + "hooks = [\"Stop\", \"Stop\"]\n", "engine.hooks"},
		{"empty hook name", engineNoHooks + "hooks = [\"\"]\n", "engine.hooks"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := config.Parse(strings.NewReader(validModels + tc.engine))
			if err == nil {
				t.Fatal("want validation error, got nil")
			}
			if !strings.Contains(err.Error(), tc.wantErr) {
				t.Errorf("error %q does not name %s", err.Error(), tc.wantErr)
			}
		})
	}
}
