// Package systemd_test validates the shipped unit files as static
// config: parseable, correctly grouped under approach.target (§7 kill
// switch), and portable. No systemd is needed, so these run everywhere
// the Go tests run.
package systemd_test

import (
	"os"
	"strings"
	"testing"
)

// unit is a parsed systemd unit: section → key → values (systemd
// allows repeated keys, so values accumulate).
type unit map[string]map[string][]string

// parseUnit reads a systemd unit file with a minimal INI parser.
func parseUnit(t *testing.T, path string) unit {
	t.Helper()
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	u := unit{}
	section := ""
	for i, line := range strings.Split(string(raw), "\n") {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") || strings.HasPrefix(line, ";") {
			continue
		}
		if strings.HasPrefix(line, "[") && strings.HasSuffix(line, "]") {
			section = strings.Trim(line, "[]")
			if u[section] == nil {
				u[section] = map[string][]string{}
			}
			continue
		}
		key, value, found := strings.Cut(line, "=")
		if !found || section == "" {
			t.Fatalf("%s:%d: malformed unit line %q", path, i+1, line)
		}
		key = strings.TrimSpace(key)
		u[section][key] = append(u[section][key], strings.TrimSpace(value))
	}
	return u
}

// get returns the single value for section/key, failing the test on
// absence or duplicates.
func (u unit) get(t *testing.T, path, section, key string) string {
	t.Helper()
	values := u[section][key]
	if len(values) != 1 {
		t.Fatalf("%s: [%s] %s = %v, want exactly one value", path, section, key, values)
	}
	return values[0]
}

// TestDaemonService: the service must run `approach daemon`, restart
// on failure, and start/stop with the target.
func TestDaemonService(t *testing.T) {
	u := parseUnit(t, "approach.service")

	exec := u.get(t, "approach.service", "Service", "ExecStart")
	if !strings.Contains(exec, "approach daemon") {
		t.Errorf("ExecStart = %q, want it to run `approach daemon`", exec)
	}
	if got := u.get(t, "approach.service", "Service", "Restart"); got != "on-failure" {
		t.Errorf("Restart = %q, want on-failure", got)
	}
	if got := u.get(t, "approach.service", "Install", "WantedBy"); got != "approach.target" {
		t.Errorf("WantedBy = %q, want approach.target (starting the target starts the daemon)", got)
	}
	// Exit 3 is the daemon's unrecoverable-refusal status (internal/cli
	// exitUnrecoverable): without this exclusion, a schema-too-new
	// refusal would restart-loop every RestartSec instead of staying
	// down with one actionable journal record.
	if got := u.get(t, "approach.service", "Service", "RestartPreventExitStatus"); got != "3" {
		t.Errorf("RestartPreventExitStatus = %q, want 3 (unrecoverable startup refusal)", got)
	}
}

// TestEveryUnitIsGroupedUnderTheTarget is the §7 kill-switch
// invariant: EVERY service and timer shipped here — including ones
// added later (heartbeat, reflection, NL-cron) — must be PartOf
// approach.target and WantedBy it, or `systemctl --user stop
// approach.target` silently stops covering it. A new unit dropped
// into this directory without the grouping fails the build's tests.
func TestEveryUnitIsGroupedUnderTheTarget(t *testing.T) {
	entries, err := os.ReadDir(".")
	if err != nil {
		t.Fatalf("read unit directory: %v", err)
	}
	units := 0
	for _, entry := range entries {
		name := entry.Name()
		if !strings.HasSuffix(name, ".service") && !strings.HasSuffix(name, ".timer") {
			continue
		}
		units++
		u := parseUnit(t, name)
		if got := u.get(t, name, "Unit", "PartOf"); got != "approach.target" {
			t.Errorf("%s: PartOf = %q, want approach.target — the kill switch must stop it", name, got)
		}
		if got := u.get(t, name, "Install", "WantedBy"); got != "approach.target" {
			t.Errorf("%s: WantedBy = %q, want approach.target — starting the target must start it", name, got)
		}
	}
	if units == 0 {
		t.Fatal("no .service or .timer units found next to this test")
	}
}

// TestNoHardcodedHomePaths: units must use systemd specifiers (%h),
// not a developer's absolute home, or they break on the next machine.
func TestNoHardcodedHomePaths(t *testing.T) {
	entries, err := os.ReadDir(".")
	if err != nil {
		t.Fatalf("read unit directory: %v", err)
	}
	for _, entry := range entries {
		name := entry.Name()
		if !strings.HasSuffix(name, ".service") && !strings.HasSuffix(name, ".timer") && !strings.HasSuffix(name, ".target") {
			continue
		}
		raw, err := os.ReadFile(name)
		if err != nil {
			t.Fatalf("read %s: %v", name, err)
		}
		for _, banned := range []string{"/home/", "/Users/"} {
			if strings.Contains(string(raw), banned) {
				t.Errorf("%s: contains hardcoded path prefix %q — use %%h", name, banned)
			}
		}
	}
}

// TestTargetUnit: approach.target is the §7 kill-switch handle — it
// must exist and attach to the user session's default target.
func TestTargetUnit(t *testing.T) {
	u := parseUnit(t, "approach.target")

	if u.get(t, "approach.target", "Unit", "Description") == "" {
		t.Error("approach.target: empty Description")
	}
	if got := u.get(t, "approach.target", "Install", "WantedBy"); got != "default.target" {
		t.Errorf("approach.target: WantedBy = %q, want default.target", got)
	}
}
