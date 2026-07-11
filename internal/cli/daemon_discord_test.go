package cli_test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/brian-bell/approach/internal/cli"
)

// TestDaemonRefusesUnreadableDiscordToken: an explicitly configured
// token_file that cannot be read (missing, empty) is a startup refusal
// no restart can fix — exit 3 (exitUnrecoverable), one actionable
// journal record naming the PATH, never any credential content, and no
// readiness line.
func TestDaemonRefusesUnreadableDiscordToken(t *testing.T) {
	cases := []struct {
		name    string
		content string // "" means do not create the token file
	}{
		{"missing token file", ""},
		{"empty token file", "  \n"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			dir := shortTempDir(t)
			state := filepath.Join(dir, "state")
			tokenPath := filepath.Join(dir, "discord-token")
			if tc.content != "" {
				if err := os.WriteFile(tokenPath, []byte(tc.content), 0o600); err != nil {
					t.Fatalf("write token file: %v", err)
				}
			}
			configPath := filepath.Join(dir, "approach.toml")
			cfg := minimalConfig + "\n[channels.sms]\nauth = \"weak\"\n"
			cfg = strings.Replace(cfg, "auth = \"strong\"", "auth = \"strong\"\ntoken_file = \""+tokenPath+"\"", 1)
			if err := os.WriteFile(configPath, []byte(cfg), 0o600); err != nil {
				t.Fatalf("write config: %v", err)
			}

			var out, errW strings.Builder
			if code := cli.Run([]string{"daemon", "--state", state, "--config", configPath}, &out, &errW); code != 3 {
				t.Errorf("daemon exit = %d, want 3 (unrecoverable — a restart cannot mint a credential)", code)
			}
			if strings.Contains(out.String(), "listening") {
				t.Errorf("stdout = %q — printed the readiness line despite refusing startup", out.String())
			}
			if !strings.Contains(errW.String(), tokenPath) {
				t.Errorf("stderr %q does not name the token file path", errW.String())
			}
			// The refusal must land BEFORE the store opens: a bad
			// credential must not migrate the schema or run the
			// identity full-sync first (same rule as a bad --config).
			if _, err := os.Stat(filepath.Join(state, "approach.db")); err == nil {
				t.Error("approach.db exists — the daemon touched the store before refusing startup")
			}
		})
	}
}

// TestDaemonWithoutDiscordTokenWarnsAndServes: [channels.discord] with
// no token_file is a bootable posture — the channel may exist only to
// enroll identities before the credential is deployed — but the
// dormant adapter must be loud (§6 posture: quiet degradation is how
// a dead channel goes unnoticed for a week).
func TestDaemonWithoutDiscordTokenWarnsAndServes(t *testing.T) {
	dir := shortTempDir(t)
	state := filepath.Join(dir, "state")
	socket := filepath.Join(state, "approach.sock")
	configPath := filepath.Join(dir, "approach.toml")
	if err := os.WriteFile(configPath, []byte(minimalConfig), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}

	done, daemonErr := startDaemon(t, socket, "--state", state, "--config", configPath)
	drainDaemon(t, socket, done, daemonErr)

	stderr := daemonErr.String()
	if !strings.Contains(stderr, "token_file") || !strings.Contains(stderr, "WARN") {
		t.Errorf("daemon stderr %q has no warning about the tokenless discord channel", stderr)
	}
}
