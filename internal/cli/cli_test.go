package cli_test

import (
	"strings"
	"testing"

	"github.com/brian-bell/approach/internal/cli"
)

func TestRun(t *testing.T) {
	tests := []struct {
		name       string
		args       []string
		wantCode   int
		wantStdout string
		wantStderr string
	}{
		{
			name:       "version prints a version and exits 0",
			args:       []string{"version"},
			wantCode:   0,
			wantStdout: "approach ",
		},
		{
			name:       "no args prints usage and exits 2",
			args:       nil,
			wantCode:   2,
			wantStderr: "usage:",
		},
		{
			name:       "unknown subcommand errors and exits 2",
			args:       []string{"nonsense"},
			wantCode:   2,
			wantStderr: "unknown command",
		},
		{
			name:       "config check on a valid file exits 0",
			args:       []string{"config", "check", "../../docs/approach.toml.example"},
			wantCode:   0,
			wantStdout: "config OK",
		},
		{
			name:       "config check on an invalid file exits 1 with the errors",
			args:       []string{"config", "check", "testdata/invalid.toml"},
			wantCode:   1,
			wantStderr: "models.message",
		},
		{
			name:       "config check without a path exits 2",
			args:       []string{"config", "check"},
			wantCode:   2,
			wantStderr: "usage:",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var stdout, stderr strings.Builder
			code := cli.Run(tt.args, &stdout, &stderr)

			if code != tt.wantCode {
				t.Errorf("Run(%v) exit code = %d, want %d", tt.args, code, tt.wantCode)
			}
			if !strings.Contains(stdout.String(), tt.wantStdout) {
				t.Errorf("Run(%v) stdout = %q, want it to contain %q", tt.args, stdout.String(), tt.wantStdout)
			}
			if !strings.Contains(stderr.String(), tt.wantStderr) {
				t.Errorf("Run(%v) stderr = %q, want it to contain %q", tt.args, stderr.String(), tt.wantStderr)
			}
		})
	}
}
