package engine_test

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/brian-bell/approach/internal/engine"
)

// versionCLI writes a fake binary that prints the given --version
// output.
func versionCLI(t *testing.T, output string) string {
	t.Helper()
	dir := t.TempDir()
	bin := filepath.Join(dir, "claude")
	script := fmt.Sprintf("#!/bin/sh\nprintf '%%s\\n' %q\n", output)
	if err := os.WriteFile(bin, []byte(script), 0o755); err != nil {
		t.Fatalf("write fake cli: %v", err)
	}
	return bin
}

// TestVerifyVersion: the §2 pin check — exact match passes, drift and
// unprovable versions refuse.
func TestVerifyVersion(t *testing.T) {
	ctx := context.Background()
	cases := []struct {
		name, output, pin, wantErr string
	}{
		{"exact match", "2.1.199 (Claude Code)", "2.1.199", ""},
		{"bare version", "2.1.199", "2.1.199", ""},
		{"drift", "2.2.0 (Claude Code)", "2.1.199", "2.2.0"},
		{"unparseable", "development build", "2.1.199", "cannot prove"},
		{"empty pin", "2.1.199", "", "empty version pin"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := engine.VerifyVersion(ctx, versionCLI(t, tc.output), tc.pin)
			if tc.wantErr == "" {
				if err != nil {
					t.Fatalf("VerifyVersion: %v, want nil", err)
				}
				return
			}
			if err == nil {
				t.Fatal("VerifyVersion accepted, want refusal")
			}
			if !strings.Contains(err.Error(), tc.wantErr) {
				t.Errorf("error %q does not mention %q", err.Error(), tc.wantErr)
			}
		})
	}
}

// TestVerifyVersionMissingBinary: a binary that is not there cannot
// prove its pin — loud error, never a skipped check.
func TestVerifyVersionMissingBinary(t *testing.T) {
	err := engine.VerifyVersion(context.Background(), filepath.Join(t.TempDir(), "claude"), "2.1.199")
	if err == nil {
		t.Fatal("VerifyVersion accepted a missing binary")
	}
}

// TestVerifyVersionRelativePath: PATH resolution is not a pin.
func TestVerifyVersionRelativePath(t *testing.T) {
	if err := engine.VerifyVersion(context.Background(), "claude", "2.1.199"); err == nil {
		t.Fatal("VerifyVersion accepted a relative binary path")
	}
}
