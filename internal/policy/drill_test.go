package policy_test

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/brian-bell/approach/internal/policy"
)

// TestDrillHostReadWriteDenied is the §9 PS drill for the path
// denylist: a host Read or Write naming ~/.ssh or the .claude control
// surface must be denied. The C9 PreToolUse hook (M1) answers from
// DeniedPath, so the drill drives THAT surface with the real host
// spellings an attack would use — the actual home directory, not a
// fixture. One verdict covers read AND write by construction: DeniedPath
// has no mode parameter, so there is no read-only loophole to drift in.
func TestDrillHostReadWriteDenied(t *testing.T) {
	home, err := os.UserHomeDir()
	if err != nil {
		t.Fatalf("resolve real home directory: %v", err)
	}

	for _, target := range []string{
		filepath.Join(home, ".ssh"),
		filepath.Join(home, ".ssh", "id_ed25519"),
		filepath.Join(home, ".ssh", "authorized_keys"),
		filepath.Join(home, ".claude"),
		filepath.Join(home, ".claude", "settings.json"),
		filepath.Join(home, ".claude", "settings.local.json"),
		filepath.Join(home, ".claude", "hooks", "pre-tool-use.sh"),
		filepath.Join(home, ".claude", "skills", "deploy", "SKILL.md"),
	} {
		denied, reason := policy.DeniedPath(target)
		if !denied {
			t.Errorf("host path %s read as allowed, want §7 denylist refusal", target)
		}
		if denied && reason == "" {
			t.Errorf("host path %s denied without a reason — the refusal message needs one", target)
		}
	}

	// The drill must not overclaim: only the enumerated control surface
	// inside .claude is denied — commands and docs stay reachable, or
	// the deny would push users into disabling the gate.
	for _, target := range []string{
		filepath.Join(home, ".claude", "commands", "ship.md"),
		filepath.Join(home, ".claude", "CLAUDE.md"),
	} {
		if denied, reason := policy.DeniedPath(target); denied {
			t.Errorf("host path %s denied (%s), want allowed — the denylist is targeted, not blanket", target, reason)
		}
	}
}

// TestDrillRecursiveHomeReadDenied is the recursive half of the §9 PS
// drill: a directory Read (or Grep) is a walk, so the verdict must hold
// for reads ROOTED at a denied directory and for reads of an ancestor
// that would traverse one. A fixture home stands in for the real one —
// walking the operator's actual home in a test would be slow and
// privacy-hostile; the decision logic is identical.
func TestDrillRecursiveHomeReadDenied(t *testing.T) {
	home := t.TempDir()
	for path, content := range map[string]string{
		filepath.Join(home, ".ssh", "id_rsa"):                 "PRIVATE KEY",
		filepath.Join(home, ".claude", "hooks", "hook.sh"):    "#!/bin/sh",
		filepath.Join(home, ".claude", "commands", "ship.md"): "docs",
		filepath.Join(home, "src", "main.go"):                 "package main",
	} {
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			t.Fatalf("mkdir %s: %v", filepath.Dir(path), err)
		}
		if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
			t.Fatalf("write %s: %v", path, err)
		}
	}

	// Reads rooted AT the denied directories are refused by name.
	for _, root := range []string{
		filepath.Join(home, ".ssh"),
		filepath.Join(home, ".claude", "hooks"),
	} {
		path, _, err := policy.DeniedDir(home, root)
		if err != nil {
			t.Fatalf("DeniedDir rooted at %s: %v", root, err)
		}
		if path == "" {
			t.Errorf("recursive read rooted at %s allowed, want refusal", root)
		}
	}

	// A recursive read of the whole home traverses .ssh — refused.
	path, _, err := policy.DeniedDir(home, home)
	if err != nil {
		t.Fatalf("DeniedDir over the home fixture: %v", err)
	}
	if path == "" {
		t.Error("recursive read of a home containing .ssh allowed, want refusal")
	}

	// And the denial is targeted: benign roots inside the same home,
	// including the .claude/commands carve-out, stay readable.
	for _, root := range []string{
		filepath.Join(home, "src"),
		filepath.Join(home, ".claude", "commands"),
	} {
		path, reason, err := policy.DeniedDir(home, root)
		if err != nil {
			t.Fatalf("DeniedDir rooted at %s: %v", root, err)
		}
		if path != "" {
			t.Errorf("benign root %s refused (%s at %s), want allowed", root, reason, path)
		}
	}
}
