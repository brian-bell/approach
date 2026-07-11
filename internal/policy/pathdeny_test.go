package policy_test

import (
	"io/fs"
	"os"
	"path/filepath"
	"testing"
	"testing/fstest"

	"github.com/brian-bell/approach/internal/policy"
)

// TestDeniedPath: the §7 hard path denylist — ~/.ssh, credentials, and
// the harness's own control surface (.claude hooks·settings·skills,
// .codex, approach.toml) are refused for BOTH read and write, so
// neither credential exfiltration nor the worst escalation — the agent
// editing its own PreToolUse hook to disable the gate — is expressible
// in-band.
func TestDeniedPath(t *testing.T) {
	denied := []string{
		// .ssh, wherever it sits — home, bare, nested in a repo.
		"/Users/brian/.ssh/id_rsa",
		"/Users/brian/.ssh",
		"repo/.ssh/key",
		// Lexical dodges: traversal and case games must not work.
		"work/../.ssh/key",
		"/Users/brian/.SSH/id_rsa",
		// Credential stores — named exactly, named partially, netrc.
		"/Users/brian/.aws/credentials",
		"secrets/credentials/token.json",
		"/opt/app/Credentials",
		"/Users/brian/.git-credentials",
		"project/oauth/credentials.json",
		"/Users/brian/.netrc",
		"C:/Users/brian/_netrc",
		// Fail-safe: even a doc naming itself after credentials.
		"docs/credentials.md",
		// Well-known token stores whose names lack the word.
		"/Users/brian/.npmrc",
		"/Users/brian/.pypirc",
		"/Users/brian/.aws/config",
		"/Users/brian/.docker/config.json",
		"/Users/brian/.kube/config",
		"/Users/brian/.gnupg/private-keys-v1.d/ABCDEF.key",
		"/Users/brian/.config/gh/hosts.yml",
		"/Users/brian/.config/gcloud/access_tokens.db",
		"repo/.env",
		"repo/.env.production",
		// The harness control surface.
		"/Users/brian/.claude/hooks/pretooluse.sh",
		"repo/.claude/settings.json",
		"repo/.claude/settings.local.json",
		"/Users/brian/.claude/skills/x/SKILL.md",
		"/Users/brian/.claude",
		"/Users/brian/.codex/config.toml",
		"repo/.codex",
		// THE config file, wherever it lives.
		"/Users/brian/approach/approach.toml",
		"approach.toml",
	}
	for _, path := range denied {
		isDenied, reason := policy.DeniedPath(path)
		if !isDenied {
			t.Errorf("DeniedPath(%q) = allowed, want denied", path)
			continue
		}
		if reason == "" {
			t.Errorf("DeniedPath(%q) denied with empty reason — refusals must name the rule", path)
		}
	}

	allowed := []string{
		"/Users/brian/dev/approach/internal/store/store.go",
		"README.md",
		// Only the enumerated .claude control surface is refused.
		"repo/.claude/commands/deploy.md",
		// Name near-misses stay reachable.
		"notes/ssh-setup.md",
		"docs/approach.toml.example",
		"src/environment.ts",
		"/Users/brian/.config/nvim/init.lua",
	}
	for _, path := range allowed {
		if isDenied, reason := policy.DeniedPath(path); isDenied {
			t.Errorf("DeniedPath(%q) = denied (%s), want allowed", path, reason)
		}
	}
}

// TestDeniedDescendant: a recursive read rooted at an allowed ancestor
// traverses everything beneath it, so an allowed root with a denied
// descendant must yield a deny verdict — otherwise Grep from the repo
// root walks straight into .env or .claude/settings.json.
func TestDeniedDescendant(t *testing.T) {
	dirty := fstest.MapFS{
		"README.md":              &fstest.MapFile{Data: []byte("ok")},
		"src/main.go":            &fstest.MapFile{Data: []byte("ok")},
		".claude/settings.json":  &fstest.MapFile{Data: []byte("secret")},
		".claude/commands/ok.md": &fstest.MapFile{Data: []byte("ok")},
	}
	path, reason, err := policy.DeniedDescendant(dirty, "/repo")
	if err != nil {
		t.Fatalf("DeniedDescendant: %v", err)
	}
	if path == "" {
		t.Error("repo containing .claude/settings.json read as clean, want a denied descendant")
	}
	if reason == "" && path != "" {
		t.Error("denied descendant reported with empty reason")
	}

	// A .claude subtree holding only allowed content stays readable —
	// the walk judges its children, not the directory's bare name.
	claudeOK := fstest.MapFS{
		"README.md":               &fstest.MapFile{Data: []byte("ok")},
		".claude/commands/ok.md":  &fstest.MapFile{Data: []byte("ok")},
		".claude/commands/two.md": &fstest.MapFile{Data: []byte("ok")},
	}
	path, reason, err = policy.DeniedDescendant(claudeOK, "/repo")
	if err != nil {
		t.Fatalf("DeniedDescendant on allowed .claude content: %v", err)
	}
	if path != "" {
		t.Errorf(".claude with only commands refused (%s at %s) — only hooks/skills/settings are control surface", reason, path)
	}

	clean := fstest.MapFS{
		"README.md":   &fstest.MapFile{Data: []byte("ok")},
		"src/main.go": &fstest.MapFile{Data: []byte("ok")},
	}
	path, _, err = policy.DeniedDescendant(clean, "/repo")
	if err != nil {
		t.Fatalf("DeniedDescendant on clean tree: %v", err)
	}
	if path != "" {
		t.Errorf("clean tree reported denied descendant %q", path)
	}

	// Symlinks are resolved and revalidated, not blanket-refused:
	// denied or root-escaping targets fail closed, while an in-tree
	// link (node_modules/.bin style) stays legal — the walk visits its
	// real target anyway.
	for name, badTarget := range map[string]string{
		"absolute denied target":  "/Users/alice/.ssh",
		"relative denied target":  "../../.ssh",
		"escapes the read root":   "../../elsewhere/data",
		"absolute allowed target": "/opt/data/files",
	} {
		linked := fstest.MapFS{
			"README.md": &fstest.MapFile{Data: []byte("ok")},
			"src/cache": &fstest.MapFile{Data: []byte(badTarget), Mode: fs.ModeSymlink},
		}
		path, reason, err = policy.DeniedDescendant(linked, "/repo")
		if err != nil {
			t.Fatalf("DeniedDescendant (%s): %v", name, err)
		}
		if path == "" {
			t.Errorf("%s: symlink src/cache -> %s read as clean, want refused", name, badTarget)
		} else if reason == "" {
			t.Errorf("%s: refused with empty reason", name)
		}
	}
	// A symlink NAMED like a denied path is judged by its own name
	// first — a benign target must not launder it.
	deniedName := fstest.MapFS{
		"README.md": &fstest.MapFile{Data: []byte("ok")},
		".codex":    &fstest.MapFile{Data: []byte("harmless"), Mode: fs.ModeSymlink},
		"harmless":  &fstest.MapFile{Data: []byte("ok")},
	}
	path, _, err = policy.DeniedDescendant(deniedName, "/repo")
	if err != nil {
		t.Fatalf("DeniedDescendant on denied-name symlink: %v", err)
	}
	if path == "" {
		t.Error("symlink named .codex read as clean — the entry's own name is denied")
	}

	benign := fstest.MapFS{
		"node_modules/.bin/tsc":           &fstest.MapFile{Data: []byte("../typescript/bin/tsc"), Mode: fs.ModeSymlink},
		"node_modules/typescript/bin/tsc": &fstest.MapFile{Data: []byte("ok")},
	}
	path, reason, err = policy.DeniedDescendant(benign, "/repo")
	if err != nil {
		t.Fatalf("DeniedDescendant on benign symlink: %v", err)
	}
	if path != "" {
		t.Errorf("in-tree symlink refused (%s at %s) — routine workspace links must stay legal", reason, path)
	}

	// A denied root is its own verdict — no walk needed.
	path, _, err = policy.DeniedDescendant(clean, "/Users/brian/.ssh")
	if err != nil {
		t.Fatalf("DeniedDescendant on denied root: %v", err)
	}
	if path == "" {
		t.Error("denied root read as clean")
	}
}

// TestDeniedDir: the os entry point resolves the read root's own
// symlink chain before walking — os.DirFS follows a symlinked root
// silently, so a benignly named link out of the cwd subtree must be
// refused here, not walked under its innocent name.
func TestDeniedDir(t *testing.T) {
	base := t.TempDir()
	cwd := filepath.Join(base, "cwd")
	outside := filepath.Join(base, "outside")
	for _, dir := range []string{
		filepath.Join(cwd, "src"),
		filepath.Join(outside, "data"),
	} {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			t.Fatalf("mkdir %s: %v", dir, err)
		}
	}
	if err := os.WriteFile(filepath.Join(cwd, "src", "main.go"), []byte("ok"), 0o644); err != nil {
		t.Fatalf("write file: %v", err)
	}
	if err := os.Symlink(outside, filepath.Join(cwd, "cache")); err != nil {
		t.Fatalf("symlink: %v", err)
	}

	// An in-cwd, clean root is allowed — absolute or relative (a
	// relative root anchors to the session cwd, not the daemon's).
	path, reason, err := policy.DeniedDir(cwd, filepath.Join(cwd, "src"))
	if err != nil {
		t.Fatalf("DeniedDir on clean root: %v", err)
	}
	if path != "" {
		t.Errorf("clean in-cwd root refused (%s at %s)", reason, path)
	}
	path, reason, err = policy.DeniedDir(cwd, "src")
	if err != nil {
		t.Fatalf("DeniedDir on relative root: %v", err)
	}
	if path != "" {
		t.Errorf("relative in-cwd root refused (%s at %s)", reason, path)
	}

	// A link that leaves the read root but stays inside the session cwd
	// is legal — the §7 boundary is the cwd subtree — and its target
	// tree is walked too, so a secret behind it still surfaces.
	if err := os.MkdirAll(filepath.Join(cwd, "build", "generated"), 0o755); err != nil {
		t.Fatalf("mkdir build: %v", err)
	}
	if err := os.Symlink(filepath.Join(cwd, "build", "generated"), filepath.Join(cwd, "src", "generated")); err != nil {
		t.Fatalf("symlink generated: %v", err)
	}
	path, reason, err = policy.DeniedDir(cwd, filepath.Join(cwd, "src"))
	if err != nil {
		t.Fatalf("DeniedDir with in-cwd link: %v", err)
	}
	if path != "" {
		t.Errorf("in-cwd out-of-root link refused (%s at %s) — the boundary is the cwd, not the read root", reason, path)
	}
	if err := os.WriteFile(filepath.Join(cwd, "build", "generated", ".env"), []byte("SECRET=1"), 0o644); err != nil {
		t.Fatalf("write linked .env: %v", err)
	}
	path, _, err = policy.DeniedDir(cwd, filepath.Join(cwd, "src"))
	if err != nil {
		t.Fatalf("DeniedDir with dirty linked tree: %v", err)
	}
	if path == "" {
		t.Error("secret behind an in-cwd link read as clean — external targets must be walked too")
	}
	if err := os.Remove(filepath.Join(cwd, "build", "generated", ".env")); err != nil {
		t.Fatalf("remove linked .env: %v", err)
	}

	// A symlinked root escaping the cwd subtree is refused even though
	// its own name is benign.
	path, _, err = policy.DeniedDir(cwd, filepath.Join(cwd, "cache"))
	if err != nil {
		t.Fatalf("DeniedDir on escaping root: %v", err)
	}
	if path == "" {
		t.Error("symlinked root escaping the cwd read as clean, want refused")
	}

	// A denylisted root NAME is refused before resolution — a benign
	// in-cwd target must not launder it.
	if err := os.Symlink(filepath.Join(cwd, "src"), filepath.Join(cwd, ".codex")); err != nil {
		t.Fatalf("symlink .codex: %v", err)
	}
	path, _, err = policy.DeniedDir(cwd, filepath.Join(cwd, ".codex"))
	if err != nil {
		t.Fatalf("DeniedDir on denied-name root: %v", err)
	}
	if path == "" {
		t.Error("root named .codex read as clean via its benign target, want refused by name")
	}

	// A root that does not resolve fails CLOSED — as an error, never a
	// silent allow.
	if _, _, err := policy.DeniedDir(cwd, filepath.Join(cwd, "missing")); err == nil {
		t.Error("unresolvable root returned no error, want fail-closed resolution error")
	}

	// A denied descendant inside the cwd still surfaces through DeniedDir.
	if err := os.WriteFile(filepath.Join(cwd, "src", ".env"), []byte("SECRET=1"), 0o644); err != nil {
		t.Fatalf("write .env: %v", err)
	}
	path, _, err = policy.DeniedDir(cwd, filepath.Join(cwd, "src"))
	if err != nil {
		t.Fatalf("DeniedDir with denied descendant: %v", err)
	}
	if path == "" {
		t.Error("root containing .env read as clean, want denied descendant")
	}
}
