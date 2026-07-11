// Package policy holds the §7 policy-gate decision rules. This
// milestone lands the hard path denylist; the PreToolUse hook that
// enforces it (C9) is M1 work and answers from these functions.
package policy

import (
	"fmt"
	"io/fs"
	"os"
	gopath "path"
	"path/filepath"
	"strings"
)

// DeniedPath reports whether path touches the §7 read/write denylist:
// ~/.ssh, credential stores, and the harness's own control surface
// (.claude hooks·settings·skills, .codex, approach.toml). One verdict
// covers read AND write — the spec refuses both, so there is no mode
// parameter to get wrong. Matching is lexical over the Cleaned path's
// segments and case-insensitive (the default macOS filesystem is);
// traversal (a/../.ssh) is neutralized by Clean, but symlink resolution
// needs the filesystem and is the ENFORCEMENT point's job — callers
// pass the fully resolved path. The reason names the matched rule for
// the refusal message.
func DeniedPath(path string) (denied bool, reason string) {
	segments := strings.Split(filepath.Clean(path), string(filepath.Separator))
	for i, segment := range segments {
		segment = strings.ToLower(segment)
		switch segment {
		case ".ssh":
			// Stricter than the spec's ~/.ssh on purpose: anchoring on
			// the home directory is resolution logic that can be wrong,
			// and no legitimate repo-local .ssh exists — fail safe.
			return true, "path touches a .ssh directory (§7 denylist)"
		case ".netrc", "_netrc":
			return true, "path touches a netrc credential file (§7 denylist)"
		case ".npmrc", ".pypirc":
			return true, "path touches a package-registry token file (§7 denylist)"
		case ".aws", ".docker", ".kube", ".gnupg":
			return true, "path touches " + segment + " — a well-known credential store (§7 denylist)"
		case ".config":
			// Only the token-bearing tools under ~/.config; the rest of
			// a user's config tree stays reachable.
			if i < len(segments)-1 {
				switch next := strings.ToLower(segments[i+1]); next {
				case "gh", "gcloud":
					return true, "path touches .config/" + next + " — a well-known credential store (§7 denylist)"
				}
			}
		case ".codex":
			return true, "path touches .codex — harness control surface (§7 denylist)"
		case ".claude":
			// Only the enumerated control surface: hooks, skills, and
			// settings files. Other .claude content (commands, docs)
			// stays reachable. A terminal .claude is the surface itself.
			if i == len(segments)-1 {
				return true, "path is a .claude directory — harness control surface (§7 denylist)"
			}
			next := strings.ToLower(segments[i+1])
			if next == "hooks" || next == "skills" || strings.HasPrefix(next, "settings") {
				return true, "path touches .claude " + next + " — harness control surface (§7 denylist)"
			}
		case "approach.toml":
			// THE config file is control surface wherever it lives —
			// exact name only, so approach.toml.example stays readable.
			return true, "path touches approach.toml — harness control surface (§7 denylist)"
		}
		// Substring, not exact: ~/.aws/credentials, .git-credentials,
		// Google OAuth credentials.json, credentials.db … all name
		// themselves. A doc ABOUT credentials is denied too — fail safe;
		// this list is a backstop behind cwd confinement and the C10
		// sandbox, not an enumeration of every secret-bearing file.
		if strings.Contains(segment, "credential") {
			return true, "path touches a credential store (§7 denylist)"
		}
		// Dotenv files: .env, .env.local, .env.production …
		if segment == ".env" || strings.HasPrefix(segment, ".env.") {
			return true, "path touches a dotenv secrets file (§7 denylist)"
		}
	}
	return false, ""
}

// DeniedDir is the os-filesystem entry point for a recursive read
// rooted at root within a session whose working directory is cwd — the
// §7 boundary is the CWD subtree, not the individual read root, so
// routine workspace links that leave the root but stay in the cwd
// (src/generated → ../build/generated) remain legal, and their targets
// are resolved and walked too, so nothing escapes inspection. A
// relative root is anchored to cwd, never the daemon's own working
// directory. The root's own name is judged before resolution — a
// denylisted name must not be laundered by a benign target — and every
// resolution failure is an error, never a silent allow.
func DeniedDir(cwd, root string) (path, reason string, err error) {
	if !filepath.IsAbs(root) {
		root = filepath.Join(cwd, root)
	}
	if denied, why := DeniedPath(root); denied {
		return root, why, nil
	}
	resolvedCwd, err := filepath.EvalSymlinks(cwd)
	if err != nil {
		return "", "", fmt.Errorf("policy: resolve session cwd %s: %w", cwd, err)
	}
	resolvedRoot, err := filepath.EvalSymlinks(root)
	if err != nil {
		return "", "", fmt.Errorf("policy: resolve read root %s: %w", root, err)
	}
	if !within(resolvedCwd, resolvedRoot) {
		return root, "read root resolves outside the session cwd subtree (§7 cwd confinement)", nil
	}

	// Link targets inside the cwd but outside the walked tree become
	// further roots to inspect; the visited set breaks link cycles.
	visited := map[string]bool{}
	queue := []string{resolvedRoot}
	for len(queue) > 0 {
		r := queue[0]
		queue = queue[1:]
		if visited[r] {
			continue
		}
		visited[r] = true
		path, reason, externals, err := walkTree(os.DirFS(r), r)
		if err != nil || path != "" {
			return path, reason, err
		}
		for _, e := range externals {
			target, err := filepath.EvalSymlinks(e.source)
			if err != nil {
				return "", "", fmt.Errorf("policy: resolve symlink %s: %w", e.source, err)
			}
			if !within(resolvedCwd, target) {
				return e.source, "symlink escapes the session cwd subtree (§7 cwd confinement)", nil
			}
			if denied, why := DeniedPath(target); denied {
				return e.source, why, nil
			}
			info, err := os.Stat(target)
			if err != nil {
				return "", "", fmt.Errorf("policy: inspect symlink target %s: %w", target, err)
			}
			if info.IsDir() {
				queue = append(queue, target)
			}
		}
	}
	return "", "", nil
}

// within reports whether p sits inside (or is) the base subtree.
func within(base, p string) bool {
	rel, err := filepath.Rel(base, p)
	return err == nil && rel != ".." && !strings.HasPrefix(rel, ".."+string(filepath.Separator))
}

// DeniedDescendant answers the recursive-read question DeniedPath alone
// cannot: a Grep or directory Read rooted at an ALLOWED ancestor still
// traverses everything beneath it, so the verdict must account for
// descendants, not just the requested root. fsys is the tree at root;
// root must already be symlink-resolved. The boundary here is the
// walked root itself: any symlink leaving it is refused, because a
// bare fs.FS offers nothing beyond the root to inspect — DeniedDir is
// the os entry point that widens the boundary to the session cwd by
// resolving and walking external targets. A walk error is returned as
// an error, never as "allowed": the enforcement point must fail closed
// on what it could not inspect.
func DeniedDescendant(fsys fs.FS, root string) (path, reason string, err error) {
	path, reason, externals, err := walkTree(fsys, root)
	if err != nil || path != "" {
		return path, reason, err
	}
	if len(externals) > 0 {
		return externals[0].source, "symlink escapes the read root (§7 cwd confinement)", nil
	}
	return "", "", nil
}

// externalLink is a walked symlink whose lexical target leaves the
// walked root: legal or not is the CALLER's boundary decision.
type externalLink struct {
	source string // absolute path of the link itself
	target string // absolute lexical target
}

// walkTree walks fsys (the tree at absolute path root) and applies the
// denylist to every entry: the entry's own name is judged FIRST — a
// link NAMED .env or .codex must not slip through on the strength of a
// benign target — then symlinks are resolved lexically and their
// targets judged. In-root targets are allowed through (the walk visits
// the real files anyway); out-of-root targets are collected for the
// caller to judge against ITS boundary. The walk does not descend into
// denied directories — the directory itself is already the verdict.
func walkTree(fsys fs.FS, root string) (path, reason string, externals []externalLink, err error) {
	if denied, why := DeniedPath(root); denied {
		return root, why, nil, nil
	}
	err = fs.WalkDir(fsys, ".", func(p string, d fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return fmt.Errorf("policy: inspect %s: %w", filepath.Join(root, p), walkErr)
		}
		if p == "." {
			return nil
		}
		// Judged with the root joined on, NOT the bare walk-relative
		// entry: rules that span the root boundary (.config/gh under a
		// walk rooted at ~/.config) need the ancestor segments, which
		// WalkDir's relative paths drop.
		if denied, why := DeniedPath(filepath.Join(root, p)); denied {
			// One carve-out: a real .claude DIRECTORY met mid-walk is
			// judged by its children — only the enumerated hooks/
			// skills/settings surface inside it is denied, and commands
			// etc. must stay readable. (DeniedPath refuses a terminal
			// .claude because a direct whole-directory target includes
			// the control surface; here the walk visits every child
			// individually, so descending IS the finer judgment.)
			if !d.IsDir() || !strings.EqualFold(gopath.Base(p), ".claude") {
				path, reason = filepath.Join(root, p), why
				return fs.SkipAll
			}
		}
		if d.Type()&fs.ModeSymlink != 0 {
			rl, ok := fsys.(fs.ReadLinkFS)
			if !ok {
				path, reason = filepath.Join(root, p), "symlinked descendant on a filesystem that cannot resolve links — refused unresolved (§7)"
				return fs.SkipAll
			}
			target, err := rl.ReadLink(p)
			if err != nil {
				path, reason = filepath.Join(root, p), "unreadable symlinked descendant — refused unresolved (§7)"
				return fs.SkipAll
			}
			absTarget := filepath.Clean(target)
			if !filepath.IsAbs(target) {
				absTarget = filepath.Clean(filepath.Join(root, gopath.Dir(p), target))
			}
			if denied, why := DeniedPath(absTarget); denied {
				path, reason = filepath.Join(root, p), why
				return fs.SkipAll
			}
			if !within(root, absTarget) {
				externals = append(externals, externalLink{
					source: filepath.Join(root, p),
					target: absTarget,
				})
			}
		}
		return nil
	})
	if err != nil {
		return "", "", nil, err
	}
	return path, reason, externals, nil
}
