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
	// Anchor everything to an absolute cwd — component-by-component root
	// resolution (rootSpellings) needs an absolute path, and this matches
	// the base EvalSymlinks uses for a relative cwd (the process dir).
	cwd, err = filepath.Abs(cwd)
	if err != nil {
		return "", "", fmt.Errorf("policy: resolve session cwd: %w", err)
	}
	if filepath.IsAbs(root) {
		root = filepath.Clean(root)
	} else {
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

	// The read root reaches the physical tree under several spellings,
	// and a denied path under ANY of them must fire. The resolved real
	// path (resolvedRoot) catches a denied segment at any depth in the
	// real tree — the full walk below. But every hop in the root's own
	// symlink chain (safe -> .config -> config-target) is another
	// spelling of the same final directory, and a denylisted segment
	// (.config) can appear at an intermediate hop that resolution
	// collapses away. Judge the final directory's direct children under
	// each hop's spelling — depth-1 is all the context rules need — so
	// neither a benign link to a denied dir nor a denied-named link
	// (direct or intermediate) to a benign dir can launder the path.
	spellings, err := rootSpellings(root)
	if err != nil {
		return "", "", err
	}
	for _, spelling := range spellings {
		if spelling == resolvedRoot {
			continue
		}
		// The root itself denied under this spelling (reading .config/gh
		// directly), or a direct child that a context rule needs
		// (reading .config, whose gh child is denied).
		if denied, why := DeniedPath(spelling); denied {
			return spelling, why, nil
		}
		if p, r, aerr := aliasDenies(os.DirFS(resolvedRoot), ".", resolvedRoot, spelling); aerr != nil || p != "" {
			return p, r, aerr
		}
	}

	// Link targets inside the cwd but outside the walked tree become
	// further roots to inspect, carrying the source's lexical spelling as
	// the deny context (a link named ~/.config aliases its target's
	// children under .config/…). visited is keyed by resolved physical
	// path so a symlink cycle within the cwd terminates — the alias
	// spelling grows each hop, so keying on it too would never converge.
	visited := map[string]bool{}
	queue := []walkItem{{physical: resolvedRoot, logical: resolvedRoot}}
	for len(queue) > 0 {
		item := queue[0]
		queue = queue[1:]
		if visited[item.physical] {
			// Already walked under some spelling, but a different alias
			// path can reach the same directory carrying a context rule
			// (…/.config) its earlier spelling lacked. Re-walking would
			// risk a cycle, so apply just this alias's depth-1 context to
			// the directory's direct children — all the context rules need.
			p, r, aerr := aliasDenies(os.DirFS(item.physical), ".", item.physical, item.logical)
			if aerr != nil || p != "" {
				return p, r, aerr
			}
			continue
		}
		visited[item.physical] = true
		path, reason, externals, err := walkTreeLogical(os.DirFS(item.physical), item.physical, item.logical)
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
			// Judge the resolved target AND its alias spelling: the link
			// name may carry a context rule (.config/gh) the real path
			// lacks. Re-walking under the source spelling then applies
			// that context to the target's children (depth-1 is all the
			// context rules need).
			if denied, why := DeniedPath(target); denied {
				return e.source, why, nil
			}
			if denied, why := DeniedPath(e.sourceLogical); denied {
				return e.source, why, nil
			}
			info, err := os.Stat(target)
			if err != nil {
				return "", "", fmt.Errorf("policy: inspect symlink target %s: %w", target, err)
			}
			if info.IsDir() {
				// Both spellings of the target enter the queue: the alias
				// spelling because the link name may carry context its real
				// path lacks, and the RESOLVED spelling because the inverse
				// holds too — src/link -> ../real/.config reaches gh/… as
				// src/link/gh under the alias, and only real/.config/gh
				// fires the context rule. Whichever runs second hits the
				// visited branch and gets the depth-1 alias check.
				queue = append(queue, walkItem{physical: target, logical: e.sourceLogical})
				queue = append(queue, walkItem{physical: target, logical: target})
			}
		}
	}
	return "", "", nil
}

// walkItem is a subtree to inspect: physical is where its files are read
// from, logical is the lexical path the denylist judges them under. The
// two differ when a symlink aliases the subtree under a name (…/.config)
// its real location lacks.
type walkItem struct {
	physical string
	logical  string
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
	source        string // absolute physical path of the link itself
	sourceLogical string // the link's lexical spelling (deny context on re-walk)
	target        string // absolute lexical target
}

// walkTree walks fsys (the tree at absolute path root) and applies the
// denylist to every entry under root's own name. See walkTreeLogical
// for the full contract; here physical and logical coincide.
func walkTree(fsys fs.FS, root string) (path, reason string, externals []externalLink, err error) {
	return walkTreeLogical(fsys, root, root)
}

// walkTreeLogical walks fsys — the tree physically at physicalRoot —
// and applies the denylist to every entry under logicalRoot, the
// lexical path by which the entry is reached. The two coincide except
// under an in-root symlink that aliases a subtree beneath a name its
// real location lacks (…/.config -> config-target): the alias spelling
// can match a context rule (.config/gh) the target's real path does
// not, so the aliased subtree is RE-WALKED under the source name, not
// only visited under the target's real one. The entry's own name is
// judged FIRST — a link NAMED .env or .codex must not slip through on
// the strength of a benign target — then symlinks are resolved
// lexically and their targets judged. Out-of-root targets are collected
// for the caller to judge against ITS boundary. The walk does not
// descend into denied directories — the directory itself is the verdict.
func walkTreeLogical(fsys fs.FS, physicalRoot, logicalRoot string) (path, reason string, externals []externalLink, err error) {
	if denied, why := DeniedPath(logicalRoot); denied {
		return logicalRoot, why, nil, nil
	}
	// In-root symlinked directories to give the depth-1 alias-context
	// check after the walk (see aliasDenies). WalkDir does not descend
	// symlinks, so the target's children are reached only under the
	// target's real name; the alias name (…/.config) must be applied to
	// those children separately.
	type aliasDir struct {
		logical   string // alias spelling of the target
		targetRel string // target path relative to fsys (slash form)
		targetAbs string // absolute physical target
	}
	var aliases []aliasDir
	err = fs.WalkDir(fsys, ".", func(p string, d fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return fmt.Errorf("policy: inspect %s: %w", filepath.Join(physicalRoot, p), walkErr)
		}
		if p == "." {
			return nil
		}
		// Judged under logicalRoot, NOT the bare walk-relative entry:
		// rules that span the root boundary (.config/gh under a walk
		// rooted at ~/.config, or an alias spelling) need the ancestor
		// segments, which WalkDir's relative paths drop.
		logical := filepath.Join(logicalRoot, p)
		physical := filepath.Join(physicalRoot, p)
		if denied, why := DeniedPath(logical); denied {
			// One carve-out: a real .claude DIRECTORY met mid-walk is
			// judged by its children — only the enumerated hooks/
			// skills/settings surface inside it is denied, and commands
			// etc. must stay readable. (DeniedPath refuses a terminal
			// .claude because a direct whole-directory target includes
			// the control surface; here the walk visits every child
			// individually, so descending IS the finer judgment.)
			if !d.IsDir() || !strings.EqualFold(gopath.Base(p), ".claude") {
				path, reason = physical, why
				return fs.SkipAll
			}
		}
		if d.Type()&fs.ModeSymlink != 0 {
			rl, ok := fsys.(fs.ReadLinkFS)
			if !ok {
				path, reason = physical, "symlinked descendant on a filesystem that cannot resolve links — refused unresolved (§7)"
				return fs.SkipAll
			}
			target, err := rl.ReadLink(p)
			if err != nil {
				path, reason = physical, "unreadable symlinked descendant — refused unresolved (§7)"
				return fs.SkipAll
			}
			absTarget := filepath.Clean(target)
			if !filepath.IsAbs(target) {
				absTarget = filepath.Clean(filepath.Join(physicalRoot, gopath.Dir(p), target))
			}
			if denied, why := DeniedPath(absTarget); denied {
				path, reason = physical, why
				return fs.SkipAll
			}
			if within(physicalRoot, absTarget) {
				// In-root alias: the target subtree is walked directly
				// under its real name, so its files are covered — but not
				// under the alias name, which may carry a context rule the
				// real path lacks. Queue a depth-1 child check under the
				// alias spelling. rel == "." (a link to the root itself)
				// still needs the check, so it is not skipped.
				rel, relErr := filepath.Rel(physicalRoot, absTarget)
				if relErr != nil {
					return fmt.Errorf("policy: relativize symlink target %s: %w", absTarget, relErr)
				}
				aliases = append(aliases, aliasDir{logical: logical, targetRel: filepath.ToSlash(rel), targetAbs: absTarget})
			} else {
				externals = append(externals, externalLink{
					source:        physical,
					sourceLogical: logical,
					target:        absTarget,
				})
			}
		}
		return nil
	})
	if err != nil {
		return "", "", nil, err
	}
	if path != "" {
		return path, reason, externals, nil
	}
	// Depth-1 alias-context check. The only context-spanning deny rules
	// (.config/{gh,gcloud}, .claude/{hooks,skills,settings}) fire on a
	// parent segment plus ONE child, so judging each direct child of an
	// aliased directory under the alias spelling is exactly sufficient —
	// and needs no recursion, so no symlink cycle can diverge here.
	for _, a := range aliases {
		p, r, err := aliasDenies(fsys, a.targetRel, a.targetAbs, a.logical)
		if err != nil || p != "" {
			return p, r, externals, err
		}
	}
	return path, reason, externals, nil
}

// rootSpellings resolves the absolute read root component by component,
// following symlinks anywhere in the path — not just the final
// component — and returns every lexical spelling of the final target
// seen along the way. A denylisted segment introduced by a symlink at
// ANY position (safe/gh where safe -> .config; or safe -> .config ->
// config-target) appears in one of these spellings even though
// EvalSymlinks collapses it away. Resolution is bounded by a hop cap;
// EvalSymlinks has already rejected a cyclic root before this runs.
func rootSpellings(root string) ([]string, error) {
	vol := filepath.VolumeName(root)
	rest := strings.Split(root[len(vol):], string(filepath.Separator))
	resolved := vol + string(filepath.Separator)
	spellings := []string{filepath.Clean(root)}
	hops := 0
	for len(rest) > 0 {
		comp := rest[0]
		rest = rest[1:]
		switch comp {
		case "", ".":
			continue
		case "..":
			resolved = filepath.Dir(resolved)
			continue
		}
		next := filepath.Join(resolved, comp)
		info, err := os.Lstat(next)
		if err != nil {
			return nil, fmt.Errorf("policy: inspect read root component %s: %w", next, err)
		}
		if info.Mode()&os.ModeSymlink == 0 {
			resolved = next
			continue
		}
		hops++
		if hops > 255 {
			return nil, fmt.Errorf("policy: read root %s exceeds the symlink-resolution limit", root)
		}
		target, err := os.Readlink(next)
		if err != nil {
			return nil, fmt.Errorf("policy: read root symlink %s: %w", next, err)
		}
		if filepath.IsAbs(target) {
			tvol := filepath.VolumeName(target)
			resolved = tvol + string(filepath.Separator)
			rest = append(strings.Split(target[len(tvol):], string(filepath.Separator)), rest...)
		} else {
			rest = append(strings.Split(target, string(filepath.Separator)), rest...)
		}
		// The still-unresolved remainder, joined onto the prefix, is
		// another spelling of the final path (…/.config/gh before .config
		// is expanded away).
		spellings = append(spellings, filepath.Join(append([]string{resolved}, rest...)...))
	}
	spellings = append(spellings, resolved)
	return spellings, nil
}

// aliasDenies judges the direct children of an aliased directory (at
// targetRel within fsys, absolute targetAbs) under the alias spelling
// aliasLogical, catching a denied path (…/.config/gh) reachable only
// through the link even though the target's real path is benign. A
// target that is not a directory re-spells only its own path, already
// judged at the symlink entry, so it contributes nothing here.
//
// INVARIANT: this checks children ONE level deep, which is correct only
// because every context-spanning rule in DeniedPath fires on a parent
// segment plus exactly ONE child (.config/{gh,gcloud},
// .claude/{hooks,skills,settings}). A deeper child alone is never denied
// (.config/x/gh is not a gh-config path), and any real denied segment
// deeper in the target is already caught by the ordinary walk under the
// target's real name — the alias only adds the parent segment its real
// path lacks. If a future rule ever needs parent-plus-two-or-more
// context, this one-level check silently under-blocks: extend the depth
// here (a bounded descent, still cycle-free) to match the deepest rule.
func aliasDenies(fsys fs.FS, targetRel, targetAbs, aliasLogical string) (path, reason string, err error) {
	info, err := fs.Stat(fsys, targetRel)
	if err != nil {
		return "", "", fmt.Errorf("policy: inspect symlink target %s: %w", targetAbs, err)
	}
	if !info.IsDir() {
		return "", "", nil
	}
	entries, err := fs.ReadDir(fsys, targetRel)
	if err != nil {
		return "", "", fmt.Errorf("policy: read symlink target %s: %w", targetAbs, err)
	}
	for _, e := range entries {
		// Same .claude carve-out as the ordinary walk: a real .claude
		// DIRECTORY is judged by its children (only hooks/skills/settings
		// are control surface), and those children are covered by the
		// walk of the target's real tree. Denying it wholesale here would
		// refuse a benign alias whose target merely holds .claude/commands.
		if e.IsDir() && strings.EqualFold(e.Name(), ".claude") {
			continue
		}
		if denied, why := DeniedPath(filepath.Join(aliasLogical, e.Name())); denied {
			return filepath.Join(targetAbs, e.Name()), why, nil
		}
	}
	return "", "", nil
}
