package engine

import (
	"context"
	"fmt"
	"os/exec"
	"path/filepath"
	"regexp"
	"syscall"
	"time"
)

// versionTimeout bounds the `--version` probe: it is a startup check
// against a local binary, and a hang here must not wedge the daemon's
// boot indefinitely.
const versionTimeout = 30 * time.Second

// semver extracts the first x.y.z token from the CLI's version output
// ("2.1.199 (Claude Code)" and similar shapes).
var semver = regexp.MustCompile(`\d+\.\d+\.\d+`)

// VerifyVersion runs `<bin> --version` and requires an EXACT match
// with the pinned version (§2): the hook lifecycle — the harness's
// enforcement and reflection substrate — is version-sensitive, so a
// silently upgraded CLI changes the ground under every enrolled hook.
// A mismatch is a startup refusal a restart cannot fix: either
// redeploy the pinned CLI or bump the pin deliberately (and re-run the
// pinned-version drills). Fail closed on every path — a binary that
// cannot report its version cannot prove its pin.
func VerifyVersion(ctx context.Context, bin, pinned string) error {
	if pinned == "" {
		return fmt.Errorf("engine: empty version pin (§2)")
	}
	if !filepath.IsAbs(bin) {
		return fmt.Errorf("engine: binary path %q is not absolute — a PATH-resolved engine is not pinned (§7)", bin)
	}
	ctx, cancel := context.WithTimeout(ctx, versionTimeout)
	defer cancel()
	// Same teardown discipline as a turn: the probe leads its own
	// process group, so a malformed or substituted binary that forks a
	// pipe-holding descendant cannot wedge daemon startup past the
	// timeout — and its output is bounded, not trusted.
	cmd := exec.CommandContext(ctx, bin, "--version")
	out := &boundedBuffer{limit: 4096}
	cmd.Stdout = out
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	cmd.Cancel = func() error {
		return termGroup(cmd.Process.Pid)
	}
	cmd.WaitDelay = 5 * time.Second
	err := cmd.Run()
	if cmd.Process != nil && ctx.Err() != nil {
		_ = syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL) // straggler sweep; ESRCH fine
	}
	if err != nil {
		return fmt.Errorf("engine: version probe %s --version: %w", bin, err)
	}
	got := semver.FindString(out.String())
	if got == "" {
		return fmt.Errorf("engine: version probe: no x.y.z token in output %q — cannot prove the pin (§2)", excerpt(out.String()))
	}
	if got != pinned {
		return fmt.Errorf("engine: version drift: binary reports %s, config pins %s — redeploy the pinned CLI or bump the pin deliberately (§2)", got, pinned)
	}
	return nil
}
