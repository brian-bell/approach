package systemd_test

import (
	"os"
	"os/exec"
	"strings"
	"testing"
)

const smokeScript = "smoke-kill-switch.sh"

// TestSmokeScriptExecutable: the kill-switch smoke test ships as an
// executable script next to the units (§7). It needs a real systemd
// to RUN, but its existence and shape are enforced everywhere.
func TestSmokeScriptExecutable(t *testing.T) {
	info, err := os.Stat(smokeScript)
	if err != nil {
		t.Fatalf("smoke script: %v", err)
	}
	if info.Mode().Perm()&0o111 == 0 {
		t.Errorf("%s mode = %04o, want executable", smokeScript, info.Mode().Perm())
	}
}

// TestSmokeScriptSyntax: bash -n parses the script without running it,
// so a quoting typo fails the build's tests, not the panic drill.
func TestSmokeScriptSyntax(t *testing.T) {
	out, err := exec.Command("bash", "-n", smokeScript).CombinedOutput()
	if err != nil {
		t.Fatalf("bash -n %s: %v\n%s", smokeScript, err, out)
	}
}

// TestSmokeScriptInvariants: the script must exercise the documented
// panic semantics — persistent disable, not a plain stop — verify the
// daemon is actually unreachable, and bring everything back up.
func TestSmokeScriptInvariants(t *testing.T) {
	raw, err := os.ReadFile(smokeScript)
	if err != nil {
		t.Fatalf("read smoke script: %v", err)
	}
	script := string(raw)
	for _, want := range []string{
		"set -euo pipefail",             // fail loud
		"disable --now approach.target", // THE panic command (§7)
		"enable --now approach.target",  // resume path
		"status",                        // daemon probed through the admin socket
		"approach.sock",                 // socket file asserted gone
	} {
		if !strings.Contains(script, want) {
			t.Errorf("%s does not contain %q", smokeScript, want)
		}
	}
}

// TestReadmeDocumentsKillSwitch: the doc and the mechanism must not
// drift apart — the README names the panic command and the smoke test.
func TestReadmeDocumentsKillSwitch(t *testing.T) {
	raw, err := os.ReadFile("README.md")
	if err != nil {
		t.Fatalf("read README: %v", err)
	}
	for _, want := range []string{
		"disable --now approach.target",
		smokeScript,
	} {
		if !strings.Contains(string(raw), want) {
			t.Errorf("README.md does not mention %q", want)
		}
	}
}
