package engine

import (
	"errors"
	"os"
	"os/exec"
	"syscall"
	"testing"
)

// TestTermGroupTranslatesESRCH: cancellation can race a clean exit —
// the process group is already gone when Cancel fires. exec.Cmd
// records the command as FAILED if Cancel returns any error but
// os.ErrProcessDone after a successful run, so ESRCH must translate:
// a completed turn misfiled as a failure would trip the §4.6 failure
// handling for work that succeeded.
func TestTermGroupTranslatesESRCH(t *testing.T) {
	// A real, already-reaped process group: spawn /usr/bin/true as its
	// own group leader and wait it out.
	cmd := exec.Command("/usr/bin/true")
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	if err := cmd.Run(); err != nil {
		t.Fatalf("run true: %v", err)
	}
	if err := termGroup(cmd.Process.Pid); !errors.Is(err, os.ErrProcessDone) {
		t.Errorf("termGroup on a dead group = %v, want os.ErrProcessDone", err)
	}
}
