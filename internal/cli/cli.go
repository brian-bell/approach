// Package cli dispatches the approach binary's subcommands.
package cli

import (
	"fmt"
	"io"
	"runtime/debug"

	"github.com/brian-bell/approach/internal/config"
)

const usage = `usage: approach <command>

commands:
  daemon [--state <dir>] [--config <path>]
                             run the daemon (admin socket + state store)
  poke [--socket <path>]     wake a running daemon
  status [--socket <path>]   report a running daemon's status
  drain [--socket <path>]    gracefully stop a running daemon
  retry <event-id> [--socket <path>]
                             re-queue an interrupted event (§4.6)
  config check <path>        validate an approach.toml file
  version                    print the approach version
`

// Run executes the subcommand named in args and returns the process exit code.
func Run(args []string, stdout, stderr io.Writer) int {
	if len(args) == 0 {
		fmt.Fprint(stderr, usage)
		return 2
	}

	switch args[0] {
	case "version":
		fmt.Fprintf(stdout, "approach %s\n", version())
		return 0
	case "config":
		return runConfig(args[1:], stdout, stderr)
	case "daemon":
		return runDaemon(args[1:], stdout, stderr)
	case "poke", "status", "drain":
		return runAdminVerb(args[0], args[0], args[1:], stdout, stderr)
	case "retry":
		return runRetry(args[1:], stdout, stderr)
	default:
		fmt.Fprintf(stderr, "approach: unknown command %q\n%s", args[0], usage)
		return 2
	}
}

func runConfig(args []string, stdout, stderr io.Writer) int {
	if len(args) != 2 || args[0] != "check" {
		fmt.Fprint(stderr, usage)
		return 2
	}
	if _, err := config.Load(args[1]); err != nil {
		fmt.Fprintf(stderr, "%v\n", err)
		return 1
	}
	fmt.Fprintln(stdout, "config OK")
	return 0
}

func version() string {
	info, ok := debug.ReadBuildInfo()
	if !ok || info.Main.Version == "" {
		return "(unknown)"
	}
	return info.Main.Version
}
