// Package cli dispatches the approach binary's subcommands.
package cli

import (
	"fmt"
	"io"
	"runtime/debug"
)

const usage = `usage: approach <command>

commands:
  version    print the approach version
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
	default:
		fmt.Fprintf(stderr, "approach: unknown command %q\n%s", args[0], usage)
		return 2
	}
}

func version() string {
	info, ok := debug.ReadBuildInfo()
	if !ok || info.Main.Version == "" {
		return "(unknown)"
	}
	return info.Main.Version
}
