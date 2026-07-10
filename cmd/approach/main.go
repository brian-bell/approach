// Command approach is the APPROACH personal agent harness daemon and admin CLI.
package main

import (
	"os"

	"github.com/brian-bell/approach/internal/cli"
)

func main() {
	os.Exit(cli.Run(os.Args[1:], os.Stdout, os.Stderr))
}
