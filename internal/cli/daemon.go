package cli

import (
	"context"
	"database/sql"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"sync/atomic"
	"syscall"

	"github.com/brian-bell/approach/internal/admin"
	"github.com/brian-bell/approach/internal/store"
)

// defaultStateDir is where the daemon keeps its socket and store:
// $APPROACH_HOME/state, defaulting to ~/approach/state (§6).
func defaultStateDir() string {
	if home := os.Getenv("APPROACH_HOME"); home != "" {
		return filepath.Join(home, "state")
	}
	home, err := os.UserHomeDir()
	if err != nil {
		return "state"
	}
	return filepath.Join(home, "approach", "state")
}

// runDaemon starts the state store and serves the admin socket until
// a drain request or SIGINT/SIGTERM.
func runDaemon(args []string, stdout, stderr io.Writer) int {
	flags := flag.NewFlagSet("daemon", flag.ContinueOnError)
	flags.SetOutput(stderr)
	state := flags.String("state", defaultStateDir(), "state directory (holds approach.db and approach.sock)")
	if err := flags.Parse(args); err != nil {
		return 2
	}
	if err := rejectLeftovers("daemon", flags, stderr); err != nil {
		return 2
	}

	// Daemon ownership (admin.New's lifetime lock) comes FIRST: opening
	// the store also migrates it, and a second — possibly newer —
	// binary must be refused before it can migrate the schema out from
	// under the daemon that is actually running.
	socket := filepath.Join(*state, "approach.sock")
	var db *sql.DB
	// Until M1 wires the event router, a poke has nothing to wake — but
	// it must still be observable, not silently dropped: the count in
	// status is how a timer's wake path is verified end to end.
	var pokes atomic.Int64
	srv, err := admin.New(socket, admin.Options{
		OnPoke: func() { pokes.Add(1) },
		Status: func() map[string]any {
			fields := map[string]any{"version": version(), "pid": os.Getpid(), "pokes": pokes.Load()}
			var schema int
			if err := db.QueryRow("PRAGMA user_version").Scan(&schema); err == nil {
				fields["schema_version"] = schema
			}
			return fields
		},
		// Readiness is printed only once the socket is bound — this
		// line is what launchers may wait on.
		OnReady: func() {
			fmt.Fprintf(stdout, "approach daemon listening on %s\n", socket)
		},
	})
	if err != nil {
		fmt.Fprintf(stderr, "%v\n", err)
		return 1
	}
	defer func() {
		if err := srv.Close(); err != nil {
			fmt.Fprintf(stderr, "%v\n", err)
		}
	}()

	db, err = store.Open(filepath.Join(*state, "approach.db"))
	if err != nil {
		fmt.Fprintf(stderr, "%v\n", err)
		return 1
	}
	defer func() {
		if err := db.Close(); err != nil {
			fmt.Fprintf(stderr, "%v\n", err)
		}
	}()

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	if err := srv.Serve(ctx); err != nil {
		fmt.Fprintf(stderr, "%v\n", err)
		return 1
	}
	return 0
}

// rejectLeftovers refuses positional arguments: flag parsing stops at
// the first one, so anything after it — including --socket — would be
// silently ignored and the command would act on the default target.
func rejectLeftovers(verb string, flags *flag.FlagSet, stderr io.Writer) error {
	if flags.NArg() == 0 {
		return nil
	}
	err := fmt.Errorf("approach %s: unexpected argument %q (flags after it are ignored)", verb, flags.Arg(0))
	fmt.Fprintf(stderr, "%v\n", err)
	return err
}

// runAdminVerb sends poke, status, or drain to a running daemon.
func runAdminVerb(verb string, args []string, stdout, stderr io.Writer) int {
	flags := flag.NewFlagSet(verb, flag.ContinueOnError)
	flags.SetOutput(stderr)
	socket := flags.String("socket", filepath.Join(defaultStateDir(), "approach.sock"), "path to the daemon admin socket")
	if err := flags.Parse(args); err != nil {
		return 2
	}
	if err := rejectLeftovers(verb, flags, stderr); err != nil {
		return 2
	}

	reply, err := admin.Request(*socket, verb)
	if err != nil {
		fmt.Fprintf(stderr, "%v\n", err)
		return 1
	}
	if body, ok := strings.CutPrefix(reply, "err "); ok {
		fmt.Fprintf(stderr, "approach %s: %s\n", verb, body)
		return 1
	}
	fmt.Fprintln(stdout, reply)
	return 0
}
