package cli

import (
	"context"
	"database/sql"
	"errors"
	"flag"
	"fmt"
	"io"
	"io/fs"
	"log/slog"
	"os"
	"os/signal"
	"path/filepath"
	"runtime/debug"
	"strings"
	"sync/atomic"
	"syscall"

	"github.com/brian-bell/approach/internal/admin"
	"github.com/brian-bell/approach/internal/config"
	"github.com/brian-bell/approach/internal/store"
)

// exitUnrecoverable is the daemon's exit status for startup refusals
// that no restart can fix (schema newer than the binary, §6): the
// systemd unit excludes it via RestartPreventExitStatus so the daemon
// stays down with one actionable journal record instead of
// restart-looping every RestartSec. Keep in sync with
// deploy/systemd/approach.service.
const exitUnrecoverable = 3

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
func runDaemon(args []string, stdout, stderr io.Writer) (code int) {
	flags := flag.NewFlagSet("daemon", flag.ContinueOnError)
	flags.SetOutput(stderr)
	state := flags.String("state", defaultStateDir(), "state directory (holds approach.db and approach.sock)")
	configPath := flags.String("config", "", "path to approach.toml (default: approach.toml beside the state directory)")
	if err := flags.Parse(args); err != nil {
		return 2
	}
	if err := rejectLeftovers("daemon", flags, stderr); err != nil {
		return 2
	}
	// stderr is the systemd journal (§8): every daemon lifecycle event
	// and error is a structured record there. stdout keeps the one
	// plain readiness line launchers wait for.
	logger := slog.New(slog.NewJSONHandler(stderr, nil))
	// The daemon must never die silently: a panic escaping the main
	// loop is logged with its stack before the nonzero exit hands
	// restarting over to systemd (Restart=on-failure).
	defer func() {
		if p := recover(); p != nil {
			logger.Error("daemon panic", "panic", fmt.Sprint(p), "stack", string(debug.Stack()))
			code = 1
		}
	}()

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
		Logger: logger,
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
			logger.Info("ready", "socket", socket)
		},
	})
	if err != nil {
		logger.Error("startup failed", "error", err.Error())
		return 1
	}
	defer func() {
		if err := srv.Close(); err != nil {
			logger.Error("release daemon lock", "error", err.Error())
		}
	}()

	// Config loads under the daemon lock but BEFORE the store opens: a
	// bad approach.toml must be refused before this process migrates the
	// schema. The file is security-load-bearing, so an explicit --config
	// fails loud on any problem; only the DEFAULTED path may be absent —
	// zero enrolled identities is a bootable, deny-by-default posture
	// (§6), but it must be loudly logged, never silent.
	cfg, err := loadDaemonConfig(*configPath, *state, logger)
	if err != nil {
		logger.Error("load config", "error", err.Error())
		return 1
	}

	db, err = store.Open(filepath.Join(*state, "approach.db"))
	if err != nil {
		logger.Error("open state store", "error", err.Error())
		if errors.Is(err, store.ErrSchemaTooNew) {
			return exitUnrecoverable
		}
		return 1
	}
	logger.Info("state store open", "state", *state)
	defer func() {
		if err := db.Close(); err != nil {
			logger.Error("close state store", "error", err.Error())
		}
	}()

	if err := seedIdentities(db, cfg, logger); err != nil {
		logger.Error("seed identities", "error", err.Error())
		return 1
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	if err := srv.Serve(ctx); err != nil {
		logger.Error("admin socket", "error", err.Error())
		return 1
	}
	logger.Info("drained, shutting down")
	return 0
}

// loadDaemonConfig loads approach.toml for the daemon. An explicit path
// must load cleanly; the defaulted path (approach.toml beside the state
// directory — the APPROACH_HOME layout, §6) may be absent, which reads
// as zero enrolled identities and is warned about, not hidden.
func loadDaemonConfig(path, state string, logger *slog.Logger) (*config.Config, error) {
	explicit := path != ""
	if !explicit {
		// Clean first: Dir on a trailing-slash spelling ("…/state/")
		// returns the state dir itself, and since absence at the
		// defaulted path is tolerated, that misderivation would silently
		// boot with zero identities instead of the enrolled set.
		path = filepath.Join(filepath.Dir(filepath.Clean(state)), "approach.toml")
	}
	cfg, err := config.Load(path)
	if err != nil {
		if !explicit && errors.Is(err, fs.ErrNotExist) {
			logger.Warn("no approach.toml — zero identities enrolled; every sender is untrusted (§6)", "path", path)
			return nil, nil
		}
		return nil, err
	}
	logger.Info("config loaded", "path", path)
	return cfg, nil
}

// seedIdentities syncs the identities table to the config (§6). A nil
// config (defaulted, absent approach.toml) syncs to empty: untrusted is
// the absence of a row, so an unconfigured daemon trusts nobody.
func seedIdentities(db *sql.DB, cfg *config.Config, logger *slog.Logger) error {
	var ids []store.Identity
	if cfg != nil {
		ids = make([]store.Identity, len(cfg.Identities))
		for i, id := range cfg.Identities {
			ids[i] = store.Identity{
				Channel:  id.Channel,
				NativeID: id.NativeID,
				Trust:    id.Trust,
				OwnerID:  id.OwnerID,
				Label:    id.Label,
			}
		}
	}
	if err := store.SeedIdentities(context.Background(), db, ids); err != nil {
		return err
	}
	logger.Info("identities seeded", "count", len(ids))
	return nil
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
