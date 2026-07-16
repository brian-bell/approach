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
	"strconv"
	"strings"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/brian-bell/approach/internal/adapter/discord"
	"github.com/brian-bell/approach/internal/admin"
	"github.com/brian-bell/approach/internal/config"
	"github.com/brian-bell/approach/internal/delivery"
	"github.com/brian-bell/approach/internal/engine"
	"github.com/brian-bell/approach/internal/router"
	"github.com/brian-bell/approach/internal/store"
)

// exitUnrecoverable is the daemon's exit status for startup refusals
// that no restart can fix (schema newer than the binary, §6): the
// systemd unit excludes it via RestartPreventExitStatus so the daemon
// stays down with one actionable journal record instead of
// restart-looping every RestartSec. Keep in sync with
// deploy/systemd/approach.service.
const exitUnrecoverable = 3

// The production adapter must satisfy the outbound surface the §4.6
// restart resend needs — a runtime type assertion alone would demote
// a signature drift from compile error to silently-skipped resend.
var _ delivery.Sender = (*discord.Adapter)(nil)

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
	// queues is assigned after the store opens; the admin socket only
	// serves after that, so the retry closure never sees it nil in a
	// living daemon — the guard is for the refusal paths.
	var queues *router.Queues
	// spendAlarmUSD is set after the config loads (same serve-after
	// ordering as db/queues): the §7 cost-alarm threshold the status
	// verb's daily-spend fields compare against.
	var spendAlarmUSD float64
	srv, err := admin.New(socket, admin.Options{
		Logger: logger,
		OnPoke: func() { pokes.Add(1) },
		// The §4.6 manual retry: interrupted → received durably, then
		// back into the thread's queue at its current tail.
		OnRetry: func(id int64) error {
			if db == nil || queues == nil {
				return fmt.Errorf("daemon still starting — store/router not ready")
			}
			ev, err := store.RequeueInterruptedEvent(context.Background(), db, id, time.Now().Unix())
			if err != nil {
				return err
			}
			queues.Readmit(ev)
			return nil
		},
		// The §4.6 manual drain: one human decision per dead letter.
		OnDeadRequeue: func(id int64) error {
			if db == nil || queues == nil {
				return fmt.Errorf("daemon still starting — store/router not ready")
			}
			ev, err := store.ResolveDeadLetterRequeue(context.Background(), db, id, time.Now().Unix())
			if err != nil {
				return err
			}
			queues.Readmit(ev)
			return nil
		},
		OnDeadDiscard: func(id int64) error {
			if db == nil {
				return fmt.Errorf("daemon still starting — store not ready")
			}
			return store.ResolveDeadLetterDiscard(context.Background(), db, id)
		},
		Status: func() map[string]any {
			fields := map[string]any{"version": version(), "pid": os.Getpid(), "pokes": pokes.Load()}
			var schema int
			if err := db.QueryRow("PRAGMA user_version").Scan(&schema); err == nil {
				fields["schema_version"] = schema
			}
			// The §7 daily-spend checklist rides the status verb —
			// C11's burn numbers, visible to a human today and to the
			// M3 heartbeat later.
			spendStatus(db, spendAlarmUSD, time.Now(), fields)
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

	// Adapter validation sits with config loading, BEFORE the store
	// opens: a refused startup must refuse before this process migrates
	// the schema or runs the identity full-sync — same rule as a bad
	// approach.toml.
	if err := validateDiscordAdapter(cfg, logger); err != nil {
		logger.Error("discord adapter", "error", err.Error())
		// A configured credential that cannot be read is a refusal a
		// restart cannot fix (deploy the file, then start) — same
		// posture as ErrSchemaTooNew.
		return exitUnrecoverable
	}

	// The §2 version pin is verified in the same pre-store phase: a
	// drifted CLI changes the hook lifecycle the harness enforces
	// through, and a restart cannot fix it — redeploy the pinned CLI or
	// bump the pin deliberately.
	if err := verifyEnginePin(cfg, logger); err != nil {
		logger.Error("engine pin", "error", err.Error())
		return exitUnrecoverable
	}
	if cfg != nil && cfg.Engine != nil {
		spendAlarmUSD = cfg.Engine.DailySpendAlarmUSD
		// An engine that can burn money with no alarm must be a
		// visible choice, never a silent default (§7 — cost as a
		// safety control).
		if spendAlarmUSD == 0 {
			logger.Warn("engine.daily_spend_alarm_usd not set — anomalous burn will not self-alarm (§7)")
		}
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

	signalCtx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	ctx, cancel := context.WithCancel(signalCtx)
	defer cancel()

	// The §4.1 router: per-thread queues over the events table, rebuilt
	// BEFORE ingest goes live — restart recovery (parking crash-
	// interrupted turns, re-indexing queued ones) must finish before new
	// traffic can interleave with it. A rebuild that cannot write its
	// parks is a startup refusal, not a degraded boot.
	// pumpKick wakes the outbox pump when a new outbound row lands
	// mid-life (a park's §4.6 notice must post now, not next restart).
	// Buffered + non-blocking: a kick during a drain coalesces.
	pumpKick := make(chan struct{}, 1)
	kickPump := func() {
		select {
		case pumpKick <- struct{}{}:
		default:
		}
	}
	queues = router.New(ctx, db, router.Options{
		Handler: placeholderTurn(logger),
		Logger:  logger,
		// Crash parks surface to the originating thread through the
		// outbox (§4.6) — write-before-send holds the notice durably
		// until the adapter is up.
		OnPark: func(ctx context.Context, ev store.QueuedEvent) error {
			if err := delivery.SurfaceInterrupted(ctx, db, ev); err != nil {
				return err
			}
			kickPump()
			return nil
		},
	})
	if err := queues.Rebuild(ctx); err != nil {
		logger.Error("rebuild event queues", "error", err.Error())
		return 1
	}

	// The adapter is built only now — its ingest handler persists through
	// the router into the store, which had to open (and migrate, and
	// seed, and rebuild) first. The credential was already proven
	// readable by validateDiscordAdapter above, so a failure here is
	// unexpected but still unrecoverable.
	runner, err := newDiscordRunner(cfg, db, queues, logger)
	if err != nil {
		logger.Error("discord adapter", "error", err.Error())
		return exitUnrecoverable
	}

	// The adapter runs supervised: Run only returns on cancellation
	// (drain/signal) or a terminal gateway refusal — retryable failures
	// stay inside its own backoff loop. On a terminal refusal the whole
	// daemon comes down with exitUnrecoverable: a restart cannot mint a
	// working credential, and a daemon that keeps serving with a dead
	// channel is quiet degradation (§8).
	adapterDone := make(chan struct{})
	var adapterErr error
	if runner != nil {
		go func() {
			defer close(adapterDone)
			if err := runner.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
				adapterErr = err // read only after <-adapterDone
				cancel()         // unwind Serve — the failure must not wait for a drain
			}
		}()
	} else {
		close(adapterDone)
	}

	// The outbox pump: one drain at start (the §4.6 restart resend —
	// rows the previous life composed but never got acked re-send from
	// their persisted payloads), then a drain per kick or ticker tick,
	// so notices composed mid-life post now instead of next restart.
	// Runs concurrent with fresh ingest — at-least-once tolerates the
	// interleaving, and per-target order is the resender's own
	// invariant. An adapter without an outbound surface must be LOUD:
	// silence would read as "nothing owed" when the outbox disagrees.
	resendDone := make(chan struct{})
	if sender, ok := runner.(delivery.Sender); ok {
		go func() {
			defer close(resendDone)
			delivery.Pump(ctx, db, map[string]delivery.Sender{"discord": sender},
				logger, time.Now, pumpKick, 30*time.Second)
		}()
	} else {
		close(resendDone)
		logger.Warn("no outbound sender — restart resend skipped; owed deliveries stay durable (§4.6)")
	}

	serveErr := srv.Serve(ctx)
	// Serve has returned (drain, signal, or adapter-triggered cancel):
	// stop the adapter and WAIT for it — the ingest path must be dead
	// before the deferred db.Close runs, or a message received during
	// shutdown races a closing store (§4.1). The router drains after
	// the adapter: Wait requires producers quiesced first, and an
	// in-flight turn must finish its writes before the store closes.
	cancel()
	<-adapterDone
	<-resendDone
	queues.Wait()
	if adapterErr != nil {
		logger.Error("discord adapter terminated", "error", adapterErr.Error())
		return exitUnrecoverable
	}
	if serveErr != nil {
		logger.Error("admin socket", "error", serveErr.Error())
		return 1
	}
	logger.Info("drained, shutting down")
	return 0
}

// spendStatus appends the §7 daily-spend checklist fields to a status
// reply: today's known burn (since LOCAL midnight — the operator's
// day, not UTC's), turn counts, and — when a threshold is configured —
// the alarm verdict. This is the C11 feed the heartbeat checklist
// reads once M3 lands; until then the status verb is where a human
// sees the burn. A failed query reports itself as spend_error rather
// than reading as $0: a made-up number is worse than none.
func spendStatus(db *sql.DB, alarmUSD float64, now time.Time, fields map[string]any) {
	midnight := time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, now.Location())
	spend, err := store.DailySpend(context.Background(), db, midnight.Unix(), now.Unix()+1)
	if err != nil {
		fields["spend_error"] = err.Error()
		return
	}
	fields["spend_today_usd"] = spend.KnownUSD
	fields["spend_today_turns"] = spend.Turns
	fields["spend_today_unknown_usage"] = spend.UnknownTurns
	if alarmUSD > 0 {
		fields["spend_alarm_usd"] = alarmUSD
		// >= not >: an alarm that stays quiet AT its threshold has a
		// dead band exactly where the operator drew the line.
		if spend.KnownUSD >= alarmUSD {
			fields["spend_alarm"] = "OVER"
		} else {
			fields["spend_alarm"] = "ok"
		}
	}
}

// adapterRunner is the daemon's view of a channel adapter: one
// blocking Run under the daemon context. An interface so lifecycle
// tests can supervise a fake without a gateway.
type adapterRunner interface {
	Run(context.Context) error
}

// newDiscordRunner builds the production discord adapter around the
// write-on-receipt ingest handler (§4.1). A package-level seam so the
// daemon lifecycle tests can inject a fake runner; production code
// never reassigns it. nil-runner (with nil error) means "nothing to
// run": no config, no discord channel, or a dormant channel without a
// token_file — validateDiscordAdapter already warned about that
// loudly before the store opened.
var newDiscordRunner = func(cfg *config.Config, db *sql.DB, queues *router.Queues, logger *slog.Logger) (adapterRunner, error) {
	if cfg == nil {
		return nil, nil
	}
	ch, ok := cfg.Channels["discord"]
	if !ok || ch.TokenFile == "" {
		return nil, nil
	}
	token, err := discord.ReadToken(ch.TokenFile)
	if err != nil {
		return nil, err
	}
	a, err := discord.New(token, discordIngest(queues, db, ch.Auth, logger, time.Now), logger)
	if err != nil {
		return nil, err
	}
	return a, nil
}

// verifyEnginePin proves the deployed Claude Code binary matches the
// [engine] version pin (§2) before the store opens — same refusal
// phase as a bad credential. No [engine] section is a bootable,
// dormant posture (adapters and queues run; turn dispatch waits for
// the engine wiring) but must be loudly visible, never silent.
func verifyEnginePin(cfg *config.Config, logger *slog.Logger) error {
	if cfg == nil || cfg.Engine == nil {
		logger.Warn("no [engine] section — no CLI pinned; turn dispatch stays dormant (§2)")
		return nil
	}
	if err := engine.VerifyVersion(context.Background(), cfg.Engine.Bin, cfg.Engine.Version); err != nil {
		return err
	}
	logger.Info("engine pin verified", "bin", cfg.Engine.Bin, "version", cfg.Engine.Version,
		"enrolled_hooks", len(cfg.Engine.Hooks))
	return nil
}

// placeholderTurn is the M1 scaffold handler: the queue claims,
// serializes, stamps 'processing', and survives restarts NOW, but the
// completion transitions land with the production dispatch wiring
// (approach-x6n.11: engine + session manager + the C11 turn recorder,
// store.InsertTurn as Engine.RecordTurn) — so a scaffold-dispatched
// row stays 'processing' and the NEXT restart parks it as interrupted
// (§4.6), where the delivery flows will surface it. Honest on
// purpose: a no-op turn did consume the event, and faking 'completed'
// would hide that no engine ever ran.
func placeholderTurn(logger *slog.Logger) router.Handler {
	return func(_ context.Context, ev store.QueuedEvent) {
		logger.Debug("turn dispatch not yet wired — event stays durably queued (approach-x6n.11)",
			"thread_key", ev.ThreadKey, "dedup_key", ev.DedupKey)
	}
}

// validateDiscordAdapter proves the C1 Discord adapter COULD start —
// credential readable, adapter constructible — without opening the
// gateway. The real adapter starts later (newDiscordRunner), after the
// store opens: its handler persists events, and the gateway must never
// be live before the insert path is (§4.1 — a connection whose handler
// cannot persist would consume messages the gateway won't redeliver).
// This early check exists because a refused startup must refuse BEFORE
// this process migrates the schema or runs the identity full-sync.
//
// Absent config or channel → nothing to validate. A discord channel
// without token_file → valid but LOUD: the channel may exist only to
// enroll identities before the credential is deployed, but a dormant
// channel must never look like a running one. A token_file that does
// not yield a working adapter is an error — explicit config must work
// (§6 posture, same rule as --config).
func validateDiscordAdapter(cfg *config.Config, logger *slog.Logger) error {
	if cfg == nil {
		return nil
	}
	ch, ok := cfg.Channels["discord"]
	if !ok {
		return nil
	}
	if ch.TokenFile == "" {
		logger.Warn("channels.discord has no token_file — adapter not started; the channel is enrolled but unreachable")
		return nil
	}
	token, err := discord.ReadToken(ch.TokenFile)
	if err != nil {
		return err
	}
	if _, err := discord.New(token, discord.PlaceholderHandler(logger), logger); err != nil {
		return err
	}
	logger.Info("discord adapter validated — gateway starts once the state store is open")
	return nil
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
// runRetry is `approach retry <event-id>`: the §4.6 manual re-queue
// of an interrupted event, exactly the invocation the park notice
// advertises. The id is validated BEFORE the socket is touched — a
// garbled id is a usage error, not a request.
func runRetry(args []string, stdout, stderr io.Writer) int {
	if len(args) == 0 {
		fmt.Fprintln(stderr, "approach retry: missing event id")
		return 2
	}
	id, err := strconv.ParseInt(args[0], 10, 64)
	if err != nil || id <= 0 {
		fmt.Fprintf(stderr, "approach retry: %q is not a positive event id\n", args[0])
		return 2
	}
	return runAdminVerb("retry", fmt.Sprintf("retry %d", id), args[1:], stdout, stderr)
}

// runDead is `approach dead <requeue|discard> <event-id>` — the §4.6
// manual drain, one decision per row. Same validate-before-socket
// contract as retry.
func runDead(args []string, stdout, stderr io.Writer) int {
	if len(args) < 2 {
		fmt.Fprintln(stderr, "approach dead: want 'requeue <event-id>' or 'discard <event-id>'")
		return 2
	}
	var verb string
	switch args[0] {
	case "requeue":
		verb = "dead-requeue"
	case "discard":
		verb = "dead-discard"
	default:
		fmt.Fprintf(stderr, "approach dead: unknown action %q — want requeue or discard\n", args[0])
		return 2
	}
	id, err := strconv.ParseInt(args[1], 10, 64)
	if err != nil || id <= 0 {
		fmt.Fprintf(stderr, "approach dead %s: %q is not a positive event id\n", args[0], args[1])
		return 2
	}
	return runAdminVerb(verb, fmt.Sprintf("%s %d", verb, id), args[2:], stdout, stderr)
}

func runAdminVerb(verb, request string, args []string, stdout, stderr io.Writer) int {
	flags := flag.NewFlagSet(verb, flag.ContinueOnError)
	flags.SetOutput(stderr)
	socket := flags.String("socket", filepath.Join(defaultStateDir(), "approach.sock"), "path to the daemon admin socket")
	if err := flags.Parse(args); err != nil {
		return 2
	}
	if err := rejectLeftovers(verb, flags, stderr); err != nil {
		return 2
	}

	reply, err := admin.Request(*socket, request)
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
