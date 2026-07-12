package discord

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/bwmarrin/discordgo"
	"github.com/gorilla/websocket"
)

// canaryToken stands in for a real credential in tests that assert the
// token never leaks into errors or logs.
const canaryToken = "CANARY-SECRET-TOKEN-DO-NOT-LOG"

func discardLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(os.NewFile(0, os.DevNull), nil))
}

func newTestAdapter(t *testing.T) *Adapter {
	t.Helper()
	a, err := New(canaryToken, func(*discordgo.MessageCreate) {}, discardLogger())
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return a
}

// TestNewRequiresTokenAndHandler: an adapter without a credential or a
// message sink is a misconfiguration, refused loudly at construction —
// and the refusal must never echo the credential back (§7).
func TestNewRequiresTokenAndHandler(t *testing.T) {
	for _, token := range []string{"", "   ", "\n"} {
		if _, err := New(token, func(*discordgo.MessageCreate) {}, discardLogger()); err == nil {
			t.Errorf("New(%q) accepted a blank token, want error", token)
		}
	}
	if _, err := New(canaryToken, nil, discardLogger()); err == nil {
		t.Error("New accepted a nil handler, want error")
	}
}

// TestNewPinsMinimalIntents: the gateway subscription is exactly DMs +
// guild messages (threads ride this intent) + message content — the
// §7 minimal-surface posture as a regression test: a widened intent
// set must be a deliberate, reviewed change.
func TestNewPinsMinimalIntents(t *testing.T) {
	a := newTestAdapter(t)
	want := discordgo.IntentsDirectMessages | discordgo.IntentsGuildMessages |
		discordgo.IntentsMessageContent | discordgo.IntentsGuilds
	if got := a.session.Identify.Intents; got != want {
		t.Errorf("Identify.Intents = %b, want exactly %b (DM + guild messages + message content + guild metadata for thread classification)", got, want)
	}
}

// TestNewDisablesLibraryReconnect: discordgo's own reconnect loop
// retries EVERY failure forever — including a 4004 on a token revoked
// mid-stream — without ever consulting the adapter's terminal
// classification. The adapter owns the whole reconnect story, so the
// library loop must stay off; this pin is what keeps a revoked
// credential from being hammered against the gateway indefinitely
// behind a daemon that still looks healthy.
func TestNewDisablesLibraryReconnect(t *testing.T) {
	a := newTestAdapter(t)
	if a.session.ShouldReconnectOnError {
		t.Error("ShouldReconnectOnError = true — library reconnect bypasses terminal-error classification (4004 retried forever)")
	}
}

// TestNewPinsSynchronousDispatch: discordgo runs each handler in its
// own goroutine unless SyncEvents is set — which would let two
// messages received in order insert out of order once the handler
// persists events (§4.1 receive-order FIFO). Dispatch is pinned
// synchronous at the connection layer so ordering never depends on
// the handler remembering to re-serialize.
func TestNewPinsSynchronousDispatch(t *testing.T) {
	a := newTestAdapter(t)
	if !a.session.SyncEvents {
		t.Error("SyncEvents = false — per-goroutine dispatch reorders inbound messages (§4.1 FIFO)")
	}
}

// TestRunOpensAndClosesOnCancel: the happy-path lifecycle — one open,
// block until the daemon cancels (drain/kill switch), one close.
func TestRunOpensAndClosesOnCancel(t *testing.T) {
	a := newTestAdapter(t)
	var opens, closes int
	a.open = func() error { opens++; return nil }
	a.closeFn = func() error { closes++; return nil }

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- a.Run(ctx) }()
	// Cancel once Run is blocked post-open; a short settle is enough
	// because open is synchronous inside Run.
	time.Sleep(10 * time.Millisecond)
	cancel()

	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Errorf("Run returned %v, want context.Canceled", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not return after cancel")
	}
	if opens != 1 || closes != 1 {
		t.Errorf("opens=%d closes=%d, want exactly one of each", opens, closes)
	}
}

// TestRunRetriesWithBackoff: a retryable open failure loops with
// exponentially growing, jittered, capped delays — never a tight loop
// against a down gateway, never giving up on what a retry can fix.
func TestRunRetriesWithBackoff(t *testing.T) {
	a := newTestAdapter(t)
	var opens int
	a.open = func() error {
		opens++
		if opens <= 4 {
			return fmt.Errorf("dial gateway: %w", &net.OpError{Op: "dial", Err: errors.New("connection refused")})
		}
		return nil
	}
	a.closeFn = func() error { return nil }
	var delays []time.Duration
	a.sleep = func(_ context.Context, d time.Duration) error {
		delays = append(delays, d)
		return nil
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- a.Run(ctx) }()
	time.Sleep(10 * time.Millisecond)
	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not return")
	}

	if opens != 5 {
		t.Fatalf("open attempts = %d, want 5 (4 failures then success)", opens)
	}
	if len(delays) != 4 {
		t.Fatalf("recorded %d delays, want 4", len(delays))
	}
	for i, d := range delays {
		base := backoffBase << i
		lo, hi := base, base+base/4
		if d < lo || d > hi {
			t.Errorf("delay[%d] = %v, want in [%v, %v] (exponential with <=25%% jitter)", i, d, lo, hi)
		}
	}
}

// TestBackoffDelayCaps: the exponential curve flattens at the cap (plus
// jitter headroom) no matter how many failures accumulate.
func TestBackoffDelayCaps(t *testing.T) {
	for _, attempt := range []int{10, 20, 63, 100} {
		d := backoffDelay(attempt)
		if max := backoffCap + backoffCap/4; d > max {
			t.Errorf("backoffDelay(%d) = %v, want <= %v (cap + jitter)", attempt, d, max)
		}
		if d < backoffCap {
			t.Errorf("backoffDelay(%d) = %v, want >= cap %v", attempt, d, backoffCap)
		}
	}
}

// emitOnClose mimics discordgo's Session.Close, which UNCONDITIONALLY
// emits a Disconnect event (wsapi.go CloseWithCode) — the fidelity the
// churn regression below depends on.
func emitOnClose(a *Adapter, closes *int) func() error {
	return func() error {
		*closes++
		select {
		case a.disconnected <- struct{}{}:
		default:
		}
		return nil
	}
}

// TestRunReconnectsAfterDisconnect: a mid-stream drop re-enters the
// adapter's OWN loop — reopen (the session is already closed: discordgo
// emits Disconnect only from Close) and reset the backoff counter once
// healthy (a link that flapped an hour ago must not inherit a
// maxed-out delay).
func TestRunReconnectsAfterDisconnect(t *testing.T) {
	a := newTestAdapter(t)
	var opens, closes int
	var delays []time.Duration
	opened := make(chan struct{}, 8)
	a.open = func() error {
		opens++
		// Second connection epoch: one retryable failure before
		// success, so the recorded delay proves the counter reset.
		if opens == 2 {
			return errors.New("gateway still settling")
		}
		opened <- struct{}{}
		return nil
	}
	a.closeFn = emitOnClose(a, &closes)
	a.sleep = func(_ context.Context, d time.Duration) error {
		delays = append(delays, d)
		return nil
	}

	// The clock advances well past healthyReset between calls, so the
	// dropped connection counts as long-lived and the counter resets.
	fakeNow := time.Unix(1700000000, 0)
	a.now = func() time.Time {
		fakeNow = fakeNow.Add(2 * healthyReset)
		return fakeNow
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- a.Run(ctx) }()

	<-opened                     // first connect up
	a.disconnected <- struct{}{} // mid-stream drop (signal comes from discordgo's own Close)
	<-opened                     // reconnected (after one retryable failure)
	cancel()

	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Errorf("Run returned %v, want context.Canceled", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not return after cancel")
	}
	if opens != 3 {
		t.Errorf("opens = %d, want 3 (connect, failed reopen, reopen)", opens)
	}
	if closes != 1 {
		t.Errorf("closes = %d, want 1 (final close on cancel ONLY — the dropped session was already closed by discordgo)", closes)
	}
	if len(delays) != 1 {
		t.Fatalf("recorded %d backoff delays, want 1", len(delays))
	}
	if lo, hi := backoffBase, backoffBase+backoffBase/4; delays[0] < lo || delays[0] > hi {
		t.Errorf("post-reconnect delay = %v, want in [%v, %v] — the counter must reset after a healthy connect", delays[0], lo, hi)
	}
}

// TestRunDisconnectDoesNotChurn: the self-poisoning regression —
// discordgo's Close unconditionally emits Disconnect, so if the
// reconnect path itself called Close, every reconnect would enqueue a
// stale signal that closes the NEXT healthy connection: an endless
// churn loop after the first real drop. With a faithful fake (close
// emits, like the real library), one drop must produce exactly one
// reconnect and then hold steady.
func TestRunDisconnectDoesNotChurn(t *testing.T) {
	a := newTestAdapter(t)
	var opens, closes int
	opened := make(chan struct{}, 8)
	a.open = func() error { opens++; opened <- struct{}{}; return nil }
	a.closeFn = emitOnClose(a, &closes)
	a.sleep = func(context.Context, time.Duration) error { return nil }

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- a.Run(ctx) }()

	<-opened                     // first connect
	a.disconnected <- struct{}{} // one real drop
	<-opened                     // one reconnect

	// Hold steady: any further open within the settle window is churn.
	select {
	case <-opened:
		t.Fatal("adapter reconnected again with no drop — stale Disconnect signal is poisoning healthy connections")
	case <-time.After(100 * time.Millisecond):
	}
	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not return after cancel")
	}
	if opens != 2 {
		t.Errorf("opens = %d, want 2 (connect + one reconnect)", opens)
	}
	if closes != 1 {
		t.Errorf("closes = %d, want 1 (final close only)", closes)
	}
}

// TestRunResumesAfterStaleDuplicateChasesLiveConnection: a single socket
// loss can emit Disconnect twice — discordgo's listen() and heartbeat()
// paths can each call Close(), and CloseWithCode emits unconditionally.
// discordgo dispatches every Disconnect to whichever handler is
// currently registered, so a duplicate that arrives late (after the
// reconnect it belongs to already completed) is indistinguishable, at
// the handler, from a genuine drop of the connection now live: Run has
// no way to tell them apart by inspecting the signal alone. It must
// instead recognize its mistake when Open reports the socket is already
// open — self-correcting by resuming supervision, not by churning the
// healthy connection or hammering an already-open session forever.
func TestRunResumesAfterStaleDuplicateChasesLiveConnection(t *testing.T) {
	a := newTestAdapter(t)
	var opens, closes int
	opened := make(chan struct{}, 8)
	refused := make(chan struct{})
	a.open = func() error {
		opens++
		if opens == 3 {
			// The chased reopen of the still-live connection: discordgo
			// itself refuses, since nothing ever actually closed it.
			close(refused)
			return discordgo.ErrWSAlreadyOpen
		}
		opened <- struct{}{}
		return nil
	}
	a.closeFn = func() error { closes++; return nil }
	a.sleep = func(context.Context, time.Duration) error {
		t.Error("slept while chasing a stale duplicate — an already-open refusal is not a failure")
		return nil
	}
	// Long-healthy connections: both reconnects are immediate (no flap
	// backoff), isolating the already-open self-correction.
	fakeNow := time.Unix(1700000000, 0)
	a.now = func() time.Time {
		fakeNow = fakeNow.Add(2 * healthyReset)
		return fakeNow
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- a.Run(ctx) }()

	<-opened                     // first connect
	a.disconnected <- struct{}{} // one real drop
	<-opened                     // reconnect (now live)

	// The stale duplicate for the drop that led to the reconnect fires
	// late. Run believes the live connection just dropped and tries to
	// reopen it — discordgo refuses (opens == 3, above) since it never
	// actually closed.
	a.disconnected <- struct{}{}
	<-refused // wait for Run to actually chase and get refused

	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not return after cancel")
	}
	if opens != 3 {
		t.Errorf("opens = %d, want 3 (connect, reconnect, chased-and-refused reopen)", opens)
	}
	if closes != 1 {
		t.Errorf("closes = %d, want 1 (final close only — no close on the chased reopen)", closes)
	}
}

// TestRunTerminalErrorOnReconnect: terminal classification applies to
// REconnects too — a token revoked mid-stream surfaces on the next
// open instead of being retried forever (the reason library reconnect
// is disabled).
func TestRunTerminalErrorOnReconnect(t *testing.T) {
	a := newTestAdapter(t)
	var opens int
	opened := make(chan struct{}, 1)
	a.open = func() error {
		opens++
		if opens == 1 {
			opened <- struct{}{}
			return nil
		}
		return &websocket.CloseError{Code: 4004, Text: "Authentication failed."}
	}
	var closes int
	a.closeFn = func() error { closes++; return nil }
	a.sleep = func(context.Context, time.Duration) error {
		t.Error("slept on a terminal reconnect error")
		return nil
	}
	// Long-healthy connection: the reconnect is immediate (no flap
	// backoff), so the terminal refusal is what surfaces.
	fakeNow := time.Unix(1700000000, 0)
	a.now = func() time.Time {
		fakeNow = fakeNow.Add(2 * healthyReset)
		return fakeNow
	}

	done := make(chan error, 1)
	go func() { done <- a.Run(context.Background()) }()
	<-opened
	a.disconnected <- struct{}{} // drop of the live connection

	select {
	case err := <-done:
		if err == nil {
			t.Fatal("Run returned nil after a terminal reconnect refusal")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not surface the terminal reconnect error")
	}
	if opens != 2 {
		t.Errorf("opens = %d, want 2 (no retry after 4004)", opens)
	}
}

// TestRunDiscardsSessionAfterResumeRefused: close codes 4007 (invalid
// seq) and 4009 (session timed out) mean the gateway refused to resume
// THIS session — discordgo keeps sessionID/sequence forever, so Open
// would retry the same dead resume until the daemon restarts. Those
// codes must discard the session (fresh Identify) before the retry;
// every other retryable failure must NOT, or a transient blip would
// throw away a perfectly resumable session.
func TestRunDiscardsSessionAfterResumeRefused(t *testing.T) {
	cases := []struct {
		name      string
		err       error
		wantReset bool
	}{
		{"4007 invalid seq", &websocket.CloseError{Code: 4007, Text: "Invalid seq."}, true},
		{"4009 session timed out", &websocket.CloseError{Code: 4009, Text: "Session timed out."}, true},
		{"4000 unknown error keeps resume state", &websocket.CloseError{Code: 4000, Text: "Unknown error."}, false},
		{"dial failure keeps resume state", errors.New("dial tcp: connection refused"), false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			a := newTestAdapter(t)
			var opens, resets int
			resetBeforeRetry := false
			a.open = func() error {
				opens++
				if opens == 1 {
					return tc.err
				}
				resetBeforeRetry = resets == 1
				return nil
			}
			a.closeFn = func() error { return nil }
			a.reset = func() error { resets++; return nil }
			a.sleep = func(context.Context, time.Duration) error { return nil }

			ctx, cancel := context.WithCancel(context.Background())
			done := make(chan error, 1)
			go func() { done <- a.Run(ctx) }()
			time.Sleep(10 * time.Millisecond)
			cancel()
			select {
			case <-done:
			case <-time.After(2 * time.Second):
				t.Fatal("Run did not return")
			}

			if tc.wantReset && resets != 1 {
				t.Errorf("resets = %d, want 1 (session must be discarded after %v)", resets, tc.err)
			}
			if tc.wantReset && resets == 1 && !resetBeforeRetry {
				t.Error("session was reset after the retry opened, not before — the retry still resumed the dead session")
			}
			if !tc.wantReset && resets != 0 {
				t.Errorf("resets = %d, want 0 — a resumable failure must not discard resume state", resets)
			}
		})
	}
}

// TestResetSessionRebuildsWithPins: the production reset must yield a
// genuinely new discordgo session (fresh resume state is the whole
// point) that carries the same credential and the same pinned posture —
// intents, library reconnect off, synchronous dispatch — as New built.
func TestResetSessionRebuildsWithPins(t *testing.T) {
	a := newTestAdapter(t)
	old := a.session
	if err := a.resetSession(); err != nil {
		t.Fatalf("resetSession: %v", err)
	}
	if a.session == old {
		t.Fatal("resetSession kept the old session — resume state survives")
	}
	if a.session.Token != old.Token {
		t.Error("resetSession changed the credential")
	}
	if a.session.Identify.Intents != old.Identify.Intents {
		t.Errorf("Identify.Intents = %b, want %b (pins must survive a session rebuild)", a.session.Identify.Intents, old.Identify.Intents)
	}
	if a.session.ShouldReconnectOnError {
		t.Error("ShouldReconnectOnError = true after rebuild — library reconnect pin lost")
	}
	if !a.session.SyncEvents {
		t.Error("SyncEvents = false after rebuild — synchronous dispatch pin lost")
	}
}

// TestRunFlappingConnectionBacksOff: a connection that drops
// immediately after every successful handshake must NOT reconnect at
// full speed — Discord rate-limits IDENTIFY, and a zero-delay
// connect/drop cycle hammers it. The backoff counter resets only after
// a connection stays healthy for healthyReset; short-lived successes
// keep accumulating delay.
func TestRunFlappingConnectionBacksOff(t *testing.T) {
	a := newTestAdapter(t)
	opened := make(chan struct{}, 8)
	a.open = func() error { opened <- struct{}{}; return nil }
	var closes int
	a.closeFn = emitOnClose(a, &closes)
	a.now = func() time.Time { return time.Unix(1700000000, 0) } // frozen: every connection is short-lived
	var delays []time.Duration
	slept := make(chan struct{}, 8)
	a.sleep = func(_ context.Context, d time.Duration) error {
		delays = append(delays, d)
		slept <- struct{}{}
		return nil
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- a.Run(ctx) }()

	for i := 0; i < 3; i++ {
		<-opened
		a.disconnected <- struct{}{} // instant drop
		<-slept                      // each short-lived cycle must back off
	}
	<-opened
	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not return after cancel")
	}

	if len(delays) != 3 {
		t.Fatalf("recorded %d delays, want 3 (one per flap)", len(delays))
	}
	for i, d := range delays {
		base := backoffBase << i
		lo, hi := base, base+base/4
		if d < lo || d > hi {
			t.Errorf("flap delay[%d] = %v, want in [%v, %v] — short-lived connections must accumulate backoff", i, d, lo, hi)
		}
	}
}

// TestRunCancelDuringOpenReturnsPromptly: drain must not wait for a
// stuck gateway handshake — discordgo's Open ignores our context, so
// Run selects around it and returns on cancel; a handshake that later
// succeeds anyway is reaped (closed), never left as a live leaked
// connection consuming messages.
func TestRunCancelDuringOpenReturnsPromptly(t *testing.T) {
	a := newTestAdapter(t)
	release := make(chan struct{})
	closed := make(chan struct{}, 1)
	a.open = func() error { <-release; return nil }
	a.closeFn = func() error { closed <- struct{}{}; return nil }

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- a.Run(ctx) }()
	time.Sleep(10 * time.Millisecond) // let Run enter open
	cancel()

	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Errorf("Run returned %v, want context.Canceled", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Run stayed blocked in a hung gateway handshake after cancel — drain is defeated")
	}

	// The handshake completes late: the reaper must close it.
	close(release)
	select {
	case <-closed:
	case <-time.After(2 * time.Second):
		t.Fatal("late-succeeding open was never closed — leaked live gateway connection")
	}
}

// TestRunCancelDuringStaleReopenClosesAlreadyOpenConnection: cancellation
// can win openCancellable's select at the exact moment a chased,
// still-live connection's Open call is about to report
// ErrWSAlreadyOpen. That error means a connection was live all along,
// not that nothing exists — the reaper must close it just as it would a
// late-succeeding brand new Open, or the live gateway survives drain
// unsupervised.
func TestRunCancelDuringStaleReopenClosesAlreadyOpenConnection(t *testing.T) {
	a := newTestAdapter(t)
	release := make(chan struct{})
	closed := make(chan struct{}, 1)
	a.open = func() error { <-release; return discordgo.ErrWSAlreadyOpen }
	a.closeFn = func() error { closed <- struct{}{}; return nil }

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- a.Run(ctx) }()
	time.Sleep(10 * time.Millisecond) // let Run enter open
	cancel()

	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Errorf("Run returned %v, want context.Canceled", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Run stayed blocked in a hung gateway handshake after cancel — drain is defeated")
	}

	// The chased Open call reports already-open late.
	close(release)
	select {
	case <-closed:
	case <-time.After(2 * time.Second):
		t.Fatal("a connection reported already-open after cancel was never closed — leaked live gateway connection")
	}
}

// TestRunCancelDuringBackoffAborts: drain must not wait out a backoff
// sleep (§3 kill switch) — a cancelled context aborts the retry loop
// immediately, with no further open attempts.
func TestRunCancelDuringBackoffAborts(t *testing.T) {
	a := newTestAdapter(t)
	var opens int
	ctx, cancel := context.WithCancel(context.Background())
	a.open = func() error {
		opens++
		return errors.New("gateway down")
	}
	a.sleep = func(ctx context.Context, _ time.Duration) error {
		cancel() // cancellation lands mid-sleep
		return ctx.Err()
	}

	err := a.Run(ctx)
	if !errors.Is(err, context.Canceled) {
		t.Errorf("Run returned %v, want context.Canceled", err)
	}
	if opens != 1 {
		t.Errorf("open attempts = %d after cancel-during-backoff, want 1", opens)
	}
}

// TestRunCancelDuringQuickDropBackoffCloses: cancellation that lands
// during the quick-drop backoff (after a disconnect signal, before the
// next Open attempt) must still close the session. That disconnect
// signal could be a stale duplicate chasing a connection that's
// actually still live (see TestRunResumesAfterStaleDuplicateChasesLiveConnection);
// only an Open attempt would reveal that, and cancellation here means
// one never happens. The kill switch must not leak a live connection
// just because a stale signal happened to be in flight when it fired.
func TestRunCancelDuringQuickDropBackoffCloses(t *testing.T) {
	a := newTestAdapter(t)
	var opens, closes int
	ctx, cancel := context.WithCancel(context.Background())
	opened := make(chan struct{}, 1)
	a.open = func() error { opens++; opened <- struct{}{}; return nil }
	a.closeFn = func() error { closes++; return nil }
	a.sleep = func(ctx context.Context, _ time.Duration) error {
		cancel() // cancellation lands mid-backoff
		return ctx.Err()
	}

	done := make(chan error, 1)
	go func() { done <- a.Run(ctx) }()
	<-opened
	a.disconnected <- struct{}{} // instant drop — quick-drop backoff path

	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Errorf("Run returned %v, want context.Canceled", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not return after cancel")
	}
	if closes != 1 {
		t.Errorf("closes = %d, want 1 — cancellation mid-backoff must still close a possibly-still-live connection", closes)
	}
}

// TestRunTerminalErrorsDoNotRetry: an auth rejection or an intents
// refusal is a credential/config problem — retrying hammers the gateway
// with a bad token and can never succeed; it must surface loudly and
// immediately (fail loud, §7). Everything else keeps retrying.
func TestRunTerminalErrorsDoNotRetry(t *testing.T) {
	terminalCases := []struct {
		name string
		err  error
	}{
		{"close 4004 authentication failed", &websocket.CloseError{Code: 4004, Text: "Authentication failed."}},
		{"close 4012 invalid API version", &websocket.CloseError{Code: 4012}},
		{"close 4013 invalid intents", &websocket.CloseError{Code: 4013}},
		{"close 4014 disallowed intents", &websocket.CloseError{Code: 4014}},
		{"wrapped close error", fmt.Errorf("open: %w", &websocket.CloseError{Code: 4004})},
		{"REST 401", discordgo.ErrUnauthorized},
	}
	for _, tc := range terminalCases {
		t.Run(tc.name, func(t *testing.T) {
			a := newTestAdapter(t)
			var opens int
			a.open = func() error { opens++; return tc.err }
			a.sleep = func(context.Context, time.Duration) error {
				t.Error("slept before a terminal error surfaced — this retries a bad credential")
				return nil
			}
			err := a.Run(context.Background())
			if err == nil {
				t.Fatal("Run returned nil on a terminal gateway error")
			}
			if opens != 1 {
				t.Errorf("open attempts = %d, want 1 (terminal errors never retry)", opens)
			}
			if strings.Contains(err.Error(), canaryToken) {
				t.Error("terminal error leaked the token")
			}
		})
	}

	// The retryable side of the table: generic close codes and network
	// errors keep looping.
	for _, err := range []error{
		&websocket.CloseError{Code: 4000, Text: "unknown error"},
		errors.New("read tcp: connection reset by peer"),
	} {
		if terminal(err) {
			t.Errorf("terminal(%v) = true, want false — a retry can fix this", err)
		}
	}
}

// TestMessageDispatch: the subscription boundary is DMs + THREADS (§3
// C1) — the guild-messages intent necessarily delivers every visible
// guild channel, so the boundary is enforced at dispatch: plain guild
// text channels are dropped, unknown channel types are dropped (fail
// closed — an unclassifiable channel must not widen the trust surface,
// §7), and the bot's own messages are dropped (echo-loop foreclosure).
// Everyone else's DM and thread traffic passes through raw: sender
// policy is x6n.1.2/1.3 territory, bots included.
func TestMessageDispatch(t *testing.T) {
	var got []*discordgo.MessageCreate
	a, err := New(canaryToken, func(m *discordgo.MessageCreate) { got = append(got, m) }, discardLogger())
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	s := a.session
	s.State.User = &discordgo.User{ID: "bot-self"}
	if err := s.State.GuildAdd(&discordgo.Guild{ID: "guild-1"}); err != nil {
		t.Fatalf("state guild add: %v", err)
	}
	for _, ch := range []*discordgo.Channel{
		{ID: "thread-1", GuildID: "guild-1", Type: discordgo.ChannelTypeGuildPublicThread},
		{ID: "text-1", GuildID: "guild-1", Type: discordgo.ChannelTypeGuildText},
		{ID: "dm-1", Type: discordgo.ChannelTypeDM},
	} {
		if err := s.State.ChannelAdd(ch); err != nil {
			t.Fatalf("state channel add %s: %v", ch.ID, err)
		}
	}

	msg := func(author, guild, channel string) *discordgo.MessageCreate {
		return &discordgo.MessageCreate{Message: &discordgo.Message{
			Author:    &discordgo.User{ID: author},
			GuildID:   guild,
			ChannelID: channel,
		}}
	}
	a.onMessageCreate(s, msg("bot-self", "", "dm-1"))                                              // own DM echo — dropped
	a.onMessageCreate(s, msg("human", "", "dm-1"))                                                 // DM — delivered
	a.onMessageCreate(s, msg("human", "guild-1", "thread-1"))                                      // thread — delivered
	a.onMessageCreate(s, msg("human", "guild-1", "text-1"))                                        // plain guild channel — dropped
	a.onMessageCreate(s, msg("human", "guild-1", "mystery-9"))                                     // unknown channel — dropped, fail closed
	a.onMessageCreate(s, &discordgo.MessageCreate{Message: &discordgo.Message{ChannelID: "dm-1"}}) // authorless DM: normalizer judges

	if len(got) != 3 {
		t.Fatalf("handler saw %d messages, want 3 (DM + thread + authorless DM)", len(got))
	}
	if got[0].Author.ID != "human" || got[0].GuildID != "" {
		t.Errorf("first delivered message = (%v, %q), want the DM", got[0].Author, got[0].GuildID)
	}
	if got[1].ChannelID != "thread-1" {
		t.Errorf("second delivered message channel = %q, want thread-1", got[1].ChannelID)
	}
}

// TestReadToken: the credential file contract — content trimmed (a
// trailing newline from echo/systemd is not part of the secret), a
// missing file names the PATH (never any secret material), and an
// empty file is a refusal, not an empty credential.
func TestReadToken(t *testing.T) {
	dir := t.TempDir()

	path := filepath.Join(dir, "token")
	if err := os.WriteFile(path, []byte(canaryToken+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	tok, err := ReadToken(path)
	if err != nil {
		t.Fatalf("ReadToken: %v", err)
	}
	if tok != canaryToken {
		t.Errorf("ReadToken = %q, want trimmed token", tok)
	}

	missing := filepath.Join(dir, "gone")
	if _, err := ReadToken(missing); err == nil {
		t.Error("ReadToken on a missing file returned nil error")
	} else if !strings.Contains(err.Error(), missing) {
		t.Errorf("missing-file error %q does not name the path", err)
	}

	empty := filepath.Join(dir, "empty")
	if err := os.WriteFile(empty, []byte("  \n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := ReadToken(empty); err == nil {
		t.Error("ReadToken on an empty file returned nil error — an empty credential must be refused")
	}
}
