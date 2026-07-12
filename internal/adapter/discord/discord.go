// Package discord is the C1 Discord adapter's gateway connection layer
// (§3, §8): a thin, owned connector living inside the daemon — never a
// separate bridge process (§10). This layer owns connect, reconnect
// backoff, and the DM + thread subscription; every inbound message is
// handed raw to an injected handler. Normalization into the §6 event
// contract lives beside it (Normalize); trust stamping and persistence
// belong to the handler the daemon injects — this package deliberately
// imports nothing from store or trust.
package discord

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/bwmarrin/discordgo"
	"github.com/gorilla/websocket"
)

// MessageHandler receives every inbound message the subscription
// delivers, raw. The connection layer filters only the bot's own
// messages (echo-loop foreclosure); sender policy belongs downstream.
type MessageHandler func(*discordgo.MessageCreate)

// intents is the pinned, minimal gateway subscription (§7 posture):
// DMs, guild messages (thread messages arrive as ordinary MessageCreate
// events on the thread's channel id under this intent), message content
// (privileged — must be enabled on the bot in the dev portal, or the
// gateway closes 4014, which Run treats as terminal), and guild
// metadata (Guilds carries the channel/thread objects the state cache
// needs to CLASSIFY a channel — without it every guild message is
// unclassifiable and the thread boundary below fails closed into
// dropping all of them). Widening this set is surface creep and must
// fail the pin test deliberately.
const intents = discordgo.IntentsDirectMessages |
	discordgo.IntentsGuildMessages |
	discordgo.IntentsMessageContent |
	discordgo.IntentsGuilds

// The adapter owns the WHOLE reconnect story: discordgo's session-level
// auto-reconnect is disabled (ShouldReconnectOnError=false, pinned by a
// test) because its internal loop retries every failure forever without
// classification — a token revoked mid-stream would be hammered against
// the gateway indefinitely behind a daemon that still looks healthy.
// Instead, a Disconnect event re-enters Run's loop: close the dead
// session, reopen through the same classified backoff below, so
// terminal refusals (4004/4013/4014) surface on REconnect exactly as
// they do on first connect.
const (
	backoffBase = time.Second
	backoffCap  = time.Minute

	// healthyReset is how long a connection must survive before the
	// backoff counter resets. Success alone is not health: a link that
	// completes the handshake and drops instantly would otherwise
	// reconnect at full speed forever — and Discord rate-limits
	// IDENTIFY, so a zero-delay connect/drop cycle is a ban risk, not
	// just noise.
	healthyReset = time.Minute
)

// Adapter is one Discord gateway connection. Construct with New; drive
// with Run; deliver outbound with Send.
type Adapter struct {
	// session is written by adopt (Run's goroutine, on session
	// rebuild) and read by Send's caller goroutine — always through
	// sessionGate. Run's own reads stay direct: they are the same
	// goroutine that writes.
	session     *discordgo.Session
	sessionGate sync.Mutex
	handle      MessageHandler
	log         *slog.Logger

	// dmChannels caches user id → DM channel id for the outbound path:
	// the §6 dm thread key holds the USER (the durable conversation
	// identity), and re-resolving the platform channel per message
	// would spend a REST round-trip under Discord's rate limits.
	dmGate     sync.Mutex
	dmChannels map[string]string

	// disconnected carries the gateway-drop signal from the Disconnect
	// handler (registered once, for the adapter's whole lifetime — see
	// New) into Run's loop. Buffered so a drop that fires before Run
	// reaches its select is held, not lost; a duplicate for the SAME
	// drop (discordgo's listen() and heartbeat() paths can each call
	// Close(), and CloseWithCode emits unconditionally) just coalesces
	// into the one pending wakeup. Run does not trust this signal
	// blindly: see the ErrWSAlreadyOpen handling in Run, which is what
	// actually protects against a stale or duplicate wakeup — discordgo
	// dispatches every Disconnect to every currently-registered handler
	// (see handle() in the library), so there is no way to scope a
	// handler itself to a single connection generation.
	disconnected chan struct{}

	// Handler join (§4.1): discordgo's Close stops the listener but
	// never joins it, so a message decoded before the close can still
	// be mid-handler when Run's loop exits — and the daemon closes the
	// store right after Run returns. Every dispatch registers here
	// under the gate; Run flips draining and waits before returning,
	// so no handler can outlive Run and race a closing store.
	handlerGate sync.Mutex
	handlers    sync.WaitGroup
	draining    bool

	// Seams for the lifecycle tests: production values are the
	// discordgo session's own methods, the session rebuild below, a
	// context-aware sleep, and the wall clock.
	open    func() error
	closeFn func() error
	reset   func() error
	sleep   func(context.Context, time.Duration) error
	now     func() time.Time
	// fetchChannel is the one REST fallback the boundary allows: a
	// private channel the state cache cannot classify (the gateway's
	// CHANNEL_CREATE-before-first-DM behavior is documented but not
	// guaranteed forever) is fetched once and cached into state. It
	// takes the DISPATCHING session, not the adapter's current one,
	// and is set once in New, never in adopt: the handler goroutine
	// reads it while Run's goroutine rebuilds sessions (resetSession),
	// and discordgo's close path does not join handlers — a write on
	// rebuild would be a data race against a straggling handler.
	fetchChannel func(s *discordgo.Session, id string) (*discordgo.Channel, error)
	// sendMessage / createDMChannel / editMessage / typing are the
	// outbound REST seams — same write-once rule as fetchChannel
	// (Send and Relay run on caller goroutines), session passed per
	// call.
	sendMessage     func(ctx context.Context, s *discordgo.Session, channelID, content string) (*discordgo.Message, error)
	createDMChannel func(ctx context.Context, s *discordgo.Session, userID string) (*discordgo.Channel, error)
	editMessage     func(ctx context.Context, s *discordgo.Session, channelID, messageID, content string) (*discordgo.Message, error)
	typing          func(ctx context.Context, s *discordgo.Session, channelID string) error
}

// New builds the adapter around a discordgo session. The token is used
// once to construct the session and never stored, logged, or echoed
// into an error (§7). A blank token or nil handler is refused loudly:
// both are misconfigurations a retry cannot fix.
func New(token string, handle MessageHandler, logger *slog.Logger) (*Adapter, error) {
	if strings.TrimSpace(token) == "" {
		return nil, errors.New("discord: empty bot token")
	}
	if handle == nil {
		return nil, errors.New("discord: nil message handler")
	}
	if logger == nil {
		return nil, errors.New("discord: nil logger — the adapter's connection state must be observable")
	}
	session, err := discordgo.New("Bot " + token)
	if err != nil {
		// discordgo.New never inspects the token today, but if that
		// changes the error must not carry it out of this package.
		return nil, errors.New("discord: session construction failed")
	}
	a := &Adapter{
		handle:       handle,
		log:          logger,
		disconnected: make(chan struct{}, 1),
		sleep:        sleepCtx,
		now:          time.Now,
		fetchChannel: func(s *discordgo.Session, id string) (*discordgo.Channel, error) {
			return s.Channel(id)
		},
		sendMessage: func(ctx context.Context, s *discordgo.Session, channelID, content string) (*discordgo.Message, error) {
			return s.ChannelMessageSend(channelID, content, discordgo.WithContext(ctx))
		},
		createDMChannel: func(ctx context.Context, s *discordgo.Session, userID string) (*discordgo.Channel, error) {
			return s.UserChannelCreate(userID, discordgo.WithContext(ctx))
		},
		editMessage: func(ctx context.Context, s *discordgo.Session, channelID, messageID, content string) (*discordgo.Message, error) {
			return s.ChannelMessageEdit(channelID, messageID, content, discordgo.WithContext(ctx))
		},
		typing: func(ctx context.Context, s *discordgo.Session, channelID string) error {
			return s.ChannelTyping(channelID, discordgo.WithContext(ctx))
		},
		dmChannels: make(map[string]string),
	}
	a.reset = a.resetSession
	a.adopt(session)
	return a, nil
}

// adopt wires a session to the adapter — shared by New and resetSession
// so a rebuilt session can never silently lose a pinned posture setting
// or a handler.
func (a *Adapter) adopt(session *discordgo.Session) {
	session.Identify.Intents = intents
	// The adapter owns reconnect (see the constant block above); the
	// library loop would bypass terminal classification.
	session.ShouldReconnectOnError = false
	// Synchronous dispatch: discordgo otherwise runs each handler in
	// its own goroutine, and two messages received in order could
	// persist out of order once the handler writes events — the §4.1
	// receive-order FIFO must not depend on downstream re-serializing.
	session.SyncEvents = true
	session.AddHandler(a.onMessageCreate)
	session.AddHandler(func(*discordgo.Session, *discordgo.Connect) {
		a.log.Info("discord gateway connected")
	})
	session.AddHandler(func(*discordgo.Session, *discordgo.Disconnect) {
		select {
		case a.disconnected <- struct{}{}:
		default: // a wakeup is already pending
		}
	})
	a.sessionGate.Lock()
	a.session = session
	a.sessionGate.Unlock()
	a.open = session.Open
	a.closeFn = session.Close
}

// resetSession replaces the session with a brand new one so the next
// Open sends a fresh Identify. It exists because discordgo never clears
// sessionID/sequence — not on Close, not on error — and Open resumes
// whenever they're set: after a 4007/4009 the old session's resume
// state is permanently poisoned and rebuilding is the only exported way
// to shed it. The token is read back from the session it constructed —
// still never stored on the adapter (§7).
func (a *Adapter) resetSession() error {
	session, err := discordgo.New(a.session.Token)
	if err != nil {
		// Same containment as New: no error text that could carry the
		// token out of this package.
		return errors.New("discord: session reconstruction failed")
	}
	a.adopt(session)
	return nil
}

// Run opens the gateway and holds it until ctx is cancelled (drain or
// the §3 kill switch), reconnecting through the same loop on mid-stream
// drops. A retryable failure backs off exponentially (counter reset
// once a connect succeeds — a link that flapped an hour ago must not
// inherit a maxed-out delay); a terminal one (bad credential, refused
// intents) returns immediately, on first connect and reconnect alike —
// the daemon must surface it, not hammer the gateway with a token that
// can never work.
func (a *Adapter) Run(ctx context.Context) error {
	// On every exit: join in-flight handlers before returning. The
	// caller closes the store the moment Run returns (§4.1), and
	// discordgo's Close does not wait for a dispatch already past the
	// socket. After the join, stragglers are dropped at the gate — an
	// unsupervised handler must never touch a closing store.
	defer func() {
		a.handlerGate.Lock()
		a.draining = true
		a.handlerGate.Unlock()
		a.handlers.Wait()
	}()
	attempt := 0
	needOpen := true
	var connectedAt time.Time
	for {
		if needOpen {
			// This is reached either before ever connecting, or right
			// after consuming a disconnect signal we have not yet
			// verified with Open — and that signal can be a stale
			// duplicate chasing a connection that's still genuinely
			// live (see the ErrWSAlreadyOpen handling below). a.close()
			// is always safe to call here: discordgo's Close is a
			// no-op when the socket is already down, so this can only
			// ever help — it's what keeps a stale-duplicate-caused
			// cancellation from leaking a live connection past the
			// kill switch instead of leaving it unsupervised.
			if err := ctx.Err(); err != nil {
				a.close()
				return err
			}
			err, cancelled := a.openCancellable(ctx)
			if cancelled {
				return ctx.Err()
			}
			if err != nil {
				if errors.Is(err, discordgo.ErrWSAlreadyOpen) {
					// The wakeup that got us here was a stale or
					// duplicate Disconnect: discordgo's listen() and
					// heartbeat() paths can each call Close() for the
					// same drop, and the second can arrive arbitrarily
					// late — even after we'd already believed the
					// connection gone and come back around to reopen
					// it. Open's own already-open check is ground
					// truth: the session was never actually torn down.
					// Resume watching the connection that's already
					// live; this isn't a failure, so no backoff and no
					// attempt-counter churn.
					a.log.Warn("discord gateway already open — a stale duplicate Disconnect was chased; resuming supervision of the live connection")
					needOpen = false
					continue
				}
				if terminal(err) {
					return fmt.Errorf("discord: gateway refused the connection — credential or intents problem, a restart cannot fix this: %w", err)
				}
				if staleSession(err) {
					// Retryable, but never with this session: shed the
					// poisoned resume state so the retry identifies
					// fresh instead of re-sending the refused resume.
					if rerr := a.reset(); rerr != nil {
						return fmt.Errorf("discord: replacing a session the gateway refused to resume: %w", rerr)
					}
					a.log.Warn("discord gateway refused to resume — session discarded, next attempt identifies fresh")
				}
				delay := backoffDelay(attempt)
				attempt++
				a.log.Warn("discord gateway open failed — retrying", "error", err.Error(), "delay", delay.String(), "attempt", attempt)
				if err := a.sleep(ctx, delay); err != nil {
					return err
				}
				continue
			}
			connectedAt = a.now()
			a.log.Info("discord gateway open")
		}
		select {
		case <-ctx.Done():
			a.close()
			return ctx.Err()
		case <-a.disconnected:
		}
		needOpen = true
		// Deliberately NO close here: the only emitter of the
		// Disconnect event is discordgo's CloseWithCode, so this
		// signal MEANS the session is already fully torn down
		// (listen() closes before reconnect-dispatch) — UNLESS it was
		// stale, which the ErrWSAlreadyOpen check above catches on the
		// next iteration. Closing here too would emit a fresh
		// Disconnect, and that extra buffered signal would close the
		// next healthy connection — an endless churn loop after the
		// first real drop (pinned by TestRunDisconnectDoesNotChurn).
		if a.now().Sub(connectedAt) >= healthyReset {
			attempt = 0
			a.log.Warn("discord gateway disconnected — reconnecting")
			continue
		}
		// A connection that died this fast counts as a failure epoch,
		// not a fresh start: back off before re-dialing.
		delay := backoffDelay(attempt)
		attempt++
		a.log.Warn("discord gateway dropped shortly after connect — backing off before reconnecting",
			"delay", delay.String(), "attempt", attempt)
		if err := a.sleep(ctx, delay); err != nil {
			// The disconnect just backed off from could be a stale
			// duplicate chasing a still-live connection (see the
			// ErrWSAlreadyOpen handling above) — this cancellation
			// lands before an Open attempt could tell us either way.
			// a.close() is always safe (a no-op if already down), and
			// it's what keeps that possibility from leaking a live
			// connection past the kill switch.
			a.close()
			return err
		}
	}
}

// openCancellable runs Open without letting it hold up a drain:
// discordgo's Open blocks in the gateway handshake with no context
// hook, so it runs in a goroutine selected against ctx. On cancel the
// call is abandoned to a reaper that closes a late-succeeding handshake
// — a connection nobody is supervising must not stay live consuming
// messages. A nil error means Open just established a brand new
// connection nobody's watching; ErrWSAlreadyOpen means the connection
// was already live before this call (chasing a stale disconnect, see
// Run) and is just as unsupervised now that Run is exiting — both must
// be reaped, so the check is not a plain success/failure branch.
func (a *Adapter) openCancellable(ctx context.Context) (err error, cancelled bool) {
	done := make(chan error, 1)
	go func() { done <- a.open() }()
	select {
	case err := <-done:
		return err, false
	case <-ctx.Done():
		go func() {
			if err := <-done; err == nil || errors.Is(err, discordgo.ErrWSAlreadyOpen) {
				a.close()
			}
		}()
		return nil, true
	}
}

// close logs rather than propagates: by the time it runs the adapter is
// reconnecting or shutting down, and a close error must not mask why.
func (a *Adapter) close() {
	if err := a.closeFn(); err != nil {
		a.log.Error("discord gateway close", "error", err.Error())
	}
}

// onMessageCreate enforces the subscription boundary: 1:1 DMs and
// threads only (§3 C1). The guild-messages intent necessarily delivers
// every visible guild channel, so plain guild channels are dropped
// here — and a channel that cannot be classified reads as NOT inside
// the boundary (fail closed, §7: an unclassifiable channel must not
// widen the trust surface). Group DMs are dropped by the same rule
// (see isDirectMessage). The bot's own messages are dropped too —
// relaying our replies back in as inbound events is an echo loop.
// Everything else passes through raw, other bots and authorless edge
// cases included: whether they become events is the normalizer's
// policy call, not the connection layer's.
func (a *Adapter) onMessageCreate(s *discordgo.Session, m *discordgo.MessageCreate) {
	if m.Author != nil && s.State != nil && s.State.User != nil && m.Author.ID == s.State.User.ID {
		return
	}
	if m.GuildID != "" && !a.isThread(s, m.ChannelID) {
		return
	}
	if m.GuildID == "" && !a.isDirectMessage(s, m.ChannelID) {
		return
	}
	// Register under the gate so Run's drain-join sees this dispatch:
	// once draining is set no new handler may start — Run has (or is
	// about to have) returned, and the store behind the handler is
	// closing. The drop is loud: the gateway will not redeliver.
	a.handlerGate.Lock()
	if a.draining {
		a.handlerGate.Unlock()
		a.log.Warn("discord message dropped — adapter draining, the store behind the handler is closing",
			"channel_id", m.ChannelID, "message_id", m.ID)
		return
	}
	a.handlers.Add(1)
	a.handlerGate.Unlock()
	defer a.handlers.Done()
	a.handle(m)
}

// isDirectMessage classifies a GuildID=="" channel as a 1:1 DM. Group
// DMs share the empty GuildID but are multi-party: keyed as
// discord:dm:<author> they would impersonate the author's PRIVATE
// thread and every reply would leak to the other participants (§4.3),
// so anything but ChannelTypeDM is dropped. The state cache normally
// answers (the gateway sends CHANNEL_CREATE before a session's first
// DM message); a miss falls back to ONE REST fetch, cached into state
// so a live DM costs the round-trip once. A fetch that fails drops
// the message — fail closed (§7) — and loudly, because a systemic
// fetch failure here means DMs are silently dead.
func (a *Adapter) isDirectMessage(s *discordgo.Session, channelID string) bool {
	if s.State == nil {
		return false
	}
	ch, err := s.State.Channel(channelID)
	if err != nil {
		ch, err = a.fetchChannel(s, channelID)
		if err != nil {
			a.log.Warn("dropping private message from unclassifiable channel — classification fetch failed",
				"channel_id", channelID, "error", err.Error())
			return false
		}
		if err := s.State.ChannelAdd(ch); err != nil {
			a.log.Debug("could not cache classified private channel", "channel_id", channelID, "error", err.Error())
		}
	}
	if ch.Type != discordgo.ChannelTypeDM {
		a.log.Debug("dropping message from multi-party private channel — outside the DM+thread boundary (§3 C1)",
			"channel_id", channelID)
		return false
	}
	return true
}

// isThread classifies a guild channel via the state cache (populated
// under the Guilds intent). Unknown is false: fail closed.
func (a *Adapter) isThread(s *discordgo.Session, channelID string) bool {
	if s.State == nil {
		return false
	}
	ch, err := s.State.Channel(channelID)
	if err != nil {
		a.log.Debug("dropping guild message from unclassifiable channel", "channel_id", channelID)
		return false
	}
	if !ch.IsThread() {
		return false
	}
	return true
}

// terminal reports whether a gateway error can never be fixed by
// retrying: an auth rejection (4004 / REST 401 — bad or revoked token)
// or an intents refusal (4013 invalid, 4014 disallowed — the
// privileged message-content intent not enabled for the bot), plus
// 4012 (invalid API version — a discordgo/pin mismatch). Retrying any
// of these hammers the gateway with a request it already refused and
// invites a ban; everything else is presumed transient.
func terminal(err error) bool {
	if errors.Is(err, discordgo.ErrUnauthorized) {
		return true
	}
	var closeErr *websocket.CloseError
	if errors.As(err, &closeErr) {
		switch closeErr.Code {
		case 4004, 4012, 4013, 4014:
			return true
		}
	}
	return false
}

// staleSession reports whether the gateway refused to RESUME this
// session: 4007 (invalid seq) and 4009 (session timed out) are
// retryable, but only with a NEW session — the gateway has already
// discarded this one. Retrying as-is would loop forever: discordgo's
// Open resumes whenever sessionID/sequence are set and nothing ever
// clears them, so the same dead resume would be re-sent until a daemon
// restart. Mid-stream drops with these codes converge here too: the
// Disconnect event carries no code, but the blind resume that follows
// is answered with the same close during Open's handshake, where it
// surfaces as this error.
func staleSession(err error) bool {
	var closeErr *websocket.CloseError
	if errors.As(err, &closeErr) {
		switch closeErr.Code {
		case 4007, 4009:
			return true
		}
	}
	return false
}

// backoffDelay is the outer connect loop's curve: base·2^attempt,
// capped, plus up to 25% jitter so a fleet of restarts (or one daemon
// behind a flapping link) does not reconnect in lockstep.
func backoffDelay(attempt int) time.Duration {
	d := backoffCap
	// Guard the shift: past 2^6 the uncapped value already exceeds the
	// cap, and shifting by ~63+ would overflow into the negative.
	if attempt < 6 {
		d = min(backoffBase<<attempt, backoffCap)
	}
	return d + rand.N(d/4+1)
}

// sleepCtx sleeps for d unless ctx is cancelled first — drain must
// never wait out a backoff (§3).
func sleepCtx(ctx context.Context, d time.Duration) error {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

// PlaceholderHandler records receipt at debug level and drops the
// message. Production ingest is Normalize + the daemon's store-backed
// handler; this stands in where a handler is structurally required
// but must never persist — the daemon's pre-store adapter validation.
// Content is never logged: message text is externally-authored data,
// and the journal is not the event store.
func PlaceholderHandler(logger *slog.Logger) MessageHandler {
	return func(m *discordgo.MessageCreate) {
		logger.Debug("discord message received — dropped, normalizer lands in x6n.1.2",
			"channel_id", m.ChannelID, "message_id", m.ID)
	}
}

// ReadToken reads the bot credential from the file named by
// channels.discord.token_file (a plain path so systemd LoadCredential
// can supply it) and trims surrounding whitespace — a trailing newline
// from echo or an editor is not part of the secret. Errors name the
// path, never any file content.
func ReadToken(path string) (string, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return "", fmt.Errorf("discord: read token file: %w", err)
	}
	token := strings.TrimSpace(string(data))
	if token == "" {
		return "", fmt.Errorf("discord: token file %s is empty", path)
	}
	return token, nil
}
