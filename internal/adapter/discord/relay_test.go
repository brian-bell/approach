package discord

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/bwmarrin/discordgo"
)

// relayFixture fakes the REST seams behind a Relay and records the
// traffic under a mutex — the typing loop runs on its own goroutine.
type relayFixture struct {
	a  *Adapter
	mu sync.Mutex

	typings int
	sends   []string // content of each fresh send
	edits   []string // content of each edit
	nextID  int
}

func newRelayFixture(t *testing.T) *relayFixture {
	t.Helper()
	f := &relayFixture{a: newTestAdapter(t), nextID: 0}
	f.a.sendMessage = func(_ context.Context, _ *discordgo.Session, _, content string) (*discordgo.Message, error) {
		f.mu.Lock()
		defer f.mu.Unlock()
		f.sends = append(f.sends, content)
		f.nextID++
		return &discordgo.Message{ID: msgID(f.nextID)}, nil
	}
	f.a.editMessage = func(_ context.Context, _ *discordgo.Session, _, _, content string) (*discordgo.Message, error) {
		f.mu.Lock()
		defer f.mu.Unlock()
		f.edits = append(f.edits, content)
		return &discordgo.Message{ID: "partial-1"}, nil
	}
	f.a.typing = func(context.Context, *discordgo.Session, string) error {
		f.mu.Lock()
		defer f.mu.Unlock()
		f.typings++
		return nil
	}
	return f
}

func msgID(n int) string {
	return "out-" + strings.Repeat("i", n) // distinct, stable ids
}

func (f *relayFixture) snapshot() (typings int, sends, edits []string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.typings, append([]string(nil), f.sends...), append([]string(nil), f.edits...)
}

// fastRelay shrinks the production intervals so tests observe the
// timing behavior in milliseconds.
func fastRelay(f *relayFixture) *Relay {
	r := f.a.NewRelay(context.Background(), "discord:thread:t1")
	r.typingInterval = 10 * time.Millisecond
	r.editInterval = 50 * time.Millisecond
	r.partialMin = 20
	return r
}

// TestRelayTypingKeepalive: a turn that has produced output but no
// visible message yet shows a heartbeat — typing repeats while the
// engine composes, and stops once a partial message replaces it.
// TestChunkMessage: the exported chunker is the one definition of how
// a reply splits across Discord's per-message cap — the outbox composer
// writes one delivery row per chunk, and Relay.Finish must send exactly
// those chunks, or acks and rows would misalign.
func TestChunkMessage(t *testing.T) {
	if got := ChunkMessage(""); got != nil {
		t.Errorf("ChunkMessage(\"\") = %v, want nil", got)
	}
	if got := ChunkMessage("hi"); len(got) != 1 || got[0] != "hi" {
		t.Errorf("ChunkMessage(short) = %v, want [hi]", got)
	}
	long := strings.Repeat("é", messageCap+1) // rune-counted, not bytes
	got := ChunkMessage(long)
	if len(got) != 2 {
		t.Fatalf("ChunkMessage(long) = %d chunks, want 2", len(got))
	}
	if utf8.RuneCountInString(got[0]) != messageCap || utf8.RuneCountInString(got[1]) != 1 {
		t.Errorf("chunk rune counts = %d,%d, want %d,1",
			utf8.RuneCountInString(got[0]), utf8.RuneCountInString(got[1]), messageCap)
	}
	if got[0]+got[1] != long {
		t.Error("chunks do not reassemble the original text")
	}
}

func TestRelayTypingKeepalive(t *testing.T) {
	f := newRelayFixture(t)
	r := fastRelay(f)
	r.Push("short") // below partialMin — no message, typing starts

	deadline := time.Now().Add(2 * time.Second)
	for {
		typings, _, _ := f.snapshot()
		if typings >= 2 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("typing never repeated while the turn stayed silent")
		}
		time.Sleep(5 * time.Millisecond)
	}

	// Crossing the partial threshold sends a message; typing stops.
	r.Push(strings.Repeat("x", 30))
	_, sends, _ := f.snapshot()
	if len(sends) != 1 {
		t.Fatalf("sends = %v, want the partial message", sends)
	}
	settled, _, _ := f.snapshot()
	time.Sleep(50 * time.Millisecond)
	after, _, _ := f.snapshot()
	if after > settled+1 { // one in-flight tick may land
		t.Errorf("typing kept firing after the partial message: %d -> %d", settled, after)
	}
	if _, err := r.Finish(); err != nil {
		t.Fatalf("Finish: %v", err)
	}
}

// TestRelayPostedTracksVisibleOutput: Posted is the §4.6 evidence bit
// — false while nothing has reached the platform (below threshold, or
// a failed partial send), true from the moment a partial message
// lands, and STICKY through channel invalidation: losing the edit
// target does not un-show what a human already saw.
func TestRelayPostedTracksVisibleOutput(t *testing.T) {
	f := newRelayFixture(t)
	r := fastRelay(f)

	if r.Posted() {
		t.Error("Posted before any push")
	}
	r.Push("tiny") // below threshold — typing only, nothing visible
	if r.Posted() {
		t.Error("Posted while below the partial threshold — nothing was shown")
	}
	r.Push(strings.Repeat("x", 30)) // crosses threshold — partial message lands
	if !r.Posted() {
		t.Error("not Posted after a partial message reached the platform")
	}
	// A dead channel clears the edit target, never the evidence bit —
	// the human already saw the text (§4.6). Locked call: the test is
	// in-package and owns the mutex contract here.
	r.mu.Lock()
	r.invalidateChannelLocked()
	r.mu.Unlock()
	if !r.Posted() {
		t.Error("Posted cleared by channel invalidation — the human still saw the text (§4.6)")
	}
	r.Cancel()
	if !r.Posted() {
		t.Error("Posted cleared by Cancel — sticky means sticky")
	}
}

// TestRelayRetractRemovesPostedPartial: Retract deletes the partial
// message the turn posted — the defer-to-pump paths call it because
// the pump re-sends the full text and a standing partial would show
// the reply twice. Posted stays sticky: deletion is not un-seeing
// (§4.6). Below the threshold there is nothing to delete.
func TestRelayRetractRemovesPostedPartial(t *testing.T) {
	f := newRelayFixture(t)
	var deleted []string
	f.a.deleteMessage = func(_ context.Context, _ *discordgo.Session, _, messageID string) error {
		f.mu.Lock()
		defer f.mu.Unlock()
		deleted = append(deleted, messageID)
		return nil
	}
	r := fastRelay(f)

	r.Push(strings.Repeat("x", 30)) // partial message lands
	r.Retract()
	f.mu.Lock()
	got := append([]string(nil), deleted...)
	f.mu.Unlock()
	if len(got) != 1 || got[0] != msgID(1) {
		t.Errorf("deleted = %v, want the posted partial %q", got, msgID(1))
	}
	if !r.Posted() {
		t.Error("Posted cleared by Retract — deletion is not un-seeing (§4.6)")
	}

	// Nothing posted → nothing deleted.
	f2 := newRelayFixture(t)
	var deleted2 int
	f2.a.deleteMessage = func(context.Context, *discordgo.Session, string, string) error {
		deleted2++
		return nil
	}
	r2 := fastRelay(f2)
	r2.Push("tiny")
	r2.Retract()
	if deleted2 != 0 {
		t.Errorf("deleted %d messages with no partial posted, want 0", deleted2)
	}
}

// TestRelayPostedFalseOnFailedPartial: a partial send the platform
// refused put nothing on screen — Posted stays false, so the §4.6
// judgment can still auto-retry a provably invisible turn.
func TestRelayPostedFalseOnFailedPartial(t *testing.T) {
	f := newRelayFixture(t)
	f.a.sendMessage = func(context.Context, *discordgo.Session, string, string) (*discordgo.Message, error) {
		return nil, fmt.Errorf("fake: refused")
	}
	r := fastRelay(f)
	r.Push(strings.Repeat("x", 30)) // crosses threshold, send fails
	if r.Posted() {
		t.Error("Posted after a refused partial send — nothing reached the platform")
	}
}

// TestRelayPartialThenThrottledEdits: content past the threshold
// becomes one partial message; further pushes edit it in place, but
// never faster than editInterval — Discord's edit bucket is small.
func TestRelayPartialThenThrottledEdits(t *testing.T) {
	f := newRelayFixture(t)
	r := fastRelay(f)

	r.Push(strings.Repeat("a", 25)) // crosses threshold -> partial send
	r.Push("b")                     // within throttle window -> no edit yet
	r.Push("c")                     // still within window

	_, sends, edits := f.snapshot()
	if len(sends) != 1 || len(edits) != 0 {
		t.Fatalf("sends=%d edits=%d, want 1 send and 0 edits inside the throttle window", len(sends), len(edits))
	}

	time.Sleep(60 * time.Millisecond) // let the window pass
	r.Push("d")
	_, _, edits = f.snapshot()
	if len(edits) != 1 {
		t.Fatalf("edits = %d, want 1 after the window passed", len(edits))
	}
	if want := strings.Repeat("a", 25) + "bcd"; edits[0] != want {
		t.Errorf("edit content = %q, want the full accumulation %q", edits[0], want)
	}
	if _, err := r.Finish(); err != nil {
		t.Fatalf("Finish: %v", err)
	}
}

// TestRelayFinishNoPartial: a short turn is one plain send — no
// typing residue, one ack.
func TestRelayFinishNoPartial(t *testing.T) {
	f := newRelayFixture(t)
	r := fastRelay(f)
	r.Push("brief")
	acks, err := r.Finish()
	if err != nil {
		t.Fatalf("Finish: %v", err)
	}
	if len(acks) != 1 || !strings.HasPrefix(acks[0], "discord:msg:") {
		t.Errorf("acks = %v, want one discord:msg ack", acks)
	}
	_, sends, edits := f.snapshot()
	if len(sends) != 1 || len(edits) != 0 {
		t.Errorf("sends=%v edits=%v, want exactly one fresh send", sends, edits)
	}
}

// TestRelayFinishEmptyTurn: nothing pushed, nothing sent — a silent
// turn is the router's decision, not a blank message.
func TestRelayFinishEmptyTurn(t *testing.T) {
	f := newRelayFixture(t)
	r := fastRelay(f)
	acks, err := r.Finish()
	if err != nil {
		t.Fatalf("Finish: %v", err)
	}
	if len(acks) != 0 {
		t.Errorf("acks = %v, want none", acks)
	}
	typings, sends, edits := f.snapshot()
	if typings+len(sends)+len(edits) != 0 {
		t.Errorf("an empty turn touched the platform: typings=%d sends=%v edits=%v", typings, sends, edits)
	}
}

// TestRelayFinishAfterPartial: the final edit carries the complete
// text, and the partial message's ack is part of the delivery record.
func TestRelayFinishAfterPartial(t *testing.T) {
	f := newRelayFixture(t)
	r := fastRelay(f)
	r.Push(strings.Repeat("a", 25))
	r.Push("tail")
	acks, err := r.Finish()
	if err != nil {
		t.Fatalf("Finish: %v", err)
	}
	_, _, edits := f.snapshot()
	if len(edits) == 0 || edits[len(edits)-1] != strings.Repeat("a", 25)+"tail" {
		t.Fatalf("final edit missing or incomplete: %v", edits)
	}
	// The ack is the partial message's own id (minted by the send that
	// created it) — the edit changes content, not identity.
	if len(acks) != 1 || acks[0] != "discord:msg:"+msgID(1) {
		t.Errorf("acks = %v, want the partial message's id", acks)
	}
}

// TestRelayFinishChunksLongText: Discord caps a message at 2000
// chars; a longer turn is split on rune boundaries, order preserved,
// every chunk's ack returned — a lost chunk must be visible to the
// outbox.
func TestRelayFinishChunksLongText(t *testing.T) {
	f := newRelayFixture(t)
	r := f.a.NewRelay(context.Background(), "discord:thread:t1")
	r.partialMin = 1 << 20            // no partials — pin pure chunking
	text := strings.Repeat("é", 4500) // 2-byte rune: byte-splitting would corrupt
	r.Push(text)
	acks, err := r.Finish()
	if err != nil {
		t.Fatalf("Finish: %v", err)
	}
	_, sends, _ := f.snapshot()
	if len(sends) != 3 {
		t.Fatalf("sends = %d, want 3 chunks of ≤2000 runes", len(sends))
	}
	if got := strings.Join(sends, ""); got != text {
		t.Error("chunks reassembled ≠ original text")
	}
	for _, s := range sends {
		if n := len([]rune(s)); n > 2000 {
			t.Errorf("chunk of %d runes exceeds the platform cap", n)
		}
		if !strings.HasPrefix(s, "é") && s != "" {
			t.Error("chunk split mid-rune")
		}
	}
	if len(acks) != 3 {
		t.Errorf("acks = %d, want 3 — every accepted chunk is delivery record", len(acks))
	}
}

// TestRelayFailedPartialStillDelivers: partial UX is best-effort; the
// FINAL message is the §4.1 durable leg. A turn whose partial send
// failed still delivers everything at Finish.
func TestRelayFailedPartialStillDelivers(t *testing.T) {
	f := newRelayFixture(t)
	var failed bool
	f.a.sendMessage = func(_ context.Context, _ *discordgo.Session, _, content string) (*discordgo.Message, error) {
		f.mu.Lock()
		defer f.mu.Unlock()
		if !failed {
			failed = true
			return nil, errors.New("rest 502")
		}
		f.sends = append(f.sends, content)
		return &discordgo.Message{ID: "final-1"}, nil
	}
	r := fastRelay(f)
	r.Push(strings.Repeat("a", 25)) // partial attempt fails
	r.Push("tail")
	acks, err := r.Finish()
	if err != nil {
		t.Fatalf("Finish after failed partial: %v", err)
	}
	_, sends, _ := f.snapshot()
	if len(sends) != 1 || sends[0] != strings.Repeat("a", 25)+"tail" {
		t.Fatalf("sends = %v, want one full fresh send", sends)
	}
	if len(acks) != 1 || acks[0] != "discord:msg:final-1" {
		t.Errorf("acks = %v, want the fresh send's ack", acks)
	}
}

// TestRelayCancel: an abandoned turn goes quiet — typing stops, no
// further sends, Finish after Cancel is a refused no-op.
func TestRelayCancel(t *testing.T) {
	f := newRelayFixture(t)
	r := fastRelay(f)
	r.Push("short")
	r.Cancel()
	typingsAtCancel, _, _ := f.snapshot()
	time.Sleep(40 * time.Millisecond)
	typingsAfter, sends, _ := f.snapshot()
	if typingsAfter > typingsAtCancel+1 { // one in-flight tick may land
		t.Errorf("typing kept firing after Cancel: %d -> %d", typingsAtCancel, typingsAfter)
	}
	if len(sends) != 0 {
		t.Errorf("Cancel still sent: %v", sends)
	}
	if acks, err := r.Finish(); err == nil || len(acks) != 0 {
		t.Errorf("Finish after Cancel = (%v, %v), want a refusal", acks, err)
	}
}

// TestRelayPartialCappedAtMessageLimit: the partial message never
// exceeds the platform's 2000-char cap — an over-cap edit would be
// rejected and kill the partial UX exactly when a long answer needs
// it. Content past the cap stops the edit stream (the shown prefix no
// longer changes) and arrives as Finish chunks.
func TestRelayPartialCappedAtMessageLimit(t *testing.T) {
	f := newRelayFixture(t)
	r := fastRelay(f)
	r.editInterval = 0 // every push may edit — pins the cap, not the throttle

	r.Push(strings.Repeat("é", 2500))
	_, sends, _ := f.snapshot()
	if len(sends) != 1 {
		t.Fatalf("sends = %d, want the partial message", len(sends))
	}
	if n := len([]rune(sends[0])); n != 2000 {
		t.Errorf("partial content = %d runes, want capped at 2000", n)
	}

	r.Push("more")
	_, _, edits := f.snapshot()
	for _, e := range edits {
		if n := len([]rune(e)); n > 2000 {
			t.Errorf("edit content = %d runes, exceeds the platform cap", n)
		}
	}

	acks, err := r.Finish()
	if err != nil {
		t.Fatalf("Finish: %v", err)
	}
	// 2504 runes total → capped partial (2000) + one overflow chunk.
	if len(acks) != 2 {
		t.Errorf("acks = %d, want 2 (partial + overflow chunk)", len(acks))
	}
}

// TestRelayFailedPartialThrottled: a failing partial send must not be
// retried on every stream delta — that is a REST storm against the
// same rate bucket the final delivery needs. Retries wait out the
// edit interval like any other partial traffic.
func TestRelayFailedPartialThrottled(t *testing.T) {
	f := newRelayFixture(t)
	attempts := 0
	f.a.sendMessage = func(context.Context, *discordgo.Session, string, string) (*discordgo.Message, error) {
		attempts++
		return nil, errors.New("rest 429")
	}
	r := fastRelay(f) // editInterval 50ms
	r.Push(strings.Repeat("a", 25))
	r.Push("b")
	r.Push("c")
	r.Push("d")
	if attempts != 1 {
		t.Errorf("partial send attempts = %d, want 1 inside the throttle window", attempts)
	}
	time.Sleep(60 * time.Millisecond)
	r.Push("e")
	if attempts != 2 {
		t.Errorf("partial send attempts = %d, want 2 after the window passed", attempts)
	}
	r.Cancel()
}

// TestRelayDMFailureInvalidatesChannelCache: Relay must share Send's
// stale-DM recovery — a failed relay send drops the cached user→
// channel mapping so the NEXT turn re-resolves, instead of every
// streaming DM wedging on a re-minted channel until restart.
func TestRelayDMFailureInvalidatesChannelCache(t *testing.T) {
	f := newRelayFixture(t)
	resolutions := 0
	f.a.createDMChannel = func(_ context.Context, _ *discordgo.Session, userID string) (*discordgo.Channel, error) {
		resolutions++
		return &discordgo.Channel{ID: "dmchan-" + userID}, nil
	}
	f.a.sendMessage = func(context.Context, *discordgo.Session, string, string) (*discordgo.Message, error) {
		return nil, errors.New("HTTP 404, {\"code\": 10003}")
	}

	r := fastRelay(f)
	r.threadKey = "discord:dm:u1"
	r.Push("brief")
	if _, err := r.Finish(); err == nil {
		t.Fatal("Finish should surface the send failure")
	}

	r2 := fastRelay(f)
	r2.threadKey = "discord:dm:u1"
	r2.Push("again")
	_, _ = r2.Finish()
	if resolutions != 2 {
		t.Errorf("DM resolutions = %d, want 2 — the failed turn must invalidate the cache", resolutions)
	}
}

// TestRelayReResolvesChannelWithinTurn: a DM failure invalidates the
// adapter's channel cache — but the relay holds its own copy, and a
// stale relay-local id would keep the SAME turn (later partials and
// the durable Finish leg) wedged on the dead channel. The failure
// must clear both, so the next attempt re-resolves.
func TestRelayReResolvesChannelWithinTurn(t *testing.T) {
	f := newRelayFixture(t)
	resolutions := 0
	f.a.createDMChannel = func(_ context.Context, _ *discordgo.Session, userID string) (*discordgo.Channel, error) {
		resolutions++
		return &discordgo.Channel{ID: fmt.Sprintf("dmchan-gen%d", resolutions)}, nil
	}
	fails := 0
	f.a.sendMessage = func(_ context.Context, _ *discordgo.Session, channelID, content string) (*discordgo.Message, error) {
		if channelID == "dmchan-gen1" {
			fails++
			return nil, errors.New("HTTP 404, {\"code\": 10003}")
		}
		f.mu.Lock()
		defer f.mu.Unlock()
		f.sends = append(f.sends, content)
		return &discordgo.Message{ID: "recovered-1"}, nil
	}

	r := fastRelay(f)
	r.threadKey = "discord:dm:u1"
	r.Push(strings.Repeat("a", 25)) // partial send into gen1 fails → invalidate BOTH caches
	acks, err := r.Finish()         // must re-resolve to gen2 and deliver
	if err != nil {
		t.Fatalf("Finish should recover via re-resolution: %v", err)
	}
	if resolutions != 2 {
		t.Errorf("resolutions = %d, want 2 — the relay-local channel must be cleared too", resolutions)
	}
	if len(acks) != 1 || acks[0] != "discord:msg:recovered-1" {
		t.Errorf("acks = %v, want the recovered send", acks)
	}
}

// TestRelayHonorsCancelledContext: NewRelay promises the context
// silences the relay — a delta arriving after drain must not reach
// the platform seams, and Finish must refuse with the cancellation.
func TestRelayHonorsCancelledContext(t *testing.T) {
	f := newRelayFixture(t)
	ctx, cancel := context.WithCancel(context.Background())
	r := f.a.NewRelay(ctx, "discord:thread:t1")
	r.partialMin = 5
	cancel()

	r.Push(strings.Repeat("x", 50)) // above threshold, but drained
	_, sends, edits := f.snapshot()
	if len(sends)+len(edits) != 0 {
		t.Errorf("a drained relay reached the platform: sends=%v edits=%v", sends, edits)
	}
	if _, err := r.Finish(); !errors.Is(err, context.Canceled) {
		t.Errorf("Finish under cancelled ctx = %v, want context.Canceled surfaced", err)
	}
}

// TestRelayFinishNamesResolutionCause: a Finish that cannot resolve
// the channel must carry the REAL cause — wrapping a nil ctx error
// renders as %!w(<nil>) and strands the outbox without a diagnosis.
func TestRelayFinishNamesResolutionCause(t *testing.T) {
	f := newRelayFixture(t)
	restErr := errors.New("rest down: 502")
	f.a.createDMChannel = func(context.Context, *discordgo.Session, string) (*discordgo.Channel, error) {
		return nil, restErr
	}
	r := fastRelay(f)
	r.threadKey = "discord:dm:u1"
	r.Push("brief")
	_, err := r.Finish()
	if !errors.Is(err, restErr) {
		t.Errorf("Finish error = %v, want the resolution cause wrapped", err)
	}
}

// TestRelayPartialThresholdCountsRunes: the partial threshold is a
// character budget, not a byte budget — 15 two-byte runes are 30
// bytes but only 15 characters, and must NOT cross a 20-character
// threshold; non-ASCII output would otherwise flicker partials early.
func TestRelayPartialThresholdCountsRunes(t *testing.T) {
	f := newRelayFixture(t)
	r := fastRelay(f) // partialMin = 20
	r.Push(strings.Repeat("é", 15))
	_, sends, _ := f.snapshot()
	if len(sends) != 0 {
		t.Fatalf("sends = %v — 15 chars crossed a 20-char threshold (byte counting)", sends)
	}
	r.Push(strings.Repeat("é", 6)) // 21 runes total
	_, sends, _ = f.snapshot()
	if len(sends) != 1 {
		t.Errorf("sends = %d, want the partial once 21 chars accumulated", len(sends))
	}
	r.Cancel()
}

// TestRelayTypingStopsOnDMInvalidation: after a DM send failure the
// channel is invalid — the keepalive goroutine, which captured that
// channel id, must stop rather than type into the dead channel until
// the turn ends.
func TestRelayTypingStopsOnDMInvalidation(t *testing.T) {
	f := newRelayFixture(t)
	f.a.sendMessage = func(context.Context, *discordgo.Session, string, string) (*discordgo.Message, error) {
		return nil, errors.New("HTTP 404, {\"code\": 10003}")
	}
	f.a.createDMChannel = func(_ context.Context, _ *discordgo.Session, userID string) (*discordgo.Channel, error) {
		return &discordgo.Channel{ID: "dmchan-" + userID}, nil
	}
	r := fastRelay(f)
	r.threadKey = "discord:dm:u1"
	r.Push("short") // below threshold — typing starts on gen1

	deadline := time.Now().Add(2 * time.Second)
	for {
		typings, _, _ := f.snapshot()
		if typings >= 1 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("typing never started")
		}
		time.Sleep(5 * time.Millisecond)
	}

	r.Push(strings.Repeat("x", 30)) // partial send fails → channel invalidated
	settled, _, _ := f.snapshot()
	time.Sleep(50 * time.Millisecond)
	after, _, _ := f.snapshot()
	if after > settled+1 { // one in-flight tick may land
		t.Errorf("typing kept firing into the invalidated channel: %d -> %d", settled, after)
	}
	r.Cancel()
}

// TestRelayFailedResolutionThrottled: below the partial threshold,
// every push wants typing — but a failing DM resolution must not
// retry UserChannelCreate once per engine delta during an outage.
// Failed resolution shares the partial-traffic throttle window.
func TestRelayFailedResolutionThrottled(t *testing.T) {
	f := newRelayFixture(t)
	attempts := 0
	f.a.createDMChannel = func(context.Context, *discordgo.Session, string) (*discordgo.Channel, error) {
		attempts++
		return nil, errors.New("rest down")
	}
	r := fastRelay(f) // editInterval 50ms
	r.threadKey = "discord:dm:u1"
	r.Push("a")
	r.Push("b")
	r.Push("c")
	if attempts != 1 {
		t.Errorf("resolution attempts = %d, want 1 inside the throttle window", attempts)
	}
	time.Sleep(60 * time.Millisecond)
	r.Push("d")
	if attempts != 2 {
		t.Errorf("resolution attempts = %d, want 2 after the window passed", attempts)
	}
	r.Cancel()
}

// TestRelayFinishSurfacesSendError: a failed final send is the
// outbox's business — acks for what landed, the error for what
// didn't.
func TestRelayFinishSurfacesSendError(t *testing.T) {
	f := newRelayFixture(t)
	restErr := errors.New("rest 502")
	f.a.sendMessage = func(context.Context, *discordgo.Session, string, string) (*discordgo.Message, error) {
		return nil, restErr
	}
	r := fastRelay(f)
	r.Push("brief")
	if _, err := r.Finish(); !errors.Is(err, restErr) {
		t.Errorf("Finish error = %v, want the REST error surfaced", err)
	}
}
