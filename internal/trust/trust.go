// Package trust models the §6 participant trust levels and the rules
// computed over them: the ordering untrusted < known < owner, and the
// session trust_floor — the least-trusted party a thread can admit.
// Confidentiality (§4.3) and the policy matrix (§7) key off these
// values, so the set is closed and every unknown reads as untrusted.
package trust

import "fmt"

// Level is a participant trust level. The zero value is not a level;
// construct via the constants or Parse.
type Level string

// The closed participant set (§6). System trust exists on events —
// daemon-stamped for heartbeats and workers — but is never a
// participant level: no human at system trust can be admitted to a
// thread, so it has no place in a floor.
const (
	Untrusted Level = "untrusted" // no identities row — deny-by-default
	Known     Level = "known"
	Owner     Level = "owner"
)

// rank orders the levels for Min. Absent (unknown) levels rank 0 —
// below every real level — so junk can never rank above untrusted.
var rank = map[Level]int{Untrusted: 1, Known: 2, Owner: 3}

// normalize maps anything outside the closed set — including the zero
// value — to Untrusted, so a junk level entering a computation can only
// ever come OUT as the bottom of the order, never leak through into a
// floor (and from there into the sessions trust_floor CHECK).
func normalize(l Level) Level {
	if rank[l] == 0 {
		return Untrusted
	}
	return l
}

// Parse validates s as a participant level. The set is closed: anything
// else is an error, never coerced — a typo'd trust string silently
// reading as some level would be a policy bug.
func Parse(s string) (Level, error) {
	l := Level(s)
	if rank[l] == 0 {
		return "", fmt.Errorf("trust: unknown level %q (owner|known|untrusted)", s)
	}
	return l, nil
}

// Min returns the less-trusted of a and b. Levels outside the closed
// set read as Untrusted.
func Min(a, b Level) Level {
	a, b = normalize(a), normalize(b)
	if rank[a] < rank[b] {
		return a
	}
	return b
}

// Stamped is the §6 decision stamped onto an inbound event: the
// effective trust after the channel-auth clamp, plus the channel's
// capability bits the §7 gates consult alongside it. MayApprove is the
// CHANNEL capability, not a verdict — §4.4 approval matching separately
// requires the owner_id match, so it can be true on an untrusted stamp.
type Stamped struct {
	Trust      Level
	ReadOnly   bool
	MayApprove bool
}

// Stamp applies the §6 channel-auth clamp: effective trust is
// min(lookup, the channel's max). Config validation already rejects
// owner enrollment on weak channels, but the identities table is the
// runtime source of truth and can drift past it (manual DB edits, a
// stale seed) — so the weak-channel-never-stamps-owner invariant is
// enforced again here, at the point of stamping. The auth set is closed
// and case-sensitive, mirroring config validation and the derived
// accessors on config.Channel (MaxTrust/MayApprove/ReadOnly — keep the
// two in sync): anything outside it, including "" from a channel absent
// from [channels], fails closed to untrusted, read-only, no approval.
func Stamp(auth string, lookup Level) Stamped {
	switch auth {
	case "strong":
		return Stamped{Trust: Min(lookup, Owner), ReadOnly: false, MayApprove: true}
	case "weak":
		return Stamped{Trust: Min(lookup, Known), ReadOnly: true, MayApprove: false}
	default:
		return Stamped{Trust: Untrusted, ReadOnly: true, MayApprove: false}
	}
}

// IngestKind classifies content arriving in a session for the taint
// rule (§7 rule 3). The set is closed: new ingestion paths must decide
// their taint behavior here, explicitly.
type IngestKind int

const (
	// The zero value is deliberately NOT a valid kind: an ingestion
	// path that forgets to set its kind must fall into Taints' tainting
	// default, never masquerade as an inbound message from the owner.
	// IngestInboundMessage is an inbound prompt or attachment, stamped
	// at receipt, pre-execution.
	IngestInboundMessage IngestKind = iota + 1
	// IngestMCPResult is a tool result from an MCP server; the author
	// level is the server's configured grade (default untrusted — the
	// poisoned-email vector arrives here).
	IngestMCPResult
	// IngestWebFetch is fetched web content.
	IngestWebFetch
	// IngestCodexWebRead is web content read by the Codex critic.
	IngestCodexWebRead
	// IngestCodexCritique is the read-only critic's own analysis of
	// local content — it must not taint, or draft→critique→approve→act
	// dies (§7).
	IngestCodexCritique
	// IngestAttachment is a file attached to an inbound message. It
	// taints at EVERY author level, owner included: file content is
	// not authored by its sender in any verifiable way — a forwarded
	// PDF is web content in a trench coat — and the §6 sessions sketch
	// lists "attachment" as a taint source unconditionally.
	IngestAttachment
)

// Taints reports whether ingesting this content gains the session the
// sticky tainted flag (§6, §7): taint follows CONTENT, not the tool —
// keying on the tool alone would miss the email vector the threat model
// names. author is who authored the content (for MCP results, the
// server's configured grade); anything but Owner is externally authored.
// Unknown kinds taint — fail safe, mirroring normalize.
func Taints(kind IngestKind, author Level) bool {
	switch kind {
	case IngestInboundMessage, IngestMCPResult:
		return normalize(author) != Owner
	case IngestCodexCritique:
		return false
	default: // IngestWebFetch, IngestCodexWebRead, IngestAttachment, anything unknown
		return true
	}
}

// Floor computes a session's trust_floor (§6): the least-trusted
// participant the thread can admit. A DM's floor is that identity's
// trust — the single-participant case; a group's floor is the minimum
// over whoever can post there. An empty participant set fails safe to
// Untrusted: knowing nothing about who will read the transcript must
// never float the floor above the bottom.
func Floor(participants ...Level) Level {
	floor := Untrusted
	for i, p := range participants {
		if i == 0 {
			floor = normalize(p)
			continue
		}
		floor = Min(floor, p)
	}
	return floor
}
