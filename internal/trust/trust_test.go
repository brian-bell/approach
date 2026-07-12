package trust_test

import (
	"testing"

	"github.com/brian-bell/approach/internal/trust"
)

// TestParse: the participant trust set is closed (§6) — owner, known,
// untrusted. Anything else is an error, never coerced: a typo'd trust
// string silently reading as some level would be a policy bug.
func TestParse(t *testing.T) {
	for _, s := range []string{"owner", "known", "untrusted"} {
		level, err := trust.Parse(s)
		if err != nil {
			t.Errorf("Parse(%q): %v", s, err)
		}
		if string(level) != s {
			t.Errorf("Parse(%q) = %q", s, level)
		}
	}
	for _, s := range []string{"", "Owner", "system", "admin"} {
		if level, err := trust.Parse(s); err == nil {
			t.Errorf("Parse(%q) = %q, want error — the participant set is closed", s, level)
		}
	}
}

// TestFloor: a session's trust_floor is the least-trusted participant
// the thread can admit (§6). A DM's floor IS that identity's trust; a
// group's floor is the minimum over whoever can post there; and knowing
// nothing about the participants must fail safe to untrusted (§4.3
// confidentiality keys off the floor).
func TestFloor(t *testing.T) {
	for _, tc := range []struct {
		name         string
		participants []trust.Level
		want         trust.Level
	}{
		{"DM with the owner", []trust.Level{trust.Owner}, trust.Owner},
		{"DM with a known person", []trust.Level{trust.Known}, trust.Known},
		{"DM with a stranger", []trust.Level{trust.Untrusted}, trust.Untrusted},
		{"group of owner surfaces", []trust.Level{trust.Owner, trust.Owner}, trust.Owner},
		{"group with a known person", []trust.Level{trust.Owner, trust.Known}, trust.Known},
		{"group with a stranger", []trust.Level{trust.Owner, trust.Known, trust.Untrusted}, trust.Untrusted},
		{"order does not matter", []trust.Level{trust.Untrusted, trust.Owner}, trust.Untrusted},
		{"no known participants fails safe", nil, trust.Untrusted},
		// Level is a string type, so a caller CAN construct junk without
		// Parse — it must come out as the bottom of the order, never leak
		// into the sessions trust_floor CHECK.
		{"junk level alone fails safe", []trust.Level{trust.Level("admin")}, trust.Untrusted},
		{"junk level in a group fails safe", []trust.Level{trust.Owner, trust.Level("admin")}, trust.Untrusted},
		{"zero value fails safe", []trust.Level{""}, trust.Untrusted},
	} {
		if got := trust.Floor(tc.participants...); got != tc.want {
			t.Errorf("%s: Floor(%v) = %q, want %q", tc.name, tc.participants, got, tc.want)
		}
	}
}

// TestMin pins the ordering untrusted < known < owner.
func TestMin(t *testing.T) {
	if got := trust.Min(trust.Owner, trust.Known); got != trust.Known {
		t.Errorf("Min(owner, known) = %q, want known", got)
	}
	if got := trust.Min(trust.Known, trust.Untrusted); got != trust.Untrusted {
		t.Errorf("Min(known, untrusted) = %q, want untrusted", got)
	}
	if got := trust.Min(trust.Untrusted, trust.Owner); got != trust.Untrusted {
		t.Errorf("Min(untrusted, owner) = %q, want untrusted", got)
	}
	if got := trust.Min(trust.Level("admin"), trust.Owner); got != trust.Untrusted {
		t.Errorf("Min(junk, owner) = %q, want untrusted — unknown levels read as the bottom", got)
	}
}

// TestTaints: taint follows content, not the tool (§7 rule 3). A
// session gains the sticky flag the moment it ingests externally-
// authored content; the read-only Codex critic must not taint, or
// draft→critique→approve→act dies.
func TestTaints(t *testing.T) {
	for _, tc := range []struct {
		name   string
		kind   trust.IngestKind
		author trust.Level
		want   bool
	}{
		{"owner prompt is the owner's own words", trust.IngestInboundMessage, trust.Owner, false},
		{"known person's prompt", trust.IngestInboundMessage, trust.Known, true},
		{"stranger's prompt", trust.IngestInboundMessage, trust.Untrusted, true},
		{"junk author fails safe", trust.IngestInboundMessage, trust.Level("admin"), true},
		{"untrusted MCP result (the poisoned email)", trust.IngestMCPResult, trust.Untrusted, true},
		{"owner-graded MCP result", trust.IngestMCPResult, trust.Owner, false},
		{"web fetch always", trust.IngestWebFetch, trust.Owner, true},
		{"codex web read always", trust.IngestCodexWebRead, trust.Owner, true},
		{"read-only codex critique never", trust.IngestCodexCritique, trust.Untrusted, false},
		// Attachments taint at EVERY author level, owner included: a
		// file's content is not authored by its sender in any
		// verifiable way — a forwarded PDF is web content in a trench
		// coat (§7; the §6 sessions sketch lists "attachment" as a
		// taint source unconditionally).
		{"owner's attachment still taints", trust.IngestAttachment, trust.Owner, true},
		{"stranger's attachment taints", trust.IngestAttachment, trust.Untrusted, true},
		// The zero value is not a kind: a path that forgot to classify
		// itself must taint, even claiming owner authorship.
		{"uninitialized kind fails safe", trust.IngestKind(0), trust.Owner, true},
	} {
		if got := trust.Taints(tc.kind, tc.author); got != tc.want {
			t.Errorf("%s: Taints(%v, %q) = %v, want %v", tc.name, tc.kind, tc.author, got, tc.want)
		}
	}
}

// TestStamp: the §6 channel-auth clamp at the identity-stamping path.
// Config validation rejects owner-on-weak at load, but the identities
// table is the runtime source of truth and can drift (manual DB edits,
// stale seed) — so the invariant "a weak channel can NEVER stamp owner
// trust" must hold again here, at the point of stamping. The auth set
// is closed and case-sensitive; anything else — including "" from a
// channel absent from [channels] — fails closed to the bottom.
func TestStamp(t *testing.T) {
	for _, tc := range []struct {
		name   string
		auth   string
		lookup trust.Level
		want   trust.Stamped
	}{
		{"strong channel, owner", "strong", trust.Owner,
			trust.Stamped{Trust: trust.Owner, ReadOnly: false, MayApprove: true}},
		{"strong channel, known", "strong", trust.Known,
			trust.Stamped{Trust: trust.Known, ReadOnly: false, MayApprove: true}},
		// MayApprove is the CHANNEL capability, not a verdict: §4.4
		// approval matching separately requires the owner_id match, so an
		// untrusted sender on a strong channel still cannot approve.
		{"strong channel, unmapped sender", "strong", trust.Untrusted,
			trust.Stamped{Trust: trust.Untrusted, ReadOnly: false, MayApprove: true}},
		// THE clamp: an owner row that drifted onto a weak channel past
		// config validation stamps at most known, read-only, no approval.
		{"weak channel, drifted owner row", "weak", trust.Owner,
			trust.Stamped{Trust: trust.Known, ReadOnly: true, MayApprove: false}},
		{"weak channel, known", "weak", trust.Known,
			trust.Stamped{Trust: trust.Known, ReadOnly: true, MayApprove: false}},
		{"weak channel, unmapped sender", "weak", trust.Untrusted,
			trust.Stamped{Trust: trust.Untrusted, ReadOnly: true, MayApprove: false}},
		{"unconfigured channel fails closed", "", trust.Owner,
			trust.Stamped{Trust: trust.Untrusted, ReadOnly: true, MayApprove: false}},
		{"junk auth fails closed", "Strong", trust.Owner,
			trust.Stamped{Trust: trust.Untrusted, ReadOnly: true, MayApprove: false}},
		// A junk lookup level (Level is a string type) reads as the
		// bottom even on a strong channel, mirroring Min.
		{"junk lookup level fails closed", "strong", trust.Level("admin"),
			trust.Stamped{Trust: trust.Untrusted, ReadOnly: false, MayApprove: true}},
	} {
		if got := trust.Stamp(tc.auth, tc.lookup); got != tc.want {
			t.Errorf("%s: Stamp(%q, %q) = %+v, want %+v", tc.name, tc.auth, tc.lookup, got, tc.want)
		}
	}
}
