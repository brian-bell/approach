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
