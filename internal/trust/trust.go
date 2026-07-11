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
