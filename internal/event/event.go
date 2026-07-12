// Package event holds the §6 normalized-event contract: the ONE shape
// every entry point — message, heartbeat, webhook, cron, approval —
// is normalized into by the adapter layer (C1) and consumed from by
// the router (C4). It is channel-neutral and imports nothing internal:
// adapters depend on it, never on each other, and the store mirrors
// four of its fields into queue columns without owning the type.
package event

import "encoding/json"

// Attachment is one externally-authored file reference carried by an
// event. Normalization of platform attachments lands in x6n.1.6; the
// type exists now so the wire shape ("attachments": []) is stable from
// the first persisted payload — this column outlives adapter versions.
type Attachment struct {
	// URL is the platform-hosted location of the attachment content —
	// recorded, never dereferenced at ingest (egress policy is
	// C9/C10 territory, §7).
	URL string `json:"url"`
	// Filename is the platform-reported name — externally authored,
	// display-only, never a filesystem path (§7). It passes through
	// verbatim (path separators included): sanitizing is the
	// consumer's job at point of use, and mangling at ingest would
	// hide what the sender actually supplied from the audit trail.
	Filename string `json:"filename"`
	// ContentType and Size are the platform's own report — policy
	// hints, not verified facts about the bytes behind the URL.
	ContentType string `json:"content_type"`
	Size        int64  `json:"size"`
}

// Event is the §6 inbound contract. The trust field is load-bearing —
// policy (§7) and sandbox routing key off it — and is stamped by the
// adapter from the identities lookup, never inferred from content;
// any unmapped sender reads as untrusted (§6).
//
// Pointer fields are the contract's nullables: owner_id, occurrence
// and reply_to marshal as JSON null when absent, so a consumer can
// trust the spelling ("no owner" is null, never "").
type Event struct {
	Channel   string `json:"channel"`
	ThreadKey string `json:"thread_key"` // per-channel contract: discord:dm:<user_id> · discord:thread:<thread_id> · …
	DedupKey  string `json:"dedup_key"`  // REQUIRED event identity; message → (channel, native message id)
	Sender    string `json:"sender"`     // native platform sender id — what the identities table keys off
	// OwnerID is the canonical principal from the identities lookup on
	// owner rows; null otherwise. Cross-surface approval matches on
	// this (§4.4).
	OwnerID *string `json:"owner_id"`
	Trust   string  `json:"trust"` // owner | known | untrusted | system | system-worker
	Kind    string  `json:"kind"`  // message | heartbeat | webhook | cron | approval | task
	// Occurrence is cron/heartbeat only: the ISO occurrence_time (plus
	// missed_count when coalescing a misfire, §4.2). Null for messages.
	Occurrence *string `json:"occurrence"`
	Text       string  `json:"text"`
	// Attachments is always an array on the wire, never null —
	// consumers range over it without a nil guard (MarshalJSON below
	// enforces this even for the zero value).
	Attachments []Attachment `json:"attachments"`
	ReplyTo     *string      `json:"reply_to"`
}

// MarshalJSON keeps the wire shape stable for the zero value: a nil
// Attachments slice would marshal as null, and "attachments": null is
// a contract violation consumers should never have to defend against.
func (e Event) MarshalJSON() ([]byte, error) {
	type wire Event // shed the method — plain struct marshal, no recursion
	w := wire(e)
	if w.Attachments == nil {
		w.Attachments = []Attachment{}
	}
	return json.Marshal(w)
}
