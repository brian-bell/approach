package event_test

import (
	"encoding/json"
	"reflect"
	"sort"
	"testing"

	"github.com/brian-bell/approach/internal/event"
)

// TestEventJSONShape pins the §6 wire contract exactly: every key
// present on every marshal (consumers must never branch on key
// absence), nullable fields spelled null — not omitted, not "" — and
// no extra keys leaking implementation detail into a payload that
// outlives the code that wrote it (§6: the queue replays from the
// payload long after the adapter that produced it has changed).
func TestEventJSONShape(t *testing.T) {
	ev := event.Event{
		Channel:   "discord",
		ThreadKey: "discord:dm:123",
		DedupKey:  "discord:msg:9871",
		Sender:    "123",
		Trust:     "untrusted",
		Kind:      "message",
		Text:      "hello",
	}
	raw, err := json.Marshal(ev)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var got map[string]any
	if err := json.Unmarshal(raw, &got); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}

	wantKeys := []string{
		"channel", "thread_key", "dedup_key", "sender", "owner_id",
		"trust", "kind", "occurrence", "text", "attachments", "reply_to",
	}
	var gotKeys []string
	for k := range got {
		gotKeys = append(gotKeys, k)
	}
	sort.Strings(gotKeys)
	sorted := append([]string(nil), wantKeys...)
	sort.Strings(sorted)
	if !reflect.DeepEqual(gotKeys, sorted) {
		t.Errorf("marshaled keys = %v, want the exact §6 set %v", gotKeys, sorted)
	}

	// Unset optionals are null — a consumer distinguishing "no owner"
	// from "" must be able to trust the spelling.
	for _, k := range []string{"owner_id", "occurrence", "reply_to"} {
		if got[k] != nil {
			t.Errorf("%s = %v, want null when unset", k, got[k])
		}
	}
	// attachments is always an array, never null: consumers range over
	// it without a nil check.
	if _, ok := got["attachments"].([]any); !ok {
		t.Errorf("attachments = %v (%T), want a JSON array even when empty", got["attachments"], got["attachments"])
	}
}

// TestEventJSONRoundTrip: what the adapter writes, the router reads
// back identically — the payload column is the §6 replay source.
func TestEventJSONRoundTrip(t *testing.T) {
	owner := "brian"
	reply := "discord:msg:111"
	ev := event.Event{
		Channel:     "discord",
		ThreadKey:   "discord:thread:42",
		DedupKey:    "discord:msg:9871",
		Sender:      "123",
		OwnerID:     &owner,
		Trust:       "owner",
		Kind:        "message",
		Text:        "hi",
		Attachments: []event.Attachment{},
		ReplyTo:     &reply,
	}
	raw, err := json.Marshal(ev)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var back event.Event
	if err := json.Unmarshal(raw, &back); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if !reflect.DeepEqual(ev, back) {
		t.Errorf("round trip drifted:\n got %+v\nwant %+v", back, ev)
	}
}
