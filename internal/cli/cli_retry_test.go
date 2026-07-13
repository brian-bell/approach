package cli_test

import (
	"strings"
	"testing"

	"github.com/brian-bell/approach/internal/cli"
)

// TestRetryUsage: `approach retry` validates its event id BEFORE
// touching the socket — a garbled id is a usage error, not a request.
func TestRetryUsage(t *testing.T) {
	for _, args := range [][]string{
		{"retry"},
		{"retry", "notanumber"},
		{"retry", "-1"},
		{"retry", "0"},
	} {
		var out, errW strings.Builder
		if code := cli.Run(args, &out, &errW); code != 2 {
			t.Errorf("%v exit = %d, want 2 (usage)", args, code)
		}
	}
}

// TestDeadUsage: `approach dead <requeue|discard> <event-id>` — the
// §4.6 manual drain's CLI face validates before touching the socket.
func TestDeadUsage(t *testing.T) {
	for _, args := range [][]string{
		{"dead"},
		{"dead", "requeue"},
		{"dead", "discard", "notanumber"},
		{"dead", "shred", "7"},
		{"dead", "requeue", "0"},
	} {
		var out, errW strings.Builder
		if code := cli.Run(args, &out, &errW); code != 2 {
			t.Errorf("%v exit = %d, want 2 (usage)", args, code)
		}
	}
}
