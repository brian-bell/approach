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
