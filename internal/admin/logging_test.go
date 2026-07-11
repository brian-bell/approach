package admin_test

import (
	"bytes"
	"encoding/json"
	"log/slog"
	"strings"
	"sync"
	"testing"

	"github.com/brian-bell/approach/internal/admin"
)

// logBuffer collects slog JSON records safely across handler goroutines.
type logBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *logBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

// records decodes every JSON log line written so far.
func (b *logBuffer) records(t *testing.T) []map[string]any {
	t.Helper()
	b.mu.Lock()
	defer b.mu.Unlock()
	var out []map[string]any
	for _, line := range strings.Split(strings.TrimSpace(b.buf.String()), "\n") {
		if line == "" {
			continue
		}
		var rec map[string]any
		if err := json.Unmarshal([]byte(line), &rec); err != nil {
			t.Fatalf("log line %q is not JSON: %v", line, err)
		}
		out = append(out, rec)
	}
	return out
}

// find returns the first record with the given msg, or nil.
func find(records []map[string]any, msg string) map[string]any {
	for _, rec := range records {
		if rec["msg"] == msg {
			return rec
		}
	}
	return nil
}

// TestPanicInHookIsRecovered: a panic in a daemon hook (OnPoke today,
// the event router later) must not kill the daemon: the client gets a
// diagnostic error, the next request is served, and the panic is
// logged with a stack for the journal.
func TestPanicInHookIsRecovered(t *testing.T) {
	logs := &logBuffer{}
	panics := true
	path := startServer(t, admin.Options{
		Logger: slog.New(slog.NewJSONHandler(logs, nil)),
		OnPoke: func() {
			if panics {
				panics = false
				panic("poke hook exploded")
			}
		},
		Status: func() map[string]any { panic("status provider exploded") },
	})

	if reply := request(t, path, "poke"); reply != "err internal error" {
		t.Errorf("poke during hook panic = %q, want %q", reply, "err internal error")
	}
	if reply := request(t, path, "poke"); reply != "ok" {
		t.Errorf("poke after recovered panic = %q, want %q — daemon must keep serving", reply, "ok")
	}
	if reply := request(t, path, "status"); reply != "err internal error" {
		t.Errorf("status with panicking provider = %q, want %q", reply, "err internal error")
	}

	rec := find(logs.records(t), "panic in admin handler")
	if rec == nil {
		t.Fatalf("no panic record in logs: %v", logs.records(t))
	}
	if !strings.Contains(rec["panic"].(string), "poke hook exploded") {
		t.Errorf("panic record = %v, want the panic value", rec["panic"])
	}
	stack, _ := rec["stack"].(string)
	if !strings.Contains(stack, "admin.") {
		t.Errorf("panic record stack %q does not look like a stack trace", stack)
	}
}

// TestRequestsAreLoggedStructured: every admin request produces one
// structured record naming the verb — the daemon's journal is how an
// operator reconstructs what woke it and when.
func TestRequestsAreLoggedStructured(t *testing.T) {
	logs := &logBuffer{}
	path := startServer(t, admin.Options{
		Logger: slog.New(slog.NewJSONHandler(logs, nil)),
	})

	if reply := request(t, path, "poke"); reply != "ok" {
		t.Fatalf("poke reply = %q, want ok", reply)
	}

	rec := find(logs.records(t), "admin request")
	if rec == nil {
		t.Fatalf("no \"admin request\" record in logs: %v", logs.records(t))
	}
	if rec["verb"] != "poke" {
		t.Errorf("logged verb = %v, want poke", rec["verb"])
	}
}
