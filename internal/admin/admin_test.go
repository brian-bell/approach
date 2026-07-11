package admin_test

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/brian-bell/approach/internal/admin"
)

// startServer runs a Server on a fresh socket and returns the socket
// path. Serve's error and exit are checked at cleanup so every test
// also asserts a clean drain.
func startServer(t *testing.T, opts admin.Options) string {
	t.Helper()
	path := filepath.Join(shortTempDir(t), "state", "approach.sock")
	srv, err := admin.New(path, opts)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- srv.Serve(ctx) }()
	t.Cleanup(func() {
		cancel()
		select {
		case err := <-done:
			if err != nil {
				t.Errorf("Serve: %v", err)
			}
		case <-time.After(5 * time.Second):
			t.Error("Serve did not return after context cancel")
		}
		if err := srv.Close(); err != nil {
			t.Errorf("Close: %v", err)
		}
	})
	waitDialable(t, path)
	return path
}

// shortTempDir is t.TempDir minus the test name: sun_path caps Unix
// socket paths at ~104 bytes on darwin, and long test names blow it.
func shortTempDir(t *testing.T) string {
	t.Helper()
	dir, err := os.MkdirTemp("", "adm")
	if err != nil {
		t.Fatalf("make temp dir: %v", err)
	}
	t.Cleanup(func() {
		if err := os.RemoveAll(dir); err != nil {
			t.Errorf("remove temp dir: %v", err)
		}
	})
	return dir
}

// waitDialable blocks until the socket accepts connections.
func waitDialable(t *testing.T, path string) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		conn, err := net.Dial("unix", path)
		if err == nil {
			if err := conn.Close(); err != nil {
				t.Fatalf("close probe connection: %v", err)
			}
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("socket %s never became dialable", path)
}

// request sends one verb and returns the single reply line.
func request(t *testing.T, path, verb string) string {
	t.Helper()
	conn, err := net.Dial("unix", path)
	if err != nil {
		t.Fatalf("dial %s: %v", path, err)
	}
	defer func() { _ = conn.Close() }()
	if _, err := conn.Write([]byte(verb + "\n")); err != nil {
		t.Fatalf("send %q: %v", verb, err)
	}
	reply, err := bufio.NewReader(conn).ReadString('\n')
	if err != nil {
		t.Fatalf("read reply to %q: %v", verb, err)
	}
	return strings.TrimSuffix(reply, "\n")
}

// TestStatusReportsDaemonFields: "status" answers ok plus a JSON
// object carrying whatever the daemon's Status provider reports.
func TestStatusReportsDaemonFields(t *testing.T) {
	path := startServer(t, admin.Options{
		Status: func() map[string]any {
			return map[string]any{"version": "test", "schema_version": 1}
		},
	})

	reply := request(t, path, "status")
	body, found := strings.CutPrefix(reply, "ok ")
	if !found {
		t.Fatalf("status reply = %q, want %q prefix", reply, "ok ")
	}
	var fields map[string]any
	if err := json.Unmarshal([]byte(body), &fields); err != nil {
		t.Fatalf("status body %q is not JSON: %v", body, err)
	}
	if fields["version"] != "test" || fields["schema_version"] != float64(1) {
		t.Errorf("status fields = %v, want version=test schema_version=1", fields)
	}
}

// TestStatusWithoutProvider: a nil Status provider still answers ok
// with an empty object rather than failing the verb.
func TestStatusWithoutProvider(t *testing.T) {
	path := startServer(t, admin.Options{})

	if reply := request(t, path, "status"); reply != "ok {}" {
		t.Errorf("status reply = %q, want %q", reply, "ok {}")
	}
}

// TestUnknownVerbAnswersError: garbage on the socket gets a diagnostic
// reply, and the server keeps serving other clients.
func TestUnknownVerbAnswersError(t *testing.T) {
	path := startServer(t, admin.Options{})

	reply := request(t, path, "reboot")
	if !strings.HasPrefix(reply, "err ") || !strings.Contains(reply, "reboot") {
		t.Errorf("reply = %q, want err naming the unknown verb %q", reply, "reboot")
	}
	if reply := request(t, path, "poke"); reply != "ok" {
		t.Errorf("poke after unknown verb = %q, want %q — server must keep serving", reply, "ok")
	}
}

// TestDrainShutsDownCleanly: "drain" acknowledges, then Serve returns
// nil and removes the socket file, so nothing can dial a drained
// daemon — the graceful half of the §7 kill switch.
func TestDrainShutsDownCleanly(t *testing.T) {
	path := filepath.Join(shortTempDir(t), "approach.sock")
	srv, err := admin.New(path, admin.Options{})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() {
		if err := srv.Close(); err != nil {
			t.Errorf("Close: %v", err)
		}
	})
	done := make(chan error, 1)
	go func() { done <- srv.Serve(context.Background()) }()
	waitDialable(t, path)

	if reply := request(t, path, "drain"); reply != "ok draining" {
		t.Errorf("drain reply = %q, want %q", reply, "ok draining")
	}
	select {
	case err := <-done:
		if err != nil {
			t.Errorf("Serve after drain: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Serve did not return after drain")
	}
	if _, err := os.Lstat(path); !os.IsNotExist(err) {
		t.Errorf("socket file still present after drain (Lstat err = %v)", err)
	}
	if _, err := net.Dial("unix", path); err == nil {
		t.Error("dial succeeded after drain, want failure")
	}
}

// TestStaleSocketFileIsReplaced: a socket file left by a crashed
// daemon (nothing listening) must not wedge the next start.
func TestStaleSocketFileIsReplaced(t *testing.T) {
	dir := shortTempDir(t)
	path := filepath.Join(dir, "approach.sock")
	stale, err := net.Listen("unix", path)
	if err != nil {
		t.Fatalf("make stale socket: %v", err)
	}
	// Close with unlink suppressed so the file survives like a crash
	// would leave it.
	stale.(*net.UnixListener).SetUnlinkOnClose(false)
	if err := stale.Close(); err != nil {
		t.Fatalf("close stale listener: %v", err)
	}
	if _, err := os.Lstat(path); err != nil {
		t.Fatalf("stale socket file missing before test: %v", err)
	}

	srv, err := admin.New(path, admin.Options{})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- srv.Serve(ctx) }()
	t.Cleanup(func() {
		cancel()
		if err := <-done; err != nil {
			t.Errorf("Serve: %v", err)
		}
		if err := srv.Close(); err != nil {
			t.Errorf("Close: %v", err)
		}
	})
	waitDialable(t, path)

	if reply := request(t, path, "poke"); reply != "ok" {
		t.Errorf("poke on reclaimed socket = %q, want %q", reply, "ok")
	}
}

// TestLiveSocketRefused: a second daemon must never steal the socket
// from a live one — and the refusal must land at New, BEFORE the
// caller does anything destructive under the assumption of ownership
// (the daemon opens and migrates the store between New and Serve).
func TestLiveSocketRefused(t *testing.T) {
	path := startServer(t, admin.Options{})

	_, err := admin.New(path, admin.Options{})
	if err == nil {
		t.Fatal("second New on a live daemon's socket succeeded, want refusal")
	}
	if !strings.Contains(err.Error(), "already running") {
		t.Errorf("error %q does not say a daemon is already running", err)
	}
	// The live daemon must be untouched.
	if reply := request(t, path, "poke"); reply != "ok" {
		t.Errorf("poke after refused second daemon = %q, want %q", reply, "ok")
	}
}

// TestSilentClientDoesNotBlockOthers: a client that connects and sends
// nothing is cut off by the read deadline, and a concurrent
// well-behaved client is served immediately meanwhile.
func TestSilentClientDoesNotBlockOthers(t *testing.T) {
	path := startServer(t, admin.Options{ReadTimeout: 200 * time.Millisecond})

	silent, err := net.Dial("unix", path)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer func() { _ = silent.Close() }()

	// Served while the silent connection is still open.
	if reply := request(t, path, "poke"); reply != "ok" {
		t.Errorf("poke alongside silent client = %q, want %q", reply, "ok")
	}

	// The silent connection is eventually closed by the server: a read
	// unblocks with EOF once the deadline cuts it off.
	if err := silent.SetReadDeadline(time.Now().Add(10 * time.Second)); err != nil {
		t.Fatalf("set client read deadline: %v", err)
	}
	buf := make([]byte, 1)
	if _, err := silent.Read(buf); err == nil {
		t.Error("silent connection produced data, want server-side close")
	} else if os.IsTimeout(err) {
		t.Error("server never closed the silent connection (client read timed out)")
	}
}

// TestSecondDaemonRefusedEvenWithoutSocketFile: liveness must not be
// inferred from the socket file — in the reclamation window between a
// probe and a bind the file can be briefly absent while a daemon is
// very much alive, and a probe-based second start would steal its
// place. Ownership is a lifetime lock, so the second start is refused
// even when the socket file is missing.
func TestSecondDaemonRefusedEvenWithoutSocketFile(t *testing.T) {
	path := startServer(t, admin.Options{})
	if err := os.Remove(path); err != nil {
		t.Fatalf("simulate reclamation window: %v", err)
	}

	_, err := admin.New(path, admin.Options{})
	if err == nil {
		t.Fatal("second New with live daemon (socket file absent) succeeded, want refusal")
	}
	if !strings.Contains(err.Error(), "already running") {
		t.Errorf("error %q does not say a daemon is already running", err)
	}
}

// TestWedgedClientCannotBlockDrain: a client that sends an oversized
// request and never reads the reply must not pin its handler — the
// drain WaitGroup would otherwise wait on it forever and defeat
// graceful shutdown. startServer's cleanup asserts Serve returns.
func TestWedgedClientCannotBlockDrain(t *testing.T) {
	// Registered before startServer so it runs AFTER startServer's
	// Serve-returned assertion (cleanups are LIFO): the client must
	// still be wedged while shutdown is asserted.
	var wedged net.Conn
	t.Cleanup(func() {
		if wedged != nil {
			_ = wedged.Close()
		}
	})
	path := startServer(t, admin.Options{ReadTimeout: 200 * time.Millisecond})

	var err error
	wedged, err = net.Dial("unix", path)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	// A megabyte of verb, newline-terminated, and no read of the reply:
	// an unbounded echo would block on the full socket buffer. A write
	// error is fine — it means the server already cut the connection
	// at the request-line cap, which is the protection at work.
	if _, err := wedged.Write(append(bytes.Repeat([]byte("x"), 1<<20), '\n')); err != nil {
		t.Logf("oversized write cut off by server: %v", err)
	}

	// The server must stay responsive alongside the wedged client...
	if reply := request(t, path, "poke"); reply != "ok" {
		t.Errorf("poke alongside wedged client = %q, want %q", reply, "ok")
	}
	// ...and cleanup (context cancel) must see Serve return promptly,
	// which fails if the wedged client's handler never exits.
}

// TestSocketMode: the admin socket is an owner-only control surface —
// anyone who can connect can drain the daemon — so it gets the same
// 0600 posture as the state store (§6, §7).
func TestSocketMode(t *testing.T) {
	path := startServer(t, admin.Options{})

	info, err := os.Lstat(path)
	if err != nil {
		t.Fatalf("Lstat socket: %v", err)
	}
	if got := info.Mode().Perm(); got != 0o600 {
		t.Errorf("socket mode = %04o, want 0600", got)
	}
}

// TestSymlinkedSocketPathRefused: a symlink at the socket path would
// let another user redirect the daemon's control surface; refuse it
// like the store refuses symlinked db paths.
func TestSymlinkedSocketPathRefused(t *testing.T) {
	dir := shortTempDir(t)
	path := filepath.Join(dir, "approach.sock")
	if err := os.Symlink(filepath.Join(dir, "elsewhere"), path); err != nil {
		t.Fatalf("make symlink: %v", err)
	}

	srv, err := admin.New(path, admin.Options{})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() {
		if err := srv.Close(); err != nil {
			t.Errorf("Close: %v", err)
		}
	})
	err = srv.Serve(context.Background())
	if err == nil {
		t.Fatal("Serve on a symlinked socket path succeeded, want refusal")
	}
	if !strings.Contains(err.Error(), "not a socket") {
		t.Errorf("error %q does not explain the refusal", err)
	}
	// The symlink must survive — refusal, not unlink.
	if _, err := os.Lstat(path); err != nil {
		t.Errorf("symlink removed by refused Serve: %v", err)
	}
}

// TestNewRefusesOverlongSocketPath: sun_path caps Unix socket paths
// (~104 bytes on darwin, 108 on Linux); bind would fail with an opaque
// "invalid argument", so New refuses with a clear error instead.
func TestNewRefusesOverlongSocketPath(t *testing.T) {
	path := filepath.Join("/tmp", strings.Repeat("x", 200), "approach.sock")

	if _, err := admin.New(path, admin.Options{}); err == nil {
		t.Fatalf("New accepted %d-byte socket path, want refusal", len(path))
	} else if !strings.Contains(err.Error(), "too long") {
		t.Errorf("error %q does not explain the path is too long", err)
	}
}

// TestPokeFiresHook: "poke" answers ok and invokes the OnPoke hook —
// the entry point systemd timers use to wake the daemon (§3, §8).
func TestPokeFiresHook(t *testing.T) {
	poked := make(chan struct{}, 1)
	path := startServer(t, admin.Options{OnPoke: func() { poked <- struct{}{} }})

	if reply := request(t, path, "poke"); reply != "ok" {
		t.Errorf("poke reply = %q, want %q", reply, "ok")
	}
	select {
	case <-poked:
	case <-time.After(5 * time.Second):
		t.Error("OnPoke hook never fired")
	}
}
