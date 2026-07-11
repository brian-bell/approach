// Package admin serves the daemon's Unix admin socket: the single
// front door through which systemd timers and the operator reach a
// running daemon (§3, §8). The protocol is deliberately minimal —
// one line-based request per connection: poke, status, or drain.
package admin

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

// Options configures the hooks a Server calls into the daemon with.
type Options struct {
	// OnPoke is invoked for each poke request — the daemon's wake-up
	// call. Nil means poke is acknowledged and otherwise ignored.
	OnPoke func()
	// Status supplies the fields reported by the status verb. Nil
	// reports an empty object.
	Status func() map[string]any
	// ReadTimeout bounds how long a connection may sit silent before
	// it is cut off. Zero means a 5-second default.
	ReadTimeout time.Duration
	// OnReady is invoked once the socket is bound and accepting — the
	// only point a launcher may treat the daemon as reachable.
	OnReady func()
}

// Server is the admin socket listener. Create with New, run with
// Serve, release with Close.
type Server struct {
	path      string
	opts      Options
	drain     chan struct{}
	drainOnce sync.Once

	lockMu sync.Mutex
	lock   *os.File
}

// maxSocketPath is a conservative sun_path budget: the kernel limit is
// 104 bytes on darwin/BSD and 108 on Linux, both including the NUL.
const maxSocketPath = 103

// maxRequestLine bounds one request: the longest real verb is 6 bytes,
// so anything past this is garbage that must not be buffered or echoed.
const maxRequestLine = 256

// New claims daemon ownership for the socket at path and prepares the
// server. Ownership is taken HERE, not in Serve, so a refused second
// daemon fails before its caller touches anything it does not own —
// the daemon opens and migrates the state store between New and
// Serve. The returned server holds the lock until Close (or process
// exit — flock dies with its holder).
func New(path string, opts Options) (*Server, error) {
	if len(path) > maxSocketPath {
		return nil, fmt.Errorf("admin: socket path %s is too long (%d bytes, sun_path caps at %d) — use a shorter state directory", path, len(path), maxSocketPath)
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return nil, fmt.Errorf("admin: %w", err)
	}
	lock, err := acquireLock(path)
	if err != nil {
		return nil, err
	}
	return &Server{path: path, opts: opts, drain: make(chan struct{}), lock: lock}, nil
}

// Close releases daemon ownership. Safe to call more than once.
func (s *Server) Close() error {
	s.lockMu.Lock()
	defer s.lockMu.Unlock()
	if s.lock == nil {
		return nil
	}
	lock := s.lock
	s.lock = nil
	if err := lock.Close(); err != nil {
		return fmt.Errorf("admin: release socket lock: %w", err)
	}
	return nil
}

// Serve listens on the socket and answers admin requests until ctx is
// canceled, then shuts down cleanly and removes the socket file.
func (s *Server) Serve(ctx context.Context) error {
	if err := s.claim(); err != nil {
		return err
	}
	listener, err := net.Listen("unix", s.path)
	if err != nil {
		return fmt.Errorf("admin: listen on %s: %w", s.path, err)
	}
	// Owner-only: any connection can drain the daemon. The parent dir
	// is already 0700, so the pre-chmod window exposes nothing.
	if err := os.Chmod(s.path, 0o600); err != nil {
		return errors.Join(fmt.Errorf("admin: chmod socket: %w", err), listener.Close())
	}
	if s.opts.OnReady != nil {
		s.opts.OnReady()
	}

	// The watcher owns intentional shutdown: context cancellation and
	// the drain verb both land here and close the listener, which the
	// accept loop reads as a clean stop. exit releases the watcher when
	// Serve returns for any other reason.
	var stopping atomic.Bool
	exit := make(chan struct{})
	defer close(exit)
	go func() {
		select {
		case <-ctx.Done():
		case <-s.drain:
		case <-exit:
			return
		}
		stopping.Store(true)
		// Close errors surface as the accept loop's exit, not here.
		_ = listener.Close()
	}()

	var handlers sync.WaitGroup
	defer handlers.Wait()
	for {
		conn, err := listener.Accept()
		if err != nil {
			if stopping.Load() {
				return nil
			}
			return fmt.Errorf("admin: accept: %w", err)
		}
		handlers.Add(1)
		go func() {
			defer handlers.Done()
			s.handle(conn)
		}()
	}
}

// Request sends one admin verb to the daemon socket at path and
// returns the reply line — the client half of the protocol, used by
// the CLI subcommands and systemd timer pokes.
func Request(path, verb string) (string, error) {
	conn, err := net.DialTimeout("unix", path, time.Second)
	if err != nil {
		return "", fmt.Errorf("admin: dial %s: %w — is the daemon running?", path, err)
	}
	defer func() { _ = conn.Close() }()
	if err := conn.SetDeadline(time.Now().Add(5 * time.Second)); err != nil {
		return "", fmt.Errorf("admin: %w", err)
	}
	if _, err := fmt.Fprintln(conn, verb); err != nil {
		return "", fmt.Errorf("admin: send %s: %w", verb, err)
	}
	reply, err := bufio.NewReader(conn).ReadString('\n')
	if err != nil {
		return "", fmt.Errorf("admin: read reply to %s: %w", verb, err)
	}
	return strings.TrimSuffix(reply, "\n"), nil
}

// acquireLock takes exclusive ownership of the socket via a flock'd
// sidecar file. Daemon ownership is this lock, NOT the socket file:
// probe-then-unlink is a TOCTOU race where two starts can each probe
// a stale socket and the loser unlinks the winner's freshly bound
// one. Failure to lock means another daemon holds it (flock dies with
// its holder, so a crash never wedges the next start, and lock-held
// means a daemon is alive even in the instant its socket file is
// absent). The caller keeps the returned file open for the daemon's
// lifetime.
func acquireLock(path string) (*os.File, error) {
	lockPath := path + ".lock"
	lock, err := os.OpenFile(lockPath, os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return nil, fmt.Errorf("admin: open socket lock: %w", err)
	}
	if err := syscall.Flock(int(lock.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		cerr := lock.Close()
		if errors.Is(err, syscall.EWOULDBLOCK) {
			return nil, errors.Join(fmt.Errorf("admin: %s is locked — a daemon is already running", lockPath), cerr)
		}
		return nil, errors.Join(fmt.Errorf("admin: lock %s: %w", lockPath, err), cerr)
	}
	return lock, nil
}

// claim clears the way to bind s.path. The caller holds the daemon
// lock, so any socket file present is stale by definition — but only
// a socket is ever unlinked; anything else at the path (file,
// directory, symlink) is refused.
func (s *Server) claim() error {
	info, err := os.Lstat(s.path)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("admin: %w", err)
	}
	if info.Mode()&os.ModeSocket == 0 {
		return fmt.Errorf("admin: %s exists and is not a socket — refusing to replace it", s.path)
	}
	if err := os.Remove(s.path); err != nil {
		return fmt.Errorf("admin: remove stale socket: %w", err)
	}
	return nil
}

// handle answers one request on conn and closes it.
func (s *Server) handle(conn net.Conn) {
	defer func() { _ = conn.Close() }()
	// A silent or wedged client must not pin its handler goroutine
	// (and the drain WaitGroup) forever: the deadline covers the WRITE
	// side too — a client that sends a verb and never reads the reply
	// would otherwise block the response forever once the socket
	// buffer fills. The request line is capped so the reply (which
	// echoes the verb) stays small, and truncated again when echoed.
	timeout := s.opts.ReadTimeout
	if timeout == 0 {
		timeout = 5 * time.Second
	}
	if err := conn.SetDeadline(time.Now().Add(timeout)); err != nil {
		return
	}
	line, err := bufio.NewReader(io.LimitReader(conn, maxRequestLine)).ReadString('\n')
	if err != nil {
		return
	}
	verb := strings.TrimSuffix(line, "\n")
	switch verb {
	case "poke":
		if s.opts.OnPoke != nil {
			s.opts.OnPoke()
		}
		fmt.Fprintln(conn, "ok")
	case "status":
		fields := map[string]any{}
		if s.opts.Status != nil {
			fields = s.opts.Status()
		}
		body, err := json.Marshal(fields)
		if err != nil {
			fmt.Fprintf(conn, "err status: %v\n", err)
			return
		}
		fmt.Fprintf(conn, "ok %s\n", body)
	case "drain":
		// Reply before triggering shutdown so the client sees the ack.
		fmt.Fprintln(conn, "ok draining")
		s.drainOnce.Do(func() { close(s.drain) })
	default:
		if len(verb) > 64 {
			verb = verb[:64] + "…"
		}
		fmt.Fprintf(conn, "err unknown command %q — expected poke, status, or drain\n", verb)
	}
}
