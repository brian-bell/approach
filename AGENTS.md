# Agent Instructions

> **Note:** this repo maintains separate `CLAUDE.md` and `AGENTS.md`
> files (rather than a symlink) to support the beads integration —
> `bd setup claude` manages a section in `CLAUDE.md` and `bd setup codex`
> manages one in this file, and neither will write through a symlink.
> Keep project instructions here; `CLAUDE.md` stays a pointer.

APPROACH is a personal agent harness: one small Go daemon that will own
channel adapters, an event router, and an agent-engine lifecycle, with a
SQLite state store and a trust/policy model designed in
`docs/approach-agent-harness-spec.html`. Section references in code
comments (§4.1, §6, §7, …) point at that spec.

**Current state**: the daemon runs end to end for discord messages —
event router (per-thread queues over the events table), discord
adapter, session lifecycle, engine spawning (`claude -p` under the §2
version pin), the C11 turns table, and the deliveries outbox with
restart resend. With `[engine]` configured, an inbound owner-stamped
message runs a real engine turn and the reply relays back; sub-owner
events are refused fail-closed (`skipped`) until the C9 policy gate
lands, and without `[engine]` the daemon boots dormant and loud.
Policy enforcement (C9 hook), sandbox (C10), heartbeats, and workers
are later milestones.

## Build & Test

```bash
make build    # go build -o bin/approach ./cmd/approach
make test     # go test -race ./...
make lint     # golangci-lint run (config: .golangci.yml)
make check    # build + test + lint
make clean    # remove bin/
```

Go 1.26; the only direct dependency is BurntSushi/toml (SQLite is the
pure-Go modernc.org/sqlite driver — no cgo). CI (`.github/workflows/ci.yml`)
runs build, test, and golangci-lint on pushes to main and PRs.

## Layout

- `cmd/approach/` — main; all logic lives in `internal/`.
- `internal/cli/` — subcommand dispatch (`daemon`, `poke`, `status`,
  `drain`, `config check`, `version`) and the daemon startup sequence:
  claim daemon lock → load config → open/migrate store → seed
  identities → serve admin socket.
- `internal/admin/` — the Unix admin socket (`approach.sock`): one
  line-based verb per connection (poke | status | drain). Daemon
  ownership is a flock'd `approach.sock.lock` sidecar, taken in
  `New` before the store is touched.
- `internal/config/` — loads and validates `approach.toml` (models,
  channels, identities, sessions, policy matrix). Fails loud: unknown
  keys are errors, enums are closed, missing policy cells mean deny.
- `internal/store/` — SQLite state store (`approach.db`): posture-checked
  open (WAL, 0700 dir / 0600 file, verified pragmas), embedded
  migrations under `migrations/`, identity seeding, session taint flag.
- `internal/trust/` — trust levels (untrusted < known < owner), session
  trust-floor computation, and the content-taint rules.
- `internal/policy/` — the §7 read/write path denylist (`DeniedPath`,
  `DeniedDir`, `DeniedDescendant`), including symlink-alias handling.
  Enforcement (the PreToolUse hook, C9) is M1 work.
- `deploy/systemd/` — user-level units plus `units_test.go`, which
  enforces that every unit is `PartOf=`/`WantedBy=approach.target`
  (the kill switch), and a kill-switch smoke test script.
- `docs/` — the design spec (HTML) and `approach.toml.example`.

## Conventions

- **Fail closed, fail loud.** Ambiguity resolves to deny/untrusted/error,
  never to a silent default: an unknown trust level reads as untrusted,
  an empty policy cell reads as deny, a store error never reads as
  "clean" or "not the owner", and a walk error in the denylist is an
  error, not an allow.
- **Comments carry the "why" and the spec section.** Code that encodes
  a security decision cites the spec (§6, §7 …) and explains the failure
  mode it forecloses. Match this density when editing these files.
- **TDD.** Every package has table-driven tests beside it; write the
  failing test first. `make test` runs with `-race`.
- **Migrations** (`internal/store/migrations/`): named
  `NNNN_description.sql`, numbered contiguously from 0001, applied as
  one transaction. Never `BEGIN`/`COMMIT`/`ROLLBACK` inside one, never
  mention `user_version` (rejected lexically — the runner owns it),
  and don't toggle pragmas. The runner detects transaction escapes at
  test time.
- **Exit codes**: the daemon exits 3 (`exitUnrecoverable` in
  `internal/cli`) on startup refusals a restart cannot fix — kept in
  sync with `RestartPreventExitStatus=3` in
  `deploy/systemd/approach.service`. CLI usage errors exit 2, other
  failures 1.
- **Logging**: the daemon logs structured JSON (slog) to stderr — the
  systemd journal. stdout carries only the single readiness line
  launchers may wait on.

## Gotchas

- The daemon lock must be taken **before** the store opens: a second
  (possibly newer) binary must be refused before it can migrate the
  schema out from under the running daemon.
- The defaulted config path is `approach.toml` **beside** the state
  directory (the `$APPROACH_HOME` layout: `~/approach/approach.toml`
  next to `~/approach/state/`). A missing file at the defaulted path
  boots with zero identities (deny-by-default, loudly warned); an
  explicit `--config` path must load cleanly or the daemon refuses to
  start.
- `store.SeedIdentities` is a full sync, not an upsert: removing an
  identity from `approach.toml` revokes it at the next startup.
- Weak-auth channels (sms, email) are clamped to at most `known` trust,
  are read-only, and can never satisfy an approval. Config validation
  rejects `owner` trust on a weak channel.
- `internal/policy` matching is lexical and case-insensitive; symlink
  resolution belongs to the enforcement point. `DeniedDir` is the
  os-filesystem entry point that resolves and walks symlinks within the
  session cwd boundary — its alias/spelling logic is subtle and heavily
  tested; read the comments before touching it.

## Non-Interactive Shell Commands

**ALWAYS use non-interactive flags** with file operations to avoid hanging on confirmation prompts.

Shell commands like `cp`, `mv`, and `rm` may be aliased to include `-i` (interactive) mode on some systems, causing the agent to hang indefinitely waiting for y/n input.

**Use these forms instead:**
```bash
# Force overwrite without prompting
cp -f source dest           # NOT: cp source dest
mv -f source dest           # NOT: mv source dest
rm -f file                  # NOT: rm file

# For recursive operations
rm -rf directory            # NOT: rm -r directory
cp -rf source dest          # NOT: cp -r source dest
```

**Other commands that may prompt:**
- `scp` - use `-o BatchMode=yes` for non-interactive
- `ssh` - use `-o BatchMode=yes` to fail instead of prompting
- `apt-get` - use `-y` flag
- `brew` - use `HOMEBREW_NO_AUTO_UPDATE=1` env var

<!-- BEGIN BEADS CODEX SETUP: generated by bd setup codex -->
## Beads Issue Tracker

Use Beads (`bd`) for durable task tracking in repositories that include it. Use the `beads` skill at `.agents/skills/beads/SKILL.md` (project install) or `~/.agents/skills/beads/SKILL.md` (global install) for Beads workflow guidance, then use the `bd` CLI for issue operations.

### Quick Reference

```bash
bd ready                # Find available work
bd show <id>            # View issue details
bd update <id> --claim  # Claim work
bd close <id>           # Complete work
bd prime                # Refresh Beads context
```

### Rules

- Use `bd` for all task tracking; do not create markdown TODO lists.
- Run `bd prime` when Beads context is missing or stale. Codex 0.129.0+ can load Beads context automatically through native hooks; use `/hooks` to inspect or toggle them.
- Keep persistent project memory in Beads via `bd remember`; do not create ad hoc memory files.

**Architecture in one line:** issues live in a local Dolt DB; sync uses `refs/dolt/data` on your git remote; `.beads/issues.jsonl` is a passive export. See https://github.com/gastownhall/beads/blob/main/docs/SYNC_CONCEPTS.md for details and anti-patterns.
<!-- END BEADS CODEX SETUP -->
