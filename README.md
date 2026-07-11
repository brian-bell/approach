# APPROACH

A personal agent harness: one small Go daemon that owns channel
adapters, an event router, and an agent-engine lifecycle, backed by a
SQLite state store and a deny-by-default trust and policy model. The
full design lives in
[`docs/approach-agent-harness-spec.html`](docs/approach-agent-harness-spec.html)
— the § references throughout the code and docs point into it.

## Status

Early. The daemon skeleton and trust foundation are built:

- **Admin socket** — a Unix socket answering `poke`, `status`, and
  `drain`, with flock-based single-daemon ownership.
- **State store** — SQLite (pure Go, no cgo) opened with a verified
  security posture (WAL, 0700/0600 permissions, checked pragmas) and
  embedded, transaction-safe schema migrations.
- **Config** — a single validated `approach.toml`: model routing,
  channel auth grading, hand-enrolled identities, session TTLs, and a
  capability × trust policy matrix. Unknown keys are errors; missing
  policy cells mean deny.
- **Trust model** — trust levels (`untrusted < known < owner`), session
  trust floors, a sticky session taint flag, and a read/write path
  denylist that survives symlink laundering.
- **Kill switch** — user-level systemd units grouped under one target
  so a single command stops everything.

The event router, channel adapters, and engine spawning are later
milestones — today a `poke` only increments a counter you can see in
`status`.

## Build

Requires Go 1.26+.

```bash
make build    # builds bin/approach
make test     # go test -race ./...
make check    # build + test + lint (needs golangci-lint v2)
```

## Usage

```
usage: approach <command>

commands:
  daemon [--state <dir>] [--config <path>]
                             run the daemon (admin socket + state store)
  poke [--socket <path>]     wake a running daemon
  status [--socket <path>]   report a running daemon's status
  drain [--socket <path>]    gracefully stop a running daemon
  config check <path>        validate an approach.toml file
  version                    print the approach version
```

The daemon keeps its socket and database in a state directory —
`$APPROACH_HOME/state`, defaulting to `~/approach/state`. Configuration
is read from `approach.toml` beside the state directory (so
`~/approach/approach.toml` by default); with no config file the daemon
still boots, but with zero enrolled identities — everyone is untrusted,
deny-by-default.

See [`docs/approach.toml.example`](docs/approach.toml.example) for an
annotated config covering model routing, channels, identities, session
rotation, and policy-matrix overrides. Validate edits with
`approach config check path/to/approach.toml`.

## Deployment

User-level systemd units live in [`deploy/systemd/`](deploy/systemd/),
including install steps, the panic-button semantics
(`systemctl --user disable --now approach.target`), and a kill-switch
smoke test. See [its README](deploy/systemd/README.md).

## Development

Issue tracking uses [beads](https://github.com/gastownhall/beads)
(`bd ready`, `bd show <id>`). Agent-facing project context is in
[`AGENTS.md`](AGENTS.md) (`CLAUDE.md` is a symlink to it).

## License

[MIT](LICENSE)
