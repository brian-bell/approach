# APPROACH systemd units

User-level units for the APPROACH daemon (§7, §8). One target —
`approach.target` — groups the daemon and every timer, so one command
stops everything (the kill switch, see §7):

```sh
systemctl --user disable --now approach.target   # panic button: nothing wakes up,
                                                 # and stays off across reboots/logins
systemctl --user enable --now approach.target    # resume
```

`systemctl --user stop approach.target` halts everything immediately
too, but the target is `WantedBy=default.target`, so the next login or
reboot would wake it again — use `disable --now` when you mean
"stay down until I say otherwise".

## Install

```sh
# Build and place the binary where the service expects it.
make build && install -d ~/bin && install bin/approach ~/bin/

# Link and enable the units.
systemctl --user link "$PWD"/deploy/systemd/approach.service \
                      "$PWD"/deploy/systemd/approach.target
systemctl --user enable approach.service approach.target
systemctl --user start approach.target
```

The daemon drains gracefully on SIGTERM (systemd's default stop
signal): it stops accepting admin connections, finishes in-flight
requests, and removes its socket. Stopping is bounded, not open-ended:
after `TimeoutStopSec` (30s) systemd force-kills the unit. That is the
point of a panic button — work interrupted by the hard stop is
recovered from durable state on the next start (§4.1, §9), never
silently replayed.

## Kill-switch smoke test

After installing, prove the panic button on the host itself:

```sh
deploy/systemd/smoke-kill-switch.sh
```

It starts `approach.target`, fires the panic command, asserts the
target, the daemon, and every shipped unit are down and the admin
socket is unreachable, then resumes and asserts the daemon answers
again. It needs a real systemd user manager (it exits 2 with a SKIP
note elsewhere, e.g. on macOS) and briefly stops the daemon — don't
run it while you depend on the agent being up.

## Adding a timer later

Every `.service`/`.timer` here must carry `PartOf=approach.target`
(in `[Unit]`) and `WantedBy=approach.target` (in `[Install]`) so the
kill switch keeps covering it — `deploy/systemd/units_test.go`
enforces this at build time.
