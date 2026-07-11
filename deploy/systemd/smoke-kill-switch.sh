#!/usr/bin/env bash
# smoke-kill-switch.sh — §7 kill-switch drill for a real (Linux) host.
#
# Proves the single documented panic command actually kills everything
# and that resume brings it back:
#   1. start approach.target, wait until the daemon answers status
#   2. panic: systemctl --user disable --now approach.target
#   3. assert the target, the daemon, and every shipped unit are down,
#      the admin socket is unreachable, and the socket file is gone
#   4. resume: enable --now, wait until status answers again
#
# Run it on the host after installing the units (see README.md).
# It exercises the REAL systemd user manager: do not run it while you
# depend on the daemon being up.
set -euo pipefail

UNIT_DIR="$(cd "$(dirname "$0")" && pwd)"
APPROACH_BIN="${APPROACH_BIN:-$HOME/bin/approach}"

# After the panic phase begins, neither a failed assertion nor an
# interrupt (Ctrl-C, TERM, HUP) may strand the daemon disabled: cleanup
# runs on EXIT — which set -e failures and the trapped signals all
# reach — re-enables the target when needed, and preserves the exit
# status. ERR alone would miss signals.
resume_needed=0
step=""
cleanup() {
    status=$?
    if [ "$resume_needed" = 1 ]; then
        echo "restoring approach.target after failed/interrupted drill" >&2
        systemctl --user enable --now approach.target || true
    fi
    if [ "$status" -ne 0 ] && [ -n "$step" ]; then
        echo "FAIL at step: $step" >&2
    fi
    exit "$status"
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM
trap 'exit 129' HUP

wait_status() {
    local want="$1"
    for _ in $(seq 1 50); do
        if [ "$want" = up ]; then
            "$APPROACH_BIN" status --socket "$SOCKET" >/dev/null 2>&1 && return 0
        else
            "$APPROACH_BIN" status --socket "$SOCKET" >/dev/null 2>&1 || return 0
        fi
        sleep 0.2
    done
    return 1
}

# Reachability, not health: is-system-running exits nonzero for a
# reachable-but-degraded manager (any failed user unit), and a drill
# that silently skips there never exercises the kill switch. Only a
# genuinely unreachable manager skips.
step="preflight: systemd user manager reachable"
manager_state="$(systemctl --user is-system-running 2>/dev/null || true)"
case "$manager_state" in
""|offline|unknown)
    echo "SKIP: no reachable systemd user manager (state: ${manager_state:-none}) — run this on the Linux host" >&2
    step=""
    exit 2
    ;;
esac

step="preflight: units installed"
systemctl --user cat approach.target approach.service >/dev/null

# The daemon spawned by systemd resolves its state dir from the USER
# MANAGER's environment (internal/cli defaultStateDir: $APPROACH_HOME/state,
# default ~/approach/state) — not from this shell. Probing a path from
# the caller's environment could fail against a healthy service, so the
# manager's own APPROACH_HOME is authoritative here.
step="derive state dir from the user manager's environment"
manager_home="$(systemctl --user show-environment 2>/dev/null | sed -n 's/^APPROACH_HOME=//p')"
STATE_DIR="${manager_home:-$HOME/approach}/state"
SOCKET="$STATE_DIR/approach.sock"

step="start approach.target"
systemctl --user enable --now approach.target

step="daemon answers status after start"
wait_status up

step="panic: disable --now approach.target"
resume_needed=1
systemctl --user disable --now approach.target

step="target inactive after panic"
if systemctl --user is-active --quiet approach.target; then false; fi

step="every shipped unit inactive after panic"
for unit in "$UNIT_DIR"/*.service "$UNIT_DIR"/*.timer; do
    [ -e "$unit" ] || continue
    name="$(basename "$unit")"
    if systemctl --user is-active --quiet "$name"; then
        echo "unit still active after panic: $name" >&2
        false
    fi
done

step="daemon unreachable after panic"
wait_status down

step="socket file removed after drain"
if [ -e "$SOCKET" ]; then false; fi

step="resume: enable --now approach.target"
systemctl --user enable --now approach.target

step="daemon answers status after resume"
wait_status up
resume_needed=0

echo "OK: kill switch stops everything and resume restores it"
