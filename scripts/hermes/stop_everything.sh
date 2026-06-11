#!/usr/bin/env bash
# stop_everything.sh — Stop the dev daemon and optionally the port-forward
#
# Usage:
#   ~/pbs_monitor_dev/bin/stop_everything.sh [--keep-bridge]
#
# Options:
#   --keep-bridge   Stop only the daemon; leave the port-forward running.
#                   Useful when you want to query Hermes manually.

set -euo pipefail

DEV_DIR="${PBS_MONITOR_DEV_DIR:-$HOME/pbs_monitor_dev}"
DEV_VENV="${PBS_MONITOR_DEV_VENV:-$DEV_DIR/venv}"
DEV_CONFIG="${PBS_MONITOR_DEV_CONFIG:-$HOME/.pbs_monitor_dev.yaml}"
RUN_DIR="$DEV_DIR/run"
BRIDGE_PID_FILE="$RUN_DIR/bridge.pid"
DAEMON_PID_FILE="$RUN_DIR/daemon.pid"

# Read daemon PID from JSON-format PID file. Empty if missing/unparseable.
_read_daemon_pid() {
    local pid_file="$1"
    [[ -f "$pid_file" ]] || return 0
    python3 -c "import json, sys
try:
    with open('$pid_file') as f:
        print(json.load(f)['pid'])
except Exception:
    sys.exit(0)" 2>/dev/null
}

KEEP_BRIDGE=false
for arg in "$@"; do
    if [[ "$arg" == "--keep-bridge" ]]; then
        KEEP_BRIDGE=true
    fi
done

# ── stop daemon ───────────────────────────────────────────────────────────────

if [[ -f "$DAEMON_PID_FILE" ]]; then
    DAEMON_PID=$(_read_daemon_pid "$DAEMON_PID_FILE" || true)
    if [[ -n "${DAEMON_PID:-}" ]] && kill -0 "$DAEMON_PID" 2>/dev/null; then
        echo "Stopping dev daemon (PID $DAEMON_PID) via 'pbs-monitor daemon stop'..."
        # Prefer the tool's own stop subcommand — it sets stop_requested in the
        # PID JSON and lets the daemon flush in-flight writes cleanly.
        ACTIVATE="$DEV_VENV/bin/activate"
        if [[ -f "$ACTIVATE" ]]; then
            # shellcheck disable=SC1090
            source "$ACTIVATE"
            pbs-monitor --config "$DEV_CONFIG" daemon stop --pid-file "$DAEMON_PID_FILE" || true
        else
            echo "  (venv missing; falling back to raw kill)"
            kill "$DAEMON_PID" 2>/dev/null || true
        fi
        # Wait up to 10s for the process to exit
        for i in $(seq 1 10); do
            if ! kill -0 "$DAEMON_PID" 2>/dev/null; then
                echo "Daemon stopped."
                break
            fi
            sleep 1
        done
        if kill -0 "$DAEMON_PID" 2>/dev/null; then
            echo "Daemon didn't exit cleanly; sending SIGTERM then SIGKILL..."
            kill -TERM "$DAEMON_PID" 2>/dev/null || true
            sleep 2
            kill -KILL "$DAEMON_PID" 2>/dev/null || true
        fi
    else
        echo "Daemon PID file at $DAEMON_PID_FILE has no running process (stale)."
    fi
    rm -f "$DAEMON_PID_FILE"
else
    echo "No daemon PID file found at $DAEMON_PID_FILE"
fi

# ── stop bridge ───────────────────────────────────────────────────────────────

if [[ "$KEEP_BRIDGE" == "true" ]]; then
    echo "Keeping port-forward bridge running (--keep-bridge)."
else
    if [[ -f "$BRIDGE_PID_FILE" ]]; then
        BRIDGE_PID=$(cat "$BRIDGE_PID_FILE")
        if kill -0 "$BRIDGE_PID" 2>/dev/null; then
            echo "Stopping port-forward bridge (PID $BRIDGE_PID)..."
            kill "$BRIDGE_PID" 2>/dev/null || true
            echo "Bridge stopped."
        else
            echo "Bridge PID $BRIDGE_PID is not running (stale PID file)."
        fi
        rm -f "$BRIDGE_PID_FILE"
    else
        echo "No bridge PID file found at $BRIDGE_PID_FILE"
    fi
fi

echo "Done."
