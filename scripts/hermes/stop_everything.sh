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
RUN_DIR="$DEV_DIR/run"
BRIDGE_PID_FILE="$RUN_DIR/bridge.pid"
DAEMON_PID_FILE="$RUN_DIR/daemon.pid"

KEEP_BRIDGE=false
for arg in "$@"; do
    if [[ "$arg" == "--keep-bridge" ]]; then
        KEEP_BRIDGE=true
    fi
done

# ── stop daemon ───────────────────────────────────────────────────────────────

if [[ -f "$DAEMON_PID_FILE" ]]; then
    DAEMON_PID=$(cat "$DAEMON_PID_FILE")
    if kill -0 "$DAEMON_PID" 2>/dev/null; then
        echo "Stopping dev daemon (PID $DAEMON_PID)..."
        kill "$DAEMON_PID"
        # Give it a few seconds to flush and exit cleanly
        for i in $(seq 1 10); do
            if ! kill -0 "$DAEMON_PID" 2>/dev/null; then
                echo "Daemon stopped."
                break
            fi
            sleep 1
        done
        if kill -0 "$DAEMON_PID" 2>/dev/null; then
            echo "Daemon didn't exit cleanly; sending SIGKILL..."
            kill -9 "$DAEMON_PID" 2>/dev/null || true
        fi
    else
        echo "Daemon PID $DAEMON_PID is not running (stale PID file)."
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
