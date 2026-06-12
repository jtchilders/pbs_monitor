#!/usr/bin/env bash
# start_web.sh — Start the pbs-monitor web dashboard on Polaris, backed by Hermes
#
# The web server connects to Hermes Postgres via the same kubectl port-forward
# the dev daemon uses (localhost:15432). It binds to 127.0.0.1 only — you reach
# it from your laptop with an SSH tunnel:
#
#   ssh -L 8080:localhost:8080 polaris-login-04
#   open http://localhost:8080
#
# Usage:
#   ~/pbs_monitor_dev/bin/start_web.sh
#
# Environment (all optional):
#   PBS_MONITOR_DEV_DIR     Base dir for dev runtime files (default: ~/pbs_monitor_dev)
#   PBS_MONITOR_DEV_VENV    Path to virtualenv (default: ~/pbs_monitor_dev/venv)
#   PBS_MONITOR_DEV_CONFIG  Config file (default: ~/.pbs_monitor_dev.yaml)
#   PBS_MONITOR_WEB_PORT    Local port to bind (default: 8080)
#   PBS_MONITOR_WEB_HOST    Host to bind (default: 127.0.0.1 — loopback only)
#   PBS_MONITOR_BRIDGE_PORT Port-forward port to Hermes (default: 15432)

set -euo pipefail

DEV_DIR="${PBS_MONITOR_DEV_DIR:-$HOME/pbs_monitor_dev}"
DEV_VENV="${PBS_MONITOR_DEV_VENV:-$DEV_DIR/venv}"
DEV_CONFIG="${PBS_MONITOR_DEV_CONFIG:-$HOME/.pbs_monitor_dev.yaml}"
WEB_PORT="${PBS_MONITOR_WEB_PORT:-8080}"
WEB_HOST="${PBS_MONITOR_WEB_HOST:-127.0.0.1}"
BRIDGE_PORT="${PBS_MONITOR_BRIDGE_PORT:-15432}"

LOG_DIR="$DEV_DIR/logs"
RUN_DIR="$DEV_DIR/run"
WEB_LOG="$LOG_DIR/web.log"
WEB_PID_FILE="$RUN_DIR/web.pid"

# ── sanity ────────────────────────────────────────────────────────────────────

if [[ ! -f "$DEV_CONFIG" ]]; then
    echo "ERROR: Dev config not found: $DEV_CONFIG"
    exit 1
fi

config_perms=$(stat -c "%a" "$DEV_CONFIG" 2>/dev/null || stat -f "%OLp" "$DEV_CONFIG" 2>/dev/null)
if [[ "$config_perms" != "600" ]]; then
    echo "ERROR: $DEV_CONFIG must be chmod 600 (has $config_perms)."
    exit 1
fi

mkdir -p "$LOG_DIR" "$RUN_DIR"

# ── verify Hermes bridge is up (the web server needs the same port-forward) ──

if ! (echo >"/dev/tcp/localhost/$BRIDGE_PORT") 2>/dev/null; then
    echo "ERROR: Hermes port-forward not reachable on localhost:$BRIDGE_PORT"
    echo "       Start it first with: ~/pbs_monitor_dev/bin/start_daemon.sh"
    echo "       (start_daemon.sh handles the bridge as well as the daemon.)"
    exit 1
fi

# ── already running? ─────────────────────────────────────────────────────────

if [[ -f "$WEB_PID_FILE" ]]; then
    WEB_PID=$(cat "$WEB_PID_FILE")
    if kill -0 "$WEB_PID" 2>/dev/null; then
        echo "Web server already running (PID $WEB_PID) on $WEB_HOST:$WEB_PORT"
        echo ""
        echo "Tunnel from your laptop:"
        echo "  ssh -L $WEB_PORT:localhost:$WEB_PORT $(hostname -s 2>/dev/null || hostname)"
        echo "  open http://localhost:$WEB_PORT"
        exit 0
    else
        echo "Stale web PID file ($WEB_PID); cleaning up..."
        rm -f "$WEB_PID_FILE"
    fi
fi

# ── start ────────────────────────────────────────────────────────────────────

ACTIVATE="$DEV_VENV/bin/activate"
if [[ ! -f "$ACTIVATE" ]]; then
    echo "ERROR: Virtualenv not found at $DEV_VENV"
    exit 1
fi

# shellcheck disable=SC1090
source "$ACTIVATE"

echo "Starting pbs-monitor web server on $WEB_HOST:$WEB_PORT..."

# pbs-monitor web is a foreground command (uvicorn.run blocks), so we wrap it
# with nohup + & + disown for survive-logout. PID written to our dev run dir.
PBS_MONITOR_CONFIG="$DEV_CONFIG" \
nohup pbs-monitor --config "$DEV_CONFIG" web \
    --host "$WEB_HOST" \
    --port "$WEB_PORT" \
    --no-browser \
    >> "$WEB_LOG" 2>&1 &
WEB_PID=$!
disown "$WEB_PID"
echo "$WEB_PID" > "$WEB_PID_FILE"

# Wait briefly and verify it actually came up
sleep 2
if ! kill -0 "$WEB_PID" 2>/dev/null; then
    echo "ERROR: Web server died immediately. Recent log:"
    tail -20 "$WEB_LOG" 2>/dev/null | sed 's/^/  /'
    rm -f "$WEB_PID_FILE"
    exit 1
fi

if ! (echo >"/dev/tcp/$WEB_HOST/$WEB_PORT") 2>/dev/null; then
    echo "WARNING: Process is alive (PID $WEB_PID) but not listening on $WEB_HOST:$WEB_PORT yet."
    echo "         Give it a few more seconds, then check: $WEB_LOG"
else
    echo "Web server up (PID $WEB_PID, log: $WEB_LOG)"
fi

echo ""
echo "From your laptop:"
echo "  ssh -L $WEB_PORT:localhost:$WEB_PORT $(hostname -s 2>/dev/null || hostname)"
echo "  open http://localhost:$WEB_PORT"
echo ""
echo "Tail the log with: tail -f $WEB_LOG"
echo "Stop with:         $DEV_DIR/bin/stop_web.sh"
