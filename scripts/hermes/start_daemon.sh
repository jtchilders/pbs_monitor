#!/usr/bin/env bash
# start_daemon.sh — Start the pbs-monitor dev collector daemon on Polaris
#
# This script:
#   1. Ensures a kubectl port-forward to Hermes Postgres is running
#   2. Starts the pbs-monitor daemon in the background (logs to dev log dir)
#
# Usage:
#   ~/pbs_monitor_dev/bin/start_daemon.sh
#
# Prerequisites:
#   - kubectl and the kubectl-oidc_login plugin must be on PATH
#   - ~/.pbs_monitor_dev.yaml must exist and be chmod 600
#   - $PBS_MONITOR_DEV_DIR must be set, or defaults to ~/pbs_monitor_dev
#   - pbs-monitor must be installed in the same venv as the prod daemon
#     OR in a dedicated dev venv specified by PBS_MONITOR_DEV_VENV
#
# Environment variables (all optional, sensible defaults):
#   PBS_MONITOR_DEV_DIR    Base dir for dev runtime files (default: ~/pbs_monitor_dev)
#   PBS_MONITOR_DEV_VENV   Path to virtualenv (default: ~/pbs_monitor_dev/venv)
#   PBS_MONITOR_DEV_CONFIG Config file (default: ~/.pbs_monitor_dev.yaml)
#   PBS_MONITOR_BRIDGE_PORT Local port for Hermes Postgres (default: 15432)

set -euo pipefail

DEV_DIR="${PBS_MONITOR_DEV_DIR:-$HOME/pbs_monitor_dev}"
DEV_VENV="${PBS_MONITOR_DEV_VENV:-$DEV_DIR/venv}"
DEV_CONFIG="${PBS_MONITOR_DEV_CONFIG:-$HOME/.pbs_monitor_dev.yaml}"
BRIDGE_PORT="${PBS_MONITOR_BRIDGE_PORT:-15432}"

LOG_DIR="$DEV_DIR/logs"
RUN_DIR="$DEV_DIR/run"
BRIDGE_LOG="$LOG_DIR/bridge.log"
DAEMON_LOG="$LOG_DIR/daemon.log"
BRIDGE_PID_FILE="$RUN_DIR/bridge.pid"
DAEMON_PID_FILE="$RUN_DIR/daemon.pid"

# ── sanity checks ─────────────────────────────────────────────────────────────

if [[ ! -f "$DEV_CONFIG" ]]; then
    echo "ERROR: Dev config not found: $DEV_CONFIG"
    echo "Create it (copy from prod, add pbs.system: polaris, update database.url), then chmod 600."
    exit 1
fi

config_perms=$(stat -c "%a" "$DEV_CONFIG" 2>/dev/null || stat -f "%OLp" "$DEV_CONFIG" 2>/dev/null)
if [[ "$config_perms" != "600" ]]; then
    echo "ERROR: $DEV_CONFIG must be chmod 600 (has $config_perms)."
    echo "Run: chmod 600 $DEV_CONFIG"
    exit 1
fi

mkdir -p "$LOG_DIR" "$RUN_DIR"

# ── kubectl port-forward ───────────────────────────────────────────────

if [[ -f "$BRIDGE_PID_FILE" ]]; then
    BRIDGE_PID=$(cat "$BRIDGE_PID_FILE")
    if kill -0 "$BRIDGE_PID" 2>/dev/null; then
        echo "Port-forward already running (PID $BRIDGE_PID) on localhost:$BRIDGE_PORT"
    else
        echo "Stale bridge PID ($BRIDGE_PID); restarting port-forward..."
        rm -f "$BRIDGE_PID_FILE"
    fi
fi

if [[ ! -f "$BRIDGE_PID_FILE" ]]; then
    echo "Starting kubectl port-forward (localhost:$BRIDGE_PORT → Hermes Postgres)..."
    kubectl port-forward -n pbs-monitor svc/pbs-postgres "$BRIDGE_PORT:5432" \
        >> "$BRIDGE_LOG" 2>&1 &
    BRIDGE_PID=$!
    disown "$BRIDGE_PID"
    echo "$BRIDGE_PID" > "$BRIDGE_PID_FILE"
    echo "Bridge PID: $BRIDGE_PID (log: $BRIDGE_LOG)"
    sleep 2  # give it a moment to establish
fi

# Quick connectivity check
if ! nc -z localhost "$BRIDGE_PORT" 2>/dev/null; then
    echo "WARNING: Cannot reach localhost:$BRIDGE_PORT — port-forward may not be ready yet."
    echo "If this is the first OIDC login, a browser window may open for authentication."
    echo "Wait a few seconds then re-run this script if the daemon fails to connect."
fi

# ── pbs-monitor dev daemon ────────────────────────────────────────────────────

if [[ -f "$DAEMON_PID_FILE" ]]; then
    DAEMON_PID=$(cat "$DAEMON_PID_FILE")
    if kill -0 "$DAEMON_PID" 2>/dev/null; then
        echo "Dev daemon already running (PID $DAEMON_PID)"
        exit 0
    else
        echo "Stale daemon PID ($DAEMON_PID); cleaning up..."
        rm -f "$DAEMON_PID_FILE"
    fi
fi

echo "Starting pbs-monitor dev daemon..."

ACTIVATE="$DEV_VENV/bin/activate"
if [[ ! -f "$ACTIVATE" ]]; then
    echo "ERROR: Virtualenv not found at $DEV_VENV"
    echo "Set PBS_MONITOR_DEV_VENV or create: python -m venv $DEV_VENV && source $ACTIVATE && pip install -e ."
    exit 1
fi

# shellcheck disable=SC1090
source "$ACTIVATE"

PBS_MONITOR_CONFIG="$DEV_CONFIG" \
nohup pbs-monitor collect \
    --config "$DEV_CONFIG" \
    >> "$DAEMON_LOG" 2>&1 &
DAEMON_PID=$!
disown "$DAEMON_PID"
echo "$DAEMON_PID" > "$DAEMON_PID_FILE"
echo "Dev daemon PID: $DAEMON_PID (log: $DAEMON_LOG)"
echo ""
echo "Tail the log with: tail -f $DAEMON_LOG"
echo "Check status with: $DEV_DIR/bin/status.sh"
