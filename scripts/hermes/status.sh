#!/usr/bin/env bash
# status.sh — Show dev daemon and port-forward bridge status
#
# Usage:
#   ~/pbs_monitor_dev/bin/status.sh

set -euo pipefail

DEV_DIR="${PBS_MONITOR_DEV_DIR:-$HOME/pbs_monitor_dev}"
BRIDGE_PORT="${PBS_MONITOR_BRIDGE_PORT:-15432}"
WEB_PORT="${PBS_MONITOR_WEB_PORT:-8080}"
WEB_HOST="${PBS_MONITOR_WEB_HOST:-127.0.0.1}"
RUN_DIR="$DEV_DIR/run"
LOG_DIR="$DEV_DIR/logs"
BRIDGE_PID_FILE="$RUN_DIR/bridge.pid"
DAEMON_PID_FILE="$RUN_DIR/daemon.pid"
WEB_PID_FILE="$RUN_DIR/web.pid"
DAEMON_LOG="$LOG_DIR/daemon.log"
BRIDGE_LOG="$LOG_DIR/bridge.log"
WEB_LOG="$LOG_DIR/web.log"

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

echo "=== pbs-monitor dev status ==="
echo ""

# ── port-forward bridge ───────────────────────────────────────────────────────

echo "[ Hermes port-forward (localhost:$BRIDGE_PORT) ]"
if [[ -f "$BRIDGE_PID_FILE" ]]; then
    BRIDGE_PID=$(cat "$BRIDGE_PID_FILE")
    if kill -0 "$BRIDGE_PID" 2>/dev/null; then
        echo "  Status : RUNNING (PID $BRIDGE_PID)"
    else
        echo "  Status : DEAD (stale PID $BRIDGE_PID — run renew_auth.sh)"
    fi
else
    echo "  Status : NOT STARTED (no PID file)"
fi

# Use bash's /dev/tcp pseudo-device so we don't depend on 'nc' being installed
# (it isn't on Polaris login nodes).
if (echo >"/dev/tcp/localhost/$BRIDGE_PORT") 2>/dev/null; then
    echo "  Network: reachable"
else
    echo "  Network: NOT REACHABLE on localhost:$BRIDGE_PORT"
fi
echo "  Log    : $BRIDGE_LOG"
echo ""

# ── dev daemon ────────────────────────────────────────────────────────────────

echo "[ pbs-monitor dev daemon ]"
if [[ -f "$DAEMON_PID_FILE" ]]; then
    DAEMON_PID=$(_read_daemon_pid "$DAEMON_PID_FILE" || true)
    if [[ -n "${DAEMON_PID:-}" ]] && kill -0 "$DAEMON_PID" 2>/dev/null; then
        echo "  Status : RUNNING (PID $DAEMON_PID)"
        ps_out=$(ps -p "$DAEMON_PID" -o etime= 2>/dev/null || true)
        [[ -n "$ps_out" ]] && echo "  Uptime : $ps_out"
        # Daemon heartbeat from its PID file (JSON)
        hb=$(python3 -c "import json
try:
    with open('$DAEMON_PID_FILE') as f: print(json.load(f).get('heartbeat', ''))
except Exception: pass" 2>/dev/null)
        [[ -n "$hb" ]] && echo "  Heartbeat: $hb"
    else
        echo "  Status : DEAD (stale PID file — run start_daemon.sh to restart)"
    fi
else
    echo "  Status : NOT STARTED (no PID file)"
fi
echo "  Log    : $DAEMON_LOG"
echo ""

# ── web server ─────────────────────────────────────────────────────────────
host=$(hostname -s 2>/dev/null || hostname)
echo "[ pbs-monitor web dashboard ($WEB_HOST:$WEB_PORT) ]"
if [[ -f "$WEB_PID_FILE" ]]; then
    WEB_PID=$(cat "$WEB_PID_FILE")
    if kill -0 "$WEB_PID" 2>/dev/null; then
        echo "  Status : RUNNING (PID $WEB_PID)"
        ps_out=$(ps -p "$WEB_PID" -o etime= 2>/dev/null || true)
        [[ -n "$ps_out" ]] && echo "  Uptime : $ps_out"
        if (echo >"/dev/tcp/$WEB_HOST/$WEB_PORT") 2>/dev/null; then
            echo "  Network: listening on $WEB_HOST:$WEB_PORT"
        else
            echo "  Network: NOT LISTENING on $WEB_HOST:$WEB_PORT (warming up?)"
        fi
        echo "  Tunnel : ssh -L $WEB_PORT:localhost:$WEB_PORT $host  # then http://localhost:$WEB_PORT"
    else
        echo "  Status : DEAD (stale PID file — run start_web.sh to restart)"
    fi
else
    echo "  Status : NOT STARTED (no PID file)"
fi
echo "  Log    : $WEB_LOG"

if [[ -f "$DAEMON_LOG" ]]; then
    echo ""
    echo "  Last 5 log lines:"
    tail -5 "$DAEMON_LOG" | sed 's/^/    /'
fi
echo ""

# ── quick Hermes row count check ──────────────────────────────────────────────

DEV_CONFIG="${PBS_MONITOR_DEV_CONFIG:-$HOME/.pbs_monitor_dev.yaml}"
if [[ -f "$DEV_CONFIG" ]] && nc -z localhost "$BRIDGE_PORT" 2>/dev/null; then
    # Extract DB URL from config (simple grep; works for standard yaml layout)
    DB_URL=$(grep -E '^\s*url:' "$DEV_CONFIG" | head -1 | awk '{print $2}' 2>/dev/null || true)
    if [[ -n "$DB_URL" ]]; then
        echo "[ Hermes DB row counts ]"
        SYSTEM=$(grep -E '^\s*system:' "$DEV_CONFIG" | head -1 | awk '{print $2}' 2>/dev/null || echo "unknown")
        psql "$DB_URL" -t -c "
            SELECT 'jobs', count(*) FROM jobs WHERE system='$SYSTEM'
            UNION ALL SELECT 'job_history', count(*) FROM job_history WHERE system='$SYSTEM'
            UNION ALL SELECT 'nodes', count(*) FROM nodes WHERE system='$SYSTEM'
            UNION ALL SELECT 'system_snapshots', count(*) FROM system_snapshots WHERE system='$SYSTEM'
        " 2>/dev/null | sed 's/^/  /' || echo "  (psql query failed — check DB_URL in config)"
        echo ""
    fi
fi

echo "=== done ==="
