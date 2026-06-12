#!/usr/bin/env bash
# status.sh — Show dev daemon, port-forward bridge, web server, and OIDC token status
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

# -- OIDC token lifetime ------------------------------------------------------
# Parse the cached id_token JWT and show expiry. ALCF Keycloak's id_token
# is ~10h and the refresh_token is only 30min, so the id_token TTL is the
# authoritative "how much time do I have left" number.
echo "[ Hermes OIDC token ]"
OIDC_CACHE_DIR="$HOME/.kube/cache/oidc-login"
oidc_cache_file=""
if [[ -d "$OIDC_CACHE_DIR" ]]; then
    # Most-recently modified non-.lock file in the cache dir
    oidc_cache_file=$(find "$OIDC_CACHE_DIR" -maxdepth 1 -type f ! -name '*.lock' \
        -printf '%T@ %p\n' 2>/dev/null | sort -rn | head -1 | awk '{print $2}')
fi
if [[ -z "$oidc_cache_file" || ! -s "$oidc_cache_file" ]]; then
    echo "  Status : NO TOKEN CACHED"
    echo "  Action : ssh -L 18712:localhost:18712 polaris-login-04, then run renew_auth.sh"
else
    python3 - "$oidc_cache_file" <<'PYEOF'
import json, base64, time, sys
cf = sys.argv[1]
try:
    with open(cf) as f:
        d = json.load(f)
    token = d.get("id_token", "")
    parts = token.split(".")
    if len(parts) < 2:
        print("  Status : CACHE PRESENT but unparseable (no JWT in id_token)")
        sys.exit(0)
    payload_b64 = parts[1] + "=" * (-len(parts[1]) % 4)
    payload = json.loads(base64.urlsafe_b64decode(payload_b64))
    exp = int(payload.get("exp", 0))
    now = int(time.time())
    secs = exp - now
    exp_str = time.strftime("%Y-%m-%d %H:%M:%S %Z", time.localtime(exp))
    if secs <= 0:
        print(f"  Status : EXPIRED ({exp_str})")
        print(f"  Action : run renew_auth.sh — daemon will fail at next collection cycle")
    else:
        hours = secs / 3600
        mins = secs / 60
        if hours >= 1:
            lifetime = f"{hours:.1f}h ({int(mins)} min)"
        else:
            lifetime = f"{int(mins)} min"
        status = "OK" if secs > 1800 else "EXPIRING SOON"
        print(f"  Status : {status}")
        print(f"  Expires: {exp_str}")
        print(f"  TTL    : {lifetime}")
        if secs <= 1800:
            print(f"  Hint   : renew before next meeting/break")
except Exception as e:
    print(f"  Status : PARSE ERROR ({e.__class__.__name__}: {e})")
PYEOF
fi
echo ""

# -- port-forward bridge ------------------------------------------------------

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

# Use bash's /dev/tcp pseudo-device — no 'nc' on Polaris login nodes.
if (echo >"/dev/tcp/localhost/$BRIDGE_PORT") 2>/dev/null; then
    echo "  Network: reachable"
else
    echo "  Network: NOT REACHABLE on localhost:$BRIDGE_PORT"
fi
echo "  Log    : $BRIDGE_LOG"
echo ""

# -- dev daemon ---------------------------------------------------------------

echo "[ pbs-monitor dev daemon ]"
if [[ -f "$DAEMON_PID_FILE" ]]; then
    DAEMON_PID=$(_read_daemon_pid "$DAEMON_PID_FILE" || true)
    if [[ -n "${DAEMON_PID:-}" ]] && kill -0 "$DAEMON_PID" 2>/dev/null; then
        echo "  Status : RUNNING (PID $DAEMON_PID)"
        ps_out=$(ps -p "$DAEMON_PID" -o etime= 2>/dev/null || true)
        [[ -n "$ps_out" ]] && echo "  Uptime : $ps_out"
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

# -- web server ---------------------------------------------------------------

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
    echo "  Last 5 daemon log lines:"
    tail -5 "$DAEMON_LOG" | sed 's/^/    /'
fi
echo ""

echo "=== done ==="
