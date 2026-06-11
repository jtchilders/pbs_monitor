#!/usr/bin/env bash
# renew_auth.sh — Renew the Hermes OIDC token and restart the port-forward
#
# The Hermes OIDC token has a ~13h lifetime. When it expires, the
# port-forward dies silently and the dev daemon can no longer write to
# Hermes Postgres. Run this script to renew the token and restore the bridge.
#
# Usage:
#   ~/pbs_monitor_dev/bin/renew_auth.sh
#
# After renewal, the dev daemon does NOT need to be restarted — it will
# reconnect to Postgres via the restored port-forward on the next collection
# cycle (SQLAlchemy pool_pre_ping will recover the connection automatically).

set -euo pipefail

DEV_DIR="${PBS_MONITOR_DEV_DIR:-$HOME/pbs_monitor_dev}"
BRIDGE_PORT="${PBS_MONITOR_BRIDGE_PORT:-15432}"
LOG_DIR="$DEV_DIR/logs"
RUN_DIR="$DEV_DIR/run"
BRIDGE_LOG="$LOG_DIR/bridge.log"
BRIDGE_PID_FILE="$RUN_DIR/bridge.pid"

mkdir -p "$LOG_DIR" "$RUN_DIR"

# ── 1. Renew the OIDC token ───────────────────────────────────────────────────

echo "Renewing Hermes OIDC token..."
echo "(A browser window may open if the token cannot be refreshed silently.)"
hermes-kubectl oidc-login get-token \
    --oidc-issuer-url="https://keycloak.alcf.anl.gov/realms/hermes" \
    --oidc-client-id=hermes \
    --grant-type=authcode \
    --listen-address=localhost:8000 || {
    echo "ERROR: oidc-login failed. Check that the browser is accessible and retry."
    exit 1
}
echo "Token renewed."

# ── 2. Kill stale port-forward ────────────────────────────────────────────────

if [[ -f "$BRIDGE_PID_FILE" ]]; then
    OLD_PID=$(cat "$BRIDGE_PID_FILE")
    if kill -0 "$OLD_PID" 2>/dev/null; then
        echo "Stopping stale port-forward (PID $OLD_PID)..."
        kill "$OLD_PID" 2>/dev/null || true
        sleep 1
    fi
    rm -f "$BRIDGE_PID_FILE"
fi

# ── 3. Start new port-forward ─────────────────────────────────────────────────

echo "Starting fresh port-forward on localhost:$BRIDGE_PORT..."
hermes-kubectl port-forward -n pbs-monitor svc/pbs-postgres "$BRIDGE_PORT:5432" \
    >> "$BRIDGE_LOG" 2>&1 &
BRIDGE_PID=$!
disown "$BRIDGE_PID"
echo "$BRIDGE_PID" > "$BRIDGE_PID_FILE"
sleep 2

# ── 4. Verify ─────────────────────────────────────────────────────────────────

if nc -z localhost "$BRIDGE_PORT" 2>/dev/null; then
    echo "Port-forward live on localhost:$BRIDGE_PORT (PID $BRIDGE_PID)"
    echo ""
    echo "Dev daemon will reconnect automatically on next collection cycle."
    echo "Check daemon status with: $DEV_DIR/bin/status.sh"
else
    echo "WARNING: localhost:$BRIDGE_PORT not reachable yet. Check $BRIDGE_LOG"
    exit 1
fi
