#!/usr/bin/env bash
# stop_web.sh — Stop the pbs-monitor web dashboard
#
# Usage:
#   ~/pbs_monitor_dev/bin/stop_web.sh

set -euo pipefail

DEV_DIR="${PBS_MONITOR_DEV_DIR:-$HOME/pbs_monitor_dev}"
RUN_DIR="$DEV_DIR/run"
WEB_PID_FILE="$RUN_DIR/web.pid"

if [[ ! -f "$WEB_PID_FILE" ]]; then
    echo "No web PID file at $WEB_PID_FILE — nothing to stop."
    exit 0
fi

WEB_PID=$(cat "$WEB_PID_FILE")
if ! kill -0 "$WEB_PID" 2>/dev/null; then
    echo "Web PID $WEB_PID not running (stale PID file). Cleaning up."
    rm -f "$WEB_PID_FILE"
    exit 0
fi

echo "Stopping web server (PID $WEB_PID)..."
kill "$WEB_PID"

# Wait up to 5s for clean exit
for i in $(seq 1 5); do
    if ! kill -0 "$WEB_PID" 2>/dev/null; then
        echo "Web server stopped."
        rm -f "$WEB_PID_FILE"
        exit 0
    fi
    sleep 1
done

echo "Web server didn't exit cleanly; sending SIGKILL..."
kill -9 "$WEB_PID" 2>/dev/null || true
rm -f "$WEB_PID_FILE"
echo "Done."
