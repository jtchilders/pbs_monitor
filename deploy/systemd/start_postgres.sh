#!/usr/bin/env bash
# Idempotent, wait-for-ready Postgres starter for pbs_monitor.
# Safe to call repeatedly (systemd ExecStart / watchdog / by hand).
set -euo pipefail

INSTALL_PATH="${1:-$HOME/pbs_monitor}"
PGDATA="${2:-/lus/flare/projects/datascience/parton/pbs-monitor-content/aurora/pgdata}"

echo "Using INSTALL_PATH=$INSTALL_PATH"
echo "Using PGDATA=$PGDATA"

VENV="$INSTALL_PATH/venv"
if [[ ! -f "$VENV/bin/activate" ]]; then
    echo "ERROR: venv not found at $VENV" >&2
    exit 1
fi

# shellcheck disable=SC1091
source "$VENV/bin/activate"
export LD_LIBRARY_PATH="$VENV/lib${LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH}"

# Idempotent: if already up, we're done (exit 0 so systemd Type=forking is happy).
if pg_ctl -D "$PGDATA" status >/dev/null 2>&1; then
    echo "Postgres already running for $PGDATA"
else
    # -w waits for readiness; -t 90 tolerates crash recovery after a maintenance kill;
    # -l keeps a log so a failed recovery leaves a trace.
    if ! pg_ctl -D "$PGDATA" -w -t 90 -l "$PGDATA/startup.log" start; then
        echo "ERROR: pg_ctl failed to start; see $PGDATA/startup.log" >&2
        exit 1
    fi
fi

# Real readiness probe (pg_ctl -w can be optimistic under crash recovery).
for _ in $(seq 1 30); do
    if pg_isready -q; then
        echo "Postgres is accepting connections"
        exit 0
    fi
    sleep 2
done

echo "ERROR: Postgres did not become ready within 60s" >&2
exit 1
