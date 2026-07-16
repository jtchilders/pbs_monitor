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

PIDFILE="$PGDATA/postmaster.pid"

# ---------------------------------------------------------------------------
# is_live_postmaster: return 0 (true) iff a REAL postmaster is currently
# running against $PGDATA. This is the guard that prevents the double-postmaster
# collision that corrupted the WAL (2026-07-16): we must NEVER let pg_ctl
# force-start a second server over a stale pid file
# ("another server might be running; trying to start server anyway").
#
# We trust the process, not just the file: pg_ctl status can be ambiguous, and a
# stale postmaster.pid (left by an immediate/crashed shutdown) names a PID that
# is either dead or has been recycled by an unrelated process. So we:
#   1. If there is no pid file, there is no server -> not live.
#   2. Read the PID from line 1 of postmaster.pid.
#   3. If that PID isn't alive, the file is stale -> not live.
#   4. If it IS alive, confirm the process is actually a postgres/postmaster for
#      THIS data dir (guards against PID recycling by some other program).
# ---------------------------------------------------------------------------
is_live_postmaster() {
    [[ -f "$PIDFILE" ]] || return 1

    local pid
    pid="$(head -n 1 "$PIDFILE" 2>/dev/null | tr -dc '0-9')"
    [[ -n "$pid" ]] || return 1

    # Is that PID alive at all?
    kill -0 "$pid" 2>/dev/null || return 1

    # Alive -- but is it really a postgres postmaster (not a recycled PID)?
    # Match the postgres binary and this exact PGDATA in its command line.
    local cmdline
    cmdline="$(tr '\0' ' ' < "/proc/$pid/cmdline" 2>/dev/null || true)"
    if [[ "$cmdline" == *"postgres"* && "$cmdline" == *"$PGDATA"* ]]; then
        return 0
    fi

    # If /proc is unavailable, fall back to pg_ctl's own view as a last resort.
    if [[ ! -r "/proc/$pid/cmdline" ]] && pg_ctl -D "$PGDATA" status >/dev/null 2>&1; then
        return 0
    fi

    return 1
}

# ---------------------------------------------------------------------------
# Decide what to do based on the ACTUAL running state, never a blind start.
# ---------------------------------------------------------------------------
if is_live_postmaster; then
    echo "Postgres already running for $PGDATA (verified live postmaster)"
else
    # No live server. If a pid file exists here, it is STALE (its PID is dead or
    # not a postmaster for this PGDATA) -- remove it so pg_ctl performs a clean
    # start instead of "trying to start server anyway" over the stale lock.
    if [[ -f "$PIDFILE" ]]; then
        stale_pid="$(head -n 1 "$PIDFILE" 2>/dev/null | tr -dc '0-9' || true)"
        echo "WARNING: stale postmaster.pid found (pid=${stale_pid:-unknown}, not a live postmaster); removing it before start" >&2
        rm -f "$PIDFILE"
    fi

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
