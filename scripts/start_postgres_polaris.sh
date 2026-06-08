#!/usr/bin/env bash
set -euo pipefail

INSTALL_PATH="${1:?Usage: $0 <install_path>}"
PGDATA="${2:-/lus/eagle/projects/datascience/parton/pbs-monitor-content/polaris/pgdata}"

echo "Using INSTALL_PATH=$INSTALL_PATH"
echo "Using PGDATA=$PGDATA"

# Switch to GNU environment; unload profiling tools that conflict with venv
module switch PrgEnv-nvidia/8.6.0 PrgEnv-gnu
module unload xalt darshan

VENV="$INSTALL_PATH/venv"
if [[ ! -f "$VENV/bin/activate" ]]; then
    echo "ERROR: venv not found at $VENV" >&2
    exit 1
fi

source "$VENV/bin/activate"
export LD_LIBRARY_PATH="$VENV/lib${LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH}"

if ! pg_ctl -D "$PGDATA" start; then
    echo "ERROR: pg_ctl failed to start" >&2
    exit 1
fi
