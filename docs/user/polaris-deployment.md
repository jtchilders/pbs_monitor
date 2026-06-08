# PBS Monitor: Polaris Deployment Guide

This guide covers building and running PBS Monitor with a PostgreSQL backend on a Polaris login node. It assumes you are deploying to a user home directory without root/sysadmin access.

---

## Prerequisites

- Python 3.12+ available (e.g. via module or existing install)
- A writable directory for the dev/prod tree (e.g. `~/pbs_monitor_dev`)
- Access to a Polaris login node

---

## Step 1 — Clone and set up the Python venv

```bash
mkdir ~/pbs_monitor_dev
cd ~/pbs_monitor_dev
git clone https://github.com/jtchilders/pbs_monitor.git .
python3.12 -m venv venv
source venv/bin/activate
pip install -e ".[postgres]"
```

---

## Step 2 — Build PostgreSQL from source

Polaris does not provide a usable system Postgres. You need to build it yourself into the venv prefix.

### ⚠️ Unload XALT first

Polaris injects XALT (`libxalt_init.so`) via `LD_PRELOAD` to track software usage. XALT causes a segfault in the `postgres` binary — even on `postgres -V` — which breaks `initdb` before it can do anything. **Unload it before building and before running any Postgres commands.**

```bash
module unload xalt
# Or if module is not available:
export XALT_EXECUTABLE_TRACKING=no
unset LD_PRELOAD
```

### Download and build

```bash
cd ~/pbs_monitor_dev
wget https://ftp.postgresql.org/pub/source/v18.4/postgresql-18.4.tar.gz
tar xf postgresql-18.4.tar.gz
cd postgresql-18.4

# --without-icu: Polaris lacks the ICU libraries; not needed for ASCII PBS data
./configure --prefix=/home/parton/pbs_monitor_dev/venv/ --without-icu
make -j4
make install
cd ..
```

Add the venv bin to PATH (add this to your startup script too):

```bash
export PATH=/home/parton/pbs_monitor_dev/venv/bin:$PATH
```

---

## Step 3 — Initialize and start PostgreSQL

```bash
# Initialize a data directory contained within the dev folder
initdb -D ~/pbs_monitor_dev/pgdata

# Start Postgres in the background (no separate terminal needed)
# Keep XALT unset in any script that calls pg_ctl
pg_ctl -D ~/pbs_monitor_dev/pgdata -l ~/pbs_monitor_dev/pgdata/postgres.log start

# Verify
pg_ctl -D ~/pbs_monitor_dev/pgdata status

# Create the database
createdb pbs_monitor_dev
```

If port 5432 is already in use on the login node, edit `pgdata/postgresql.conf` before starting:
```
port = 5433
```

To stop Postgres later:
```bash
pg_ctl -D ~/pbs_monitor_dev/pgdata stop
```

---

## Step 4 — Migrate SQLite data to PostgreSQL

```bash
# Dry run first — counts only, no writes
python scripts/migrate_sqlite_to_postgres.py \
  --sqlite "sqlite:////home/parton/pbs_data/polaris_pbs_data.db" \
  --postgres "postgresql://localhost/pbs_monitor_dev" \
  --dry-run

# Real migration
python scripts/migrate_sqlite_to_postgres.py \
  --sqlite "sqlite:////home/parton/pbs_data/polaris_pbs_data.db" \
  --postgres "postgresql://localhost/pbs_monitor_dev"
```

---

## Step 5 — Configure pbs_monitor to use PostgreSQL

To keep dev config isolated from any `~/.pbs_monitor*` files used by a production instance on another login node, place config inside the dev folder:

```bash
cat > ~/pbs_monitor_dev/pbs_monitor.yaml << 'EOF'
database:
  url: "postgresql://localhost/pbs_monitor_dev"
EOF
```

The app searches for `pbs_monitor.yaml` in the current directory first, so run all commands from `~/pbs_monitor_dev`. Alternatively, export the env var:

```bash
export PBS_MONITOR_DB_URL="postgresql://localhost/pbs_monitor_dev"
```

This overrides any config file and prevents touching the production SQLite DB.

---

## Step 6 — Test the collector daemon

```bash
cd ~/pbs_monitor_dev
source venv/bin/activate
export XALT_EXECUTABLE_TRACKING=no  # keep XALT out of the way

# Start daemon (reads pbs_monitor.yaml in current dir → dev Postgres)
pbs-monitor daemon start

# Check it's running and writing
pbs-monitor daemon status
pbs-monitor database status
```

---

## Step 7 — Test the web server

Run on a different port from production (prod uses 9998):

```bash
python -m pbs_monitor.cli.main web --host 127.0.0.1 --port 9999
```

---

## Isolation summary

| Thing | Production | Dev |
|---|---|---|
| Login node | polaris-login-03 (or whichever) | polaris-login-04 |
| Config | `~/.pbs_monitor.yaml` | `~/pbs_monitor_dev/pbs_monitor.yaml` |
| Database | SQLite `~/pbs_data/polaris_pbs_data.db` | Postgres `pbs_monitor_dev` |
| Postgres data | n/a | `~/pbs_monitor_dev/pgdata/` |
| Web port | 9998 | 9999 |
| Daemon PID/logs | `~/.pbs_monitor*` | `~/pbs_monitor_dev/` (verify in config) |

---

## Startup script

Save as `~/pbs_monitor_dev/start-dev.sh`:

```bash
#!/bin/bash
set -e

DEV_DIR=/home/parton/pbs_monitor_dev

# XALT segfaults postgres — keep it out
module unload xalt 2>/dev/null || true
export XALT_EXECUTABLE_TRACKING=no
unset LD_PRELOAD

export PATH=$DEV_DIR/venv/bin:$PATH
source $DEV_DIR/venv/bin/activate

# Start Postgres if not already running
pg_ctl -D $DEV_DIR/pgdata status || \
  pg_ctl -D $DEV_DIR/pgdata -l $DEV_DIR/pgdata/postgres.log start

cd $DEV_DIR
echo "Postgres up. Run 'pbs-monitor daemon start' to start the collector."
```

```bash
chmod +x ~/pbs_monitor_dev/start-dev.sh
```
