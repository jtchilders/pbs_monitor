# PBS Monitor: SQLite → PostgreSQL Migration Plan

**Status:** In progress — migration tested on Polaris login-04
**Branch:** `feature/postgres-backend` (created off main)
**Last updated:** 2026-06-04

---

## Goal

Replace SQLite with PostgreSQL as the database backend to support multi-user deployments with concurrent readers and writers.

---

## Deployment Scenarios

### Phase 1 (Near-term): Single login node

Run Postgres on the same Polaris login node as the collector daemon and web server.

```
Polaris login node
├── pbs_monitor collector daemon  (writer)
├── pbs_monitor web server        (reader)
└── PostgreSQL                    (DB server)

ALCF staff → http://polaris-login-XX:PORT
```

**Tradeoffs:**
- Pinned to one login node's disk
- Maintenance = downtime
- Sysadmin awareness needed
- But: familiar ops model, simple to set up

### Phase 2 (Longer-term): External dedicated server

Move Postgres to an external CELS VM/server once one is available. Both Polaris and Aurora collectors write to a single instance.

```
Polaris collector ──┐
                    ├──→ Postgres (external server) ←── Web server (same box)
Aurora collector  ──┘
```

**Advantages over Phase 1:**
- Single source of truth for both systems
- No dependency on any specific login node's disk
- Web server isn't competing with HPC workloads for resources
- Extensible: add ALCF-4 collector without rearchitecting
- Proper backups, monitoring, upgrades on a dedicated machine

---

## Why Postgres (vs. alternatives)

| Option | Verdict |
|--------|---------|
| **Keep SQLite + WAL** | Fine for 1 writer + N readers on the same node. Breaks down for network access or multiple writers. |
| **PostgreSQL** | ✅ Boring-correct choice. Concurrent connections, real transaction isolation, network listener. Taylor already runs it elsewhere. |
| **DuckDB** | Good for analytics queries, but same single-process limits as SQLite. Doesn't solve multi-access. |
| **MySQL/MariaDB** | Lateral move. No advantage over Postgres. |
| **ClickHouse** | Overkill — designed for billions of rows. |
| **TimescaleDB** | Postgres extension, worth revisiting if time-series query perf matters. Premature optimization for now. |
| **Litestream/LiteFS** | Replication for SQLite. Interesting but doesn't help multi-writer case. |

---

## Implementation Plan

### Phase 1 — Migration script

- Finalize `scripts/migrate_sqlite_to_postgres.py` (already drafted)
- Handles: schema creation from SQLAlchemy models, FK-ordered data transfer, JSON coercion, idempotent reruns
- Add `psycopg2` (or `psycopg`) to project dependencies
- Run with:

```bash
# Dry run first (counts only, no writes)
python scripts/migrate_sqlite_to_postgres.py \
  --sqlite "sqlite:////home/parton/pbs_data/polaris_pbs_data.db" \
  --postgres "postgresql://pbs_monitor:PASSWORD@localhost:5432/pbs_monitor" \
  --dry-run

# Real migration
python scripts/migrate_sqlite_to_postgres.py \
  --sqlite "sqlite:////home/parton/pbs_data/polaris_pbs_data.db" \
  --postgres "postgresql://pbs_monitor:PASSWORD@localhost:5432/pbs_monitor"
```

**Key design decisions:**
- **Schema from models** — calls `Base.metadata.create_all()` so it's always in sync with SQLAlchemy models, including PG enum types
- **FK-safe order** — `data_collection_log` first, then jobs/queues/nodes, then history/snapshots
- **Idempotent** — uses `INSERT ... ON CONFLICT DO NOTHING`, safe to rerun if interrupted
- **JSON coercion** — SQLite stores JSON as text; script deserializes before inserting so Postgres gets actual JSON objects
- **`--batch-size 500`** default, tunable for throughput

### Phase 2 — Connection layer changes

- `database/connection.py`:
  - Remove SQLite-specific defaults (`StaticPool`, `check_same_thread`, WAL pragmas)
  - Add Postgres-appropriate pool settings as new defaults (`pool_size`, `max_overflow`, `pool_pre_ping`)
  - Keep SQLite path working for local dev/testing
- `ReadOnlyDatabaseError` — update messaging (SQLite-specific language → generic)

### Phase 3 — Model compatibility

- `database/models.py` — audit for SQLite-isms:
  - `JSON` columns → use `JSONB` on Postgres
  - Enum types — verify SQLAlchemy's `SQLEnum` creates PG enums in correct order
  - `DateTime(timezone=True)` — confirm all timestamps are timezone-aware (Postgres is strict)
  - Any raw SQL using SQLite syntax

### Phase 4 — Repository / query audit

- `database/repositories.py` — check for raw SQL, SQLite functions (`datetime('now')`, `strftime`, etc.)
- `database/migrations.py` — likely needs rework or removal if it has SQLite-specific ALTER TABLE workarounds
- `analytics/*.py` — audit for dialect-specific SQL

### Phase 5 — Configuration

- Update `config.py` default URL and sample config to show Postgres DSN
- Document `PBS_MONITOR_DB_URL` env var for easy switching
- Add a note about `~/.pbs_monitor.yaml` format:

```yaml
database:
  url: postgresql://pbs_monitor:PASSWORD@localhost:5432/pbs_monitor
```

### Phase 6 — Collector daemon adjustments

- `data_collector.py` — SQLite uses single-writer; Postgres handles concurrent writes natively
- Remove any SQLite locking workarounds or retry logic that's no longer needed
- Verify the daemon's session lifecycle works with PG connection pooling

### Phase 7 — Deployment documentation

README or `docs/` section covering:
- Setting up Postgres on a login node
- `createdb`, `createuser`, `pg_hba.conf` for local-only access
- Systemd/cron for the collector daemon
- How co-workers connect (`http://polaris-login-XX:PORT`)

#### ⚠️ Polaris build note: unload XALT before building Postgres

Polaris injects XALT (`libxalt_init.so`) via `LD_PRELOAD` to track software usage. XALT causes a segfault when `postgres -V` is run, which breaks `initdb` before it can do anything. Unload it before running `./configure` and `make`, and keep it unset when running the Postgres daemon:

```bash
module unload xalt   # or: unset LD_PRELOAD / export XALT_EXECUTABLE_TRACKING=no
./configure --prefix=/home/parton/pbs_monitor_dev/venv/ --without-icu
make -j4
make install
```

Keep `XALT_EXECUTABLE_TRACKING=no` (or `unset LD_PRELOAD`) in any script that starts `pg_ctl` or the pbs_monitor daemon.

### Phase 8 — Testing

- Run existing test suite against a local Postgres instance
- Verify the migration script against the real 1.76 GB SQLite DB
- Spot-check row counts, JSON fields, enum values post-migration

---

## Multi-System Support

### Design: PostgreSQL schemas

Each monitored system gets its own Postgres schema within a single `pbs_monitor` database. Tables don't collide; cross-system queries are still possible.

```
pbs_monitor DB
├── aurora.jobs, aurora.nodes, aurora.queues, ...
├── polaris.jobs, polaris.nodes, polaris.queues, ...
└── <future_system>.jobs, <future_system>.nodes, ...
```

Each collector connects with a role whose `search_path` is set to its own schema, so it writes to `jobs` and Postgres routes it to e.g. `aurora.jobs` automatically. Adding a new system = create a new schema, grant the collector role, point a new collector instance at it.

### Configuration

Schema name is a config value each collector reads at startup:

```yaml
database:
  url: postgresql://collector_user:PASSWORD@cels-vm:5432/pbs_monitor
  schema: aurora   # or "polaris", etc.
```

### SQLAlchemy changes

SQLAlchemy supports schema-qualified tables natively via `schema=` on model definitions. The schema name will be injected from config at startup rather than hardcoded — models will be dynamically bound to the configured schema. This is the main code change required beyond the SQLite→Postgres connection layer work.

### Web dashboard

Single unified view with a system selector. The web server queries the selected schema (e.g. `SET search_path TO aurora`) and re-queries on system switch. A system list endpoint will enumerate available schemas so the selector populates dynamically as new systems are added — no hardcoding required.

Cross-system aggregate views (e.g. total jobs running across all systems) are possible via SQL `UNION ALL` across schemas and can be added later.

### Why not alternatives

- **Single table + `system` column** — simpler migration but tables grow large, indexes bloat, easy to forget the filter and get mixed data.
- **Separate databases per system** — complete isolation but cross-system queries require app-level joins and more ops overhead.

---

## Authentication Architecture

### Web users → web server (SSO/OIDC)

The web server handles user authentication. Postgres never sees end users — it only sees the web server's service account. Access control lives in the application layer.

The standard flow:
1. User hits the dashboard
2. Web server redirects to ANL/ALCF identity provider
3. User logs in with their ANL credentials
4. Identity provider returns a token to the web server
5. Web server validates the token and creates a session
6. All DB queries run under a single `web_user` service account

**Identity provider:** ALCF/ANL has its own SSO system — the specific technology (OIDC, SAML, CILogon, Globus Auth, or something internal) needs to be confirmed with ALCF IT or the CELS team. Do not assume Globus Auth. Key questions to answer:
- What is the OIDC/SSO endpoint URL?
- Is there a self-service app registration process, or does IT need to create a client ID/secret?
- Are there existing internal Python libraries or examples for ANL SSO integration?

### Collector daemon → Postgres (long-lived service credential)

OIDC is for interactive human logins — not the right tool for a daemon. Options:

- **Username + password over SSL** — daemon config file with `chmod 600`, Postgres role with INSERT/UPDATE only. Simple, auditable, fine for a trusted internal network.
- **SSL client certificate** — daemon has a cert signed by a CA you control; Postgres verifies at connection time. Stronger than password, no secret to rotate.
- **`pg_hba.conf` trust by IP** — if the collector always comes from a known Polaris login node IP range, Postgres can trust connections from that CIDR without a password. Simplest operationally but less auditable.

**Recommendation:** Username + password over SSL to start. Simple to set up, easy to rotate, works across IP changes if Polaris login nodes aren't pinned.

### DB roles (least privilege)

```sql
-- Collector: write-only to data tables
CREATE ROLE collector_user LOGIN PASSWORD '...';
GRANT INSERT, UPDATE ON data tables TO collector_user;

-- Web server: read-only (or read-write if needed)
CREATE ROLE web_user LOGIN PASSWORD '...';
GRANT SELECT ON ALL TABLES IN SCHEMA public TO web_user;
```

### pg_hba.conf sketch

```
# Collector from Polaris login nodes (ANL internal CIDR)
host  pbs_monitor  collector_user  <polaris_cidr>  scram-sha-256

# Web server on localhost
host  pbs_monitor  web_user        127.0.0.1/32    scram-sha-256
```

### Open auth questions

- What SSO technology does ALCF/ANL use? (OIDC endpoint, provider name, self-service vs. IT-managed registration)
- Are there firewall rules between Polaris login nodes and the CELS VM cluster blocking port 5432?
- Does the CELS VM service allow persistent daemons (Postgres, web server)?
- Any existing internal Python examples for ANL SSO integration in a web app?

---

## Out of Scope (for now)

- No Docker/container deployment — vanilla Postgres on the login node
- No read replicas or rsync — Postgres handles concurrent readers natively
- No schema changes to the models themselves — this is a backend swap
- CLI subcommand for migration — keep as a standalone script for now

---

## Open Questions

- When will an external server be available? That's the gating question for Phase 2.
- Firewall rules: can Polaris/Aurora login nodes reach an external server on the ALCF network?
- Login node sysadmin awareness for running Postgres on shared hardware.

---

## Known Bugs / Tech Debt

### Config file not respected by daemon and web server when run from a non-home directory

**Discovered:** 2026-06-04 during Polaris dev testing

The daemon (`pbs-monitor daemon start`) and web server pick up `~/.pbs_monitor.yaml` even when a local `pbs_monitor.yaml` exists in the current working directory. The documented config search order (current dir first) is not being honored, at least for the daemon and web subcommands.

**Impact:** Running a dev instance in `~/pbs_monitor_dev` alongside a production instance on a different login node is difficult — both end up using the same `~/.pbs_monitor.yaml` and therefore the same database.

**Workaround:** Temporarily edit `~/.pbs_monitor.yaml` to point at the dev DB, or export `PBS_MONITOR_DB_URL` before starting the daemon/web server.

**Proper fix:** 
- Investigate where the daemon resolves config (likely at import time or via a singleton, not from `cwd` at startup)
- Add a `--config` CLI flag to all subcommands that need it (daemon, web)
- Or: support `PBS_MONITOR_CONFIG_FILE` env var as an explicit override
- Ensure the daemon records which config file it loaded in its status output
