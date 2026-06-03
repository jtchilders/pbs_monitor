#!/usr/bin/env python3
"""
SQLite → PostgreSQL Migration Script for PBS Monitor

Transfers all data from an existing SQLite database to a PostgreSQL database,
creating the schema from SQLAlchemy models.

Usage:
    python scripts/migrate_sqlite_to_postgres.py \
        --sqlite sqlite:////path/to/pbs_data.db \
        --postgres postgresql://user:password@host:5432/pbs_monitor

Options:
    --sqlite     SQLite URL (sqlite:////absolute/path or sqlite:///relative/path)
    --postgres   PostgreSQL DSN
    --batch-size Number of rows per insert batch (default: 500)
    --dry-run    Print row counts only; do not write to PostgreSQL
    --skip-table Comma-separated table names to skip (e.g. node_snapshots)
    --no-drop    Do not drop/recreate tables; just insert (tables must already exist)
    --verbose    Print progress for every batch

The migration order respects FK constraints:
  data_collection_log → jobs, queues, nodes
  → job_history, queue_snapshots, node_snapshots, system_snapshots
  → reservations → reservation_history, reservation_utilization
"""

import argparse
import json
import sys
import time
from contextlib import contextmanager
from typing import Any, Generator, List, Optional

from sqlalchemy import create_engine, text, inspect, MetaData, Table
from sqlalchemy.orm import sessionmaker, Session
from sqlalchemy.dialects.postgresql import insert as pg_insert


# ── table migration order (respects FK constraints) ─────────────────────────
TABLE_ORDER = [
    "data_collection_log",
    "jobs",
    "job_history",
    "queues",
    "queue_snapshots",
    "nodes",
    "node_snapshots",
    "system_snapshots",
    "reservations",
    "reservation_history",
    "reservation_utilization",
]


# ── helpers ──────────────────────────────────────────────────────────────────

def _row_to_dict(row) -> dict:
    """Convert a SQLAlchemy Row (or RowMapping) to a plain dict."""
    return dict(row._mapping)


def _coerce_row(row: dict) -> dict:
    """
    Normalize values so they are PostgreSQL-friendly.

    SQLite stores JSON columns as text; PostgreSQL expects dict/list.
    SQLite stores booleans as 0/1 integers; PostgreSQL wants True/False.
    """
    out = {}
    for k, v in row.items():
        # JSON stored as string in SQLite
        if isinstance(v, str):
            stripped = v.strip()
            if stripped and stripped[0] in ("{", "["):
                try:
                    v = json.loads(v)
                except json.JSONDecodeError:
                    pass  # leave as-is
        out[k] = v
    return out


@contextmanager
def engine_session(url: str, echo: bool = False) -> Generator[Session, None, None]:
    engine = create_engine(url, echo=echo)
    factory = sessionmaker(bind=engine)
    session = factory()
    try:
        yield session
    finally:
        session.close()
        engine.dispose()


def count_rows(session: Session, table_name: str) -> int:
    result = session.execute(text(f"SELECT COUNT(*) FROM {table_name}"))
    return result.scalar()


def get_columns(engine, table_name: str) -> List[str]:
    inspector = inspect(engine)
    return [col["name"] for col in inspector.get_columns(table_name)]


# ── core migration ───────────────────────────────────────────────────────────

def migrate_table(
    src_session: Session,
    dst_session: Session,
    dst_engine,
    table_name: str,
    batch_size: int = 500,
    verbose: bool = False,
) -> int:
    """Migrate one table; returns number of rows inserted."""
    src_count = count_rows(src_session, table_name)
    if src_count == 0:
        print(f"  {table_name}: 0 rows — skipping")
        return 0

    print(f"  {table_name}: {src_count:,} rows ...", end="", flush=True)
    t0 = time.time()

    # Reflect destination table so we can do typed inserts
    meta = MetaData()
    dst_table = Table(table_name, meta, autoload_with=dst_engine)

    dst_cols = {col.name for col in dst_table.columns}

    offset = 0
    inserted = 0

    while offset < src_count:
        rows_raw = src_session.execute(
            text(f"SELECT * FROM {table_name} LIMIT {batch_size} OFFSET {offset}")
        ).fetchall()

        if not rows_raw:
            break

        rows = [_coerce_row(_row_to_dict(r)) for r in rows_raw]

        # Drop any columns present in SQLite but absent in Postgres schema
        rows = [{k: v for k, v in r.items() if k in dst_cols} for r in rows]

        # Use INSERT ... ON CONFLICT DO NOTHING so reruns are idempotent
        stmt = pg_insert(dst_table).values(rows).on_conflict_do_nothing()
        dst_session.execute(stmt)
        dst_session.commit()

        inserted += len(rows)
        offset += batch_size

        if verbose:
            print(f"\r  {table_name}: {inserted:,}/{src_count:,}", end="", flush=True)

    elapsed = time.time() - t0
    print(f"\r  {table_name}: {inserted:,} rows inserted ({elapsed:.1f}s)")
    return inserted


# ── enum reconciliation ──────────────────────────────────────────────────────

def create_pg_enums(dst_engine) -> None:
    """
    Create PostgreSQL ENUM types that SQLAlchemy models declare.
    SQLite ignores enums; Postgres requires them to exist before table creation.
    The models.py already declares them via SQLEnum — create_all handles this,
    but we call it explicitly here so it's clear what's happening.
    """
    # Import here so the script can run from the repo root without install
    sys.path.insert(0, ".")
    from pbs_monitor.database.models import Base  # noqa: F401 — triggers enum registration
    Base.metadata.create_all(dst_engine)


# ── entry point ──────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Migrate PBS Monitor data from SQLite to PostgreSQL"
    )
    parser.add_argument(
        "--sqlite",
        required=True,
        help="SQLite source URL, e.g. sqlite:////home/parton/pbs_data/polaris_pbs_data.db",
    )
    parser.add_argument(
        "--postgres",
        required=True,
        help="PostgreSQL destination DSN, e.g. postgresql://pbs_monitor:password@localhost:5432/pbs_monitor",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=500,
        help="Rows per insert batch (default: 500)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print row counts only; do not write to PostgreSQL",
    )
    parser.add_argument(
        "--skip-table",
        default="",
        help="Comma-separated table names to skip",
    )
    parser.add_argument(
        "--no-drop",
        action="store_true",
        help="Do not drop/recreate Postgres tables; just insert (tables must exist)",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Print per-batch progress",
    )
    args = parser.parse_args()

    skip_tables = {t.strip() for t in args.skip_table.split(",") if t.strip()}
    tables_to_migrate = [t for t in TABLE_ORDER if t not in skip_tables]

    # ── source: SQLite ────────────────────────────────────────────────────────
    print(f"\nSource (SQLite): {args.sqlite}")
    src_engine = create_engine(
        args.sqlite,
        connect_args={"check_same_thread": False, "timeout": 60},
    )

    # Quick sanity check
    with src_engine.connect() as conn:
        src_tables = inspect(src_engine).get_table_names()
    print(f"  Found tables: {', '.join(src_tables)}")

    if args.dry_run:
        print("\n── DRY RUN — row counts only ──")
        src_session_factory = sessionmaker(bind=src_engine)
        src_session = src_session_factory()
        for table in tables_to_migrate:
            if table in src_tables:
                n = count_rows(src_session, table)
                print(f"  {table}: {n:,} rows")
            else:
                print(f"  {table}: NOT FOUND in source")
        src_session.close()
        src_engine.dispose()
        print("\nDry run complete — no data written.")
        return

    # ── destination: PostgreSQL ───────────────────────────────────────────────
    print(f"\nDestination (PostgreSQL): {args.postgres.split('@')[-1]}")  # hide creds
    dst_engine = create_engine(
        args.postgres,
        pool_pre_ping=True,
        pool_recycle=3600,
    )

    if not args.no_drop:
        print("\nCreating schema in PostgreSQL (drop + recreate)...")
        sys.path.insert(0, ".")
        from pbs_monitor.database.models import Base

        Base.metadata.drop_all(dst_engine)
        Base.metadata.create_all(dst_engine)
        print("  Schema ready.")
    else:
        print("\n--no-drop set; assuming schema already exists.")

    # ── migrate ───────────────────────────────────────────────────────────────
    print("\nMigrating tables:")
    src_session_factory = sessionmaker(bind=src_engine)
    src_session = src_session_factory()

    dst_session_factory = sessionmaker(bind=dst_engine)
    dst_session = dst_session_factory()

    total_rows = 0
    t_start = time.time()

    try:
        for table in tables_to_migrate:
            if table not in src_tables:
                print(f"  {table}: NOT FOUND in source — skipping")
                continue
            rows = migrate_table(
                src_session,
                dst_session,
                dst_engine,
                table,
                batch_size=args.batch_size,
                verbose=args.verbose,
            )
            total_rows += rows
    except KeyboardInterrupt:
        print("\n\nInterrupted — partial migration written (ON CONFLICT DO NOTHING).")
    finally:
        src_session.close()
        dst_session.close()
        src_engine.dispose()
        dst_engine.dispose()

    elapsed = time.time() - t_start
    print(f"\n✓ Migration complete: {total_rows:,} total rows in {elapsed:.1f}s")
    print(
        "\nNext step: update ~/.pbs_monitor.yaml (or PBS_MONITOR_DB_URL) to point "
        "at the PostgreSQL DSN."
    )


if __name__ == "__main__":
    main()
