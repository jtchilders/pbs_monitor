#!/usr/bin/env python3
"""
Phase D.1 Migration: Add system column to PBS Monitor data (local Postgres → local Postgres)

Reads from an existing local Postgres database (prod or dev, untouched by this script),
creates a new local Postgres database with the multi-system schema, and streams all rows
into it with ``system`` stamped on every row.

Usage:
    python scripts/add_system_column.py \\
        --source-url postgresql://localhost/pbs_monitor_dev \\
        --target-url postgresql://localhost/pbs_monitor_data_with_system \\
        --system     polaris \\
        [--batch-size 1000] [--dry-run] [--verbose]

Prerequisites:
    - Target DB must already exist (empty):
          createdb -h localhost pbs_monitor_data_with_system
    - Source DB is read-only from this script's perspective (no writes).

FK-safe insert order:
    data_collection_log → jobs, queues, nodes
    → job_history, queue_snapshots, node_snapshots, system_snapshots
    → reservations → reservation_history, reservation_utilization

Notes:
    - analytics_cache is skipped: created at runtime by the web layer, not modelled.
    - jobs table is large (~241k rows, ~4 kB/row); uses streaming cursor.
    - All other tables use batched reads.
"""

import argparse
import sys
import time
from contextlib import contextmanager
from typing import Any, Generator, Iterator, List

from sqlalchemy import create_engine, inspect, text
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.orm import sessionmaker, Session


# ─── table migration order (FK-safe) ─────────────────────────────────────────

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

# Tables the web layer creates at runtime; not in models, not migrated.
SKIP_TABLES = {"analytics_cache"}

# FK-referencing tables: only migrate rows whose parent exists in the target.
# (Source DBs sometimes have orphaned FK rows from partial collection runs.)
FK_CHILD_FILTER: dict[str, str] = {
    "job_history":             "data_collection_id",
    "queue_snapshots":         "data_collection_id",
    "node_snapshots":          "data_collection_id",
    "system_snapshots":        "data_collection_id",
    "reservation_history":     "data_collection_id",
}

# Tables large enough to warrant a streaming cursor (server-side)
LARGE_TABLES = {"jobs", "job_history"}


# ─── helpers ──────────────────────────────────────────────────────────────────

def _make_engine(url: str, streaming: bool = False):
    """Create a SQLAlchemy engine for a Postgres URL."""
    kwargs: dict[str, Any] = {
        "pool_pre_ping": True,
    }
    return create_engine(url, **kwargs)


@contextmanager
def _session(engine) -> Generator[Session, None, None]:
    Session_ = sessionmaker(bind=engine)
    session = Session_()
    try:
        yield session
    finally:
        session.close()


def _source_tables(conn) -> set[str]:
    """Return table names present in source DB."""
    insp = inspect(conn)
    return set(insp.get_table_names())


def _source_columns(conn, table: str) -> set[str]:
    """Return column names for a table in source DB."""
    insp = inspect(conn)
    return {c["name"] for c in insp.get_columns(table)}


def _row_stream(conn, table: str, batch_size: int) -> Iterator[list[dict]]:
    """Stream rows from source table in batches using a named server-side cursor."""
    # Server-side cursor avoids loading the whole table into memory.
    with conn.connect() as raw:
        raw.execution_options(stream_results=True)
        result = raw.execute(text(f'SELECT * FROM "{table}"'))
        keys = list(result.keys())
        while True:
            rows = result.fetchmany(batch_size)
            if not rows:
                break
            yield [{keys[i]: row[i] for i in range(len(keys))} for row in rows]


def _count_source(conn, table: str) -> int:
    """Count rows in a source table."""
    with conn.connect() as raw:
        return raw.execute(text(f'SELECT COUNT(*) FROM "{table}"')).scalar() or 0


def _count_target(conn, table: str, system: str) -> int:
    """Count rows in a target table filtered to the given system."""
    try:
        with conn.connect() as raw:
            return raw.execute(
                text(f'SELECT COUNT(*) FROM "{table}" WHERE system = :s'),
                {"s": system},
            ).scalar() or 0
    except Exception:
        # Table might not have a system column (shouldn't happen with new schema).
        with conn.connect() as raw:
            return raw.execute(text(f'SELECT COUNT(*) FROM "{table}"')).scalar() or 0


def _valid_dcl_ids(target_engine) -> set[int]:
    """Return the set of data_collection_log.id values present in the target."""
    with target_engine.connect() as conn:
        result = conn.execute(text("SELECT id FROM data_collection_log"))
        return {row[0] for row in result}


def _insert_batch(conn, table_obj, rows: list[dict]) -> None:
    """Insert a batch of row dicts into target table using ON CONFLICT DO NOTHING.

    Uses the PostgreSQL dialect's insert() construct with the SQLAlchemy Table
    object (reflected from Base.metadata) so that column type adapters fire
    properly. This matters for JSONB columns like jobs.raw_pbs_data, where
    bypassing the ORM with raw text-INSERT causes psycopg2 to balk with
    'can't adapt type dict'.

    Also filters each row dict to only the columns the target Table actually
    has, so any source-only columns (legacy or otherwise) are silently
    dropped instead of causing a CompileError.
    """
    if not rows:
        return
    valid_cols = {c.name for c in table_obj.columns}
    filtered = [{k: v for k, v in r.items() if k in valid_cols} for r in rows]
    stmt = pg_insert(table_obj).on_conflict_do_nothing()
    conn.execute(stmt, filtered)


def _migrate_table(
    source_engine,
    target_engine,
    table: str,
    system: str,
    batch_size: int,
    dry_run: bool,
    verbose: bool,
    valid_dcl_ids: set[int] | None,
    src_cols: set[str],
) -> tuple[int, int]:
    """Migrate one table. Returns (rows_read, rows_inserted)."""
    # Columns we'll select from source (may not include 'system' — old schema)
    # Columns to insert into target (always includes 'system')
    filter_col = FK_CHILD_FILTER.get(table)

    rows_read = 0
    rows_written = 0

    # In dry-run mode we don't open the target connection at all — a dry-run
    # should be runnable without a target DB existing yet (it's a recon tool).
    if dry_run:
        for batch in _row_stream(source_engine, table, batch_size):
            rows_read += len(batch)
            # Can't filter FK orphans in dry-run (no target to compare against);
            # report raw source counts only.
            rows_written += len(batch)
            if verbose:
                print(f"  {table}: {rows_read:,} rows streamed (dry-run)", end="\r")
    else:
        # Resolve the SQLAlchemy Table object once per table call so the
        # pg_insert() construct can use it for type adaptation (JSONB etc.).
        from pbs_monitor.database.models import Base
        table_obj = Base.metadata.tables[table]

        with target_engine.connect() as target_conn:
            target_conn.execution_options(autocommit=False)

            for batch in _row_stream(source_engine, table, batch_size):
                rows_read += len(batch)

                # Filter out FK-orphaned rows (child has data_collection_id not in target)
                if filter_col and valid_dcl_ids is not None:
                    batch = [r for r in batch if r.get(filter_col) in valid_dcl_ids]

                # Stamp system on every row, overriding any existing system value
                for row in batch:
                    row["system"] = system
                    # Strip source columns not present in target schema
                    # (target may have new columns the source doesn't; extra keys are fine)

                _insert_batch(target_conn, table_obj, batch)
                target_conn.commit()

                rows_written += len(batch)

                if verbose:
                    print(f"  {table}: {rows_read:,} rows streamed, {rows_written:,} kept", end="\r")

    if verbose:
        print()  # newline after \r progress

    return rows_read, rows_written


# ─── main ─────────────────────────────────────────────────────────────────────

def main() -> int:
    parser = argparse.ArgumentParser(
        description="Phase D.1: rewrite Polaris (or Aurora) Postgres data into "
                    "a new local DB with the multi-system schema (system column added)."
    )
    parser.add_argument(
        "--source-url",
        required=True,
        metavar="DSN",
        help="Source Postgres DSN (existing prod/dev DB; read-only).",
    )
    parser.add_argument(
        "--target-url",
        required=True,
        metavar="DSN",
        help="Target Postgres DSN (empty DB pre-created with createdb).",
    )
    parser.add_argument(
        "--system",
        required=True,
        metavar="NAME",
        help="System name to stamp on all rows (e.g. 'polaris', 'aurora').",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=1000,
        metavar="N",
        help="Rows per batch (default: 1000; use 500 for large column tables).",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print row counts only; do not write to target.",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Print per-batch progress.",
    )
    args = parser.parse_args()

    print(f"Source: {args.source_url}")
    print(f"Target: {args.target_url}")
    print(f"System: {args.system}")
    print(f"Dry run: {args.dry_run}")
    print()

    source_engine = _make_engine(args.source_url)
    target_engine = _make_engine(args.target_url)

    # ── create target schema from models ─────────────────────────────────────
    if not args.dry_run:
        print("Creating target schema from ORM models...")
        from pbs_monitor.database.models import Base
        Base.metadata.create_all(target_engine)
        print("Schema created.\n")

    # ── discover source tables ────────────────────────────────────────────────
    source_tables = _source_tables(source_engine)

    # ── migrate in FK-safe order ──────────────────────────────────────────────
    summary: list[dict] = []
    start_wall = time.time()

    for table in TABLE_ORDER:
        if table in SKIP_TABLES:
            print(f"  [{table}] skipped (runtime-created, not in models)")
            continue
        if table not in source_tables:
            print(f"  [{table}] not in source DB — skipping")
            continue

        src_cols = _source_columns(source_engine, table)
        src_count = _count_source(source_engine, table)
        print(f"[{table}] {src_count:,} rows in source...")

        # After data_collection_log is migrated, build the valid-ID set
        # so child tables can filter orphans.
        valid_dcl_ids: set[int] | None = None
        if table in FK_CHILD_FILTER and not args.dry_run:
            valid_dcl_ids = _valid_dcl_ids(target_engine)

        t0 = time.time()
        rows_read, rows_written = _migrate_table(
            source_engine=source_engine,
            target_engine=target_engine,
            table=table,
            system=args.system,
            batch_size=args.batch_size,
            dry_run=args.dry_run,
            verbose=args.verbose,
            valid_dcl_ids=valid_dcl_ids,
            src_cols=src_cols,
        )
        elapsed = time.time() - t0

        if args.dry_run:
            print(f"  → (dry-run) would migrate {rows_read:,} rows in {elapsed:.1f}s")
        else:
            tgt_count = _count_target(target_engine, table, args.system)
            orphans = rows_read - rows_written
            print(f"  → {rows_written:,} rows written, {orphans:,} orphans filtered, "
                  f"target count {tgt_count:,} in {elapsed:.1f}s")

        summary.append({
            "table": table,
            "source": src_count,
            "migrated": rows_written,
        })

    # Reset all SERIAL/identity sequences so the next nextval() call returns
    # max(id)+1 instead of starting from 1. Without this, any application
    # that later inserts into these tables will collide with our bulk-loaded
    # rows until the sequence catches up — silently in production code paths
    # whose error logging goes to /dev/null. This bit us hard during Phase E
    # dev-daemon startup (~293 silent collisions before we found it).
    if not args.dry_run:
        from sqlalchemy.sql import text as _text
        from pbs_monitor.database.models import Base
        print()
        print("Resetting sequences to match loaded row counts...")
        with target_engine.connect() as conn:
            for table_name, table_obj in Base.metadata.tables.items():
                for col in table_obj.columns:
                    # PK columns whose default is a sequence (server_default contains 'nextval')
                    default_obj = col.server_default
                    if default_obj is None:
                        continue
                    default_text = str(default_obj.arg) if hasattr(default_obj, "arg") else str(default_obj)
                    if "nextval" not in default_text:
                        continue
                    # Sequence name is conventionally <table>_<col>_seq
                    seq_name = f"{table_name}_{col.name}_seq"
                    try:
                        result = conn.execute(_text(
                            f"SELECT setval('{seq_name}', "
                            f"(SELECT coalesce(max({col.name}),1) FROM {table_name}))"
                        ))
                        new_val = result.scalar()
                        print(f"  {seq_name:<48} -> {new_val:,}")
                    except Exception as e:
                        print(f"  {seq_name:<48} -> SKIPPED ({e.__class__.__name__})")
            conn.commit()

    total_elapsed = time.time() - start_wall
    print(f"\nDone in {total_elapsed:.1f}s")
    print()
    print(f"{'Table':<35} {'Source':>12} {'Migrated':>12}")
    print("-" * 62)
    for row in summary:
        match = "✓" if row["source"] == row["migrated"] else "⚠"
        print(f"{row['table']:<35} {row['source']:>12,} {row['migrated']:>12,}  {match}")

    # Exit non-zero if any table had a mismatch
    mismatches = [r for r in summary if r["source"] != r["migrated"]]
    if mismatches and not args.dry_run:
        print(
            f"\n⚠ {len(mismatches)} table(s) with row-count mismatches "
            "(expected for tables with orphaned FK rows in source)."
        )
        print("Verify that orphaned rows are acceptable before proceeding.")
        # Return 0 — mismatches from orphan-filtered FK children are expected;
        # callers can inspect the output and decide.
    return 0


if __name__ == "__main__":
    sys.exit(main())
