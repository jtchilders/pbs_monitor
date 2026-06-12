#!/usr/bin/env python3
"""
Phase D.3 Migration: Upload local multi-system DB to Hermes Postgres

Reads from the local ``pbs_monitor_data_with_system`` DB produced by
``add_system_column.py`` and streams rows into the Hermes-hosted Postgres
instance via the ``hermes-kubectl port-forward`` bridge.

The upload is **idempotent**: ``ON CONFLICT DO NOTHING`` means you can
re-run after an OIDC token expiry (which kills the port-forward) without
creating duplicate rows. Only rows not already present in the target will
be inserted.

Usage:
    python scripts/upload_to_hermes.py \\
        --source-url postgresql://localhost/pbs_monitor_data_with_system \\
        --target-url postgresql://pbs_monitor:PASS@localhost:15432/pbs_monitor_data \\
        --system     polaris \\
        [--batch-size 500] [--dry-run] [--verbose]

Prerequisites:
    - hermes-kubectl port-forward running:
          hermes-kubectl port-forward -n pbs-monitor svc/pbs-postgres 15432:5432 &
    - Target schema already initialized (pbs-monitor database init against Hermes)
    - Source is the output of add_system_column.py (has system column populated)

Row-count verification:
    Final check compares ``SELECT count(*) FROM <table> WHERE system=<system>``
    between source and target.  Because the target may already hold partial data
    from a previous interrupted run, we compare only the per-system count, not
    the total row count.

FK-safe insert order:
    data_collection_log → jobs, queues, nodes
    → job_history, queue_snapshots, node_snapshots, system_snapshots
    → reservations → reservation_history, reservation_utilization
"""

import argparse
import sys
import time
from contextlib import contextmanager
from typing import Any, Generator, Iterator

from sqlalchemy import create_engine, inspect, text
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.orm import sessionmaker, Session


# ─── constants ────────────────────────────────────────────────────────────────

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

SKIP_TABLES = {"analytics_cache"}

# Tables large enough to warrant streaming reads
LARGE_TABLES = {"jobs", "job_history"}

# For jobs specifically, use a smaller batch size to avoid long insert transactions
BATCH_OVERRIDES = {
    "jobs": 500,
}


# ─── helpers ──────────────────────────────────────────────────────────────────

def _make_engine(url: str) -> Any:
    return create_engine(url, pool_pre_ping=True, pool_timeout=60)


def _source_tables(engine) -> set[str]:
    insp = inspect(engine)
    return set(insp.get_table_names())


def _count_by_system(engine, table: str, system: str) -> int:
    """Count rows for a specific system in the given table."""
    try:
        with engine.connect() as conn:
            return conn.execute(
                text(f'SELECT COUNT(*) FROM "{table}" WHERE system = :s'),
                {"s": system},
            ).scalar() or 0
    except Exception:
        with engine.connect() as conn:
            return conn.execute(text(f'SELECT COUNT(*) FROM "{table}"')).scalar() or 0


def _row_stream(engine, table: str, system: str, batch_size: int) -> Iterator[list[dict]]:
    """Stream rows from source table for the given system."""
    with engine.connect() as raw:
        raw.execution_options(stream_results=True)
        result = raw.execute(
            text(f'SELECT * FROM "{table}" WHERE system = :s'),
            {"s": system},
        )
        keys = list(result.keys())
        while True:
            rows = result.fetchmany(batch_size)
            if not rows:
                break
            yield [{keys[i]: row[i] for i in range(len(keys))} for row in rows]


def _insert_batch(conn, table_obj, rows: list[dict]) -> None:
    """Insert a batch using ON CONFLICT DO NOTHING.

    Uses pg_insert(Table) so the ORM's type adapters handle JSONB columns
    (jobs.raw_pbs_data etc.) correctly. Also filters rows to columns the
    target Table actually has, dropping any legacy/source-only keys.
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
) -> tuple[int, int]:
    """Upload one table. Returns (rows_read, rows_inserted)."""
    effective_batch = BATCH_OVERRIDES.get(table, batch_size)
    rows_read = 0
    rows_written = 0

    # Resolve the SA Table object once for type-correct inserts.
    from pbs_monitor.database.models import Base
    table_obj = Base.metadata.tables[table]

    with target_engine.connect() as tgt:
        tgt.execution_options(autocommit=False)

        for batch in _row_stream(source_engine, table, system, effective_batch):
            rows_read += len(batch)

            if not dry_run and batch:
                _insert_batch(tgt, table_obj, batch)
                tgt.commit()

            rows_written += len(batch)

            if verbose:
                print(f"  {table}: {rows_read:,} rows uploaded", end="\r")

    if verbose:
        print()  # newline after \r progress

    return rows_read, rows_written


# ─── main ─────────────────────────────────────────────────────────────────────

def main() -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Phase D.3: upload local multi-system DB to Hermes Postgres "
            "via the hermes-kubectl port-forward bridge. Idempotent — "
            "safe to re-run after an OIDC token expiry."
        )
    )
    parser.add_argument(
        "--source-url",
        required=True,
        metavar="DSN",
        help="Source Postgres DSN (output of add_system_column.py).",
    )
    parser.add_argument(
        "--target-url",
        required=True,
        metavar="DSN",
        help="Target Postgres DSN (Hermes, via port-forward).",
    )
    parser.add_argument(
        "--system",
        required=True,
        metavar="NAME",
        help="System name to upload (e.g. 'polaris'). Only rows with "
             "this system value are read from source.",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=500,
        metavar="N",
        help="Rows per insert batch (default: 500; jobs table always uses 500).",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print counts only; do not write to target.",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Print per-batch progress.",
    )
    args = parser.parse_args()

    # Mask passwords in URLs for any logged output — these messages may end up
    # in shared log files or chat transcripts.
    def _mask(u: str) -> str:
        if '://' in u and '@' in u:
            scheme, rest = u.split('://', 1)
            if '@' in rest:
                auth, host = rest.split('@', 1)
                if ':' in auth:
                    user, _ = auth.split(':', 1)
                    return f"{scheme}://{user}:***@{host}"
        return u

    print(f"Source: {_mask(args.source_url)}")
    print(f"Target: {_mask(args.target_url)}")
    print(f"System: {args.system}")
    print(f"Dry run: {args.dry_run}")
    print()

    source_engine = _make_engine(args.source_url)
    target_engine = _make_engine(args.target_url)

    # ── verify target schema exists ───────────────────────────────────────────
    if not args.dry_run:
        try:
            with target_engine.connect() as conn:
                conn.execute(text("SELECT 1 FROM jobs LIMIT 1"))
        except Exception as e:
            print(
                f"ERROR: Target schema not initialised. Run:\n"
                f"  pbs-monitor database init --config <dev-config>\n"
                f"against the Hermes endpoint first.\n({e})"
            )
            return 1

    source_tables = _source_tables(source_engine)
    summary: list[dict] = []
    start_wall = time.time()

    for table in TABLE_ORDER:
        if table in SKIP_TABLES:
            print(f"  [{table}] skipped (runtime-created)")
            continue
        if table not in source_tables:
            print(f"  [{table}] not in source — skipping")
            continue

        src_count = _count_by_system(source_engine, table, args.system)
        print(f"[{table}] {src_count:,} source rows (system={args.system})...")

        t0 = time.time()
        rows_read, rows_written = _migrate_table(
            source_engine=source_engine,
            target_engine=target_engine,
            table=table,
            system=args.system,
            batch_size=args.batch_size,
            dry_run=args.dry_run,
            verbose=args.verbose,
        )
        elapsed = time.time() - t0

        if args.dry_run:
            print(f"  → (dry-run) would upload {rows_read:,} rows in {elapsed:.1f}s")
        else:
            tgt_count = _count_by_system(target_engine, table, args.system)
            print(
                f"  → {rows_written:,} rows uploaded, "
                f"target now has {tgt_count:,} rows for {args.system} in {elapsed:.1f}s"
            )

        summary.append({
            "table": table,
            "source": src_count,
            "uploaded": rows_written,
        })

    # Reset target sequences after bulk upload — see add_system_column.py for
    # the full rationale. Without this, the live daemon (or any other writer)
    # collides forever with our pre-loaded rows.
    if not args.dry_run:
        from sqlalchemy.sql import text as _text
        from pbs_monitor.database.models import Base
        print()
        print("Resetting target sequences to match loaded row counts...")
        with target_engine.connect() as conn:
            for table_name, table_obj in Base.metadata.tables.items():
                for col in table_obj.columns:
                    default_obj = col.server_default
                    if default_obj is None:
                        continue
                    default_text = str(default_obj.arg) if hasattr(default_obj, "arg") else str(default_obj)
                    if "nextval" not in default_text:
                        continue
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
    print(f"{'Table':<35} {'Source':>12} {'Uploaded':>12}")
    print("-" * 62)
    for row in summary:
        match = "✓" if row["source"] == row["uploaded"] else "⚠"
        print(f"{row['table']:<35} {row['source']:>12,} {row['uploaded']:>12,}  {match}")

    mismatches = [r for r in summary if r["source"] != r["uploaded"]]
    if mismatches and not args.dry_run:
        print(
            f"\n⚠ {len(mismatches)} table(s) may have skipped rows "
            "(already existed in target — ON CONFLICT DO NOTHING)."
        )
        print(
            "This is expected on re-runs after OIDC expiry. "
            "Verify final counts are ≥ source counts."
        )
    return 0


if __name__ == "__main__":
    sys.exit(main())
