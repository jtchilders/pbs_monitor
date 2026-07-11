"""Tests for occupied_seconds (v1.3): the node-occupancy field derived from PBS
resources_used.walltime, which excludes HELD/QUEUED gaps for requeued jobs.

Covers:
  - _parse_duration_to_seconds (HH:MM:SS and DD:HH:MM:SS)
  - _compute_occupied_seconds preference/fallback logic at ingest
  - migrate_to_v1_3_occupied_seconds adds the column and is idempotent
  - backfill_occupied_seconds populates from raw_pbs_data and leaves never-ran NULL
"""
import pytest

from pbs_monitor.database.model_converters import (
    _parse_duration_to_seconds,
    _compute_occupied_seconds,
)


class _FakePBSJob:
    """Minimal stand-in for PBSJob with the attributes _compute_occupied_seconds reads."""
    def __init__(self, raw_attributes, actual_runtime_seconds=None):
        self.raw_attributes = raw_attributes
        self.actual_runtime_seconds = actual_runtime_seconds


class TestParseDuration:
    def test_hms(self):
        assert _parse_duration_to_seconds("01:00:00") == 3600
        assert _parse_duration_to_seconds("00:30:00") == 1800
        assert _parse_duration_to_seconds("24:00:00") == 86400

    def test_ddhhmmss(self):
        # 1 day 1 hour 1 minute 1 second
        assert _parse_duration_to_seconds("1:01:01:01") == 86400 + 3600 + 60 + 1

    def test_invalid(self):
        assert _parse_duration_to_seconds(None) is None
        assert _parse_duration_to_seconds("") is None
        assert _parse_duration_to_seconds("garbage") is None
        assert _parse_duration_to_seconds("1:2") is None


class TestComputeOccupiedSeconds:
    def test_prefers_resources_used_walltime(self):
        job = _FakePBSJob(
            raw_attributes={"resources_used": {"walltime": "02:00:00"}},
            actual_runtime_seconds=999999,  # should be ignored
        )
        assert _compute_occupied_seconds(job) == 7200

    def test_resources_used_present_no_walltime_falls_back_to_span(self):
        job = _FakePBSJob(
            raw_attributes={"resources_used": {"cput": "10:00:00"}},
            actual_runtime_seconds=1234,
        )
        assert _compute_occupied_seconds(job) == 1234

    def test_never_ran_returns_none(self):
        # No resources_used, no exec record -> job never occupied nodes.
        job = _FakePBSJob(
            raw_attributes={"comment": "job held, too many failed attempts"},
            actual_runtime_seconds=3921750,  # the pathological 45-day span
        )
        assert _compute_occupied_seconds(job) is None

    def test_exec_host_without_resources_used_falls_back_to_span(self):
        job = _FakePBSJob(
            raw_attributes={"exec_host": "x4206c2s0b0n0/0*208"},
            actual_runtime_seconds=1983,
        )
        assert _compute_occupied_seconds(job) == 1983

    def test_empty_raw_returns_none(self):
        job = _FakePBSJob(raw_attributes={}, actual_runtime_seconds=500)
        assert _compute_occupied_seconds(job) is None


class TestMigrationV13:
    def test_migrate_adds_column_and_idempotent(self, tmp_path):
        from pbs_monitor.config import Config
        from pbs_monitor.database.migrations import DatabaseMigration
        from sqlalchemy import inspect

        db = tmp_path / "v13.db"
        cfg = Config()
        cfg.database.url = f"sqlite:///{db}"
        m = DatabaseMigration(cfg)
        m.create_fresh_database()  # fresh models already include occupied_seconds

        insp = inspect(m.db_manager.engine)
        cols = [c["name"] for c in insp.get_columns("jobs")]
        assert "occupied_seconds" in cols
        assert m.check_schema_version() == "1.3.0"

        # Running the explicit migration again must not raise (idempotent).
        m.migrate_to_v1_3_occupied_seconds()
        m.migrate_to_v1_3_occupied_seconds()


class TestBackfillOccupiedSeconds:
    def test_backfill_populates_and_leaves_never_ran_null(self, tmp_path):
        import json
        from pbs_monitor.config import Config
        from pbs_monitor.database.migrations import DatabaseMigration
        from sqlalchemy import text

        db = tmp_path / "bf.db"
        cfg = Config()
        cfg.database.url = f"sqlite:///{db}"
        m = DatabaseMigration(cfg)
        m.create_fresh_database()

        with m.db_manager.get_session() as s:
            # ran job with resources_used.walltime
            s.execute(text(
                "INSERT INTO jobs (job_id, state, nodes, actual_runtime_seconds, raw_pbs_data) "
                "VALUES ('ran1', 'FINISHED', 10, 9999, :raw)"
            ), {"raw": json.dumps({"resources_used": {"walltime": "01:00:00"}})})
            # never-ran job (no resources_used, no exec) with huge span
            s.execute(text(
                "INSERT INTO jobs (job_id, state, nodes, actual_runtime_seconds, raw_pbs_data) "
                "VALUES ('neverran', 'FINISHED', 5000, 3921750, :raw)"
            ), {"raw": json.dumps({"comment": "job held"})})
            s.commit()

        res = m.backfill_occupied_seconds(batch_size=100, dry_run=False)
        assert res["updated"] == 1
        assert res["skipped"] == 1
        assert res["errors"] == 0

        with m.db_manager.get_session() as s:
            ran = s.execute(text("SELECT occupied_seconds FROM jobs WHERE job_id='ran1'")).scalar()
            never = s.execute(text("SELECT occupied_seconds FROM jobs WHERE job_id='neverran'")).scalar()
        assert ran == 3600
        assert never is None

        # Idempotent: re-run touches nothing already populated.
        res2 = m.backfill_occupied_seconds(batch_size=100, dry_run=False)
        assert res2["updated"] == 0
