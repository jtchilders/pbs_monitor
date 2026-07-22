"""
Tests for T0: outcome_class classifier and related migration / backfill.

Coverage:
  - classify_exit() — every rule branch (plan §6, verbatim)
  - migrate_to_v1_2_outcome_class() — column + indexes created, idempotent
  - backfill_exit_status_and_outcome_class() — batched, idempotent, JSON parsing
  - model_converters.JobConverter.to_database() — outcome_class populated
"""

import json
import tempfile
import os
from datetime import datetime, timedelta
from typing import Optional

import pytest

from pbs_monitor.analytics.outcome_classifier import classify_exit, OUTCOME_CLASSES


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _classify(state: str, es: Optional[int], runtime: Optional[int] = None, wt: Optional[int] = None) -> str:
    """Thin wrapper for brevity in tests."""
    return classify_exit(state, es, actual_runtime_seconds=runtime, requested_walltime_seconds=wt)


# ---------------------------------------------------------------------------
# Unit tests: classify_exit — every plan §6 branch
# ---------------------------------------------------------------------------

class TestClassifyExitRules:
    """Exhaustive branch coverage of the §6 mapping rules."""

    # Rule: exit_status == 0  ->  success
    def test_zero_is_success(self):
        assert _classify("F", 0) == "success"

    def test_zero_is_success_completed_state(self):
        assert _classify("C", 0) == "success"

    # Rule: exit_status is None  ->  unknown
    def test_none_exit_status_is_unknown(self):
        assert _classify("F", None) == "unknown"

    def test_none_with_walltime_info_still_unknown(self):
        assert _classify("F", None, runtime=3600, wt=3600) == "unknown"

    # Rule: exit_status == 271  ->  requeued
    def test_271_is_requeued(self):
        assert _classify("F", 271) == "requeued"

    # Rule: exit_status == -29  ->  walltime_killed (PBS walltime-exceeded kill)
    # Confirmed against real Aurora data: -29 is the walltime-exceeded code, the
    # single most common non-zero exit code, 100% ran >=95% of requested walltime.
    # It is checked BEFORE the generic `exit_status < 0 -> could_not_run` rule.
    def test_minus_29_is_walltime_killed(self):
        assert _classify("F", -29) == "walltime_killed"

    def test_minus_29_walltime_killed_without_runtime_info(self):
        """-29 alone is definitive — no runtime/walltime refinement required."""
        assert _classify("F", -29, runtime=None, wt=None) == "walltime_killed"

    # Rule: other exit_status < 0  ->  could_not_run
    def test_minus_1_is_could_not_run(self):
        assert _classify("F", -1) == "could_not_run"

    def test_minus_20_is_could_not_run(self):
        assert _classify("F", -20) == "could_not_run"

    def test_minus_3_is_could_not_run(self):
        assert _classify("F", -3) == "could_not_run"

    def test_minus_14_is_could_not_run(self):
        assert _classify("F", -14) == "could_not_run"

    # Rule: 128 < exit_status < 192  ->  signal_killed  (generic)
    def test_137_sigkill_is_signal_killed(self):
        """SIGKILL (128+9)."""
        assert _classify("F", 137) == "signal_killed"

    def test_139_sigsegv_is_signal_killed(self):
        """SIGSEGV (128+11)."""
        assert _classify("F", 139) == "signal_killed"

    def test_129_is_signal_killed(self):
        """Boundary: 128 < 129."""
        assert _classify("F", 129) == "signal_killed"

    def test_191_is_signal_killed(self):
        """Boundary: 191 < 192."""
        assert _classify("F", 191) == "signal_killed"

    # Boundary conditions that should NOT be signal_killed
    def test_128_is_error_not_signal_killed(self):
        """128 itself is not in the (128, 192) range."""
        assert _classify("F", 128) == "error"

    def test_192_is_error_not_signal_killed(self):
        """192 itself is not in the (128, 192) range."""
        assert _classify("F", 192) == "error"

    # Rule: exit_status == 143 AND ran >= 0.95 * req_wt  ->  walltime_killed
    def test_143_near_walltime_limit_is_walltime_killed(self):
        """SIGTERM at 95% of requested walltime → walltime_killed."""
        wt = 3600
        runtime = int(0.95 * wt)  # exactly 95%
        assert _classify("F", 143, runtime=runtime, wt=wt) == "walltime_killed"

    def test_143_over_walltime_limit_is_walltime_killed(self):
        """Ran past the walltime (possible on PBS): definitely walltime kill."""
        wt = 3600
        runtime = wt + 60  # 101%
        assert _classify("F", 143, runtime=runtime, wt=wt) == "walltime_killed"

    def test_143_well_under_walltime_limit_is_signal_killed(self):
        """SIGTERM at only 50% of requested walltime → generic signal kill."""
        wt = 3600
        runtime = int(0.50 * wt)
        assert _classify("F", 143, runtime=runtime, wt=wt) == "signal_killed"

    def test_143_just_under_threshold_is_signal_killed(self):
        """SIGTERM at 94.9% → signal_killed (below 0.95 threshold)."""
        wt = 3600
        runtime = int(0.949 * wt)  # 94.9%
        assert _classify("F", 143, runtime=runtime, wt=wt) == "signal_killed"

    def test_143_no_walltime_info_is_signal_killed(self):
        """143 but no walltime info → cannot determine walltime kill, falls to signal_killed."""
        assert _classify("F", 143) == "signal_killed"

    def test_143_no_runtime_is_signal_killed(self):
        """143 with walltime but no runtime → cannot determine, falls to signal_killed."""
        assert _classify("F", 143, runtime=None, wt=3600) == "signal_killed"

    def test_143_zero_walltime_is_signal_killed(self):
        """Edge: requested walltime of 0 → division guard → signal_killed."""
        assert _classify("F", 143, runtime=100, wt=0) == "signal_killed"

    # Rule: otherwise  ->  error
    def test_1_is_error(self):
        assert _classify("F", 1) == "error"

    def test_2_is_error(self):
        assert _classify("F", 2) == "error"

    def test_127_is_error(self):
        assert _classify("F", 127) == "error"

    def test_255_is_error(self):
        assert _classify("F", 255) == "error"

    def test_200_above_signal_band_is_error(self):
        """Above 192: outside signal band."""
        assert _classify("F", 200) == "error"

    # Active job states return "unknown" (defensive guard)
    @pytest.mark.parametrize("state", ["Q", "R", "H", "W", "T", "E", "S"])
    def test_active_states_return_unknown(self, state):
        """Active jobs are not counted — classify_exit returns 'unknown'."""
        assert _classify(state, 0) == "unknown"

    # All returned values must be members of OUTCOME_CLASSES
    @pytest.mark.parametrize("es, runtime, wt", [
        (0, None, None),
        (None, None, None),
        (271, None, None),
        (-29, None, None),
        (143, 3420, 3600),
        (143, 1800, 3600),
        (137, None, None),
        (1, None, None),
        (127, None, None),
    ])
    def test_return_value_always_in_outcome_classes(self, es, runtime, wt):
        result = _classify("F", es, runtime=runtime, wt=wt)
        assert result in OUTCOME_CLASSES, f"classify_exit returned '{result}' which is not in OUTCOME_CLASSES"


# ---------------------------------------------------------------------------
# Integration tests: migration + backfill against an in-memory fixture DB
# ---------------------------------------------------------------------------

@pytest.fixture
def fixture_db_config():
    """Create a temporary SQLite database for integration tests."""
    temp_db = tempfile.NamedTemporaryFile(delete=False, suffix='.db')
    temp_db.close()

    from pbs_monitor.config import Config
    config = Config()
    config.database.url = f"sqlite:///{temp_db.name}"

    yield config

    try:
        os.unlink(temp_db.name)
    except OSError:
        pass


@pytest.fixture
def initialized_db(fixture_db_config):
    """Create tables (fresh schema) in the fixture DB."""
    from pbs_monitor.database import initialize_database
    initialize_database(fixture_db_config)
    yield fixture_db_config


class TestMigration:
    """Test migrate_to_v1_2_outcome_class (idempotency, column, indexes)."""

    def test_outcome_class_column_created(self, initialized_db):
        """Migration adds the outcome_class column to jobs."""
        from pbs_monitor.database.migrations import DatabaseMigration
        from sqlalchemy import inspect

        m = DatabaseMigration(initialized_db)
        # Fresh DB already has outcome_class because the model defines it;
        # we verify the migration method is idempotent on an already-complete schema.
        m.migrate_to_v1_2_outcome_class()

        inspector = inspect(m.db_manager.engine)
        col_names = [c['name'] for c in inspector.get_columns('jobs')]
        assert 'outcome_class' in col_names

    def test_migration_idempotent(self, initialized_db):
        """Running migrate_to_v1_2_outcome_class twice does not raise."""
        from pbs_monitor.database.migrations import DatabaseMigration

        m = DatabaseMigration(initialized_db)
        m.migrate_to_v1_2_outcome_class()
        m.migrate_to_v1_2_outcome_class()  # Should not raise

    def test_schema_version_detection(self, initialized_db):
        """check_schema_version returns 1.4.0 when run_count column is present."""
        from pbs_monitor.database.migrations import DatabaseMigration

        m = DatabaseMigration(initialized_db)
        # A fresh DB initialized from current models already has run_count.
        version = m.check_schema_version()
        assert version == "1.4.0"


class TestBackfill:
    """Test backfill_exit_status_and_outcome_class against tiny fixture data."""

    def _insert_test_jobs(self, config, jobs):
        """Insert raw rows directly via SQLAlchemy text() for tight control."""
        from pbs_monitor.database.connection import get_database_manager
        from sqlalchemy import text

        dm = get_database_manager(config)
        with dm.get_session() as session:
            for j in jobs:
                raw_json = json.dumps(j.get('raw_pbs_data', {}))
                session.execute(
                    text(
                        "INSERT OR REPLACE INTO jobs "
                        "(job_id, state, exit_status, outcome_class, "
                        " actual_runtime_seconds, walltime, raw_pbs_data, "
                        " job_name, owner, queue) "
                        "VALUES (:job_id, :state, :exit_status, :outcome_class, "
                        "        :actual_runtime_seconds, :walltime, :raw_pbs_data, "
                        "        :job_name, :owner, :queue)"
                    ),
                    {
                        "job_id": j["job_id"],
                        "state": j.get("state", "F"),
                        "exit_status": j.get("exit_status"),
                        "outcome_class": j.get("outcome_class"),
                        "actual_runtime_seconds": j.get("actual_runtime_seconds"),
                        "walltime": j.get("walltime"),
                        "raw_pbs_data": raw_json,
                        "job_name": j.get("job_name", "test"),
                        "owner": j.get("owner", "tester"),
                        "queue": j.get("queue", "default"),
                    },
                )
            session.commit()

    def _read_job(self, config, job_id):
        """Read a single job row back from the DB."""
        from pbs_monitor.database.connection import get_database_manager
        from sqlalchemy import text

        dm = get_database_manager(config)
        with dm.get_session() as session:
            row = session.execute(
                text("SELECT exit_status, outcome_class FROM jobs WHERE job_id = :jid"),
                {"jid": job_id},
            ).fetchone()
        return row

    def test_backfill_parses_exit_status_from_raw_json(self, initialized_db):
        """Backfill extracts Exit_status from raw_pbs_data and classifies correctly."""
        self._insert_test_jobs(initialized_db, [
            {
                "job_id": "1.pbs",
                "state": "F",
                "exit_status": None,          # unpopulated
                "raw_pbs_data": {"Exit_status": 0},
            }
        ])

        from pbs_monitor.database.migrations import DatabaseMigration
        m = DatabaseMigration(initialized_db)
        stats = m.backfill_exit_status_and_outcome_class(batch_size=100)

        assert stats["updated"] >= 1
        row = self._read_job(initialized_db, "1.pbs")
        assert row[0] == 0                  # exit_status
        assert row[1] == "success"          # outcome_class

    def test_backfill_various_exit_codes(self, initialized_db):
        """Backfill correctly classifies a range of exit codes."""
        test_cases = [
            ("2.pbs", 0,    None, None, "success"),
            ("3.pbs", None, None, None, "unknown"),
            ("4.pbs", 271,  None, None, "requeued"),
            ("5.pbs", -29,  None, None, "walltime_killed"),
            ("5b.pbs", -20, None, None, "could_not_run"),
            ("6.pbs", 137,  None, None, "signal_killed"),
            ("7.pbs", 143,  3420, 3600, "walltime_killed"),   # 95% of 3600
            ("8.pbs", 143,  1800, 3600, "signal_killed"),     # 50% of 3600
            ("9.pbs", 1,    None, None, "error"),
        ]

        jobs_to_insert = []
        for job_id, es, runtime, wt, _ in test_cases:
            jobs_to_insert.append({
                "job_id": job_id,
                "state": "F",
                "exit_status": es,
                "actual_runtime_seconds": runtime,
                "walltime": f"{wt//3600:02d}:{(wt%3600)//60:02d}:{wt%60:02d}" if wt else None,
            })
        self._insert_test_jobs(initialized_db, jobs_to_insert)

        from pbs_monitor.database.migrations import DatabaseMigration
        m = DatabaseMigration(initialized_db)
        m.backfill_exit_status_and_outcome_class(batch_size=100)

        for job_id, es, runtime, wt, expected_class in test_cases:
            row = self._read_job(initialized_db, job_id)
            assert row[1] == expected_class, (
                f"job_id={job_id}: expected outcome_class={expected_class!r}, got {row[1]!r}"
            )

    def test_backfill_is_idempotent(self, initialized_db):
        """Running backfill twice: second run skips already-populated rows."""
        self._insert_test_jobs(initialized_db, [
            {"job_id": "10.pbs", "state": "F", "exit_status": 0},
        ])

        from pbs_monitor.database.migrations import DatabaseMigration
        m = DatabaseMigration(initialized_db)
        stats1 = m.backfill_exit_status_and_outcome_class(batch_size=100)
        stats2 = m.backfill_exit_status_and_outcome_class(batch_size=100)

        # First run: updated the row; second run: skipped it
        assert stats1["updated"] >= 1
        assert stats2["skipped"] >= stats1["updated"]
        assert stats2["updated"] == 0

    def test_backfill_dry_run_does_not_write(self, initialized_db):
        """dry_run=True reports counts but writes nothing."""
        self._insert_test_jobs(initialized_db, [
            {"job_id": "11.pbs", "state": "F", "exit_status": 0, "outcome_class": None},
        ])

        from pbs_monitor.database.migrations import DatabaseMigration
        m = DatabaseMigration(initialized_db)
        stats = m.backfill_exit_status_and_outcome_class(batch_size=100, dry_run=True)

        assert stats["dry_run"] is True
        # The row should still be unclassified after dry run
        row = self._read_job(initialized_db, "11.pbs")
        assert row[1] is None  # outcome_class not written


# ---------------------------------------------------------------------------
# Integration: JobConverter.to_database populates outcome_class on new jobs
# ---------------------------------------------------------------------------

class TestJobConverterOutcomeClass:
    """Verify that the model_converter wires outcome_class for new collections."""

    def _make_pbs_job(self, state_str, exit_status, runtime=None, walltime=None):
        """Build a minimal PBSJob for conversion tests."""
        from pbs_monitor.models.job import PBSJob, JobState
        return PBSJob(
            job_id="99.pbs",
            job_name="converter_test",
            owner="tester",
            state=JobState(state_str),
            queue="default",
            exit_status=exit_status,
            actual_runtime_seconds=runtime,
            walltime=walltime,
        )

    def test_converter_sets_success(self):
        from pbs_monitor.database.model_converters import JobConverter
        job = self._make_pbs_job("F", 0)
        db_job = JobConverter.to_database(job)
        assert db_job.outcome_class == "success"

    def test_converter_sets_error(self):
        from pbs_monitor.database.model_converters import JobConverter
        job = self._make_pbs_job("F", 1)
        db_job = JobConverter.to_database(job)
        assert db_job.outcome_class == "error"

    def test_converter_sets_signal_killed(self):
        from pbs_monitor.database.model_converters import JobConverter
        job = self._make_pbs_job("F", 137)
        db_job = JobConverter.to_database(job)
        assert db_job.outcome_class == "signal_killed"

    def test_converter_sets_walltime_killed(self):
        from pbs_monitor.database.model_converters import JobConverter
        # SIGTERM at exactly 95% of 1-hour walltime
        job = self._make_pbs_job("F", 143, runtime=3420, walltime="01:00:00")
        db_job = JobConverter.to_database(job)
        assert db_job.outcome_class == "walltime_killed"

    def test_converter_sets_requeued(self):
        from pbs_monitor.database.model_converters import JobConverter
        job = self._make_pbs_job("F", 271)
        db_job = JobConverter.to_database(job)
        assert db_job.outcome_class == "requeued"

    def test_converter_sets_could_not_run(self):
        from pbs_monitor.database.model_converters import JobConverter
        job = self._make_pbs_job("F", -20)
        db_job = JobConverter.to_database(job)
        assert db_job.outcome_class == "could_not_run"

    def test_converter_sets_walltime_killed_on_minus_29(self):
        """-29 (walltime exceeded) -> walltime_killed, not could_not_run."""
        from pbs_monitor.database.model_converters import JobConverter
        job = self._make_pbs_job("F", -29)
        db_job = JobConverter.to_database(job)
        assert db_job.outcome_class == "walltime_killed"

    def test_converter_active_job_returns_unknown(self):
        """Running job gets outcome_class='unknown' (it's still active)."""
        from pbs_monitor.database.model_converters import JobConverter
        job = self._make_pbs_job("R", None)
        db_job = JobConverter.to_database(job)
        assert db_job.outcome_class == "unknown"
