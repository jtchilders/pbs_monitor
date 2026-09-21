"""
Tests for PBS history reconciliation feature.

TDD approach: tests written first, production code to follow.

Covers:
- HistoryReconciler.reconcile(): missing insert, UNKNOWN_END repair,
  nonterminal rejection, existing-row preservation, malformed-record
  accounting, dry-run, idempotency, batch bulk state lookup.
- Atomic/conditional repository writes (no race between classify and write).
- Config defaults and overrides.
- DataCollector scheduling: async startup, single-flight, 12-hour
  completed-run interval, failure isolation, bounded stop.
- CLI subcommand: reconcile-history, --dry-run, --batch-size, summaries.
- history_reconciliation_log table: idempotently ensured, independent of
  version ladder (schema_version stays 1.4.0 on this branch).
- Audit row written on success and failure; audit failure is best-effort.

Schema version note: history_reconciliation_log is NOT a numbered migration
(v1.5 is reserved by a separate pending branch). It is ensured by
ensure_history_reconciliation_log_table() called from migrate_to_latest()
independently of the 1.0→1.4 version ladder.
"""

import os
import sys
import tempfile
import threading
import time
import types
from datetime import datetime, timezone
from typing import Dict, List, Optional, Set
from unittest.mock import MagicMock, patch, PropertyMock, call
from dataclasses import dataclass

import pytest

# ---------------------------------------------------------------------------
# Helpers – minimal stand-ins that avoid touching the real DB or PBS
# ---------------------------------------------------------------------------


def _make_pbs_job(job_id: str, state_char: str, job_name: str = "testjob"):
    """Return a minimal PBSJob-like object."""
    obj = MagicMock()
    obj.job_id = job_id
    obj.job_name = job_name
    state_mock = MagicMock()
    state_mock.value = state_char
    obj.state = state_mock
    obj.owner = "testuser"
    obj.queue = "debug"
    obj.start_time = datetime(2026, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
    obj.end_time = datetime(2026, 1, 1, 1, 0, 0, tzinfo=timezone.utc)
    return obj


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def temp_db_config():
    """SQLite temp-database config."""
    from pbs_monitor.config import Config

    f = tempfile.NamedTemporaryFile(delete=False, suffix=".db")
    f.close()
    cfg = Config()
    cfg.database.url = f"sqlite:///{f.name}"
    yield cfg
    try:
        os.unlink(f.name)
    except OSError:
        pass


@pytest.fixture
def initialized_db(temp_db_config):
    from pbs_monitor.database.migrations import DatabaseMigration
    dm = DatabaseMigration(temp_db_config)
    dm.migrate_to_latest()
    yield temp_db_config


# ===========================================================================
# SECTION 1 – HistoryReconciler core logic
# ===========================================================================


class TestReconciliationResult:
    """ReconciliationResult dataclass exists and has required fields."""

    def test_result_fields(self):
        from pbs_monitor.history_reconciliation import ReconciliationResult

        r = ReconciliationResult()
        for attr in (
            "observed",
            "terminal_candidates",
            "inserted",
            "repaired",
            "unchanged",
            "parse_errors",
            "duration_seconds",
            "status",
            "error_message",
        ):
            assert hasattr(r, attr), f"missing field: {attr}"

    def test_result_oldest_newest(self):
        from pbs_monitor.history_reconciliation import ReconciliationResult

        r = ReconciliationResult()
        assert hasattr(r, "oldest_terminal_time")
        assert hasattr(r, "newest_terminal_time")


class TestHistoryReconcilerMissingInsert:
    """F and C jobs absent from the DB are inserted."""

    def _make_reconciler(self, pbs_jobs, db_states: Dict[str, str]):
        from pbs_monitor.history_reconciliation import HistoryReconciler

        pbs_mock = MagicMock()
        pbs_mock.qstat_completed_jobs.return_value = pbs_jobs

        repo_mock = MagicMock()
        repo_mock.fetch_states_for_ids.return_value = db_states
        # Return actual-count dict matching planned lengths (no races in these tests).
        repo_mock.apply_reconciliation_batch.side_effect = (
            lambda to_insert, to_repair: {"inserted": len(to_insert), "repaired": len(to_repair)}
        )

        converter_mock = MagicMock()
        converter_mock.to_database.side_effect = lambda pbs_job: {"job_id": pbs_job.job_id}

        return HistoryReconciler(
            pbs_commands=pbs_mock,
            repository=repo_mock,
            converter=converter_mock,
        ), repo_mock

    def test_missing_f_job_inserted(self):
        job = _make_pbs_job("12345.pbs", "F")
        reconciler, repo = self._make_reconciler([job], {})

        result = reconciler.reconcile()

        assert result.inserted == 1
        assert result.repaired == 0
        assert result.unchanged == 0
        repo.apply_reconciliation_batch.assert_called_once()

    def test_missing_c_job_inserted(self):
        job = _make_pbs_job("22222.pbs", "C")
        reconciler, repo = self._make_reconciler([job], {})

        result = reconciler.reconcile()

        assert result.inserted == 1

    def test_both_f_and_c_inserted(self):
        jobs = [_make_pbs_job("1.pbs", "F"), _make_pbs_job("2.pbs", "C")]
        reconciler, repo = self._make_reconciler(jobs, {})

        result = reconciler.reconcile()

        assert result.inserted == 2

    def test_terminal_candidates_counted(self):
        jobs = [_make_pbs_job("1.pbs", "F"), _make_pbs_job("2.pbs", "C")]
        reconciler, _ = self._make_reconciler(jobs, {})

        result = reconciler.reconcile()

        assert result.terminal_candidates == 2
        assert result.observed == 2


class TestHistoryReconcilerUnknownEndRepair:
    """Existing UNKNOWN_END rows are replaced from F/C records."""

    def _make_reconciler(self, pbs_jobs, db_states: Dict[str, str]):
        from pbs_monitor.history_reconciliation import HistoryReconciler

        pbs_mock = MagicMock()
        pbs_mock.qstat_completed_jobs.return_value = pbs_jobs

        repo_mock = MagicMock()
        repo_mock.fetch_states_for_ids.return_value = db_states
        # Return actual-count dict matching planned lengths (no races in these tests).
        repo_mock.apply_reconciliation_batch.side_effect = (
            lambda to_insert, to_repair: {"inserted": len(to_insert), "repaired": len(to_repair)}
        )

        converter_mock = MagicMock()
        converter_mock.to_database.side_effect = lambda pbs_job: {"job_id": pbs_job.job_id}

        return HistoryReconciler(
            pbs_commands=pbs_mock,
            repository=repo_mock,
            converter=converter_mock,
        ), repo_mock

    def test_unknown_end_repaired(self):
        job = _make_pbs_job("33333.pbs", "F")
        reconciler, repo = self._make_reconciler([job], {"33333.pbs": "UNKNOWN_END"})

        result = reconciler.reconcile()

        assert result.repaired == 1
        assert result.inserted == 0
        assert result.unchanged == 0

    def test_unknown_end_c_repaired(self):
        job = _make_pbs_job("44444.pbs", "C")
        reconciler, repo = self._make_reconciler([job], {"44444.pbs": "UNKNOWN_END"})

        result = reconciler.reconcile()

        assert result.repaired == 1


class TestHistoryReconcilerNonterminalRejection:
    """E, R, Q, H records are not imported."""

    def _make_reconciler(self, pbs_jobs, db_states=None):
        from pbs_monitor.history_reconciliation import HistoryReconciler

        pbs_mock = MagicMock()
        pbs_mock.qstat_completed_jobs.return_value = pbs_jobs

        repo_mock = MagicMock()
        repo_mock.fetch_states_for_ids.return_value = db_states or {}
        # Return actual-count dict matching planned lengths (no races in these tests).
        repo_mock.apply_reconciliation_batch.side_effect = (
            lambda to_insert, to_repair: {"inserted": len(to_insert), "repaired": len(to_repair)}
        )

        converter_mock = MagicMock()
        converter_mock.to_database.side_effect = lambda pbs_job: {"job_id": pbs_job.job_id}

        return HistoryReconciler(
            pbs_commands=pbs_mock,
            repository=repo_mock,
            converter=converter_mock,
        ), repo_mock

    @pytest.mark.parametrize("state_char", ["E", "R", "Q", "H"])
    def test_nonterminal_not_inserted(self, state_char):
        job = _make_pbs_job("55555.pbs", state_char)
        reconciler, repo = self._make_reconciler([job])

        result = reconciler.reconcile()

        assert result.inserted == 0
        assert result.repaired == 0
        assert result.terminal_candidates == 0

    def test_e_record_not_imported(self):
        job = _make_pbs_job("66666.pbs", "E")
        reconciler, repo = self._make_reconciler([job])

        result = reconciler.reconcile()

        repo.apply_reconciliation_batch.assert_not_called()


class TestHistoryReconcilerPreservesExisting:
    """Rows in any non-UNKNOWN_END state are left untouched."""

    def _make_reconciler(self, pbs_jobs, db_states: Dict[str, str]):
        from pbs_monitor.history_reconciliation import HistoryReconciler

        pbs_mock = MagicMock()
        pbs_mock.qstat_completed_jobs.return_value = pbs_jobs

        repo_mock = MagicMock()
        repo_mock.fetch_states_for_ids.return_value = db_states
        # Return actual-count dict matching planned lengths (no races in these tests).
        repo_mock.apply_reconciliation_batch.side_effect = (
            lambda to_insert, to_repair: {"inserted": len(to_insert), "repaired": len(to_repair)}
        )

        converter_mock = MagicMock()
        converter_mock.to_database.side_effect = lambda pbs_job: {"job_id": pbs_job.job_id}

        return HistoryReconciler(
            pbs_commands=pbs_mock,
            repository=repo_mock,
            converter=converter_mock,
        ), repo_mock

    @pytest.mark.parametrize("existing_state", ["F", "C", "R", "Q"])
    def test_existing_non_unknown_not_mutated(self, existing_state):
        job = _make_pbs_job("77777.pbs", "F")
        reconciler, repo = self._make_reconciler([job], {"77777.pbs": existing_state})

        result = reconciler.reconcile()

        assert result.unchanged == 1
        assert result.inserted == 0
        assert result.repaired == 0

    def test_mixed_batch(self):
        """One missing, one UNKNOWN_END, one existing terminal."""
        jobs = [
            _make_pbs_job("1.pbs", "F"),  # missing → insert
            _make_pbs_job("2.pbs", "F"),  # UNKNOWN_END → repair
            _make_pbs_job("3.pbs", "C"),  # existing C → unchanged
        ]
        db_states = {"2.pbs": "UNKNOWN_END", "3.pbs": "C"}
        reconciler, repo = self._make_reconciler(jobs, db_states)

        result = reconciler.reconcile()

        assert result.inserted == 1
        assert result.repaired == 1
        assert result.unchanged == 1


class TestHistoryReconcilerMalformedRecords:
    """Malformed individual records are counted and skipped; total parse failure = no writes."""

    def test_malformed_individual_counted(self):
        from pbs_monitor.history_reconciliation import HistoryReconciler

        pbs_mock = MagicMock()
        bad_job = _make_pbs_job("bad.pbs", "F")
        good_job = _make_pbs_job("good.pbs", "F")
        pbs_mock.qstat_completed_jobs.return_value = [bad_job, good_job]

        repo_mock = MagicMock()
        repo_mock.fetch_states_for_ids.return_value = {}
        repo_mock.apply_reconciliation_batch.side_effect = (
            lambda to_insert, to_repair: {"inserted": len(to_insert), "repaired": len(to_repair)}
        )

        converter_mock = MagicMock()

        def _convert(j):
            if j.job_id == "bad.pbs":
                raise ValueError("bad field")
            return {"job_id": j.job_id}

        converter_mock.to_database.side_effect = _convert

        reconciler = HistoryReconciler(
            pbs_commands=pbs_mock,
            repository=repo_mock,
            converter=converter_mock,
        )
        result = reconciler.reconcile()

        assert result.parse_errors >= 1
        assert result.inserted == 1  # good job still inserted

    def test_whole_response_failure_no_writes(self):
        from pbs_monitor.history_reconciliation import HistoryReconciler
        from pbs_monitor.pbs_commands import PBSCommandError

        pbs_mock = MagicMock()
        pbs_mock.qstat_completed_jobs.side_effect = PBSCommandError("qstat failed")

        repo_mock = MagicMock()
        converter_mock = MagicMock()

        reconciler = HistoryReconciler(
            pbs_commands=pbs_mock,
            repository=repo_mock,
            converter=converter_mock,
        )
        result = reconciler.reconcile()

        repo_mock.apply_reconciliation_batch.assert_not_called()
        assert result.status == "failed"
        assert result.error_message is not None


class TestHistoryReconcilerDryRun:
    """Dry-run classifies but writes nothing."""

    def _make_reconciler(self, pbs_jobs, db_states):
        from pbs_monitor.history_reconciliation import HistoryReconciler

        pbs_mock = MagicMock()
        pbs_mock.qstat_completed_jobs.return_value = pbs_jobs

        repo_mock = MagicMock()
        repo_mock.fetch_states_for_ids.return_value = db_states
        # Dry-run never calls apply_reconciliation_batch; side_effect is unused
        # but must satisfy the contract if called unexpectedly.
        repo_mock.apply_reconciliation_batch.side_effect = (
            lambda to_insert, to_repair: {"inserted": len(to_insert), "repaired": len(to_repair)}
        )

        converter_mock = MagicMock()
        converter_mock.to_database.side_effect = lambda j: {"job_id": j.job_id}

        return HistoryReconciler(
            pbs_commands=pbs_mock,
            repository=repo_mock,
            converter=converter_mock,
        ), repo_mock

    def test_dry_run_no_writes(self):
        jobs = [_make_pbs_job("1.pbs", "F"), _make_pbs_job("2.pbs", "C")]
        reconciler, repo = self._make_reconciler(jobs, {"2.pbs": "UNKNOWN_END"})

        result = reconciler.reconcile(dry_run=True)

        repo.apply_reconciliation_batch.assert_not_called()
        # But classification is done
        assert result.inserted == 1
        assert result.repaired == 1

    def test_dry_run_status_success(self):
        reconciler, _ = self._make_reconciler([_make_pbs_job("1.pbs", "F")], {})
        result = reconciler.reconcile(dry_run=True)
        assert result.status == "success"


class TestHistoryReconcilerIdempotent:
    """Running twice with the same data: second run writes nothing."""

    def test_second_run_all_unchanged(self):
        from pbs_monitor.history_reconciliation import HistoryReconciler

        pbs_mock = MagicMock()
        pbs_mock.qstat_completed_jobs.return_value = [_make_pbs_job("1.pbs", "F")]

        repo_mock = MagicMock()
        # After first run the job is now 'F' in db
        repo_mock.fetch_states_for_ids.return_value = {"1.pbs": "F"}

        converter_mock = MagicMock()
        converter_mock.to_database.side_effect = lambda j: {"job_id": j.job_id}

        reconciler = HistoryReconciler(
            pbs_commands=pbs_mock,
            repository=repo_mock,
            converter=converter_mock,
        )
        result = reconciler.reconcile()

        assert result.unchanged == 1
        assert result.inserted == 0
        repo_mock.apply_reconciliation_batch.assert_not_called()


class TestAtomicConditionalWrites:
    """apply_reconciliation_batch must use atomic conditional SQL, not generic upsert."""

    def test_insert_uses_conflict_do_nothing(self, initialized_db):
        """Inserting same job_id twice doesn't raise; second call is a no-op."""
        from pbs_monitor.database.repositories import JobRepository

        repo = JobRepository(initialized_db)
        job_dict = {"job_id": "atomic_test.pbs", "state": "F", "owner": "alice", "queue": "debug"}
        # First insert
        repo.apply_reconciliation_batch(to_insert=[job_dict], to_repair=[])
        # Second insert (same job_id) – must not raise, must not overwrite
        repo.apply_reconciliation_batch(to_insert=[job_dict], to_repair=[])

        result = repo.get_job_by_id("atomic_test.pbs")
        assert result is not None

    def test_repair_guarded_by_unknown_end_state(self, initialized_db):
        """Repair UPDATE applies only when state is still UNKNOWN_END."""
        from pbs_monitor.database.repositories import JobRepository
        from pbs_monitor.database.models import Job, JobState

        repo = JobRepository(initialized_db)

        # Insert a job that is already FINISHED (not UNKNOWN_END)
        with repo.get_session() as session:
            j = Job(job_id="guarded.pbs", state=JobState.FINISHED, owner="bob", queue="debug")
            session.add(j)
            session.commit()

        # Attempt repair – should be a no-op because state != UNKNOWN_END
        repair_dict = {"job_id": "guarded.pbs", "state": "C", "owner": "eve", "queue": "prod"}
        repo.apply_reconciliation_batch(to_insert=[], to_repair=[repair_dict])

        # Owner should still be "bob", not "eve"
        updated = repo.get_job_by_id("guarded.pbs")
        assert updated.state.value == "F"  # unchanged

    def test_repair_applies_when_unknown_end(self, initialized_db):
        """Repair UPDATE applies when state IS UNKNOWN_END."""
        from pbs_monitor.database.repositories import JobRepository
        from pbs_monitor.database.models import Job, JobState

        repo = JobRepository(initialized_db)

        with repo.get_session() as session:
            j = Job(job_id="repairable.pbs", state=JobState.UNKNOWN_END, owner="old", queue="debug")
            session.add(j)
            session.commit()

        repair_dict = {"job_id": "repairable.pbs", "state": "F", "owner": "new", "queue": "debug"}
        repo.apply_reconciliation_batch(to_insert=[], to_repair=[repair_dict])

        updated = repo.get_job_by_id("repairable.pbs")
        assert updated.state.value == "F"

    def test_race_simulation_insert_then_classify_missing(self, initialized_db):
        """
        Race: job classified as 'missing' but becomes active between classify and write.
        The conflict-do-nothing INSERT must leave the active row intact.
        """
        from pbs_monitor.database.repositories import JobRepository
        from pbs_monitor.database.models import Job, JobState

        repo = JobRepository(initialized_db)

        # Simulate race: job appears in DB as RUNNING after classification
        with repo.get_session() as session:
            j = Job(job_id="race.pbs", state=JobState.RUNNING, owner="runner", queue="debug")
            session.add(j)
            session.commit()

        # Reconciler tries to insert it (classified as missing)
        insert_dict = {"job_id": "race.pbs", "state": "F", "owner": "runner", "queue": "debug"}
        # Must not raise, must not overwrite RUNNING with F
        repo.apply_reconciliation_batch(to_insert=[insert_dict], to_repair=[])

        # Row should still be RUNNING
        updated = repo.get_job_by_id("race.pbs")
        assert updated.state.value == "R"

    def test_unsupported_dialect_raises_value_error(self, initialized_db):
        """
        apply_reconciliation_batch must raise ValueError for any dialect other
        than 'postgresql' or 'sqlite'. Project fails closed rather than silently
        applying SQLite semantics to an unsupported engine.
        """
        from pbs_monitor.database.repositories import JobRepository
        from unittest.mock import patch, MagicMock

        repo = JobRepository(initialized_db)
        job_dict = {"job_id": "dialect_test.pbs", "state": "F", "owner": "x", "queue": "q"}

        # Patch session.get_bind() to report an unsupported dialect.
        # (Uses get_bind() rather than .bind — the API change in Q-1 fix.)
        fake_dialect = MagicMock()
        fake_dialect.name = "mysql"
        fake_bind = MagicMock()
        fake_bind.dialect = fake_dialect

        original_get_session = repo.get_session

        from contextlib import contextmanager

        @contextmanager
        def patched_get_session():
            with original_get_session() as session:
                # Override get_bind() to return a fake "mysql" bind
                session.get_bind = lambda: fake_bind
                yield session

        with patch.object(repo, "get_session", patched_get_session):
            with pytest.raises(ValueError, match="Unsupported database dialect"):
                repo.apply_reconciliation_batch(to_insert=[job_dict], to_repair=[])


class TestRepositoryBulkOps:
    """fetch_states_for_ids uses chunked IN queries, not per-job lookups."""

    def test_fetch_states_for_ids_exists(self, initialized_db):
        from pbs_monitor.database.repositories import JobRepository

        repo = JobRepository(initialized_db)
        result = repo.fetch_states_for_ids(set())
        assert isinstance(result, dict)

    def test_fetch_states_for_ids_returns_known(self, initialized_db):
        from pbs_monitor.database.repositories import JobRepository
        from pbs_monitor.database.models import Job, JobState

        repo = JobRepository(initialized_db)
        with repo.get_session() as session:
            j = Job(job_id="test_bulk.pbs", state=JobState.FINISHED)
            session.add(j)
            session.commit()

        result = repo.fetch_states_for_ids({"test_bulk.pbs", "missing.pbs"})
        assert "test_bulk.pbs" in result
        assert result["test_bulk.pbs"] == "F"
        assert "missing.pbs" not in result

    def test_apply_reconciliation_batch_exists(self, initialized_db):
        from pbs_monitor.database.repositories import JobRepository

        repo = JobRepository(initialized_db)
        repo.apply_reconciliation_batch(to_insert=[], to_repair=[])

    def test_apply_reconciliation_batch_inserts(self, initialized_db):
        from pbs_monitor.database.repositories import JobRepository

        repo = JobRepository(initialized_db)
        job_dict = {
            "job_id": "newjob.pbs",
            "state": "F",
            "owner": "alice",
            "queue": "debug",
        }
        repo.apply_reconciliation_batch(to_insert=[job_dict], to_repair=[])

        result = repo.get_job_by_id("newjob.pbs")
        assert result is not None

    def test_apply_reconciliation_batch_repairs_unknown_end(self, initialized_db):
        from pbs_monitor.database.repositories import JobRepository
        from pbs_monitor.database.models import Job, JobState

        repo = JobRepository(initialized_db)
        with repo.get_session() as session:
            j = Job(job_id="unk.pbs", state=JobState.UNKNOWN_END)
            session.add(j)
            session.commit()

        repair_dict = {
            "job_id": "unk.pbs",
            "state": "F",
            "owner": "bob",
            "queue": "debug",
        }
        repo.apply_reconciliation_batch(to_insert=[], to_repair=[repair_dict])

        updated = repo.get_job_by_id("unk.pbs")
        assert updated.state.value == "F"

    def test_fetch_states_chunked(self, initialized_db):
        """Fetching many IDs in one call works (tests chunked SQL)."""
        from pbs_monitor.database.repositories import JobRepository
        from pbs_monitor.database.models import Job, JobState

        repo = JobRepository(initialized_db)
        with repo.get_session() as session:
            for i in range(150):
                j = Job(job_id=f"bulk_{i}.pbs", state=JobState.FINISHED)
                session.add(j)
            session.commit()

        ids = {f"bulk_{i}.pbs" for i in range(150)}
        result = repo.fetch_states_for_ids(ids)
        assert len(result) == 150


# ===========================================================================
# SECTION 2 – Config defaults and overrides
# ===========================================================================


class TestConfigDefaults:
    def test_history_reconciliation_enabled_default(self):
        from pbs_monitor.config import DatabaseConfig

        cfg = DatabaseConfig()
        assert cfg.history_reconciliation_enabled is True

    def test_history_reconciliation_interval_default(self):
        from pbs_monitor.config import DatabaseConfig

        cfg = DatabaseConfig()
        assert cfg.history_reconciliation_interval_seconds == 43200

    def test_history_reconciliation_batch_size_default(self):
        from pbs_monitor.config import DatabaseConfig

        cfg = DatabaseConfig()
        assert cfg.history_reconciliation_batch_size == 500

    def test_config_override_via_yaml(self, tmp_path):
        import yaml
        from pbs_monitor.config import Config

        cfg_file = tmp_path / "test.yaml"
        cfg_file.write_text(
            yaml.dump(
                {
                    "database": {
                        "history_reconciliation_enabled": False,
                        "history_reconciliation_interval_seconds": 7200,
                        "history_reconciliation_batch_size": 100,
                    }
                }
            )
        )
        cfg = Config(str(cfg_file))
        assert cfg.database.history_reconciliation_enabled is False
        assert cfg.database.history_reconciliation_interval_seconds == 7200
        assert cfg.database.history_reconciliation_batch_size == 100


# ===========================================================================
# SECTION 3 – DataCollector scheduling
# ===========================================================================


class TestDataCollectorScheduling:
    """Startup is nonblocking; scheduling and single-flight work correctly."""

    def test_start_background_returns_immediately(self):
        """start_background_updates must return without waiting for reconciliation."""
        from pbs_monitor.config import Config
        from pbs_monitor.data_collector import DataCollector

        cfg = Config()
        cfg.database.url = "sqlite:///:memory:"
        dc = DataCollector(config=cfg, use_sample_data=True, enable_database=False)

        started = time.time()
        dc.start_background_updates()
        elapsed = time.time() - started
        dc.stop_background_updates()

        assert elapsed < 2.0, f"start_background_updates blocked for {elapsed:.1f}s"

    def test_stop_background_updates_bounded(self):
        """stop_background_updates must return within a bounded time."""
        from pbs_monitor.config import Config
        from pbs_monitor.data_collector import DataCollector

        cfg = Config()
        cfg.database.url = "sqlite:///:memory:"
        dc = DataCollector(config=cfg, use_sample_data=True, enable_database=False)
        dc.start_background_updates()

        started = time.time()
        dc.stop_background_updates()
        elapsed = time.time() - started

        assert elapsed < 10.0, f"stop_background_updates took {elapsed:.1f}s"

    def test_reconciliation_worker_attribute_exists(self):
        """DataCollector has _reconciliation_thread attribute."""
        from pbs_monitor.config import Config
        from pbs_monitor.data_collector import DataCollector

        cfg = Config()
        cfg.database.url = "sqlite:///:memory:"
        dc = DataCollector(config=cfg, use_sample_data=True, enable_database=False)
        assert hasattr(dc, "_reconciliation_thread")

    def test_reconciliation_lock_exists(self):
        """DataCollector has _reconciliation_lock attribute."""
        from pbs_monitor.config import Config
        from pbs_monitor.data_collector import DataCollector

        cfg = Config()
        cfg.database.url = "sqlite:///:memory:"
        dc = DataCollector(config=cfg, use_sample_data=True, enable_database=False)
        assert hasattr(dc, "_reconciliation_lock")

    def test_reconciliation_last_completed_exists(self):
        """DataCollector has _last_reconciliation_completed attribute."""
        from pbs_monitor.config import Config
        from pbs_monitor.data_collector import DataCollector

        cfg = Config()
        cfg.database.url = "sqlite:///:memory:"
        dc = DataCollector(config=cfg, use_sample_data=True, enable_database=False)
        assert hasattr(dc, "_last_reconciliation_completed")

    def test_overlap_suppression_via_lock(self):
        """A second reconciliation request while one is running is skipped (single-flight)."""
        from pbs_monitor.config import Config
        from pbs_monitor.data_collector import DataCollector

        cfg = Config()
        cfg.database.url = "sqlite:///:memory:"
        dc = DataCollector(config=cfg, use_sample_data=True, enable_database=False)

        call_count = {"n": 0}

        def _mock_reconcile(*args, **kwargs):
            call_count["n"] += 1
            from pbs_monitor.history_reconciliation import ReconciliationResult
            return ReconciliationResult()

        dc._run_reconciliation = _mock_reconcile

        # Simulate a running reconciliation by holding the lock
        assert dc._reconciliation_lock.acquire(blocking=False)
        try:
            dc._try_reconcile_once()
        finally:
            dc._reconciliation_lock.release()

        assert call_count["n"] == 0

    def test_failure_isolation(self):
        """Reconciliation exception does not propagate to caller."""
        from pbs_monitor.config import Config
        from pbs_monitor.data_collector import DataCollector

        cfg = Config()
        cfg.database.url = "sqlite:///:memory:"
        dc = DataCollector(config=cfg, use_sample_data=True, enable_database=False)

        def _failing_reconcile(*args, **kwargs):
            raise RuntimeError("PBS exploded")

        dc._run_reconciliation = _failing_reconcile
        # Must not raise
        dc._try_reconcile_once()

    def test_try_reconcile_once_exists(self):
        """DataCollector has _try_reconcile_once method."""
        from pbs_monitor.config import Config
        from pbs_monitor.data_collector import DataCollector

        cfg = Config()
        cfg.database.url = "sqlite:///:memory:"
        dc = DataCollector(config=cfg, use_sample_data=True, enable_database=False)
        assert callable(getattr(dc, "_try_reconcile_once", None))


# ===========================================================================
# SECTION 4 – CLI subcommand
# ===========================================================================


class TestCLIReconcileHistory:
    """pbs-monitor database reconcile-history subcommand."""

    def _invoke(self, extra_args=None):
        from pbs_monitor.cli.main import create_parser

        parser = create_parser()
        argv = ["database", "reconcile-history"] + (extra_args or [])
        return parser.parse_args(argv)

    def test_subcommand_exists(self):
        args = self._invoke()
        assert args.database_action == "reconcile-history"

    def test_dry_run_flag_default_false(self):
        args = self._invoke()
        assert args.dry_run is False

    def test_dry_run_flag_set(self):
        args = self._invoke(["--dry-run"])
        assert args.dry_run is True

    def test_batch_size_default(self):
        args = self._invoke()
        assert args.batch_size == 500

    def test_batch_size_override(self):
        args = self._invoke(["--batch-size", "100"])
        assert args.batch_size == 100

    def test_command_routes_to_reconcile_history(self):
        """DatabaseCommand.execute dispatches 'reconcile-history' without error."""
        from pbs_monitor.cli.commands import DatabaseCommand
        from pbs_monitor.history_reconciliation import ReconciliationResult

        mock_result = ReconciliationResult()
        mock_result.status = "success"
        mock_result.observed = 10
        mock_result.terminal_candidates = 5
        mock_result.inserted = 2
        mock_result.repaired = 1
        mock_result.unchanged = 2
        mock_result.parse_errors = 0
        mock_result.duration_seconds = 1.5

        with patch("pbs_monitor.cli.commands.HistoryReconciler") as MockReconciler:
            MockReconciler.return_value.reconcile.return_value = mock_result

            collector_mock = MagicMock()
            config_mock = MagicMock()
            config_mock.display.auto_width = False
            config_mock.display.max_table_width = 120
            config_mock.display.use_colors = False
            config_mock.pbs.command_timeout = 30

            with patch("pbs_monitor.cli.commands.RepositoryFactory") as MockRepo, \
                 patch("pbs_monitor.cli.commands.PBSCommands"), \
                 patch("pbs_monitor.cli.commands.ModelConverters"):
                MockRepo.return_value.get_job_repository.return_value = MagicMock()

                cmd = DatabaseCommand(collector=collector_mock, config=config_mock)

                args = MagicMock()
                args.database_action = "reconcile-history"
                args.dry_run = False
                args.batch_size = 500

                rc = cmd.execute(args)
                assert rc == 0

    def test_failed_reconciliation_returns_nonzero(self):
        """CLI returns nonzero exit code when reconciliation fails."""
        from pbs_monitor.cli.commands import DatabaseCommand
        from pbs_monitor.history_reconciliation import ReconciliationResult

        mock_result = ReconciliationResult()
        mock_result.status = "failed"
        mock_result.error_message = "qstat timed out"
        mock_result.observed = 0
        mock_result.terminal_candidates = 0
        mock_result.inserted = 0
        mock_result.repaired = 0
        mock_result.unchanged = 0
        mock_result.parse_errors = 0
        mock_result.duration_seconds = 0.1

        with patch("pbs_monitor.cli.commands.HistoryReconciler") as MockReconciler:
            MockReconciler.return_value.reconcile.return_value = mock_result

            collector_mock = MagicMock()
            config_mock = MagicMock()
            config_mock.display.auto_width = False
            config_mock.display.max_table_width = 120
            config_mock.display.use_colors = False
            config_mock.pbs.command_timeout = 30

            with patch("pbs_monitor.cli.commands.RepositoryFactory") as MockRepo, \
                 patch("pbs_monitor.cli.commands.PBSCommands"), \
                 patch("pbs_monitor.cli.commands.ModelConverters"):
                MockRepo.return_value.get_job_repository.return_value = MagicMock()

                cmd = DatabaseCommand(collector=collector_mock, config=config_mock)
                args = MagicMock()
                args.database_action = "reconcile-history"
                args.dry_run = False
                args.batch_size = 500

                rc = cmd.execute(args)
                assert rc != 0


# ===========================================================================
# SECTION 5 – Audit model / migration (idempotent table, not numbered)
# ===========================================================================


class TestAuditTableEnsured:
    """history_reconciliation_log table is ensured by migrate_to_latest()."""

    def test_table_created_after_migrate_to_latest(self, initialized_db):
        from pbs_monitor.database.migrations import DatabaseMigration
        from sqlalchemy import inspect as sa_inspect

        dm = DatabaseMigration(initialized_db)
        inspector = sa_inspect(dm.db_manager.engine)
        assert "history_reconciliation_log" in inspector.get_table_names()

    def test_schema_version_stays_140(self, initialized_db):
        """Version stays 1.4.0 — history_reconciliation_log is not a numbered migration."""
        from pbs_monitor.database.migrations import DatabaseMigration

        dm = DatabaseMigration(initialized_db)
        assert dm.check_schema_version() == "1.4.0"

    def test_table_has_required_columns(self, initialized_db):
        from pbs_monitor.database.migrations import DatabaseMigration
        from sqlalchemy import inspect as sa_inspect

        dm = DatabaseMigration(initialized_db)
        inspector = sa_inspect(dm.db_manager.engine)
        cols = {c["name"] for c in inspector.get_columns("history_reconciliation_log")}
        required = {
            "id",
            "start_time",
            "end_time",
            "status",
            "observed",
            "terminal_candidates",
            "inserted",
            "repaired",
            "unchanged",
            "parse_errors",
            "oldest_terminal_time",
            "newest_terminal_time",
            "duration_seconds",
            "error_message",
        }
        missing = required - cols
        assert not missing, f"Missing columns: {missing}"

    def test_table_ensured_idempotent(self, initialized_db):
        """Calling ensure_history_reconciliation_log_table twice doesn't raise."""
        from pbs_monitor.database.migrations import DatabaseMigration

        dm = DatabaseMigration(initialized_db)
        dm.ensure_history_reconciliation_log_table()
        dm.ensure_history_reconciliation_log_table()

    def test_v14_db_gets_table_without_version_change(self, temp_db_config):
        """A v1.4-shaped DB gets the table but stays at 1.4.0."""
        from pbs_monitor.database.migrations import DatabaseMigration
        from sqlalchemy import inspect as sa_inspect

        dm = DatabaseMigration(temp_db_config)
        dm.migrate_to_latest()  # fresh DB → all tables including hrlog
        assert dm.check_schema_version() == "1.4.0"
        inspector = sa_inspect(dm.db_manager.engine)
        assert "history_reconciliation_log" in inspector.get_table_names()

    def test_fresh_db_has_table(self, temp_db_config):
        """A brand-new database includes history_reconciliation_log."""
        from pbs_monitor.database.migrations import DatabaseMigration
        from sqlalchemy import inspect as sa_inspect

        dm = DatabaseMigration(temp_db_config)
        dm.create_fresh_database()
        inspector = sa_inspect(dm.db_manager.engine)
        assert "history_reconciliation_log" in inspector.get_table_names()


class TestAuditModel:
    """HistoryReconciliationLog ORM model."""

    def test_model_importable(self):
        from pbs_monitor.database.models import HistoryReconciliationLog  # noqa: F401

    def test_model_tablename(self):
        from pbs_monitor.database.models import HistoryReconciliationLog

        assert HistoryReconciliationLog.__tablename__ == "history_reconciliation_log"


class TestAuditRowWritten:
    """Reconciler writes an audit row after each run."""

    def _make_reconciler_with_audit_repo(self, pbs_jobs, db_states, audit_repo):
        from pbs_monitor.history_reconciliation import HistoryReconciler

        pbs_mock = MagicMock()
        pbs_mock.qstat_completed_jobs.return_value = pbs_jobs

        repo_mock = MagicMock()
        repo_mock.fetch_states_for_ids.return_value = db_states
        repo_mock.apply_reconciliation_batch.side_effect = (
            lambda to_insert, to_repair: {"inserted": len(to_insert), "repaired": len(to_repair)}
        )

        converter_mock = MagicMock()
        converter_mock.to_database.side_effect = lambda j: {"job_id": j.job_id}

        return HistoryReconciler(
            pbs_commands=pbs_mock,
            repository=repo_mock,
            converter=converter_mock,
            audit_repository=audit_repo,
        )

    def test_success_audit_row_written(self):
        audit_repo = MagicMock()
        reconciler = self._make_reconciler_with_audit_repo(
            [_make_pbs_job("1.pbs", "F")], {}, audit_repo
        )
        reconciler.reconcile()
        audit_repo.write_reconciliation_log.assert_called_once()

    def test_failure_audit_row_written(self):
        from pbs_monitor.pbs_commands import PBSCommandError

        audit_repo = MagicMock()
        pbs_mock = MagicMock()
        pbs_mock.qstat_completed_jobs.side_effect = PBSCommandError("network error")
        repo_mock = MagicMock()
        converter_mock = MagicMock()

        from pbs_monitor.history_reconciliation import HistoryReconciler

        reconciler = HistoryReconciler(
            pbs_commands=pbs_mock,
            repository=repo_mock,
            converter=converter_mock,
            audit_repository=audit_repo,
        )
        reconciler.reconcile()
        audit_repo.write_reconciliation_log.assert_called_once()

    def test_audit_failure_does_not_rollback_job_batches(self):
        """If the audit write raises, job inserts already committed are kept."""
        audit_repo = MagicMock()
        audit_repo.write_reconciliation_log.side_effect = RuntimeError("DB full")

        job = _make_pbs_job("1.pbs", "F")
        from pbs_monitor.history_reconciliation import HistoryReconciler

        pbs_mock = MagicMock()
        pbs_mock.qstat_completed_jobs.return_value = [job]
        repo_mock = MagicMock()
        repo_mock.fetch_states_for_ids.return_value = {}
        repo_mock.apply_reconciliation_batch.side_effect = (
            lambda to_insert, to_repair: {"inserted": len(to_insert), "repaired": len(to_repair)}
        )
        converter_mock = MagicMock()
        converter_mock.to_database.side_effect = lambda j: {"job_id": j.job_id}

        reconciler = HistoryReconciler(
            pbs_commands=pbs_mock,
            repository=repo_mock,
            converter=converter_mock,
            audit_repository=audit_repo,
        )
        # Must not raise despite audit failure
        result = reconciler.reconcile()
        # Job batch was still applied
        repo_mock.apply_reconciliation_batch.assert_called_once()


class TestAuditRepositoryWriteLog:
    """JobRepository.write_reconciliation_log persists to DB."""

    def test_write_reconciliation_log_exists(self, initialized_db):
        from pbs_monitor.database.repositories import JobRepository

        repo = JobRepository(initialized_db)
        assert callable(getattr(repo, "write_reconciliation_log", None))

    def test_write_reconciliation_log_inserts_row(self, initialized_db):
        from pbs_monitor.database.repositories import JobRepository
        from pbs_monitor.history_reconciliation import ReconciliationResult
        from sqlalchemy import text

        repo = JobRepository(initialized_db)
        r = ReconciliationResult()
        r.status = "success"
        r.observed = 10
        r.terminal_candidates = 5
        r.inserted = 2
        r.repaired = 1
        r.unchanged = 2
        r.parse_errors = 0
        r.duration_seconds = 1.23
        r.start_time = datetime(2026, 9, 21, 12, 0, 0, tzinfo=timezone.utc)
        r.end_time = datetime(2026, 9, 21, 12, 0, 1, tzinfo=timezone.utc)

        repo.write_reconciliation_log(r)

        with repo.get_session() as session:
            count = session.execute(
                text("SELECT COUNT(*) FROM history_reconciliation_log")
            ).scalar()
        assert count == 1


# ===========================================================================
# DATABASE CORRECTNESS FIXES — TDD RED tests
# BUG-1, BUG-4, BUG-6 from production correctness review
# ===========================================================================


class TestBug1EnumMemberComparison:
    """BUG-1: UPDATE WHERE state must use the enum member, not .value.

    Using Job.__table__.c.state == JobState.UNKNOWN_END.value (raw string)
    rather than the enum member bypasses SQLAlchemy's type adapter.  For
    SQLite this is accidentally correct because UNKNOWN_END.value == 'UNKNOWN_END'
    (same as the stored enum name), but the contract requires the type-safe form
    so PostgreSQL and future schema changes are safe.

    Fix: change to Job.__table__.c.state == JobState.UNKNOWN_END (no .value).
    """

    def test_repair_where_clause_uses_enum_member_not_raw_value(self, initialized_db):
        """
        The WHERE clause in the repair UPDATE must be compiled using the enum
        member so SQLAlchemy's type processor handles the binding correctly.

        We verify this by inspecting that the compiled SQL for the UPDATE
        WHERE comparison goes through the TypeDecorator/Enum processor:
        the stored value of UNKNOWN_END (the enum NAME 'UNKNOWN_END') must
        match exactly what a repair UPDATE finds — confirmed by actually
        applying a repair to a job in UNKNOWN_END state and verifying the
        count is 1, not 0.

        The critical regression guard: if the WHERE clause used JobState.FINISHED.value
        ('F') instead of JobState.FINISHED, it would silently not match any row on
        SQLite (stored as 'FINISHED', compared against 'F') — confirming the risk.
        We replicate this pattern for UNKNOWN_END via source inspection.
        """
        import ast
        import inspect
        from pbs_monitor.database.repositories import JobRepository

        # Inspect the source of apply_reconciliation_batch for the .value pattern
        source = inspect.getsource(JobRepository.apply_reconciliation_batch)

        # The fix must NOT use .value in the WHERE comparison for state
        # The problematic pattern is: c.state == JobState.UNKNOWN_END.value
        assert "JobState.UNKNOWN_END.value" not in source, (
            "BUG-1: apply_reconciliation_batch uses 'JobState.UNKNOWN_END.value' "
            "(raw string) in the WHERE clause. This bypasses SQLAlchemy's type "
            "adapter and may silently no-op on PostgreSQL. "
            "Fix: change to 'JobState.UNKNOWN_END' (enum member)."
        )

    def test_repair_actually_updates_unknown_end_row(self, initialized_db):
        """
        Repair must actually update a job in UNKNOWN_END state and return
        repaired=1.  This guards against a WHERE clause that never matches.
        """
        from pbs_monitor.database.repositories import JobRepository
        from pbs_monitor.database.models import Job, JobState

        repo = JobRepository(initialized_db)
        with repo.get_session() as session:
            j = Job(job_id="bug1_repair.pbs", state=JobState.UNKNOWN_END, owner="old", queue="q")
            session.add(j)
            session.commit()

        repair_dict = {"job_id": "bug1_repair.pbs", "state": "F", "owner": "new", "queue": "q"}
        counts = repo.apply_reconciliation_batch(to_insert=[], to_repair=[repair_dict])

        assert counts.get("repaired") == 1, (
            f"Repair of UNKNOWN_END row returned repaired={counts.get('repaired')}, expected 1. "
            "The WHERE state comparison may be broken."
        )
        updated = repo.get_job_by_id("bug1_repair.pbs")
        assert updated.state.value == "F", (
            f"After repair, state is {updated.state.value!r}, expected 'F'. "
            "The WHERE clause failed to match the UNKNOWN_END row."
        )


class TestBug6InvalidStateFailsClosed:
    """BUG-6: Invalid/unrecognized state string must fail closed — not insert a NULL-state row.

    Current code silently drops the 'state' key from col_values when
    JobState(value) raises, causing INSERT to proceed with state=NULL.
    This is a data quality defect: the row appears in the DB with no state.

    Fix: when state conversion fails, skip the row entirely (increment
    parse_errors) rather than inserting a NULL-state row.
    """

    def test_invalid_state_does_not_insert_null_state_row(self, initialized_db):
        """
        When to_insert contains a dict with an invalid 'state' value,
        apply_reconciliation_batch must NOT insert a row with state=NULL.
        Instead it must either skip the row or raise — but never silently
        produce a NULL-state row.
        """
        from pbs_monitor.database.repositories import JobRepository
        from sqlalchemy import text

        repo = JobRepository(initialized_db)
        bad_dict = {
            "job_id": "nullstate.pbs",
            "state": "NOT_A_REAL_STATE_XYZ",
            "owner": "x",
            "queue": "q",
        }
        # Should not raise, but must not insert a NULL-state row
        counts = repo.apply_reconciliation_batch(to_insert=[bad_dict], to_repair=[])

        # Verify the row is either absent or has a valid non-NULL state
        job = repo.get_job_by_id("nullstate.pbs")
        if job is not None:
            assert job.state is not None, (
                "BUG-6: apply_reconciliation_batch inserted 'nullstate.pbs' with "
                "state=NULL after receiving an invalid state string. "
                "Invalid state must cause the row to be skipped, not inserted with NULL state."
            )

    def test_invalid_state_in_insert_does_not_count_as_inserted(self, initialized_db):
        """
        A row skipped due to invalid state must NOT be counted in 'inserted'.
        Reporting inserted=1 when no valid row was written is a false count.
        """
        from pbs_monitor.database.repositories import JobRepository

        repo = JobRepository(initialized_db)
        bad_dict = {
            "job_id": "badcount.pbs",
            "state": "GARBAGE_STATE",
            "owner": "x",
            "queue": "q",
        }
        counts = repo.apply_reconciliation_batch(to_insert=[bad_dict], to_repair=[])

        # Either inserted==0 (row was skipped) or the row has a valid state
        job = repo.get_job_by_id("badcount.pbs")
        if job is None:
            # Row was skipped — inserted count must be 0
            assert counts.get("inserted") == 0, (
                f"BUG-6: Row with invalid state was skipped but inserted count is "
                f"{counts.get('inserted')} (expected 0)."
            )
        else:
            # Row was inserted — it must have a valid non-NULL state
            assert job.state is not None, (
                "BUG-6: Row with invalid state was inserted with state=NULL."
            )

    def test_missing_state_key_in_insert_dict_fails_closed(self, initialized_db):
        """
        A to_insert dict with no 'state' key must also fail closed — not insert
        a row with state=NULL.  The state field is required for reconciliation
        correctness; a missing state is as bad as an invalid one.
        """
        from pbs_monitor.database.repositories import JobRepository

        repo = JobRepository(initialized_db)
        no_state_dict = {
            "job_id": "nostate.pbs",
            # 'state' key deliberately omitted
            "owner": "x",
            "queue": "q",
        }
        counts = repo.apply_reconciliation_batch(to_insert=[no_state_dict], to_repair=[])

        job = repo.get_job_by_id("nostate.pbs")
        if job is not None:
            assert job.state is not None, (
                "BUG-6: apply_reconciliation_batch inserted 'nostate.pbs' with "
                "state=NULL when 'state' key was absent from the dict. "
                "A missing state must cause the row to be skipped."
            )


class TestBug4WriteBatchingByBatchSize:
    """BUG-4: batch_size must govern write chunking, not just read chunking.

    Currently apply_reconciliation_batch writes all to_insert/to_repair in a
    single transaction regardless of list size.  The spec requires:
    - parse ALL records first (already done)
    - THEN write in chunks of batch_size
    - if one chunk fails, do NOT falsely report success or count rows from
      other chunks as written (all-or-nothing per chunk).

    The reconciler must pass batch_size to the write path, which must chunk.
    """

    def test_reconciler_passes_batch_size_to_write_path(self):
        """
        When batch_size=2 and there are 5 records to insert, the reconciler
        must call apply_reconciliation_batch multiple times (or with a
        batch_size argument), not once with all 5 records.

        This test verifies the write path is aware of batch_size.
        """
        from pbs_monitor.history_reconciliation import HistoryReconciler

        pbs_mock = MagicMock()
        pbs_mock.qstat_completed_jobs.return_value = [
            _make_pbs_job(f"{i}.pbs", "F") for i in range(5)
        ]

        call_sizes = []

        def _apply(to_insert, to_repair, **kwargs):
            call_sizes.append(len(to_insert) + len(to_repair))
            return {"inserted": len(to_insert), "repaired": len(to_repair)}

        repo_mock = MagicMock()
        repo_mock.fetch_states_for_ids.return_value = {}  # all missing → insert
        repo_mock.apply_reconciliation_batch.side_effect = _apply

        converter_mock = MagicMock()
        converter_mock.to_database.side_effect = lambda j: {"job_id": j.job_id, "state": "F"}

        reconciler = HistoryReconciler(
            pbs_commands=pbs_mock,
            repository=repo_mock,
            converter=converter_mock,
        )
        result = reconciler.reconcile(batch_size=2)

        # With batch_size=2 and 5 records, we expect multiple smaller calls,
        # not a single call with all 5.
        max_chunk = max(call_sizes) if call_sizes else 0
        assert max_chunk <= 2, (
            f"BUG-4: write chunking not applied — apply_reconciliation_batch was "
            f"called with chunks of size {call_sizes}, but batch_size=2 requires "
            f"each chunk to be at most 2 rows. "
            f"batch_size must govern write chunking as well as read chunking."
        )

    def test_write_chunk_failure_does_not_report_false_success_counts(self):
        """
        If the second write chunk raises, the total reported count must NOT
        include rows from the failed chunk.  The reconciler must not sum
        partial progress from a failed batch call.
        """
        from pbs_monitor.history_reconciliation import HistoryReconciler

        pbs_mock = MagicMock()
        pbs_mock.qstat_completed_jobs.return_value = [
            _make_pbs_job(f"{i}.pbs", "F") for i in range(4)
        ]

        call_count = {"n": 0}

        def _apply_with_second_fail(to_insert, to_repair, **kwargs):
            call_count["n"] += 1
            if call_count["n"] == 2:
                raise RuntimeError("DB write failed on chunk 2")
            return {"inserted": len(to_insert), "repaired": len(to_repair)}

        repo_mock = MagicMock()
        repo_mock.fetch_states_for_ids.return_value = {}
        repo_mock.apply_reconciliation_batch.side_effect = _apply_with_second_fail

        converter_mock = MagicMock()
        converter_mock.to_database.side_effect = lambda j: {"job_id": j.job_id, "state": "F"}

        reconciler = HistoryReconciler(
            pbs_commands=pbs_mock,
            repository=repo_mock,
            converter=converter_mock,
        )
        result = reconciler.reconcile(batch_size=2)

        # The second chunk raised — result must be 'failed' and inserted must
        # NOT report the full 4 (which would be a false count).
        assert result.status == "failed" or result.inserted < 4, (
            f"BUG-4: After a write chunk failure, result.status={result.status!r} "
            f"and result.inserted={result.inserted}. "
            "Either status must be 'failed' or inserted must not count unwritten rows."
        )
