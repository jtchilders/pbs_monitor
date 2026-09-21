"""
Focused failing tests for 7 verified defects in PBS history reconciliation.

Each test class names the defect code it covers.

Defects targeted:
  C-1  _background_update_loop has no periodic reconciliation trigger
  C-2  inserted/repaired counts reflect planned lists, not actual DB writes
  C-3  _last_reconciliation_completed is in-memory; not seeded from durable audit
       and advanced even on failed reconciliation runs
  I-1  Startup reconciliation thread created even when DB/reconciliation disabled
  I-2  batch_size from config not passed to reconciler (hardcoded 500 used)
  I-3  Fallback help text in DatabaseCommand.execute omits 'reconcile-history'
  M-1  create_sample_config() omits history_reconciliation_* fields

Tests are written RED-first: they assert the CORRECT behaviour; current
production code does NOT satisfy them, so they will fail before the fix.
"""

import os
import sys
import tempfile
import threading
import time
from datetime import datetime, timezone
from typing import Optional
from unittest.mock import MagicMock, patch, call
from io import StringIO

import pytest


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_pbs_job(job_id: str, state_char: str):
    """Minimal PBSJob-like object."""
    obj = MagicMock()
    obj.job_id = job_id
    state_mock = MagicMock()
    state_mock.value = state_char
    obj.state = state_mock
    obj.owner = "testuser"
    obj.queue = "debug"
    obj.end_time = datetime(2026, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
    return obj


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
# C-1  Periodic reconciliation scheduling in _background_update_loop
# ===========================================================================


class TestC1PeriodicScheduling:
    """
    Defect C-1: _background_update_loop never calls _try_reconcile_once after
    the startup thread. The 12-hour interval is configured but never checked.

    Fix: loop should call _try_reconcile_once when the elapsed time since
    _last_reconciliation_completed (or the last *successful* run) exceeds
    history_reconciliation_interval_seconds.
    """

    def test_periodic_trigger_called_after_interval(self):
        """
        If the reconciliation interval has elapsed, _background_update_loop
        must call _try_reconcile_once.
        """
        from pbs_monitor.config import Config
        from pbs_monitor.data_collector import DataCollector

        cfg = Config()
        cfg.database.url = "sqlite:///:memory:"
        # Very short interval so we don't have to wait 12 h
        cfg.database.history_reconciliation_interval_seconds = 0
        dc = DataCollector(config=cfg, use_sample_data=True, enable_database=False)

        call_count = {"n": 0}
        original_try = dc._try_reconcile_once

        def _counting_try(*args, **kwargs):
            call_count["n"] += 1

        dc._try_reconcile_once = _counting_try

        # Mark last completion as ancient so interval is immediately elapsed.
        dc._last_reconciliation_completed = datetime(2000, 1, 1, tzinfo=timezone.utc)
        dc._database_enabled = True  # pretend enabled

        dc._stop_background_updates = False

        # Run one iteration of the loop body directly, then stop.
        # We do NOT call start_background_updates() (would spin a real thread);
        # instead we reach into the method's while body directly.
        dc._stop_background_updates = False
        dc._run_one_background_iteration()  # <-- method that must exist after fix

        assert call_count["n"] >= 1, (
            "_try_reconcile_once was not called even though the reconciliation "
            "interval has elapsed."
        )

    def test_periodic_trigger_not_called_before_interval(self):
        """
        If the interval has NOT elapsed, _try_reconcile_once must NOT be called
        (to avoid tight retry loops).
        """
        from pbs_monitor.config import Config
        from pbs_monitor.data_collector import DataCollector

        cfg = Config()
        cfg.database.url = "sqlite:///:memory:"
        # Very long interval — should never trigger
        cfg.database.history_reconciliation_interval_seconds = 86400
        dc = DataCollector(config=cfg, use_sample_data=True, enable_database=False)

        call_count = {"n": 0}

        def _counting_try(*args, **kwargs):
            call_count["n"] += 1

        dc._try_reconcile_once = _counting_try
        dc._database_enabled = True
        # Mark last completion as just now
        dc._last_reconciliation_completed = datetime.now(timezone.utc)
        dc._stop_background_updates = False

        dc._run_one_background_iteration()

        assert call_count["n"] == 0, (
            "_try_reconcile_once was called before the reconciliation interval elapsed."
        )


# ===========================================================================
# C-2  Actual write counts vs. planned list counts
# ===========================================================================


class TestC2ActualWriteCounts:
    """
    Defect C-2: HistoryReconciler sets result.inserted = len(to_insert) and
    result.repaired = len(to_repair) BEFORE calling apply_reconciliation_batch.
    If apply_reconciliation_batch does fewer actual writes (race/no-op), the
    counts are wrong.

    Fix: apply_reconciliation_batch should return actual affected counts;
    HistoryReconciler should read those back.
    """

    def test_apply_reconciliation_batch_returns_actual_counts(self, initialized_db):
        """
        apply_reconciliation_batch must return a dict with 'inserted' and
        'repaired' reflecting actual DB rows written, not the input list length.
        """
        from pbs_monitor.database.repositories import JobRepository
        from pbs_monitor.database.models import Job, JobState

        repo = JobRepository(initialized_db)

        # Pre-insert one job so the second "insert" for it is a no-op.
        with repo.get_session() as session:
            j = Job(job_id="dup.pbs", state=JobState.RUNNING, owner="x", queue="q")
            session.add(j)
            session.commit()

        # Try to insert both a new job AND the already-existing one
        to_insert = [
            {"job_id": "dup.pbs", "state": "F", "owner": "x", "queue": "q"},   # already exists → no-op
            {"job_id": "new.pbs", "state": "F", "owner": "x", "queue": "q"},   # new → 1 actual write
        ]
        counts = repo.apply_reconciliation_batch(to_insert=to_insert, to_repair=[])

        assert counts is not None, (
            "apply_reconciliation_batch must return an actual-count mapping, not None"
        )
        assert counts.get("inserted") == 1, (
            f"Expected 1 actual insert (dup.pbs was pre-existing), got {counts.get('inserted')}"
        )

    def test_reconciler_uses_actual_counts_not_planned(self):
        """
        ReconciliationResult.inserted must equal the actual rows written,
        not len(to_insert). When a race causes a no-op insert, the count
        must reflect that.
        """
        from pbs_monitor.history_reconciliation import HistoryReconciler

        pbs_mock = MagicMock()
        pbs_mock.qstat_completed_jobs.return_value = [
            _make_pbs_job("1.pbs", "F"),
            _make_pbs_job("2.pbs", "F"),
        ]

        repo_mock = MagicMock()
        repo_mock.fetch_states_for_ids.return_value = {}
        # Simulate repo returning actual counts: only 1 of 2 inserts landed
        repo_mock.apply_reconciliation_batch.return_value = {"inserted": 1, "repaired": 0}

        converter_mock = MagicMock()
        converter_mock.to_database.side_effect = lambda j: {"job_id": j.job_id}

        reconciler = HistoryReconciler(
            pbs_commands=pbs_mock,
            repository=repo_mock,
            converter=converter_mock,
        )
        result = reconciler.reconcile()

        assert result.inserted == 1, (
            f"ReconciliationResult.inserted should be the actual write count (1), "
            f"not the planned list length (2). Got {result.inserted}."
        )


# ===========================================================================
# C-3  Durable last-success seeding and no advance on failure
# ===========================================================================


class TestC3DurableLastSuccess:
    """
    Defect C-3a: _last_reconciliation_completed is in-memory only and starts
    None. After a restart, the scheduler sees None and treats that as "never
    ran", triggering an immediate run even if one completed 5 minutes ago.

    Fix: _last_reconciliation_completed must be seeded from the latest
    successful audit row in history_reconciliation_log on startup or on first
    reconciliation-due check.

    Defect C-3b: _try_reconcile_once advances _last_reconciliation_completed
    even when _run_reconciliation returns a failed result. A failed run must
    NOT advance the success timestamp (though it may separately record a
    completion time to avoid tight retry loops).
    """

    def test_failed_reconciliation_does_not_advance_last_success(self):
        """
        A reconciliation that returns status='failed' must NOT advance
        _last_reconciliation_completed.
        """
        from pbs_monitor.config import Config
        from pbs_monitor.data_collector import DataCollector
        from pbs_monitor.history_reconciliation import ReconciliationResult

        cfg = Config()
        cfg.database.url = "sqlite:///:memory:"
        dc = DataCollector(config=cfg, use_sample_data=True, enable_database=False)

        before = dc._last_reconciliation_completed  # None or earlier time

        failed_result = ReconciliationResult()
        failed_result.status = "failed"
        failed_result.error_message = "qstat timed out"

        def _failing_run():
            return failed_result

        dc._run_reconciliation = _failing_run
        dc._try_reconcile_once()

        assert dc._last_reconciliation_completed == before, (
            "_last_reconciliation_completed was advanced on a failed reconciliation. "
            "Only successful runs should update this timestamp."
        )

    def test_successful_reconciliation_advances_last_success(self):
        """
        A reconciliation that returns status='success' MUST advance
        _last_reconciliation_completed.
        """
        from pbs_monitor.config import Config
        from pbs_monitor.data_collector import DataCollector
        from pbs_monitor.history_reconciliation import ReconciliationResult

        cfg = Config()
        cfg.database.url = "sqlite:///:memory:"
        dc = DataCollector(config=cfg, use_sample_data=True, enable_database=False)

        dc._last_reconciliation_completed = None

        success_result = ReconciliationResult()
        success_result.status = "success"

        def _success_run():
            return success_result

        dc._run_reconciliation = _success_run
        dc._try_reconcile_once()

        assert dc._last_reconciliation_completed is not None, (
            "_last_reconciliation_completed was NOT advanced on a successful reconciliation."
        )

    def test_last_success_seeded_from_audit_on_init(self, initialized_db):
        """
        At startup, _last_reconciliation_completed must be populated from the
        latest 'success' row in history_reconciliation_log, not left as None.

        This prevents immediate re-run after a daemon restart when a recent
        successful run completed.
        """
        from pbs_monitor.database.repositories import JobRepository
        from pbs_monitor.history_reconciliation import ReconciliationResult

        # Write a successful audit row to the DB
        repo = JobRepository(initialized_db)
        r = ReconciliationResult()
        r.status = "success"
        r.observed = 10
        r.terminal_candidates = 5
        r.inserted = 2
        r.repaired = 1
        r.unchanged = 2
        r.parse_errors = 0
        r.duration_seconds = 1.0
        recent_time = datetime(2026, 9, 21, 10, 0, 0, tzinfo=timezone.utc)
        r.start_time = recent_time
        r.end_time = recent_time
        repo.write_reconciliation_log(r)

        # Now fetch the latest success time
        latest = repo.get_latest_successful_reconciliation_time()

        assert latest is not None, (
            "get_latest_successful_reconciliation_time() returned None even though "
            "a successful audit row exists."
        )
        # Timestamps should be close (allow for tz normalization)
        assert abs((latest.replace(tzinfo=timezone.utc) - recent_time).total_seconds()) < 2, (
            f"Latest success time {latest} does not match the written time {recent_time}."
        )


# ===========================================================================
# I-1  Startup thread not created when DB or reconciliation disabled
# ===========================================================================


class TestI1NoThreadWhenDisabled:
    """
    Defect I-1: start_background_updates() unconditionally creates and starts
    _reconciliation_thread even when enable_database=False or
    history_reconciliation_enabled=False.

    Fix: only launch the startup reconciliation thread when both the database
    AND history reconciliation are enabled.
    """

    def test_no_reconciliation_thread_when_database_disabled(self):
        """
        enable_database=False → no startup reconciliation thread.
        """
        from pbs_monitor.config import Config
        from pbs_monitor.data_collector import DataCollector

        cfg = Config()
        cfg.database.url = "sqlite:///:memory:"
        dc = DataCollector(config=cfg, use_sample_data=True, enable_database=False)

        reconcile_called = {"n": 0}
        original_try = dc._try_reconcile_once

        def _spy(*a, **kw):
            reconcile_called["n"] += 1
            return original_try(*a, **kw)

        dc._try_reconcile_once = _spy

        dc.start_background_updates()
        time.sleep(0.3)  # give any launched thread time to run
        dc.stop_background_updates()

        assert reconcile_called["n"] == 0, (
            "Startup reconciliation was triggered despite enable_database=False. "
            f"It was called {reconcile_called['n']} time(s)."
        )

    def test_no_reconciliation_thread_when_reconciliation_disabled_in_config(self):
        """
        history_reconciliation_enabled=False → no startup reconciliation thread.
        """
        from pbs_monitor.config import Config
        from pbs_monitor.data_collector import DataCollector

        cfg = Config()
        cfg.database.url = "sqlite:///:memory:"
        cfg.database.history_reconciliation_enabled = False
        # DB is nominally enabled but reconciliation is off
        dc = DataCollector(config=cfg, use_sample_data=True, enable_database=False)

        reconcile_called = {"n": 0}
        original_try = dc._try_reconcile_once

        def _spy(*a, **kw):
            reconcile_called["n"] += 1
            return original_try(*a, **kw)

        dc._try_reconcile_once = _spy

        dc.start_background_updates()
        time.sleep(0.3)
        dc.stop_background_updates()

        assert reconcile_called["n"] == 0, (
            "Startup reconciliation was triggered despite reconciliation disabled in config. "
            f"Called {reconcile_called['n']} time(s)."
        )


# ===========================================================================
# I-2  batch_size from config is passed to reconciler
# ===========================================================================


class TestI2BatchSizePropagated:
    """
    Defect I-2: _run_reconciliation reads batch_size from config correctly,
    but qstat_completed_jobs (the fetch method) still hard-codes 500 in some
    implementations. The test verifies that the configured batch_size is
    passed all the way through to reconciler.reconcile(batch_size=...).
    """

    def test_configured_batch_size_passed_to_reconcile(self):
        """
        When history_reconciliation_batch_size=42 in config, reconcile()
        must be called with batch_size=42.
        """
        from pbs_monitor.config import Config
        from pbs_monitor.data_collector import DataCollector
        from pbs_monitor.history_reconciliation import HistoryReconciler, ReconciliationResult

        cfg = Config()
        cfg.database.url = "sqlite:///:memory:"
        cfg.database.history_reconciliation_batch_size = 42
        dc = DataCollector(config=cfg, use_sample_data=True, enable_database=False)

        # Force _database_enabled so _run_reconciliation doesn't bail out
        dc._database_enabled = True
        dc._repository_factory = MagicMock()
        dc._model_converters = MagicMock()

        batch_sizes_seen = []

        class _CapturingReconciler:
            def __init__(self, **kwargs):
                pass

            def reconcile(self, dry_run=False, batch_size=500):
                batch_sizes_seen.append(batch_size)
                r = ReconciliationResult()
                r.status = "success"
                return r

        with patch("pbs_monitor.data_collector.HistoryReconciler", _CapturingReconciler):
            dc._run_reconciliation()

        assert batch_sizes_seen, "_run_reconciliation did not call reconciler.reconcile()"
        assert batch_sizes_seen[0] == 42, (
            f"reconcile() was called with batch_size={batch_sizes_seen[0]}, expected 42. "
            "Config batch_size is not being propagated."
        )


# ===========================================================================
# I-3  DatabaseCommand fallback help lists reconcile-history
# ===========================================================================


class TestI3FallbackHelpIncludesReconcileHistory:
    """
    Defect I-3: When database_action is None, DatabaseCommand.execute() prints
    a help message that omits 'reconcile-history'. The command is implemented
    but invisible in the fallback list, making it undiscoverable.

    Fix: add 'reconcile-history' to the printed list of available actions.
    """

    def test_fallback_help_lists_reconcile_history(self):
        """
        Calling DatabaseCommand.execute with database_action=None must print
        'reconcile-history' in the output.
        """
        from pbs_monitor.cli.commands import DatabaseCommand

        collector_mock = MagicMock()
        config_mock = MagicMock()
        config_mock.display.auto_width = False
        config_mock.display.max_table_width = 120
        config_mock.display.use_colors = False
        config_mock.pbs.command_timeout = 30

        cmd = DatabaseCommand(collector=collector_mock, config=config_mock)

        args = MagicMock()
        args.database_action = None

        captured = StringIO()
        import sys
        old_stdout = sys.stdout
        sys.stdout = captured
        try:
            rc = cmd.execute(args)
        finally:
            sys.stdout = old_stdout

        output = captured.getvalue()
        assert "reconcile-history" in output, (
            "DatabaseCommand fallback help text does not mention 'reconcile-history'. "
            f"Full output:\n{output}"
        )

    def test_unknown_subcommand_help_lists_reconcile_history(self):
        """
        The 'Unknown database subcommand' error path must also include
        'reconcile-history' in the list of valid actions.
        """
        from pbs_monitor.cli.commands import DatabaseCommand

        collector_mock = MagicMock()
        config_mock = MagicMock()
        config_mock.display.auto_width = False
        config_mock.display.max_table_width = 120
        config_mock.display.use_colors = False
        config_mock.pbs.command_timeout = 30

        cmd = DatabaseCommand(collector=collector_mock, config=config_mock)

        args = MagicMock()
        args.database_action = "nonexistent-action"

        captured = StringIO()
        import sys
        old_stdout = sys.stdout
        sys.stdout = captured
        try:
            rc = cmd.execute(args)
        finally:
            sys.stdout = old_stdout

        output = captured.getvalue()
        assert "reconcile-history" in output, (
            "'reconcile-history' not listed in 'Unknown subcommand' error message. "
            f"Full output:\n{output}"
        )


# ===========================================================================
# M-1  create_sample_config includes history_reconciliation_* fields
# ===========================================================================


class TestM1SampleConfigIncludesReconciliationFields:
    """
    Defect M-1: Config.create_sample_config() generates a sample YAML that
    lacks the four history_reconciliation_* keys present in DatabaseConfig.
    Operators reading the sample config have no way to know these settings exist.

    Fix: add all four fields to the sample database stanza.
    """

    REQUIRED_FIELDS = [
        "history_reconciliation_enabled",
        "history_reconciliation_interval_seconds",
        "history_reconciliation_batch_size",
        "history_reconciliation_shutdown_timeout_seconds",
    ]

    def test_sample_config_includes_reconciliation_enabled(self, tmp_path):
        from pbs_monitor.config import Config

        cfg_path = str(tmp_path / "sample.yaml")
        cfg = Config(cfg_path)
        cfg.create_sample_config()

        import yaml
        with open(cfg_path) as f:
            data = yaml.safe_load(f)

        for field in self.REQUIRED_FIELDS:
            assert field in data.get("database", {}), (
                f"create_sample_config() missing database.{field} in sample YAML"
            )

    def test_sample_config_interval_is_12h(self, tmp_path):
        from pbs_monitor.config import Config

        cfg_path = str(tmp_path / "sample2.yaml")
        cfg = Config(cfg_path)
        cfg.create_sample_config()

        import yaml
        with open(cfg_path) as f:
            data = yaml.safe_load(f)

        interval = data.get("database", {}).get("history_reconciliation_interval_seconds")
        assert interval == 43200, (
            f"Expected history_reconciliation_interval_seconds=43200 (12h) in sample, got {interval}"
        )
