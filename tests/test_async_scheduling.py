"""
RED tests for asynchronous scheduling and durable last-success defects.

These tests assert correct behaviour that the current implementation does NOT
satisfy.  They are expected to FAIL before the fix and PASS after.

Defects covered:
  A-1  _run_one_background_iteration calls _try_reconcile_once() synchronously,
       blocking the collection loop.  It must instead start a worker thread and
       return promptly.
  A-2  Only one reconciliation worker may exist at a time; a second due-trigger
       while a worker is alive must not launch a second thread.
  A-3  At startup, _last_reconciliation_completed must be seeded from the durable
       audit (get_latest_successful_reconciliation_time) before deciding whether
       an immediate run is due — so a recent success suppresses startup run.
  A-4  An absent or old durable success triggers an asynchronous startup run.
  A-5  Failed reconciliation runs do not advance _last_reconciliation_completed
       (already partially covered by C-3 but extended here for the async path).
  A-6  stop_background_updates joins the current worker with a bounded timeout.
"""

import threading
import time
from datetime import datetime, timedelta, timezone
from typing import Optional
from unittest.mock import MagicMock, patch

import pytest

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_config(interval: int = 0, enabled: bool = True):
    """Minimal config with reconciliation settings."""
    from pbs_monitor.config import Config
    cfg = Config()
    cfg.database.url = "sqlite:///:memory:"
    cfg.database.history_reconciliation_enabled = enabled
    cfg.database.history_reconciliation_interval_seconds = interval
    cfg.database.history_reconciliation_shutdown_timeout_seconds = 2
    return cfg


def _make_dc(interval: int = 0, enabled: bool = True):
    """DataCollector with DB disabled (no real DB needed for scheduling tests)."""
    from pbs_monitor.data_collector import DataCollector
    cfg = _make_config(interval=interval, enabled=enabled)
    dc = DataCollector(config=cfg, use_sample_data=True, enable_database=False)
    return dc


# ---------------------------------------------------------------------------
# A-1  Periodic trigger must start a worker thread and return promptly
# ---------------------------------------------------------------------------


class TestA1AsyncWorkerSpawned:
    """
    _run_one_background_iteration must NOT call _try_reconcile_once() on the
    calling thread when reconciliation is due.  Instead it must start a
    separate threading.Thread (the reconciliation worker) and return
    immediately.  The collection loop must not block on qstat / DB I/O.
    """

    def test_due_trigger_returns_promptly(self):
        """
        When the interval has elapsed, _run_one_background_iteration returns
        in well under the time a real reconciliation would take (< 0.5 s).
        """
        dc = _make_dc(interval=0)
        dc._database_enabled = True
        # Mark last completion as ancient so the interval is immediately due.
        dc._last_reconciliation_completed = datetime(2000, 1, 1, tzinfo=timezone.utc)

        # Replace _try_reconcile_once with a slow blocker so we can detect
        # if it was called on the calling thread.
        called_on_thread = []

        def _slow_reconcile():
            called_on_thread.append(threading.current_thread().name)
            time.sleep(10)  # would block for 10 s if called synchronously

        dc._try_reconcile_once = _slow_reconcile

        start = time.monotonic()
        dc._run_one_background_iteration()
        elapsed = time.monotonic() - start

        # If the call returned in < 2.0 s the slow reconcile ran in a
        # separate thread (or was not called synchronously at all).
        # (Thread spawn overhead on macOS can be ~0.5–1 s; the synchronous
        # path would block for 10 s, which is unambiguously distinguishable.)
        assert elapsed < 2.0, (
            f"_run_one_background_iteration blocked for {elapsed:.2f} s; "
            "it must start an async worker and return immediately."
        )

    def test_due_trigger_starts_thread_not_direct_call(self):
        """
        After _run_one_background_iteration returns, a reconciliation worker
        thread must be alive (or have been joined/completed).
        The reconciliation must NOT have run on the loop's own thread.
        """
        dc = _make_dc(interval=0)
        dc._database_enabled = True
        dc._last_reconciliation_completed = datetime(2000, 1, 1, tzinfo=timezone.utc)

        reconcile_threads = []
        reconcile_event = threading.Event()

        def _capturing_reconcile():
            reconcile_threads.append(threading.current_thread())
            reconcile_event.set()

        dc._try_reconcile_once = _capturing_reconcile

        caller_thread = threading.current_thread()
        dc._run_one_background_iteration()

        # Give the worker thread a moment to execute.
        reconcile_event.wait(timeout=2.0)

        assert reconcile_threads, (
            "Reconciliation was never called after interval elapsed."
        )
        worker_thread = reconcile_threads[0]
        assert worker_thread is not caller_thread, (
            "Reconciliation ran on the collection loop's thread; "
            "it must run in a dedicated worker thread."
        )


# ---------------------------------------------------------------------------
# A-2  Only one worker at a time
# ---------------------------------------------------------------------------


class TestA2SingleWorkerOwnership:
    """
    If a reconciliation worker is already alive, a second due-check in
    _run_one_background_iteration must NOT create another thread.
    The DataCollector must own exactly one worker at a time.
    """

    def test_second_trigger_does_not_launch_another_thread(self):
        """
        Two back-to-back _run_one_background_iteration calls while a worker
        is still running must not spawn a second thread.
        """
        dc = _make_dc(interval=0)
        dc._database_enabled = True
        dc._last_reconciliation_completed = datetime(2000, 1, 1, tzinfo=timezone.utc)

        # Slow worker so it stays alive across both calls.
        gate = threading.Event()
        thread_count = [0]

        def _slow_reconcile():
            thread_count[0] += 1
            gate.wait(timeout=5.0)

        dc._try_reconcile_once = _slow_reconcile

        # First call — should spawn the worker.
        dc._run_one_background_iteration()
        time.sleep(0.1)  # allow worker to start

        # Second call while worker is still running — must NOT spawn another.
        dc._run_one_background_iteration()
        time.sleep(0.1)

        gate.set()  # let the worker finish

        # Allow worker thread to exit.
        if dc._reconciliation_thread is not None:
            dc._reconciliation_thread.join(timeout=2.0)

        assert thread_count[0] == 1, (
            f"Expected exactly 1 reconciliation worker invocation, "
            f"got {thread_count[0]}."
        )


# ---------------------------------------------------------------------------
# A-3  Recent durable success suppresses startup run
# ---------------------------------------------------------------------------


class TestA3RecentDurableSuccessSuppressesStartup:
    """
    If get_latest_successful_reconciliation_time() returns a timestamp within
    the reconciliation interval, start_background_updates must NOT launch an
    immediate reconciliation thread.
    """

    def test_recent_success_suppresses_startup_reconciliation(self):
        """
        A durable success 1 second ago with a 1-hour interval → no startup run.
        """
        dc = _make_dc(interval=3600)  # 1 hour
        dc._database_enabled = True
        dc._repository_factory = MagicMock()

        # Simulate a recent success from the audit.
        recent = datetime.now(timezone.utc) - timedelta(seconds=1)
        job_repo_mock = MagicMock()
        job_repo_mock.get_latest_successful_reconciliation_time.return_value = recent
        dc._repository_factory.get_job_repository.return_value = job_repo_mock

        reconcile_called = []

        def _spy_reconcile():
            reconcile_called.append(True)

        dc._try_reconcile_once = _spy_reconcile

        dc.start_background_updates()
        time.sleep(0.4)  # wait long enough for any erroneous startup thread
        dc.stop_background_updates()

        assert not reconcile_called, (
            "Startup reconciliation was triggered despite a recent durable "
            f"success ({recent.isoformat()}); the 1-hour interval has not elapsed."
        )


# ---------------------------------------------------------------------------
# A-4  Absent / old durable success triggers async startup run
# ---------------------------------------------------------------------------


class TestA4AbsentOrOldSuccessTriggersAsyncStartup:
    """
    If get_latest_successful_reconciliation_time() returns None (no prior run)
    or a timestamp older than the interval, start_background_updates must
    launch an asynchronous reconciliation worker.

    The startup thread — not the collection loop — is responsible for this,
    so it must remain a real threading.Thread, not a synchronous call.
    """

    def test_no_prior_success_triggers_startup_run(self):
        """
        No prior success in the audit → startup reconciliation worker is launched.
        """
        dc = _make_dc(interval=3600)
        dc._database_enabled = True
        dc._repository_factory = MagicMock()

        job_repo_mock = MagicMock()
        job_repo_mock.get_latest_successful_reconciliation_time.return_value = None
        dc._repository_factory.get_job_repository.return_value = job_repo_mock

        reconcile_event = threading.Event()

        def _spy_reconcile():
            reconcile_event.set()

        dc._try_reconcile_once = _spy_reconcile

        dc.start_background_updates()
        triggered = reconcile_event.wait(timeout=2.0)
        dc.stop_background_updates()

        assert triggered, (
            "Startup reconciliation was NOT triggered despite no prior "
            "successful run in the audit."
        )

    def test_old_success_triggers_startup_run(self):
        """
        A durable success older than the interval → startup worker is launched.
        """
        dc = _make_dc(interval=3600)
        dc._database_enabled = True
        dc._repository_factory = MagicMock()

        old = datetime.now(timezone.utc) - timedelta(hours=25)
        job_repo_mock = MagicMock()
        job_repo_mock.get_latest_successful_reconciliation_time.return_value = old
        dc._repository_factory.get_job_repository.return_value = job_repo_mock

        reconcile_event = threading.Event()

        def _spy_reconcile():
            reconcile_event.set()

        dc._try_reconcile_once = _spy_reconcile

        dc.start_background_updates()
        triggered = reconcile_event.wait(timeout=2.0)
        dc.stop_background_updates()

        assert triggered, (
            "Startup reconciliation was NOT triggered despite durable success "
            f"being older ({old.isoformat()}) than the 1-hour interval."
        )


# ---------------------------------------------------------------------------
# A-5  Failed runs do not advance last-success; async path
# ---------------------------------------------------------------------------


class TestA5FailedRunNoAdvance:
    """
    When the reconciliation worker completes with status='failed', the
    _last_reconciliation_completed timestamp must NOT be updated.

    Extends C-3 by exercising the async thread-based path (not just the
    direct _try_reconcile_once call).
    """

    def test_async_failed_run_does_not_advance_last_success(self):
        """
        Even when reconciliation runs in a background thread and returns a
        failed result, _last_reconciliation_completed stays unchanged.
        """
        from pbs_monitor.history_reconciliation import ReconciliationResult

        dc = _make_dc(interval=0)
        dc._database_enabled = True
        dc._last_reconciliation_completed = None

        failed_result = ReconciliationResult()
        failed_result.status = "failed"
        failed_result.error_message = "simulated failure"

        done_event = threading.Event()

        def _failing_run():
            return failed_result

        original_try = dc._try_reconcile_once

        def _patched_try():
            dc._run_reconciliation = _failing_run
            original_try()
            done_event.set()

        dc._try_reconcile_once = _patched_try
        dc._last_reconciliation_completed = datetime(2000, 1, 1, tzinfo=timezone.utc)
        before = dc._last_reconciliation_completed

        # Trigger via the scheduling path (not direct call)
        dc._run_one_background_iteration()
        done_event.wait(timeout=3.0)

        assert dc._last_reconciliation_completed == before, (
            "_last_reconciliation_completed was advanced despite a failed "
            "reconciliation run in the async worker thread."
        )


# ---------------------------------------------------------------------------
# A-6  stop_background_updates joins the worker with bounded timeout
# ---------------------------------------------------------------------------


class TestA6StopJoinsWorkerBoundedly:
    """
    stop_background_updates must join the current reconciliation worker
    with a bounded timeout (not block indefinitely on qstat or DB I/O).
    """

    def test_stop_joins_reconciliation_worker(self):
        """
        stop_background_updates must call .join() on the reconciliation thread
        (if alive) with a non-zero timeout so the caller is never blocked
        indefinitely.
        """
        dc = _make_dc(interval=3600)
        dc._database_enabled = True

        join_calls = []
        finished = threading.Event()

        def _slow_target():
            finished.wait(timeout=60)  # holds the thread alive until we release it

        t = threading.Thread(target=_slow_target, daemon=True)
        t.start()

        original_join = t.join

        def _spy_join(timeout=None):
            join_calls.append(timeout)
            finished.set()          # release the slow thread so join returns fast
            return original_join(timeout=timeout)

        t.join = _spy_join
        dc._reconciliation_thread = t

        dc.start_background_updates()
        dc.stop_background_updates()

        assert join_calls, (
            "stop_background_updates never called .join() on the "
            "reconciliation worker thread."
        )
        assert join_calls[0] is not None and join_calls[0] > 0, (
            f"stop_background_updates called join(timeout={join_calls[0]}); "
            "must pass a positive timeout to avoid blocking indefinitely."
        )
