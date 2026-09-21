"""
TDD tests for 5 code-quality findings:

  Q-1  Dialect detection uses session.bind (may be None) → use session.get_bind()
  Q-2  hasattr(Job, key) admits relationships/methods; use Job.__table__.c membership
  Q-3  Explicit session.commit() inside apply_reconciliation_batch is redundant
       (get_session already auto-commits on exit)
  Q-4  qstat_completed_jobs / qstat_completed_jobs_detailed share duplicate fetch/parse
       logic → extract shared private _fetch_completed_jobs_data() helper
  Q-5  Manual index DDL in ensure_history_reconciliation_log_table() is redundant;
       __table__.create(checkfirst=True) already emits them from __table_args__

Tests are written RED-first.  Each asserts the CORRECT behaviour;
failing before the fix confirms the test targets the right thing.
"""

import os
import tempfile
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional
from unittest.mock import MagicMock, patch, call

import pytest


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_pbs_job(job_id: str, state_char: str):
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
# Q-1  Dialect detection: session.get_bind() not session.bind
# ===========================================================================

class TestQ1DialectDetectionViaGetBind:
    """
    Q-1: apply_reconciliation_batch uses ``session.bind.dialect.name``
    guarded by ``if session.bind``.  In SQLAlchemy 2.x the ``bind`` attribute
    is deprecated and may return ``None`` on some configurations (e.g. when
    the session is created via scoped_session without an explicit bind).

    Correct approach: ``session.get_bind().dialect.name`` (raises on truly
    unbound sessions, which we *want* — fail closed rather than silently
    defaulting to SQLite).

    This test verifies the code path works end-to-end on a normal SQLite
    session (the behavioral correctness), and a source-inspection test confirms
    the implementation detail.
    """

    def test_apply_reconciliation_batch_works_on_sqlite(self, initialized_db):
        """apply_reconciliation_batch succeeds end-to-end using get_bind() for dialect."""
        from pbs_monitor.database.repositories import JobRepository

        repo = JobRepository(initialized_db)
        result = repo.apply_reconciliation_batch(
            to_insert=[{"job_id": "q1.test", "state": "F"}],
            to_repair=[],
        )
        assert result["inserted"] == 1
        assert result["repaired"] == 0


class TestQ1DialectViaGetBindDirect:
    """
    Unit-level: the dialect detection line should call session.get_bind()
    rather than accessing session.bind.  Inspect the source to confirm.
    """

    def test_apply_reconciliation_batch_source_uses_get_bind(self):
        import inspect as py_inspect
        from pbs_monitor.database.repositories import JobRepository

        source = py_inspect.getsource(JobRepository.apply_reconciliation_batch)
        # The corrected code must NOT rely on bare `session.bind`
        # (the old fragile path); it must call session.get_bind()
        assert "session.get_bind()" in source, (
            "apply_reconciliation_batch must use session.get_bind() for dialect detection, "
            "not session.bind (deprecated/None in SQLAlchemy 2.x)."
        )
        # And must NOT use the old guarded form
        assert "session.bind.dialect" not in source, (
            "apply_reconciliation_batch must not use session.bind.dialect — "
            "use session.get_bind().dialect instead."
        )


# ===========================================================================
# Q-2  hasattr(Job, key) → Job.__table__.c membership
# ===========================================================================

class TestQ2TableColumnMembership:
    """
    Q-2: apply_reconciliation_batch gates extra column writes with
    ``hasattr(Job, key)``.  This admits ORM relationships, methods, and
    class attributes that are not actual database columns (e.g. ``history``,
    ``is_active``, ``calculate_derived_fields``).  Setting one of these on
    the INSERT/UPDATE values dict would produce an invalid SQL statement.

    Correct guard: ``key in {c.key for c in Job.__table__.c}``

    This test verifies that a dict with a relationship-name key (``history``)
    and a method name (``is_active``) does NOT appear in the SQL values sent
    to the DB, even though hasattr(Job, 'history') and hasattr(Job, 'is_active')
    would both return True.
    """

    def test_relationship_key_not_sent_to_db_on_insert(self, initialized_db):
        from pbs_monitor.database.repositories import JobRepository

        repo = JobRepository(initialized_db)

        # 'history' is a relationship attribute on Job — hasattr returns True
        # but it must NOT be included in INSERT values (not a column)
        bad_data = {
            "job_id": "q2a.test",
            "state": "F",
            "history": "this_should_not_be_inserted",   # relationship — not a column
            "is_active": True,                           # method — not a column
            "calculate_derived_fields": "nope",          # method — not a column
        }
        # Should not raise; the non-column keys should be silently ignored
        result = repo.apply_reconciliation_batch(to_insert=[bad_data], to_repair=[])
        assert result["inserted"] == 1

    def test_source_uses_table_columns_not_hasattr(self):
        import inspect as py_inspect
        from pbs_monitor.database.repositories import JobRepository

        source = py_inspect.getsource(JobRepository.apply_reconciliation_batch)
        # The corrected code uses table column membership check
        assert "Job.__table__.c" in source, (
            "apply_reconciliation_batch must guard column writes with "
            "'key in {c.key for c in Job.__table__.c}' (or equivalent), "
            "not 'hasattr(Job, key)'."
        )
        # Old hasattr guard should be gone
        assert "hasattr(Job, key)" not in source, (
            "apply_reconciliation_batch must not use hasattr(Job, key) — "
            "use Job.__table__.c membership instead."
        )


# ===========================================================================
# Q-3  Explicit session.commit() redundancy
# ===========================================================================

class TestQ3ExplicitCommitRedundancy:
    """
    Q-3: apply_reconciliation_batch calls session.commit() explicitly at the
    end of the ``with self.get_session()`` block.  DatabaseManager.get_session()
    already calls session.commit() on clean exit (see connection.py).  The
    explicit commit is therefore redundant.

    Removing it is safe: the auto-commit from get_session() covers the
    transaction.  This test verifies that commit() is called at most ONCE
    during a successful apply_reconciliation_batch (by the context manager
    exit), not twice.
    """

    def test_no_explicit_commit_in_apply_reconciliation_batch_source(self):
        """The apply_reconciliation_batch method body must not call session.commit()."""
        import inspect as py_inspect
        from pbs_monitor.database.repositories import JobRepository

        source = py_inspect.getsource(JobRepository.apply_reconciliation_batch)
        # Remove the method signature line(s) and doc, then count literal commits.
        # We want ZERO explicit session.commit() calls in the body.
        assert "session.commit()" not in source, (
            "apply_reconciliation_batch must not call session.commit() explicitly — "
            "DatabaseManager.get_session() auto-commits on clean exit. "
            "Remove the redundant explicit commit."
        )

    def test_apply_reconciliation_batch_commits_exactly_once(self, initialized_db):
        """session.commit() is called exactly once (by the context manager)."""
        from pbs_monitor.database.repositories import JobRepository

        repo = JobRepository(initialized_db)

        commit_calls: List[int] = []

        original_get_session = repo.get_session

        from contextlib import contextmanager

        @contextmanager
        def counting_get_session():
            with original_get_session() as session:
                original_commit = session.commit

                def counting_commit():
                    commit_calls.append(1)
                    return original_commit()

                session.commit = counting_commit
                yield session

        with patch.object(repo, 'get_session', counting_get_session):
            repo.apply_reconciliation_batch(
                to_insert=[{"job_id": "q3.test", "state": "F"}],
                to_repair=[],
            )

        assert len(commit_calls) == 1, (
            f"Expected exactly 1 commit() call (from context manager exit), "
            f"got {len(commit_calls)}. Remove the explicit session.commit() inside "
            f"apply_reconciliation_batch."
        )


# ===========================================================================
# Q-4  qstat_completed_jobs / _detailed share duplicated fetch logic
# ===========================================================================

class TestQ4QstatFetchDedupe:
    """
    Q-4: qstat_completed_jobs() and qstat_completed_jobs_detailed() contain
    near-identical fetch/parse blocks (~50 lines each duplicated).

    After refactoring, both must delegate the HTTP/command fetch to a shared
    private helper ``_fetch_completed_jobs_data()`` that returns the raw
    ``data`` dict.  The public methods only differ in how they handle
    per-record parse errors (silently vs. counting them).

    Backward-compatibility invariants (MUST hold after refactor):
    * qstat_completed_jobs() returns a list of PBSJob (same as before)
    * qstat_completed_jobs_detailed() returns a QstatDetailedResult
    * Both still filter to states C/F/E
    * Both still raise PBSCommandError on whole-response failure
    * parse_errors in detailed result still counts per-record failures
    """

    def test_private_fetch_helper_exists(self):
        """PBSCommands must expose a private _fetch_completed_jobs_data method."""
        from pbs_monitor.pbs_commands import PBSCommands

        assert hasattr(PBSCommands, "_fetch_completed_jobs_data"), (
            "PBSCommands must define a private _fetch_completed_jobs_data() helper "
            "shared by qstat_completed_jobs and qstat_completed_jobs_detailed."
        )
        import inspect as py_inspect
        assert callable(PBSCommands._fetch_completed_jobs_data), (
            "_fetch_completed_jobs_data must be callable."
        )

    def test_qstat_completed_jobs_calls_fetch_helper(self):
        """qstat_completed_jobs() must delegate data fetching to the shared helper."""
        import inspect as py_inspect
        from pbs_monitor.pbs_commands import PBSCommands

        source = py_inspect.getsource(PBSCommands.qstat_completed_jobs)
        assert "_fetch_completed_jobs_data" in source, (
            "qstat_completed_jobs must call self._fetch_completed_jobs_data() "
            "instead of duplicating the fetch logic inline."
        )

    def test_qstat_completed_jobs_detailed_calls_fetch_helper(self):
        """qstat_completed_jobs_detailed() must delegate data fetching to the shared helper."""
        import inspect as py_inspect
        from pbs_monitor.pbs_commands import PBSCommands

        source = py_inspect.getsource(PBSCommands.qstat_completed_jobs_detailed)
        assert "_fetch_completed_jobs_data" in source, (
            "qstat_completed_jobs_detailed must call self._fetch_completed_jobs_data() "
            "instead of duplicating the fetch logic inline."
        )

    def test_qstat_completed_jobs_backward_compat_with_sample_data(self):
        """qstat_completed_jobs() still returns a list of PBSJob after refactoring."""
        from pbs_monitor.pbs_commands import PBSCommands
        from pbs_monitor.models.job import PBSJob

        cmds = PBSCommands(use_sample_data=True)
        result = cmds.qstat_completed_jobs()
        assert isinstance(result, list), "qstat_completed_jobs must return a list"
        for item in result:
            assert isinstance(item, PBSJob), f"Expected PBSJob, got {type(item)}"

    def test_qstat_completed_jobs_detailed_backward_compat_with_sample_data(self):
        """qstat_completed_jobs_detailed() still returns QstatDetailedResult."""
        from pbs_monitor.pbs_commands import PBSCommands, QstatDetailedResult

        cmds = PBSCommands(use_sample_data=True)
        result = cmds.qstat_completed_jobs_detailed()
        assert isinstance(result, QstatDetailedResult), (
            "qstat_completed_jobs_detailed must return QstatDetailedResult"
        )
        assert isinstance(result.jobs, list)
        assert isinstance(result.parse_errors, int)

    def test_qstat_detailed_parse_errors_counted_separately(self):
        """Per-record parse errors go into parse_errors, not into the jobs list."""
        from pbs_monitor.pbs_commands import PBSCommands, QstatDetailedResult
        from pbs_monitor.models.job import PBSJob
        from unittest.mock import patch as _patch

        cmds = PBSCommands(use_sample_data=False)

        # Inject data with two records; patch from_qstat_json to raise on the second
        good_data = {
            "Jobs": {
                "123.good": {
                    "Job_Id": "123.good",
                    "job_state": "F",
                    "Job_Owner": "user@host",
                    "queue": "debug",
                },
                "456.bad": {
                    "Job_Id": "456.bad",
                    "job_state": "F",
                    "Job_Owner": "user@host",
                    "queue": "debug",
                },
            }
        }

        call_count = [0]
        original_from_qstat = PBSJob.from_qstat_json.__func__  # noqa: E501

        def side_effect_from_qstat(info, score=None):
            call_count[0] += 1
            if call_count[0] == 2:
                raise ValueError("Simulated parse failure on second record")
            return original_from_qstat(PBSJob, info, score=score)

        with _patch.object(cmds, "_fetch_completed_jobs_data", return_value=good_data):
            with _patch.object(PBSJob, "from_qstat_json", staticmethod(side_effect_from_qstat)):
                result = cmds.qstat_completed_jobs_detailed()

        assert result.parse_errors >= 1, (
            "parse_errors should be >= 1 when a record raises during parsing"
        )
        assert len(result.jobs) >= 1, (
            "Successfully-parsed jobs should still be in the result"
        )

    def test_fetch_helper_returns_dict_with_jobs_key(self):
        """_fetch_completed_jobs_data() returns a dict with a 'Jobs' key."""
        from pbs_monitor.pbs_commands import PBSCommands

        cmds = PBSCommands(use_sample_data=True)
        data = cmds._fetch_completed_jobs_data()
        assert isinstance(data, dict), "_fetch_completed_jobs_data must return a dict"
        assert "Jobs" in data, "_fetch_completed_jobs_data must return dict with 'Jobs' key"

    def test_fetch_helper_raises_pbs_command_error_on_failure(self):
        """_fetch_completed_jobs_data() propagates PBSCommandError on whole-response failure."""
        from pbs_monitor.pbs_commands import PBSCommands, PBSCommandError

        cmds = PBSCommands(use_sample_data=False)

        with patch.object(cmds, "_run_command", side_effect=PBSCommandError("command failed")):
            with pytest.raises(PBSCommandError):
                cmds._fetch_completed_jobs_data()


# ===========================================================================
# Q-5  Redundant manual index DDL in ensure_history_reconciliation_log_table
# ===========================================================================

class TestQ5RedundantIndexDDL:
    """
    Q-5: ensure_history_reconciliation_log_table() calls
    ``HistoryReconciliationLog.__table__.create(engine, checkfirst=True)``
    which already emits the indexes declared in the model's ``__table_args__``
    (``ix_history_reconciliation_log_start_time`` and
    ``ix_history_reconciliation_log_status``).

    The method then ALSO issues manual ``CREATE INDEX IF NOT EXISTS`` SQL
    for those same indexes — a no-op on a fresh DB and a silent waste on
    subsequent calls.

    After the fix: the manual DDL is removed.  The table.create(checkfirst=True)
    call is sufficient.  The indexes still exist and are still correct.
    """

    def test_ensure_history_reconciliation_log_no_manual_index_ddl(self):
        """ensure_history_reconciliation_log_table body must not execute manual CREATE INDEX."""
        import inspect as py_inspect
        from pbs_monitor.database.migrations import DatabaseMigration

        source = py_inspect.getsource(
            DatabaseMigration.ensure_history_reconciliation_log_table
        )
        assert "CREATE INDEX" not in source, (
            "ensure_history_reconciliation_log_table must not issue manual "
            "'CREATE INDEX IF NOT EXISTS' DDL — the indexes are already created by "
            "HistoryReconciliationLog.__table__.create(engine, checkfirst=True) "
            "via __table_args__. Remove the redundant manual DDL."
        )

    def test_indexes_still_exist_after_table_create(self, initialized_db):
        """After ensure_history_reconciliation_log_table(), both indexes exist on the table."""
        from pbs_monitor.database.connection import DatabaseManager
        from sqlalchemy import inspect as sa_inspect

        dm = DatabaseManager(initialized_db)
        dm.initialize()

        inspector = sa_inspect(dm.engine)
        indexes = {idx["name"] for idx in inspector.get_indexes("history_reconciliation_log")}

        assert "ix_history_reconciliation_log_start_time" in indexes, (
            "ix_history_reconciliation_log_start_time index must exist after table creation"
        )
        assert "ix_history_reconciliation_log_status" in indexes, (
            "ix_history_reconciliation_log_status index must exist after table creation"
        )
