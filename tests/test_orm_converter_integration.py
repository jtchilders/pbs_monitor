"""
Integration tests for the ORM-converter/repository type mismatch and
invalid repair-state handling defects.

TDD RED: These tests prove the production defects and drive the fixes.

Defects covered
---------------
ORM-1  JobConverter.to_database() returns a Job ORM object, not a dict.
       apply_reconciliation_batch calls .get() / .items() on it → AttributeError
       in production (existing tests only mock the converter returning dicts).

ORM-2  apply_reconciliation_batch silently ignores an invalid state in a
       repair dict: it skips the 'state' key but still runs the UPDATE,
       potentially setting final_state_recorded=True while state stays
       UNKNOWN_END. Must fail closed: skip the row, do not write it.

Integration scenario tested here uses:
- Real ModelConverters().job (no mocking of the converter)
- A realistic terminal PBSJob fixture
- SQLite in-memory/tmp database via the initialized_db fixture
"""

import os
import tempfile
from datetime import datetime, timezone

import pytest


# ---------------------------------------------------------------------------
# Fixtures (mirror those in test_history_reconciliation.py)
# ---------------------------------------------------------------------------


@pytest.fixture
def temp_db_config():
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


# ---------------------------------------------------------------------------
# Realistic PBSJob fixture for a terminal (F-state) job
# ---------------------------------------------------------------------------


def _make_terminal_pbs_job(
    job_id: str = "12345.pbs",
    state_char: str = "F",
    exit_status: int = 0,
    owner: str = "juser",
    queue: str = "normal",
    job_name: str = "science_run",
) -> "PBSJob":
    """Return a realistic PBSJob that JobConverter.to_database() can handle."""
    from pbs_monitor.models.job import PBSJob, JobState

    start = datetime(2026, 9, 20, 8, 0, 0, tzinfo=timezone.utc)
    end = datetime(2026, 9, 20, 10, 0, 0, tzinfo=timezone.utc)
    submit = datetime(2026, 9, 20, 7, 45, 0, tzinfo=timezone.utc)

    raw = {
        "exec_host": "n001/0+n001/1",
        "exec_vnode": "(n001:ncpus=2)",
        "resources_used": {
            "walltime": "02:00:00",
            "cput": "01:58:00",
        },
        "run_count": "1",
    }

    state = JobState(state_char)

    return PBSJob(
        job_id=job_id,
        job_name=job_name,
        owner=owner,
        state=state,
        queue=queue,
        nodes=1,
        ppn=2,
        walltime="04:00:00",
        memory="16gb",
        submit_time=submit,
        start_time=start,
        end_time=end,
        priority=0,
        execution_node="n001",
        exit_status=exit_status,
        project="allocation_A",
        total_cores=2,
        actual_runtime_seconds=7200,
        queue_time_seconds=900,
        raw_attributes=raw,
    )


# ===========================================================================
# ORM-1: Real converter produces Job ORM object — must survive round-trip
# ===========================================================================


class TestORM1RealConverterInsertIntegration:
    """
    End-to-end: real JobConverter.to_database() + real JobRepository.apply_reconciliation_batch().

    JobConverter.to_database() returns a Job ORM object.
    apply_reconciliation_batch receives it as part of to_insert list.
    The interface must accept the ORM object and persist it correctly.

    This is the RED test: before the fix, apply_reconciliation_batch calls
    job_dict.get() on a Job ORM object (which has no .get()) → AttributeError.
    """

    def test_real_converter_orm_object_inserted_into_db(self, initialized_db):
        """
        Using the real ModelConverters().job converter (not a mock), convert a
        terminal PBSJob to a Job ORM object and insert it via apply_reconciliation_batch.
        The row must appear in the DB with correct fields after the call.
        """
        from pbs_monitor.database.model_converters import ModelConverters
        from pbs_monitor.database.repositories import JobRepository

        converters = ModelConverters()
        repo = JobRepository(initialized_db)

        pbs_job = _make_terminal_pbs_job(job_id="orm_int_test.pbs", state_char="F")

        # This is what the real reconciler does: convert → pass to repository.
        # Before the fix, this raises AttributeError because Job ORM has no .get().
        job_obj = converters.job.to_database(pbs_job)

        # apply_reconciliation_batch receives a list containing the ORM object.
        counts = repo.apply_reconciliation_batch(to_insert=[job_obj], to_repair=[])

        assert isinstance(counts, dict), "apply_reconciliation_batch must return a dict"
        assert counts.get("inserted") == 1, (
            f"Expected 1 row inserted, got {counts.get('inserted')}. "
            "The ORM object was not handled correctly by apply_reconciliation_batch."
        )

        # Verify the row is actually in the DB with correct fields
        result = repo.get_job_by_id("orm_int_test.pbs")
        assert result is not None, "Job was not found in DB after insert via real converter"
        assert result.state.value == "F", (
            f"Expected state='F', got {result.state.value!r}"
        )
        assert result.owner == "juser", (
            f"Expected owner='juser', got {result.owner!r}"
        )
        assert result.queue == "normal", (
            f"Expected queue='normal', got {result.queue!r}"
        )

    def test_real_converter_orm_object_raw_pbs_data_preserved(self, initialized_db):
        """
        raw_pbs_data from the PBSJob.raw_attributes must survive the ORM→DB round-trip.
        This verifies that actual column fields are copied, not just job_id/state.
        """
        from pbs_monitor.database.model_converters import ModelConverters
        from pbs_monitor.database.repositories import JobRepository

        converters = ModelConverters()
        repo = JobRepository(initialized_db)

        pbs_job = _make_terminal_pbs_job(job_id="raw_data_test.pbs", state_char="F")
        job_obj = converters.job.to_database(pbs_job)

        repo.apply_reconciliation_batch(to_insert=[job_obj], to_repair=[])

        result = repo.get_job_by_id("raw_data_test.pbs")
        assert result is not None
        assert result.raw_pbs_data is not None, (
            "raw_pbs_data was None after ORM insert — raw_attributes not propagated"
        )
        assert "exec_host" in result.raw_pbs_data, (
            f"raw_pbs_data missing 'exec_host': {result.raw_pbs_data}"
        )

    def test_real_converter_conflict_do_nothing_is_safe(self, initialized_db):
        """
        Inserting the same real-converter ORM object twice must not raise;
        the second call must be a no-op (conflict-do-nothing).
        """
        from pbs_monitor.database.model_converters import ModelConverters
        from pbs_monitor.database.repositories import JobRepository

        converters = ModelConverters()
        repo = JobRepository(initialized_db)

        pbs_job = _make_terminal_pbs_job(job_id="idempotent_orm.pbs", state_char="F")
        job_obj = converters.job.to_database(pbs_job)

        counts1 = repo.apply_reconciliation_batch(to_insert=[job_obj], to_repair=[])
        # Second call with a fresh ORM object for the same job_id
        job_obj2 = converters.job.to_database(pbs_job)
        counts2 = repo.apply_reconciliation_batch(to_insert=[job_obj2], to_repair=[])

        assert counts1.get("inserted") == 1, f"First insert should land: {counts1}"
        assert counts2.get("inserted") == 0, (
            f"Second insert for same job_id must be a no-op, got inserted={counts2.get('inserted')}"
        )

    def test_real_converter_orm_object_unknown_end_repair(self, initialized_db):
        """
        UNKNOWN_END repair path: convert a real PBSJob to a Job ORM object,
        then pass it as a repair item. The UNKNOWN_END row must be updated.
        """
        from pbs_monitor.database.model_converters import ModelConverters
        from pbs_monitor.database.models import Job, JobState
        from pbs_monitor.database.repositories import JobRepository

        converters = ModelConverters()
        repo = JobRepository(initialized_db)

        # Insert an UNKNOWN_END stub
        with repo.get_session() as session:
            stub = Job(
                job_id="repair_orm.pbs",
                state=JobState.UNKNOWN_END,
                owner="old_owner",
                queue="normal",
            )
            session.add(stub)
            session.commit()

        # Convert the real F-state PBS job and use it as the repair payload
        pbs_job = _make_terminal_pbs_job(job_id="repair_orm.pbs", state_char="F")
        job_obj = converters.job.to_database(pbs_job)

        counts = repo.apply_reconciliation_batch(to_insert=[], to_repair=[job_obj])

        assert counts.get("repaired") == 1, (
            f"Expected repaired=1 for UNKNOWN_END row, got {counts.get('repaired')}. "
            "The real ORM object repair path is broken."
        )

        updated = repo.get_job_by_id("repair_orm.pbs")
        assert updated.state.value == "F", (
            f"After repair, state is {updated.state.value!r}, expected 'F'."
        )


# ===========================================================================
# ORM-2: Invalid repair state must fail closed — do not mark final without
#         a valid target state
# ===========================================================================


class TestORM2InvalidRepairStateFailsClosed:
    """
    When a repair dict/ORM object has an invalid or missing 'state', the
    apply_reconciliation_batch repair path must skip the row entirely
    rather than applying a partial UPDATE that leaves UNKNOWN_END in an
    ambiguous half-repaired state (final_state_recorded=True but state
    still UNKNOWN_END).
    """

    def test_repair_with_invalid_state_string_leaves_row_unchanged(self, initialized_db):
        """
        A repair dict whose 'state' is an unrecognized string must NOT update
        the DB row. The UNKNOWN_END row must remain exactly as it was.
        """
        from pbs_monitor.database.models import Job, JobState
        from pbs_monitor.database.repositories import JobRepository

        repo = JobRepository(initialized_db)

        with repo.get_session() as session:
            stub = Job(
                job_id="invalid_repair_state.pbs",
                state=JobState.UNKNOWN_END,
                owner="orig",
                queue="q",
            )
            session.add(stub)
            session.commit()

        # Repair dict with a garbage state value
        repair_dict = {
            "job_id": "invalid_repair_state.pbs",
            "state": "NOT_A_REAL_STATE_XYZ",
            "owner": "new_owner",
            "queue": "q",
        }

        # Must not raise; must silently skip the row
        counts = repo.apply_reconciliation_batch(to_insert=[], to_repair=[repair_dict])

        # The row must be unchanged
        row = repo.get_job_by_id("invalid_repair_state.pbs")
        assert row is not None
        assert row.state == JobState.UNKNOWN_END, (
            f"State changed from UNKNOWN_END to {row.state!r} despite invalid repair state. "
            "Invalid repair state must fail closed (row skipped, not partially updated)."
        )
        assert row.owner == "orig", (
            f"Owner changed from 'orig' to {row.owner!r} despite invalid repair state. "
            "No fields should be updated when repair state is invalid."
        )
        assert counts.get("repaired") == 0, (
            f"repaired count is {counts.get('repaired')}, expected 0 for an invalid-state skip."
        )

    def test_repair_with_invalid_state_does_not_set_final_state_recorded(self, initialized_db):
        """
        The partial-update bug: current code updates final_state_recorded=True
        even when the state is invalid, leaving the row in a confusing half-state.
        After the fix, no fields may be updated when the repair state is invalid.
        """
        from pbs_monitor.database.models import Job, JobState
        from pbs_monitor.database.repositories import JobRepository
        from sqlalchemy import inspect as sa_inspect, text

        repo = JobRepository(initialized_db)

        # Create an UNKNOWN_END row with final_state_recorded=False (default)
        with repo.get_session() as session:
            stub = Job(
                job_id="partial_update_bug.pbs",
                state=JobState.UNKNOWN_END,
                owner="orig",
                queue="q",
            )
            # Explicitly set to False to detect any partial write
            stub.final_state_recorded = False
            session.add(stub)
            session.commit()

        repair_dict = {
            "job_id": "partial_update_bug.pbs",
            "state": "GARBAGE_STATE",
            "owner": "new",
            "queue": "q",
        }
        repo.apply_reconciliation_batch(to_insert=[], to_repair=[repair_dict])

        # Read raw to see if final_state_recorded was touched
        with repo.get_session() as session:
            row = session.query(Job).filter_by(job_id="partial_update_bug.pbs").first()
            assert row is not None
            # final_state_recorded must remain False — the invalid repair must be a total no-op
            assert row.final_state_recorded is not True, (
                "BUG: apply_reconciliation_batch set final_state_recorded=True even "
                "though the repair state was invalid. An invalid repair must be a "
                "complete no-op — no fields updated."
            )

    def test_repair_with_missing_state_key_leaves_row_unchanged(self, initialized_db):
        """
        A repair dict missing the 'state' key entirely must also fail closed.
        No UPDATE should be applied.
        """
        from pbs_monitor.database.models import Job, JobState
        from pbs_monitor.database.repositories import JobRepository

        repo = JobRepository(initialized_db)

        with repo.get_session() as session:
            stub = Job(
                job_id="missing_repair_state.pbs",
                state=JobState.UNKNOWN_END,
                owner="orig",
                queue="q",
            )
            session.add(stub)
            session.commit()

        repair_dict = {
            "job_id": "missing_repair_state.pbs",
            # 'state' deliberately omitted
            "owner": "new",
            "queue": "q",
        }
        counts = repo.apply_reconciliation_batch(to_insert=[], to_repair=[repair_dict])

        row = repo.get_job_by_id("missing_repair_state.pbs")
        assert row.state == JobState.UNKNOWN_END, (
            f"State changed from UNKNOWN_END to {row.state!r} despite missing repair state. "
            "A missing state key must cause the repair to be skipped."
        )
        assert counts.get("repaired") == 0, (
            f"repaired count is {counts.get('repaired')}, expected 0 when repair state is absent."
        )


# ===========================================================================
# Full reconciler integration: real converter, real repo, real SQLite
# ===========================================================================


class TestFullReconcilerIntegration:
    """
    End-to-end: real HistoryReconciler + real ModelConverters().job + real JobRepository.

    No mocking of the converter. The reconciler must correctly orchestrate
    conversion (returning a Job ORM object) and pass it through to the repository.
    """

    def test_full_reconciler_inserts_terminal_job_using_real_converter(self, initialized_db):
        """
        HistoryReconciler driven by a real PBS-like terminal job list, real
        ModelConverters, and real JobRepository against SQLite.

        After reconcile(), the job must appear in the DB with correct fields.
        This is the primary integration guard for the ORM-type-mismatch defect.
        """
        from unittest.mock import MagicMock

        from pbs_monitor.database.model_converters import ModelConverters
        from pbs_monitor.database.repositories import JobRepository
        from pbs_monitor.history_reconciliation import HistoryReconciler

        converters = ModelConverters()
        repo = JobRepository(initialized_db)

        pbs_job = _make_terminal_pbs_job(job_id="full_int_test.pbs", state_char="F")

        pbs_mock = MagicMock()
        pbs_mock.qstat_completed_jobs.return_value = [pbs_job]

        reconciler = HistoryReconciler(
            pbs_commands=pbs_mock,
            repository=repo,
            converter=converters.job,  # real converter, not a mock
        )
        result = reconciler.reconcile()

        assert result.status == "success", (
            f"Reconciliation failed: {result.error_message}"
        )
        assert result.inserted == 1, (
            f"Expected inserted=1, got {result.inserted}. "
            "The real converter ORM object was not accepted by the repository."
        )
        assert result.repaired == 0
        assert result.unchanged == 0

        row = repo.get_job_by_id("full_int_test.pbs")
        assert row is not None, "Job not found in DB after full reconciliation"
        assert row.state.value == "F"
        assert row.owner == "juser"
        assert row.raw_pbs_data is not None

    def test_full_reconciler_repairs_unknown_end_using_real_converter(self, initialized_db):
        """
        HistoryReconciler with real converter repairs an UNKNOWN_END row.
        Verifies the full ORM-object repair path end-to-end.
        """
        from unittest.mock import MagicMock

        from pbs_monitor.database.model_converters import ModelConverters
        from pbs_monitor.database.models import Job, JobState
        from pbs_monitor.database.repositories import JobRepository
        from pbs_monitor.history_reconciliation import HistoryReconciler

        converters = ModelConverters()
        repo = JobRepository(initialized_db)

        # Pre-insert an UNKNOWN_END row
        with repo.get_session() as session:
            stub = Job(
                job_id="full_repair_int.pbs",
                state=JobState.UNKNOWN_END,
                owner="old",
                queue="normal",
            )
            session.add(stub)
            session.commit()

        pbs_job = _make_terminal_pbs_job(job_id="full_repair_int.pbs", state_char="F")

        pbs_mock = MagicMock()
        pbs_mock.qstat_completed_jobs.return_value = [pbs_job]

        reconciler = HistoryReconciler(
            pbs_commands=pbs_mock,
            repository=repo,
            converter=converters.job,
        )
        result = reconciler.reconcile()

        assert result.status == "success", f"Reconciliation failed: {result.error_message}"
        assert result.repaired == 1, (
            f"Expected repaired=1, got {result.repaired}. "
            "UNKNOWN_END repair via real converter did not work."
        )
        assert result.inserted == 0

        row = repo.get_job_by_id("full_repair_int.pbs")
        assert row.state.value == "F", (
            f"After repair, state is {row.state.value!r}, expected 'F'."
        )
