"""
TDD tests for parser-level malformed-record accounting in reconciliation.

Covers:
- QstatDetailedResult dataclass exists with (jobs, parse_errors) fields.
- PBSCommands.qstat_completed_jobs_detailed() returns QstatDetailedResult.
- With a real parser fixture containing 1 valid terminal + 1 malformed record:
    * the valid job is returned in jobs
    * parse_errors == 1
- With malformed whole-JSON:
    * PBSCommandError is raised (zero writes path via existing reconciler contract)
- HistoryReconciler._do_reconcile() accumulates parser-level parse_errors into
  ReconciliationResult.parse_errors when pbs_commands has qstat_completed_jobs_detailed().
- Existing qstat_completed_jobs() callers are unaffected (API unchanged).
- Repair result counts are correct when invalid-item skips occur in the same batch.

Design invariants confirmed:
- Whole-response parse failure → PBSCommandError → reconciler catches → zero writes, status=failed
- Individual malformed records → counted in parse_errors, skipped; rest proceeds
"""

import copy
import json
import tempfile
import os
import types
from pathlib import Path
from typing import List, Optional
from unittest.mock import MagicMock, patch
from datetime import datetime, timezone

import pytest

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

WORKTREE = Path(__file__).parent.parent

SAMPLE_COMPLETED_JOBS_PATH = (
    WORKTREE / "pbs_monitor" / "sample_json" / "qstat_x_f_F_json-output.json"
)


def _load_sample_jobs_raw() -> dict:
    """Load the real sample completed-jobs JSON."""
    with open(SAMPLE_COMPLETED_JOBS_PATH) as f:
        return json.load(f)


def _make_pbs_job(job_id: str, state_char: str, end_time=None):
    """Minimal PBSJob-like mock."""
    obj = MagicMock()
    obj.job_id = job_id
    state_mock = MagicMock()
    state_mock.value = state_char
    obj.state = state_mock
    obj.owner = "testuser"
    obj.queue = "debug"
    obj.end_time = end_time or datetime(2026, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
    return obj


# ---------------------------------------------------------------------------
# Fixture: in-memory PBSCommands with injected raw JSON
# ---------------------------------------------------------------------------


def _make_pbs_commands_with_raw_json(raw_json_str: str):
    """
    Return a PBSCommands instance that, when qstat_completed_jobs_detailed()
    is called (no job_ids), parses raw_json_str as if it were the subprocess
    output.  No real subprocess is spawned.
    """
    from pbs_monitor.pbs_commands import PBSCommands

    cmd = PBSCommands(use_sample_data=False)

    def _fake_run_command(command, timeout=None):
        return raw_json_str

    cmd._run_command = _fake_run_command
    return cmd


# ===========================================================================
# SECTION 1 – QstatDetailedResult dataclass
# ===========================================================================


class TestQstatDetailedResult:
    """QstatDetailedResult must be importable and have required fields."""

    def test_importable(self):
        from pbs_monitor.pbs_commands import QstatDetailedResult  # noqa: F401

    def test_has_jobs_field(self):
        from pbs_monitor.pbs_commands import QstatDetailedResult

        r = QstatDetailedResult(jobs=[], parse_errors=0)
        assert hasattr(r, "jobs")
        assert r.jobs == []

    def test_has_parse_errors_field(self):
        from pbs_monitor.pbs_commands import QstatDetailedResult

        r = QstatDetailedResult(jobs=[], parse_errors=3)
        assert r.parse_errors == 3

    def test_default_parse_errors_zero(self):
        from pbs_monitor.pbs_commands import QstatDetailedResult

        r = QstatDetailedResult(jobs=[])
        assert r.parse_errors == 0


# ===========================================================================
# SECTION 2 – PBSCommands.qstat_completed_jobs_detailed() API
# ===========================================================================


class TestQstatCompletedJobsDetailedExists:
    """qstat_completed_jobs_detailed() method must exist on PBSCommands."""

    def test_method_exists(self):
        from pbs_monitor.pbs_commands import PBSCommands

        cmd = PBSCommands()
        assert hasattr(cmd, "qstat_completed_jobs_detailed"), (
            "PBSCommands is missing qstat_completed_jobs_detailed() method"
        )

    def test_signature_accepts_same_args_as_qstat_completed_jobs(self):
        """Should accept user, project, days, job_ids (same as qstat_completed_jobs)."""
        import inspect
        from pbs_monitor.pbs_commands import PBSCommands

        sig = inspect.signature(PBSCommands.qstat_completed_jobs_detailed)
        params = set(sig.parameters.keys()) - {"self"}
        for expected in ("user", "project", "days", "job_ids"):
            assert expected in params, (
                f"qstat_completed_jobs_detailed() is missing parameter '{expected}'"
            )

    def test_returns_qstat_detailed_result(self):
        """Calling with sample_data=True returns a QstatDetailedResult."""
        from pbs_monitor.pbs_commands import PBSCommands, QstatDetailedResult

        cmd = PBSCommands(use_sample_data=True)
        result = cmd.qstat_completed_jobs_detailed()

        assert isinstance(result, QstatDetailedResult), (
            f"Expected QstatDetailedResult, got {type(result).__name__}"
        )

    def test_sample_data_jobs_are_list(self):
        """qstat_completed_jobs_detailed() returns jobs as a list."""
        from pbs_monitor.pbs_commands import PBSCommands

        cmd = PBSCommands(use_sample_data=True)
        result = cmd.qstat_completed_jobs_detailed()
        assert isinstance(result.jobs, list)

    def test_sample_data_parse_errors_is_int(self):
        from pbs_monitor.pbs_commands import PBSCommands

        cmd = PBSCommands(use_sample_data=True)
        result = cmd.qstat_completed_jobs_detailed()
        assert isinstance(result.parse_errors, int)


# ===========================================================================
# SECTION 3 – Real parser fixture: 1 valid terminal + 1 malformed record
# ===========================================================================


class TestDetailedWithMixedFixture:
    """
    Use real PBSCommands parser with a synthetic fixture:
    - one valid terminal (F) job
    - one malformed job record (missing Job_Owner → PBSJob.from_qstat_json fails)

    Expected: valid job returned; parse_errors == 1.
    """

    def _build_mixed_fixture_json(self) -> str:
        """Build JSON with 1 good F-state job + 1 malformed record."""
        raw = _load_sample_jobs_raw()

        # Pick first terminal job as the valid one
        good_id = None
        good_data = None
        for jid, jdata in raw["Jobs"].items():
            if jdata.get("job_state") == "F":
                good_id = jid
                good_data = copy.deepcopy(jdata)
                break

        assert good_id is not None, "No F-state job in fixture"

        # Malformed: missing Job_Owner + set state to F but make it
        # unparseable by having an invalid job_state value that our
        # fallback won't silently accept — actually the real parser
        # tolerates missing fields, so let's cause a TypeError by making
        # job_state a non-string type that breaks internal logic.
        # The real parser calls JobState(state_str); None will cause TypeError.
        bad_id = "9999999.aurora-pbs-0001.hostmgmt.cm.aurora.alcf.anl.gov"
        bad_data = {
            "Job_Name": "bad_job",
            "Job_Owner": None,  # None → str(None).split('@')[0] = 'None' — safe
            # Force a real parse error: invalid walltime that causes a crash
            # Actually Job_Owner=None is silently handled. Use resources_used
            # with a bad ctime that crashes the date parser.
            "job_state": "F",
            "queue": "debug",
            # Omit required ctime, mtime — that's fine, optional fields
            # The real fault: set Job_Owner to a non-string non-None that
            # will trigger an AttributeError on .split('@')
            "TRIGGER_PARSE_ERROR": True,
        }
        # Override: make Job_Owner an integer so .split('@') raises AttributeError
        bad_data["Job_Owner"] = 12345  # int.split('@') → AttributeError

        payload = {
            "timestamp": raw["timestamp"],
            "pbs_version": raw["pbs_version"],
            "pbs_server": raw["pbs_server"],
            "Jobs": {
                good_id: good_data,
                bad_id: bad_data,
            },
        }
        return json.dumps(payload)

    def test_valid_job_returned(self):
        """Good job is parsed and returned in result.jobs."""
        from pbs_monitor.pbs_commands import PBSCommands

        fixture_json = self._build_mixed_fixture_json()
        cmd = _make_pbs_commands_with_raw_json(fixture_json)

        result = cmd.qstat_completed_jobs_detailed()

        assert len(result.jobs) == 1, (
            f"Expected 1 valid job, got {len(result.jobs)}"
        )
        assert result.jobs[0].state.value == "F"

    def test_parse_errors_equals_one(self):
        """Malformed record is counted; parse_errors == 1."""
        from pbs_monitor.pbs_commands import PBSCommands

        fixture_json = self._build_mixed_fixture_json()
        cmd = _make_pbs_commands_with_raw_json(fixture_json)

        result = cmd.qstat_completed_jobs_detailed()

        assert result.parse_errors == 1, (
            f"Expected parse_errors=1 for one malformed record, got {result.parse_errors}"
        )

    def test_total_parse_errors_plus_jobs_equals_input_count(self):
        """parse_errors + len(jobs) == total records in fixture (2)."""
        from pbs_monitor.pbs_commands import PBSCommands

        fixture_json = self._build_mixed_fixture_json()
        cmd = _make_pbs_commands_with_raw_json(fixture_json)

        result = cmd.qstat_completed_jobs_detailed()

        total = len(result.jobs) + result.parse_errors
        assert total == 2, (
            f"Expected jobs({len(result.jobs)}) + parse_errors({result.parse_errors}) == 2, "
            f"got {total}"
        )


# ===========================================================================
# SECTION 4 – Malformed whole-JSON → PBSCommandError raised (zero writes)
# ===========================================================================


class TestDetailedWholeJsonParseFailure:
    """
    When the entire JSON response is unparseable, qstat_completed_jobs_detailed()
    must raise PBSCommandError (not swallow it). The reconciler's existing
    PBSCommandError handler then ensures zero writes.
    """

    def test_raises_pbs_command_error_on_invalid_json(self):
        from pbs_monitor.pbs_commands import PBSCommands, PBSCommandError

        cmd = _make_pbs_commands_with_raw_json("THIS IS NOT JSON {{{")

        with pytest.raises(PBSCommandError):
            cmd.qstat_completed_jobs_detailed()

    def test_raises_pbs_command_error_on_empty_string(self):
        """Empty output is treated as empty (no error); but truly malformed raises."""
        from pbs_monitor.pbs_commands import PBSCommands, PBSCommandError

        # Truncated valid-looking JSON is malformed
        cmd = _make_pbs_commands_with_raw_json('{"Jobs": {invalid}')

        with pytest.raises(PBSCommandError):
            cmd.qstat_completed_jobs_detailed()

    def test_reconciler_zero_writes_on_whole_json_failure(self):
        """When qstat_completed_jobs_detailed raises PBSCommandError, reconciler writes nothing."""
        from pbs_monitor.history_reconciliation import HistoryReconciler
        from pbs_monitor.pbs_commands import PBSCommandError

        pbs_mock = MagicMock()
        # Provide qstat_completed_jobs_detailed that raises PBSCommandError
        pbs_mock.qstat_completed_jobs_detailed.side_effect = PBSCommandError("whole json bad")
        # Also make qstat_completed_jobs raise to avoid fallback confusion
        pbs_mock.qstat_completed_jobs.side_effect = PBSCommandError("whole json bad")

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


# ===========================================================================
# SECTION 5 – HistoryReconciler folds parser-level parse_errors
# ===========================================================================


class TestReconcilerFoldsParserParseErrors:
    """
    When pbs_commands has qstat_completed_jobs_detailed(), the reconciler
    must call it and fold its parse_errors into ReconciliationResult.parse_errors.
    """

    def _make_reconciler_with_detailed(self, jobs, parser_parse_errors):
        """
        Return a HistoryReconciler whose pbs_commands supports
        qstat_completed_jobs_detailed() returning given jobs + parse_errors.
        """
        from pbs_monitor.history_reconciliation import HistoryReconciler
        from pbs_monitor.pbs_commands import QstatDetailedResult

        pbs_mock = MagicMock(spec=["qstat_completed_jobs_detailed", "qstat_completed_jobs"])
        pbs_mock.qstat_completed_jobs_detailed.return_value = QstatDetailedResult(
            jobs=jobs, parse_errors=parser_parse_errors
        )
        # Also expose qstat_completed_jobs as a fallback (returns jobs only)
        pbs_mock.qstat_completed_jobs.return_value = jobs

        repo_mock = MagicMock()
        repo_mock.fetch_states_for_ids.return_value = {}
        repo_mock.apply_reconciliation_batch.side_effect = (
            lambda to_insert, to_repair: {"inserted": len(to_insert), "repaired": len(to_repair)}
        )

        converter_mock = MagicMock()
        converter_mock.to_database.side_effect = lambda j: {"job_id": j.job_id}

        return HistoryReconciler(
            pbs_commands=pbs_mock,
            repository=repo_mock,
            converter=converter_mock,
        )

    def test_parser_parse_errors_folded_into_result(self):
        """parse_errors from qstat_detailed must appear in ReconciliationResult."""
        good_job = _make_pbs_job("good.pbs", "F")
        reconciler = self._make_reconciler_with_detailed(
            jobs=[good_job], parser_parse_errors=1
        )

        result = reconciler.reconcile()

        assert result.parse_errors >= 1, (
            f"Expected parse_errors >= 1 (from parser), got {result.parse_errors}"
        )

    def test_valid_job_still_inserted_despite_parser_errors(self):
        """The valid job is still inserted even when parse_errors > 0."""
        good_job = _make_pbs_job("good.pbs", "F")
        reconciler = self._make_reconciler_with_detailed(
            jobs=[good_job], parser_parse_errors=1
        )

        result = reconciler.reconcile()

        assert result.inserted == 1, (
            f"Valid job should be inserted; got inserted={result.inserted}"
        )

    def test_parser_parse_errors_accumulate_with_converter_errors(self):
        """
        Parser-level parse_errors + converter-level parse_errors both
        accumulate into the same result.parse_errors counter.
        """
        from pbs_monitor.history_reconciliation import HistoryReconciler
        from pbs_monitor.pbs_commands import QstatDetailedResult

        good_job = _make_pbs_job("good.pbs", "F")
        converter_bad_job = _make_pbs_job("converter_bad.pbs", "F")

        pbs_mock = MagicMock(spec=["qstat_completed_jobs_detailed", "qstat_completed_jobs"])
        pbs_mock.qstat_completed_jobs_detailed.return_value = QstatDetailedResult(
            jobs=[good_job, converter_bad_job],
            parse_errors=1,  # 1 from parser level
        )
        pbs_mock.qstat_completed_jobs.return_value = [good_job, converter_bad_job]

        repo_mock = MagicMock()
        repo_mock.fetch_states_for_ids.return_value = {}
        repo_mock.apply_reconciliation_batch.side_effect = (
            lambda to_insert, to_repair: {"inserted": len(to_insert), "repaired": len(to_repair)}
        )

        converter_mock = MagicMock()

        def _convert(j):
            if j.job_id == "converter_bad.pbs":
                raise ValueError("converter error")
            return {"job_id": j.job_id}

        converter_mock.to_database.side_effect = _convert

        reconciler = HistoryReconciler(
            pbs_commands=pbs_mock,
            repository=repo_mock,
            converter=converter_mock,
        )
        result = reconciler.reconcile()

        # 1 from parser + 1 from converter = 2
        assert result.parse_errors == 2, (
            f"Expected parse_errors=2 (1 parser + 1 converter), got {result.parse_errors}"
        )
        assert result.inserted == 1

    def test_reconciler_uses_detailed_not_plain_when_available(self):
        """
        When qstat_completed_jobs_detailed() is available, reconciler must
        call it instead of qstat_completed_jobs() so parser errors are captured.
        """
        from pbs_monitor.history_reconciliation import HistoryReconciler
        from pbs_monitor.pbs_commands import QstatDetailedResult

        good_job = _make_pbs_job("good.pbs", "F")

        pbs_mock = MagicMock(spec=["qstat_completed_jobs_detailed", "qstat_completed_jobs"])
        pbs_mock.qstat_completed_jobs_detailed.return_value = QstatDetailedResult(
            jobs=[good_job], parse_errors=0
        )
        pbs_mock.qstat_completed_jobs.return_value = [good_job]

        repo_mock = MagicMock()
        repo_mock.fetch_states_for_ids.return_value = {}
        repo_mock.apply_reconciliation_batch.return_value = {"inserted": 1, "repaired": 0}

        converter_mock = MagicMock()
        converter_mock.to_database.return_value = {"job_id": "good.pbs"}

        reconciler = HistoryReconciler(
            pbs_commands=pbs_mock,
            repository=repo_mock,
            converter=converter_mock,
        )
        reconciler.reconcile()

        pbs_mock.qstat_completed_jobs_detailed.assert_called_once()
        pbs_mock.qstat_completed_jobs.assert_not_called()

    def test_reconciler_falls_back_to_plain_when_detailed_absent(self):
        """
        When pbs_commands does NOT have qstat_completed_jobs_detailed(),
        reconciler must fall back to qstat_completed_jobs() without error.
        """
        from pbs_monitor.history_reconciliation import HistoryReconciler

        good_job = _make_pbs_job("good.pbs", "F")

        # spec without qstat_completed_jobs_detailed — simulates old PBSCommands
        pbs_mock = MagicMock(spec=["qstat_completed_jobs"])
        pbs_mock.qstat_completed_jobs.return_value = [good_job]

        repo_mock = MagicMock()
        repo_mock.fetch_states_for_ids.return_value = {}
        repo_mock.apply_reconciliation_batch.return_value = {"inserted": 1, "repaired": 0}

        converter_mock = MagicMock()
        converter_mock.to_database.return_value = {"job_id": "good.pbs"}

        reconciler = HistoryReconciler(
            pbs_commands=pbs_mock,
            repository=repo_mock,
            converter=converter_mock,
        )
        result = reconciler.reconcile()

        pbs_mock.qstat_completed_jobs.assert_called_once()
        assert result.inserted == 1


# ===========================================================================
# SECTION 6 – Repair counts after invalid-item skips
# ===========================================================================


class TestRepairCountsAfterInvalidSkips:
    """
    When a batch includes UNKNOWN_END repair candidates alongside malformed
    items, repair counts must reflect only actually converted-and-written rows.
    """

    def test_repair_count_excludes_skipped_malformed(self):
        """
        3 UNKNOWN_END candidates; 1 malformed (converter raises).
        repaired should be 2, parse_errors should include 1 from converter.
        """
        from pbs_monitor.history_reconciliation import HistoryReconciler
        from pbs_monitor.pbs_commands import QstatDetailedResult

        job_a = _make_pbs_job("a.pbs", "F")
        job_b = _make_pbs_job("b.pbs", "F")
        job_c = _make_pbs_job("c.pbs", "F")  # this one will fail converter

        pbs_mock = MagicMock(spec=["qstat_completed_jobs_detailed", "qstat_completed_jobs"])
        pbs_mock.qstat_completed_jobs_detailed.return_value = QstatDetailedResult(
            jobs=[job_a, job_b, job_c], parse_errors=0
        )
        pbs_mock.qstat_completed_jobs.return_value = [job_a, job_b, job_c]

        repo_mock = MagicMock()
        repo_mock.fetch_states_for_ids.return_value = {
            "a.pbs": "UNKNOWN_END",
            "b.pbs": "UNKNOWN_END",
            "c.pbs": "UNKNOWN_END",
        }
        repo_mock.apply_reconciliation_batch.side_effect = (
            lambda to_insert, to_repair: {"inserted": len(to_insert), "repaired": len(to_repair)}
        )

        converter_mock = MagicMock()

        def _convert(j):
            if j.job_id == "c.pbs":
                raise ValueError("malformed field")
            return {"job_id": j.job_id}

        converter_mock.to_database.side_effect = _convert

        reconciler = HistoryReconciler(
            pbs_commands=pbs_mock,
            repository=repo_mock,
            converter=converter_mock,
        )
        result = reconciler.reconcile()

        assert result.repaired == 2, (
            f"Expected repaired=2 (a + b), got {result.repaired}"
        )
        assert result.parse_errors >= 1, (
            f"Expected parse_errors>=1 for c.pbs converter failure, got {result.parse_errors}"
        )

    def test_insert_and_repair_mixed_with_parser_parse_errors(self):
        """
        Full integration: parser reports 1 skipped record + valid jobs
        include 1 insert candidate and 1 UNKNOWN_END repair. All counts correct.
        """
        from pbs_monitor.history_reconciliation import HistoryReconciler
        from pbs_monitor.pbs_commands import QstatDetailedResult

        insert_job = _make_pbs_job("new.pbs", "F")
        repair_job = _make_pbs_job("old.pbs", "F")

        pbs_mock = MagicMock(spec=["qstat_completed_jobs_detailed", "qstat_completed_jobs"])
        pbs_mock.qstat_completed_jobs_detailed.return_value = QstatDetailedResult(
            jobs=[insert_job, repair_job],
            parse_errors=1,  # 1 record skipped by parser
        )

        repo_mock = MagicMock()
        repo_mock.fetch_states_for_ids.return_value = {"old.pbs": "UNKNOWN_END"}
        repo_mock.apply_reconciliation_batch.side_effect = (
            lambda to_insert, to_repair: {"inserted": len(to_insert), "repaired": len(to_repair)}
        )

        converter_mock = MagicMock()
        converter_mock.to_database.side_effect = lambda j: {"job_id": j.job_id}

        reconciler = HistoryReconciler(
            pbs_commands=pbs_mock,
            repository=repo_mock,
            converter=converter_mock,
        )
        result = reconciler.reconcile()

        assert result.inserted == 1
        assert result.repaired == 1
        assert result.parse_errors == 1  # from parser, no converter errors


# ===========================================================================
# SECTION 7 – Backward-compat: qstat_completed_jobs() unchanged
# ===========================================================================


class TestQstatCompletedJobsUnchanged:
    """
    qstat_completed_jobs() must continue to work exactly as before.
    Existing callers must not be broken.
    """

    def test_qstat_completed_jobs_still_returns_list(self):
        from pbs_monitor.pbs_commands import PBSCommands

        cmd = PBSCommands(use_sample_data=True)
        jobs = cmd.qstat_completed_jobs()
        assert isinstance(jobs, list)

    def test_qstat_completed_jobs_signature_unchanged(self):
        """Verify the method signature still has user, project, days, job_ids."""
        import inspect
        from pbs_monitor.pbs_commands import PBSCommands

        sig = inspect.signature(PBSCommands.qstat_completed_jobs)
        params = set(sig.parameters.keys()) - {"self"}
        for expected in ("user", "project", "days", "job_ids"):
            assert expected in params, (
                f"qstat_completed_jobs() missing parameter '{expected}' — API broken"
            )

    def test_qstat_completed_jobs_returns_pbsjob_objects(self):
        from pbs_monitor.pbs_commands import PBSCommands
        from pbs_monitor.models.job import PBSJob

        cmd = PBSCommands(use_sample_data=True)
        jobs = cmd.qstat_completed_jobs()
        for j in jobs:
            assert isinstance(j, PBSJob), (
                f"qstat_completed_jobs() returned non-PBSJob object: {type(j)}"
            )
