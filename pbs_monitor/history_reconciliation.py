"""
PBS History Reconciliation Service

A focused service that discovers terminal PBS jobs missed during collector
outages and repairs UNKNOWN_END rows when PBS retains an authoritative
terminal record.

Design invariants
-----------------
- Only F and C (scheduler-terminal) records are candidates.
- Parse the complete response before writing.
- Missing rows → INSERT with conflict-do-nothing (race-safe).
- UNKNOWN_END rows → UPDATE guarded by WHERE state = UNKNOWN_END (race-safe).
- Existing non-UNKNOWN_END rows → untouched (unchanged).
- Whole-response parse failure → zero writes.
- Individual-record errors → counted, skipped; rest of batch proceeds.
- Audit write is best-effort; failure is logged, not re-raised.
- Dry-run → classify fully, report, write nothing.
"""

import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from .pbs_commands import PBSCommandError, QstatDetailedResult

logger = logging.getLogger(__name__)

# Scheduler-terminal state codes accepted for import.
_TERMINAL_STATES = frozenset({"F", "C"})


@dataclass
class ReconciliationResult:
    """Structured result of a single reconciliation run."""

    # Raw PBS response counts
    observed: int = 0
    terminal_candidates: int = 0

    # Classification outcomes (set to actual DB rows affected after write)
    inserted: int = 0
    repaired: int = 0
    unchanged: int = 0
    parse_errors: int = 0

    # Terminal timestamp range of candidates processed
    oldest_terminal_time: Optional[datetime] = None
    newest_terminal_time: Optional[datetime] = None

    # Run metadata
    start_time: Optional[datetime] = None
    end_time: Optional[datetime] = None
    duration_seconds: float = 0.0
    status: str = "success"   # "success" | "failed"
    error_message: Optional[str] = None


class HistoryReconciler:
    """Reconcile the PBS full-history response against the local database.

    Dependency-injected for testability: accepts PBS command wrapper, job
    repository, model converter, and optional audit repository.

    Parameters
    ----------
    pbs_commands:
        Object with a ``qstat_completed_jobs()`` method that returns a list
        of PBSJob-like objects (including E state; we filter to F/C here).
    repository:
        ``JobRepository``-like object with ``fetch_states_for_ids`` and
        ``apply_reconciliation_batch`` methods.
    converter:
        Object with a ``to_database(pbs_job)`` method that returns a dict
        suitable for passing to ``apply_reconciliation_batch``.
    audit_repository:
        Optional repository with a ``write_reconciliation_log(result)``
        method. When provided, one row is written after every run.
        Failures are logged and swallowed (best-effort).
    clock:
        Callable returning the current datetime (UTC). Used for testing.
    """

    def __init__(
        self,
        pbs_commands: Any,
        repository: Any,
        converter: Any,
        audit_repository: Any = None,
        clock=None,
    ):
        self._pbs = pbs_commands
        self._repo = repository
        self._converter = converter
        self._audit_repo = audit_repository
        self._clock = clock or (lambda: datetime.now(timezone.utc))
        self._logger = logging.getLogger(__name__)

    def reconcile(
        self,
        dry_run: bool = False,
        batch_size: int = 500,
    ) -> ReconciliationResult:
        """Run one full reconciliation cycle.

        Parses the complete PBS history response before performing any writes.
        On whole-response parse failure, returns immediately with no writes.

        Parameters
        ----------
        dry_run:
            When True, classify and report but write nothing.
        batch_size:
            Number of IDs to look up per bulk-state query chunk.
            (The repository itself chunks its IN queries; this controls
            how many candidates are fetched per reconcile call — reserved
            for future streaming behaviour.)

        Returns
        -------
        ReconciliationResult
        """
        result = ReconciliationResult()
        result.start_time = self._clock()

        try:
            result = self._do_reconcile(result, dry_run=dry_run, batch_size=batch_size)
        except PBSCommandError as exc:
            result.status = "failed"
            result.error_message = str(exc)
            self._logger.error("History reconciliation failed (PBS error): %s", exc)
        except Exception as exc:
            result.status = "failed"
            result.error_message = str(exc)
            self._logger.error("History reconciliation failed: %s", exc, exc_info=True)
        finally:
            result.end_time = self._clock()
            result.duration_seconds = (
                (result.end_time - result.start_time).total_seconds()
                if result.start_time
                else 0.0
            )
            self._write_audit(result)

        return result

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _do_reconcile(
        self, result: ReconciliationResult, dry_run: bool, batch_size: int
    ) -> ReconciliationResult:
        """Core reconciliation logic — raises on whole-response failure."""
        # Phase 1: Fetch full history (raises PBSCommandError on failure → no writes).
        #
        # Prefer qstat_completed_jobs_detailed() when available — it returns
        # both the parsed jobs AND a count of per-record parse failures so we
        # can fold them into result.parse_errors.  Falls back gracefully to the
        # plain qstat_completed_jobs() for callers that inject a minimal stub.
        #
        # We verify the return value is a QstatDetailedResult (not just that
        # the method attribute exists on the mock/stub) so that unconstrained
        # MagicMock() objects in existing tests don't accidentally take the
        # detailed path.
        _detailed_fn = getattr(self._pbs, "qstat_completed_jobs_detailed", None)
        _used_detailed = False
        all_pbs_jobs: List[Any] = []
        if callable(_detailed_fn):
            _candidate = _detailed_fn()
            if isinstance(_candidate, QstatDetailedResult):
                all_pbs_jobs = _candidate.jobs
                result.parse_errors += _candidate.parse_errors
                _used_detailed = True
        if not _used_detailed:
            all_pbs_jobs = self._pbs.qstat_completed_jobs()

        # Phase 2: Filter to F/C terminal candidates only.
        # qstat -x returns E records too; we must independently require F or C.
        terminal_jobs: List[Any] = []
        for pbs_job in all_pbs_jobs:
            result.observed += 1
            if pbs_job.state.value in _TERMINAL_STATES:
                terminal_jobs.append(pbs_job)

        result.terminal_candidates = len(terminal_jobs)

        if not terminal_jobs:
            return result

        # Phase 3: Bulk-fetch current DB states for all candidate IDs.
        candidate_ids = {j.job_id for j in terminal_jobs}
        db_states: Dict[str, str] = self._repo.fetch_states_for_ids(
            candidate_ids, chunk_size=batch_size
        )

        # Phase 4: Classify each candidate and convert (parse before any writes).
        to_insert: List[Dict] = []
        to_repair: List[Dict] = []

        for pbs_job in terminal_jobs:
            job_id = pbs_job.job_id
            current_state = db_states.get(job_id)

            if current_state is None:
                # Missing → insert
                try:
                    job_dict = self._converter.to_database(pbs_job)
                    to_insert.append(job_dict)
                    # Track terminal time range
                    self._update_terminal_range(result, pbs_job)
                except Exception as exc:
                    result.parse_errors += 1
                    self._logger.warning(
                        "Skipping malformed job %s: %s", job_id, exc
                    )

            elif current_state == "UNKNOWN_END":
                # Repairable → update (guarded by WHERE state = UNKNOWN_END)
                try:
                    job_dict = self._converter.to_database(pbs_job)
                    to_repair.append(job_dict)
                    self._update_terminal_range(result, pbs_job)
                except Exception as exc:
                    result.parse_errors += 1
                    self._logger.warning(
                        "Skipping malformed job %s: %s", job_id, exc
                    )

            else:
                # Existing non-UNKNOWN_END → unchanged; do not mutate
                result.unchanged += 1

        # For dry-run: set planned counts (no writes happen)
        if dry_run:
            result.inserted = len(to_insert)
            result.repaired = len(to_repair)
            return result

        # Phase 5: Write and read back ACTUAL affected row counts.
        # apply_reconciliation_batch returns {"inserted": N, "repaired": M}
        # reflecting real DB rows written (may be < planned due to races).
        if to_insert or to_repair:
            # Write in chunks of batch_size so large history sets don't hold a
            # single lock indefinitely.  All parsing is complete before any writes
            # begin (parse-before-write invariant satisfied above).  Each chunk is
            # an independent call to apply_reconciliation_batch; if a chunk raises,
            # we stop writing and set status=failed so partial counts are not
            # falsely reported as a full success.
            for i in range(0, max(len(to_insert), len(to_repair), 1), batch_size):
                chunk_insert = to_insert[i:i + batch_size]
                chunk_repair = to_repair[i:i + batch_size]
                if not chunk_insert and not chunk_repair:
                    break
                try:
                    actual_counts = self._repo.apply_reconciliation_batch(
                        to_insert=chunk_insert,
                        to_repair=chunk_repair,
                    )
                except Exception as exc:
                    # Chunk write failed; stop writing and propagate failure so
                    # counts already accumulated are not falsely reported as success.
                    raise RuntimeError(
                        f"Write chunk at offset {i} failed: {exc}"
                    ) from exc
                # actual_counts must be a dict; if it is not (e.g. old/mock repo
                # returning None) we do NOT fall back to planned list lengths —
                # that would silently report wrong counts.  Raise instead so the
                # caller knows the contract is violated.
                if not isinstance(actual_counts, dict):
                    raise TypeError(
                        f"apply_reconciliation_batch must return a dict with "
                        f"'inserted' and 'repaired' keys; got {type(actual_counts).__name__!r}. "
                        "Repository contract violated."
                    )
                result.inserted += actual_counts.get("inserted", 0)
                result.repaired += actual_counts.get("repaired", 0)

        return result

    def _update_terminal_range(
        self, result: ReconciliationResult, pbs_job: Any
    ) -> None:
        """Update oldest/newest terminal time on result from a PBS job."""
        t = getattr(pbs_job, "end_time", None)
        if t is None:
            return
        if result.oldest_terminal_time is None or t < result.oldest_terminal_time:
            result.oldest_terminal_time = t
        if result.newest_terminal_time is None or t > result.newest_terminal_time:
            result.newest_terminal_time = t

    def _write_audit(self, result: ReconciliationResult) -> None:
        """Write audit row (best-effort — never raises)."""
        if self._audit_repo is None:
            return
        try:
            self._audit_repo.write_reconciliation_log(result)
        except Exception as exc:
            self._logger.error(
                "Failed to write reconciliation audit row (best-effort, job batches unaffected): %s",
                exc,
            )
