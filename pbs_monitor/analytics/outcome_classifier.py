"""
Job Outcome Classifier for PBS Monitor

Single source of truth for mapping a job's PBS exit_status (and optional
walltime information) to a human-readable outcome class.

Outcome classes (verbatim from ANALYTICS_REORG_PLAN.md §6):
    success         — exit_status == 0
    signal_killed   — 128 < exit_status < 192 (killed by Unix signal)
    walltime_killed — exit_status == 143 AND actual_runtime >= 0.95 * requested_walltime
    requeued        — exit_status == 271 (256+15, provisional: preemption/maintenance)
    could_not_run   — exit_status < 0 (PBS special: -29, -20, -3, …)
    error           — any other non-zero integer exit status
    unknown         — exit_status is None / not recorded

NOTE: active jobs (state in Q/R/H/W/T/E/S) are NOT passed to this function;
the caller is responsible for filtering them out before calling classify_exit.
Negative codes and 271 are mapped provisionally — confirm semantics with
ALCF/Aurora PBS admins before surfacing in user-facing labels (plan §9).
"""

from typing import Optional

# Stable set of valid outcome class strings — used as a type alias for documentation.
OUTCOME_CLASSES = frozenset({
    "success",
    "signal_killed",
    "walltime_killed",
    "requeued",
    "could_not_run",
    "error",
    "unknown",
})


def classify_exit(
    state: str,
    exit_status: Optional[int],
    actual_runtime_seconds: Optional[int] = None,
    requested_walltime_seconds: Optional[int] = None,
) -> str:
    """Classify a finished PBS job into one of the canonical outcome classes.

    The classification rules are applied in priority order (verbatim from
    ANALYTICS_REORG_PLAN.md §6):

        state != FINISHED and state in (Q,R,H)  -> not counted (still active)
        exit_status == 0                         -> success
        exit_status is None                      -> unknown
        exit_status == 271 (256+15)              -> requeued
        exit_status < 0                          -> could_not_run
        128 < exit_status < 192                  -> signal_killed
        exit_status == 143 AND
            actual_runtime >= 0.95*requested_wt  -> walltime_killed
        otherwise (1,2,127,255,…)               -> error

    Args:
        state: PBS job state string (e.g. "F", "C", "Q", "R").  Active states
               (Q, R, H, W, T, E, S) return ``"unknown"`` — callers should
               filter those out before calling this function.
        exit_status: Integer exit code from PBS ``Exit_status`` attribute, or
                     ``None`` if not yet recorded.
        actual_runtime_seconds: Observed runtime in seconds (end - start).
                                Used only for the walltime-killed refinement.
        requested_walltime_seconds: Requested walltime in seconds parsed from
                                    ``Resource_List.walltime``.  Used only for
                                    the walltime-killed refinement.

    Returns:
        One of the strings in ``OUTCOME_CLASSES``.
    """
    # Active states — caller should filter these, but guard defensively.
    _ACTIVE_STATES = {"Q", "R", "H", "W", "T", "E", "S"}
    if state in _ACTIVE_STATES:
        # Still running — not counted in outcome analysis.
        return "unknown"

    # --- Apply rules in priority order (plan §6) ---

    if exit_status == 0:
        return "success"

    if exit_status is None:
        return "unknown"

    if exit_status == 271:
        return "requeued"

    if exit_status < 0:
        return "could_not_run"

    # 128 < es < 192 is the signal-killed band (Unix signal n = es - 128).
    # The walltime-killed refinement for es==143 (SIGTERM) must be checked
    # *before* the generic signal_killed bucket, so we do it here.
    if exit_status == 143:
        # Refine SIGTERM: if the job ran ≥ 95% of its requested walltime,
        # PBS likely killed it for exceeding the limit.
        if (
            actual_runtime_seconds is not None
            and requested_walltime_seconds is not None
            and requested_walltime_seconds > 0
            and actual_runtime_seconds >= 0.95 * requested_walltime_seconds
        ):
            return "walltime_killed"
        # SIGTERM but not near walltime limit → generic signal kill.
        return "signal_killed"

    if 128 < exit_status < 192:
        return "signal_killed"

    # Everything else: 1, 2, 127, 255, 192+, etc.
    return "error"
