"""
Tests for /api/analytics/wait-current endpoint.

Covers the per-state breakdown introduced in the stacked-bar refactor:
  - ``queued_counts`` and ``held_counts`` arrays are always present
  - ``held_counts`` are all-zero when include_held is False
  - ``held_counts`` are populated when include_held is True
  - ``counts`` equals the element-wise sum of queued_counts + held_counts

Tests the endpoint logic directly against an in-memory SQLite database,
using the same ORM models and query structure as the production endpoint.
No HTTP server is started (avoids httpx/TestClient dependency).
"""

import pytest
from datetime import datetime, timedelta, timezone
from typing import Dict, List

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from pbs_monitor.database.models import Job, JobState, Base


# ── fixtures ──────────────────────────────────────────────────────────────────

@pytest.fixture(scope="function")
def db_session():
    """Create a fresh in-memory SQLite session for each test."""
    engine = create_engine("sqlite:///:memory:", echo=False)
    # Create all ORM-mapped tables directly from the declarative base.
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)
    session = Session()
    yield session
    session.close()
    engine.dispose()


def _make_job(job_id: str, state: JobState, submit_hours_ago: float) -> dict:
    """Return a minimal Job kwargs dict with a submit_time set hours ago."""
    submit_time = datetime.now(timezone.utc).replace(tzinfo=None) - timedelta(hours=submit_hours_ago)
    return dict(
        job_id=job_id,
        job_name=f"job_{job_id}",
        owner="testuser",
        state=state,
        queue="default",
        submit_time=submit_time,
    )


def _count_wait_dist(session, include_held: bool = False) -> Dict[str, Dict[str, int]]:
    """
    Replicate the /api/analytics/wait-current _fetch() logic.

    Returns a dict with keys:
        'queued_counts'  — dict mapping bin-label → queued job count
        'held_counts'    — dict mapping bin-label → held job count (all 0 when
                          include_held is False)
        'counts'         — dict mapping bin-label → queued + held (element-wise sum)

    Keeping this local avoids importing server.py (which has side-effects
    like creating a FastAPI app).
    """
    # KEEP IN SYNC with BINS in server.py api_analytics_wait_current()
    BINS: List[tuple] = [
        ('<1hr',    0,    1),
        ('1-6hr',   1,    6),
        ('6-12hr',  6,   12),
        ('12-24hr', 12,  24),
        ('1-2d',    24,  48),
        ('2-7d',    48, 168),
        ('7-14d',  168, 336),
        ('2-3wk',  336, 504),
        ('3-5wk',  504, 840),
        ('>1mo',   840, float('inf')),
    ]
    now = datetime.now(timezone.utc).replace(tzinfo=None)

    def _bin_index(wait_h: float) -> int:
        for i, (_, lo, hi) in enumerate(BINS):
            if lo <= wait_h < hi:
                return i
        return -1

    bin_labels = [label for label, _, _ in BINS]
    queued_counts: Dict[str, int] = {label: 0 for label in bin_labels}
    held_counts: Dict[str, int]   = {label: 0 for label in bin_labels}

    # Always count QUEUED jobs.
    for job in session.query(Job).filter(
        Job.state == JobState.QUEUED,
        Job.submit_time.isnot(None),
    ).all():
        st = job.submit_time
        if st is None:
            continue
        if st.tzinfo is not None:
            st = st.replace(tzinfo=None)
        idx = _bin_index((now - st).total_seconds() / 3600)
        if idx >= 0:
            queued_counts[bin_labels[idx]] += 1

    # Only count HELD jobs when explicitly requested.
    if include_held:
        for job in session.query(Job).filter(
            Job.state == JobState.HELD,
            Job.submit_time.isnot(None),
        ).all():
            st = job.submit_time
            if st is None:
                continue
            if st.tzinfo is not None:
                st = st.replace(tzinfo=None)
            idx = _bin_index((now - st).total_seconds() / 3600)
            if idx >= 0:
                held_counts[bin_labels[idx]] += 1

    counts = {label: queued_counts[label] + held_counts[label] for label in bin_labels}
    return {"queued_counts": queued_counts, "held_counts": held_counts, "counts": counts}


# ── tests ─────────────────────────────────────────────────────────────────────

class TestWaitCurrentResponseShape:
    """Verify that the response always contains all three count arrays."""

    def test_response_keys_present_no_held(self, db_session):
        """queued_counts, held_counts, and counts are all present when include_held=False."""
        db_session.add(Job(**_make_job("q0", JobState.QUEUED, submit_hours_ago=2.0)))
        db_session.commit()

        result = _count_wait_dist(db_session, include_held=False)
        assert "queued_counts" in result
        assert "held_counts" in result
        assert "counts" in result

    def test_response_keys_present_with_held(self, db_session):
        """queued_counts, held_counts, and counts are all present when include_held=True."""
        db_session.add(Job(**_make_job("q0b", JobState.QUEUED, submit_hours_ago=2.0)))
        db_session.commit()

        result = _count_wait_dist(db_session, include_held=True)
        assert "queued_counts" in result
        assert "held_counts" in result
        assert "counts" in result

    def test_counts_equals_sum(self, db_session):
        """counts[bin] == queued_counts[bin] + held_counts[bin] for every bin."""
        db_session.add(Job(**_make_job("qs1", JobState.QUEUED, submit_hours_ago=2.0)))
        db_session.add(Job(**_make_job("hs1", JobState.HELD,   submit_hours_ago=2.0)))
        db_session.commit()

        result = _count_wait_dist(db_session, include_held=True)
        for label in result["queued_counts"]:
            expected = result["queued_counts"][label] + result["held_counts"][label]
            assert result["counts"][label] == expected, (
                f"counts[{label!r}] should equal queued+held but got "
                f"{result['counts'][label]} != {expected}"
            )

    def test_held_counts_all_zero_when_flag_false(self, db_session):
        """held_counts must be all-zero when include_held is False."""
        db_session.add(Job(**_make_job("hz1", JobState.HELD, submit_hours_ago=2.0)))
        db_session.commit()

        result = _count_wait_dist(db_session, include_held=False)
        assert all(v == 0 for v in result["held_counts"].values()), (
            "held_counts must be all-zero when include_held=False"
        )

    def test_held_counts_populated_when_flag_true(self, db_session):
        """held_counts must reflect HELD jobs when include_held is True."""
        db_session.add(Job(**_make_job("hp1", JobState.HELD, submit_hours_ago=2.0)))
        db_session.commit()

        result = _count_wait_dist(db_session, include_held=True)
        assert result["held_counts"]["1-6hr"] == 1
        assert sum(result["held_counts"].values()) == 1


class TestWaitCurrentIncludeHeld:
    """Verify include_held behaviour of /api/analytics/wait-current."""

    def test_held_excluded_by_default(self, db_session):
        """Held jobs must NOT appear in any count when include_held is False."""
        # One queued job waiting ~2 hours → lands in '1-6hr'
        db_session.add(Job(**_make_job("q1", JobState.QUEUED, submit_hours_ago=2.0)))
        # One held job waiting ~2 hours → should be invisible by default
        db_session.add(Job(**_make_job("h1", JobState.HELD,   submit_hours_ago=2.0)))
        db_session.commit()

        result = _count_wait_dist(db_session, include_held=False)

        assert result["queued_counts"]["1-6hr"] == 1, (
            "Only the QUEUED job should appear in queued_counts; held must be excluded"
        )
        assert sum(result["queued_counts"].values()) == 1, "Total queued must be 1"
        assert all(v == 0 for v in result["held_counts"].values()), (
            "held_counts must be all-zero when include_held=False"
        )
        assert result["counts"]["1-6hr"] == 1, "counts must equal queued_counts when no held"

    def test_held_included_with_flag(self, db_session):
        """Held jobs MUST appear in held_counts and counts when include_held is True."""
        db_session.add(Job(**_make_job("q2", JobState.QUEUED, submit_hours_ago=2.0)))
        db_session.add(Job(**_make_job("h2", JobState.HELD,   submit_hours_ago=2.0)))
        db_session.commit()

        result = _count_wait_dist(db_session, include_held=True)

        assert result["queued_counts"]["1-6hr"] == 1, "Queued job must appear in queued_counts"
        assert result["held_counts"]["1-6hr"] == 1,   "Held job must appear in held_counts"
        assert result["counts"]["1-6hr"] == 2,        "counts must be the sum (2 total)"
        assert sum(result["counts"].values()) == 2,   "Total must be 2 (queued + held)"

    def test_queued_and_held_in_separate_arrays(self, db_session):
        """Queued and held counts land in separate arrays, not blended."""
        # queued at 2h → '1-6hr', held at 25h → '1-2d'
        db_session.add(Job(**_make_job("sep_q", JobState.QUEUED, submit_hours_ago=2.0)))
        db_session.add(Job(**_make_job("sep_h", JobState.HELD,   submit_hours_ago=25.0)))
        db_session.commit()

        result = _count_wait_dist(db_session, include_held=True)

        assert result["queued_counts"]["1-6hr"] == 1
        assert result["queued_counts"]["1-2d"]  == 0
        assert result["held_counts"]["1-2d"]    == 1
        assert result["held_counts"]["1-6hr"]   == 0
        assert result["counts"]["1-6hr"]        == 1
        assert result["counts"]["1-2d"]         == 1

    def test_running_jobs_never_counted(self, db_session):
        """Running jobs must never appear regardless of include_held."""
        db_session.add(Job(**_make_job("r1", JobState.RUNNING, submit_hours_ago=2.0)))
        db_session.commit()

        for flag in (False, True):
            result = _count_wait_dist(db_session, include_held=flag)
            assert sum(result["counts"].values()) == 0, (
                f"Running job must not appear in wait dist (include_held={flag})"
            )

    def test_bin_edge_less_than_1hr(self, db_session):
        """A job queued 30 min ago lands in '<1hr'."""
        db_session.add(Job(**_make_job("q3", JobState.QUEUED, submit_hours_ago=0.5)))
        db_session.commit()

        result = _count_wait_dist(db_session, include_held=False)
        assert result["queued_counts"]["<1hr"] == 1
        assert result["counts"]["<1hr"] == 1

    def test_bin_edge_greater_than_1mo(self, db_session):
        """A job queued 900 hours (~37.5 days) ago lands in '>1mo'."""
        db_session.add(Job(**_make_job("q4", JobState.QUEUED, submit_hours_ago=900.0)))
        db_session.commit()

        result = _count_wait_dist(db_session, include_held=False)
        assert result["queued_counts"][">1mo"] == 1
        assert result["counts"][">1mo"] == 1

    def test_job_without_submit_time_skipped(self, db_session):
        """Jobs with NULL submit_time are silently excluded from the distribution."""
        job = Job(
            job_id="q5",
            job_name="no_submit",
            owner="testuser",
            state=JobState.QUEUED,
            queue="default",
            submit_time=None,
        )
        db_session.add(job)
        db_session.commit()

        result = _count_wait_dist(db_session, include_held=False)
        assert sum(result["counts"].values()) == 0

    def test_multiple_bins_multiple_jobs(self, db_session):
        """Multiple jobs spread across bins are counted correctly per state."""
        jobs = [
            _make_job("m1", JobState.QUEUED, submit_hours_ago=0.5),   # <1hr
            _make_job("m2", JobState.QUEUED, submit_hours_ago=3.0),   # 1-6hr
            _make_job("m3", JobState.QUEUED, submit_hours_ago=3.5),   # 1-6hr
            _make_job("m4", JobState.HELD,   submit_hours_ago=25.0),  # 1-2d (held)
        ]
        for j in jobs:
            db_session.add(Job(**j))
        db_session.commit()

        # Without held: 3 queued across two bins; held_counts all zero
        result_no_held = _count_wait_dist(db_session, include_held=False)
        assert result_no_held["queued_counts"]["<1hr"]  == 1
        assert result_no_held["queued_counts"]["1-6hr"] == 2
        assert result_no_held["queued_counts"]["1-2d"]  == 0
        assert all(v == 0 for v in result_no_held["held_counts"].values())
        assert sum(result_no_held["counts"].values()) == 3

        # With held: 4 total; held lands in its own array in '1-2d'
        result_with_held = _count_wait_dist(db_session, include_held=True)
        assert result_with_held["queued_counts"]["<1hr"]  == 1
        assert result_with_held["queued_counts"]["1-6hr"] == 2
        assert result_with_held["queued_counts"]["1-2d"]  == 0
        assert result_with_held["held_counts"]["1-2d"]    == 1
        assert result_with_held["held_counts"]["1-6hr"]   == 0
        assert result_with_held["counts"]["1-2d"]         == 1
        assert sum(result_with_held["counts"].values())   == 4
