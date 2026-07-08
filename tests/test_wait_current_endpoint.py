"""
Tests for /api/analytics/wait-current endpoint include_held parameter.

Tests the endpoint logic directly against an in-memory SQLite database,
using the same ORM models and query structure as the production endpoint.
No HTTP server is started (avoids httpx/TestClient dependency).
"""

import pytest
from datetime import datetime, timedelta, timezone

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


def _count_wait_dist(session, include_held: bool = False) -> dict[str, int]:
    """
    Replicate the /api/analytics/wait-current _fetch() logic.

    Returns a dict mapping bin-label → count, mirroring the production
    endpoint.  Keeping this local avoids importing server.py (which has
    side-effects like creating a FastAPI app).
    """
    # KEEP IN SYNC with BINS in server.py api_analytics_wait_current()
    BINS = [
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
    states = [JobState.QUEUED]
    if include_held:
        states.append(JobState.HELD)
    jobs = session.query(Job).filter(
        Job.state.in_(states),
        Job.submit_time.isnot(None),
    ).all()

    counts: dict[str, int] = {label: 0 for label, _, _ in BINS}
    for job in jobs:
        st = job.submit_time
        if st is None:
            continue
        if st.tzinfo is not None:
            st = st.replace(tzinfo=None)
        wait_h = (now - st).total_seconds() / 3600
        for label, lo, hi in BINS:
            if lo <= wait_h < hi:
                counts[label] += 1
                break
    return counts


# ── tests ─────────────────────────────────────────────────────────────────────

class TestWaitCurrentIncludeHeld:
    """Verify include_held behaviour of /api/analytics/wait-current."""

    def test_held_excluded_by_default(self, db_session):
        """Held jobs must NOT appear in the distribution when include_held is False."""
        # One queued job waiting ~2 hours → lands in '1-6hr'
        db_session.add(Job(**_make_job("q1", JobState.QUEUED, submit_hours_ago=2.0)))
        # One held job waiting ~2 hours → should be invisible by default
        db_session.add(Job(**_make_job("h1", JobState.HELD,   submit_hours_ago=2.0)))
        db_session.commit()

        counts = _count_wait_dist(db_session, include_held=False)

        assert counts['1-6hr'] == 1, (
            "Only the QUEUED job should appear; held job must be excluded"
        )
        assert sum(counts.values()) == 1, "Total must be 1 (queued only)"

    def test_held_included_with_flag(self, db_session):
        """Held jobs MUST appear in the distribution when include_held is True."""
        db_session.add(Job(**_make_job("q2", JobState.QUEUED, submit_hours_ago=2.0)))
        db_session.add(Job(**_make_job("h2", JobState.HELD,   submit_hours_ago=2.0)))
        db_session.commit()

        counts = _count_wait_dist(db_session, include_held=True)

        assert counts['1-6hr'] == 2, (
            "Both the QUEUED and HELD job must appear in the same bin"
        )
        assert sum(counts.values()) == 2, "Total must be 2 (queued + held)"

    def test_running_jobs_never_counted(self, db_session):
        """Running jobs must never appear regardless of include_held."""
        db_session.add(Job(**_make_job("r1", JobState.RUNNING, submit_hours_ago=2.0)))
        db_session.commit()

        for flag in (False, True):
            counts = _count_wait_dist(db_session, include_held=flag)
            assert sum(counts.values()) == 0, (
                f"Running job must not appear in wait dist (include_held={flag})"
            )

    def test_bin_edge_less_than_1hr(self, db_session):
        """A job queued 30 min ago lands in '<1hr'."""
        db_session.add(Job(**_make_job("q3", JobState.QUEUED, submit_hours_ago=0.5)))
        db_session.commit()

        counts = _count_wait_dist(db_session, include_held=False)
        assert counts['<1hr'] == 1

    def test_bin_edge_greater_than_1mo(self, db_session):
        """A job queued 900 hours (~37.5 days) ago lands in '>1mo'."""
        db_session.add(Job(**_make_job("q4", JobState.QUEUED, submit_hours_ago=900.0)))
        db_session.commit()

        counts = _count_wait_dist(db_session, include_held=False)
        assert counts['>1mo'] == 1

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

        counts = _count_wait_dist(db_session, include_held=False)
        assert sum(counts.values()) == 0

    def test_multiple_bins_multiple_jobs(self, db_session):
        """Multiple jobs spread across bins are counted correctly."""
        jobs = [
            _make_job("m1", JobState.QUEUED, submit_hours_ago=0.5),   # <1hr
            _make_job("m2", JobState.QUEUED, submit_hours_ago=3.0),   # 1-6hr
            _make_job("m3", JobState.QUEUED, submit_hours_ago=3.5),   # 1-6hr
            _make_job("m4", JobState.HELD,   submit_hours_ago=25.0),  # 1-2d (held)
        ]
        for j in jobs:
            db_session.add(Job(**j))
        db_session.commit()

        # Without held: 3 queued across two bins
        counts_no_held = _count_wait_dist(db_session, include_held=False)
        assert counts_no_held['<1hr']   == 1
        assert counts_no_held['1-6hr']  == 2
        assert counts_no_held['1-2d']   == 0
        assert sum(counts_no_held.values()) == 3

        # With held: 4 total, 1-2d gains 1
        counts_with_held = _count_wait_dist(db_session, include_held=True)
        assert counts_with_held['<1hr']  == 1
        assert counts_with_held['1-6hr'] == 2
        assert counts_with_held['1-2d']  == 1
        assert sum(counts_with_held.values()) == 4
