"""Tests for notification rules against a small in-memory SQLite DB.

These verify the rule *logic* (thresholds, routing-queue aggregation) without
touching the real multi-GB DB. Uses raw CREATE/INSERT so no ORM/migration is
needed -- the rules query via raw SQL text() anyway.
"""

from datetime import datetime

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

from pbs_monitor.config import SlackConfig
from pbs_monitor.notifications import rules


@pytest.fixture
def db():
    eng = create_engine("sqlite:///:memory:")
    with eng.begin() as cx:
        cx.execute(text(
            "CREATE TABLE system_snapshots("
            "timestamp TEXT, total_nodes INT, available_nodes INT)"
        ))
        cx.execute(text(
            "CREATE TABLE queue_snapshots("
            "timestamp TEXT, queue_name TEXT, queued_jobs INT, running_jobs INT)"
        ))
        cx.execute(text(
            "CREATE TABLE jobs("
            "job_id TEXT, owner TEXT, job_name TEXT, outcome_class TEXT, end_time TEXT)"
        ))
    session = sessionmaker(bind=eng)()
    yield session
    session.close()


def _sys(db, ts, total, avail):
    db.execute(text("INSERT INTO system_snapshots VALUES(:t,:a,:b)"),
               {"t": ts, "a": total, "b": avail})


def _q(db, ts, name, queued, running):
    db.execute(text("INSERT INTO queue_snapshots VALUES(:t,:n,:q,:r)"),
               {"t": ts, "n": name, "q": queued, "r": running})


NOW = datetime(2026, 6, 4, 21, 10)


# ---- queue_drying: routing-queue aggregation ------------------------------- #

def test_queue_drying_healthy_backlog_does_not_fire(db):
    # 109 queued across exec queues at 94% util -> healthy, no alert.
    _sys(db, "2026-06-04 20:52:00", 10624, 666)
    _q(db, "2026-06-04 20:52:00", "small", 66, 15)
    _q(db, "2026-06-04 20:52:00", "medium", 10, 0)
    _q(db, "2026-06-04 20:52:00", "large", 33, 1)
    db.commit()
    cfg = SlackConfig(enabled=True, cluster_label="Aurora",
                      rules={"queue_drying": {"enabled": True}})
    assert rules.queue_drying(db, cfg, NOW) is None


def test_queue_drying_fires_on_thin_backlog_high_util(db):
    # 10 queued aggregate at 97% util -> drying up, alert.
    _sys(db, "2026-05-26 19:39:00", 10624, 300)
    _q(db, "2026-05-26 19:39:00", "small", 4, 20)
    _q(db, "2026-05-26 19:39:00", "medium", 3, 5)
    _q(db, "2026-05-26 19:39:00", "large", 3, 2)
    db.commit()
    cfg = SlackConfig(enabled=True, cluster_label="Aurora",
                      rules={"queue_drying": {"enabled": True}})
    msg = rules.queue_drying(db, cfg, NOW)
    assert msg is not None
    assert "prod backlog drying up" in msg.text
    assert "10 queued" in msg.text


def test_queue_drying_thin_backlog_but_low_util_does_not_fire(db):
    # Thin backlog but machine not busy -> not actionable, no alert.
    _sys(db, "2026-05-26 19:39:00", 10624, 8000)  # ~25% util
    _q(db, "2026-05-26 19:39:00", "small", 2, 1)
    _q(db, "2026-05-26 19:39:00", "medium", 1, 0)
    _q(db, "2026-05-26 19:39:00", "large", 1, 0)
    db.commit()
    cfg = SlackConfig(enabled=True, cluster_label="Aurora",
                      rules={"queue_drying": {"enabled": True}})
    assert rules.queue_drying(db, cfg, NOW) is None


def test_queue_drying_ignores_routing_queue_own_counts(db):
    # Even if 'prod' itself shows 1 queued, we aggregate exec queues, which are
    # healthy -> no false alarm.
    _sys(db, "2026-06-04 20:52:00", 10624, 666)
    _q(db, "2026-06-04 20:52:00", "prod", 1, 0)      # routing queue, ignored
    _q(db, "2026-06-04 20:52:00", "small", 66, 15)
    _q(db, "2026-06-04 20:52:00", "medium", 10, 0)
    _q(db, "2026-06-04 20:52:00", "large", 33, 1)
    db.commit()
    cfg = SlackConfig(enabled=True, cluster_label="Aurora",
                      rules={"queue_drying": {"enabled": True}})
    assert rules.queue_drying(db, cfg, NOW) is None


# ---- repeated_crash -------------------------------------------------------- #

def test_repeated_crash_fires_on_looping_user(db):
    for i in range(5):
        db.execute(text("INSERT INTO jobs VALUES(:j,:o,:n,:c,:t)"),
                   {"j": f"job{i}", "o": "kaiyuyue", "n": "gpt_synth",
                    "c": "error", "t": "2026-06-04 1%d:00:00" % (i + 3)})
    db.commit()
    cfg = SlackConfig(enabled=True, cluster_label="Aurora",
                      rules={"repeated_crash": {"enabled": True, "min_repeats": 3,
                                                "window_hours": 24}})
    msg = rules.repeated_crash(db, cfg, NOW)
    assert msg is not None
    assert "kaiyuyue" in msg.text
    assert "gpt_synth" in msg.text


def test_repeated_crash_below_threshold_does_not_fire(db):
    for i in range(2):  # only 2 failures, threshold is 3
        db.execute(text("INSERT INTO jobs VALUES(:j,:o,:n,:c,:t)"),
                   {"j": f"job{i}", "o": "someone", "n": "run",
                    "c": "error", "t": "2026-06-04 1%d:00:00" % (i + 3)})
    db.commit()
    cfg = SlackConfig(enabled=True, cluster_label="Aurora",
                      rules={"repeated_crash": {"enabled": True, "min_repeats": 3,
                                                "window_hours": 24}})
    assert rules.repeated_crash(db, cfg, NOW) is None


def test_disabled_rule_skipped_by_evaluate_all(db):
    _sys(db, "2026-05-26 19:39:00", 10624, 300)
    _q(db, "2026-05-26 19:39:00", "small", 1, 20)
    db.commit()
    cfg = SlackConfig(enabled=True, cluster_label="Aurora",
                      rules={"queue_drying": {"enabled": False}})
    res = rules.evaluate_all(db, cfg, NOW)
    assert res["queue_drying"] is None
