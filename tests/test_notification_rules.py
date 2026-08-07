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
            "job_id TEXT, owner TEXT, job_name TEXT, outcome_class TEXT, "
            "exit_status INT, end_time TEXT)"
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


def _job(db, job_id, owner, job_name, outcome_class, exit_status, end_time):
    db.execute(
        text("INSERT INTO jobs VALUES(:j,:o,:n,:c,:e,:t)"),
        {"j": job_id, "o": owner, "n": job_name, "c": outcome_class,
         "e": exit_status, "t": end_time},
    )


NOW = datetime(2026, 6, 4, 21, 10)


# ---- _short_job_id helper -------------------------------------------------- #

def test_short_job_id_strips_host_suffix():
    assert rules._short_job_id(
        "7257550.polaris-pbs-01.hsn.cm.polaris.alcf.anl.gov") == "7257550"
    # No dot -> unchanged.
    assert rules._short_job_id("12345") == "12345"
    # Non-string / None handled gracefully.
    assert rules._short_job_id(None) == "?"
    assert rules._short_job_id(98765) == "98765"


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
    # 5 identical REAL failures (error / exit 1) -> fires and reports the exit code.
    for i in range(5):
        _job(db, f"job{i}", "kaiyuyue", "gpt_synth", "error", 1,
             "2026-06-04 1%d:00:00" % (i + 3))
    db.commit()
    cfg = SlackConfig(enabled=True, cluster_label="Aurora",
                      rules={"repeated_crash": {"enabled": True, "min_repeats": 3,
                                                "window_hours": 24}})
    msg = rules.repeated_crash(db, cfg, NOW)
    assert msg is not None
    assert "kaiyuyue" in msg.text
    assert "gpt_synth" in msg.text
    # The exit code and its class must be in the message.
    assert "exit 1 x5" in msg.text
    assert "error x5" in msg.text
    # Sample job ids are surfaced (bare ids here have no host suffix).
    assert "ids:" in msg.text
    assert "job0" in msg.text or "job4" in msg.text


def test_repeated_crash_excludes_walltime_by_default(db):
    # 5 walltime kills (-29) with NO other failure -> does NOT fire by default,
    # because many users deliberately code to the wall (checkpoint/restart).
    for i in range(5):
        _job(db, f"job{i}", "codes_to_wall", "long_run", "walltime_killed", -29,
             "2026-06-04 1%d:00:00" % (i + 3))
    db.commit()
    cfg = SlackConfig(enabled=True, cluster_label="Aurora",
                      rules={"repeated_crash": {"enabled": True, "min_repeats": 3,
                                                "window_hours": 24}})
    assert rules.repeated_crash(db, cfg, NOW) is None


def test_repeated_crash_can_include_walltime_via_config(db):
    # With exclude_outcome_classes=[] walltime kills DO count again (opt-in).
    for i in range(5):
        _job(db, f"job{i}", "codes_to_wall", "long_run", "walltime_killed", -29,
             "2026-06-04 1%d:00:00" % (i + 3))
    db.commit()
    cfg = SlackConfig(enabled=True, cluster_label="Aurora",
                      rules={"repeated_crash": {"enabled": True, "min_repeats": 3,
                                                "window_hours": 24,
                                                "exclude_outcome_classes": []}})
    msg = rules.repeated_crash(db, cfg, NOW)
    assert msg is not None
    assert "long_run" in msg.text
    assert "-29 (walltime exceeded) x5" in msg.text
    assert "walltime_killed x5" in msg.text


def test_repeated_crash_walltime_does_not_count_toward_threshold(db):
    # A job with 4 walltime kills + only 2 real errors: the 4 walltime kills are
    # excluded, leaving 2 errors -> BELOW the min_repeats=3 threshold -> no fire.
    for i in range(4):
        _job(db, f"w{i}", "mixed", "solver", "walltime_killed", -29,
             "2026-06-04 1%d:00:00" % (i + 3))
    for i in range(2):
        _job(db, f"e{i}", "mixed", "solver", "error", 1,
             "2026-06-04 1%d:30:00" % (i + 3))
    db.commit()
    cfg = SlackConfig(enabled=True, cluster_label="Aurora",
                      rules={"repeated_crash": {"enabled": True, "min_repeats": 3,
                                                "window_hours": 24}})
    assert rules.repeated_crash(db, cfg, NOW) is None


def test_repeated_crash_reports_only_non_excluded_codes(db):
    # 3 errors + 2 walltime kills for the same job. Errors (3) meet the threshold
    # and fire; the message reports ONLY the non-excluded (error) codes, not the
    # walltime kills (which would reintroduce the noise we deliberately dropped).
    for i in range(3):
        _job(db, f"e{i}", "alice", "train", "error", 1,
             "2026-06-04 1%d:00:00" % (i + 3))
    for i in range(2):
        _job(db, f"w{i}", "alice", "train", "walltime_killed", -29,
             "2026-06-04 1%d:30:00" % (i + 3))
    db.commit()
    cfg = SlackConfig(enabled=True, cluster_label="Aurora",
                      rules={"repeated_crash": {"enabled": True, "min_repeats": 3,
                                                "window_hours": 24}})
    msg = rules.repeated_crash(db, cfg, NOW)
    assert msg is not None
    assert "alice" in msg.text and "train" in msg.text
    assert "error x3" in msg.text
    assert "exit 1 x3" in msg.text
    # Walltime kills are excluded from the trigger AND the message.
    assert "walltime_killed" not in msg.text
    assert "-29" not in msg.text


def test_repeated_crash_below_threshold_does_not_fire(db):
    for i in range(2):  # only 2 failures, threshold is 3
        _job(db, f"job{i}", "someone", "run", "error", 1,
             "2026-06-04 1%d:00:00" % (i + 3))
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


# ---- repeated_rerun_held: single-job PBS auto-requeue loop ------------------ #
#
# This rule needs run_count + state columns the shared `db` fixture lacks, so it
# uses its own fixture/table.

@pytest.fixture
def rerun_db():
    eng = create_engine("sqlite:///:memory:")
    with eng.begin() as cx:
        cx.execute(text(
            "CREATE TABLE jobs("
            "job_id TEXT, owner TEXT, job_name TEXT, state TEXT, "
            "run_count INT, outcome_class TEXT, exit_status INT, end_time TEXT)"
        ))
    session = sessionmaker(bind=eng)()
    yield session
    session.close()


def _rjob(db, job_id, owner, job_name, state, run_count, outcome_class,
          exit_status, end_time):
    db.execute(
        text("INSERT INTO jobs VALUES(:j,:o,:n,:s,:rc,:c,:e,:t)"),
        {"j": job_id, "o": owner, "n": job_name, "s": state, "rc": run_count,
         "c": outcome_class, "e": exit_status, "t": end_time},
    )


def test_repeated_rerun_held_fires_on_held_after_retries(rerun_db):
    # The classic case: one job PBS requeued 21x then held (could_not_run/-3).
    _rjob(rerun_db, "7257550.polaris-pbs-01.hsn.cm.polaris.alcf.anl.gov",
          "vaseline555", "gpt2m", "FINISHED", 21,
          "could_not_run", -3, "2026-06-04 19:21:15")
    rerun_db.commit()
    cfg = SlackConfig(enabled=True, cluster_label="Polaris",
                      rules={"repeated_rerun_held":
                             {"enabled": True, "min_run_count": 5,
                              "window_hours": 24}})
    msg = rules.repeated_rerun_held(rerun_db, cfg, NOW)
    assert msg is not None
    assert "vaseline555" in msg.text and "gpt2m" in msg.text
    assert "21x" in msg.text
    assert "Polaris" in msg.text
    # The job id is shown, shortened to the numeric prefix (host suffix dropped).
    assert "7257550" in msg.text
    assert "polaris-pbs-01" not in msg.text


def test_repeated_rerun_held_ignores_low_run_count(rerun_db):
    # run_count below threshold -> no alert, even though it failed.
    _rjob(rerun_db, "j1", "bob", "run", "FINISHED", 3, "error", 1,
          "2026-06-04 20:00:00")
    rerun_db.commit()
    cfg = SlackConfig(enabled=True, cluster_label="Aurora",
                      rules={"repeated_rerun_held":
                             {"enabled": True, "min_run_count": 5,
                              "window_hours": 24}})
    assert rules.repeated_rerun_held(rerun_db, cfg, NOW) is None


def test_repeated_rerun_held_ignores_successful_requeue_by_default(rerun_db):
    # Requeued many times but ultimately SUCCEEDED (preemption, not a stuck loop)
    # -> excluded by default (require_failure=True).
    _rjob(rerun_db, "j2", "carol", "run", "FINISHED", 12, "success", 0,
          "2026-06-04 20:00:00")
    rerun_db.commit()
    cfg = SlackConfig(enabled=True, cluster_label="Aurora",
                      rules={"repeated_rerun_held":
                             {"enabled": True, "min_run_count": 5,
                              "window_hours": 24}})
    assert rules.repeated_rerun_held(rerun_db, cfg, NOW) is None
    # ...but with require_failure=False it DOES fire.
    cfg2 = SlackConfig(enabled=True, cluster_label="Aurora",
                       rules={"repeated_rerun_held":
                              {"enabled": True, "min_run_count": 5,
                               "window_hours": 24, "require_failure": False}})
    assert rules.repeated_rerun_held(rerun_db, cfg2, NOW) is not None


def test_repeated_rerun_held_dedupes_duplicate_rows(rerun_db):
    # Same logical job present as several rows -> listed once, counted once.
    for i in range(4):
        _rjob(rerun_db, f"dup{i}", "dave", "same_job", "FINISHED", 21,
              "could_not_run", -3, "2026-06-04 19:00:00")
    rerun_db.commit()
    cfg = SlackConfig(enabled=True, cluster_label="Aurora",
                      rules={"repeated_rerun_held":
                             {"enabled": True, "min_run_count": 5,
                              "window_hours": 24, "max_report": 5}})
    msg = rules.repeated_rerun_held(rerun_db, cfg, NOW)
    assert msg is not None
    # Only one distinct offender -> "1 job(s)" and a single bullet line.
    assert "1 job(s)" in msg.text
    assert msg.text.count("`dave`") == 1


def test_repeated_rerun_held_outside_window_does_not_fire(rerun_db):
    # Failed 21x but ended long before the window -> no alert.
    _rjob(rerun_db, "old", "erin", "run", "FINISHED", 21, "could_not_run", -3,
          "2026-05-01 00:00:00")
    rerun_db.commit()
    cfg = SlackConfig(enabled=True, cluster_label="Aurora",
                      rules={"repeated_rerun_held":
                             {"enabled": True, "min_run_count": 5,
                              "window_hours": 24}})
    assert rules.repeated_rerun_held(rerun_db, cfg, NOW) is None


# ---- repeated_rerun_held: walltime exclusion ------------------------------- #

def test_repeated_rerun_held_excludes_walltime_by_default(rerun_db):
    # A job requeued 5x that ended in a walltime kill (-29) is NOT a
    # "requeued to death" loop -- excluded by default, exactly like repeated_crash.
    # This is the Aurora `jdtun Up_30.r3` false-positive from the alert dump.
    _rjob(rerun_db, "8724481", "jdtun", "Up_30.r3", "FINISHED", 5,
          "walltime_killed", -29, "2026-06-04 19:00:00")
    rerun_db.commit()
    cfg = SlackConfig(enabled=True, cluster_label="Aurora",
                      rules={"repeated_rerun_held":
                             {"enabled": True, "min_run_count": 5,
                              "window_hours": 24}})
    assert rules.repeated_rerun_held(rerun_db, cfg, NOW) is None


def test_repeated_rerun_held_can_include_walltime_via_config(rerun_db):
    # Opt back in with exclude_outcome_classes=[] -> the walltime kill counts.
    _rjob(rerun_db, "8724481", "jdtun", "Up_30.r3", "FINISHED", 5,
          "walltime_killed", -29, "2026-06-04 19:00:00")
    rerun_db.commit()
    cfg = SlackConfig(enabled=True, cluster_label="Aurora",
                      rules={"repeated_rerun_held":
                             {"enabled": True, "min_run_count": 5,
                              "window_hours": 24,
                              "exclude_outcome_classes": []}})
    msg = rules.repeated_rerun_held(rerun_db, cfg, NOW)
    assert msg is not None
    assert "jdtun" in msg.text and "Up_30.r3" in msg.text


def test_repeated_rerun_held_walltime_excluded_but_held_still_fires(rerun_db):
    # Mixed batch: one walltime kill (excluded) + one genuine could_not_run loop
    # (kept). Only the real offender should appear.
    _rjob(rerun_db, "8724481", "jdtun", "Up_30.r3", "FINISHED", 5,
          "walltime_killed", -29, "2026-06-04 19:00:00")
    _rjob(rerun_db, "7366765", "shudson", "qaoa_bench_pol", "FINISHED", 21,
          "could_not_run", -3, "2026-06-04 19:05:00")
    rerun_db.commit()
    cfg = SlackConfig(enabled=True, cluster_label="Polaris",
                      rules={"repeated_rerun_held":
                             {"enabled": True, "min_run_count": 5,
                              "window_hours": 24}})
    msg = rules.repeated_rerun_held(rerun_db, cfg, NOW)
    assert msg is not None
    assert "1 job(s)" in msg.text
    assert "shudson" in msg.text and "qaoa_bench_pol" in msg.text
    assert "jdtun" not in msg.text


# ---- content signatures (anti-spam dedupe) --------------------------------- #

def test_repeated_rerun_held_attaches_signature(rerun_db):
    _rjob(rerun_db, "7366765", "shudson", "qaoa_bench_pol", "FINISHED", 21,
          "could_not_run", -3, "2026-06-04 19:00:00")
    rerun_db.commit()
    cfg = SlackConfig(enabled=True, cluster_label="Polaris",
                      rules={"repeated_rerun_held":
                             {"enabled": True, "min_run_count": 5,
                              "window_hours": 24}})
    msg = rules.repeated_rerun_held(rerun_db, cfg, NOW)
    assert msg is not None
    assert msg.signature is not None
    assert msg.signature.startswith("repeated_rerun_held:")


def test_repeated_rerun_held_signature_stable_and_offender_dependent(rerun_db):
    # Same offender set -> same signature (independent of row insertion order).
    # A new offender -> different signature (the engine will then re-post).
    _rjob(rerun_db, "7365090", "purnavindhyak", "flops_metrics", "FINISHED", 21,
          "could_not_run", -3, "2026-06-04 19:00:00")
    _rjob(rerun_db, "7366765", "shudson", "qaoa_bench_pol", "FINISHED", 21,
          "could_not_run", -3, "2026-06-04 19:01:00")
    rerun_db.commit()
    cfg = SlackConfig(enabled=True, cluster_label="Polaris",
                      rules={"repeated_rerun_held":
                             {"enabled": True, "min_run_count": 5,
                              "window_hours": 24}})
    msg1 = rules.repeated_rerun_held(rerun_db, cfg, NOW)
    assert msg1 is not None
    sig1 = msg1.signature

    # Re-evaluating the SAME data yields the SAME signature (stable across runs).
    msg1b = rules.repeated_rerun_held(rerun_db, cfg, NOW)
    assert msg1b is not None
    assert sig1 == msg1b.signature

    # A genuinely NEW stuck job appears -> signature must change.
    _rjob(rerun_db, "7365080", "purnavindhyak", "flops_metrics", "FINISHED", 15,
          "could_not_run", -3, "2026-06-04 19:02:00")
    rerun_db.commit()
    msg2 = rules.repeated_rerun_held(rerun_db, cfg, NOW)
    assert msg2 is not None
    assert msg2.signature != sig1


def test_repeated_crash_attaches_signature(db):
    for i in range(5):
        _job(db, f"job{i}", "kaiyuyue", "gpt_synth", "error", 1,
             "2026-06-04 1%d:00:00" % (i + 3))
    db.commit()
    cfg = SlackConfig(enabled=True, cluster_label="Aurora",
                      rules={"repeated_crash": {"enabled": True, "min_repeats": 3,
                                                "window_hours": 24}})
    msg = rules.repeated_crash(db, cfg, NOW)
    assert msg is not None
    assert msg.signature is not None
    assert msg.signature.startswith("repeated_crash:")
