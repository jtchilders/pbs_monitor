"""Tests for the notification engine: edge-trigger, cooldown, global floor, state."""

import contextlib
import json
import os
import tempfile

import pytest

from pbs_monitor.config import SlackConfig
from pbs_monitor.notifications.engine import NotificationEngine, default_state_path
from pbs_monitor.notifications.slack import SlackMessage, SlackNotifier


class FakeClock:
    def __init__(self, t=1000.0):
        self.t = t

    def __call__(self):
        return self.t


class RecordingNotifier(SlackNotifier):
    """Notifier that records posts instead of sending."""
    def __init__(self):
        super().__init__(dry_run=False)
        self.posts = []

    @property
    def is_configured(self):
        return True

    def post(self, message):
        self.posts.append(message.text if isinstance(message, SlackMessage) else message)
        return True


def _session_getter(rule_result):
    """Return a session_getter whose evaluate_all is monkeypatched via the
    engine module; here we just yield a dummy db object."""
    @contextlib.contextmanager
    def getter():
        yield object()
    return getter


@pytest.fixture
def statefile():
    p = tempfile.mktemp(suffix=".json")
    yield p
    if os.path.exists(p):
        os.remove(p)


def _make_engine(monkeypatch, statefile, fired_message, clock, cfg=None):
    """Build an engine whose rule evaluation returns a fixed result set."""
    import pbs_monitor.notifications.engine as eng

    def fake_eval(db, cfg_):
        return {"repeated_crash": fired_message}

    monkeypatch.setattr(eng, "evaluate_all", fake_eval)
    cfg = cfg or SlackConfig(enabled=True, cluster_label="Aurora",
                             rules={"repeated_crash": {"enabled": True,
                                                       "cooldown_seconds": 3600}})
    notifier = RecordingNotifier()
    engine = NotificationEngine(
        slack_config=cfg,
        session_getter=_session_getter(fired_message),
        notifier=notifier,
        state_path=statefile,
        clock=clock,
    )
    return engine, notifier


def test_edge_trigger_posts_once_then_suppresses(monkeypatch, statefile):
    clk = FakeClock()
    msg = SlackMessage(text="stuck user")
    engine, notifier = _make_engine(monkeypatch, statefile, msg, clk)

    engine.run_once()            # cycle 1: fresh edge -> post
    assert len(notifier.posts) == 1

    clk.t += 300                 # +5 min, still fired, within cooldown
    engine.run_once()
    assert len(notifier.posts) == 1  # suppressed


def test_reposts_after_cooldown(monkeypatch, statefile):
    clk = FakeClock()
    msg = SlackMessage(text="stuck user")
    engine, notifier = _make_engine(monkeypatch, statefile, msg, clk)

    engine.run_once()
    clk.t += 7200                # +2h > 3600 cooldown
    engine.run_once()
    assert len(notifier.posts) == 2


def test_clear_rearms_edge(monkeypatch, statefile):
    clk = FakeClock()
    import pbs_monitor.notifications.engine as eng

    state = {"msg": SlackMessage(text="stuck")}

    def fake_eval(db, cfg_):
        return {"repeated_crash": state["msg"]}

    monkeypatch.setattr(eng, "evaluate_all", fake_eval)
    cfg = SlackConfig(enabled=True, rules={"repeated_crash": {"enabled": True,
                                                              "cooldown_seconds": 3600}})
    notifier = RecordingNotifier()
    engine = NotificationEngine(cfg, _session_getter(None), notifier=notifier,
                                state_path=statefile, clock=clk)

    engine.run_once()                 # fires -> post (1)
    assert len(notifier.posts) == 1

    state["msg"] = None               # condition clears
    clk.t += 300
    engine.run_once()                 # cleared -> re-arm, no post
    assert len(notifier.posts) == 1

    state["msg"] = SlackMessage(text="stuck again")
    clk.t += 300                      # re-fires soon after clearing...
    engine.run_once()
    # within cooldown of last post -> still suppressed
    assert len(notifier.posts) == 1


def test_global_floor_blocks_second_rule(monkeypatch, statefile):
    clk = FakeClock()
    import pbs_monitor.notifications.engine as eng

    def fake_eval(db, cfg_):
        return {"rule_a": SlackMessage(text="A"), "rule_b": SlackMessage(text="B")}

    monkeypatch.setattr(eng, "evaluate_all", fake_eval)
    cfg = SlackConfig(enabled=True, min_interval_seconds=3600,
                      rules={"rule_a": {"enabled": True},
                             "rule_b": {"enabled": True}})
    notifier = RecordingNotifier()
    engine = NotificationEngine(cfg, _session_getter(None), notifier=notifier,
                                state_path=statefile, clock=clk)
    engine.run_once()
    # global floor: only ONE post allowed in the interval
    assert len(notifier.posts) == 1


def test_state_persists_across_engine_instances(monkeypatch, statefile):
    clk = FakeClock()
    msg = SlackMessage(text="stuck")
    engine1, notifier1 = _make_engine(monkeypatch, statefile, msg, clk)
    engine1.run_once()
    assert len(notifier1.posts) == 1

    # New engine instance (simulating a daemon restart) reads persisted state.
    engine2, notifier2 = _make_engine(monkeypatch, statefile, msg, clk)
    clk.t += 300  # within cooldown
    engine2.run_once()
    assert len(notifier2.posts) == 0  # remembered last_fired -> suppress


def test_default_state_path_sqlite_beside_db():
    p = default_state_path("sqlite:////tmp/foo/bar.db")
    assert p == "/tmp/foo/.pbs_monitor_alert_state.json"


def test_default_state_path_postgres_home():
    p = default_state_path("postgresql://localhost/pbs_monitor_dev")
    assert p.endswith(".pbs_monitor_alert_state.json")
    assert "/tmp/foo" not in p
