"""
Notification rules for PBS Monitor.

Each rule is a pure function with the signature::

    rule(db, cfg, now) -> Optional[SlackMessage]

    db  : an active SQLAlchemy Session (live daemon session, or a session
          opened against a copied DB for testing)
    cfg : the SlackConfig object (rule reads its own thresholds from cfg.rules)
    now : the reference "current time" (naive UTC datetime), injectable for tests

A rule returns a SlackMessage when its condition fires, or None otherwise.
Rules DO NOT post and DO NOT manage cooldown/dedup -- that is the notifier
engine's job (see engine.py). Keeping rules pure makes them unit-testable and
runnable in dry-run against a real DB copy without any side effects.

Threshold config lives under ``slack.rules.<rule_key>`` in the YAML, e.g.::

    slack:
      rules:
        repeated_crash:
          enabled: true
          min_repeats: 3
          window_hours: 6
        down_node_surge:
          enabled: true
          min_unavailable: 30
          jump_factor: 2.0
        queue_drying:
          enabled: true
          queues: ["prod"]
          min_queued_jobs: 20
          high_util_pct: 85.0

All thresholds have safe defaults so a rule works before it is tuned.
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

from sqlalchemy import text
from sqlalchemy.orm import Session

from .slack import SlackMessage

logger = logging.getLogger(__name__)


# --------------------------------------------------------------------------- #
# helpers
# --------------------------------------------------------------------------- #

def _rule_cfg(cfg: Any, key: str) -> Dict[str, Any]:
    """Return the config dict for a rule key, or {} if absent."""
    rules = getattr(cfg, "rules", None) or {}
    val = rules.get(key) if isinstance(rules, dict) else None
    return val if isinstance(val, dict) else {}


def _rule_enabled(cfg: Any, key: str, default: bool = True) -> bool:
    return bool(_rule_cfg(cfg, key).get("enabled", default))


def _cluster(cfg: Any) -> str:
    return getattr(cfg, "cluster_label", None) or "cluster"


# --------------------------------------------------------------------------- #
# S3 -- Down / offline node surge  (system-level, no exit-code semantics)
# --------------------------------------------------------------------------- #

def down_node_surge(db: Session, cfg: Any, now: datetime) -> Optional[SlackMessage]:
    """Fire when the number of unavailable nodes is high AND rose sharply.

    Uses system_snapshots (available_nodes / total_nodes), which the daemon
    already writes every cycle -- no per-node TEXT-blob parsing required.

    Condition: latest_unavailable >= min_unavailable AND
               latest_unavailable >= jump_factor * baseline_unavailable
    where baseline is the unavailable count ~1 lookback_hours ago.
    """
    rc = _rule_cfg(cfg, "down_node_surge")
    min_unavailable = int(rc.get("min_unavailable", 30))
    jump_factor = float(rc.get("jump_factor", 2.0))
    lookback_hours = float(rc.get("lookback_hours", 1.0))

    # latest snapshot
    latest = db.execute(
        text(
            "SELECT timestamp, total_nodes, available_nodes "
            "FROM system_snapshots ORDER BY timestamp DESC LIMIT 1"
        )
    ).fetchone()
    if not latest or latest[1] is None or latest[2] is None:
        return None

    total_nodes = int(latest[1])
    avail = int(latest[2])
    unavailable = max(0, total_nodes - avail)

    # baseline snapshot ~lookback_hours ago
    baseline_cutoff = now - timedelta(hours=lookback_hours)
    baseline = db.execute(
        text(
            "SELECT total_nodes, available_nodes FROM system_snapshots "
            "WHERE timestamp <= :cut ORDER BY timestamp DESC LIMIT 1"
        ),
        {"cut": baseline_cutoff},
    ).fetchone()

    baseline_unavailable = 0
    if baseline and baseline[0] is not None and baseline[1] is not None:
        baseline_unavailable = max(0, int(baseline[0]) - int(baseline[1]))

    if unavailable < min_unavailable:
        return None
    # Require a real jump; if baseline is 0 treat any breach of min as a surge.
    if baseline_unavailable > 0 and unavailable < jump_factor * baseline_unavailable:
        return None

    cluster = _cluster(cfg)
    was = f"was {baseline_unavailable} ~{lookback_hours:g}h ago" if baseline_unavailable else "up from ~0"
    text_msg = (
        f":warning: *{cluster} - node availability drop.* "
        f"{unavailable} nodes now unavailable ({was}). "
        f"{avail:,} / {total_nodes:,} free. "
        f"Possible rack/cooling/health event - worth a look."
    )
    return SlackMessage(text=text_msg)


# --------------------------------------------------------------------------- #
# S1 -- Queue drying up  (system-level; the "shallow queue" signal)
# --------------------------------------------------------------------------- #

def queue_drying(db: Session, cfg: Any, now: datetime) -> Optional[SlackMessage]:
    """Fire when routed production backlog is thin WHILE utilization is high.

    The actionable moment: the machine is nearly full now but the queued work
    that keeps it full is running out -> idle capacity risk soon.

    IMPORTANT -- routing queues hold nothing. On Aurora ``prod`` is a *routing*
    queue that dispatches to execution queues (small/medium/large); its own
    ``queued_jobs`` is ~0 by design, so watching it directly is useless. Instead
    we aggregate the queued backlog across the EXECUTION queues it routes to.

    Config (``slack.rules.queue_drying``):
        exec_queues     : list of execution queues to aggregate
                          (default ["small", "medium", "large"])
        routing_label   : display name for the group (default "prod")
        min_queued_jobs : fire when aggregate queued <= this (default 10;
                          ~bottom decile of Aurora's small+medium+large backlog)
        high_util_pct   : only fire when system utilization >= this (default 85)

    Condition: aggregate_queued(exec_queues) <= min_queued_jobs
               AND system utilization >= high_util_pct
    """
    rc = _rule_cfg(cfg, "queue_drying")
    exec_queues: List[str] = list(rc.get("exec_queues", ["small", "medium", "large"]))
    routing_label: str = str(rc.get("routing_label", "prod"))
    min_queued_jobs = int(rc.get("min_queued_jobs", 10))
    high_util_pct = float(rc.get("high_util_pct", 85.0))

    # Current system utilization from the latest system snapshot.
    # NOTE: available_nodes = FREE nodes; utilization = busy fraction.
    sysrow = db.execute(
        text(
            "SELECT total_nodes, available_nodes FROM system_snapshots "
            "ORDER BY timestamp DESC LIMIT 1"
        )
    ).fetchone()
    if not sysrow or not sysrow[0]:
        return None
    total_nodes = int(sysrow[0])
    avail = int(sysrow[1]) if sysrow[1] is not None else 0
    util_pct = 100.0 * (total_nodes - avail) / total_nodes if total_nodes else 0.0

    if util_pct < high_util_pct:
        return None

    # Aggregate the LATEST queued backlog across the execution queues.
    per_queue: List[str] = []
    total_queued = 0
    total_running = 0
    for q in exec_queues:
        qrow = db.execute(
            text(
                "SELECT queued_jobs, running_jobs FROM queue_snapshots "
                "WHERE queue_name = :q ORDER BY timestamp DESC LIMIT 1"
            ),
            {"q": q},
        ).fetchone()
        if not qrow:
            continue
        queued = int(qrow[0] or 0)
        running = int(qrow[1] or 0)
        total_queued += queued
        total_running += running
        per_queue.append(f"`{q}` {queued}q/{running}r")

    if not per_queue:
        return None
    if total_queued > min_queued_jobs:
        return None

    cluster = _cluster(cfg)
    breakdown = ", ".join(per_queue)
    text_msg = (
        f":chart_with_downwards_trend: *{cluster} - {routing_label} backlog drying up.* "
        f"Utilization {util_pct:.0f}% but only *{total_queued} queued* across "
        f"{routing_label}'s execution queues ({breakdown}). "
        f"Idle-capacity risk if the backlog isn't refilled soon."
    )
    return SlackMessage(text=text_msg)


# --------------------------------------------------------------------------- #
# U1 -- Repeated identical crash  (user-experience; highest-signal, least ambiguous)
# --------------------------------------------------------------------------- #

# Outcome classes that represent a REAL user/job failure (not a benign requeue).
# NOTE: exit-code / outcome-class semantics are site-specific -- confirm with the
# scheduler admins before this drives actual user outreach. Safe for channel testing.
_REAL_FAILURE_CLASSES = ("signal_killed", "walltime_killed", "error")


def repeated_crash(db: Session, cfg: Any, now: datetime) -> Optional[SlackMessage]:
    """Fire when one user re-runs the same-named job and it keeps failing.

    Pattern: same owner + same job_name + a real-failure outcome_class, occurring
    >= min_repeats times within window_hours. Unambiguous "stuck in a loop"
    signal -- a strong candidate for UX outreach.

    Returns a message describing up to ``max_report`` distinct offending
    (owner, job_name) signatures.
    """
    rc = _rule_cfg(cfg, "repeated_crash")
    min_repeats = int(rc.get("min_repeats", 3))
    window_hours = float(rc.get("window_hours", 6.0))
    max_report = int(rc.get("max_report", 3))

    cutoff = now - timedelta(hours=window_hours)
    placeholders = ",".join(f"'{c}'" for c in _REAL_FAILURE_CLASSES)

    rows = db.execute(
        text(
            "SELECT owner, job_name, outcome_class, COUNT(*) AS n "
            "FROM jobs "
            "WHERE end_time >= :cut "
            f"  AND outcome_class IN ({placeholders}) "
            "  AND job_name IS NOT NULL AND job_name != '' "
            "GROUP BY owner, job_name, outcome_class "
            "HAVING COUNT(*) >= :minr "
            "ORDER BY n DESC "
            "LIMIT :lim"
        ),
        {"cut": cutoff, "minr": min_repeats, "lim": max_report},
    ).fetchall()

    if not rows:
        return None

    cluster = _cluster(cfg)
    lines = []
    for owner, job_name, outcome_class, n in rows:
        lines.append(
            f"- `{owner}` ran `{job_name}` {n}x, all *{outcome_class}*"
        )
    body = "\n".join(lines)
    text_msg = (
        f":repeat: *{cluster} - repeated job failures (UX flag).* "
        f"In the last {window_hours:g}h:\n{body}\n"
        f"_Likely stuck - candidate(s) for outreach. "
        f"Verify exit-code semantics before contacting users._"
    )
    return SlackMessage(text=text_msg)


# --------------------------------------------------------------------------- #
# registry
# --------------------------------------------------------------------------- #

# Ordered registry of all rules: key -> function. The engine iterates this,
# skipping any rule disabled in config.
ALL_RULES = {
    "down_node_surge": down_node_surge,
    "queue_drying": queue_drying,
    "repeated_crash": repeated_crash,
}


def evaluate_all(db: Session, cfg: Any, now: Optional[datetime] = None) -> Dict[str, Optional[SlackMessage]]:
    """Evaluate every enabled rule; return {rule_key: SlackMessage|None}.

    Never raises: a broken rule logs and yields None so one bad rule can't
    block the others or crash the collection cycle.
    """
    if now is None:
        now = datetime.utcnow()
    results: Dict[str, Optional[SlackMessage]] = {}
    for key, fn in ALL_RULES.items():
        if not _rule_enabled(cfg, key):
            results[key] = None
            continue
        try:
            results[key] = fn(db, cfg, now)
        except Exception as e:  # noqa: BLE001 - isolate rule failures
            logger.error("Notification rule '%s' failed: %s", key, e)
            results[key] = None
    return results
