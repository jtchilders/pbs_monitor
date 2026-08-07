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
          # Real-failure classes to EXCLUDE from the trigger. Default excludes
          # walltime_killed (many users deliberately code to the wall via
          # checkpoint/restart). Set to [] to count every real-failure class.
          exclude_outcome_classes: ["walltime_killed"]
        down_node_surge:
          enabled: true
          min_unavailable: 30
          jump_factor: 2.0
        queue_drying:
          enabled: true
          queues: ["prod"]
          min_queued_jobs: 20
          high_util_pct: 85.0
        repeated_rerun_held:
          enabled: true
          min_run_count: 5       # PBS run attempts before we care
          window_hours: 24
          require_failure: true  # only jobs that failed/held (not clean requeues)
          max_report: 5
          # A walltime kill isn't a "requeued to death" loop -- excluded by
          # default (many users deliberately code to the wall). [] counts all.
          exclude_outcome_classes: ["walltime_killed"]
          # Re-post cadence for the SAME set of offenders. The engine now
          # re-posts immediately when the offender set CHANGES (content
          # signature), so this only paces re-alerts of an UNCHANGED finding.
          # Match it to window_hours (86400 = 24h) so a persistent finding
          # alerts at most once/day instead of every collection cycle.
          cooldown_seconds: 86400

All thresholds have safe defaults so a rule works before it is tuned.

Anti-spam note: rules that report a *set of offenders* (repeated_rerun_held,
repeated_crash) attach a content ``signature`` to their SlackMessage. The
NotificationEngine re-posts as soon as that signature changes (a new offender
appears or one ages out) but suppresses an unchanged finding until
``cooldown_seconds`` elapses -- so a stuck job that lingers in the 24h window is
reported once, not once per cycle.
"""

from __future__ import annotations

import hashlib
import logging
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Sequence

from sqlalchemy import text
from sqlalchemy.orm import Session

from .slack import SlackMessage

logger = logging.getLogger(__name__)


def _signature(rule_key: str, items: Sequence[Any]) -> str:
    """Build a stable content signature for an alert.

    ``items`` is an iterable of the facts that define WHAT the alert reports
    (e.g. per-offender tuples). We sort them so ordering differences between
    cycles don't change the signature, join into a canonical string, and hash.
    The rule key is folded in so two rules can never collide.

    The NotificationEngine compares this signature across cycles: an unchanged
    signature within the cooldown is treated as "same finding, stay quiet";
    a changed signature (a new/different offender appeared or one dropped off)
    is treated as a fresh edge and posts immediately. See engine.py.
    """
    canon = "|".join(sorted(str(i) for i in items))
    digest = hashlib.sha1(f"{rule_key}::{canon}".encode("utf-8")).hexdigest()
    return f"{rule_key}:{digest[:16]}"


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


# Human-readable annotations for the exit codes most worth calling out in an
# alert. Anything not listed is shown as the bare number. Kept intentionally
# small -- the goal is to explain the *actionable* codes at a glance, not to be
# an exhaustive PBS/signal table (the web dashboard has the full taxonomy).
_EXIT_CODE_NOTES: Dict[int, str] = {
    -29: "walltime exceeded",   # confirmed on Aurora: 100% ran >=95% of walltime
    143: "SIGTERM",             # graceful kill (walltime/qdel/drain)
    137: "SIGKILL/OOM",         # hard kill -- frequently the OOM killer
    139: "SIGSEGV",             # segfault
    134: "SIGABRT",             # abort()
    271: "requeued",
    127: "command not found",
    126: "not executable",
}


def _fmt_exit_code(code: Any) -> str:
    """Format an exit code for a Slack message, annotating notable codes.

    ``None`` (no recorded exit code) -> ``"none"``. Known actionable codes get a
    short parenthetical (e.g. ``"-29 (walltime exceeded)"``); everything else is
    the bare integer.
    """
    if code is None:
        return "none"
    try:
        ci = int(code)
    except (TypeError, ValueError):
        return str(code)
    note = _EXIT_CODE_NOTES.get(ci)
    return f"{ci} ({note})" if note else str(ci)


def _short_job_id(job_id: Any) -> str:
    """Shorten a PBS job id for display in an alert.

    PBS ids are fully-qualified, e.g.
    ``7257550.polaris-pbs-01.hsn.cm.polaris.alcf.anl.gov``. The numeric prefix
    before the first dot is the sequence number admins actually use with
    ``qstat``/``qstat -x``, so keep that and drop the host suffix. Returns the
    id unchanged if it has no dot or isn't a string.
    """
    if job_id is None:
        return "?"
    s = str(job_id)
    return s.split(".", 1)[0] if "." in s else s


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

# Outcome classes excluded from the repeated-crash TRIGGER by default. walltime_killed
# (PBS -29 / SIGTERM-at-wall) is excluded because many ALCF users deliberately request
# LESS walltime than their job needs and checkpoint/restart to maximize allocation
# utilization -- for them a walltime kill is expected per-cycle behavior, not a stuck
# loop, so counting it toward "repeated failures" produces alarm fatigue. This is
# TRIGGER-only: walltime kills are still classified and still shown in the web exit
# taxonomy; they just don't drive the Slack alert. Override per cluster via
# `slack.rules.repeated_crash.exclude_outcome_classes` (a list; [] to count everything).
_REPEATED_CRASH_DEFAULT_EXCLUDE = ("walltime_killed",)


def repeated_crash(db: Session, cfg: Any, now: datetime) -> Optional[SlackMessage]:
    """Fire when one user re-runs the same-named job and it keeps failing.

    Pattern: same owner + same job_name + a real-failure outcome_class, occurring
    >= min_repeats times within window_hours. Unambiguous "stuck in a loop"
    signal -- a strong candidate for UX outreach.

    Excluded classes (default ``walltime_killed``) do NOT count toward the
    trigger: many users deliberately under-request walltime and checkpoint/restart
    to maximize utilization, so a walltime kill is expected behavior, not a stuck
    loop. Tune via ``slack.rules.repeated_crash.exclude_outcome_classes`` (a list;
    pass ``[]`` to count every real-failure class including walltime).

    The message reports, for each offending (owner, job_name) signature, the
    failure outcome class(es), the raw PBS exit code(s) behind them, and the
    count -- so the reader immediately sees *why* the jobs died (e.g. 137 =
    SIGKILL/OOM, 1 = user error) without opening the dashboard.

    Returns a message describing up to ``max_report`` distinct offending
    (owner, job_name) signatures.
    """
    rc = _rule_cfg(cfg, "repeated_crash")
    min_repeats = int(rc.get("min_repeats", 3))
    window_hours = float(rc.get("window_hours", 6.0))
    max_report = int(rc.get("max_report", 3))

    # Which real-failure classes count toward the trigger. Start from the full
    # real-failure set, then drop the configured exclusions (default: walltime_killed).
    exclude_cfg = rc.get("exclude_outcome_classes", list(_REPEATED_CRASH_DEFAULT_EXCLUDE))
    exclude = {str(c) for c in exclude_cfg} if isinstance(exclude_cfg, (list, tuple, set)) else set()
    trigger_classes = [c for c in _REAL_FAILURE_CLASSES if c not in exclude]
    if not trigger_classes:
        # Everything excluded -> nothing can fire. Bail cheaply.
        return None

    cutoff = now - timedelta(hours=window_hours)
    placeholders = ",".join(f"'{c}'" for c in trigger_classes)

    # Group by (owner, job_name, outcome_class, exit_status) so we can both count
    # per-signature failures AND surface the distinct raw exit codes. Rolling the
    # exit codes up in Python keeps this portable across SQLite (test copies) and
    # Postgres (production) -- no GROUP_CONCAT/STRING_AGG dialect split. We also
    # pull job_id so the message can show a few sample ids per offender (these
    # are DISTINCT job_ids -- the user resubmitting the same-named job N times).
    rows = db.execute(
        text(
            "SELECT owner, job_name, outcome_class, exit_status, job_id, end_time "
            "FROM jobs "
            "WHERE end_time >= :cut "
            f"  AND outcome_class IN ({placeholders}) "
            "  AND job_name IS NOT NULL AND job_name != '' "
            "ORDER BY end_time DESC"
        ),
        {"cut": cutoff},
    ).fetchall()

    if not rows:
        return None

    # Roll up per (owner, job_name): total failures, per-class counts, exit codes,
    # and a few sample job_ids (most recent first, thanks to ORDER BY end_time).
    from collections import defaultdict

    sig_total: Dict[tuple, int] = defaultdict(int)
    sig_classes: Dict[tuple, Dict[str, int]] = defaultdict(lambda: defaultdict(int))
    sig_codes: Dict[tuple, Dict[Any, int]] = defaultdict(lambda: defaultdict(int))
    sig_job_ids: Dict[tuple, list] = defaultdict(list)
    _MAX_SAMPLE_IDS = 3
    for owner, job_name, outcome_class, exit_status, job_id, _end in rows:
        key = (owner, job_name)
        sig_total[key] += 1
        sig_classes[key][outcome_class] += 1
        sig_codes[key][exit_status] += 1
        if len(sig_job_ids[key]) < _MAX_SAMPLE_IDS:
            sig_job_ids[key].append(_short_job_id(job_id))

    # Keep only signatures whose TOTAL failures across classes/codes >= min_repeats.
    offenders = [(k, t) for k, t in sig_total.items() if t >= min_repeats]
    if not offenders:
        return None
    offenders.sort(key=lambda kt: -kt[1])
    offenders = offenders[:max_report]

    cluster = _cluster(cfg)
    lines = []
    for (owner, job_name), total in offenders:
        # class summary, most common first: "walltime_killed x4, error x1"
        cls_parts = [
            f"{c} x{n}"
            for c, n in sorted(sig_classes[(owner, job_name)].items(), key=lambda x: -x[1])
        ]
        cls_summary = ", ".join(cls_parts)
        # exit-code summary with human labels, most common first:
        # "exit -29 (walltime exceeded) x4, exit 1 x1"
        code_parts = [
            f"exit {_fmt_exit_code(code)} x{n}"
            for code, n in sorted(sig_codes[(owner, job_name)].items(), key=lambda x: -x[1])
        ]
        code_summary = ", ".join(code_parts)
        # A few sample job ids (distinct submissions) so the reader can look one
        # up directly (e.g. qstat -x <id>) without hunting in the dashboard.
        ids = sig_job_ids[(owner, job_name)]
        total_ids = sig_total[(owner, job_name)]
        ids_str = ", ".join(f"`{i}`" for i in ids)
        if total_ids > len(ids):
            ids_str += ", …"
        lines.append(
            f"- `{owner}` ran `{job_name}` {total}x: *{cls_summary}* — "
            f"{code_summary} (ids: {ids_str})"
        )
    body = "\n".join(lines)
    text_msg = (
        f":repeat: *{cluster} - repeated job failures (UX flag).* "
        f"In the last {window_hours:g}h:\n{body}\n"
        f"_Likely stuck - candidate(s) for outreach. "
        f"Verify exit-code semantics before contacting users._"
    )
    # Content signature = every offender (owner, job_name, total-failure-count)
    # across ALL offenders, not just the max_report shown. The engine re-posts
    # only when this set changes (a new offender appears, or a count moves)
    # rather than re-alerting the same offenders each cooldown tick.
    sig_items = [(k[0], k[1], t) for k, t in sig_total.items() if t >= min_repeats]
    signature = _signature("repeated_crash", sig_items)
    return SlackMessage(text=text_msg, signature=signature)


# --------------------------------------------------------------------------- #
# repeated auto-rerun then held/failed  (UX flag, per-job requeue loop)
# --------------------------------------------------------------------------- #

def repeated_rerun_held(db: Session, cfg: Any, now: datetime) -> Optional[SlackMessage]:
    """Fire when PBS auto-requeues a SINGLE job many times and it then fails/held.

    This is distinct from ``repeated_crash``. ``repeated_crash`` catches a *user*
    resubmitting the same-named job N times (N distinct job_ids). This rule
    catches PBS *auto-requeuing one job* (one job_id) via ``run_count`` until it
    gave up -- the ``comment: "job held, too many failed attempts to run and
    terminated"`` pattern. ``repeated_crash`` cannot see this: it's a single row,
    so nothing repeats at the (owner, job_name) level.

    A high ``run_count`` that ended in a terminal failure is a strong signal the
    user needs help (bad submit script, unsatisfiable resource request, a node
    the job keeps landing on and dying). It also badly distorts naive
    run/request efficiency, which is what surfaced this rule in the first place.

    Trigger: a finished job with ``run_count >= min_run_count`` whose end_time is
    within ``window_hours``. By default only genuinely-failed outcomes count
    (``require_failure: true``) so a job that was preempted many times but
    ultimately succeeded doesn't page; set ``require_failure: false`` to alert on
    ANY heavily-requeued job.

    Config (``slack.rules.repeated_rerun_held``):
        enabled          : bool  (default true)
        min_run_count    : int   (default 5)   -- attempts before we care
        window_hours     : float (default 24)
        require_failure  : bool  (default true) -- only failed/held outcomes
        max_report       : int   (default 5)    -- jobs listed in the message
        exclude_outcome_classes : list (default ["walltime_killed"]) -- outcome
                          classes that do NOT count toward the trigger even when
                          ``require_failure`` is set. Mirrors repeated_crash:
                          many ALCF users deliberately under-request walltime and
                          checkpoint/restart, so a walltime kill is expected
                          per-cycle behavior, not a stuck requeue loop. Pass ``[]``
                          to count every failure class (incl. walltime_killed).
    """
    rc = _rule_cfg(cfg, "repeated_rerun_held")
    min_run_count = int(rc.get("min_run_count", 5))
    window_hours = float(rc.get("window_hours", 24.0))
    require_failure = bool(rc.get("require_failure", True))
    max_report = int(rc.get("max_report", 5))
    # By default a walltime kill does not count as a "requeued to death" loop
    # (see repeated_crash's exclusion rationale). Configurable per cluster.
    exclude_cfg = rc.get("exclude_outcome_classes", ["walltime_killed"])
    exclude_classes = (
        {str(c) for c in exclude_cfg}
        if isinstance(exclude_cfg, (list, tuple, set))
        else set()
    )

    cutoff = now - timedelta(hours=window_hours)

    # Finished states: the job has reached a terminal record so run_count is
    # final. SQLAlchemy stores SQLEnum(JobState) as the enum NAME, so the DB
    # holds 'FINISHED'/'COMPLETED' (uppercase), NOT the enum values 'F'/'C'.
    sql = (
        "SELECT job_id, owner, job_name, run_count, outcome_class, exit_status, end_time "
        "FROM jobs "
        "WHERE run_count >= :minrc "
        "  AND end_time >= :cut "
        "  AND state IN ('FINISHED', 'COMPLETED') "
    )
    if require_failure:
        # Only jobs that ultimately failed/held. For THIS rule, "failure" is
        # broader than repeated_crash's _REAL_FAILURE_CLASSES: the dominant
        # outcome for a job PBS requeues to death is `could_not_run` (exit -3 /
        # "job held, too many failed attempts to run and terminated") -- verified
        # on real Aurora data, ~all of the top offenders are could_not_run/-3.
        # That class is deliberately EXCLUDED from repeated_crash (it's not a
        # user-code crash) but IS the point here. Include it plus the real-
        # failure classes; a non-zero exit code also counts as a backstop.
        trigger_classes = list(_REAL_FAILURE_CLASSES) + ["could_not_run"]
        placeholders = ",".join(f"'{c}'" for c in trigger_classes)
        sql += (
            f"  AND (outcome_class IN ({placeholders}) "
            "       OR (exit_status IS NOT NULL AND exit_status != 0)) "
        )
    sql += "ORDER BY run_count DESC, end_time DESC"

    rows = db.execute(text(sql), {"minrc": min_run_count, "cut": cutoff}).fetchall()
    if not rows:
        return None

    # Dedupe by (owner, job_name, run_count, exit_status): the jobs table can
    # hold several rows for the same logical job (requeue history / re-collection
    # of the same job_id family), and listing the same offender five times is
    # noise. Keep first occurrence (already ordered by run_count DESC).
    # Also drop excluded outcome classes here rather than in SQL: the
    # require_failure clause has an `exit_status != 0` backstop, and an excluded
    # class like walltime_killed carries a non-zero exit (-29) that would slip
    # through that backstop -- filtering in Python guarantees the exclusion holds
    # regardless of exit code.
    seen = set()
    distinct = []
    for row in rows:
        job_id, owner, job_name, run_count, outcome_class, exit_status, _end = row
        if outcome_class in exclude_classes:
            continue
        key = (owner, job_name, int(run_count), exit_status)
        if key in seen:
            continue
        seen.add(key)
        distinct.append(row)

    if not distinct:
        return None

    total_distinct = len(distinct)
    offenders = distinct[:max_report]
    cluster = _cluster(cfg)
    lines = []
    for job_id, owner, job_name, run_count, outcome_class, exit_status, _end in offenders:
        name = job_name or "(unnamed)"
        code = _fmt_exit_code(exit_status)
        cls = outcome_class or "unknown"
        jid = _short_job_id(job_id)
        lines.append(
            f"- `{owner}` job `{name}` (`{jid}`) was re-run *{int(run_count)}x* "
            f"then {cls} — last exit {code}"
        )
    body = "\n".join(lines)
    extra = ""
    if total_distinct > max_report:
        extra = f"\n_…and {total_distinct - max_report} more._"
    text_msg = (
        f":recycle: *{cluster} - jobs auto-requeued repeatedly then failed "
        f"(UX flag).* In the last {window_hours:g}h, {total_distinct} job(s) hit "
        f">={min_run_count} run attempts and did not succeed:\n{body}{extra}\n"
        f"_PBS kept retrying and gave up — likely a bad submit script, "
        f"unsatisfiable request, or a recurring node/launch failure. "
        f"Candidate(s) for outreach._"
    )
    # Content signature = the full set of distinct offenders (ALL of them, not
    # just the max_report shown), keyed by (owner, job_name, run_count,
    # exit_status). The engine re-posts only when this set changes -- a new
    # heavily-requeued job appears, or one ages out of the window -- instead of
    # re-alerting the identical finding on every cooldown tick. Independent of
    # the cluster label / wording so cosmetic changes don't re-fire.
    sig_items = [
        (owner, job_name, int(run_count), exit_status)
        for _jid, owner, job_name, run_count, _oc, exit_status, _end in distinct
    ]
    signature = _signature("repeated_rerun_held", sig_items)
    return SlackMessage(text=text_msg, signature=signature)


# --------------------------------------------------------------------------- #
# registry
# --------------------------------------------------------------------------- #

# Ordered registry of all rules: key -> function. The engine iterates this,
# skipping any rule disabled in config.
ALL_RULES = {
    "down_node_surge": down_node_surge,
    "queue_drying": queue_drying,
    "repeated_crash": repeated_crash,
    "repeated_rerun_held": repeated_rerun_held,
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
