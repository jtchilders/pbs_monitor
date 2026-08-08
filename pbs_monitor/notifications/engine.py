"""
Notification engine for PBS Monitor.

Wraps rule evaluation with the anti-spam machinery that turns raw rule firings
into sensible Slack posts:

    * Edge-triggering: a rule that stays fired for many collection cycles posts
      ONCE (on the transition None -> fired), not every cycle. It re-arms only
      after the condition clears (fired -> None) or the cooldown elapses.
    * Content-aware dedupe: when a rule attaches a ``signature`` to its message
      (a stable hash of WHAT it's reporting -- e.g. the set of offending jobs),
      the engine re-posts as soon as that signature CHANGES (a new/different
      finding), even inside the cooldown, but stays quiet while the signature is
      unchanged. This stops the "same alert every cooldown tick for 24h" spam
      while still surfacing a genuinely new offender immediately. Rules that
      supply no signature keep the classic time-only behavior below.
    * Per-rule cooldown: even a genuine re-fire of the SAME content won't post
      more often than ``cooldown_seconds`` (rule config) / a sane default.
    * Global floor: at most one post per ``min_interval_seconds`` across ALL
      rules (SlackConfig.min_interval_seconds), so a bad hour can't flood. This
      floor applies even to signature-changed posts.
    * Durable state: last-fired time + last-signature + last-state per rule
      persisted to a small JSON file next to the DB, so restarts don't re-alert.

The engine never raises into the caller (the daemon collection cycle): all
failures are logged and swallowed.
"""

from __future__ import annotations

import json
import logging
import os
import time
from dataclasses import dataclass
from typing import Any, Callable, Dict, List, Optional

from .rules import evaluate_all
from .slack import SlackMessage, SlackNotifier

logger = logging.getLogger(__name__)

DEFAULT_COOLDOWN_SECONDS = 3600  # 1h per-rule default if not set in config


@dataclass
class RuleOutcome:
    """Result of processing one rule in a cycle (for reporting / testing)."""
    key: str
    fired: bool          # did the rule's condition match this cycle?
    posted: bool         # did we actually post (or would-post in dry-run)?
    suppressed: Optional[str] = None  # why not posted: 'edge'|'cooldown'|'global'|'not-configured'
    message: Optional[SlackMessage] = None


def default_state_path(db_url: str = "") -> str:
    """Choose the state file location.

    For sqlite:///path put state beside the DB file; otherwise (Postgres, or
    unknown) use the user's home directory. Callers that know the real DB url
    (CLI/daemon) should pass it; the engine falls back to home when unset.
    """
    prefix = "sqlite:///"
    if db_url.startswith(prefix):
        db_path = db_url[len(prefix):]
        base = os.path.dirname(os.path.abspath(os.path.expanduser(db_path)))
        return os.path.join(base, ".pbs_monitor_alert_state.json")
    return os.path.expanduser("~/.pbs_monitor_alert_state.json")


class NotificationEngine:
    """Evaluate rules and post fired messages with edge-trigger + cooldown."""

    def __init__(
        self,
        slack_config: Any,
        session_getter: Callable[[], Any],
        notifier: Optional[SlackNotifier] = None,
        state_path: Optional[str] = None,
        dry_run: bool = False,
        clock: Callable[[], float] = time.time,
    ) -> None:
        """
        Args:
            slack_config:   SlackConfig object.
            session_getter: a zero-arg callable returning a *context manager*
                            yielding a SQLAlchemy Session (e.g. functools.partial
                            of database.connection.get_db_session).
            notifier:       optional pre-built SlackNotifier; if None, built from
                            slack_config (honoring dry_run).
            state_path:     override JSON state file location.
            dry_run:        render + log messages but never POST.
            clock:          injectable time source (for tests).
        """
        self.cfg = slack_config
        self.session_getter = session_getter
        self.dry_run = dry_run or getattr(slack_config, "dry_run", False)
        self.notifier = notifier or SlackNotifier.from_config(
            slack_config, dry_run=self.dry_run
        )
        self.state_path = state_path or default_state_path()
        self.clock = clock
        self.state: Dict[str, Any] = self._load_state()

    # -- state persistence -------------------------------------------------- #

    def _load_state(self) -> Dict[str, Any]:
        try:
            if os.path.exists(self.state_path):
                with open(self.state_path, "r") as f:
                    return json.load(f)
        except Exception as e:  # noqa: BLE001
            logger.warning("Could not read alert state %s: %s", self.state_path, e)
        return {"rules": {}, "last_global_post": 0.0}

    def _save_state(self) -> None:
        try:
            tmp = self.state_path + ".tmp"
            with open(tmp, "w") as f:
                json.dump(self.state, f, indent=2)
            os.replace(tmp, self.state_path)
        except Exception as e:  # noqa: BLE001
            logger.warning("Could not write alert state %s: %s", self.state_path, e)

    # -- helpers ------------------------------------------------------------ #

    def _rule_cooldown(self, key: str) -> int:
        rules = getattr(self.cfg, "rules", None) or {}
        rc = rules.get(key, {}) if isinstance(rules, dict) else {}
        return int(rc.get("cooldown_seconds", DEFAULT_COOLDOWN_SECONDS))

    def _global_floor(self) -> int:
        return int(getattr(self.cfg, "min_interval_seconds", 0) or 0)

    # -- core --------------------------------------------------------------- #

    def run_once(self, now_ts: Optional[float] = None) -> List[RuleOutcome]:
        """Evaluate all rules once; post any that pass edge/cooldown/global gates.

        Returns a list of RuleOutcome (including non-firing rules) for reporting.
        Never raises.
        """
        ts = now_ts if now_ts is not None else self.clock()
        outcomes: List[RuleOutcome] = []
        rule_state: Dict[str, Any] = self.state.setdefault("rules", {})

        # Evaluate all rules inside a single DB session.
        results: Dict[str, Optional[SlackMessage]] = {}
        try:
            with self.session_getter() as db:
                results = evaluate_all(db, self.cfg)
        except Exception as e:  # noqa: BLE001
            logger.error("Notification rule evaluation failed: %s", e)
            return outcomes

        for key, msg in results.items():
            fired = msg is not None
            prev = rule_state.setdefault(key, {"last_state": "clear", "last_fired": 0.0})
            was_fired = prev.get("last_state") == "fired"

            if not fired:
                # Condition cleared -> re-arm the edge trigger.
                if was_fired:
                    logger.info("Notification rule '%s' cleared; re-armed.", key)
                prev["last_state"] = "clear"
                outcomes.append(RuleOutcome(key=key, fired=False, posted=False))
                continue

            # Rule fired this cycle. Decide whether to post.
            # Post when any of:
            #   * fresh edge: the rule wasn't fired last cycle; OR
            #   * content changed: the message carries a signature that differs
            #     from the last one we posted (a NEW/different finding -- a new
            #     stuck job appeared or one dropped off -- should surface right
            #     away, not wait out the cooldown timer); OR
            #   * still fired with the SAME content but the cooldown has elapsed
            #     (a persistent problem re-surfaces, but only on the cooldown
            #     cadence, never every cycle).
            # Then also respect the global floor across all rules.
            suppressed: Optional[str] = None
            since_fired = ts - float(prev.get("last_fired", 0.0))
            cooldown = self._rule_cooldown(key)
            ever_posted = float(prev.get("last_fired", 0.0)) > 0

            new_sig = getattr(msg, "signature", None)
            last_sig = prev.get("last_signature")
            # Content-aware only when the rule supplies a signature. Rules that
            # leave it None keep the classic time-only behavior (signature_changed
            # stays False -> gating falls back purely to edge + cooldown).
            signature_changed = bool(new_sig is not None and new_sig != last_sig)

            is_fresh_edge = not was_fired
            cooldown_elapsed = since_fired >= cooldown

            if signature_changed:
                # A genuinely different finding -> post now, cooldown or not.
                suppressed = None
            elif not is_fresh_edge and not cooldown_elapsed:
                # Sustained condition, same content, still within cooldown -> quiet.
                suppressed = "cooldown"
            elif is_fresh_edge and ever_posted and not cooldown_elapsed:
                # Re-fired quickly after clearing, same content, within cooldown.
                suppressed = "cooldown"

            # Global floor is a hard cap across all rules -- but a changed
            # signature is important enough to bypass the per-rule cooldown, yet
            # NOT the global anti-flood floor (that exists to stop a bad hour from
            # spamming the channel regardless of which rule fires).
            if suppressed is None and (
                self._global_floor()
                and float(self.state.get("last_global_post", 0.0)) > 0
                and (ts - float(self.state.get("last_global_post", 0.0)))
                < self._global_floor()
            ):
                suppressed = "global"

            prev["last_state"] = "fired"

            if suppressed:
                outcomes.append(RuleOutcome(key=key, fired=True, posted=False,
                                            suppressed=suppressed, message=msg))
                continue

            posted = self.notifier.post(msg)  # type: ignore[arg-type]
            if posted:
                prev["last_fired"] = ts
                prev["last_signature"] = new_sig
                self.state["last_global_post"] = ts
            outcomes.append(RuleOutcome(key=key, fired=True, posted=posted, message=msg))

        self._save_state()
        return outcomes

    def evaluate_dry(self) -> Dict[str, Optional[SlackMessage]]:
        """Evaluate all rules and return raw messages WITHOUT any posting,
        state changes, or cooldown gating. For the ``notify test`` CLI."""
        try:
            with self.session_getter() as db:
                return evaluate_all(db, self.cfg)
        except Exception as e:  # noqa: BLE001
            logger.error("Dry evaluation failed: %s", e)
            return {}
