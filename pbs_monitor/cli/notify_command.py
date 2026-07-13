"""
`pbs-monitor notify` CLI: test and send Slack notifications.

Subcommands:
    test  -- evaluate all rules against the DB and PRINT what would post
             (dry-run; no Slack post, no cooldown, no state change).
    send  -- evaluate + post via the notification engine, honoring
             edge-trigger + cooldown + global floor + durable state.
    status-- show current alert state (last fired times, config summary).

Neither requires a PBS connection; both operate against the configured DB.
"""

from __future__ import annotations

import argparse
import functools
import logging

from ..config import Config
from ..database.connection import get_db_session
from ..notifications.engine import NotificationEngine, default_state_path
from ..notifications.slack import SlackNotifier

logger = logging.getLogger(__name__)


class NotifyCommand:
    """Handle `pbs-monitor notify ...` subcommands."""

    def __init__(self, collector, config: Config):
        # collector unused (no PBS connection needed); kept for CLI symmetry.
        self.config = config
        self.logger = logging.getLogger(__name__)

    # -- helpers ------------------------------------------------------------ #

    def _session_getter(self):
        return functools.partial(get_db_session, self.config)

    def _state_path(self, override: str = "") -> str:
        if override:
            return override
        return default_state_path(getattr(self.config.database, "url", "") or "")

    def _engine(self, dry_run: bool, state_path: str = "") -> NotificationEngine:
        return NotificationEngine(
            slack_config=self.config.slack,
            session_getter=self._session_getter(),
            state_path=self._state_path(state_path),
            dry_run=dry_run,
        )

    # -- dispatch ----------------------------------------------------------- #

    def execute(self, args: argparse.Namespace) -> int:
        action = getattr(args, "notify_action", None)
        if action == "test":
            return self._test(args)
        if action == "send":
            return self._send(args)
        if action == "status":
            return self._status(args)
        print("Usage: pbs-monitor notify {test|send|status}")
        return 1

    # -- test --------------------------------------------------------------- #

    def _test(self, args: argparse.Namespace) -> int:
        """Evaluate rules and print what WOULD post. No posting, no state."""
        sc = self.config.slack
        notifier = SlackNotifier.from_config(sc, dry_run=True)
        print("=" * 70)
        print("pbs-monitor notify test  (DRY RUN -- nothing is posted)")
        print("=" * 70)
        print(f"  slack.enabled      : {getattr(sc, 'enabled', False)}")
        print(f"  transport          : {notifier.transport}")
        print(f"  cluster_label      : {getattr(sc, 'cluster_label', None)}")
        print(f"  db                 : {getattr(self.config.database, 'url', '?')}")
        print("-" * 70)

        engine = self._engine(dry_run=True)
        results = engine.evaluate_dry()

        any_fired = False
        for key, msg in results.items():
            if msg is None:
                print(f"  [ - ] {key}: (did not fire)")
            else:
                any_fired = True
                print(f"  [FIRE] {key}:")
                for line in msg.text.splitlines():
                    print(f"         {line}")
        print("-" * 70)
        if not any_fired:
            print("  No rules fired against current data.")
        if notifier.transport == "none":
            print("  NOTE: no webhook/bot token configured -- 'send' would be a no-op.")
        return 0

    # -- send --------------------------------------------------------------- #

    def _send(self, args: argparse.Namespace) -> int:
        """Evaluate + post through the engine (honors cooldown/state)."""
        sc = self.config.slack
        if not getattr(sc, "enabled", False):
            print("slack.enabled is False -- refusing to send. Set it in your config.")
            return 1
        force_dry = bool(getattr(args, "dry_run", False))
        engine = self._engine(dry_run=force_dry)
        outcomes = engine.run_once()

        posted = [o for o in outcomes if o.posted]
        fired = [o for o in outcomes if o.fired]
        suppressed = [o for o in outcomes if o.fired and not o.posted]

        tag = "DRY RUN" if (force_dry or engine.dry_run) else "LIVE"
        print(f"notify send [{tag}]: {len(fired)} fired, "
              f"{len(posted)} posted, {len(suppressed)} suppressed")
        for o in outcomes:
            if o.fired:
                status = "posted" if o.posted else f"suppressed({o.suppressed})"
                print(f"  - {o.key}: {status}")
        return 0

    # -- status ------------------------------------------------------------- #

    def _status(self, args: argparse.Namespace) -> int:
        import json
        import os
        sp = self._state_path()
        sc = self.config.slack
        print(f"State file: {sp}")
        print(f"slack.enabled: {getattr(sc, 'enabled', False)}  "
              f"dry_run: {getattr(sc, 'dry_run', False)}  "
              f"min_interval_seconds: {getattr(sc, 'min_interval_seconds', 0)}")
        if not os.path.exists(sp):
            print("  (no state yet -- nothing has fired)")
            return 0
        try:
            with open(sp) as f:
                state = json.load(f)
        except Exception as e:  # noqa: BLE001
            print(f"  could not read state: {e}")
            return 1
        for key, rs in (state.get("rules") or {}).items():
            print(f"  {key}: last_state={rs.get('last_state')} "
                  f"last_fired={rs.get('last_fired')}")
        return 0
