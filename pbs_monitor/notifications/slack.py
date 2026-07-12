"""
Slack notification support for PBS Monitor.

Provides a thin, dependency-light client that can post messages to Slack via
either an incoming webhook URL or a bot token (chat.postMessage). The client
auto-detects which transport to use based on the configuration provided.

Design goals:
    * No hard dependency on the Slack SDK -- uses ``requests`` only.
    * Credential-agnostic: works with a webhook URL OR a bot token.
    * Dry-run mode: renders and logs the exact message WITHOUT posting, so
      message formatting can be verified against real data before going live.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Union

logger = logging.getLogger(__name__)

# requests is already an indirect dependency of the project (PBS command layer
# and web client use it). Import lazily-tolerant so a missing dep degrades to a
# clear error only when an actual post is attempted.
try:
    import requests  # type: ignore
    _REQUESTS_AVAILABLE = True
except ImportError:  # pragma: no cover - environment-specific
    requests = None  # type: ignore
    _REQUESTS_AVAILABLE = False


SLACK_POST_MESSAGE_URL = "https://slack.com/api/chat.postMessage"
DEFAULT_TIMEOUT_SECONDS = 10


@dataclass
class SlackMessage:
    """A single Slack message to be posted.

    ``text`` is always required (Slack uses it as the notification fallback and
    accessibility text even when ``blocks`` are present). ``blocks`` is optional
    Block Kit structure for richer formatting.
    """

    text: str
    blocks: Optional[List[Dict[str, Any]]] = None

    def to_payload(self, channel: Optional[str] = None) -> Dict[str, Any]:
        payload: Dict[str, Any] = {"text": self.text}
        if self.blocks:
            payload["blocks"] = self.blocks
        if channel:
            payload["channel"] = channel
        return payload


class SlackNotifier:
    """Posts messages to Slack via webhook URL or bot token.

    Transport selection:
        * If ``webhook_url`` is set -> POST the payload to that URL.
        * Elif ``bot_token`` is set -> POST to chat.postMessage with the token
          as a Bearer credential and ``channel`` in the payload.
        * Else -> disabled; ``post`` is a no-op that logs a warning.

    In ``dry_run`` mode no HTTP request is made; the rendered message is logged
    at INFO and returned, so callers can verify formatting against real data.
    """

    def __init__(
        self,
        webhook_url: Optional[str] = None,
        bot_token: Optional[str] = None,
        channel: Optional[str] = None,
        dry_run: bool = False,
        timeout: int = DEFAULT_TIMEOUT_SECONDS,
    ) -> None:
        self.webhook_url = webhook_url or None
        self.bot_token = bot_token or None
        self.channel = channel or None
        self.dry_run = dry_run
        self.timeout = timeout
        self.logger = logging.getLogger(__name__)

    @property
    def transport(self) -> str:
        """Return the active transport: 'webhook', 'bot_token', or 'none'."""
        if self.webhook_url:
            return "webhook"
        if self.bot_token:
            return "bot_token"
        return "none"

    @property
    def is_configured(self) -> bool:
        return self.transport != "none"

    def post(self, message: Union[SlackMessage, str]) -> bool:
        """Post a message to Slack.

        Returns True on success (or on a successful dry-run render), False on
        failure or when not configured. Never raises on transport errors --
        a notification failure must not crash the collection cycle.
        """
        if isinstance(message, str):
            message = SlackMessage(text=message)

        if self.dry_run:
            self.logger.info(
                "[slack dry-run] would post via %s to %s:\n%s",
                self.transport,
                self.channel or "(webhook default channel)",
                message.text,
            )
            return True

        if not self.is_configured:
            self.logger.warning(
                "Slack notifier not configured (no webhook_url or bot_token); "
                "skipping post: %s",
                message.text[:80],
            )
            return False

        if not _REQUESTS_AVAILABLE:
            self.logger.error(
                "Cannot post to Slack: the 'requests' package is not installed."
            )
            return False

        try:
            if self.transport == "webhook":
                return self._post_webhook(message)
            return self._post_bot_token(message)
        except Exception as e:  # noqa: BLE001 - must never crash the caller
            self.logger.error("Failed to post Slack message: %s", e)
            return False

    def _post_webhook(self, message: SlackMessage) -> bool:
        # Incoming webhooks ignore the 'channel' field for modern apps (the
        # channel is fixed at webhook creation), but we include it harmlessly
        # for legacy webhooks that still honor it.
        payload = message.to_payload(channel=self.channel)
        assert self.webhook_url is not None  # transport=='webhook' guarantees this
        resp = requests.post(  # type: ignore[union-attr]
            self.webhook_url, json=payload, timeout=self.timeout
        )
        if resp.status_code == 200 and resp.text == "ok":
            return True
        self.logger.error(
            "Slack webhook returned %s: %s", resp.status_code, resp.text[:200]
        )
        return False

    def _post_bot_token(self, message: SlackMessage) -> bool:
        if not self.channel:
            self.logger.error(
                "Bot-token transport requires a 'channel' (name or ID); none set."
            )
            return False
        headers = {
            "Authorization": f"Bearer {self.bot_token}",
            "Content-Type": "application/json; charset=utf-8",
        }
        payload = message.to_payload(channel=self.channel)
        resp = requests.post(  # type: ignore[union-attr]
            SLACK_POST_MESSAGE_URL,
            json=payload,
            headers=headers,
            timeout=self.timeout,
        )
        # chat.postMessage always returns 200; success is in the JSON body.
        try:
            body = resp.json()
        except ValueError:
            self.logger.error(
                "Slack chat.postMessage non-JSON response (%s): %s",
                resp.status_code,
                resp.text[:200],
            )
            return False
        if body.get("ok"):
            return True
        self.logger.error(
            "Slack chat.postMessage error: %s", body.get("error", "unknown")
        )
        return False

    @classmethod
    def from_config(cls, slack_config: Any, dry_run: bool = False) -> "SlackNotifier":
        """Build a SlackNotifier from a SlackConfig-like object.

        Accepts anything with the attributes: enabled, webhook_url, bot_token,
        channel. If ``enabled`` is False, returns a notifier with no transport
        (posts become no-ops), preserving the caller's simple ``.post()`` flow.
        """
        enabled = getattr(slack_config, "enabled", False)
        if not enabled:
            return cls(dry_run=dry_run)
        return cls(
            webhook_url=getattr(slack_config, "webhook_url", None),
            bot_token=getattr(slack_config, "bot_token", None),
            channel=getattr(slack_config, "channel", None),
            dry_run=dry_run or getattr(slack_config, "dry_run", False),
            timeout=getattr(slack_config, "timeout_seconds", DEFAULT_TIMEOUT_SECONDS),
        )
