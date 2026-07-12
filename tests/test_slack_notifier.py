"""Tests for the Slack notifier transport + dry-run logic (no network)."""

from pbs_monitor.notifications.slack import SlackNotifier, SlackMessage


def test_transport_selection_webhook():
    n = SlackNotifier(webhook_url="https://hooks.slack.com/services/x")
    assert n.transport == "webhook"
    assert n.is_configured


def test_transport_selection_bot_token():
    n = SlackNotifier(bot_token="xoxb-abc", channel="#ops")
    assert n.transport == "bot_token"
    assert n.is_configured


def test_transport_none_when_unconfigured():
    n = SlackNotifier()
    assert n.transport == "none"
    assert not n.is_configured


def test_dry_run_never_posts_and_returns_true():
    # No credentials, but dry_run should still "succeed" (render only).
    n = SlackNotifier(dry_run=True)
    assert n.post("hello") is True
    assert n.post(SlackMessage(text="hi", blocks=[{"type": "section"}])) is True


def test_unconfigured_post_is_noop_false():
    n = SlackNotifier(dry_run=False)
    assert n.post("nobody home") is False


def test_from_config_disabled_returns_no_transport():
    class Cfg:
        enabled = False
        webhook_url = "https://hooks.slack.com/services/x"
        bot_token = None
        channel = None

    n = SlackNotifier.from_config(Cfg(), dry_run=False)
    assert n.transport == "none"  # disabled overrides credentials


def test_from_config_enabled_webhook():
    class Cfg:
        enabled = True
        webhook_url = "https://hooks.slack.com/services/x"
        bot_token = None
        channel = "#ops"
        dry_run = False
        timeout_seconds = 5

    n = SlackNotifier.from_config(Cfg())
    assert n.transport == "webhook"
    assert n.channel == "#ops"


def test_message_payload_includes_blocks_and_channel():
    m = SlackMessage(text="t", blocks=[{"type": "section"}])
    payload = m.to_payload(channel="#c")
    assert payload["text"] == "t"
    assert payload["blocks"] == [{"type": "section"}]
    assert payload["channel"] == "#c"
