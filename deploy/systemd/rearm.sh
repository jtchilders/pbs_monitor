#!/usr/bin/env bash
# Re-arm systemd-user linger + services after a maintenance reboot wipes the
# node-local linger flag (/var/lib/systemd/linger/<user>, root-owned, not on Lustre).
# The unit FILES live under ~/.config/systemd/user (Lustre, persistent); only the
# linger enable-bit and the running manager are lost on reboot. This restores them.
# Idempotent: safe to run any time, by hand or from an off-cluster watchdog.
set -euo pipefail

echo "[rearm] enabling linger for $USER ..."
loginctl enable-linger "$USER" || true

systemctl --user daemon-reload 2>/dev/null || true

echo "[rearm] enabling + starting units ..."
systemctl --user enable pbs-postgres.service pbs-monitor-daemon.service pbs-postgres.timer 2>/dev/null || true
systemctl --user start pbs-postgres.service
systemctl --user start pbs-monitor-daemon.service
systemctl --user start pbs-postgres.timer

echo "[rearm] status:"
systemctl --user --no-pager status pbs-postgres.service pbs-monitor-daemon.service pbs-postgres.timer 2>/dev/null | \
    grep -E 'Loaded:|Active:' || true

echo "[rearm] linger state:"
loginctl show-user "$USER" 2>/dev/null | grep -i linger || true
