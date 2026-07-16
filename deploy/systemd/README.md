# PBS Monitor — survive-maintenance service setup (Aurora / Polaris)

## Problem
Login-node maintenance kills the pbs_monitor daemon and Postgres, and nothing
brings them back. `crontab -e` doesn't help because on these UANs:
  - `crond` is not running, and
  - crontab state lives in `/var/spool/cron` (node-local, reimaged on maintenance).

## Solution: systemd *user* services under Lustre $HOME
Unit files live in `~/.config/systemd/user/` which is on Lustre ($HOME) and
SURVIVES maintenance. `loginctl enable-linger` makes the user systemd manager
start at boot and keep services running when you're logged out — so services
auto-restart after a maintenance reboot, and `Restart=always` covers plain crashes.

### The one caveat (and its fix)
The linger *enable-bit* (`/var/lib/systemd/linger/<user>`) is node-local and
root-owned, so maintenance CAN wipe it. When that happens the unit files still
exist; you just need to re-arm with ONE command:

    ~/pbs_monitor/systemd/rearm.sh

The off-cluster Hermes watchdog runs this automatically if it detects services
down or linger cleared, so in practice you shouldn't have to.

## Two environment gotchas these units work around (systemd runs a CLEAN env)
Under an interactive login your shell has module-loaded paths; systemd user
services do NOT. Two things broke and are now fixed:

1. **`pg_isready`/`psql`/`pg_dump` need Intel runtime libs** (`libimf.so`,
   `libintlc.so.5`) that live in `/opt/aurora/.../oneapi/compiler/latest/lib`.
   Fix: those two libs were COPIED into `~/pbs_monitor/venv/lib` so the Postgres
   tooling is self-contained (no dependency on the oneAPI module stack or its
   version). If you ever rebuild the venv, re-copy them:
       cp /opt/aurora/default/oneapi/compiler/latest/lib/libimf.so      ~/pbs_monitor/venv/lib/
       cp /opt/aurora/default/oneapi/compiler/latest/lib/libintlc.so.5  ~/pbs_monitor/venv/lib/
   (`pg_ctl` and `postgres` itself do NOT need these — only the client tools do,
   which is why Postgres started fine but the readiness probe hung.)

2. **PBS commands (`pbsnodes`/`qstat`/`pbs_rstat`/`qsub`) live in `/opt/pbs/bin`**,
   not on the systemd PATH. Symptom: daemon logs `Command not found: pbsnodes` and
   node/job collection silently fails. Fix: the daemon unit sets
       Environment=PATH=%h/pbs_monitor/venv/bin:/opt/pbs/bin:...
   Verify a healthy collection cycle in the log:
       Completed daemon data collection successfully: N jobs, M queues, 10624 nodes...

## The cgroup-teardown bug that killed PG every 5 min (fixed 2026-07-16)
On 2026-07-16 Postgres was found down and stuck in a loop: every ~6 min it
started, became ready, then ~40-95 ms later logged `received smart shutdown
request` and exited. Root cause was a systemd control-group lifecycle race in
the TIMER path (not a client, not the daemon, not the ExecStop):

  1. `pbs-postgres.timer` fires `pbs-postgres-check.service` (a `oneshot`).
  2. `start_postgres.sh` runs `pg_ctl start`, which FORKS the postmaster. For a
     brief moment that postmaster is a member of the check service's cgroup.
  3. The oneshot had `RemainAfterExit=no`, so the unit went `inactive` the
     instant the script exited. With the systemd default `KillMode=control-group`
     + `KillSignal=SIGTERM`, systemd swept the unit's cgroup with SIGTERM —
     killing the just-forked postmaster before it reparented to init. SIGTERM to
     the postmaster == "smart shutdown".
  4. The 5-min timer re-ran it, looping forever; PG never stayed up.

Why it hid: `pbs-postgres.service` (the boot/main ensure-up) has
`RemainAfterExit=yes`, so IT keeps its cgroup alive and the reparent completes —
the boot path was fine. Only the timer-driven CHECK oneshot raced. And a
hand-run `pg_ctl` survives because that postmaster lands in the SSH login
*session scope*, which systemd never tears down. That mismatch (manual OK,
automated killed) is the diagnostic signature.

Proof: `KillMode=control-group` + `KillSignal=15` on both PG units confirmed via
`systemctl --user show`; the `ready -> smart shutdown` pair within the same
second confirmed in `pgdata/startup.log`; postmaster cgroup membership confirmed
via `/proc/<pid>/cgroup`.

THE FIX (both PG units): add `KillMode=process` + `RemainAfterExit=yes` to
`pbs-postgres-check.service`, and `KillMode=process` to `pbs-postgres.service`.
`KillMode=process` makes systemd only ever signal the tracked MAIN pid on stop,
never sweep forked children — so a unit teardown can never SIGTERM the detached
postmaster. Verified by firing `pbs-postgres-check.service` by hand while PG was
running under the systemd cgroup: PG survived, same PID, no smart shutdown in
startup.log. (Before the fix that same action killed PG within 40 ms.)

Note: after the fix, PG started via `systemctl --user start pbs-postgres.service`
lives in `.../app.slice/pbs-postgres.service` cgroup (stable, survives SSH
logout) rather than a login session scope — start it through systemd, not by
hand, so it has a durable home.

## Files (source of truth, on Lustre)
  ~/pbs_monitor/systemd/start_postgres.sh          idempotent, wait-for-ready PG starter
  ~/pbs_monitor/systemd/pbs-postgres.service       Type=oneshot ensure-up, RemainAfterExit, KillMode=process
  ~/pbs_monitor/systemd/pbs-postgres-check.service timer-driven ensure-up (oneshot, RemainAfterExit + KillMode=process)
  ~/pbs_monitor/systemd/pbs-postgres.timer         re-check PG every 5 min
  ~/pbs_monitor/systemd/pbs-monitor-daemon.service daemon unit, Requires=PG, Restart=always
  ~/pbs_monitor/systemd/rearm.sh                   re-arm after maintenance (idempotent)

Installed as symlinks into ~/.config/systemd/user/ so edits to the source files
take effect after `systemctl --user daemon-reload`.

## Why this shape
- Postgres is a oneshot ensure-up (NOT Type=forking — pg_ctl double-forks and
  systemd mis-tracks it). RemainAfterExit keeps the dependency satisfied for the
  daemon. A 5-min timer re-runs the check to catch a mid-life PG crash (oneshot
  can't Restart=always, so the timer is its watchdog).
- The daemon runs `--foreground` under Type=simple so systemd owns the process
  directly (no PID-file fork chase). Restart=always handles daemon crashes.
  ExecStartPre gates on pg_isready so it never starts before Postgres is ready.

## Operate
    systemctl --user status pbs-postgres.service pbs-monitor-daemon.service pbs-postgres.timer
    systemctl --user restart pbs-monitor-daemon.service
    tail -f ~/pbs_monitor/systemd/pbs-monitor-daemon.log
    tail -f ~/pbs_monitor/systemd/pbs-postgres.log
  (journalctl --user is NOT available for this user — logs go to the files above.)

## After maintenance (if the watchdog hasn't already done it)
    ~/pbs_monitor/systemd/rearm.sh

## Polaris note
Same layout, but re-verify BOTH env gotchas on Polaris independently:
  - Polaris venv psql/pg_isready already documented to need LD_LIBRARY_PATH
    (libpq) — the Intel libimf/libintlc copy step may differ; run
    `ldd ~/pbs_monitor/venv/bin/pg_isready` in a clean env to see what's missing.
  - PBS bin path is /opt/pbs/bin on Polaris too, but confirm.
  - PGDATA path differs — update the ExecStart lines in pbs-postgres.service and
    pbs-postgres-check.service.
