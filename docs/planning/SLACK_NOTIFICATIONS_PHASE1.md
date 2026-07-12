# Slack Notifications — Phase 1 (starter messages)

**Status:** Built on branch `feature/slack-notifications`. Plumbing + 3 starter rules
complete and **verified in dry-run against the real Aurora DB**. NOT wired into the
daemon and NOT posting to Slack yet — deliberately held for calibration + credential.
**Last updated:** 2026-07-12

---

## What this is

A minimal, reversible first cut of daemon → Slack alerting. The daemon already collects
fresh PBS data every cycle and has DB access; posting to Slack is just a hook at the end
of a successful collection. **No new service** (respects the service-creep concern), no
external reachability needed — these are *content* alerts, not liveness monitoring.

## What was built (Phase A, no daemon wiring yet)

- **`notifications/slack.py`** — `SlackNotifier`, credential-agnostic: supports an
  incoming **webhook URL** OR a **bot token** (chat.postMessage), auto-detected from
  config. Has a **dry-run mode** that renders + logs the exact message without posting.
  Never raises on transport errors (a failed notification must not crash collection).
  8 unit tests (`tests/test_slack_notifier.py`), all passing.
- **`SlackConfig`** in `config.py` — `enabled`, `webhook_url`, `bot_token`, `channel`,
  `dry_run`, `cluster_label`, `min_interval_seconds`, and a free-form `rules` dict so new
  rules need no config-schema change. Wired into load/save like the other config sections.
- **`notifications/rules.py`** — three starter rules as pure, testable functions
  `(db, cfg, now) -> Optional[SlackMessage]`. Rules don't post and don't manage
  cooldown/dedup (that's the engine's job, Phase B).

## The three starter rules + real Aurora verification

Evaluated against `pbs_monitor_aurora.db` (457,946 jobs, migrated, data → 2026-06-04),
`now = 2026-06-04 21:10`.

### U1 — `repeated_crash` (user-experience) — ✅ SHIP-READY
Same owner + same job_name + real-failure `outcome_class` ≥ N times in a window.
Verified output:
> :repeat: *Aurora - repeated job failures (UX flag).* In the last 24h:
> - `kaiyuyue` ran `gpt_commitpack2_synth` 22x, all *error*
> - `eisenste` ran `iprof.sh` 12x, all *error* …

Confirmed real: `kaiyuyue`'s 22 failures are distinct job IDs (8525779, 8525695, …), ~1/hr,
all exit status 1. **This is a genuine stuck-user signal — exactly the intended outreach
trigger.** Least ambiguous of the three; turn on first.
- **Caveat:** exit-code / outcome-class semantics are site-specific. `_REAL_FAILURE_CLASSES`
  = signal_killed / walltime_killed / error (excludes benign requeue/could_not_run).
  **Confirm with scheduler admins before this drives actual user contact.** Fine for
  channel testing now.

### S1 — `queue_drying` (system) — ⚠️ WORKS, NEEDS QUEUE CALIBRATION
High utilization + thin backlog on watched queue(s) = idle-capacity risk soon.
Mechanic verified (fired at 94% util). **BUT:** the default `queues: ["prod"]` is wrong
for Aurora — there is no load-bearing `prod` queue. Real Aurora running queues (latest
snapshot): `capacity` (27 running), `small` (15), `debug-scaling` (11), `nre-priority`
(11), `debug` (8), `large`. **Action for Taylor: set `slack.rules.queue_drying.queues`
to the real production queues per cluster, and tune `min_queued_jobs` / `high_util_pct`
against real backlog behavior.** This is the "what does *shallow* mean" calibration.

### S3 — `down_node_surge` (system) — ❌ CANNOT SHIP FROM system_snapshots
**Real-data finding:** `system_snapshots.available_nodes` counts *idle/free* nodes, not
*healthy* nodes. Latest snapshot: 666 available of 10,624 → the naive
`unavailable = total − available` = 9,958, which is **busy nodes, not down nodes**. This
rule as drafted would false-fire on any busy healthy machine. **Correct implementation
needs true per-node down/offline state, which lives in the `node_snapshots.snapshot_data`
TEXT blob and requires a parser first** (the diagnostics catalog already flagged this as
the least-ready data source). Rule is left in the registry but should stay disabled until
the node-state parser exists. Do NOT enable against system_snapshots.

## Remaining work (Phase B — when Taylor is back)

1. **Provide a Slack credential** (webhook URL or bot token) → set in a *local*
   `~/.pbs_monitor.yaml` (keep secrets out of git).
2. **Calibrate `queue_drying`** queue list + thresholds against real per-cluster topology.
3. **Notifier engine + cooldown/dedup** — edge-trigger (fire on entering a bad state, not
   every cycle) + per-rule cooldown + global `min_interval_seconds`. Start with a small
   JSON state file next to the DB; graduate to an `alert_events` table later.
4. **Daemon hook** — call `evaluate_all()` on the SUCCESS path of `collect_and_persist`,
   post any fired messages through the engine. One small, well-bounded insertion.
5. **`pbs-monitor notify test` CLI** — evaluate all rules against the live/DB data and
   print what *would* post (dry-run), for eyeballing before going live.
6. **Replace `down_node_surge`** with a node-state-parser-backed implementation, or drop it.

## Design notes / pitfalls captured

- `available_nodes` = FREE nodes, not HEALTHY nodes. Never infer down-nodes from it.
- Aurora has no `prod` queue; load lives in `capacity`/`large`/`small`/`debug*`. Queue
  lists are per-cluster config, not hardcoded.
- Rules are pure + isolated: `evaluate_all` catches per-rule exceptions so one bad rule
  can't block others or crash collection.
- Notifier swallows transport errors by design — notifications are best-effort.
