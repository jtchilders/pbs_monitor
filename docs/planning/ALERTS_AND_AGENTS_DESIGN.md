# Automated Alerts & Autonomous Agents — Design Brainstorm

**Status:** Brainstorm / design reference (not yet scheduled for implementation)
**Audience:** ALCF operations staff (Aurora/Polaris), the User Experience group, and PBS Monitor maintainers
**Last updated:** 2026-07-12

---

## 1. Purpose

PBS Monitor already collects ~13.5 months of rich scheduler data (jobs, job-state
history, per-queue and system snapshots, node snapshots, reservations, and the
collector's own self-health log). Today that data powers analytics dashboards but
drives **no automated action**. This document brainstorms two related capabilities
we can build on top of the existing data:

1. **Automated alerts** — detectors that watch the collected data and notify a human
   when something crosses a threshold.
2. **Autonomous agents** — the same detectors given *agency* to take (bounded,
   auditable) actions: draft outreach, file tickets, restart the collector, flag
   reservations for teardown, etc.

The intent is to help two audiences:

- **Operators** managing Aurora, Polaris, and other systems (machine health,
  utilization, contention, data-integrity of the monitor itself).
- **The User Experience group**, who may want to proactively reach out to a user who
  is struggling (chronic failures, repeated identical crashes, walltime misconfig).

---

## 2. Framing: one detector, three agency tiers

Every idea below is fundamentally a **detector** that reads existing tables. What
differs is what the system is allowed to *do* when a detector fires. We define three
agency tiers and tag each idea with the **highest tier we'd trust for a first cut**.

| Tier | Name | Behavior |
|------|------|----------|
| **0** | **Alert** | Notify a human. No action taken. |
| **1** | **Draft** | Compose the artifact (email, ticket, chat message) for a human to review and send with one click. |
| **2** | **Act** | Take a reversible, low-risk action autonomously, and write an audit record. |

**Hard requirements for any Tier 2 agent:**

- An **audit log** (`alert_events` table, see §6) recording every action, the
  triggering data, and the outcome.
- A **kill switch** (global + per-agent enable/disable in config).
- **Reversibility** — no destructive or irreversible action without a human.

**Cross-cutting requirement for every detector (all tiers):** a baseline +
hysteresis + a dedupe/cooldown window. Without this, operators will mute everything
within a week (alarm fatigue is the primary failure mode of this whole effort).

---

## 3. Data grounding

All numbers below are **real signals** derived from the ~13.5-month Aurora example DB
(`pbs_monitor_aurora.db`, 2025-04-21 → 2026-06-04). They are illustrative of scale;
**re-derive on the live DB before acting on any threshold.**

Relevant tables (see `references/db-schema.md` in the `pbs-monitor` skill for full schema):

- `jobs` (~458K rows) — terminal record per job. `outcome_class` populated post-T0;
  real exit code lives in `raw_pbs_data.Exit_status`.
- `job_history` (~1.04M rows) — state transitions + scheduler `score` trajectory.
- `queue_snapshots` (~309K) / `system_snapshots` (~6.3K) — depth/util over time.
- `node_snapshots` (~6.3K) — per-node-set TEXT blob (`snapshot_data`).
- `reservations` (259) / `reservation_utilization` (1,441) — reserved vs used node-hours.
- `data_collection_log` (~36K) — the collector's own health (status, duration, errors).

---

## 4. Operator-facing detectors

### A1. Collector self-health watchdog — **Tier 0/1 — DEFERRED until Hermes hosting exists**
**The monitor's own blind spot.** `data_collection_log` shows 22 gaps >60 min and one
95 h gap, plus 7 FAILED collections — and *nothing alerts on it today*. Silent gaps
corrupt trend plots (this is the root of the known "utilization reads low on 90 d"
artifact).

- **Detector:** the freshness question — has a new `SUCCESS` row appeared in Postgres
  within the expected cadence? Signals: stall (`now − last_success > 2 ×
  auto_persist_interval`), new `FAILED` rows, snapshot gap (`> 60 min AND > 2× median`,
  the existing `/api/analytics/collector-health` math), duration spike, silent-empty
  (SUCCESS row with `jobs_collected = 0`). Detection logic **already exists** inside the
  web endpoint `api_analytics_collector_health` (`web/server.py`) and should be extracted
  into a pure, reusable `analytics/collector_health.evaluate(db, config)` function.

- **CRITICAL architecture constraint — the watchdog must NOT run on the cluster it
  watches.** A watchdog co-located with the daemon shares its failure domain: if an
  Aurora login node reboots or its Postgres goes down, a same-node watchdog dies *with*
  the daemon and the outage that matters most goes unreported. A same-node watchdog is
  false comfort — worse than no watchdog. The observer must sit on the *other* side of
  the network boundary so that "daemon wedged," "Postgres down," and "login node gone"
  are all detectable (the last two become "connection refused," which is itself an alert).

- **Correct home: an external, stateless, scheduled job on Hermes (K8s).** It is a
  short-lived `SELECT max(timestamp)`-style freshness probe against each cluster's
  Postgres — a *query*, not a standing service. A **K8s `CronJob`** is the right
  primitive: K8s schedules a throwaway pod every ~5 min running
  `pbs-monitor watchdog --once --targets aurora,polaris`, then tears it down. K8s owns
  its lifecycle and restarts on failure — solving "who watches the watchdog" with the
  orchestrator we already want. This is a *soft, low-stakes first workload for Hermes*,
  achievable long before Hermes can serve the website (a freshness probe is trivial
  next to hosting the full app).

- **Dead-man's-switch (closes the silent-failure gap).** A watchdog that only speaks up
  on bad news fails silently if it *itself* stops (silence looks like health). Invert it:
  emit a positive heartbeat on every healthy run to an external dead-man's-switch
  (Healthchecks.io or self-hosted). Daemon dies → watchdog alerts; watchdog dies →
  heartbeat stops → dead-man's-switch alerts. One tiny external dependency, not a
  standing service per cluster.

- **Tier for Tier 2 (auto-restart daemon):** only viable if Hermes has a control path
  back to the cluster (e.g. SSH). Ship at **Tier 0 (notify only)** first; promote to
  auto-restart per-cluster only after burn-in, and only with idempotency + rate-limit
  (≤1 restart/hr) + verify-before/after + a maintenance flag so it never fights an
  operator who stopped the daemon on purpose.

- **Hard prerequisite (the real gate):** Hermes needs a network path to each cluster's
  `localhost:5432` Postgres (today reached only via SSH tunnels from Taylor's MacBook).
  Solving this is the same connectivity Hermes needs to eventually serve the website —
  so it is down-payment on the bigger goal, not throwaway work. **Until that path
  exists, do NOT build the watchdog** (see build order Phase B). The interim state —
  daemon self-logging plus eyes-on — is acceptable; a wrong-architecture watchdog is not.

### A2. Machine-utilization anomaly — **Tier 0/1**
Ground truth from `system_snapshots`: `(total_nodes − available_nodes) / total_nodes`.

- **Detector:** utilization below a floor for N consecutive prime-time snapshots
  (idle capacity = wasted allocation-hours), OR pinned ~100% with a deep queue (contention).
- **Tier 1 draft:** standup summary — "Aurora sat at 34% for 3 h overnight; largest
  queued job needed 2048 nodes; 4100 free."

### A3. Node-health / down-node tracker — **Tier 1**
Parse `node_snapshots.snapshot_data` for offline/down counts over time.

- **Detector:** down-node count crosses a threshold or climbs monotonically (rack
  going bad).
- **Tier 1 draft:** ticket listing the specific offline nodes.
- **Caveat:** `snapshot_data` is a TEXT blob — a parser must be built first. This is
  the **least-ready** data source in this document.

### A4. Fragmentation / large-job-starvation detector — **Tier 0/1**
Cross `system_snapshots` free-node count against the largest queued job's node request.

- **Detector:** a big job has waited > queue p95 AND free nodes never simultaneously
  reach its size → the scheduler cannot drain for it.
- **Value:** flags when operators may need to intervene (drain / reservation). A
  signal ops usually compute by hand.

### A5. Queue-wait SLA breach — **Tier 0**
Wait ECDF + p50/p90/p99 per queue already exist. Turn p90/p99 into a live tripwire.

- **Detector:** a queue's rolling p90 wait exceeds its historical baseline + kσ.
- **Value:** distinguishes "prod is congested" from "one whale job skews the tail."

### A6. System-wide failure surge — **Tier 0/1**
30.8% of finished jobs carry a non-zero exit (`outcome_class` now available). A sudden
surge in a *specific* class — `134/139/137` (SIGABRT/SEGV/SIGKILL = real crashes) or
`127` (cmd-not-found) — clustered in time often signals a **filesystem hiccup, a bad
module, or a compute-node problem**, not user error.

- **Detector:** rate of a specific failure class over baseline + kσ within a window.
- **Tier 1 draft:** ops alert with exit-class breakdown and affected node list.
- **Why notable:** turns failure data into an **early-warning system for machine problems**.

### A7. Reservation-expiry-with-low-utilization — **Tier 1/2**
Mean reservation utilization is 24.8%; 65% run under 25%.

- **Detector:** reservation nearing end with node-hours-used far below reserved.
- **Tier 1 draft:** nudge the owner — "your reservation ends in 4 h at 18% use —
  release early?"
- **Tier 2 (policy sign-off required):** auto-flag for early teardown.
- **Value:** directly recovers wasted capacity.

---

## 5. User-Experience-group-facing detectors (proactive outreach)

**All capped at Tier 1 (draft-for-approval) for a first cut.** User-facing messages are
reputational, and exit-code semantics are site-specific — a `-29` / `271` is usually a
**benign requeue/preempt**, not the user's fault. Misfiring erodes trust and causes
alarm fatigue. **Confirm exact code semantics with the scheduler admins before any
user-facing text ships.**

### U1. Chronic-failure user — **Tier 1**
Several users have 200+ jobs at 88–99.5% non-zero-exit rate.

- **Detector:** ≥ N jobs in a window with fail-rate > X, **excluding requeue/preempt codes**.
- **Draft:** warm, specific email — "we noticed most of your recent Aurora jobs are
  exiting with SIGSEGV — want help?"

### U2. Repeated-identical-crash — **Tier 1**
Same `owner` + `job_name` + exit code ≥ K times = a user stuck in a loop burning
allocation on the same broken run.

- **Detector:** grouped count over the signature.
- **Value:** **highest-signal outreach trigger** — the pattern is unambiguous. Draft
  includes the repeating signature and job IDs.

### U3. Chronic walltime over-requester — **Tier 1**
77,380 jobs requested ≥ 1 h but ran < 5 min; median job uses 40% of requested walltime.
Over-requesting hurts the user's own queue priority and the scheduler's backfill.

- **Detector:** rolling median walltime-usage fraction < 10% over ≥ M jobs.
- **Draft:** gentle right-sizing tip with their actual distribution.
- **Note:** use measured `resources_used.walltime` / `occupied_seconds`, **not**
  `end − start` (that field overstates for requeued jobs — see the occupancy fix).

### U4. Walltime-kill repeat offender — **Tier 1**
Mirror image of U3: 15.3% of jobs use > 95% of walltime; those repeatedly hitting exit
`143` (SIGTERM/walltime-kill) are losing work at the finish line.

- **Draft:** suggest requesting more time or checkpointing.

### U5. New-user / first-jobs-failing — **Tier 1**
A user's *first* handful of jobs all failing is the highest-leverage UX moment — catch
them before they give up.

- **Detector:** owner with < M lifetime jobs and a failing opening streak.
- **Draft:** warmer, onboarding-flavored template (distinct from the chronic-offender one).

### U6. Stuck-job nudge — **Tier 0/1**
A job queued/held far beyond its queue's p95 wait with a low/flat scheduler `score`
(available in `job_history`). Often a dependency, a forgotten hold, or an unsatisfiable
resource request.

- **Draft:** nudge, or flag for UX triage.

---

## 6. Self-service (no outreach; user pulls)

### S1. Per-job post-mortem panel — **Tier 0 (UI feature)**
"Why did my job fail?" — plain-English exit-code translation + walltime-usage + wait
context on the existing user page. Deflects UX tickets entirely.

### S2. Per-user / per-project health-score badge — **Tier 0 (UI feature)**
Composite badge (fail-rate, walltime efficiency, wait exposure) on the user/project
pages. Self-serve for PIs; free triage-priority list for the UX team.

---

## 7. Shared infrastructure (build once, reuse everywhere)

Rather than N one-off cron jobs, build a single engine that all detectors plug into:

- **`pbs-monitor alerts` CLI subcommand** — run/evaluate detectors, list recent events,
  test a rule against historical data (dry-run).
- **`alert_rules` config** — declarative rule definitions (detector type, thresholds,
  baseline window, hysteresis, cooldown, agency tier, notification target).
- **`alert_events` audit table** — one row per firing: rule id, timestamp, triggering
  data snapshot, agency tier, action taken, outcome, human-ack status. **Mandatory for
  every Tier 2 action.**
- **Notification adapters** — chat (ops channel), email (UX drafts), ticket system.
- **Global + per-rule kill switch.**

---

## 8. Recommended build order (impact × readiness)

Sequencing is now split into two phases by their infrastructure dependency, to avoid
**service creep** (see §9). The floor per cluster is **two** standing services —
Postgres + the collector daemon (the web server is on-demand, not standing). No detector
below should add a *third* standing service to a cluster.

### Phase A — buildable now (no new cluster services; runs in-app or as one shared job)
1. **Shared alert engine** (§7) — the substrate everything else needs. Detectors that
   run *inside* the existing app/daemon or as a single scheduled job add no per-cluster
   service.
2. **A6 — System-wide failure-surge detector** (Tier 0→1). `outcome_class` backfill
   already unlocked this; it bridges the alert half and the machine-problem-detection
   half. Runs against the local DB — no new service, no external reachability needed.
3. **U2 — Repeated-identical-crash** (Tier 1). Least ambiguous outreach trigger; good
   first user-facing agent.
4. Then broaden: A2/A4/A5 (operator situational awareness), U1/U3 (outreach), S1/S2 (UI).

### Phase B — gated on Hermes (K8s) hosting + network path to cluster Postgres
5. **A1 — Collector self-health watchdog** — DEFERRED here **by design decision
   (2026-07-12)**, not oversight. It cannot be built correctly on-cluster (shared
   failure domain, §4/A1) and requires an *external* observer with a network path to the
   cluster Postgres instances — the same connectivity Hermes needs to serve the website.
   Build it as Hermes's first low-stakes K8s `CronJob` workload once that path exists.
   In the interim, rely on daemon self-logging + eyes-on; do **not** ship a same-node
   watchdog as a stopgap (false comfort).

**Prep work that can happen during Phase A** (no dependency, reduces Phase B risk):
extract the collector-health detection out of `web/server.py` into a pure
`analytics/collector_health.evaluate()` function with unit tests. Safe refactor, no
behavior change, and it makes the watchdog a thin scheduler around an already-tested core.

---

## 9. Caveats & risks

- **Exit-code semantics are site-specific.** Confirm with Aurora/Polaris scheduler
  admins which codes are benign requeues (`-29`, `271`) vs. real faults before any
  user-facing text ships. Getting this wrong causes alarm fatigue and bad outreach.
- **Node-health (A3)** is the least-ready idea — the data is in a TEXT blob needing a parser.
- **Alarm fatigue is the real enemy.** Every detector needs a baseline + hysteresis +
  dedupe/cooldown, or all of it gets muted.
- **`actual_runtime_seconds` overstates runtime** for requeued/preempted jobs
  (`end − start` brackets idle time). Use measured `occupied_seconds` /
  `resources_used.walltime` for any walltime-based detector.
- **Tier 2 requires audit + kill switch + reversibility**, no exceptions.
- **Guard against service creep.** The per-cluster floor is two standing services
  (Postgres + collector daemon); the web server is on-demand. Prefer detectors that run
  *inside* the existing daemon/app or as a *single* shared scheduled job over anything
  that adds a standing per-cluster service. A monitoring component whose whole purpose is
  to detect that a cluster-local thing died must NOT live on that cluster — it shares the
  failure domain and reports nothing at the moment it matters most. Cross-boundary
  observation (from Hermes) or a dead-man's-switch is the only honest design for
  liveness monitoring.
