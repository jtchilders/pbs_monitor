# Usage Insights Plotting Plan

## Objectives
- Provide clear visuals that explain scheduling behavior and user experience
- Center analysis around the job score at run start and Aurora user queues
- Respect scheduling mechanics: users request whole nodes and walltime (not CPUs/GPUs/Memory)
- Offer heuristics to surface likely score boosts despite no explicit flag in the DB

## Context
- Scheduling is based on requested nodes and requested walltime, with entire nodes allocated per job.
- Some jobs receive manual score increases ("boosts") prior to start; these are not explicitly flagged in the DB.
- Use Aurora Running Jobs queue taxonomy to facet results and comparisons.

### Queue selection strategy (portable and noise‑resistant)
- Derive the queue list dynamically from PBS (e.g., equivalent of querying `qstat -Q`), not hard‑coded, to keep the system portable.
- Apply an inclusion filter so plots focus on meaningful queues:
  - Configurable node‑hours threshold within the selected time window (default options to consider: ≥ 1% of total node‑hours in window OR ≥ 100 node‑hours over 30 days).
  - Optionally cap at top N queues by node‑hours and provide an allowlist to always include specific queues if desired.

## Derived Metrics
- requested_node_hours = nodes_requested × walltime_hours
- wait_time = start_time − submit_time
- run_time = end_time − start_time (if available)
- slowdown = (wait_time + run_time) / max(run_time, ε)
- start_score_quantile = start score normalized within queue and rolling time window

## Proposed Plots

### Score‑centric
- Score at start vs wait time (hexbin/scatter; log axes); by queue
- Score at start vs requested_node_hours (hexbin; log x); by queue
- Score at start vs walltime and vs nodes (small multiples)
- Start‑score distribution by queue (violin/box + medians)
- Rolling median start‑score over time (per queue)

### Queue dynamics and performance
- ECDF of wait_time by queue
- Wait time vs walltime (hexbin; log axes)
- Wait time vs nodes (hexbin)
- Slowdown vs requested_node_hours (hexbin)
- Throughput over time (stacked area of node‑hours started per day); by queue
- Backlog over time (node‑hours queued); by queue

### Capacity and utilization
- Active nodes over time by queue (stacked area)
- Calendar heatmap of node‑hours started per day (seasonality)
- If available: runtime/requested_walltime ratio (violin) to quantify padding; by queue/user

### User/project fairness and behavior
- User share of demand vs share of starts (node‑hours; bubble or slope chart)
- Median wait_time vs average requested_node_hours per user (scatter)
- Per‑user start‑score distributions (box)
- User slowdown distributions (violin)

### Boost detection heuristics (no explicit flag)
- Low‑score early starts: jobs starting with bottom‑decile start_score_quantile while higher‑scored, similar‑size jobs were waiting at the same time
- Score jump near start: if score history exists, flag large deltas shortly before start
- Expected vs observed start_score: model start_score as a function of queue, nodes, walltime, time‑of‑day/week; highlight negative residuals at start
- Visuals: time series of "suspect starts" counts; scatter of residual vs requested_node_hours; table of top users/projects by suspected boosts

Data constraints and forward plan:
- Current job histories may not be robust enough for reliable boost detection; treat all detections as "suspected" with clear disclaimers.
- Future (requires separate approval): add non‑intrusive score change auditing (e.g., periodic ready‑queue snapshots or an append‑only audit log) to improve recall/precision without changing existing job tables.

### Scheduling/backfill proxies
- Feasibility band: wait_time vs walltime with bands indicating typical backfill windows (estimated from historical gaps)
- Job size spectrum at start: histograms of nodes at start by queue and hour‑of‑day

### Reliability and hygiene
- Start failure/early termination rates (if available) by queue and requested size
- Over/underestimation of walltime: runtime/requested_walltime ratio distributions; correlate with wait_time

### Operational lenses
- Rolling 7‑day medians for wait_time, slowdown, start_score per queue
- Event overlays: maintenance windows, allocation changes, major paper deadlines

## UX and Presentation
- Filters: time range, queue, user/project, size buckets (nodes, walltime), log/linear toggles
- Defaults: hexbin for dense scatters; log scales for heavy‑tailed variables (wait_time, node_hours)
- Normalize start_score within queue/time window to compare apples‑to‑apples
- Exportable CSV tables for suspected boosts and per‑user summaries

## Data Requirements
Minimum:
- job_id, user/project, queue, submit_time, start_time, end_time (if available)
- nodes_requested, walltime_requested, score_at_start

Nice‑to‑have:
- Periodic ready‑queue snapshots (waiting jobs with scores)
- Score history per job near start time
- Node availability over time

## Implementation Notes (no code yet)
- Place plotting logic in `pbs_monitor/analytics/` as new modules, e.g., `usage_insights.py` and helpers shared with existing analytics.
- Provide both CLI entry points and notebook‑friendly functions.
- Start with queue‑faceted hexbin plots and time‑series; add fairness and heuristics once validated.
- Queue filtering parameters: expose time window, node‑hours threshold, and optional top‑N/allowlist via CLI and config.

## Milestones
1) Foundations: derived metrics functions; initial plots (score vs wait, ECDF by queue, rolling start‑score)
2) Capacity and fairness: throughput/backlog, active nodes, per‑user distributions
3) Heuristics: suspected boost detection visuals and review tables
4) Fit‑and‑finish: UX polish, documentation, and performance tuning


