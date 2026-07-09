# Analytics Page Reorganization — Technical Implementation Plan

**Author:** Wesley (AI assistant), with Taylor (jtchil0)
**Date:** 2026-07-08
**Status:** Ready for sub-agent hand-off
**Repo:** `~/workspaces/pbs_monitor/repo` · web layer: `pbs_monitor/web/`

---

## 1. Motivation & Decisions

The front page (`index.html`) is the **real-time** view (node map, live running/queued/held tables, current queue depth, current wait distribution, reservations). It stays real-time — **out of scope for this plan except where noted in §8**.

The analytics page (`analytics.html`) is the **historical deep-dive**. Today it renders 3 charts eagerly on mount (utilization-over-time, queue-depth backlog, wait-vs-score scatter). Problems: (a) the scatter is too busy to be useful; (b) adding the diagnostics from `DIAGNOSTICS_ANALYSIS.md` would turn one long page into a dumping ground; (c) eager loading everything is slow.

**Decisions (locked with Taylor):**
1. **Navigation:** Tabs with **lazy per-tab loading**. Only the active tab fetches; loaded tabs are cached in memory for the session. One theme visible at a time.
2. **Scatter:** **Cut entirely.** Remove the `wait-vs-score` endpoint + client code.
3. **Grouping:** **Unify to `queue | allocation_type | project`** across every applicable tab (today it's only `queue | allocation_type`).
4. **No dumping ground:** each tab is a coherent *theme* (2–3 related plots), not one-plot-per-idea.

---

## 2. Target Architecture

### 2.1 Tab taxonomy

| Tab | Plots | Backend endpoints | Priority |
|---|---|---|---|
| **Trends** (default) | Utilization % over time; Queue-depth backlog (system-hours) | existing `utilization`, `queue-depth` | P0 (migrate) |
| **Job Outcomes** | Failure-rate over time (stacked by outcome class); Exit-status taxonomy (bar) | new `job-outcomes`, `exit-taxonomy` | P1 |
| **Walltime Accuracy** | Requested-vs-used 2D histogram; efficiency scorecard table | new `walltime-histogram`; reuse `WalltimeEfficiencyAnalyzer` | P1 |
| **Wait Times** | Wait-time ECDF per queue; wait-tail (p50/p90/p99) over time | new `wait-ecdf`, `wait-percentiles` | P2 |
| **Reservations** | Utilization timeline; wasted-node-hours ranking | reuse `ReservationUtilizationAnalyzer` + new time-series wrapper | P2 |
| **Collector Health** | Collection cadence/gap chart; completeness summary | new `collector-health` | P1 (protects all other tabs) |

The **completeness banner** (a one-line "⚠ N collection gaps in this range" strip) renders **on every tab**, not just Collector Health — it is fed by a lightweight `collector-health` summary call made once per range change (see §5.6).

### 2.2 Shared controls bar (unchanged position, one addition)

Existing controls (`analytics.html` lines 28–93) are reused verbatim except:
- **Group by** button group gains a third option: `Project` → `<button ... @click="groupBy='project'; reload()">Project</button>`.
- Add a **Tab bar** above the controls (below the header). Style-match the front page's existing `.tab-bar`/`.tab-btn` classes (index.html lines 112–125) for visual consistency.

Range (7/30/90/365d), Bin (auto/h/d/w), and Filters (include/exclude on queue/owner/project/allocation_type) are **global** — changing any of them invalidates all tabs' in-memory cache and reloads the active tab.

---

## 3. Formatting & Color Conventions (MANDATORY — read before writing any chart)

These already exist in `static/js/analytics.js`. Every new plot MUST reuse them; do NOT introduce a second palette or a second dark-theme axis config.

- **Palette + stable colors:** `ANALYTICS_PALETTE` (20 colors) + `colorFor(groupName)` global registry (analytics.js lines 5–23). Same queue/alloc/project name → same color across every chart and every tab. Pre-register groups in one sorted pass (see analytics.js lines 220–221) before rendering so colors are deterministic.
- **Line chart options:** `_commonLineOpts(yLabel, stacked)` (lines 266–309) — dark axes (`#94a3b8` ticks, `#2d3748` grid), bottom legend, index-mode tooltip that shows top-5 series + total. Reuse for all time-series line/area charts.
- **Bin label formatting:** `fmtBin(iso, freq)` (lines 25–32).
- **Reservation collapsing:** `collapseResvGroups()` (lines 34–62) when grouping by queue and reservation-queues (R#####/M#####) appear.
- **Categorical (non-group) colors** for fixed classes (e.g. outcome classes success/signal-killed/walltime-killed/requeued/could-not-run): define ONE new constant `OUTCOME_COLORS = { success:'#10b981', signal_killed:'#ef4444', walltime_killed:'#f59e0b', requeued:'#8b5cf6', could_not_run:'#6b7280', other:'#94a3b8' }` in analytics.js. Green=good, red/amber=bad, grey=neutral/unknown. Keep this semantic mapping stable across the whole app.
- **Chart lib:** Chart.js 4 (already loaded via CDN in both HTML files). No new chart library. 2D histogram = Chart.js `matrix` controller **only if** we add the `chartjs-chart-matrix` plugin (one CDN `<script>`); otherwise render the histogram as a stacked/heatmap-styled bar. Prefer the matrix plugin — cleaner, one line of CDN.
- **Numbers:** node-hours/system-hours rounded to 2 dp; percentages to 1 dp; counts with `toLocaleString()`.

---

## 4. Performance Model (the constraint that shapes everything)

The existing cache is the backbone; every new endpoint MUST follow the same pattern or the page will be slow.

### 4.1 Caching rules (from `analytics_cache.py` + server.py utilization endpoint lines 1244–1342)
1. Build a `cache_key` from `AnalyticsCache.make_key({...})` including: endpoint name, `freq`, `window_start.isoformat()`, `last_complete.isoformat()`, `group_by`, and all sorted filter lists.
2. **Only cache complete bins.** `window_start = _floor_bin(now - days, freq)`, `last_complete = _floor_bin(now, freq)`. Query `< last_complete` so the current incomplete bin is excluded. This makes past results immutable → cache never goes stale, no TTL.
3. On cache miss, run heavy compute inside `await asyncio.get_event_loop().run_in_executor(None, _compute)`, then `_analytics_cache.set(key, result)`.
4. Return the same JSON shape `{freq, group_by, groups, bins, series, ...}` so the client renderers are reusable.

### 4.2 Lazy loading (client)
- Do NOT fetch on mount for all tabs. On mount, fetch only the **default tab (Trends)** + the lightweight completeness banner.
- Maintain `const tabData = reactive({})` keyed by `tabName + '|' + buildParams()`. When a tab is activated, if `tabData[key]` exists, re-render from memory; else fetch.
- Changing any global control (range/bin/group/filters) clears `tabData` and reloads the active tab only.
- Keep chart instances per tab; `destroy()` on tab switch to avoid Chart.js canvas leaks (existing code already destroys before re-render — follow that).

### 4.3 Payload-size guards (new plots can return a LOT of points)
- **2D histogram / ECDF:** bin server-side. Never ship raw per-job arrays to the browser. The scatter's per-point payload is exactly what we're removing; do not reintroduce it elsewhere.
- **Exit-status taxonomy:** aggregate to counts per class server-side.
- **Failure-rate stacked area:** same bin structure as utilization; one value per (class, bin).
- Cap group cardinality: when grouping by `project`/`owner` there can be hundreds; server should return top-N by volume + an aggregated `other` bucket (mirror the front page's `other` bucket pattern, index.html queue-depth).

### 4.4 Backfill dependency (blocks Job Outcomes tab)
`Exit_status` currently lives only in `jobs.raw_pbs_data` JSON; the typed `jobs.exit_status` column exists but is unpopulated. Building the Job Outcomes tab on live JSON parsing of 458k rows per request is too slow even with caching on cold bins.
**Required precursor task (T0):** a migration + backfill that (a) populates `jobs.exit_status` from `raw_pbs_data.Exit_status`, and (b) adds a computed `outcome_class` VARCHAR column (`success | signal_killed | walltime_killed | requeued | could_not_run | error | unknown`) with an index on `(outcome_class)` and `(end_time, outcome_class)`. New collections must fill both columns going forward (update `data_collector.py`). See §6 for the class-mapping rules.

---

## 5. Endpoint Specifications

All new endpoints live in `web/server.py`, mirror the utilization endpoint's structure (filters, cache, executor), accept the same filter params, and add `group_by` support for `queue|allocation_type|project`.

### 5.1 `GET /api/analytics/job-outcomes`
Params: `days, freq, group_by, <filters>`.
Compute: bin finished jobs by `end_time` into the same bin grid; per bin, count jobs per `outcome_class`. Return stacked series keyed by outcome class.
Return: `{freq, bins, classes:[...], series:{class:[counts...]}, total}`.
Client: stacked area using `OUTCOME_COLORS`; optional toggle "rate %" vs "count".

### 5.2 `GET /api/analytics/exit-taxonomy`
Params: `days, <filters>` (no time binning — it's a distribution over the window).
Compute: count finished jobs per `outcome_class` and per raw `exit_status` (top 20 codes + `other`), with human labels (signal name, PBS-special note).
Return: `{classes:{class:count}, codes:[{code, count, label}]}`.
Client: horizontal bar, colored by `OUTCOME_COLORS[class]`.

### 5.3 `GET /api/analytics/walltime-histogram`
Params: `days, group_by, <filters>`.
Compute: for finished jobs with parseable `HH:MM:SS` walltime and `actual_runtime_seconds>0`, compute `used_fraction = runtime/requested`. **Bin server-side** into a 2D grid: x = requested-walltime buckets (log), y = used-fraction buckets (e.g. 0–10,10–25,…,>100%). Return counts per cell.
Return: `{x_edges:[...], y_edges:[...], cells:[{x,y,count}], median_used_fraction, pct_under_25, pct_over_95}`.
Client: Chart.js matrix heatmap; annotate the diagonal (perfect estimate) and the >95% band (walltime-kill risk).

### 5.4 `GET /api/analytics/wait-ecdf`
Params: `days, group_by, <filters>`.
Compute: per group, ECDF of `queue_time_seconds` **downsampled server-side** to ~200 (x=wait_hours, y=cumulative_fraction) points via quantile sampling. Never ship raw waits.
Return: `{groups:[...], curves:{group:[[wait_h, frac], ...]}}`.
Client: multi-line, `colorFor(group)`, x in hours (log optional), y 0–1.

### 5.5 `GET /api/analytics/wait-percentiles`
Params: `days, freq, group_by, <filters>`.
Compute: per bin per group, p50/p90/p99 of `queue_time_seconds` for jobs that STARTED in that bin.
Return: `{freq, bins, groups, series:{group:{p50:[...],p90:[...],p99:[...]}}}`.
Client: line chart; default show p90, toggles for p50/p99. This is the "queue degrading" early-warning view.

### 5.6 `GET /api/analytics/collector-health`
Params: `days` (+ optional `summary=1`).
Compute: from `data_collection_log` + `system_snapshots` timestamps — collection cadence, gaps > 2× median, FAILED events, and last-successful-collection age. `summary=1` returns only `{gap_count, max_gap_min, last_success_age_min, failed_count}` for the banner (cheap, uncached-OK).
Return (full): `{cadence:[{t, gap_min}], gaps:[...], failures:[...], median_gap_min}`.
Client (tab): step/scatter of gap sizes over time with >60min flagged red. Client (banner): one-line strip on all tabs when `gap_count>0` or `last_success_age_min` exceeds threshold.

### 5.7 Reservations tab
Reuse `ReservationUtilizationAnalyzer` / `ReservationTrendAnalyzer` (already power the CLI). Add a thin `GET /api/analytics/reservation-utilization-timeline` that returns per-reservation reserved-vs-used node-hours over time + a `wasted_node_hours = reserved - used` ranking. Cache by window.

### 5.8 Removals
- Delete `GET /api/analytics/wait-vs-score` (server.py ~line 1474) and its `_compute`.
- Delete `reloadScatter`, `renderScatter`, `scatterCanvas`, `xAxis` state, and the scatter `<section>` (analytics.html lines 122–135) from the client.

---

## 6. Outcome-class mapping (single source of truth)

Implement once (server util `classify_exit(state, exit_status)`), reuse in backfill + live collection + endpoints. **Confirm negative/271 semantics with ALCF PBS admins before shipping user-facing labels** (see §9).

```
state != FINISHED and state in (Q,R,H)     -> not counted (still active)
exit_status == 0                           -> success
exit_status is None                        -> unknown
exit_status == 271 (256+15)                -> requeued            # provisional: may be preemption/maintenance
exit_status < 0                            -> could_not_run       # PBS special (e.g. -29, -20, -3); provisional
128 < exit_status < 192                    -> signal_killed       # 143=SIGTERM(walltime?), 137=SIGKILL, 139=SIGSEGV...
exit_status == 143 AND ran >= 0.95*req_wt  -> walltime_killed     # refine SIGTERM-near-limit as walltime kill
otherwise (1,2,127,255,...)                -> error
```

Grounding numbers from the Aurora DB (2025-04 → 2026-06, 420,258 classified finished jobs): success 69.2%, could_not_run(-29) 12.6%, requeued(271) 5.4%, signal_killed(143) 4.6%, error(1) 3.5%. **≈31% non-zero** — the headline the Job Outcomes tab surfaces.

---

## 7. Sub-agent Work Breakdown (parallelizable)

Dependencies: **T0 blocks A**. B/C/D/E are independent of each other and of A once the shared scaffold (S) lands. **S must land first** (it defines the tab framework every other task plugs into).

- **T0 — Exit-status backfill (BLOCKER for A):** migration adds `outcome_class` col + indexes; backfill from `raw_pbs_data`; wire `data_collector.py` to populate on new collections; implement `classify_exit()`. Include a `pbs-monitor database` backup reminder (per CLAUDE.md). Tests for the classifier.
- **S — Tab scaffold + shared controls (BLOCKER for A–E):** convert `analytics.html`/`analytics.js` to tabbed lazy-loading; add `project` to group-by; add completeness banner wiring; migrate the two existing Trends charts into the Trends tab unchanged; delete scatter (§5.8). Deliver the `tabData` cache pattern and `OUTCOME_COLORS` constant.
- **A — Job Outcomes tab:** endpoints 5.1, 5.2; stacked-area + taxonomy-bar renderers. Depends on T0 + S.
- **B — Walltime Accuracy tab:** endpoint 5.3 (+ matrix plugin CDN); histogram renderer + efficiency scorecard (reuse `WalltimeEfficiencyAnalyzer`). Depends on S.
- **C — Wait Times tab:** endpoints 5.4, 5.5; ECDF + percentile-trend renderers. Depends on S.
- **D — Reservations tab:** endpoint 5.7 (reuse analyzers); timeline + wasted-hours ranking. Depends on S.
- **E — Collector Health tab + banner:** endpoint 5.6; gap chart + banner. Depends on S. (Banner is high value — protects integrity of A–D.)

Recommended order: **T0 + S in parallel first**, then **E** (cheap, protective) and **A** (highest value), then **B/C/D**.

---

## 8. Cross-cutting requirements

- **Consistency with front page:** reuse `.tab-bar`/`.tab-btn`, `.queue-badge`, `queueColor()` semantics, `fmtDuration`, `fmtSysHours`. A queue's color on the front page and analytics should match — both derive from the same palette; verify `colorFor` and the front page's `queueColor` produce identical assignments, or unify them into one shared `colors.js` imported by both pages (recommended small refactor).
- **Grouping parity:** front-page queue-depth already has queue/allocation/project; analytics now matches.
- **Filters:** all new endpoints honor the existing include/exclude filter params via the shared `_apply_job_filters` helper (server.py).
- **Empty/edge states:** every tab shows a clean "no data in range" state (mirror existing `analytics-loading`/`analytics-error`).
- **Tests:** each new endpoint gets a unit test with a small fixture DB; classifier gets its own tests. Run `pytest` (note: system python3 is 3.9 and can't import the package due to `X|Y` syntax — use the repo `.venv`).
- **No regressions to real-time front page** beyond the optional shared `colors.js` refactor.

---

## 9. Caveats / must-confirm before shipping (scientific honesty)

1. **Exit-code semantics are site-specific.** `-29`/`-20`/`271` are mapped provisionally (could_not_run / requeued). Confirm with ALCF/Aurora PBS admins whether these are user failures, preemptions, or maintenance requeues. Mislabeling will cause alarm fatigue and is worse than omitting the class. The Job Outcomes tab MUST visually separate "system/requeue" from "user-fault" outcomes.
2. **271 and negatives may be benign.** Until confirmed, label them neutrally ("requeued", "could not run") — not "failed".
3. **Walltime parsing** assumes `HH:MM:SS`; other formats are excluded (small fraction). Document the excluded count in the histogram meta.
4. **Reservation utilization** depends on `analysis_method` (varies); validate before publishing "65% under-utilized" externally.
5. **Backfill is a one-time heavy op** on 458k rows — run off-peak, back up first.

---

## 10. Out of scope (this plan)
- The alerting/outreach triggers from `DIAGNOSTICS_ANALYSIS.md` §4 (separate plan; they consume the same endpoints once built).
- Node/system health plots (down-node timeline) — candidate for a future "System Health" tab.
- Any change to collection cadence or the daemon.
