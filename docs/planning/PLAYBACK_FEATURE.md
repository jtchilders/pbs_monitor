# Playback Feature Implementation Plan

## Objective
- Provide a historical "playback" mode showing how the machine was occupied during a user-specified time range.
- Focus exclusively on jobs that were actively running in each timestep; queued/held jobs are excluded.
- Present each timestep with a timestamp header, visual node-occupancy bar, and table of running jobs.
- Derive results directly from the `jobs` table using `start_time` and `end_time` to avoid JobHistory reliability issues.

## Primary Deliverables
- New CLI command `pbs-monitor playback` for console-based playback.
- Core playback engine that iterates through timesteps and gathers running jobs.
- Occupancy bar renderer plus configurable job table output.
- On-the-fly score calculation per running job using existing score logic.

## Key Requirements
- **Time Range**: user supplies `--start-time` and `--end-time` (ideally timezone-aware; default to UTC if unspecified).
- **Time Step**: user supplies `--time-step` (default from config). Interpret as `DD:HH:MM` or `HH:MM:SS` for flexibility.
- **Playback Speed**: `--speed` seconds per rendered timestep (default from config).
- **Columns**: default to `job_id,owner,project,allocation,nodes,score_at_runtime,walltime_actual`; allow override via `--columns`.
- **Data Source**: `jobs` table; query by overlap condition `start_time <= tick < end_time`.
- **Total Nodes**: derive via PBS (`pbsnodes`) when available; provide fallback (config value or CLI override) when PBS unreachable.
- **Output Medium**: console only for MVP, using existing Rich-based table formatting.

## Architecture Overview
1. **PlaybackCommand (CLI Layer)**
   - Parse arguments, validate time range and step, apply config overrides.
   - Instantiate `DataCollector`; ensure DB connectivity; enforce requirement that database be enabled.
   - Construct and drive a `PlaybackEngine`, handling Ctrl+C gracefully.

2. **PlaybackEngine (Service Layer)**
   - Parse timestep strings into `timedelta`.
   - Generate chronological ticks between start and end times.
   - Query running jobs for each tick via new repository method.
   - Calculate occupancy percentage using total node count (cached per run).
   - Compute `score_at_runtime` for each job (one-time per job, cached).
   - Return structured snapshot payload for rendering.

3. **Display Helpers (Presentation Layer)**
   - Occupancy bar builder (ASCII/Rich) with configurable width and graceful fallback.
   - Job table formatter leveraging existing formatting utilities and configurable columns.

4. **Data Layer Enhancements**
   - New `PlaybackRepository` exposing `get_running_jobs_between(tick_start, tick_end)` or equivalent.
   - Confirm/index `jobs` table for `(state, start_time, end_time)` access pattern.
   - Ensure repository returns ORM models convertible to display layer utilities.

5. **Configuration**
   - Extend `Config` with `PlaybackConfig` (defaults for timestep, speed, bar width, max playback span, etc.).
   - Allow CLI overrides to supersede config values.

## Implementation Plan

### Phase 1 – Configuration & Data Access
- **Config (`pbs_monitor/config.py`)**
  - Add `PlaybackConfig` dataclass with defaults: `default_time_step`, `default_speed`, `default_bar_width`, `max_time_span_days`, etc.
  - Embed `PlaybackConfig` into global `Config`.

- **Database (`pbs_monitor/database/repositories.py`)**
  - Introduce `PlaybackRepository` (or extend existing repository) with method to fetch jobs overlapping a time window.
  - Add/verify SQLAlchemy indexes in `database/models.py` for `(state, start_time)` and `(start_time, end_time)` (if absent).
  - Ensure method filters to states representing jobs that reached or passed RUNNING (e.g., RUNNING, COMPLETED, FINISHED, EXITING, UNKNOWN_END) and excludes missing timestamps.

### Phase 2 – Playback Engine & Utilities
- **Playback Module (`pbs_monitor/playback/engine.py` or similar new module)**
  - Implement `PlaybackEngine` encapsulating:
    - `parse_time_step(step_str) -> timedelta` (support `DD:HH:MM` and `HH:MM:SS`, reject invalid strings).
    - `generate_ticks(start, end, step)` iterator (inclusive start, exclusive end).
    - `resolve_total_nodes()` using `DataCollector.get_nodes()` (count nodes) with caching and fallback.
    - `fetch_running_jobs(tick)` calling repository to get overlapping jobs.
    - `annotate_score(job)` using `PBSCommands.calculate_job_score`; results cached per `job_id`.
    - `compute_occupancy(jobs)` returning percentage and raw node count.
    - `build_snapshot(tick)` returning dict with time, occupancy, job list (with computed score/walltime).
  - Handle exceptions (database unavailable, PBS errors) with informative logs/messages.

- **Display Helpers (`pbs_monitor/playback/display.py`)**
  - `render_header(tick)` prints timestamp and summary.
  - `render_occupancy_bar(percent, width)` builds visual bar using blocks/spaces; degrade to text percentage when width too small or terminal lacks support.
  - `render_jobs_table(jobs, columns)` produces Rich table via existing formatters; support column validation and fallbacks.

### Phase 3 – CLI Integration
- **CLI Parser Updates (`pbs_monitor/cli/main.py`)**
  - Register `playback` subcommand with arguments:
    - `--start-time`, `--end-time` (required, formatted as `YYYY-MM-DD HH:MM[:SS]` or ISO 8601).
    - `--time-step` (default from config, formats as above).
    - `--speed` (float seconds per tick, default from config).
    - `--columns` (comma-separated list, optional).
    - `--bar-width` (int, optional, default from config).
    - (Optional future) `--total-nodes` override; note here for follow-up decision.

- **PlaybackCommand (`pbs_monitor/cli/commands.py`)**
  - Implement command class derived from `BaseCommand`:
    - Validate datetimes and step (start < end, step > 0, duration <= config.max span).
    - Instantiate `PlaybackEngine`; fetch total nodes once.
    - Iterate ticks: optionally clear screen, print header, occupancy bar, job table; if no jobs running, display friendly message.
    - Sleep for `speed` seconds unless `speed == 0` (render static output and exit).
    - Catch `KeyboardInterrupt` for graceful exit.

### Phase 4 – Testing & Documentation
- **Unit Tests**
  - `tests/test_playback_engine.py`: timestep parsing, occupancy calculation, score caching logic.
  - `tests/test_playback_repository.py`: job overlap queries (various edge cases: open end_time, short runs, boundary conditions).
  - `tests/test_playback_display.py`: occupancy bar rendering for multiple percentages/widths.

- **CLI Tests**
  - `tests/test_cli_playback.py`: argument parsing, validation errors, stubbed playback loop execution.

- **Documentation**
  - Update CLI help text and add usage examples to docs (README or dedicated section).
  - Document config keys in sample config file if available.

## Risk & Mitigation
- **Large Job Counts per Timestep**: may overwhelm console. Mitigate by offering row limit warning and potential `--max-rows` enhancement in backlog.
- **Incomplete Job Data**: jobs lacking `start_time`/`end_time` skipped with warnings; encourage upstream data cleanup.
- **PBS Unavailability**: fallback to user-specified or cached total nodes; display warning.
- **Timezone Ambiguity**: default to UTC; consider config-based timezone if needed later.

## Future Enhancements (Post-MVP)
- Interactive playback controls (pause/resume, step backward, jump ahead).
- Export snapshots (CSV/JSON) for analytics.
- Advanced filtering (user/project/queue, resource type).
- Visualizations (heatmaps, occupancy trends) and potential UI integration.
- Performance optimizations (precomputed snapshots, caching strategies).

## Outstanding Decisions
- Confirm whether to expose `--total-nodes` override in MVP or later.
- Decide on strict datetime parsing rules and timezone handling policy.
- Determine acceptable behavior when playback range spans extremely long periods (auto-clamp step or enforce stricter limits).
