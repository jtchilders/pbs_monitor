# PBS History Reconciliation Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add fail-safe, nonblocking full `qstat -x` reconciliation at daemon startup and every 12 hours, plus a dry-run-capable manual command.

**Architecture:** A focused history-reconciliation service owns terminal filtering, bulk classification, and batched writes. `DataCollector` owns only worker lifecycle and scheduling; `DatabaseCommand` calls the same service synchronously. Existing terminal rows are immutable, and only missing or `UNKNOWN_END` rows may be changed.

**Tech Stack:** Python 3.10+, SQLAlchemy, argparse, pytest, threading.

---

### Task 1: Pure reconciliation service and repository bulk operations

**Files:**
- Create: `pbs_monitor/history_reconciliation.py`
- Modify: `pbs_monitor/database/repositories.py`
- Create: `tests/test_history_reconciliation.py`

- [ ] Write tests for missing terminal insertion, `UNKNOWN_END` repair, nonterminal rejection, existing-row preservation, malformed-record accounting, dry-run, idempotency, and bounded bulk state lookup.
- [ ] Run `.venv/bin/pytest tests/test_history_reconciliation.py -v` and verify failure because the service does not exist.
- [ ] Add a result dataclass and `HistoryReconciler.reconcile(dry_run=False, batch_size=500)` with dependency injection for PBS commands, repository, converter, and clock.
- [ ] Add repository methods that fetch states for a set of IDs in chunked `IN` queries and apply converted jobs only when absent or `UNKNOWN_END`.
- [ ] Parse the complete response before writes, accept only `F`/`C`, and classify every candidate before mutation.
- [ ] Run the focused test file and verify all cases pass.
- [ ] Commit with `feat: add terminal PBS history reconciler`.

### Task 2: Nonblocking daemon scheduling

**Files:**
- Modify: `pbs_monitor/config.py`
- Modify: `pbs_monitor/data_collector.py`
- Modify: `tests/test_history_reconciliation.py`

- [ ] Add failing tests proving startup returns without waiting, one startup run is requested, completed-run time anchors the 12-hour interval, overlap is skipped, failures do not stop normal collection, and stop performs only a bounded join.
- [ ] Run focused tests and verify the expected failures.
- [ ] Add configuration fields `history_reconciliation_enabled`, `history_reconciliation_interval_seconds`, and `history_reconciliation_batch_size` with defaults `True`, `43200`, and `500`, including sample configuration output.
- [ ] Add a dedicated reconciliation thread, nonblocking lock, completion timestamp, scheduler check, and bounded shutdown handling to `DataCollector`.
- [ ] Ensure `start_background_updates()` starts normal collection independently and then requests reconciliation.
- [ ] Run focused tests and verify all pass.
- [ ] Commit with `feat: schedule nonblocking history reconciliation`.

### Task 3: Manual CLI

**Files:**
- Modify: `pbs_monitor/cli/main.py`
- Modify: `pbs_monitor/cli/commands.py`
- Modify: `tests/test_history_reconciliation.py`

- [ ] Add failing parser/command tests for `database reconcile-history`, `--dry-run`, `--batch-size`, summaries, success, and acquisition failure.
- [ ] Run focused tests and verify failure because the subcommand is absent.
- [ ] Register the argparse subcommand and dispatch it from `DatabaseCommand` to the shared service.
- [ ] Print observed, terminal, inserted, repaired, unchanged, malformed, range, and duration fields; return nonzero on failed reconciliation.
- [ ] Run focused tests and verify all pass.
- [ ] Commit with `feat: expose history reconciliation CLI`.

### Task 4: Durable audit model and migration

**Files:**
- Modify: `pbs_monitor/database/models.py`
- Modify: `pbs_monitor/database/migrations.py`
- Modify: `pbs_monitor/database/repositories.py`
- Modify: `pbs_monitor/history_reconciliation.py`
- Modify: `tests/test_database.py`
- Modify: `tests/test_history_reconciliation.py`

- [ ] Add failing SQLite migration/model tests for `history_reconciliation_log` and service tests for success/failure audit rows.
- [ ] Run tests and verify expected missing-table failures.
- [ ] Add an additive migration and model containing timestamps, status, counts, terminal range, duration, and bounded error text.
- [ ] Keep audit-write failure best-effort and explicitly logged without rolling back completed job batches.
- [ ] Run migration and service tests until green.
- [ ] Commit with `feat: persist history reconciliation audit results`.

### Task 5: Documentation, integration, and verification

**Files:**
- Modify: `docs/user/cli_reference.md`
- Modify: `docs/user/polaris-deployment.md`
- Modify: `README.md` if configuration is documented there

- [ ] Document defaults, terminal-only semantics, dry-run usage, failure isolation, and the fact that expired `UNKNOWN_END` rows cannot be inferred safely.
- [ ] Run `.venv/bin/python -m pbs_monitor.cli.main database reconcile-history --help` and verify the command contract.
- [ ] Run `.venv/bin/python -c "import pbs_monitor.data_collector as dc; assert dc.DATABASE_AVAILABLE"` to guard the daemon import path.
- [ ] Run `.venv/bin/pytest -q -k "not test_cli_help and not test_json_preprocessing_no_changes_needed"`; expect all selected tests to pass. The two exclusions are pre-existing baseline failures verified before implementation.
- [ ] Run `.venv/bin/flake8 pbs_monitor/history_reconciliation.py tests/test_history_reconciliation.py` and address introduced errors.
- [ ] Run the reconciliation twice against a temporary SQLite database and fixture; verify first-run inserts/repairs and second-run zero mutations.
- [ ] Commit with `docs: document history reconciliation`.

### Task 6: Review and pull request

**Files:** none expected unless review finds defects.

- [ ] Review `git diff origin/main...HEAD` for secrets, accidental broad changes, schema/version consistency, and every design invariant.
- [ ] Run the complete selected test suite again from a clean process.
- [ ] Push `feat/history-reconciliation` to the SSH GitHub remote.
- [ ] Open a PR to `main` describing the Aurora outage evidence, safety policy, tests, migration, and deployment note.
- [ ] Read back the PR URL, head SHA, changed files, and CI state; fix failures before reporting completion.
