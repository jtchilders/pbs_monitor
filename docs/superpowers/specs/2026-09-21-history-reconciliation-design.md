# PBS History Reconciliation Design

## Problem

The normal collector closes jobs that were previously observed as active by querying `qstat -x` for those known job IDs. It does not discover jobs submitted and completed entirely while the daemon was unavailable. On Aurora, a 4 day 10 hour outage left 9,876 terminal jobs absent from the database until a manual full-history import.

`UNKNOWN_END` also cannot safely be translated into success or failure from disappearance, estimated end time, or `Exit_status` alone. A retained authoritative terminal PBS record is required.

## Goals

- Discover terminal jobs missed during collector outages.
- Repair `UNKNOWN_END` rows when PBS still retains an authoritative terminal record.
- Start normal collection immediately; history reconciliation must not block daemon startup.
- Reconcile at startup and every 12 hours by default.
- Remain idempotent, auditable, bounded in database-query count, and isolated from normal collection failure.
- Provide the same operation through a manual dry-run-capable CLI command.

## Non-goals

- Reconstruct missed queue, node, system, or intermediate job-history snapshots.
- Infer terminal state when PBS no longer retains a terminal record.
- Import nonterminal history records; ordinary live collection owns active states.
- Rewrite an existing terminal database row from a retained history record.
- Routinely retain raw full-history JSON.

## Reconciliation semantics

A dedicated `HistoryReconciler` obtains the full `qstat -x -f -F json` response through `PBSCommands.qstat_completed_jobs()`. The existing tolerant decoding and JSON preprocessing remain the single parser path.

Only scheduler-terminal `F` and `C` records are candidates. `qstat_completed_jobs()` currently also returns transitional `E`, so the reconciler independently reapplies the strict `F`/`C` filter. Its `days` argument does not constrain PBS acquisition; the reconciliation path deliberately requests the complete retained history. For each candidate, the reconciler performs a bulk database-state lookup and classifies it as:

- **missing:** no database row exists; insert the converted terminal job;
- **repairable:** the row exists with state `UNKNOWN_END`; replace it from the complete terminal scheduler record;
- **unchanged:** the row exists in any other state; do not mutate it.

The unchanged rule prevents stale history from regressing live or terminal data. `E` remains transitional and is not imported. Malformed individual jobs are counted and skipped. A failure obtaining or parsing the overall response causes no database writes.

Database writes are batched and idempotent. Classification and mutation use atomic write predicates: missing rows are inserted with conflict-do-nothing semantics, and repairs update only while the database row is still `UNKNOWN_END`. This prevents a concurrent normal collection from being overwritten after a stale reconciliation read. The result reports observed records, terminal candidates, parse failures, inserted rows, repaired rows, unchanged rows, oldest/newest terminal times, duration, status, and error text when applicable.

## Scheduling

The daemon starts normal background collection first, then requests history reconciliation asynchronously. A dedicated `_reconciliation_lock` (`threading.Lock`, acquired with `blocking=False`) prevents overlap between startup, scheduled, and manual in-process requests; it is distinct from the normal refresh lock. The successful-run timestamp is read from the audit table at startup and updated at completion. With no prior successful row the startup sweep runs immediately; a recent successful row suppresses redundant restart sweeps until the interval elapses. This preserves startup coverage without repeatedly issuing a full history query during rapid service restarts.

Defaults:

```yaml
database:
  history_reconciliation_enabled: true
  history_reconciliation_interval_seconds: 43200
  history_reconciliation_batch_size: 500
  history_reconciliation_shutdown_timeout_seconds: 30
```

A reconciliation exception is logged and recorded but never terminates or pauses ordinary collection. Daemon shutdown joins the reconciliation worker for the configurable bounded interval; after that it logs that the daemon thread is still finishing and does not wait indefinitely on `qstat` or database I/O.

## CLI

Add:

```text
pbs-monitor database reconcile-history [--dry-run] [--batch-size N]
```

The command invokes the same service synchronously. Dry-run performs acquisition, parsing, classification, and reporting but no writes. Its summary distinguishes missing inserts from `UNKNOWN_END` repairs.

## Observability

Each run writes a structured reconciliation result to logs. Add a dedicated `history_reconciliation_log` table rather than overloading `data_collection_log`, because reconciliation counters and semantics differ from snapshot collection. The table records start/end time, status, candidate/insert/repair/unchanged/error counts, oldest/newest terminal timestamps, and an error message. Logging failure must not roll back successfully reconciled job batches, but it must be emitted to the application log.

The table is an **idempotently ensured auxiliary table**, not a new numbered schema version. Main is currently v1.4, while an existing pending multi-system branch already reserves v1.5 for `jobs.system`; consuming v1.5 here would create a migration collision. Fresh databases include the table through ORM metadata, and `migrate_to_latest()` always ensures it after the numbered migration ladder while `check_schema_version()` remains unchanged.

The pre-existing `_check_for_orphaned_job_recovery()` remains responsible only for terminal records observed in the current normal collection cycle. Atomic conditional writes make overlap harmless: whichever path repairs an `UNKNOWN_END` row first wins, and the other observes or records no repair. Reconciliation counts only rows actually changed.

## Existing `UNKNOWN_END` rows

Repair requires a matching retained `F` or `C` scheduler record. Current Aurora evidence:

- 4,677 `UNKNOWN_END` rows;
- none matched the retained `qstat -x` snapshot after emergency import;
- 327 contain an exit code, but only 27 also contain `obittime` and none store raw state `F` or `C`;
- therefore no existing row should be force-classified solely from current stored fields.

Rows that have aged out of PBS history remain `UNKNOWN_END` unless another authoritative archive is found.

## Failure safety

- Full-response parsing completes before writes begin.
- Nonterminal records cannot overwrite any row.
- Existing non-`UNKNOWN_END` rows cannot be changed by reconciliation.
- Single-flight locking prevents duplicate local sweeps.
- Batched upserts make reruns safe after partial completion.
- Normal collection remains independent of reconciliation success.
- No raw scheduler payload is written to logs.

## Verification

Automated tests cover:

- missing `F`/`C` insertion;
- `UNKNOWN_END` replacement from `F`/`C`;
- rejection of `Q`, `R`, `H`, and `E` records;
- preservation of existing active and terminal rows;
- idempotent reruns;
- malformed whole-response zero-write behavior;
- malformed individual-record accounting;
- dry-run zero-write behavior;
- batch behavior without per-job database lookups;
- startup nonblocking behavior;
- 12-hour completed-run scheduling;
- overlap suppression;
- reconciliation failure isolation from normal collection;
- configuration defaults and overrides;
- CLI summaries and exit status.

A production-like verification uses a throwaway Postgres database and a captured, sanitized PBS fixture. Live Aurora deployment is a separate post-merge operation and is not part of the PR.