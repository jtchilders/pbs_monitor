# Database Guide

PBS Monitor persists PBS data for historical analysis and richer queries.

## Features

- SQLite for development; PostgreSQL for production
- Automatic schema creation and migration
- Connection pooling and concurrent access support

## Commands

```bash
# Initialize database
pbs-monitor database init

# Status and validation
pbs-monitor database status
pbs-monitor database validate

# Backup / Restore (SQLite only)
pbs-monitor database backup [path]
pbs-monitor database restore <path>

# Cleanup old data
pbs-monitor database cleanup --job-history-days 365 --snapshot-days 90

# Migrate schema
pbs-monitor database migrate
```

## On-demand collection

```bash
# Collect data while running commands
pbs-monitor status --collect
pbs-monitor jobs --collect
pbs-monitor nodes --collect
pbs-monitor queues --collect
```

## Completed jobs and history

```bash
# View historical jobs
pbs-monitor history

# User filter and lookback window
pbs-monitor history -u username --days 30

# Include recent PBS completed jobs
pbs-monitor history --include-pbs-history

# Sort and limit
pbs-monitor history -s F --sort runtime --reverse --limit 50
```

## Schema overview

- jobs: current/final job state (one per job)
- job_history: every job state change
- queues, nodes: configuration and properties
- queue_snapshots, node_snapshots, system_snapshots: historical utilization
- data_collection_log: audit trail of collection events


