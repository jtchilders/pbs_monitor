# Quick Start

This guide covers installation, setup, and basic usage of PBS Monitor.

## Installation

```bash
git clone https://github.com/jtchilders/pbs_monitor.git
cd pbs_monitor

# Create and activate a virtual environment (recommended)
python -m venv venv
source venv/bin/activate

# Install (dependencies are installed automatically)
pip install -e .
```

**Requirements:** Python 3.8+, PBS commands (`qstat`, `pbsnodes`) in PATH.

## Setup

### Create Configuration

```bash
pbs-monitor config --create   # Creates ~/.pbs_monitor.yaml
```

### Initialize Database

```bash
pbs-monitor database init      # Creates database (default: ~/.pbs_monitor.db)
```

**Note:** The default database location is `~/.pbs_monitor.db`. On large systems this file can grow significantly. Edit `~/.pbs_monitor.yaml` to place it on project space before running `database init`.

## Basic Commands

```bash
# System status
pbs-monitor status

# Jobs
pbs-monitor jobs                # All jobs
pbs-monitor jobs -u myuser      # Filter by user
pbs-monitor jobs -s R           # Filter by state (Running)
pbs-monitor jobs -q prod        # Filter by queue

# Nodes and queues
pbs-monitor nodes
pbs-monitor queues

# Historical jobs from database
pbs-monitor history
pbs-monitor history -u myuser -d 7   # Last 7 days for user
```

## Collecting Data

### On-Demand Collection

Persist data to the database while viewing:

```bash
pbs-monitor status --collect
pbs-monitor jobs --collect
pbs-monitor nodes --collect
```

### Background Daemon

For continuous collection:

```bash
# Start in background
pbs-monitor daemon start

# Check status
pbs-monitor daemon status

# Stop
pbs-monitor daemon stop
```

The daemon periodically collects job, node, and queue data to the database.

## Configuration

Configuration files are searched in order:
1. `~/.pbs_monitor.yaml`
2. `~/.config/pbs_monitor/config.yaml`
3. `/etc/pbs_monitor/config.yaml`
4. `pbs_monitor.yaml` (current directory)

### Example Configuration

```yaml
database:
  url: "sqlite:////project/myproject/pbs_monitor.db"  # Note: 4 slashes for absolute path
  pool_size: 5
  daemon_enabled: true
  job_collection_interval: 900      # 15 minutes
  node_collection_interval: 1800    # 30 minutes
  queue_collection_interval: 3600   # 60 minutes

display:
  use_colors: true
  max_table_width: 120

logging:
  level: INFO
```

### Environment Override

```bash
export PBS_MONITOR_DB_URL="sqlite:////path/to/database.db"
```

## Database Management

```bash
# Check status
pbs-monitor database status

# Validate schema
pbs-monitor database validate

# Backup (SQLite only)
pbs-monitor database backup /path/to/backup.db

# Clean up old data
pbs-monitor database cleanup --job-history-days 365 --snapshot-days 90
```

## Analytics

```bash
# Find job shapes that can run immediately
pbs-monitor analyze run-now

# Top users/projects by node-hours
pbs-monitor analyze leaderboard -d 30

# Walltime efficiency
pbs-monitor analyze walltime-efficiency-by-user -d 7

# Usage insights with plots
pbs-monitor analyze usage-insights -d 30 -o ./plots
```

## Visualization

```bash
# Replay historical job timelines
pbs-monitor replay --start "24h ago" --output-format split-panel

# PBS scoring formula analysis
pbs-monitor score-formula --plot --output-dir ./plots
```

## Development

```bash
# Run tests
pytest
pytest --cov=pbs_monitor

# Code formatting
black pbs_monitor/
flake8 pbs_monitor/
```

## Next Steps

- [Full CLI reference](cli_reference.md)
- [Database relocation guide](database_relocation.md)
- [Polaris deployment guide (PostgreSQL)](polaris-deployment.md)
