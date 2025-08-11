# Quick Start

This guide helps you install PBS Monitor, initialize the database, and run the first commands.

## Install

```bash
git clone https://github.com/jtchilders/pbs_monitor.git
cd pbs_monitor
pip install -r requirements.txt
pip install -e .
```

Prerequisites:
- Python 3.8+
- PBS Pro or OpenPBS installed
- PBS commands (`qstat`, `qsub`, `pbsnodes`, etc.) in PATH

## Initialize Database

```bash
# Create database (SQLite by default)
pbs-monitor database init

# Verify
pbs-monitor database status
pbs-monitor database validate
```

## First Commands

```bash
# System status
pbs-monitor status

# Jobs (all / by user)
pbs-monitor jobs
pbs-monitor jobs -u myuser

# Nodes / Queues
pbs-monitor nodes
pbs-monitor queues
```

## Collect Data On Demand

```bash
# Persist data to the database while viewing
pbs-monitor status --collect
pbs-monitor jobs --collect
pbs-monitor nodes --collect
pbs-monitor queues --collect
```

## Daemon (optional)

```bash
# Foreground (testing)
pbs-monitor daemon start

# Background (production)
pbs-monitor daemon start --detach

# Status / Stop
pbs-monitor daemon status
pbs-monitor daemon stop
```

## Next Steps

- Configuration: `user/configuration.md`
- Database guide: `user/database.md`
- Daemon & deployment: `user/daemon_and_deployment.md`
- CLI reference: `user/cli_reference.md`


