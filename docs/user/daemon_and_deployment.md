# Daemon and Deployment

## Daemon

```bash
# Start daemon (foreground)
pbs-monitor daemon start

# Start daemon (background)
pbs-monitor daemon start --detach

# Custom PID file
pbs-monitor daemon start --detach --pid-file /var/run/pbs-monitor.pid
pbs-monitor daemon stop --pid-file /var/run/pbs-monitor.pid

# Status / Stop
pbs-monitor daemon status
pbs-monitor daemon stop
```

Notes:
- Requires database to be initialized
- Uses a JSON PID file at `~/.pbs_monitor_daemon.pid` by default

## Deployment

Single user (SQLite):
```bash
pip install -e .
pbs-monitor database init
pbs-monitor config --create
```

Multi-user (PostgreSQL):
```bash
createdb pbs_monitor
export PBS_MONITOR_DB_URL="postgresql://user:password@host:port/pbs_monitor"
pbs-monitor database init
pbs-monitor database status
```

## Collection Intervals

Configure in YAML:
```yaml
database:
  daemon_enabled: true
  auto_persist: false
  job_collection_interval: 900
  node_collection_interval: 1800
  queue_collection_interval: 3600
```


