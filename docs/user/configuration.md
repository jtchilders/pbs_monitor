# Configuration

PBS Monitor reads configuration from the following locations (first found wins):
1. `~/.pbs_monitor.yaml`
2. `~/.config/pbs_monitor/config.yaml`
3. `/etc/pbs_monitor/config.yaml`
4. `pbs_monitor.yaml` (current directory)

## Example

```yaml
# PBS system configuration
pbs:
  command_timeout: 30
  job_refresh_interval: 30
  node_refresh_interval: 60

# Database configuration
database:
  url: "sqlite:///~/.pbs_monitor.db"
  pool_size: 5
  daemon_enabled: true
  auto_persist: false
  job_collection_interval: 900
  node_collection_interval: 1800
  queue_collection_interval: 3600

# Display configuration
display:
  use_colors: true
  max_table_width: 120
  truncate_long_names: true

# Logging configuration
logging:
  level: INFO
  date_format: "%d-%m %H:%M"
```

## Environment Variables

```bash
# Override database URL
export PBS_MONITOR_DB_URL="postgresql://user:password@host:port/database"
```

## Database presets

SQLite (dev):
```yaml
database:
  url: "sqlite:///~/.pbs_monitor.db"
  pool_size: 5
```

PostgreSQL (prod):
```yaml
database:
  url: "postgresql://pbs_monitor:password@localhost:5432/pbs_monitor"
  pool_size: 10
  max_overflow: 20
```


