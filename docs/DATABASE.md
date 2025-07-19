# Database Documentation

## Overview

PBS Monitor includes a comprehensive database system for persistent storage of PBS system data. This solves the limitation of PBS's typical 1-week job history retention by continuously collecting and storing job, queue, and node information.

## Database Features

### Persistent Storage
- **SQLite** for development and single-user deployments
- **PostgreSQL** for production multi-user environments
- Automatic schema creation and migration
- Connection pooling and concurrent access support

### Data Models

#### Core Tables
- **jobs** - Current/final job state (one record per job)
- **job_history** - Every job state change (tracks lifecycle)
- **queues** - Queue configuration and limits
- **nodes** - Node hardware specifications and properties

#### Snapshot Tables
- **queue_snapshots** - Historical queue utilization
- **node_snapshots** - Historical node utilization
- **system_snapshots** - Overall system metrics over time
- **data_collection_log** - Audit trail of data collection events

### Key Benefits

1. **Overcomes PBS History Limitation** - Regular collection captures data before PBS purges it
2. **Historical Analysis** - Rich historical data for trend analysis and ML training
3. **Concurrent Access** - Multiple users and processes can access data safely
4. **Scalable** - Works from single-user to large multi-user deployments

## Database Commands

### Initialize Database
```bash
# Create fresh database with all tables
pbs-monitor database init

# Force initialization (drops existing tables)
pbs-monitor database init --force
```

### Database Status
```bash
# Show database information and table counts
pbs-monitor database status

# Validate database schema
pbs-monitor database validate
```

### Database Maintenance
```bash
# Backup database (SQLite only)
pbs-monitor database backup [backup_path]

# Restore from backup (SQLite only)
pbs-monitor database restore backup_path

# Clean up old data
pbs-monitor database cleanup --job-history-days 365 --snapshot-days 90
```

### Migration
```bash
# Migrate to latest schema version
pbs-monitor database migrate
```

### Historical Job Analysis
```bash
# View completed jobs from database
pbs-monitor history

# Show specific user's job history
pbs-monitor history -u username --days 30

# Include recent PBS completed jobs
pbs-monitor history --include-pbs-history

# Filter by completion state and sort by runtime
pbs-monitor history -s F --sort runtime --reverse --limit 50
```

### On-Demand Data Collection
```bash
# Collect data while viewing system status
pbs-monitor status --collect

# Collect job data to database
pbs-monitor jobs --collect

# Collect node and queue data  
pbs-monitor nodes --collect
pbs-monitor queues --collect
```

### Daemon Management
```bash
# Start background data collection daemon
pbs-monitor daemon start

# Start daemon in background (detached mode)
pbs-monitor daemon start --detach

# Check daemon status and recent activity
pbs-monitor daemon status

# Stop daemon gracefully
pbs-monitor daemon stop

# Use custom PID file location
pbs-monitor daemon start --detach --pid-file /var/run/pbs-monitor.pid
pbs-monitor daemon stop --pid-file /var/run/pbs-monitor.pid
```

## Configuration

### SQLite Configuration (Development)
```yaml
# ~/.pbs_monitor.yaml
database:
  url: "sqlite:///~/.pbs_monitor.db"
  pool_size: 5
  echo_sql: false
```

### PostgreSQL Configuration (Production)
```yaml
# /etc/pbs_monitor/config.yaml
database:
  url: "postgresql://user:password@localhost:5432/pbs_monitor"
  pool_size: 10
  max_overflow: 20
  echo_sql: false
  
  # Daemon configuration for background collection
  daemon_enabled: true
  auto_persist: true
  job_collection_interval: 600      # 10 minutes (production)
  node_collection_interval: 1200    # 20 minutes
  queue_collection_interval: 3600   # 60 minutes
  
  # Data retention settings
  job_history_days: 365
  snapshot_retention_days: 90
```

### Environment Variables
```bash
# Override database URL
export PBS_MONITOR_DB_URL="postgresql://user:password@localhost:5432/pbs_monitor"
```

## Data Collection

### Collection Strategy
The database system uses a comprehensive data collection strategy:

1. **On-Demand Updates** - Triggered when users run CLI commands with --collect flag
2. **Completed Job Collection** - Automatic collection using `qstat -x` to capture jobs before PBS purges them
3. **Background Daemon** - Continuous data collection service with configurable intervals

### Completed Job Tracking
To overcome PBS's typical 1-week history limitation, the system automatically collects completed jobs:
- Uses `qstat -x -f -F json` to retrieve recently completed jobs
- Integrated into `collect_and_persist()` operations
- Prevents data loss when jobs are purged from PBS history
- Accessible via the `history` command for comprehensive analysis

### Collection Frequency
- **Jobs**: Every 15 minutes (to catch state transitions)
- **Completed Jobs**: During each collection cycle (prevents data loss)
- **Nodes**: Every 30 minutes (hardware changes less frequently)
- **Queues**: Every 60 minutes (configuration changes infrequently)
- **System Snapshots**: Every 30 minutes (for trend analysis)

### Daemon Configuration
The background daemon can be configured with custom collection intervals:

```yaml
database:
  daemon_enabled: true
  auto_persist: true
  job_collection_interval: 900      # 15 minutes (default)
  node_collection_interval: 1800    # 30 minutes (default)
  queue_collection_interval: 3600   # 60 minutes (default)
```

## Schema Details

### Job Lifecycle Tracking
```sql
-- Core job table
CREATE TABLE jobs (
    job_id VARCHAR(100) PRIMARY KEY,
    job_name VARCHAR(200),
    owner VARCHAR(50),
    state VARCHAR(10),
    queue VARCHAR(50),
    submit_time DATETIME,
    start_time DATETIME,
    end_time DATETIME,
    -- ... additional fields
);

-- Job state change history
CREATE TABLE job_history (
    id INTEGER PRIMARY KEY,
    job_id VARCHAR(100),
    timestamp DATETIME,
    state VARCHAR(10),
    -- ... additional fields
);
```

### Snapshot Tables
```sql
-- System-wide snapshots
CREATE TABLE system_snapshots (
    id INTEGER PRIMARY KEY,
    timestamp DATETIME,
    total_jobs INTEGER,
    running_jobs INTEGER,
    total_cores INTEGER,
    used_cores INTEGER,
    -- ... additional metrics
);
```

## Performance

### Indexing Strategy
- Job ID and timestamp indexes for efficient queries
- State-based indexes for filtering
- Composite indexes for common query patterns

### Query Optimization
- Connection pooling for concurrent access
- Materialized views for common aggregations (planned)
- Efficient pagination for large result sets

## Data Quality

### Validation
- Schema validation on startup
- Data consistency checks
- Foreign key constraints
- Audit trail for all changes

### Error Handling
- Graceful degradation when PBS is unavailable
- Retry logic for transient failures
- Comprehensive logging of collection events

## Future Enhancements

### Planned Features
- **Data Retention Policies** - Automatic cleanup of old data
- **Advanced Analytics** - Pre-computed metrics and trends
- **Data Export** - Export to various formats for analysis
- **Real-time Updates** - WebSocket support for live data
- **Multi-cluster Support** - Manage multiple PBS clusters

### Machine Learning Integration
- Feature engineering utilities
- Training data preparation
- Model performance tracking
- Prediction result storage

## Troubleshooting

### Common Issues

#### Database Connection Errors
```bash
# Check database status
pbs-monitor database status

# Validate schema
pbs-monitor database validate

# Test connection
pbs-monitor database init
```

#### Performance Issues
```bash
# Clean up old data
pbs-monitor database cleanup

# Check database size
pbs-monitor database status
```

#### Data Consistency
```bash
# Validate schema
pbs-monitor database validate

# Re-initialize if needed
pbs-monitor database init --force
```

### Logging
Enable database logging for debugging:
```yaml
database:
  echo_sql: true
  
logging:
  level: DEBUG
```

## Migration from Phase 1

The database system is designed to be compatible with existing Phase 1 functionality. All existing CLI commands continue to work, with the database providing additional persistence and historical data capabilities.

### Migration Steps
1. **Install Dependencies** - `pip install -r requirements.txt`
2. **Initialize Database** - `pbs-monitor database init`
3. **Verify Installation** - `pbs-monitor database status`
4. **Continue Normal Usage** - All existing commands work as before

The system will automatically collect data to the database while maintaining backward compatibility with the original PBS command functionality. 