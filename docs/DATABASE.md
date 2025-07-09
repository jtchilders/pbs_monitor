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
```

### Environment Variables
```bash
# Override database URL
export PBS_MONITOR_DB_URL="postgresql://user:password@localhost:5432/pbs_monitor"
```

## Data Collection

### Collection Strategy
The database system uses a dual update strategy:

1. **On-Demand Updates** - Triggered when users run CLI commands
2. **Scheduled Daemon Updates** - Background process (planned for future implementation)

### Collection Frequency (Planned)
- **Jobs**: Every 15 minutes (to catch state transitions)
- **Nodes**: Every 30 minutes (hardware changes less frequently)
- **Queues**: Every 60 minutes (configuration changes infrequently)
- **System Snapshots**: Every 30 minutes (for trend analysis)

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
- **Background Daemon** - Continuous data collection
- **Data Retention Policies** - Automatic cleanup of old data
- **Advanced Analytics** - Pre-computed metrics and trends
- **Data Export** - Export to various formats for analysis
- **Real-time Updates** - WebSocket support for live data

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