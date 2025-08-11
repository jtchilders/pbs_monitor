# Phase 2 Database Implementation Plan

## Overview
This document outlines the complete database design and implementation strategy for Phase 2 of the PBS Monitor project. The plan addresses persistent storage, historical data management, and data collection strategies to overcome PBS's 1-week history limitation.

## Architecture Decisions

### 1. SQL vs Pandas Decision

**Selected Approach: Hybrid SQL + Pandas**

**Primary Storage: SQLite/PostgreSQL**
- **SQLite** for development, testing, and single-user deployments
- **PostgreSQL** for production multi-user environments
- Persistent storage survives system restarts
- Handles concurrent access from daemon + CLI
- Efficient for large historical datasets
- ACID compliance ensures data integrity

**Analysis Layer: Pandas**
- Load SQL data into Pandas DataFrames for analysis
- Leverage Pandas' powerful data manipulation for ML feature engineering
- Export analysis results back to SQL for persistence
- Best of both worlds approach

**Implementation:**
```python
# Example of hybrid approach
def get_job_analysis_data(start_date, end_date):
    # Load from SQL
    query = """
    SELECT j.*, h.timestamp, h.state 
    FROM jobs j
    JOIN job_history h ON j.job_id = h.job_id
    WHERE h.timestamp BETWEEN ? AND ?
    """
    df = pd.read_sql(query, engine, params=[start_date, end_date])
    
    # Pandas analysis
    df['queue_time'] = (df['start_time'] - df['submit_time']).dt.total_seconds()
    df['runtime'] = (df['end_time'] - df['start_time']).dt.total_seconds()
    
    return df
```

### 2. Database Schema Strategy

**Core Design Principles:**
1. **Job Lifecycle Tracking**: Track jobs from submission to completion
2. **State Transition History**: Record every state change for ML training
3. **Snapshot-based Analytics**: Regular system state captures for trend analysis
4. **Raw Data Preservation**: Keep original PBS JSON for debugging
5. **Audit Trail**: Track all data collection events

**Key Tables:**
- `jobs`: Current/final job state (one record per job)
- `job_history`: Every job state change (multiple records per job)
- `queue_snapshots`: Queue utilization over time
- `node_snapshots`: Node utilization over time
- `system_snapshots`: Overall system metrics over time
- `data_collection_log`: Audit trail of collection events

### 3. The PBS 1-Week History Problem

**Problem**: PBS servers typically only retain completed job information for 1 week.

**Solution Strategy:**
1. **Proactive Collection**: Collect job data while jobs are still visible
2. **Final State Capture**: Ensure we capture job completion before PBS purges it
3. **Graceful Degradation**: Handle cases where final state is missed
4. **Completion Detection**: Use multiple collection runs to detect state transitions

**Implementation Approach:**
```python
class JobTracker:
    def collect_jobs(self):
        current_jobs = self.pbs_commands.qstat_jobs()
        
        for job in current_jobs:
            # Update or insert job record
            db_job = self.get_or_create_job(job.job_id)
            
            # Record state change if different
            if db_job.state != job.state:
                self.record_state_change(job)
            
            # Update current state
            self.update_job_state(job)
            
            # Mark as final if completed
            if job.state in [JobState.COMPLETED, JobState.FINISHED]:
                db_job.final_state_recorded = True
```

### 4. Update Mechanisms

**Dual Update Strategy:**

**A. On-Demand Updates (CLI)**
- Triggered when user runs commands
- Fresh data for immediate queries
- Minimal latency for user interactions

**B. Scheduled Daemon Updates**
- Background process runs on login node
- Collects data every 15-30 minutes
- Ensures no job state transitions are missed
- Handles the 1-week PBS history limitation

**Data Collection Frequency:**
- **Jobs**: Every 15 minutes (to catch state transitions)
- **Nodes**: Every 30 minutes (hardware changes less frequently)
- **Queues**: Every 60 minutes (configuration changes infrequently)
- **System Snapshots**: Every 30 minutes (for trend analysis)

**Implementation:**
```python
class DataCollectionDaemon:
    def __init__(self):
        self.job_interval = 15 * 60      # 15 minutes
        self.node_interval = 30 * 60     # 30 minutes
        self.queue_interval = 60 * 60    # 60 minutes
        self.snapshot_interval = 30 * 60 # 30 minutes
    
    def run_collection_loop(self):
        while not self.should_stop:
            current_time = time.time()
            
            # Collect jobs most frequently
            if current_time - self.last_job_collection >= self.job_interval:
                self.collect_jobs()
                self.last_job_collection = current_time
            
            # Collect nodes
            if current_time - self.last_node_collection >= self.node_interval:
                self.collect_nodes()
                self.last_node_collection = current_time
            
            # Collect queues
            if current_time - self.last_queue_collection >= self.queue_interval:
                self.collect_queues()
                self.last_queue_collection = current_time
            
            # Create system snapshot
            if current_time - self.last_snapshot >= self.snapshot_interval:
                self.create_system_snapshot()
                self.last_snapshot = current_time
            
            time.sleep(60)  # Check every minute
```

### 5. Data Population Strategy

**Initial Population:**
1. **Bootstrap**: Collect current system state
2. **Backfill**: Attempt to get any available historical data
3. **Ongoing**: Start regular collection cycles

**Update Process:**
1. **Collect**: Query PBS systems for current data
2. **Compare**: Check for changes since last collection
3. **Record**: Store new states and transitions
4. **Aggregate**: Update snapshot tables
5. **Cleanup**: Archive old data if needed

**Handling Missing Data:**
- Mark jobs with incomplete lifecycle data
- Use estimation techniques for missing metrics
- Provide data quality indicators in analysis

## Implementation Steps

### Phase 2A: Database Foundation (Week 1-2)

1. **Database Setup**
   - Create SQLAlchemy models from schema
   - Database migration scripts
   - Connection management utilities
   - Configuration for SQLite/PostgreSQL

2. **Data Access Layer**
   - Create repository classes for each entity
   - Implement CRUD operations
   - Add query utilities for common patterns
   - Connection pooling and error handling

3. **Integration with Existing Code**
   - Modify DataCollector to use database
   - Update models to work with SQLAlchemy
   - Maintain backward compatibility

### Phase 2B: Data Collection Engine (Week 3-4) ✅ COMPLETE

1. **Enhanced DataCollector** ✅
   - Database-backed data collection
   - State transition detection
   - Batch operations for performance
   - Error handling and retry logic

2. **Daemon Process** ✅
   - Background collection service with CLI management
   - Configurable collection intervals
   - Process management with PID files and signal handling
   - Graceful shutdown handling
   - Auto-persist functionality for continuous collection

3. **CLI Integration** ✅
   - Update CLI commands to use database
   - Add database management commands
   - Add daemon management commands (start/stop/status)
   - Add --collect flag for on-demand collection
   - Historical data queries
   - Performance optimizations

### Phase 2C: Analytics and Optimization (Week 5-6)

**Daemon Implementation Summary:**
The daemon functionality has been fully implemented with:
- Complete CLI commands (`pbs-monitor daemon start/stop/status`)
- Background data collection with configurable intervals
- Process management with PID files and signal handling  
- Auto-persist configuration for continuous collection
- Collection activity monitoring and reporting
- On-demand collection via --collect flag on all data commands

1. **Query Optimization**
   - Index analysis and tuning
   - Common query patterns
   - Materialized views for aggregations
   - Performance monitoring

2. **Data Quality**
   - Validation rules
   - Duplicate detection
   - Data completeness checks
   - Cleanup utilities

3. **Historical Analysis**
   - Pandas integration layer
   - Common analysis functions
   - Export utilities
   - Visualization helpers

## Database Configuration

### Development Configuration
```yaml
# ~/.pbs_monitor.yaml
database:
  url: "sqlite:///~/.pbs_monitor.db"
  echo_sql: false
  pool_size: 5
  
collection:
  daemon_enabled: true
  job_interval: 900      # 15 minutes
  node_interval: 1800    # 30 minutes
  queue_interval: 3600   # 60 minutes
  snapshot_interval: 1800 # 30 minutes
```

### Production Configuration
```yaml
# /etc/pbs_monitor/config.yaml
database:
  url: "postgresql://pbs_monitor:password@localhost:5432/pbs_monitor"
  pool_size: 10
  max_overflow: 20
  echo_sql: false
  
collection:
  daemon_enabled: true
  job_interval: 600      # 10 minutes
  node_interval: 1200    # 20 minutes
  queue_interval: 3600   # 60 minutes
  snapshot_interval: 1800 # 30 minutes
  
logging:
  level: INFO
  file: /var/log/pbs_monitor.log
```

## Benefits of This Approach

1. **Solves PBS History Limitation**: Regular collection captures data before PBS purges it
2. **Supports ML Goals**: Rich historical data for training prediction models
3. **Concurrent Access**: Multiple users and processes can access data safely
4. **Scalable**: Works from single-user to large multi-user deployments
5. **Flexible**: Easy to add new metrics and analysis capabilities
6. **Maintainable**: Clean separation between data collection and analysis

## Potential Challenges and Mitigations

### 1. Storage Growth
**Challenge**: Snapshot tables will grow continuously
**Mitigation**: 
- Implement data retention policies
- Use database partitioning for large deployments
- Provide cleanup utilities

### 2. Collection Failures
**Challenge**: Network issues or PBS downtime
**Mitigation**:
- Retry logic with exponential backoff
- Graceful degradation
- Comprehensive logging

### 3. Data Consistency
**Challenge**: Multiple processes updating database
**Mitigation**:
- Use database transactions
- Implement proper locking
- Design for concurrent access

### 4. Performance
**Challenge**: Large historical datasets
**Mitigation**:
- Proper indexing strategy
- Query optimization
- Materialized views for common aggregations

## Next Steps

1. **Review and Approve**: Confirm this approach meets your requirements
2. **Start Implementation**: Begin with Phase 2A (Database Foundation)
3. **Incremental Testing**: Test each phase with real PBS data
4. **Performance Tuning**: Optimize as we gather real-world usage data
5. **Documentation**: Create user and admin documentation

This plan provides a solid foundation for Phase 2 while addressing all your specific concerns about data persistence, the PBS history limitation, and update mechanisms.
