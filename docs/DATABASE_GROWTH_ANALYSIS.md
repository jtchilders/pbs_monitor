# PBS Monitor Database Growth Analysis

## 🚨 **TL;DR - Why Your Database is Growing 10s of GB per Day**

Your PBS Monitor database is likely growing rapidly due to:

1. **Auto-persist enabled** → Data collection every 10 seconds
2. **Historical snapshots** → Every job state change saved forever  
3. **Raw PBS data storage** → Full JSON from PBS commands stored in each record
4. **No data cleanup** → All historical data kept indefinitely

## 📊 **What's Being Stored**

### Core Tables (grow slowly)
- `jobs` - One record per job (current/final state)
- `queues` - Queue configuration (rarely changes)  
- `nodes` - Node configuration (rarely changes)

### Historical Tables (grow rapidly ⚠️)
- `job_history` - **Every time a job state changes** 
- `queue_snapshots` - Point-in-time queue utilization
- `node_snapshots` - Point-in-time node utilization
- `system_snapshots` - Overall system state snapshots
- `data_collection_log` - Collection event logs

### Raw Data Storage (very large 💾)
Each record stores the **complete original PBS JSON** in `raw_pbs_data` fields:
- Jobs: Full `qstat -f` output per job
- Nodes: Full `pbsnodes` output per node  
- Queues: Full `qstat -Qf` output per queue

## 🔥 **Growth Rate Analysis**

### Default Configuration Issues

**Background Loop**: Daemon runs every **10 seconds**
```python
# From data_collector.py line 699
time.sleep(10)  # Loop every 10 seconds!
```

**When Auto-Persist is ON**:
- Data collection triggered every 10 seconds
- Each collection creates snapshots for ALL jobs, nodes, queues
- With 100 nodes, 500 jobs → 600+ new records every 10 seconds
- **Daily growth: 5.2 million records**

**Job History Growth**:
- Jobs change state frequently (Q→R→C)
- Each state change = new record in `job_history`
- Long-running jobs may have 50+ state change records

### Example Growth Calculation

**Scenario**: 100 nodes, 500 jobs, auto-persist enabled

**Per collection (every 10 seconds)**:
- 500 job_history records
- 100 node_snapshots 
- 10 queue_snapshots
- 1 system_snapshot
- **Total: ~611 records per collection**

**Daily**: 611 × 6 collections/min × 1440 min = **5.3 million records/day**

**With raw data**: Each record ~1-5KB → **5-25 GB per day**

## 🎯 **Immediate Solutions**

### 1. **CRITICAL: Disable Auto-Persist**
```yaml
# In your config file
database:
  auto_persist: false  # ← Add this line
```

### 2. **Run Manual Collections Instead**
```bash
# Collect data hourly via cron instead
0 * * * * pbs-monitor status --collect
```

### 3. **Clean Up Existing Data**
```bash
# Remove old data (keeps last 30 days)
pbs-monitor database cleanup --job-history-days 30 --snapshot-days 30 --force
```

### 4. **Adjust Collection Intervals**
```yaml
database:
  job_collection_interval: 900     # 15 minutes (was 900)
  node_collection_interval: 3600   # 1 hour (was 1800) 
  queue_collection_interval: 7200  # 2 hours (was 3600)
```

## 🛠️ **Long-term Optimizations**

### 1. **Implement Data Retention**
```bash
# Set up automated cleanup
pbs-monitor database cleanup --job-history-days 90 --snapshot-days 30
```

### 2. **Reduce Raw Data Storage**
Modify the database schema to optionally disable raw data storage for production:
```python
# In production config
raw_pbs_data = None  # Don't store raw data
```

### 3. **Use PostgreSQL for Large Deployments**
SQLite isn't optimal for high-frequency writes:
```yaml
database:
  url: "postgresql://user:pass@host:5432/pbs_monitor"
```

### 4. **Partition Large Tables**
For PostgreSQL, partition snapshot tables by date:
```sql
-- Partition job_history by month
CREATE TABLE job_history_2024_01 PARTITION OF job_history
FOR VALUES FROM ('2024-01-01') TO ('2024-02-01');
```

## 📊 **Using the Analysis Tool**

Run the provided analysis script to understand your current situation:

```bash
# Basic analysis
python analyze_database_growth.py

# Detailed analysis with growth patterns  
python analyze_database_growth.py --detailed

# Specify config file
python analyze_database_growth.py --config /path/to/config.yaml
```

**Sample Output**:
```
📊 Table Sizes and Record Counts:
  📋 jobs                      1,234 records      1.2 MB
  📋 job_history           2,456,789 records     2.4 GB  ⚠️
  📋 queue_snapshots         123,456 records   123.5 MB
  📋 node_snapshots        1,234,567 records     1.2 GB
  📋 system_snapshots         12,345 records    12.3 MB
  ──────────────────────────────────────────────────────
  📊 TOTAL                 3,828,391 records     3.8 GB

⏱️ Data Collection Frequency (Last 24 Hours):
  🔄 daemon         144 collections (every 10.0 min)  ⚠️

🎯 Recommendations:
  🔥 CRITICAL: auto_persist is enabled. Disable unless needed.
  🔥 CRITICAL: Daemon collecting every 10.0 minutes (144 times in 24h)
  ⚠️  HIGH: job_history has 2,456,789 records. Implement retention.
```

## ⚙️ **Configuration Recommendations**

### Conservative (Reduced Growth)
```yaml
database:
  auto_persist: false
  job_collection_interval: 1800    # 30 minutes
  node_collection_interval: 7200   # 2 hours  
  queue_collection_interval: 14400 # 4 hours
  job_history_days: 90             # 3 months
  snapshot_retention_days: 30      # 1 month
```

### Production (Minimal Growth)
```yaml
database:
  auto_persist: false
  job_collection_interval: 3600    # 1 hour
  node_collection_interval: 14400  # 4 hours
  queue_collection_interval: 43200 # 12 hours
  job_history_days: 30             # 1 month
  snapshot_retention_days: 7       # 1 week
```

## 🔧 **Quick Fix Commands**

```bash
# 1. Stop the daemon
pbs-monitor daemon stop

# 2. Clean up data  
pbs-monitor database cleanup --job-history-days 30 --snapshot-days 7 --force

# 3. Update config to disable auto-persist
echo "database:\n  auto_persist: false" >> ~/.pbs_monitor.yaml

# 4. Restart daemon
pbs-monitor daemon start

# 5. Set up hourly manual collection
echo "0 * * * * pbs-monitor status --collect" | crontab -
```

## 📈 **Monitoring Database Growth**

Add to your monitoring:
```bash
# Check database size daily
du -h ~/.pbs_monitor.db

# Check recent growth
python analyze_database_growth.py --detailed | grep "Last 24 hours"

# Set up alerts for rapid growth
if [ $(du -s ~/.pbs_monitor.db | cut -f1) -gt 10000000 ]; then
  echo "Warning: PBS Monitor DB > 10GB" | mail admin@company.com
fi
```

## 🎯 **Expected Results**

After implementing these changes:

- **Before**: 10-25 GB/day growth
- **After**: 100-500 MB/day growth (95% reduction)
- Daemon still provides real-time monitoring
- Historical data preserved (with retention limits)
- Much better performance and manageability