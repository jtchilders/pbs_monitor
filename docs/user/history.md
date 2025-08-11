# Historical Job Analysis

Analyze completed jobs stored in the database and recent PBS history.

## Commands

```bash
# Default (last 30 days)
pbs-monitor history

# Specific user and days
pbs-monitor history -u myuser -d 7

# Filter by completion state
pbs-monitor history -s F

# Sorting and limiting
pbs-monitor history --sort runtime --reverse --limit 200

# Include recent PBS completed jobs
pbs-monitor history --include-pbs-history
```

## Columns

Use `--columns` with a comma-separated list. Examples:
```bash
pbs-monitor history --columns job_id,name,owner,nodes,walltime,queued,runtime,exit_status
```


