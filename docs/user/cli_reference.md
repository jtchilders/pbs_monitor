# CLI Reference
This page is generated from `pbs-monitor --help` and subcommand help outputs.

## Global Help

```
usage: pbs-monitor [-h] [-c CONFIG] [-v] [-q] [--log-file LOG_FILE]
                   [--use-sample-data] [--max-width MAX_WIDTH] [--auto-width]
                   [--no-expand] [--wrap]
                   {status,jobs,nodes,queues,resv,reservations,reserv,history,analyze,config,database,daemon} ...

PBS scheduler monitoring and management tools

Configuration file locations (searched in order):
  ~/.pbs_monitor.yaml
  ~/.config/pbs_monitor/config.yaml
  /etc/pbs_monitor/config.yaml
  pbs_monitor.yaml (current directory)

Many command options (columns, display settings, etc.) can be configured in the config file.

positional arguments:
  {status,jobs,nodes,queues,resv,reservations,reserv,history,analyze,config,database,daemon}
                        Available commands
    status              Show PBS system status
    jobs                Show job information
    nodes               Show node information
    queues              Show queue information
    resv (reservations, reserv)
                        Reservation information and management
    history             Show historical job information from database
    analyze             Analytics and analysis commands
    config              Configuration management
    database            Database management
    daemon              Background data collection daemon management

options:
  -h, --help            show this help message and exit
  -c, --config CONFIG   Configuration file path
  -v, --verbose         Enable verbose logging
  -q, --quiet           Suppress normal output
  --log-file LOG_FILE   Log file path
  --use-sample-data     Use sample JSON data instead of actual PBS commands
                        (for testing)
  --max-width MAX_WIDTH
                        Maximum table width (overrides config)
  --auto-width          Auto-detect terminal width
  --no-expand           Don't expand columns to fit content
  --wrap                Enable word wrapping in table cells

Examples:
  pbs-monitor status              # Show system status
  pbs-monitor jobs                # Show all jobs
  pbs-monitor jobs -u myuser      # Show jobs for specific user
  pbs-monitor history             # Show completed jobs from database
  pbs-monitor history -u myuser   # Show user's completed jobs
  pbs-monitor nodes               # Show node information
  pbs-monitor queues              # Show queue information
  pbs-monitor config --create     # Create sample configuration
      
```

## status

```
usage: pbs-monitor status [-h] [-r] [--collect] [--queue-depth]

options:
  -h, --help     show this help message and exit
  -r, --refresh  Force refresh of data
  --collect      Collect and persist data to database after displaying
  --queue-depth  Show detailed queue depth breakdown by job size
```

## jobs

```
usage: pbs-monitor jobs [-h] [-u USER] [-s {R,Q,H,W,T,E,S,C,F}] [-r]
                        [--columns COLUMNS] [--sort SORT] [--reverse]
                        [--collect] [-d] [--history]
                        [--format {table,detailed,json}] [--show-raw]
                        [job_ids ...]

positional arguments:
  job_ids               Specific job IDs to show details for (numerical
                        portion only, e.g., 12345)

options:
  -h, --help            show this help message and exit
  -u, --user USER       Filter by username
  -s, --state {R,Q,H,W,T,E,S,C,F}
                        Filter by job state
  -r, --refresh         Force refresh of data
  --columns COLUMNS     Comma-separated list of columns to display: job_id,
                        name, owner, project, allocation, state, queue, nodes,
                        ppn, walltime, walltime_actual, memory, submit_time,
                        start_time, end_time, runtime, priority, cores, score,
                        queue_time, exit_status, execution_node
  --sort SORT           Column to sort by: job_id, name, owner, project,
                        allocation, state, queue, nodes, ppn, walltime,
                        priority, cores, score (default: score)
  --reverse             Sort in ascending order (default is descending for
                        score, ascending for others)
  --collect             Collect and persist data to database after displaying
  -d, --detailed        Show detailed information for specific jobs
  --history             Include job history from database (for detailed view)
  --format {table,detailed,json}
                        Output format for job details (default: table)
  --show-raw            Show raw PBS attributes (for detailed view)
```

## nodes

```
usage: pbs-monitor nodes [-h]
                         [-s {free,offline,down,busy,job-exclusive,job-sharing}]
                         [-r] [--columns COLUMNS] [-d] [--collect]

options:
  -h, --help            show this help message and exit
  -s, --state {free,offline,down,busy,job-exclusive,job-sharing}
                        Filter by node state
  -r, --refresh         Force refresh of data
  --columns COLUMNS     Comma-separated list of columns to display
  -d, --detailed        Show detailed table format instead of summary
  --collect             Collect and persist data to database after displaying
```

## queues

```
usage: pbs-monitor queues [-h] [-r] [--columns COLUMNS] [--collect]

options:
  -h, --help         show this help message and exit
  -r, --refresh      Force refresh of data
  --columns COLUMNS  Comma-separated list of columns to display
  --collect          Collect and persist data to database after displaying
```

## history

```
usage: pbs-monitor history [-h] [-u USER] [-d DAYS] [-s {C,F,E,all}]
                           [--columns COLUMNS] [--sort SORT] [--reverse]
                           [--limit LIMIT] [--include-pbs-history]

options:
  -h, --help            show this help message and exit
  -u, --user USER       Filter by username
  -d, --days DAYS       Number of days to look back (default: 30)
  -s, --state {C,F,E,all}
                        Filter by completion state: C (completed), F
                        (finished), E (exiting), all (default: all)
  --columns COLUMNS     Comma-separated list of columns to display: job_id,
                        name, owner, project, allocation, state, queue, nodes,
                        walltime, submit_time, start_time, end_time, queued,
                        runtime, exit_status, cores
  --sort SORT           Column to sort by: job_id, name, owner, project,
                        allocation, state, queue, nodes, walltime,
                        submit_time, start_time, end_time, queued, runtime
                        (default: submit_time)
  --reverse             Sort in reverse order
  --limit LIMIT         Maximum number of jobs to show (default: 100)
  --include-pbs-history
                        Also include recent completed jobs from qstat -x
```

## database

```
usage: pbs-monitor database [-h]
                            {init,migrate,status,validate,backup,restore,cleanup} ...

positional arguments:
  {init,migrate,status,validate,backup,restore,cleanup}
                        Database management actions
    init                Initialize database with fresh schema
    migrate             Migrate database to latest schema
    status              Show database status and information
    validate            Validate database schema
    backup              Create database backup
    restore             Restore database from backup
    cleanup             Clean up old data from database

options:
  -h, --help            show this help message and exit
```

### database init

```
usage: pbs-monitor database init [-h] [--force]

options:
  -h, --help  show this help message and exit
  --force     Force initialization (drops existing tables)
```

### database migrate

```
usage: pbs-monitor database migrate [-h]

options:
  -h, --help  show this help message and exit
```

### database status

```
usage: pbs-monitor database status [-h]

options:
  -h, --help  show this help message and exit
```

### database validate

```
usage: pbs-monitor database validate [-h]

options:
  -h, --help  show this help message and exit
```

### database backup

```
usage: pbs-monitor database backup [-h] [backup_path]

positional arguments:
  backup_path  Backup file path (optional)

options:
  -h, --help   show this help message and exit
```

### database restore

```
usage: pbs-monitor database restore [-h] backup_path

positional arguments:
  backup_path  Backup file path to restore from

options:
  -h, --help   show this help message and exit
```

### database cleanup

```
usage: pbs-monitor database cleanup [-h] [--job-history-days JOB_HISTORY_DAYS]
                                    [--snapshot-days SNAPSHOT_DAYS] [--force]

options:
  -h, --help            show this help message and exit
  --job-history-days JOB_HISTORY_DAYS
                        Keep job history for N days (default: 365)
  --snapshot-days SNAPSHOT_DAYS
                        Keep snapshots for N days (default: 90)
  --force               Skip confirmation prompt
```

## daemon

```
usage: pbs-monitor daemon [-h] {start,stop,status} ...

positional arguments:
  {start,stop,status}  Daemon management actions
    start              Start background data collection daemon
    stop               Stop background data collection daemon
    status             Show daemon status and recent collection activity

options:
  -h, --help           show this help message and exit
```

### daemon start

```
usage: pbs-monitor daemon start [-h] [--detach] [--pid-file PID_FILE]

options:
  -h, --help           show this help message and exit
  --detach             Run daemon in background (detached mode)
  --pid-file PID_FILE  PID file path (default: ~/.pbs_monitor_daemon.pid)
```

### daemon stop

```
usage: pbs-monitor daemon stop [-h] [--pid-file PID_FILE]

options:
  -h, --help           show this help message and exit
  --pid-file PID_FILE  PID file path (default: ~/.pbs_monitor_daemon.pid)
```

### daemon status

```
usage: pbs-monitor daemon status [-h]

options:
  -h, --help  show this help message and exit
```

## resv

```
usage: pbs-monitor resv [-h] {list,show} ...

positional arguments:
  {list,show}  Reservation actions
    list       List reservations
    show       Show detailed reservation information

options:
  -h, --help   show this help message and exit
```

### resv list

```
usage: pbs-monitor resv list [-h] [-u USER] [-s STATE] [-r] [--collect]
                             [--format {table,json}] [--columns COLUMNS]

options:
  -h, --help            show this help message and exit
  -u, --user USER       Filter by user
  -s, --state STATE     Filter by state
  -r, --refresh         Force refresh of data
  --collect             Collect data to database
  --format {table,json}
                        Output format
  --columns COLUMNS     Comma-separated list of columns to display. Available:
                        reservation_id, name, owner, state, type, start_time,
                        end_time, duration, nodes, queue
```

### resv show

```
usage: pbs-monitor resv show [-h] [--format {table,json,yaml}]
                             [reservation_ids ...]

positional arguments:
  reservation_ids       Reservation IDs to show

options:
  -h, --help            show this help message and exit
  --format {table,json,yaml}
                        Output format
```

## analyze

```
usage: pbs-monitor analyze [-h]
                           {run-score,walltime-efficiency-by-user,walltime-efficiency-by-project,reservation-utilization,reservation-trends,reservation-owner-ranking} ...

positional arguments:
  {run-score,walltime-efficiency-by-user,walltime-efficiency-by-project,reservation-utilization,reservation-trends,reservation-owner-ranking}
                        Analysis actions
    run-score           Analyze job scores at queue → run transitions
    walltime-efficiency-by-user
                        Analyze walltime efficiency by user
    walltime-efficiency-by-project
                        Analyze walltime efficiency by project
    reservation-utilization
                        Analyze reservation utilization efficiency
    reservation-trends  Analyze reservation utilization trends over time
    reservation-owner-ranking
                        Rank reservation owners by utilization efficiency

options:
  -h, --help            show this help message and exit
```

### analyze run-score

```
usage: pbs-monitor analyze run-score [-h] [-d DAYS] [--format {table,csv}]

options:
  -h, --help            show this help message and exit
  -d, --days DAYS       Number of days to analyze (default: 30)
  --format {table,csv}  Output format (default: table)
```

