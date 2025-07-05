# PBS Monitor

A comprehensive Python toolkit for monitoring and managing PBS (Portable Batch System) scheduler environments. This tool provides command-line interfaces to understand queue status, predict job start times, and optimize resource usage.

## Features

### Phase 1 (Current)
- **PBS Command Abstractions**: Wrapper around PBS CLI tools with JSON parsing
- **Data Collection**: Automated gathering of job, queue, and node information
- **Command Line Interface**: Easy-to-use CLI for monitoring PBS systems
- **Configuration Management**: Flexible configuration system
- **Rich Output**: Beautiful table displays with color support

### Planned Features
- **Prediction Engine**: Machine learning-based job start time prediction
- **Web Dashboard**: Real-time monitoring interface
- **Historical Analysis**: Trend analysis and reporting
- **Optimization Suggestions**: Resource usage recommendations

## Installation

### Prerequisites
- Python 3.8 or higher
- PBS Pro or OpenPBS installed and configured
- PBS commands (`qstat`, `qsub`, `pbsnodes`, etc.) available in PATH

### Install from Source
```bash
git clone https://github.com/jtchilders/pbs_monitor.git
cd pbs_monitor
pip install -r requirements.txt
pip install -e .
```

## Quick Start

### Basic Usage
```bash
# Show system status
pbs-monitor status

# Show all jobs
pbs-monitor jobs

# Show jobs for a specific user
pbs-monitor jobs -u myuser

# Show node information
pbs-monitor nodes

# Show queue information
pbs-monitor queues
```

### Configuration
```bash
# Create sample configuration
pbs-monitor config --create

# Show current configuration
pbs-monitor config --show
```

## Command Reference

### Global Options
- `-c, --config`: Specify configuration file path
- `-v, --verbose`: Enable verbose logging
- `-q, --quiet`: Suppress normal output
- `--log-file`: Specify log file path

### Commands

#### `status`
Show PBS system status summary.

**Options:**
- `-r, --refresh`: Force refresh of data

**Example:**
```bash
pbs-monitor status
```

#### `jobs`
Show job information.

**Options:**
- `-u, --user`: Filter by username
- `-s, --state`: Filter by job state (R, Q, H, W, T, E, S, C, F)
- `-r, --refresh`: Force refresh of data
- `--columns`: Comma-separated list of columns to display

**Available Columns:**
- `job_id`: Job identifier
- `name`: Job name
- `owner`: Job owner
- `state`: Job state
- `queue`: Queue name
- `nodes`: Number of nodes
- `ppn`: Processors per node
- `walltime`: Requested walltime
- `memory`: Requested memory
- `submit_time`: Submission time
- `start_time`: Start time
- `runtime`: Current runtime
- `priority`: Job priority
- `cores`: Total cores requested

**Examples:**
```bash
# Show all jobs
pbs-monitor jobs

# Show jobs for user 'alice'
pbs-monitor jobs -u alice

# Show only running jobs
pbs-monitor jobs -s R

# Show specific columns
pbs-monitor jobs --columns job_id,name,owner,state,queue
```

#### `nodes`
Show node information.

**Options:**
- `-s, --state`: Filter by node state
- `-r, --refresh`: Force refresh of data
- `--columns`: Comma-separated list of columns to display

**Available Columns:**
- `name`: Node name
- `state`: Node state
- `ncpus`: Number of CPUs
- `memory`: Memory amount
- `jobs`: Number of jobs
- `load`: Load percentage
- `utilization`: CPU utilization
- `available`: Available CPUs
- `properties`: Node properties

**Examples:**
```bash
# Show all nodes
pbs-monitor nodes

# Show only free nodes
pbs-monitor nodes -s free

# Show specific columns
pbs-monitor nodes --columns name,state,ncpus,jobs,utilization
```

#### `queues`
Show queue information.

**Options:**
- `-r, --refresh`: Force refresh of data
- `--columns`: Comma-separated list of columns to display

**Available Columns:**
- `name`: Queue name
- `state`: Queue state
- `type`: Queue type
- `running`: Running jobs
- `queued`: Queued jobs
- `total`: Total jobs
- `max_running`: Maximum running jobs
- `max_queued`: Maximum queued jobs
- `utilization`: Utilization percentage
- `available`: Available slots
- `priority`: Queue priority
- `max_walltime`: Maximum walltime
- `max_nodes`: Maximum nodes

**Examples:**
```bash
# Show all queues
pbs-monitor queues

# Show specific columns
pbs-monitor queues --columns name,state,running,queued,utilization
```

## Configuration

PBS Monitor uses YAML configuration files. The configuration file is searched in the following locations:
1. `~/.pbs_monitor.yaml`
2. `~/.config/pbs_monitor/config.yaml`
3. `/etc/pbs_monitor/config.yaml`
4. `pbs_monitor.yaml` (current directory)

### Configuration Sections

#### PBS Configuration
```yaml
pbs:
  command_timeout: 30          # Timeout for PBS commands
  default_queue: default       # Default queue name
  job_refresh_interval: 30     # Job data refresh interval (seconds)
  node_refresh_interval: 60    # Node data refresh interval (seconds)
  queue_refresh_interval: 300  # Queue data refresh interval (seconds)
```

#### Display Configuration
```yaml
display:
  max_table_width: 120        # Maximum table width
  truncate_long_names: true   # Truncate long names
  max_name_length: 20         # Maximum name length
  use_colors: true           # Enable color output
  time_format: "%d-%m %H:%M" # Time format
  default_job_columns:       # Default job columns
    - job_id
    - name
    - owner
    - state
    - queue
    - nodes
    - ppn
    - walltime
```

#### Logging Configuration
```yaml
logging:
  level: INFO                 # Log level
  log_file: null             # Log file path (null for no file)
  log_format: "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
  date_format: "%d-%m %H:%M" # Date format
```

## Development

### Project Structure
```
pbs_monitor/
├── __init__.py
├── config.py              # Configuration management
├── pbs_commands.py        # PBS command wrappers
├── data_collector.py      # Data collection orchestration
├── models/               # Data models
│   ├── job.py           # Job data structure
│   ├── queue.py         # Queue data structure
│   └── node.py          # Node data structure
├── utils/               # Utility functions
│   ├── logging_setup.py # Logging configuration
│   └── formatters.py    # Output formatters
└── cli/                # Command line interface
    ├── main.py         # Main CLI entry point
    └── commands.py     # Command implementations
```

### Testing
```bash
# Run tests
pytest

# Run with coverage
pytest --cov=pbs_monitor
```

### Code Quality
```bash
# Format code
black pbs_monitor/

# Lint code
flake8 pbs_monitor/
```

## Troubleshooting

### Common Issues

#### "Unable to connect to PBS system"
- Ensure PBS commands are available in PATH
- Check if PBS server is running
- Verify user has appropriate permissions

#### "Command not found: qstat"
- Install PBS Pro or OpenPBS
- Add PBS bin directory to PATH
- Check PBS installation

#### "Permission denied"
- Ensure user has PBS access permissions
- Check PBS server configuration
- Verify network connectivity to PBS server

### Debug Mode
Enable debug logging for troubleshooting:
```bash
pbs-monitor -v status
```

## Contributing

1. Fork the repository at https://github.com/jtchilders/pbs_monitor
2. Create a feature branch
3. Make your changes
4. Add tests for new functionality
5. Ensure all tests pass
6. Submit a pull request

## License

This project is licensed under the MIT License - see the LICENSE file for details.

## Support

For issues and questions:
- Check the troubleshooting section above
- Create an issue at https://github.com/jtchilders/pbs_monitor/issues
- Review the configuration documentation

## Roadmap

### Phase 2 (Planned)
- Machine learning prediction engine
- Historical data analysis
- Performance optimization recommendations

### Phase 3 (Planned)
- Web-based dashboard
- Real-time monitoring
- Advanced analytics and reporting

### Phase 4 (Planned)
- Multi-cluster support
- Integration with other schedulers
- Advanced optimization features 