# PBS Monitor

A comprehensive Python toolkit for monitoring and managing PBS (Portable Batch System) scheduler environments. This tool provides command-line interfaces to understand queue status, predict job start times, and optimize resource usage with persistent historical data storage.

## Features

### Phase 1 (Completed)
- **PBS Command Abstractions**: Wrapper around PBS CLI tools with JSON parsing
- **Data Collection**: Automated gathering of job, queue, and node information
- **Command Line Interface**: Easy-to-use CLI for monitoring PBS systems
- **Configuration Management**: Flexible configuration system
- **Rich Output**: Beautiful table displays with color support

### Phase 2 (Current - Database Implementation)
- **Persistent Storage**: SQLite for development, PostgreSQL for production
- **Historical Data**: Overcome PBS's 1-week history limitation
- **Database Management**: Complete CLI for database operations
- **Concurrent Access**: Multi-user and multi-process support
- **Data Quality**: Validation, auditing, and error handling
- **Migration System**: Automated schema updates and data management

### Planned Features (Phase 3+)
- **Prediction Engine**: Machine learning-based job start time prediction
- **Background Daemon**: Continuous data collection service
- **Web Dashboard**: Real-time monitoring interface
- **Historical Analysis**: Trend analysis and reporting
- **Optimization Suggestions**: Resource usage recommendations

## Documentation

For comprehensive documentation, see the [docs](docs/) folder:
- **[Database Documentation](docs/DATABASE.md)** - Database setup, configuration, and usage
- **[Phase 2 Database Plan](docs/PHASE2_DATABASE_PLAN.md)** - Detailed implementation plan
- **[Documentation Index](docs/README.md)** - Complete documentation overview

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

### Database Setup (Phase 2)
```bash
# Initialize database (creates SQLite database in ~/.pbs_monitor.db by default)
pbs-monitor database init

# Verify installation
pbs-monitor database status

# Show database information
pbs-monitor database validate
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

### Database Management
```bash
# Initialize database
pbs-monitor database init

# Check database status
pbs-monitor database status

# Backup database (SQLite only)
pbs-monitor database backup

# Clean up old data
pbs-monitor database cleanup --job-history-days 365
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

### Core Commands

#### `status`
Show PBS system status summary.

**Options:**
- `-r, --refresh`: Force refresh of data

#### `jobs`
Show job information with persistent historical data.

**Options:**
- `-u, --user`: Filter by username
- `-s, --state`: Filter by job state (R, Q, H, W, T, E, S, C, F)
- `-r, --refresh`: Force refresh of data
- `--columns`: Comma-separated list of columns to display
- `--sort`: Sort by column (job_id, name, owner, state, queue, score, etc.)

#### `nodes`
Show node information with utilization history.

**Options:**
- `-s, --state`: Filter by node state
- `-r, --refresh`: Force refresh of data
- `--columns`: Comma-separated list of columns to display
- `-d, --detailed`: Show detailed table format

#### `queues`
Show queue information with historical metrics.

**Options:**
- `-r, --refresh`: Force refresh of data
- `--columns`: Comma-separated list of columns to display

### Database Commands

#### `database init`
Initialize database with fresh schema.

**Options:**
- `--force`: Force initialization (drops existing tables)

#### `database status`
Show database information and table counts.

#### `database validate`
Validate database schema and data integrity.

#### `database backup [path]`
Create database backup (SQLite only).

#### `database restore <path>`
Restore database from backup (SQLite only).

#### `database cleanup`
Clean up old data from database.

**Options:**
- `--job-history-days`: Keep job history for N days (default: 365)
- `--snapshot-days`: Keep snapshots for N days (default: 90)
- `--force`: Skip confirmation prompt

#### `database migrate`
Migrate database to latest schema version.

For detailed command documentation, see [Database Documentation](docs/DATABASE.md).

## Configuration

PBS Monitor uses YAML configuration files searched in order:
1. `~/.pbs_monitor.yaml`
2. `~/.config/pbs_monitor/config.yaml`
3. `/etc/pbs_monitor/config.yaml`
4. `pbs_monitor.yaml` (current directory)

### Basic Configuration
```yaml
# PBS system configuration
pbs:
  command_timeout: 30
  job_refresh_interval: 30
  node_refresh_interval: 60
  
# Database configuration
database:
  url: "sqlite:///~/.pbs_monitor.db"  # SQLite for development
  # url: "postgresql://user:password@host:port/database"  # PostgreSQL for production
  pool_size: 5
  
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

### Database Configuration

#### SQLite (Development)
```yaml
database:
  url: "sqlite:///~/.pbs_monitor.db"
  pool_size: 5
```

#### PostgreSQL (Production)
```yaml
database:
  url: "postgresql://pbs_monitor:password@localhost:5432/pbs_monitor"
  pool_size: 10
  max_overflow: 20
```

#### Environment Variables
```bash
# Override database URL
export PBS_MONITOR_DB_URL="postgresql://user:password@host:port/database"
```

## Development

### Project Structure
```
pbs_monitor/
├── __init__.py
├── config.py              # Configuration management
├── pbs_commands.py        # PBS command wrappers
├── data_collector.py      # Data collection orchestration
├── database/              # Database system (Phase 2)
│   ├── models.py          # SQLAlchemy models
│   ├── connection.py      # Database connection management
│   ├── repositories.py    # Data access layer
│   ├── migrations.py      # Database migrations
│   └── model_converters.py # PBS to database model conversion
├── models/                # PBS data models
│   ├── job.py             # Job data structure
│   ├── queue.py           # Queue data structure
│   └── node.py            # Node data structure
├── utils/                 # Utility functions
│   ├── logging_setup.py   # Logging configuration
│   └── formatters.py      # Output formatters
└── cli/                   # Command line interface
    ├── main.py            # Main CLI entry point
    └── commands.py        # Command implementations
```

### Key Components

#### Database System (Phase 2)
- **Models**: SQLAlchemy models for persistent storage
- **Repositories**: Data access layer with query optimization
- **Migrations**: Schema versioning and updates
- **Converters**: Bridge between PBS and database models

#### Data Collection
- **Real-time**: On-demand updates when commands are run
- **Historical**: Persistent storage of job lifecycles
- **Audit Trail**: Complete logging of collection events

### Testing
```bash
# Run tests
pytest

# Run with coverage
pytest --cov=pbs_monitor

# Test database functionality
pytest tests/test_database.py
```

### Code Quality
```bash
# Format code
black pbs_monitor/

# Lint code
flake8 pbs_monitor/
```

## Deployment

### Single User (SQLite)
```bash
# Install and initialize
pip install -e .
pbs-monitor database init
pbs-monitor config --create
```

### Multi-User (PostgreSQL)
```bash
# Setup PostgreSQL database
createdb pbs_monitor

# Configure connection
export PBS_MONITOR_DB_URL="postgresql://user:password@host:port/pbs_monitor"

# Initialize database
pbs-monitor database init

# Validate installation
pbs-monitor database status
```

## Troubleshooting

### Common Issues

#### Database Connection
```bash
# Check database status
pbs-monitor database status

# Validate schema
pbs-monitor database validate

# Re-initialize if needed
pbs-monitor database init --force
```

#### PBS Connection
```bash
# Test PBS commands
qstat -V
pbsnodes -a | head

# Check PATH
which qstat
which pbsnodes
```

#### Performance
```bash
# Clean up old data
pbs-monitor database cleanup

# Check database size
pbs-monitor database status
```

### Debug Mode
```bash
# Enable debug logging
pbs-monitor -v status

# Enable database query logging
echo "database: { echo_sql: true }" >> ~/.pbs_monitor.yaml
```

For detailed troubleshooting, see [Database Documentation](docs/DATABASE.md#troubleshooting).

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests for new functionality
5. Ensure all tests pass
6. Update documentation
7. Submit a pull request

## License

This project is licensed under the MIT License - see the LICENSE file for details.

## Support

For issues and questions:
- Check the [documentation](docs/)
- Review [troubleshooting guide](docs/DATABASE.md#troubleshooting)
- Create an issue at https://github.com/jtchilders/pbs_monitor/issues

## Roadmap

### Phase 2 (Current) - Database Implementation ✅
- Persistent storage with SQLite/PostgreSQL
- Historical data collection beyond PBS limits
- Database management CLI
- Concurrent access support

### Phase 3 (Planned) - Analytics & Prediction
- Machine learning prediction engine
- Background daemon for continuous collection
- Advanced historical analysis
- Performance optimization recommendations

### Phase 4 (Future) - Web Interface
- Real-time web dashboard
- REST API for integrations
- Advanced visualization
- Multi-cluster support

For detailed roadmap, see [Phase 2 Database Plan](docs/PHASE2_DATABASE_PLAN.md). 