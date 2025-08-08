# PBS Reservations Implementation Plan

## 🎉 Implementation Status: COMPLETED

**All planned phases have been successfully implemented!** The PBS Monitor toolkit now includes full reservation monitoring and analysis capabilities.

### Available Commands:
- `pbs-monitor resv list` - List reservations with filtering options
- `pbs-monitor resv show [ids...]` - Show detailed reservation information  
- `pbs-monitor analyze reservation-utilization` - Analyze reservation efficiency
- `pbs-monitor analyze reservation-trends` - View utilization trends over time
- `pbs-monitor analyze reservation-owner-ranking` - Rank owners by efficiency

### Key Features Implemented:
- **Data Models**: Complete reservation data structures with state tracking
- **PBS Command Integration**: Parsing of `pbs_rstat` and `pbs_rstat -f` output
- **Database Schema**: Full reservation tables with history and utilization tracking
- **CLI Interface**: Comprehensive command-line interface with filtering and formatting
- **Analytics Engine**: Utilization analysis, trend analysis, and efficiency ranking
- **Sample Data**: Complete test data for development and validation

## Overview

This document outlines the comprehensive plan for adding PBS reservation monitoring and analysis capabilities to the PBS Monitor toolkit. Reservations are an important part of resource allocation in PBS systems, and this enhancement provides visibility into reservation usage patterns and utilization efficiency.

## Background & Context

### Current PBS Reservation Commands
- `pbs_rstat` - Lists reservation summary (tabular format, no JSON support)
- `pbs_rstat -f [reservation_id]` - Detailed reservation information (key-value format, no JSON support)
- `/opt/pbs/bin/pbs_rstat` is the typical command path

### Parsing Challenge
Unlike other PBS commands (qstat, pbsnodes) that support JSON output via `-F json`, `pbs_rstat` only outputs in text format:

**Summary format (`pbs_rstat`):**
```
Resv ID         Queue         User     State             Start / Duration / End              
-------------------------------------------------------------------------------
S6703362.aurora S6703362      richp@au RN          Today 10:00 / 14400 / Today 14:00      
R6710677.aurora R6710677      richp@au RN          Today 08:00 / 39600 / Today 19:00      
```

**Detailed format (`pbs_rstat -f <id>`):**
```
Resv ID: S6703362.aurora-pbs-0001.hostmgmt.cm.aurora.alcf.anl.gov
Reserve_Name = HACC-DAOS-Dbg
Reserve_Owner = richp@aurora-uan-0010.hostmgmt1000.cm.aurora.alcf.anl.gov
reserve_state = RESV_RUNNING
reserve_start = Wed Aug 06 10:00:00 2025
Resource_List.nodect = 8200
```

## Implementation Strategy

### Phase 1: Core Infrastructure (Essential)

#### 1.1 Data Models

**Create `pbs_monitor/models/reservation.py`:**

```python
from enum import Enum
from dataclasses import dataclass
from typing import Optional, Dict, Any, List
from datetime import datetime

class ReservationState(Enum):
    CONFIRMED = "RESV_CONFIRMED"      # Scheduled but not started
    RUNNING = "RESV_RUNNING"          # Currently active
    FINISHED = "RESV_FINISHED"        # Completed
    DELETED = "RESV_DELETED"          # Cancelled
    DEGRADED = "RESV_DEGRADED"        # Some nodes unavailable
    UNKNOWN = "unknown"

@dataclass
class PBSReservation:
    """Represents a PBS reservation"""
    
    # Core identifiers
    reservation_id: str                    # e.g., "S6703362.aurora-pbs-0001..."
    reservation_name: Optional[str] = None # e.g., "HACC-DAOS-Dbg"
    owner: Optional[str] = None            # Username without hostname
    
    # State and timing
    state: ReservationState = ReservationState.UNKNOWN
    start_time: Optional[datetime] = None
    end_time: Optional[datetime] = None
    duration_seconds: Optional[int] = None
    
    # Resources
    queue: Optional[str] = None            # Associated queue
    nodes: Optional[int] = None            # Node count
    ncpus: Optional[int] = None            # Total CPUs
    ngpus: Optional[int] = None            # Total GPUs
    walltime: Optional[str] = None         # HH:MM:SS format
    
    # Access control
    authorized_users: List[str] = field(default_factory=list)
    authorized_groups: List[str] = field(default_factory=list)
    
    # Additional metadata
    server: Optional[str] = None
    creation_time: Optional[datetime] = None
    modification_time: Optional[datetime] = None
    partition: Optional[str] = None
    
    # Reserved nodes (if available)
    reserved_nodes: Optional[str] = None   # Formatted node list
    
    # Raw PBS attributes
    raw_attributes: Dict[str, Any] = field(default_factory=dict)
    
    @classmethod
    def from_detailed_output(cls, reservation_text: str) -> 'PBSReservation':
        """Parse detailed pbs_rstat -f output into PBSReservation object"""
        # Implementation details in parsing section
        
    @classmethod
    def from_summary_line(cls, summary_line: str) -> 'PBSReservation':
        """Parse single line from pbs_rstat summary into PBSReservation object"""
        # Implementation details in parsing section
```

#### 1.2 Command Parsing

**Extend `pbs_monitor/pbs_commands.py`:**

```python
def pbs_rstat_summary(self) -> List[PBSReservation]:
    """Get reservation summary list"""
    if self.use_sample_data:
        return self._load_sample_reservations_summary()
    
    output = self._run_command(["/opt/pbs/bin/pbs_rstat"])
    return self._parse_rstat_summary(output)

def pbs_rstat_detailed(self, reservation_id: str) -> PBSReservation:
    """Get detailed reservation information"""
    if self.use_sample_data:
        return self._load_sample_reservation_detail(reservation_id)
    
    output = self._run_command(["/opt/pbs/bin/pbs_rstat", "-f", reservation_id])
    return self._parse_rstat_detailed(output)

def pbs_rstat_all_detailed(self) -> List[PBSReservation]:
    """Get detailed information for all reservations"""
    # Strategy: Get summary first, then detailed for each ID
    summary_reservations = self.pbs_rstat_summary()
    detailed_reservations = []
    
    for reservation in summary_reservations:
        try:
            detailed = self.pbs_rstat_detailed(reservation.reservation_id)
            detailed_reservations.append(detailed)
        except Exception as e:
            self.logger.warning(f"Failed to get details for {reservation.reservation_id}: {e}")
            # Fall back to summary data
            detailed_reservations.append(reservation)
    
    return detailed_reservations

def _parse_rstat_summary(self, output: str) -> List[PBSReservation]:
    """Parse pbs_rstat summary output"""
    reservations = []
    lines = output.strip().split('\n')
    
    # Skip header lines
    data_lines = [line for line in lines if not line.startswith('Resv ID') and not line.startswith('---')]
    
    for line in data_lines:
        if line.strip():
            try:
                reservation = PBSReservation.from_summary_line(line)
                reservations.append(reservation)
            except Exception as e:
                self.logger.warning(f"Failed to parse reservation line: {line[:50]}... Error: {e}")
    
    return reservations

def _parse_rstat_detailed(self, output: str) -> PBSReservation:
    """Parse pbs_rstat -f detailed output"""
    return PBSReservation.from_detailed_output(output)
```

#### 1.3 Database Schema

**Add to `pbs_monitor/database/models.py`:**

```python
class ReservationState(enum.Enum):
    CONFIRMED = "RESV_CONFIRMED"
    RUNNING = "RESV_RUNNING"
    FINISHED = "RESV_FINISHED"
    DELETED = "RESV_DELETED"
    DEGRADED = "RESV_DEGRADED"
    UNKNOWN = "unknown"

class Reservation(Base):
    """
    Core reservation tracking table - represents current/final state of reservations
    
    Similar to jobs table but for PBS reservations.
    Historical state changes tracked in reservation_history table.
    """
    __tablename__ = 'reservations'
    
    # Primary identifiers
    reservation_id = Column(String(200), primary_key=True)  # Full ID can be long
    reservation_name = Column(String(200))
    owner = Column(String(50), index=True)
    
    # Current state
    state = Column(SQLEnum(ReservationState), index=True)
    queue = Column(String(50), index=True)
    
    # Resource allocation
    nodes = Column(Integer)
    ncpus = Column(Integer)
    ngpus = Column(Integer)
    walltime = Column(String(20))  # HH:MM:SS format
    
    # Timing information
    start_time = Column(DateTime(timezone=True))
    end_time = Column(DateTime(timezone=True))
    duration_seconds = Column(Integer)
    creation_time = Column(DateTime(timezone=True))
    modification_time = Column(DateTime(timezone=True))
    
    # Access control
    authorized_users = Column(JSON)  # Array of usernames
    authorized_groups = Column(JSON)  # Array of group names
    
    # Additional metadata
    server = Column(String(100))
    partition = Column(String(50))
    reserved_nodes = Column(Text)  # Can be very long
    
    # System tracking
    first_seen = Column(DateTime(timezone=True), default=func.now())
    last_updated = Column(DateTime(timezone=True), default=func.now(), onupdate=func.now())
    final_state_recorded = Column(Boolean, default=False)
    
    # Raw data
    raw_pbs_data = Column(JSON)  # Store original PBS text output
    
    # Relationships
    history = relationship("ReservationHistory", back_populates="reservation", order_by="ReservationHistory.timestamp")
    utilization_analyses = relationship("ReservationUtilization", back_populates="reservation")
    
    # Indexes
    __table_args__ = (
        Index('ix_reservations_owner_state', 'owner', 'state'),
        Index('ix_reservations_start_end', 'start_time', 'end_time'),
        Index('ix_reservations_state_updated', 'state', 'last_updated'),
    )

class ReservationHistory(Base):
    """
    Historical reservation state changes - tracks reservation lifecycle
    
    Similar to job_history but for reservations.
    """
    __tablename__ = 'reservation_history'
    
    id = Column(Integer, primary_key=True)
    reservation_id = Column(String(200), ForeignKey('reservations.reservation_id'), index=True)
    timestamp = Column(DateTime(timezone=True), default=func.now())
    
    # State at this point in time
    state = Column(SQLEnum(ReservationState))
    
    # System info
    data_collection_id = Column(Integer, ForeignKey('data_collection_log.id'))
    
    # Relationships
    reservation = relationship("Reservation", back_populates="history")
    collection_event = relationship("DataCollectionLog")
    
    # Indexes
    __table_args__ = (
        Index('ix_reservation_history_reservation_timestamp', 'reservation_id', 'timestamp'),
        Index('ix_reservation_history_state_timestamp', 'state', 'timestamp'),
    )

class ReservationUtilization(Base):
    """
    Reservation utilization analysis results
    
    Stores calculated metrics about how well reservations were used.
    """
    __tablename__ = 'reservation_utilization'
    
    id = Column(Integer, primary_key=True)
    reservation_id = Column(String(200), ForeignKey('reservations.reservation_id'), index=True)
    analysis_timestamp = Column(DateTime(timezone=True), default=func.now())
    
    # Utilization metrics
    total_node_hours_reserved = Column(Float)  # nodes * duration_hours
    total_node_hours_used = Column(Float)      # Sum of job node-hours
    utilization_percentage = Column(Float)     # used / reserved * 100
    
    # Job statistics
    jobs_submitted = Column(Integer)           # Jobs submitted to reservation queue
    jobs_completed = Column(Integer)           # Jobs that completed successfully
    jobs_failed = Column(Integer)              # Jobs that failed
    
    # Resource efficiency
    cpu_hours_reserved = Column(Float)
    cpu_hours_used = Column(Float)
    cpu_utilization_percentage = Column(Float)
    
    gpu_hours_reserved = Column(Float, nullable=True)
    gpu_hours_used = Column(Float, nullable=True)
    gpu_utilization_percentage = Column(Float, nullable=True)
    
    # Peak usage
    peak_nodes_used = Column(Integer)
    peak_usage_timestamp = Column(DateTime(timezone=True))
    
    # Analysis metadata
    analysis_method = Column(String(50))  # e.g., "job_queue_analysis"
    jobs_analyzed = Column(Integer)       # Number of jobs included in analysis
    
    # Relationships
    reservation = relationship("Reservation", back_populates="utilization_analyses")
    
    # Indexes
    __table_args__ = (
        Index('ix_reservation_utilization_reservation_analysis', 'reservation_id', 'analysis_timestamp'),
        Index('ix_reservation_utilization_utilization', 'utilization_percentage'),
    )
```

#### 1.4 Database Migration

**Add migration in `pbs_monitor/database/migrations.py`:**

```python
def migrate_to_v1_1_reservations(self) -> None:
    """Add reservation tables for version 1.1"""
    logger.info("Migrating to v1.1 - Adding reservation tables")
    
    # Import reservation models
    from .models import Reservation, ReservationHistory, ReservationUtilization
    
    try:
        # Check if tables already exist
        inspector = inspect(self.db_manager.engine)
        existing_tables = inspector.get_table_names()
        
        new_tables = ['reservations', 'reservation_history', 'reservation_utilization']
        tables_to_create = [table for table in new_tables if table not in existing_tables]
        
        if tables_to_create:
            logger.info(f"Creating reservation tables: {', '.join(tables_to_create)}")
            
            # Create only the new tables
            Reservation.__table__.create(self.db_manager.engine, checkfirst=True)
            ReservationHistory.__table__.create(self.db_manager.engine, checkfirst=True)
            ReservationUtilization.__table__.create(self.db_manager.engine, checkfirst=True)
            
            logger.info("Reservation tables created successfully")
        else:
            logger.info("Reservation tables already exist")
            
        # Update schema version
        self._update_schema_version("1.1.0")
        
    except Exception as e:
        logger.error(f"Failed to create reservation tables: {str(e)}")
        raise
```

### Phase 2: CLI Integration (Essential)

#### 2.1 Reservation Commands

**Create `pbs_monitor/cli/reservation_commands.py`:**

```python
class ReservationsCommand(BaseCommand):
    """Handle reservation listing and details"""
    
    def execute(self, args: argparse.Namespace) -> int:
        if args.reservation_action == "list":
            return self._list_reservations(args)
        elif args.reservation_action == "show":
            return self._show_reservation_details(args)
        else:
            print(f"Unknown reservation action: {args.reservation_action}")
            return 1
    
    def _list_reservations(self, args: argparse.Namespace) -> int:
        """List reservations with summary information"""
        try:
            reservations = self.collector.get_reservations()
            
            if args.collect:
                self.collector.collect_and_persist()
            
            # Apply filters
            filtered_reservations = self._filter_reservations(reservations, args)
            
            if not filtered_reservations:
                print("No reservations found matching criteria")
                return 0
            
            # Display table
            self._display_reservations_table(filtered_reservations, args)
            return 0
            
        except Exception as e:
            self.logger.error(f"Failed to list reservations: {str(e)}")
            print(f"Error: {str(e)}")
            return 1
    
    def _show_reservation_details(self, args: argparse.Namespace) -> int:
        """Show detailed information for specific reservation(s)"""
        try:
            if args.reservation_ids:
                # Show specific reservations
                reservations = []
                for res_id in args.reservation_ids:
                    try:
                        reservation = self.collector.pbs_commands.pbs_rstat_detailed(res_id)
                        reservations.append(reservation)
                    except Exception as e:
                        print(f"Warning: Could not get details for {res_id}: {e}")
            else:
                # Show all reservations with details
                reservations = self.collector.pbs_commands.pbs_rstat_all_detailed()
            
            if not reservations:
                print("No reservations found")
                return 0
            
            # Display detailed information
            self._display_reservation_details(reservations, args)
            return 0
            
        except Exception as e:
            self.logger.error(f"Failed to show reservation details: {str(e)}")
            print(f"Error: {str(e)}")
            return 1
```

#### 2.2 CLI Integration

**Update `pbs_monitor/cli/main.py`:**

```python
def create_parser() -> argparse.ArgumentParser:
    # Add reservations subcommand
    reservations_parser = subparsers.add_parser(
        "resv",
        help="Reservation information and management",
        aliases=["reservations", "reserv"]
    )
    
    # Reservation subcommands
    resv_subparsers = reservations_parser.add_subparsers(
        dest="reservation_action",
        help="Reservation actions"
    )
    
    # List reservations
    list_parser = resv_subparsers.add_parser(
        "list",
        help="List reservations",
        aliases=["ls"]
    )
    list_parser.add_argument("-u", "--user", help="Filter by user")
    list_parser.add_argument("-s", "--state", help="Filter by state")
    list_parser.add_argument("--collect", action="store_true", help="Collect data to database")
    list_parser.add_argument("--format", choices=["table", "json"], default="table")
    
    # Show reservation details  
    show_parser = resv_subparsers.add_parser(
        "show",
        help="Show detailed reservation information"
    )
    show_parser.add_argument("reservation_ids", nargs="*", help="Reservation IDs to show")
    show_parser.add_argument("--format", choices=["table", "json", "yaml"], default="table")

# Update main() function
elif args.command in ["resv", "reservations", "reserv"]:
    cmd = ReservationsCommand(collector, config)
    return cmd.execute(args)
```

### Phase 3: Data Collection Integration (Essential)

#### 3.1 Data Collector Updates

**Update `pbs_monitor/data_collector.py`:**

```python
class DataCollector:
    def __init__(self, config: Config, use_sample_data: bool = False):
        # Add reservation tracking
        self._reservations: List[PBSReservation] = []
        self._reservation_state_cache: Dict[str, ReservationState] = {}
    
    def refresh_reservations(self) -> None:
        """Refresh reservation data from PBS"""
        try:
            self._reservations = self.pbs_commands.pbs_rstat_all_detailed()
            self.logger.debug(f"Refreshed {len(self._reservations)} reservations")
        except Exception as e:
            self.logger.error(f"Failed to refresh reservations: {str(e)}")
            self._reservations = []
    
    def get_reservations(self) -> List[PBSReservation]:
        """Get current reservation list"""
        if not self._reservations:
            self.refresh_reservations()
        return self._reservations.copy()
    
    def collect_and_persist(self, collection_type: str = "manual") -> Dict[str, Any]:
        # Update to include reservations
        try:
            # Collect all data including reservations
            self.refresh_all()
            self.refresh_reservations()  # Add this line
            
            # Convert to database models
            db_data = {
                'jobs': [self._model_converters.job.to_database(job) for job in all_jobs_for_db],
                'queues': [self._model_converters.queue.to_database(queue) for queue in self._queues],
                'nodes': [self._model_converters.node.to_database(node) for node in self._nodes],
                'reservations': [self._model_converters.reservation.to_database(reservation) for reservation in self._reservations],  # Add this
                'job_history': self._create_job_history_for_changes(all_jobs_for_db, log_id),
                'reservation_history': self._create_reservation_history_for_changes(self._reservations, log_id),  # Add this
                # ... other existing data
            }
            
            # Update database
            result = self.db_manager.store_collection_data(db_data)
            result['reservations_collected'] = len(self._reservations)  # Add to result
            
            return result
```

#### 3.2 Model Converters

**Add to `pbs_monitor/database/model_converters.py`:**

```python
class ReservationConverter:
    """Converter between PBSReservation and database Reservation models"""
    
    @staticmethod
    def to_database(pbs_reservation: PBSReservation) -> Reservation:
        """Convert PBSReservation to database Reservation model"""
        return Reservation(
            reservation_id=pbs_reservation.reservation_id,
            reservation_name=pbs_reservation.reservation_name,
            owner=pbs_reservation.owner,
            state=ReservationState(pbs_reservation.state.value),
            queue=pbs_reservation.queue,
            
            # Resources
            nodes=pbs_reservation.nodes,
            ncpus=pbs_reservation.ncpus,
            ngpus=pbs_reservation.ngpus,
            walltime=pbs_reservation.walltime,
            
            # Timing
            start_time=pbs_reservation.start_time,
            end_time=pbs_reservation.end_time,
            duration_seconds=pbs_reservation.duration_seconds,
            creation_time=pbs_reservation.creation_time,
            modification_time=pbs_reservation.modification_time,
            
            # Access control
            authorized_users=pbs_reservation.authorized_users,
            authorized_groups=pbs_reservation.authorized_groups,
            
            # Metadata
            server=pbs_reservation.server,
            partition=pbs_reservation.partition,
            reserved_nodes=pbs_reservation.reserved_nodes,
            
            # Raw data
            raw_pbs_data=pbs_reservation.raw_attributes,
            last_updated=datetime.now()
        )
```

### Phase 4: Analysis Features (Future Enhancement)

#### 4.1 Reservation Utilization Analysis

**Create `pbs_monitor/analysis/reservation_analysis.py`:**

```python
class ReservationUtilizationAnalyzer:
    """Analyze reservation utilization efficiency"""
    
    def analyze_reservation_utilization(self, reservation_id: str, 
                                      start_date: Optional[datetime] = None,
                                      end_date: Optional[datetime] = None) -> ReservationUtilization:
        """
        Analyze how well a reservation was utilized by examining:
        1. Jobs submitted to the reservation's queue
        2. Actual node-hours used vs. reserved
        3. Resource efficiency metrics
        """
        
        # Get reservation details
        reservation = self._get_reservation(reservation_id)
        
        # Find jobs that used this reservation
        reservation_jobs = self._find_reservation_jobs(reservation, start_date, end_date)
        
        # Calculate utilization metrics
        metrics = self._calculate_utilization_metrics(reservation, reservation_jobs)
        
        # Store results in database
        utilization = ReservationUtilization(
            reservation_id=reservation_id,
            **metrics
        )
        
        return utilization
    
    def _find_reservation_jobs(self, reservation: Reservation, 
                              start_date: Optional[datetime],
                              end_date: Optional[datetime]) -> List[Job]:
        """Find jobs that submitted to the reservation's queue during the reservation period"""
        
        # Jobs that:
        # 1. Were submitted to the reservation's queue
        # 2. Had submit_time during reservation period
        # 3. Actually ran (not just queued)
        
        query_start = start_date or reservation.start_time
        query_end = end_date or reservation.end_time
        
        with self.db_manager.get_session() as session:
            jobs = session.query(Job).filter(
                Job.queue == reservation.queue,
                Job.submit_time >= query_start,
                Job.submit_time <= query_end,
                Job.state.in_([JobState.RUNNING, JobState.COMPLETED, JobState.FINISHED])
            ).all()
        
        return jobs
    
    def _calculate_utilization_metrics(self, reservation: Reservation, 
                                     jobs: List[Job]) -> Dict[str, Any]:
        """Calculate detailed utilization metrics"""
        
        # Reserved resources
        duration_hours = reservation.duration_seconds / 3600
        total_node_hours_reserved = reservation.nodes * duration_hours
        total_cpu_hours_reserved = reservation.ncpus * duration_hours
        total_gpu_hours_reserved = (reservation.ngpus * duration_hours) if reservation.ngpus else None
        
        # Used resources (from jobs)
        total_node_hours_used = 0
        total_cpu_hours_used = 0
        total_gpu_hours_used = 0
        jobs_completed = 0
        jobs_failed = 0
        
        for job in jobs:
            if job.actual_runtime_seconds and job.nodes:
                job_hours = job.actual_runtime_seconds / 3600
                total_node_hours_used += job.nodes * job_hours
                total_cpu_hours_used += (job.total_cores or job.nodes) * job_hours
                
                # GPU usage (if job used GPUs)
                if hasattr(job, 'ngpus') and job.ngpus:
                    total_gpu_hours_used += job.ngpus * job_hours
            
            if job.state == JobState.COMPLETED:
                jobs_completed += 1
            elif job.state in [JobState.FINISHED]:  # Assume finished = failed for now
                jobs_failed += 1
        
        # Calculate percentages
        node_utilization = (total_node_hours_used / total_node_hours_reserved * 100) if total_node_hours_reserved > 0 else 0
        cpu_utilization = (total_cpu_hours_used / total_cpu_hours_reserved * 100) if total_cpu_hours_reserved > 0 else 0
        gpu_utilization = (total_gpu_hours_used / total_gpu_hours_reserved * 100) if total_gpu_hours_reserved and total_gpu_hours_reserved > 0 else None
        
        return {
            'total_node_hours_reserved': total_node_hours_reserved,
            'total_node_hours_used': total_node_hours_used,
            'utilization_percentage': node_utilization,
            'jobs_submitted': len(jobs),
            'jobs_completed': jobs_completed,
            'jobs_failed': jobs_failed,
            'cpu_hours_reserved': total_cpu_hours_reserved,
            'cpu_hours_used': total_cpu_hours_used,
            'cpu_utilization_percentage': cpu_utilization,
            'gpu_hours_reserved': total_gpu_hours_reserved,
            'gpu_hours_used': total_gpu_hours_used if total_gpu_hours_reserved else None,
            'gpu_utilization_percentage': gpu_utilization,
            'analysis_method': 'job_queue_analysis',
            'jobs_analyzed': len(jobs)
        }
```

#### 4.2 Analysis CLI Commands

**Add to CLI:**

```bash
# Analyze specific reservation
pbs-monitor analyze reservation-utilization R6710677.aurora

# Analyze all reservations in a time period
pbs-monitor analyze reservation-utilization --start 2025-08-01 --end 2025-08-07

# Generate utilization report
pbs-monitor analyze reservation-utilization --report --format json
```

## Detailed Implementation Questions & Decisions

### Q1: Parsing Strategy - Summary First vs Direct Detailed?

**Recommended Approach: Summary First (Two-Stage)**

**Reasoning:**
- `pbs_rstat` summary is faster and lighter weight
- Allows filtering before expensive detailed calls
- Graceful degradation if some detailed calls fail
- Matches the user workflow (list first, then drill down)

**Implementation:**
```python
def get_reservations(self, include_details: bool = False) -> List[PBSReservation]:
    """Get reservations with optional detailed information"""
    
    # Always start with summary for reservation list
    summary_reservations = self.pbs_commands.pbs_rstat_summary()
    
    if not include_details:
        return summary_reservations
    
    # Get detailed information for each reservation
    detailed_reservations = []
    for reservation in summary_reservations:
        try:
            detailed = self.pbs_commands.pbs_rstat_detailed(reservation.reservation_id)
            detailed_reservations.append(detailed)
        except Exception as e:
            # Fall back to summary data for this reservation
            self.logger.warning(f"Could not get details for {reservation.reservation_id}: {e}")
            detailed_reservations.append(reservation)
    
    return detailed_reservations
```

### Q2: Database Migration Strategy

**Recommended Approach: Incremental Migration with Version Tracking**

**Schema Version: 1.0.0 → 1.1.0**

1. **Check Current Version**: Use existing migration system
2. **Add New Tables**: Create reservation tables alongside existing ones
3. **No Breaking Changes**: Existing functionality unaffected
4. **Update Version**: Bump to 1.1.0

**Migration Command:**
```bash
pbs-monitor database migrate  # Automatically applies 1.1.0 migration
```

### Q3: Reservation ID Parsing - Simple vs Robust?

**Recommended Approach: Robust Parsing with Fallbacks**

**Key Parsing Challenges:**
- Reservation IDs can be very long (e.g., "S6703362.aurora-pbs-0001.hostmgmt.cm.aurora.alcf.anl.gov")
- Summary table has fixed-width columns that may truncate
- Time formats vary ("Today 10:00" vs "Wed Aug 06 10:00:00 2025")

**Implementation Strategy:**
```python
def _parse_summary_line(self, line: str) -> PBSReservation:
    """Parse reservation summary line with robust field extraction"""
    
    # Use regex for flexible parsing
    # Pattern: ResID | Queue | User | State | Start/Duration/End
    pattern = r'^(\S+)\s+(\S+)\s+(\S+)\s+(\S+)\s+(.+)$'
    match = re.match(pattern, line.strip())
    
    if not match:
        raise ValueError(f"Could not parse reservation line: {line}")
    
    resv_id, queue, user, state, timing = match.groups()
    
    # Parse timing with multiple format support
    start_time, duration, end_time = self._parse_timing_field(timing)
    
    return PBSReservation(
        reservation_id=resv_id,
        queue=queue,
        owner=user.split('@')[0],  # Remove hostname
        state=ReservationState(state) if state in ReservationState._value2member_map_ else ReservationState.UNKNOWN,
        start_time=start_time,
        end_time=end_time,
        duration_seconds=duration
    )
```

### Q4: Resource Allocation Analysis - Job Queue vs Node Overlap?

**Recommended Approach: Job Queue Analysis (Phase 1)**

**Primary Method: Job Queue Analysis**
- Find jobs submitted to reservation's queue during reservation period
- Calculate node-hours used vs. reserved
- More reliable and easier to implement

**Future Enhancement: Node Overlap Analysis**
- Compare reservation nodes with job execution nodes
- More accurate but significantly more complex
- Requires detailed node mapping

**Implementation Priority:**
1. **Phase 1**: Job queue analysis (immediate value)
2. **Phase 2**: Node overlap analysis (accuracy improvement)

### Q5: Sample Data Strategy

**Sample Data Files Needed:**
1. `pbs_monitor/sample_json/pbs_rstat_summary.txt` - Extract summary from existing pbs_rstat_f.txt
2. `pbs_monitor/sample_json/pbs_rstat_detailed_*.txt` - Use existing pbs_rstat_f.txt as template
3. Update existing sample job data to include reservation queue jobs

**Testing Strategy:**
```bash
pbs-monitor resv list --use-sample-data
pbs-monitor resv show R6710677.aurora --use-sample-data
pbs-monitor analyze reservation-utilization --use-sample-data
```

## Implementation Timeline & Phases

### Phase 1: Core Infrastructure ✅ COMPLETED
- [x] **Create reservation data models** - `pbs_monitor/models/reservation.py` with `PBSReservation` class and `ReservationState` enum
- [x] **Implement PBS command parsing** - `pbs_commands.py` has `pbs_rstat_summary()`, `pbs_rstat_detailed()`, and parsing methods
- [x] **Add database schema and migration** - `Reservation`, `ReservationHistory`, and `ReservationUtilization` tables in `models.py` with v1.1.0 migration
- [x] **Basic CLI commands** - `pbs-monitor resv list` and `pbs-monitor resv show` implemented in `commands.py`
- [x] **Sample data for testing** - `pbs_rstat.txt` (summary) and `pbs_rstat_f.txt` (detailed) sample files exist

### Phase 2: Integration & Testing ✅ COMPLETED  
- [x] **Integrate with data collector** - `DataCollector` class includes reservation collection in `collect_and_persist()` method
- [x] **Add reservation history tracking** - `ReservationHistory` model and state tracking implemented
- [x] **CLI command refinement and testing** - Full CLI integration with filtering, display options, and collection support
- [x] **Model converters** - `ReservationConverter` class implemented for database model conversion

### Phase 3: Analysis Features ✅ COMPLETED
- [x] **Reservation utilization analysis** - `ReservationUtilizationAnalyzer` and `ReservationTrendAnalyzer` classes in `analytics/reservation_analysis.py`
- [x] **Analysis CLI commands** - `pbs-monitor analyze reservation-utilization`, `reservation-trends`, and `reservation-owner-ranking` implemented
- [x] **Reporting and visualization** - Table and CSV output formats with rich console display
- [x] **Repository integration** - `ReservationRepository` class for database operations
- [x] **Data collection integration** - Reservations automatically collected with `--collect` flag and in daemon mode

## Phase 4: Advanced Features & Future Enhancements
*The following features could be added in future versions:*
- [ ] Recurring reservation pattern analysis
- [ ] Cross-reservation resource conflict detection  
- [ ] Integration with job performance metrics
- [ ] Advanced visualization dashboards
- [ ] Reservation efficiency alerts/notifications

## Risk Mitigation

### Risk 1: PBS Command Availability
**Mitigation**: Check for `/opt/pbs/bin/pbs_rstat` availability, graceful fallback, clear error messages

### Risk 2: Parsing Reliability  
**Mitigation**: Robust regex patterns, extensive sample data testing, fallback to partial data

### Risk 3: Database Migration Issues
**Mitigation**: Non-breaking migration, backup recommendations, rollback procedures

### Risk 4: Performance Impact
**Mitigation**: Optional detailed collection, configurable refresh intervals, efficient database queries

## Configuration Integration

### Reservation Collection Settings

**Add to configuration system:**
```yaml
pbs:
  reservation_refresh_interval: 7200  # 2 hours - reservations change less frequently than jobs
  enable_reservation_collection: true
  reservation_detailed_collection: true  # Whether to collect detailed info by default

display:
  reservation_columns: ["reservation_id", "name", "owner", "state", "start_time", "duration", "nodes"]
  max_reservation_name_width: 30
```

### Implementation Details

1. **Utilization Timeframe**: Analysis will support both entire reservation period and custom date ranges
2. **Node Details**: Store both node count and full node list (with configurable truncation for display)
3. **Historical Analysis**: Track reservation efficiency trends over time for pattern identification
4. **Integration**: Reservation analysis will integrate with existing job analysis through shared database queries

## Conclusion

✅ **IMPLEMENTATION COMPLETE**: All phases of the PBS reservation monitoring plan have been successfully implemented!

The PBS Monitor toolkit now provides comprehensive reservation monitoring capabilities through the `pbs-monitor resv` command family. The implementation includes:

### Successfully Implemented Features:
- **Primary command**: `pbs-monitor resv` for minimal typing ✅
- **Sample data**: Complete test data based on `pbs_rstat_f.txt` ✅ 
- **Database integration**: Full reservation persistence with history tracking ✅
- **Analysis features**: Utilization analysis, trends, and efficiency ranking ✅
- **Robust parsing**: Handles non-JSON PBS command output reliably ✅

### Technical Achievements:
- **Consistent design**: Maintains existing codebase patterns and architecture
- **Performance**: Efficient database queries with proper indexing
- **Reliability**: Robust error handling and graceful fallbacks
- **Usability**: Intuitive CLI with filtering, formatting, and collection options
- **Extensibility**: Well-structured foundation for future enhancements

The implementation significantly enhances the toolkit's visibility into resource allocation and usage patterns, providing valuable insights for system administrators and researchers using reserved resources. Users can now monitor reservation utilization efficiency, track trends over time, and identify optimization opportunities through comprehensive analytics.