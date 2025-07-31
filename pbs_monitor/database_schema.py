"""
Database Schema Design for PBS Monitor Phase 2
==============================================

This module defines the database schema for persistent storage of PBS system data.
The schema is designed to:
1. Track job lifecycles beyond PBS's 1-week history limit
2. Store historical queue and node information for trend analysis
3. Support machine learning predictions with comprehensive historical data
4. Handle concurrent access from CLI and daemon processes

Schema Overview:
- jobs: Core job tracking with state transitions
- job_history: Historical snapshots of job state changes
- queues: Queue configuration and limits
- queue_snapshots: Historical queue utilization data
- nodes: Node configuration and properties
- node_snapshots: Historical node utilization data
- system_snapshots: Overall system state for trend analysis
- data_collection_log: Track data collection events and errors

Design Principles:
- Normalize core entities (jobs, queues, nodes) but denormalize snapshots for analysis
- Use job_history for tracking state transitions and final outcomes
- Snapshot tables capture point-in-time states for historical analysis
- All timestamps in UTC for consistency
- Use indexes for common query patterns (job_id, timestamp ranges, state filtering)
"""

from datetime import datetime, timezone
from typing import Dict, List, Optional, Any
from sqlalchemy import (
    create_engine, Column, Integer, String, DateTime, Float, Boolean, 
    Text, ForeignKey, Index, UniqueConstraint, JSON, Enum as SQLEnum
)
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship, sessionmaker
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.sql import func
import enum

Base = declarative_base()

# Enums for job and system states
class JobState(enum.Enum):
    QUEUED = "Q"
    RUNNING = "R"
    HELD = "H"
    WAITING = "W"
    TRANSITIONING = "T"
    EXITING = "E"
    SUSPENDED = "S"
    COMPLETED = "C"
    FINISHED = "F"

class QueueState(enum.Enum):
    ENABLED_STARTED = "enabled_started"
    ENABLED_STOPPED = "enabled_stopped"
    DISABLED = "disabled"

class NodeState(enum.Enum):
    FREE = "free"
    OFFLINE = "offline"
    DOWN = "down"
    BUSY = "busy"
    JOB_EXCLUSIVE = "job-exclusive"
    JOB_SHARING = "job-sharing"
    RESERVE = "reserve"
    UNKNOWN = "unknown"

class DataCollectionStatus(enum.Enum):
    SUCCESS = "success"
    PARTIAL_SUCCESS = "partial_success"
    FAILED = "failed"

# Core Tables

class Job(Base):
    """
    Core job tracking table - represents the current/final state of jobs
    
    This table maintains one record per job, updated as job progresses.
    Historical state changes are tracked in job_history table.
    """
    __tablename__ = 'jobs'
    
    # Primary identifiers
    job_id = Column(String(100), primary_key=True)  # e.g., "12345.pbs01"
    job_name = Column(String(200))
    owner = Column(String(50), index=True)
    project = Column(String(100), index=True, nullable=True)  # From Account_Name
    allocation_type = Column(String(100), index=True, nullable=True)  # From Resource_List.award_category
    
    # Current state
    state = Column(SQLEnum(JobState), index=True)
    queue = Column(String(50), index=True)
    
    # Resource requirements
    nodes = Column(Integer, default=1)
    ppn = Column(Integer, default=1)
    walltime = Column(String(20))  # HH:MM:SS format
    memory = Column(String(20))    # e.g., "8gb"
    
    # Timing information
    submit_time = Column(DateTime(timezone=True))
    start_time = Column(DateTime(timezone=True))
    end_time = Column(DateTime(timezone=True))
    
    # Job outcomes
    priority = Column(Integer, default=0)
    exit_status = Column(Integer)
    execution_nodes = Column(String(500))  # Comma-separated list
    
    # Calculated fields
    total_cores = Column(Integer)  # nodes * ppn
    actual_runtime_seconds = Column(Integer)  # For completed jobs
    queue_time_seconds = Column(Integer)      # start_time - submit_time
    
    # System tracking
    first_seen = Column(DateTime(timezone=True), default=func.now())
    last_updated = Column(DateTime(timezone=True), default=func.now(), onupdate=func.now())
    final_state_recorded = Column(Boolean, default=False)  # True when job reached final state
    
    # Raw data
    raw_pbs_data = Column(JSON)  # Store original PBS JSON for debugging
    
    # Relationships
    history = relationship("JobHistory", back_populates="job", order_by="JobHistory.timestamp")
    
    # Indexes
    __table_args__ = (
        Index('ix_jobs_owner_state', 'owner', 'state'),
        Index('ix_jobs_submit_time', 'submit_time'),
        Index('ix_jobs_queue_state', 'queue', 'state'),
        Index('ix_jobs_final_state', 'final_state_recorded'),
        Index('ix_jobs_project_state', 'project', 'state'),
        Index('ix_jobs_allocation_type_state', 'allocation_type', 'state'),
    )

class JobHistory(Base):
    """
    Historical job state changes - tracks job lifecycle
    
    Every time we see a job in PBS, we record its state here.
    This allows us to track state transitions and calculate metrics.
    """
    __tablename__ = 'job_history'
    
    id = Column(Integer, primary_key=True)
    job_id = Column(String(100), ForeignKey('jobs.job_id'), index=True)
    timestamp = Column(DateTime(timezone=True), default=func.now())
    
    # State at this point in time
    state = Column(SQLEnum(JobState))
    queue = Column(String(50))
    priority = Column(Integer)
    execution_nodes = Column(String(500))
    comment = Column(Text, nullable=True) # Added comment field
    
    # PBS score (if available)
    score = Column(Float)
    
    # System info
    data_collection_id = Column(Integer, ForeignKey('data_collection_log.id'))
    
    # Relationships
    job = relationship("Job", back_populates="history")
    collection_event = relationship("DataCollectionLog")
    
    # Indexes
    __table_args__ = (
        Index('ix_job_history_job_timestamp', 'job_id', 'timestamp'),
        Index('ix_job_history_state_timestamp', 'state', 'timestamp'),
    )

class Queue(Base):
    """
    Queue configuration and limits
    
    Stores queue properties that change infrequently.
    Current utilization is tracked in queue_snapshots.
    """
    __tablename__ = 'queues'
    
    name = Column(String(100), primary_key=True)
    queue_type = Column(String(50), default="execution")
    
    # Limits (null means unlimited)
    max_running = Column(Integer)
    max_queued = Column(Integer)
    max_user_run = Column(Integer)
    max_user_queued = Column(Integer)
    max_nodes = Column(Integer)
    max_ppn = Column(Integer)
    max_walltime = Column(String(20))
    
    # Configuration
    priority = Column(Integer, default=0)
    
    # Tracking
    first_seen = Column(DateTime(timezone=True), default=func.now())
    last_updated = Column(DateTime(timezone=True), default=func.now(), onupdate=func.now())
    is_active = Column(Boolean, default=True)
    
    # Raw data
    raw_pbs_data = Column(JSON)
    
    # Relationships
    snapshots = relationship("QueueSnapshot", back_populates="queue")

class QueueSnapshot(Base):
    """
    Point-in-time queue utilization snapshots
    
    Captures queue state and job counts at regular intervals.
    Used for historical trend analysis and capacity planning.
    """
    __tablename__ = 'queue_snapshots'
    
    id = Column(Integer, primary_key=True)
    queue_name = Column(String(100), ForeignKey('queues.name'), index=True)
    timestamp = Column(DateTime(timezone=True), default=func.now())
    
    # State
    state = Column(SQLEnum(QueueState))
    
    # Job counts
    total_jobs = Column(Integer, default=0)
    running_jobs = Column(Integer, default=0)
    queued_jobs = Column(Integer, default=0)
    held_jobs = Column(Integer, default=0)
    
    # Calculated metrics
    utilization_percent = Column(Float)  # running/max_running * 100
    queue_depth = Column(Integer)        # queued + held jobs
    
    # System info
    data_collection_id = Column(Integer, ForeignKey('data_collection_log.id'))
    
    # Relationships
    queue = relationship("Queue", back_populates="snapshots")
    collection_event = relationship("DataCollectionLog")
    
    # Indexes
    __table_args__ = (
        Index('ix_queue_snapshots_name_timestamp', 'queue_name', 'timestamp'),
    )

class Node(Base):
    """
    Compute node configuration and properties
    
    Stores node hardware specs and properties that change infrequently.
    Current utilization is tracked in node_snapshots.
    """
    __tablename__ = 'nodes'
    
    name = Column(String(100), primary_key=True)
    
    # Hardware specs
    ncpus = Column(Integer)
    memory_gb = Column(Float)
    
    # Properties and features
    properties = Column(JSON)  # List of node properties
    
    # Tracking
    first_seen = Column(DateTime(timezone=True), default=func.now())
    last_updated = Column(DateTime(timezone=True), default=func.now(), onupdate=func.now())
    is_active = Column(Boolean, default=True)
    
    # Raw data
    raw_pbs_data = Column(JSON)
    
    # Relationships
    snapshots = relationship("NodeSnapshot", back_populates="node")

class NodeSnapshot(Base):
    """
    Point-in-time node utilization snapshots
    
    Captures node state and job assignments at regular intervals.
    Used for resource utilization analysis and capacity planning.
    """
    __tablename__ = 'node_snapshots'
    
    id = Column(Integer, primary_key=True)
    node_name = Column(String(100), ForeignKey('nodes.name'), index=True)
    timestamp = Column(DateTime(timezone=True), default=func.now())
    
    # State
    state = Column(SQLEnum(NodeState))
    
    # Resource usage
    jobs_running = Column(Integer, default=0)
    jobs_list = Column(JSON)  # List of job IDs running on this node
    
    # Performance metrics
    load_average = Column(Float)
    cpu_utilization_percent = Column(Float)
    memory_used_gb = Column(Float)
    
    # System info
    data_collection_id = Column(Integer, ForeignKey('data_collection_log.id'))
    
    # Relationships
    node = relationship("Node", back_populates="snapshots")
    collection_event = relationship("DataCollectionLog")
    
    # Indexes
    __table_args__ = (
        Index('ix_node_snapshots_name_timestamp', 'node_name', 'timestamp'),
    )

class SystemSnapshot(Base):
    """
    Overall system state snapshots
    
    Captures high-level system metrics for trend analysis.
    Pre-computed aggregations for dashboard and ML features.
    """
    __tablename__ = 'system_snapshots'
    
    id = Column(Integer, primary_key=True)
    timestamp = Column(DateTime(timezone=True), default=func.now())
    
    # Job statistics
    total_jobs = Column(Integer, default=0)
    running_jobs = Column(Integer, default=0)
    queued_jobs = Column(Integer, default=0)
    held_jobs = Column(Integer, default=0)
    
    # Resource statistics
    total_nodes = Column(Integer, default=0)
    available_nodes = Column(Integer, default=0)
    total_cores = Column(Integer, default=0)
    used_cores = Column(Integer, default=0)
    
    # Queue statistics
    active_queues = Column(Integer, default=0)
    
    # Performance metrics
    avg_queue_time_minutes = Column(Float)
    avg_runtime_minutes = Column(Float)
    system_utilization_percent = Column(Float)
    
    # System info
    data_collection_id = Column(Integer, ForeignKey('data_collection_log.id'))
    
    # Relationships
    collection_event = relationship("DataCollectionLog")
    
    # Indexes
    __table_args__ = (
        Index('ix_system_snapshots_timestamp', 'timestamp'),
    )

class DataCollectionLog(Base):
    """
    Log of data collection events
    
    Tracks when data was collected, what was collected, and any errors.
    Used for debugging and ensuring data quality.
    """
    __tablename__ = 'data_collection_log'
    
    id = Column(Integer, primary_key=True)
    timestamp = Column(DateTime(timezone=True), default=func.now())
    
    # Collection details
    collection_type = Column(String(50))  # "manual", "daemon", "cli"
    status = Column(SQLEnum(DataCollectionStatus))
    
    # What was collected
    jobs_collected = Column(Integer, default=0)
    queues_collected = Column(Integer, default=0)
    nodes_collected = Column(Integer, default=0)
    
    # Timing
    duration_seconds = Column(Float)
    
    # Error tracking
    error_message = Column(Text)
    error_details = Column(JSON)
    
    # Indexes
    __table_args__ = (
        Index('ix_data_collection_timestamp', 'timestamp'),
        Index('ix_data_collection_status', 'status'),
    )

# Database Configuration and Utilities

class DatabaseConfig:
    """Database configuration for different environments"""
    
    # SQLite for development/testing
    SQLITE_URL = "sqlite:///pbs_monitor.db"
    
    # PostgreSQL for production
    @staticmethod
    def postgresql_url(host: str, port: int, database: str, username: str, password: str) -> str:
        return f"postgresql://{username}:{password}@{host}:{port}/{database}"
    
    # Connection pool settings
    POOL_SIZE = 5
    MAX_OVERFLOW = 10
    
    # SQLAlchemy engine options
    ENGINE_OPTIONS = {
        'echo': False,  # Set to True for SQL debugging
        'pool_pre_ping': True,
        'pool_recycle': 3600,
    }

# Key Design Decisions and Trade-offs:

"""
1. Job State Tracking:
   - Main 'jobs' table has current state
   - 'job_history' tracks all state changes
   - This allows both current status queries and historical analysis

2. Snapshot Tables:
   - Separate snapshot tables for queues, nodes, and system
   - Denormalized for efficient time-series analysis
   - Linked to data collection events for audit trail

3. Raw Data Preservation:
   - Store original PBS JSON in all main tables
   - Enables debugging and future schema changes
   - Uses JSON/JSONB for flexibility

4. Indexing Strategy:
   - Compound indexes for common query patterns
   - Timestamp indexes for time-range queries
   - State indexes for filtering

5. Concurrency Support:
   - Foreign key constraints maintain referential integrity
   - Timestamps track when data was collected
   - Collection log prevents duplicate processing

6. Data Retention:
   - No automatic cleanup - admin controlled
   - Snapshot tables will grow over time
   - Consider partitioning for large deployments

7. Future ML Features:
   - Job history provides training data
   - Snapshots provide system state features
   - Easy to add prediction result tables later
""" 