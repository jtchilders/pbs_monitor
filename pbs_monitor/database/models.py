"""
SQLAlchemy Models for PBS Monitor Database

This module contains the SQLAlchemy model definitions for persistent storage
of PBS system data. The models correspond to the database schema designed
for Phase 2 of the PBS Monitor project.
"""

from datetime import datetime, timezone
from typing import Dict, List, Optional, Any, Union
from sqlalchemy import (
    create_engine, Column, Integer, String, DateTime, Float, Boolean, 
    Text, ForeignKey, Index, UniqueConstraint, JSON, Enum as SQLEnum
)
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship, sessionmaker, Session
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.sql import func
from sqlalchemy.engine import Engine
import enum

# Import existing model enums to maintain compatibility
from ..models.job import JobState as PBSJobState
from ..models.queue import QueueState as PBSQueueState  
from ..models.node import NodeState as PBSNodeState
from ..models.reservation import ReservationState as PBSReservationState

Base = declarative_base()

# Database enums (converted from existing enums)
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
    
    @classmethod
    def from_pbs_state(cls, pbs_state: PBSJobState) -> 'JobState':
        """Convert from existing PBSJobState enum"""
        return cls(pbs_state.value)

class QueueState(enum.Enum):
    ENABLED_STARTED = "enabled_started"
    ENABLED_STOPPED = "enabled_stopped"
    DISABLED = "disabled"
    
    @classmethod
    def from_pbs_state(cls, pbs_state: PBSQueueState) -> 'QueueState':
        """Convert from existing PBSQueueState enum"""
        return cls(pbs_state.value)

class NodeState(enum.Enum):
    FREE = "free"
    OFFLINE = "offline"
    DOWN = "down"
    BUSY = "busy"
    JOB_EXCLUSIVE = "job-exclusive"
    JOB_SHARING = "job-sharing"
    RESERVE = "reserve"
    UNKNOWN = "unknown"
    
    @classmethod
    def from_pbs_state(cls, pbs_state: PBSNodeState) -> 'NodeState':
        """Convert from existing PBSNodeState enum"""
        return cls(pbs_state.value)

class ReservationState(enum.Enum):
    CONFIRMED = "RESV_CONFIRMED"
    RUNNING = "RESV_RUNNING"
    FINISHED = "RESV_FINISHED"
    DELETED = "RESV_DELETED"
    DEGRADED = "RESV_DEGRADED"
    CONFIRMED_SHORT = "CO"
    RUNNING_SHORT = "RN"
    UNKNOWN = "unknown"
    
    @classmethod
    def from_pbs_state(cls, pbs_state: PBSReservationState) -> 'ReservationState':
        """Convert from existing PBSReservationState enum"""
        return cls(pbs_state.value)

class DataCollectionStatus(enum.Enum):
    SUCCESS = "success"
    PARTIAL_SUCCESS = "partial_success"
    FAILED = "failed"

# Database Models

class Job(Base):
    """
    Core job tracking table - represents the current/final state of jobs
    
    This table maintains one record per job, updated as job progresses.
    Historical state changes are tracked in job_history table.
    """
    __tablename__ = 'jobs'
    
    # Primary identifiers
    job_id = Column(String(100), primary_key=True)
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
    walltime = Column(String(20))
    memory = Column(String(20))
    
    # Timing information
    submit_time = Column(DateTime(timezone=True))
    start_time = Column(DateTime(timezone=True))
    end_time = Column(DateTime(timezone=True))
    
    # Job outcomes
    priority = Column(Integer, default=0)
    exit_status = Column(Integer)
    execution_node = Column(String(500))
    
    # Calculated fields
    total_cores = Column(Integer)
    actual_runtime_seconds = Column(Integer)
    queue_time_seconds = Column(Integer)
    
    # System tracking
    first_seen = Column(DateTime(timezone=True), default=func.now())
    last_updated = Column(DateTime(timezone=True), default=func.now(), onupdate=func.now())
    final_state_recorded = Column(Boolean, default=False)
    
    # Raw data
    raw_pbs_data = Column(JSON)
    
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
    
    def is_active(self) -> bool:
        """Check if job is currently active"""
        return self.state in [JobState.QUEUED, JobState.RUNNING, JobState.HELD]
    
    def is_completed(self) -> bool:
        """Check if job has completed"""
        return self.state in [JobState.COMPLETED, JobState.FINISHED]
    
    def estimated_total_cores(self) -> int:
        """Calculate total cores requested"""
        nodes = self.nodes or 1
        ppn = self.ppn or 1
        return nodes * ppn
    
    def calculate_derived_fields(self) -> None:
        """Calculate derived fields from timing information"""
        self.total_cores = self.estimated_total_cores()
        
        if self.start_time and self.submit_time:
            self.queue_time_seconds = int((self.start_time - self.submit_time).total_seconds())
        
        if self.end_time and self.start_time:
            self.actual_runtime_seconds = int((self.end_time - self.start_time).total_seconds())

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
    execution_node = Column(String(500))
    
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
    
    def is_enabled(self) -> bool:
        """Check if queue is enabled (from latest snapshot)"""
        if not self.snapshots:
            return True
        latest = max(self.snapshots, key=lambda s: s.timestamp)
        return latest.state in [QueueState.ENABLED_STARTED, QueueState.ENABLED_STOPPED]
    
    def is_started(self) -> bool:
        """Check if queue is started (from latest snapshot)"""
        if not self.snapshots:
            return True
        latest = max(self.snapshots, key=lambda s: s.timestamp)
        return latest.state == QueueState.ENABLED_STARTED

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
    utilization_percent = Column(Float)
    queue_depth = Column(Integer)
    
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
    properties = Column(JSON)
    
    # Tracking
    first_seen = Column(DateTime(timezone=True), default=func.now())
    last_updated = Column(DateTime(timezone=True), default=func.now(), onupdate=func.now())
    is_active = Column(Boolean, default=True)
    
    # Raw data
    raw_pbs_data = Column(JSON)
    
    # Relationships
    snapshots = relationship("NodeSnapshot", back_populates="node")
    
    def is_available(self) -> bool:
        """Check if node is available (from latest snapshot)"""
        if not self.snapshots:
            return False
        latest = max(self.snapshots, key=lambda s: s.timestamp)
        return latest.state in [NodeState.FREE, NodeState.JOB_SHARING]
    
    def is_occupied(self) -> bool:
        """Check if node is occupied (from latest snapshot)"""
        if not self.snapshots:
            return False
        latest = max(self.snapshots, key=lambda s: s.timestamp)
        return latest.jobs_running > 0

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
    jobs_list = Column(JSON)
    
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
    collection_type = Column(String(50))
    status = Column(SQLEnum(DataCollectionStatus))
    
    # What was collected
    jobs_collected = Column(Integer, default=0)
    queues_collected = Column(Integer, default=0)
    nodes_collected = Column(Integer, default=0)
    reservations_collected = Column(Integer, default=0)
    
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
    
    def is_successful(self) -> bool:
        """Check if collection was successful"""
        return self.status == DataCollectionStatus.SUCCESS
    
    def total_entities_collected(self) -> int:
        """Get total number of entities collected"""
        return (self.jobs_collected or 0) + (self.queues_collected or 0) + (self.nodes_collected or 0) + (self.reservations_collected or 0)

# Export all models for easy import
__all__ = [
    'Base',
    'Job',
    'JobHistory', 
    'Queue',
    'QueueSnapshot',
    'Node',
    'NodeSnapshot',
    'SystemSnapshot',
    'Reservation',
    'ReservationHistory',
    'ReservationUtilization',
    'DataCollectionLog',
    'JobState',
    'QueueState',
    'NodeState',
    'ReservationState',
    'DataCollectionStatus'
] 