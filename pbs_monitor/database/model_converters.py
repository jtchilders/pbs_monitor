"""
Model converters for PBS Monitor database integration

Converts between in-memory PBS models (PBSJob, PBSQueue, PBSNode) 
and database models (Job, Queue, Node) for seamless data flow.
"""

from typing import List, Optional, Dict, Any
from datetime import datetime

from ..models.job import PBSJob, JobState as PBSJobState
from ..models.queue import PBSQueue, QueueState as PBSQueueState
from ..models.node import PBSNode, NodeState as PBSNodeState
from ..models.reservation import PBSReservation, ReservationState as PBSReservationState
from .models import (
    Job, Queue, Node, Reservation, JobHistory, QueueSnapshot, NodeSnapshot, SystemSnapshot,
    JobState, QueueState, NodeState, ReservationState, ReservationHistory
)


class JobConverter:
    """Converter between PBSJob and database Job models"""
    
    @staticmethod
    def to_database(pbs_job: PBSJob) -> Job:
        """Convert PBSJob to database Job model"""
        return Job(
            job_id=pbs_job.job_id,
            job_name=pbs_job.job_name,
            owner=pbs_job.owner,
            state=JobState(pbs_job.state.value),
            queue=pbs_job.queue,
            
            # Resource requirements
            nodes=pbs_job.nodes,
            ppn=pbs_job.ppn,
            walltime=pbs_job.walltime,
            memory=pbs_job.memory,
            
            # Timing
            submit_time=pbs_job.submit_time,
            start_time=pbs_job.start_time,
            end_time=pbs_job.end_time,
            
            # Additional attributes
            priority=pbs_job.priority,
            execution_node=pbs_job.execution_node,
            exit_status=pbs_job.exit_status,
            
            # Project and allocation information
            project=pbs_job.project,
            allocation_type=pbs_job.allocation_type,
            
            # Calculated fields
            total_cores=pbs_job.total_cores,
            actual_runtime_seconds=pbs_job.actual_runtime_seconds,
            queue_time_seconds=pbs_job.queue_time_seconds,
            
            # Metadata
            last_updated=datetime.now(),
            raw_pbs_data=pbs_job.raw_attributes
        )
    
    @staticmethod
    def from_database(db_job: Job) -> PBSJob:
        """Convert database Job to PBSJob model"""
        return PBSJob(
            job_id=db_job.job_id,
            job_name=db_job.job_name,
            owner=db_job.owner,
            state=PBSJobState(db_job.state.value),
            queue=db_job.queue,
            
            # Resource requirements
            nodes=db_job.nodes,
            ppn=db_job.ppn,
            walltime=db_job.walltime,
            memory=db_job.memory,
            
            # Timing
            submit_time=db_job.submit_time,
            start_time=db_job.start_time,
            end_time=db_job.end_time,
            
            # Additional attributes
            priority=db_job.priority,
            execution_node=db_job.execution_node,
            exit_status=db_job.exit_status,
            
            # Project and allocation information
            project=db_job.project,
            allocation_type=db_job.allocation_type,
            
            # Calculated fields
            total_cores=db_job.total_cores,
            actual_runtime_seconds=db_job.actual_runtime_seconds,
            queue_time_seconds=db_job.queue_time_seconds,
            
            score=None,  # Score is only stored in job history
            
            # Raw attributes
            raw_attributes=db_job.raw_pbs_data or {}
        )
    
    @staticmethod
    def to_job_history(pbs_job: PBSJob, data_collection_id: Optional[int] = None) -> JobHistory:
        """Convert PBSJob to JobHistory entry"""
        return JobHistory(
            job_id=pbs_job.job_id,
            timestamp=datetime.now(),
            state=JobState(pbs_job.state.value),
            queue=pbs_job.queue,
            priority=pbs_job.priority,
            execution_node=pbs_job.execution_node,
            score=pbs_job.score,
            data_collection_id=data_collection_id
        )


class QueueConverter:
    """Converter between PBSQueue and database Queue models"""
    
    @staticmethod
    def to_database(pbs_queue: PBSQueue) -> Queue:
        """Convert PBSQueue to database Queue model"""
        return Queue(
            name=pbs_queue.name,
            queue_type=pbs_queue.queue_type,
            
            # Limits
            max_running=pbs_queue.max_running,
            max_queued=pbs_queue.max_queued,
            max_user_run=pbs_queue.max_user_run,
            max_user_queued=pbs_queue.max_user_queued,
            max_nodes=pbs_queue.max_nodes,
            max_ppn=pbs_queue.max_ppn,
            max_walltime=pbs_queue.max_walltime,
            
            # Priority and scheduling
            priority=pbs_queue.priority,
            
            # Metadata
            last_updated=datetime.now(),
            raw_pbs_data=pbs_queue.raw_attributes
        )
    
    @staticmethod
    def from_database(db_queue: Queue) -> PBSQueue:
        """Convert database Queue to PBSQueue model"""
        return PBSQueue(
            name=db_queue.name,
            state=PBSQueueState.ENABLED_STARTED,  # Default state
            queue_type=db_queue.queue_type,
            
            # Limits
            max_running=db_queue.max_running,
            max_queued=db_queue.max_queued,
            max_user_run=db_queue.max_user_run,
            max_user_queued=db_queue.max_user_queued,
            max_nodes=db_queue.max_nodes,
            max_ppn=db_queue.max_ppn,
            max_walltime=db_queue.max_walltime,
            
            # Current job statistics (from latest snapshot)
            total_jobs=0,  # Would need to get from latest snapshot
            transit_jobs=0,
            queued_jobs=0,
            held_jobs=0,
            waiting_jobs=0,
            running_jobs=0,
            exiting_jobs=0,
            begun_jobs=0,
            
            # Priority and scheduling
            priority=db_queue.priority,
            
            # Raw attributes
            raw_attributes=db_queue.raw_pbs_data or {}
        )
    
    @staticmethod
    def to_queue_snapshot(pbs_queue: PBSQueue, data_collection_id: Optional[int] = None) -> QueueSnapshot:
        """Convert PBSQueue to QueueSnapshot entry"""
        return QueueSnapshot(
            queue_name=pbs_queue.name,
            timestamp=datetime.now(),
            state=QueueState(pbs_queue.state.value),
            total_jobs=pbs_queue.total_jobs,
            running_jobs=pbs_queue.running_jobs,
            queued_jobs=pbs_queue.queued_jobs,
            held_jobs=pbs_queue.held_jobs,
            utilization_percent=pbs_queue.utilization_percentage(),
            data_collection_id=data_collection_id
        )


class NodeConverter:
    """Converter between PBSNode and database Node models"""
    
    @staticmethod
    def to_database(pbs_node: PBSNode) -> Node:
        """Convert PBSNode to database Node model"""
        return Node(
            name=pbs_node.name,
            
            # Hardware specifications
            ncpus=pbs_node.ncpus,
            memory_gb=pbs_node.memory_gb(),
            
            # Node properties
            properties=pbs_node.properties,
            
            # Metadata
            last_updated=datetime.now(),
            raw_pbs_data=pbs_node.raw_attributes
        )
    
    @staticmethod
    def from_database(db_node: Node) -> PBSNode:
        """Convert database Node to PBSNode model"""
        return PBSNode(
            name=db_node.name,
            state=PBSNodeState.UNKNOWN,  # Would need to get from latest snapshot
            
            # Hardware specifications
            ncpus=db_node.ncpus,
            memory=f"{db_node.memory_gb}gb" if db_node.memory_gb else None,
            
            # Current usage (from latest snapshot)
            jobs=[],  # Would need to get from latest snapshot
            
            # Node properties
            properties=db_node.properties or [],
            
            # Load and performance (from latest snapshot)
            loadavg=None,  # Would need to get from latest snapshot
            
            # Raw attributes
            raw_attributes=db_node.raw_pbs_data or {}
        )
    
    @staticmethod
    def to_node_snapshot(pbs_node: PBSNode, data_collection_id: Optional[int] = None) -> NodeSnapshot:
        """Convert PBSNode to NodeSnapshot entry"""
        return NodeSnapshot(
            node_name=pbs_node.name,
            timestamp=datetime.now(),
            state=NodeState(pbs_node.state.value),
            jobs_running=len(pbs_node.jobs),
            jobs_list=pbs_node.jobs,
            load_average=pbs_node.loadavg,
            cpu_utilization_percent=pbs_node.cpu_utilization(),
            memory_used_gb=pbs_node.memory_gb() if pbs_node.memory_gb() else None,
            data_collection_id=data_collection_id
        )


class SystemConverter:
    """Converter for system-level statistics"""
    
    @staticmethod
    def to_system_snapshot(jobs: List[PBSJob], queues: List[PBSQueue], nodes: List[PBSNode],
                          data_collection_id: Optional[int] = None) -> SystemSnapshot:
        """Convert system state to SystemSnapshot"""
        # Job statistics
        total_jobs = len(jobs)
        running_jobs = len([j for j in jobs if j.state == PBSJobState.RUNNING])
        queued_jobs = len([j for j in jobs if j.state == PBSJobState.QUEUED])
        held_jobs = len([j for j in jobs if j.state == PBSJobState.HELD])
        
        # Resource statistics
        total_nodes = len(nodes)
        available_nodes = len([n for n in nodes if n.is_available()])
        total_cores = sum(node.ncpus for node in nodes)
        used_cores = sum(len(node.jobs) for node in nodes)
        
        # Queue statistics
        active_queues = len([q for q in queues if q.is_enabled()])
        
        # Performance metrics
        running_job_times = [
            (datetime.now() - job.start_time).total_seconds() / 60
            for job in jobs
            if job.state == PBSJobState.RUNNING and job.start_time
        ]
        avg_runtime_minutes = sum(running_job_times) / len(running_job_times) if running_job_times else None
        
        queued_job_times = [
            (datetime.now() - job.submit_time).total_seconds() / 60
            for job in jobs
            if job.state == PBSJobState.QUEUED and job.submit_time
        ]
        avg_queue_time_minutes = sum(queued_job_times) / len(queued_job_times) if queued_job_times else None
        
        system_utilization_percent = (used_cores / total_cores * 100) if total_cores > 0 else 0
        
        return SystemSnapshot(
            timestamp=datetime.now(),
            total_jobs=total_jobs,
            running_jobs=running_jobs,
            queued_jobs=queued_jobs,
            held_jobs=held_jobs,
            total_nodes=total_nodes,
            available_nodes=available_nodes,
            total_cores=total_cores,
            used_cores=used_cores,
            active_queues=active_queues,
            avg_queue_time_minutes=avg_queue_time_minutes,
            avg_runtime_minutes=avg_runtime_minutes,
            system_utilization_percent=system_utilization_percent,
            data_collection_id=data_collection_id
        )


class ReservationConverter:
    """Converter between PBSReservation and database Reservation models"""
    
    @staticmethod
    def to_database(pbs_reservation: PBSReservation) -> Reservation:
        """Convert PBSReservation to database Reservation model"""
        return Reservation(
            reservation_id=pbs_reservation.reservation_id,
            reservation_name=pbs_reservation.reservation_name,
            owner=pbs_reservation.owner,
            state=ReservationState.from_pbs_state(pbs_reservation.state),
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
    
    @staticmethod
    def from_database(db_reservation: Reservation) -> PBSReservation:
        """Convert database Reservation to PBSReservation model"""
        # Convert state back to PBS state
        pbs_state = PBSReservationState(db_reservation.state.value)
        
        return PBSReservation(
            reservation_id=db_reservation.reservation_id,
            reservation_name=db_reservation.reservation_name,
            owner=db_reservation.owner,
            state=pbs_state,
            queue=db_reservation.queue,
            
            # Resources
            nodes=db_reservation.nodes,
            ncpus=db_reservation.ncpus,
            ngpus=db_reservation.ngpus,
            walltime=db_reservation.walltime,
            
            # Timing
            start_time=db_reservation.start_time,
            end_time=db_reservation.end_time,
            duration_seconds=db_reservation.duration_seconds,
            creation_time=db_reservation.creation_time,
            modification_time=db_reservation.modification_time,
            
            # Access control
            authorized_users=db_reservation.authorized_users or [],
            authorized_groups=db_reservation.authorized_groups or [],
            
            # Metadata
            server=db_reservation.server,
            partition=db_reservation.partition,
            reserved_nodes=db_reservation.reserved_nodes,
            
            # Raw data
            raw_attributes=db_reservation.raw_pbs_data or {}
        )
    
    @staticmethod
    def to_reservation_history(pbs_reservation: PBSReservation, data_collection_id: Optional[int] = None) -> ReservationHistory:
        """Convert PBSReservation to ReservationHistory for tracking state changes"""
        return ReservationHistory(
            reservation_id=pbs_reservation.reservation_id,
            state=ReservationState.from_pbs_state(pbs_reservation.state),
            data_collection_id=data_collection_id,
            timestamp=datetime.now()
        )


class ModelConverters:
    """Main converter class with all converters"""
    
    def __init__(self):
        self.job = JobConverter()
        self.queue = QueueConverter()
        self.node = NodeConverter()
        self.reservation = ReservationConverter()
        self.system = SystemConverter()
    
    def convert_pbs_data_to_database(self, jobs: List[PBSJob], queues: List[PBSQueue], 
                                   nodes: List[PBSNode]) -> Dict[str, Any]:
        """Convert all PBS data to database models"""
        return {
            'jobs': [self.job.to_database(job) for job in jobs],
            'queues': [self.queue.to_database(queue) for queue in queues],
            'nodes': [self.node.to_database(node) for node in nodes],
            'job_history': [self.job.to_job_history(job) for job in jobs],
            'queue_snapshots': [self.queue.to_queue_snapshot(queue) for queue in queues],
            'node_snapshots': [self.node.to_node_snapshot(node) for node in nodes],
            'system_snapshot': self.system.to_system_snapshot(jobs, queues, nodes)
        }
    
    def convert_database_to_pbs_data(self, db_jobs: List[Job], db_queues: List[Queue], 
                                   db_nodes: List[Node]) -> Dict[str, Any]:
        """Convert database models to PBS data"""
        return {
            'jobs': [self.job.from_database(job) for job in db_jobs],
            'queues': [self.queue.from_database(queue) for queue in db_queues],
            'nodes': [self.node.from_database(node) for node in db_nodes]
        } 