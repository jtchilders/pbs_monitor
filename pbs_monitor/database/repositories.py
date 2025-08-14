"""
Database repositories for PBS Monitor

Provides data access layer for database operations.
"""

from typing import List, Optional, Dict, Any
from datetime import datetime, timedelta
from sqlalchemy import desc, func, and_, or_
from sqlalchemy.orm import Session

from .connection import DatabaseManager
from .models import (
    Job, Queue, Node, JobHistory, QueueSnapshot, NodeSnapshot, 
    SystemSnapshot, DataCollectionLog, JobState, QueueState, 
    NodeState, DataCollectionStatus, Reservation, ReservationHistory, 
    ReservationUtilization, ReservationState
)
from ..config import Config
from ..models.job import PBSJob


class BaseRepository:
    """Base repository class with common functionality"""
    
    def __init__(self, config: Optional[Config] = None):
        self.config = config or Config()
        self._db_manager = DatabaseManager(self.config)
    
    def get_session(self) -> Session:
        """Get database session"""
        return self._db_manager.get_session()


class JobRepository(BaseRepository):
    """Repository for job-related database operations"""
    
    def create_or_update_job(self, job_data: Dict[str, Any]) -> Job:
        """Create or update a Job from a dict and return the Job instance."""
        with self.get_session() as session:
            job = session.query(Job).filter(Job.job_id == job_data.get('job_id')).first()
            if not job:
                job = Job(job_id=job_data.get('job_id'))
                session.add(job)
            for key, value in job_data.items():
                if hasattr(job, key):
                    setattr(job, key, value)
            # Calculate derived fields where possible
            try:
                job.calculate_derived_fields()
            except Exception:
                pass
            session.commit()
            session.refresh(job)
            # Detach to avoid DetachedInstanceError on access
            session.expunge(job)
            return job
    
    def get_job_by_id(self, job_id: str) -> Optional[Job]:
        """Get job by ID"""
        with self.get_session() as session:
            job = session.query(Job).filter(Job.job_id == job_id).first()
            if job:
                # Force loading of all attributes to avoid detached instance issues
                session.expunge(job)
            return job
    
    def get_active_jobs(self) -> List[Job]:
        """Get all active jobs (running or queued)"""
        with self.get_session() as session:
            jobs = session.query(Job).filter(
                Job.state.in_([JobState.RUNNING, JobState.QUEUED, JobState.HELD])
            ).all()
            # Force loading of all attributes to avoid detached instance issues
            session.expunge_all()
            return jobs
    
    def get_jobs_by_user(self, user: str) -> List[Job]:
        """Get jobs for specific user"""
        with self.get_session() as session:
            jobs = session.query(Job).filter(Job.owner == user).all()
            # Force loading of all attributes to avoid detached instance issues
            session.expunge_all()
            return jobs
    
    def get_jobs_by_queue(self, queue: str) -> List[Job]:
        """Get jobs in specific queue"""
        with self.get_session() as session:
            jobs = session.query(Job).filter(Job.queue == queue).all()
            # Force loading of all attributes to avoid detached instance issues
            session.expunge_all()
            return jobs
    
    def get_jobs_by_state(self, state: JobState) -> List[Job]:
        """Get jobs in specific state"""
        with self.get_session() as session:
            jobs = session.query(Job).filter(Job.state == state).all()
            # Force loading of all attributes to avoid detached instance issues
            session.expunge_all()
            return jobs
    
    def get_historical_jobs(self, user: Optional[str] = None, days: int = 30) -> List[Job]:
        """Get historical jobs from database"""
        cutoff_date = datetime.now() - timedelta(days=days)
        with self.get_session() as session:
            query = session.query(Job).filter(Job.last_updated >= cutoff_date)
            if user:
                query = query.filter(Job.owner == user)
            jobs = query.all()
            # Force loading of all attributes to avoid detached instance issues
            session.expunge_all()
            return jobs
    
    def add_job(self, job: Job) -> Job:
        """Add new job to database"""
        with self.get_session() as session:
            session.add(job)
            session.commit()
            return job
    
    def upsert_jobs(self, jobs: List[Job]) -> None:
        """Insert or update jobs in database"""
        with self.get_session() as session:
            for job in jobs:
                existing = session.query(Job).filter(Job.job_id == job.job_id).first()
                if existing:
                    # Update existing job
                    for attr, value in job.__dict__.items():
                        if not attr.startswith('_'):
                            setattr(existing, attr, value)
                else:
                    # Add new job
                    session.add(job)
            session.commit()
    
    def update_job(self, job: Job) -> Job:
        """Update existing job"""
        with self.get_session() as session:
            session.merge(job)
            session.commit()
            return job
    
    def delete_job(self, job_id: str) -> bool:
        """Delete job by ID"""
        with self.get_session() as session:
            job = session.query(Job).filter(Job.job_id == job_id).first()
            if job:
                session.delete(job)
                session.commit()
                return True
            return False
    
    def get_job_history(self, job_id: str) -> List[JobHistory]:
        """Get history entries for a job"""
        with self.get_session() as session:
            records = session.query(JobHistory).filter(
                JobHistory.job_id == job_id
            ).order_by(JobHistory.timestamp).all()
            # Detach records so attributes are accessible after session closes
            for rec in records:
                session.expunge(rec)
            return records
    
    def add_job_history(self, job_history: JobHistory | str, state: Optional[JobState] = None) -> JobHistory:
        """Add job history entry. Accepts either a JobHistory or (job_id, state)."""
        with self.get_session() as session:
            if isinstance(job_history, JobHistory):
                hist = job_history
            else:
                hist = JobHistory(job_id=job_history, state=state)
            session.add(hist)
            session.commit()
            session.refresh(hist)
            session.expunge(hist)
            return hist

    def get_job_statistics(self) -> Dict[str, Any]:
        """Return simple aggregate statistics across all jobs."""
        with self.get_session() as session:
            total_jobs = session.query(func.count(Job.job_id)).scalar() or 0
            counts = {
                f"{st.value}_count": session.query(func.count(Job.job_id)).filter(Job.state == st).scalar() or 0
                for st in JobState
            }
            active_jobs = session.query(func.count(Job.job_id)).filter(Job.state.in_([JobState.RUNNING, JobState.QUEUED, JobState.HELD])).scalar() or 0
            return {
                'total_jobs': total_jobs,
                'active_jobs': active_jobs,
                **counts,
            }
    
    def add_job_history_batch(self, job_histories: List[JobHistory]) -> None:
        """Add multiple job history entries"""
        with self.get_session() as session:
            session.add_all(job_histories)
            session.commit()
    
    def get_latest_job_states(self) -> Dict[str, 'JobStateInfo']:
        """Get the latest state information for all jobs from job_history"""
        from ..data_collector import JobStateInfo  # Import here to avoid circular import
        
        with self.get_session() as session:
            # Get the latest job_history entry for each job_id
            # Use a window function to get the most recent entry per job
            subquery = session.query(
                JobHistory.job_id,
                JobHistory.state,
                JobHistory.priority,
                JobHistory.execution_node,
                JobHistory.queue,
                JobHistory.timestamp,
                func.row_number().over(
                    partition_by=JobHistory.job_id,
                    order_by=JobHistory.timestamp.desc()
                ).label('rn')
            ).subquery()
            
            # Get only the most recent entries (rn = 1)
            latest_entries = session.query(subquery).filter(subquery.c.rn == 1).all()
            
            # Convert to JobStateInfo objects
            result = {}
            for entry in latest_entries:
                # Convert database JobState enum to PBSJob JobState enum
                from ..models.job import JobState as PBSJobState
                pbs_state = PBSJobState(entry.state.value)
                
                result[entry.job_id] = JobStateInfo(
                    state=pbs_state,
                    priority=entry.priority or 0,
                    execution_node=entry.execution_node,
                    queue=entry.queue or ''
                )
            
            return result
    
    def get_user_job_statistics(self, user: str, days: int = 30) -> Dict[str, Any]:
        """Get job statistics for a user"""
        cutoff_date = datetime.now() - timedelta(days=days)
        with self.get_session() as session:
            # Get basic counts
            total_jobs = session.query(Job).filter(
                and_(Job.owner == user, Job.submit_time >= cutoff_date)
            ).count()
            
            completed_jobs = session.query(Job).filter(
                and_(Job.owner == user, Job.submit_time >= cutoff_date,
                     Job.state.in_([JobState.COMPLETED, JobState.FINISHED]))
            ).count()
            
            failed_jobs = session.query(Job).filter(
                and_(Job.owner == user, Job.submit_time >= cutoff_date,
                     Job.exit_status.isnot(None), Job.exit_status != 0)
            ).count()
            
            # Get average runtimes
            avg_runtime = session.query(func.avg(
                func.extract('epoch', Job.end_time - Job.start_time) / 60
            )).filter(
                and_(Job.owner == user, Job.submit_time >= cutoff_date,
                     Job.start_time.isnot(None), Job.end_time.isnot(None))
            ).scalar()
            
            return {
                'total_jobs': total_jobs,
                'completed_jobs': completed_jobs,
                'failed_jobs': failed_jobs,
                'success_rate': (completed_jobs / total_jobs * 100) if total_jobs > 0 else 0,
                'avg_runtime_minutes': avg_runtime or 0,
                'period_days': days
            }
    
    def get_recent_jobs(self, limit: int = 100) -> List[Job]:
        """Get recent jobs"""
        with self.get_session() as session:
            jobs = session.query(Job).order_by(desc(Job.last_updated)).limit(limit).all()
            session.expunge_all()
            return jobs


class JobStateInfo:
    """Information about a job's current state"""
    
    def __init__(self, state: JobState, priority: int, execution_node: Optional[str], queue: str):
        self.state = state
        self.priority = priority
        self.execution_node = execution_node
        self.queue = queue
    
    @classmethod
    def from_job(cls, job: Job) -> 'JobStateInfo':
        """Create from job object"""
        return cls(
            state=job.state,
            priority=job.priority,
            execution_node=job.execution_node,
            queue=job.queue
        )
    
    @classmethod
    def from_pbs_job(cls, job: 'PBSJob') -> 'JobStateInfo':
        """Create from PBSJob object"""
        return cls(
            state=job.state,
            priority=job.priority,
            execution_node=job.execution_node,
            queue=job.queue
        )
    
    def has_changes(self, job: Job) -> bool:
        """Check if job has state changes"""
        return (
            self.state != job.state or
            self.priority != job.priority or
            self.execution_node != job.execution_node or
            self.queue != job.queue
        )


class QueueRepository(BaseRepository):
    """Repository for queue-related database operations"""
    def create_or_update_queue(self, queue_data: Dict[str, Any]) -> Queue:
        with self.get_session() as session:
            q = session.query(Queue).filter(Queue.name == queue_data.get('name')).first()
            if not q:
                q = Queue(name=queue_data.get('name'))
                session.add(q)
            for key, value in queue_data.items():
                if hasattr(q, key):
                    setattr(q, key, value)
            session.commit()
            session.refresh(q)
            session.expunge(q)
            return q
    
    def get_queue_by_name(self, name: str) -> Optional[Queue]:
        """Get queue by name"""
        with self.get_session() as session:
            queue = session.query(Queue).filter(Queue.name == name).first()
            if queue:
                # Force loading of all attributes to avoid detached instance issues
                session.expunge(queue)
            return queue
    
    def get_all_queues(self) -> List[Queue]:
        """Get all queues"""
        with self.get_session() as session:
            queues = session.query(Queue).all()
            # Force loading of all attributes to avoid detached instance issues
            session.expunge_all()
            return queues
    
    def get_enabled_queues(self) -> List[Queue]:
        """Get enabled queues"""
        with self.get_session() as session:
            queues = session.query(Queue).filter(
                Queue.state.in_([QueueState.ENABLED_STARTED, QueueState.ENABLED_STOPPED])
            ).all()
            # Force loading of all attributes to avoid detached instance issues
            session.expunge_all()
            return queues
    
    def add_queue(self, queue: Queue) -> Queue:
        """Add new queue to database"""
        with self.get_session() as session:
            session.add(queue)
            session.commit()
            return queue
    
    def upsert_queues(self, queues: List[Queue]) -> None:
        """Insert or update queues in database"""
        with self.get_session() as session:
            for queue in queues:
                existing = session.query(Queue).filter(Queue.name == queue.name).first()
                if existing:
                    # Update existing queue
                    for attr, value in queue.__dict__.items():
                        if not attr.startswith('_'):
                            setattr(existing, attr, value)
                else:
                    # Add new queue
                    session.add(queue)
            session.commit()
    
    def update_queue(self, queue: Queue) -> Queue:
        """Update existing queue"""
        with self.get_session() as session:
            session.merge(queue)
            session.commit()
            return queue
    
    def delete_queue(self, name: str) -> bool:
        """Delete queue by name"""
        with self.get_session() as session:
            queue = session.query(Queue).filter(Queue.name == name).first()
            if queue:
                session.delete(queue)
                session.commit()
                return True
            return False
    
    def get_queue_snapshots(self, queue_name: str, hours: int = 24) -> List[QueueSnapshot]:
        """Get recent queue snapshots"""
        cutoff_time = datetime.now() - timedelta(hours=hours)
        with self.get_session() as session:
            return session.query(QueueSnapshot).filter(
                and_(QueueSnapshot.queue_name == queue_name, 
                     QueueSnapshot.timestamp >= cutoff_time)
            ).order_by(QueueSnapshot.timestamp).all()
    
    def add_queue_snapshot(self, queue_name: str | QueueSnapshot, snapshot_data: Optional[Dict[str, Any]] = None) -> QueueSnapshot:
        """Add queue snapshot. Accepts either a QueueSnapshot or (queue_name, data)."""
        with self.get_session() as session:
            if isinstance(queue_name, QueueSnapshot):
                snap = queue_name
            else:
                data = snapshot_data or {}
                filtered = {k: v for k, v in data.items() if hasattr(QueueSnapshot, k)}
                snap = QueueSnapshot(queue_name=queue_name, **filtered)
            session.add(snap)
            session.commit()
            session.refresh(snap)
            session.expunge(snap)
            return snap
    
    def add_queue_snapshots(self, snapshots: List[QueueSnapshot]) -> None:
        """Add multiple queue snapshots"""
        with self.get_session() as session:
            session.add_all(snapshots)
            session.commit()
    
    def get_queue_utilization_history(self, queue_name: str, days: int = 7) -> List[Dict[str, Any]]:
        """Get queue utilization history"""
        cutoff_time = datetime.now() - timedelta(days=days)
        with self.get_session() as session:
            snapshots = session.query(QueueSnapshot).filter(
                and_(QueueSnapshot.queue_name == queue_name,
                     QueueSnapshot.timestamp >= cutoff_time)
            ).order_by(QueueSnapshot.timestamp).all()
            
            return [
                {
                    'timestamp': snapshot.timestamp,
                    'utilization_percent': snapshot.utilization_percent,
                    'running_jobs': snapshot.running_jobs,
                    'queued_jobs': snapshot.queued_jobs
                }
                for snapshot in snapshots
            ]


class NodeRepository(BaseRepository):
    """Repository for node-related database operations"""
    def create_or_update_node(self, node_data: Dict[str, Any]) -> Node:
        with self.get_session() as session:
            n = session.query(Node).filter(Node.name == node_data.get('name')).first()
            if not n:
                n = Node(name=node_data.get('name'))
                session.add(n)
            for key, value in node_data.items():
                if hasattr(n, key):
                    setattr(n, key, value)
            session.commit()
            session.refresh(n)
            session.expunge(n)
            return n
    
    def get_node_by_name(self, name: str) -> Optional[Node]:
        """Get node by name"""
        with self.get_session() as session:
            node = session.query(Node).filter(Node.name == name).first()
            if node:
                # Force loading of all attributes to avoid detached instance issues
                session.expunge(node)
            return node
    
    def get_all_nodes(self) -> List[Node]:
        """Get all nodes"""
        with self.get_session() as session:
            nodes = session.query(Node).all()
            # Force loading of all attributes to avoid detached instance issues
            session.expunge_all()
            return nodes
    
    def get_available_nodes(self) -> List[Node]:
        """Get available nodes"""
        with self.get_session() as session:
            nodes = session.query(Node).filter(
                Node.state.in_([NodeState.FREE, NodeState.JOB_SHARING])
            ).all()
            # Force loading of all attributes to avoid detached instance issues
            session.expunge_all()
            return nodes
    
    def get_nodes_by_state(self, state: NodeState) -> List[Node]:
        """Get nodes in specific state"""
        with self.get_session() as session:
            nodes = session.query(Node).filter(Node.state == state).all()
            # Force loading of all attributes to avoid detached instance issues
            session.expunge_all()
            return nodes
    
    def add_node(self, node: Node) -> Node:
        """Add new node to database"""
        with self.get_session() as session:
            session.add(node)
            session.commit()
            return node
    
    def upsert_nodes(self, nodes: List[Node]) -> None:
        """Insert or update nodes in database"""
        with self.get_session() as session:
            for node in nodes:
                existing = session.query(Node).filter(Node.name == node.name).first()
                if existing:
                    # Update existing node
                    for attr, value in node.__dict__.items():
                        if not attr.startswith('_'):
                            setattr(existing, attr, value)
                else:
                    # Add new node
                    session.add(node)
            session.commit()
    
    def update_node(self, node: Node) -> Node:
        """Update existing node"""
        with self.get_session() as session:
            session.merge(node)
            session.commit()
            return node
    
    def delete_node(self, name: str) -> bool:
        """Delete node by name"""
        with self.get_session() as session:
            node = session.query(Node).filter(Node.name == name).first()
            if node:
                session.delete(node)
                session.commit()
                return True
            return False
    
    def get_node_snapshots(self, node_name: str, hours: int = 24) -> List[NodeSnapshot]:
        """Get recent node snapshots"""
        cutoff_time = datetime.now() - timedelta(hours=hours)
        with self.get_session() as session:
            return session.query(NodeSnapshot).filter(
                and_(NodeSnapshot.node_name == node_name,
                     NodeSnapshot.timestamp >= cutoff_time)
            ).order_by(NodeSnapshot.timestamp).all()
    
    def add_node_snapshot(self, node_name: str | NodeSnapshot, snapshot_data: Optional[Dict[str, Any]] = None) -> NodeSnapshot:
        """Add node snapshot. Accepts either a NodeSnapshot or (node_name, data)."""
        with self.get_session() as session:
            if isinstance(node_name, NodeSnapshot):
                snap = node_name
            else:
                data = snapshot_data or {}
                filtered = {k: v for k, v in data.items() if hasattr(NodeSnapshot, k)}
                snap = NodeSnapshot(node_name=node_name, **filtered)
            session.add(snap)
            session.commit()
            session.refresh(snap)
            session.expunge(snap)
            return snap
    
    def add_node_snapshots(self, snapshots: List[NodeSnapshot]) -> None:
        """Add multiple node snapshots"""
        with self.get_session() as session:
            session.add_all(snapshots)
            session.commit()
    
    def get_node_utilization_history(self, node_name: str, days: int = 7) -> List[Dict[str, Any]]:
        """Get node utilization history"""
        cutoff_time = datetime.now() - timedelta(days=days)
        with self.get_session() as session:
            snapshots = session.query(NodeSnapshot).filter(
                and_(NodeSnapshot.node_name == node_name,
                     NodeSnapshot.timestamp >= cutoff_time)
            ).order_by(NodeSnapshot.timestamp).all()
            
            return [
                {
                    'timestamp': snapshot.timestamp,
                    'cpu_utilization_percent': snapshot.cpu_utilization_percent,
                    'jobs_count': snapshot.jobs_count,
                    'load_percent': snapshot.load_percent
                }
                for snapshot in snapshots
            ]


class SystemRepository(BaseRepository):
    """Repository for system snapshot operations"""
    
    def get_latest_system_snapshot(self) -> Optional[SystemSnapshot]:
        """Get the most recent system snapshot"""
        with self.get_session() as session:
            snapshot = session.query(SystemSnapshot).order_by(desc(SystemSnapshot.timestamp)).first()
            if snapshot:
                session.expunge(snapshot)
            return snapshot
    
    def get_system_snapshots(self, hours: int = 24) -> List[SystemSnapshot]:
        """Get system snapshots from the last N hours"""
        cutoff_time = datetime.now() - timedelta(hours=hours)
        with self.get_session() as session:
            snapshots = session.query(SystemSnapshot).filter(
                SystemSnapshot.timestamp >= cutoff_time
            ).order_by(desc(SystemSnapshot.timestamp)).all()
            session.expunge_all()
            return snapshots
    
    def add_system_snapshot(self, snapshot: SystemSnapshot) -> SystemSnapshot:
        """Add system snapshot to database"""
        with self.get_session() as session:
            session.add(snapshot)
            session.commit()
            return snapshot
    
    def get_system_utilization_history(self, days: int = 7) -> List[Dict[str, Any]]:
        """Get system utilization history for analysis"""
        cutoff_date = datetime.now() - timedelta(days=days)
        with self.get_session() as session:
            snapshots = session.query(SystemSnapshot).filter(
                SystemSnapshot.timestamp >= cutoff_date
            ).order_by(SystemSnapshot.timestamp).all()
            
            history = []
            for snapshot in snapshots:
                history.append({
                    'timestamp': snapshot.timestamp,
                    'total_jobs': snapshot.total_jobs,
                    'running_jobs': snapshot.running_jobs,
                    'queued_jobs': snapshot.queued_jobs,
                    'total_nodes': snapshot.total_nodes,
                    'available_nodes': snapshot.available_nodes,
                    'system_utilization_percent': snapshot.system_utilization_percent,
                    'avg_queue_time_minutes': snapshot.avg_queue_time_minutes,
                    'avg_runtime_minutes': snapshot.avg_runtime_minutes
                })
            
            session.expunge_all()
            return history


class ReservationRepository(BaseRepository):
    """Repository for reservation-related database operations"""
    
    def get_reservation_by_id(self, reservation_id: str) -> Optional[Reservation]:
        """Get reservation by ID"""
        with self.get_session() as session:
            reservation = session.query(Reservation).filter(Reservation.reservation_id == reservation_id).first()
            if reservation:
                session.expunge(reservation)
            return reservation
    
    def get_active_reservations(self) -> List[Reservation]:
        """Get all active reservations (confirmed or running)"""
        with self.get_session() as session:
            reservations = session.query(Reservation).filter(
                Reservation.state.in_([ReservationState.CONFIRMED, ReservationState.RUNNING, 
                                     ReservationState.CONFIRMED_SHORT, ReservationState.RUNNING_SHORT])
            ).all()
            session.expunge_all()
            return reservations
    
    def get_reservations_by_user(self, user: str) -> List[Reservation]:
        """Get reservations for specific user"""
        with self.get_session() as session:
            reservations = session.query(Reservation).filter(Reservation.owner == user).all()
            session.expunge_all()
            return reservations
    
    def get_reservations_by_queue(self, queue: str) -> List[Reservation]:
        """Get reservations in specific queue"""
        with self.get_session() as session:
            reservations = session.query(Reservation).filter(Reservation.queue == queue).all()
            session.expunge_all()
            return reservations
    
    def get_reservations_by_state(self, state: ReservationState) -> List[Reservation]:
        """Get reservations in specific state"""
        with self.get_session() as session:
            reservations = session.query(Reservation).filter(Reservation.state == state).all()
            session.expunge_all()
            return reservations
    
    def get_historical_reservations(self, user: Optional[str] = None, days: int = 30) -> List[Reservation]:
        """Get historical reservations from database"""
        cutoff_date = datetime.now() - timedelta(days=days)
        with self.get_session() as session:
            query = session.query(Reservation).filter(Reservation.last_updated >= cutoff_date)
            if user:
                query = query.filter(Reservation.owner == user)
            reservations = query.all()
            session.expunge_all()
            return reservations
    
    def add_reservation(self, reservation: Reservation) -> Reservation:
        """Add new reservation to database"""
        with self.get_session() as session:
            session.add(reservation)
            session.commit()
            return reservation
    
    def upsert_reservations(self, reservations: List[Reservation]) -> None:
        """Insert or update reservations in database"""
        with self.get_session() as session:
            for reservation in reservations:
                # Check if reservation exists
                existing = session.query(Reservation).filter(
                    Reservation.reservation_id == reservation.reservation_id
                ).first()
                
                if existing:
                    # Update existing reservation
                    for key, value in reservation.__dict__.items():
                        if not key.startswith('_'):
                            setattr(existing, key, value)
                    existing.last_updated = datetime.now()
                else:
                    # Add new reservation
                    session.add(reservation)
            
            session.commit()
    
    def update_reservation(self, reservation: Reservation) -> Reservation:
        """Update existing reservation"""
        with self.get_session() as session:
            session.merge(reservation)
            session.commit()
            return reservation
    
    def delete_reservation(self, reservation_id: str) -> bool:
        """Delete reservation by ID"""
        with self.get_session() as session:
            reservation = session.query(Reservation).filter(Reservation.reservation_id == reservation_id).first()
            if reservation:
                session.delete(reservation)
                session.commit()
                return True
            return False
    
    def get_reservation_history(self, reservation_id: str) -> List[ReservationHistory]:
        """Get history for a specific reservation"""
        with self.get_session() as session:
            history = session.query(ReservationHistory).filter(
                ReservationHistory.reservation_id == reservation_id
            ).order_by(ReservationHistory.timestamp).all()
            session.expunge_all()
            return history
    
    def add_reservation_history(self, history: ReservationHistory) -> ReservationHistory:
        """Add reservation history entry"""
        with self.get_session() as session:
            session.add(history)
            session.commit()
            return history
    
    def add_reservation_history_batch(self, histories: List[ReservationHistory]) -> None:
        """Add multiple reservation history entries"""
        with self.get_session() as session:
            session.add_all(histories)
            session.commit()
    
    def get_latest_reservation_states(self) -> Dict[str, 'ReservationStateInfo']:
        """Get latest state for each reservation"""
        with self.get_session() as session:
            # Get the most recent history entry for each reservation
            latest_states = {}
            
            # Get all reservations with their current state
            reservations = session.query(Reservation).all()
            for reservation in reservations:
                latest_states[reservation.reservation_id] = ReservationStateInfo(
                    state=reservation.state,
                    owner=reservation.owner,
                    queue=reservation.queue,
                    last_updated=reservation.last_updated
                )
            
            session.expunge_all()
            return latest_states
    
    def get_user_reservation_statistics(self, user: str, days: int = 30) -> Dict[str, Any]:
        """Get reservation statistics for a user"""
        cutoff_date = datetime.now() - timedelta(days=days)
        with self.get_session() as session:
            # Get user's reservations
            reservations = session.query(Reservation).filter(
                Reservation.owner == user,
                Reservation.last_updated >= cutoff_date
            ).all()
            
            # Calculate statistics
            total_reservations = len(reservations)
            active_reservations = len([r for r in reservations if r.state in [
                ReservationState.CONFIRMED, ReservationState.RUNNING,
                ReservationState.CONFIRMED_SHORT, ReservationState.RUNNING_SHORT
            ]])
            
            total_node_hours = sum(
                (r.nodes or 0) * (r.duration_seconds or 0) / 3600 
                for r in reservations if r.nodes and r.duration_seconds
            )
            
            # Group by state
            state_counts = {}
            for reservation in reservations:
                state = reservation.state.value
                state_counts[state] = state_counts.get(state, 0) + 1
            
            session.expunge_all()
            
            return {
                'total_reservations': total_reservations,
                'active_reservations': active_reservations,
                'total_node_hours': total_node_hours,
                'state_distribution': state_counts,
                'period_days': days
            }
    
    def get_recent_reservations(self, limit: int = 100) -> List[Reservation]:
        """Get recent reservations"""
        with self.get_session() as session:
            reservations = session.query(Reservation).order_by(
                desc(Reservation.last_updated)
            ).limit(limit).all()
            session.expunge_all()
            return reservations


class ReservationStateInfo:
    """Information about a reservation's current state"""
    
    def __init__(self, state: ReservationState, owner: str, queue: str, last_updated: datetime):
        self.state = state
        self.owner = owner
        self.queue = queue
        self.last_updated = last_updated
    
    @classmethod
    def from_reservation(cls, reservation: Reservation) -> 'ReservationStateInfo':
        """Create from reservation object"""
        return cls(
            state=reservation.state,
            owner=reservation.owner,
            queue=reservation.queue,
            last_updated=reservation.last_updated
        )
    
    def has_changes(self, reservation: Reservation) -> bool:
        """Check if reservation has state changes"""
        return (
            self.state != reservation.state or
            self.owner != reservation.owner or
            self.queue != reservation.queue
        )


class DataCollectionRepository(BaseRepository):
    """Repository for data collection logging"""
    
    def log_collection_start(self, collection_type: str) -> int:
        """Log start of data collection"""
        with self.get_session() as session:
            log_entry = DataCollectionLog(
                collection_type=collection_type,
                status=DataCollectionStatus.SUCCESS,  # Will be updated on completion
                timestamp=datetime.now()
            )
            session.add(log_entry)
            session.commit()
            session.refresh(log_entry)
            return log_entry.id
    
    def log_collection_complete(self, log_id: int, status: DataCollectionStatus,
                              jobs_collected: int = 0, queues_collected: int = 0,
                              nodes_collected: int = 0, reservations_collected: int = 0,
                              duration: float = 0, error_message: str = None) -> None:
        """Log completion of data collection"""
        with self.get_session() as session:
            collection_log = session.query(DataCollectionLog).filter(DataCollectionLog.id == log_id).first()
            if collection_log:
                collection_log.status = status
                collection_log.jobs_collected = jobs_collected
                collection_log.queues_collected = queues_collected
                collection_log.nodes_collected = nodes_collected
                collection_log.reservations_collected = reservations_collected
                collection_log.duration_seconds = duration
                collection_log.error_message = error_message
                session.commit()
    
    def get_recent_collections(self, hours: int = 24) -> List[Dict[str, Any]]:
        """Get recent data collection logs"""
        cutoff_time = datetime.now() - timedelta(hours=hours)
        with self.get_session() as session:
            logs = session.query(DataCollectionLog).filter(
                DataCollectionLog.timestamp >= cutoff_time
            ).order_by(desc(DataCollectionLog.timestamp)).all()
            
            # Convert to plain dictionaries to avoid session detachment issues
            result = []
            for log in logs:
                result.append({
                    'timestamp': log.timestamp,
                    'collection_type': log.collection_type,
                    'status': log.status.value if log.status else 'UNKNOWN',
                    'jobs_collected': log.jobs_collected or 0,
                    'queues_collected': log.queues_collected or 0,
                    'nodes_collected': log.nodes_collected or 0,
                    'duration_seconds': log.duration_seconds or 0,
                    'error_message': log.error_message
                })
            return result
    
    def get_collection_statistics(self) -> Dict[str, Any]:
        """Get collection statistics"""
        with self.get_session() as session:
            # Count collections by status
            stats = {}
            for status in DataCollectionStatus:
                count = session.query(DataCollectionLog).filter(
                    DataCollectionLog.status == status
                ).count()
                stats[f"{status.value}_count"] = count
            
            # Recent success rate
            recent_logs = session.query(DataCollectionLog).filter(
                DataCollectionLog.timestamp >= datetime.now() - timedelta(hours=24)
            ).all()
            
            if recent_logs:
                success_count = sum(1 for log in recent_logs if log.status == DataCollectionStatus.SUCCESS)
                stats['recent_success_rate'] = success_count / len(recent_logs) * 100
            else:
                stats['recent_success_rate'] = 0
            
            # Average collection time
            avg_duration = session.query(func.avg(DataCollectionLog.duration_seconds)).filter(
                DataCollectionLog.duration_seconds.isnot(None)
            ).scalar()
            stats['avg_collection_time_seconds'] = avg_duration or 0
            
            return stats

# Repository factory for easy access
class RepositoryFactory:
    """Factory for creating repository instances"""
    
    def __init__(self, config: Optional[Config] = None):
        self.config = config or Config()
    
    def get_job_repository(self) -> JobRepository:
        return JobRepository(self.config)
    
    def get_queue_repository(self) -> QueueRepository:
        return QueueRepository(self.config)
    
    def get_node_repository(self) -> NodeRepository:
        return NodeRepository(self.config)
    
    def get_system_repository(self) -> SystemRepository:
        return SystemRepository(self.config)
    
    def get_reservation_repository(self) -> ReservationRepository:
        return ReservationRepository(self.config)
    
    def get_data_collection_repository(self) -> DataCollectionRepository:
        return DataCollectionRepository(self.config) 