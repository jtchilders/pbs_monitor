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
    NodeState, DataCollectionStatus
)
from ..config import Config


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
            return session.query(Job).filter(
                Job.state.in_([JobState.RUNNING, JobState.QUEUED, JobState.HELD])
            ).all()
    
    def get_jobs_by_user(self, user: str) -> List[Job]:
        """Get jobs for specific user"""
        with self.get_session() as session:
            return session.query(Job).filter(Job.owner == user).all()
    
    def get_jobs_by_queue(self, queue: str) -> List[Job]:
        """Get jobs in specific queue"""
        with self.get_session() as session:
            return session.query(Job).filter(Job.queue == queue).all()
    
    def get_jobs_by_state(self, state: JobState) -> List[Job]:
        """Get jobs in specific state"""
        with self.get_session() as session:
            return session.query(Job).filter(Job.state == state).all()
    
    def get_historical_jobs(self, user: Optional[str] = None, days: int = 30) -> List[Job]:
        """Get historical jobs from database"""
        cutoff_date = datetime.now() - timedelta(days=days)
        with self.get_session() as session:
            query = session.query(Job).filter(Job.last_updated >= cutoff_date)
            if user:
                query = query.filter(Job.owner == user)
            return query.all()
    
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
            return session.query(JobHistory).filter(
                JobHistory.job_id == job_id
            ).order_by(JobHistory.timestamp).all()
    
    def add_job_history(self, job_history: JobHistory) -> JobHistory:
        """Add job history entry"""
        with self.get_session() as session:
            session.add(job_history)
            session.commit()
            return job_history
    
    def add_job_history_batch(self, job_histories: List[JobHistory]) -> None:
        """Add multiple job history entries"""
        with self.get_session() as session:
            session.add_all(job_histories)
            session.commit()
    
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
        """Get most recent jobs"""
        with self.get_session() as session:
            return session.query(Job).order_by(desc(Job.submit_time)).limit(limit).all()


class QueueRepository(BaseRepository):
    """Repository for queue-related database operations"""
    
    def get_queue_by_name(self, name: str) -> Optional[Queue]:
        """Get queue by name"""
        with self.get_session() as session:
            return session.query(Queue).filter(Queue.name == name).first()
    
    def get_all_queues(self) -> List[Queue]:
        """Get all queues"""
        with self.get_session() as session:
            return session.query(Queue).all()
    
    def get_enabled_queues(self) -> List[Queue]:
        """Get enabled queues"""
        with self.get_session() as session:
            return session.query(Queue).filter(
                Queue.state.in_([QueueState.ENABLED_STARTED, QueueState.ENABLED_STOPPED])
            ).all()
    
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
    
    def add_queue_snapshot(self, snapshot: QueueSnapshot) -> QueueSnapshot:
        """Add queue snapshot"""
        with self.get_session() as session:
            session.add(snapshot)
            session.commit()
            return snapshot
    
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
    
    def get_node_by_name(self, name: str) -> Optional[Node]:
        """Get node by name"""
        with self.get_session() as session:
            return session.query(Node).filter(Node.name == name).first()
    
    def get_all_nodes(self) -> List[Node]:
        """Get all nodes"""
        with self.get_session() as session:
            return session.query(Node).all()
    
    def get_available_nodes(self) -> List[Node]:
        """Get available nodes"""
        with self.get_session() as session:
            return session.query(Node).filter(
                Node.state.in_([NodeState.FREE, NodeState.JOB_SHARING])
            ).all()
    
    def get_nodes_by_state(self, state: NodeState) -> List[Node]:
        """Get nodes in specific state"""
        with self.get_session() as session:
            return session.query(Node).filter(Node.state == state).all()
    
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
    
    def add_node_snapshot(self, snapshot: NodeSnapshot) -> NodeSnapshot:
        """Add node snapshot"""
        with self.get_session() as session:
            session.add(snapshot)
            session.commit()
            return snapshot
    
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
    """Repository for system-level statistics"""
    
    def get_latest_system_snapshot(self) -> Optional[SystemSnapshot]:
        """Get most recent system snapshot"""
        with self.get_session() as session:
            return session.query(SystemSnapshot).order_by(
                desc(SystemSnapshot.timestamp)
            ).first()
    
    def get_system_snapshots(self, hours: int = 24) -> List[SystemSnapshot]:
        """Get recent system snapshots"""
        cutoff_time = datetime.now() - timedelta(hours=hours)
        with self.get_session() as session:
            return session.query(SystemSnapshot).filter(
                SystemSnapshot.timestamp >= cutoff_time
            ).order_by(SystemSnapshot.timestamp).all()
    
    def add_system_snapshot(self, snapshot: SystemSnapshot) -> SystemSnapshot:
        """Add system snapshot"""
        with self.get_session() as session:
            session.add(snapshot)
            session.commit()
            return snapshot
    
    def get_system_utilization_history(self, days: int = 7) -> List[Dict[str, Any]]:
        """Get system utilization history"""
        cutoff_time = datetime.now() - timedelta(days=days)
        with self.get_session() as session:
            snapshots = session.query(SystemSnapshot).filter(
                SystemSnapshot.timestamp >= cutoff_time
            ).order_by(SystemSnapshot.timestamp).all()
            
            return [
                {
                    'timestamp': snapshot.timestamp,
                    'system_utilization_percent': snapshot.system_utilization_percent,
                    'total_jobs': snapshot.total_jobs,
                    'running_jobs': snapshot.running_jobs,
                    'queued_jobs': snapshot.queued_jobs,
                    'total_cores': snapshot.total_cores,
                    'used_cores': snapshot.used_cores
                }
                for snapshot in snapshots
            ]


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
                              nodes_collected: int = 0, duration: float = 0,
                              error_message: str = None) -> None:
        """Log completion of data collection"""
        with self.get_session() as session:
            log_entry = session.query(DataCollectionLog).filter(
                DataCollectionLog.id == log_id
            ).first()
            
            if log_entry:
                log_entry.status = status
                log_entry.jobs_collected = jobs_collected
                log_entry.queues_collected = queues_collected
                log_entry.nodes_collected = nodes_collected
                log_entry.duration_seconds = duration
                log_entry.error_message = error_message
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
        """Get job repository instance"""
        return JobRepository(self.config)
    
    def get_queue_repository(self) -> QueueRepository:
        """Get queue repository instance"""
        return QueueRepository(self.config)
    
    def get_node_repository(self) -> NodeRepository:
        """Get node repository instance"""
        return NodeRepository(self.config)
    
    def get_system_repository(self) -> SystemRepository:
        """Get system repository instance"""
        return SystemRepository(self.config)
    
    def get_data_collection_repository(self) -> DataCollectionRepository:
        """Get data collection repository instance"""
        return DataCollectionRepository(self.config) 