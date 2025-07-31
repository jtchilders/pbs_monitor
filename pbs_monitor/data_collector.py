"""
Data Collector for PBS Monitor - Orchestrates data gathering from PBS system
"""

import logging
from typing import Dict, List, Optional, Set, Tuple, Any
from datetime import datetime, timedelta
import threading
import time

from .pbs_commands import PBSCommands, PBSCommandError
from .models.job import PBSJob, JobState
from .models.queue import PBSQueue
from .models.node import PBSNode
from .config import Config
from .utils.logging_setup import create_pbs_logger

# Database integration (optional)
try:
   from .database import (
      RepositoryFactory, ModelConverters, DataCollectionStatus,
      DatabaseManager, initialize_database
   )
   DATABASE_AVAILABLE = True
except ImportError:
   DATABASE_AVAILABLE = False


class JobStateInfo:
   """Cached job state information for change detection"""
   
   def __init__(self, state: JobState, priority: int, execution_node: Optional[str], queue: str):
      self.state = state
      self.priority = priority
      self.execution_node = execution_node
      self.queue = queue
   
   @classmethod
   def from_pbs_job(cls, job: PBSJob) -> 'JobStateInfo':
      """Create JobStateInfo from PBSJob"""
      return cls(
         state=job.state,
         priority=job.priority,
         execution_node=job.execution_node,
         queue=job.queue
      )
   
   def has_changes(self, job: PBSJob) -> bool:
      """Check if job has significant changes compared to cached state"""
      return (
         self.state != job.state or
         self.priority != job.priority or
         self.execution_node != job.execution_node or
         self.queue != job.queue
      )


class DataCollector:
   """Collects and manages PBS system data"""
   
   def __init__(self, config: Optional[Config] = None, use_sample_data: bool = False, 
                enable_database: bool = True):
      """
      Initialize data collector
      
      Args:
         config: Configuration object (optional)
         use_sample_data: Use sample JSON data instead of actual PBS commands
         enable_database: Enable database persistence (default: True)
      """
      self.config = config or Config()
      self.pbs_commands = PBSCommands(
         timeout=self.config.pbs.command_timeout,
         use_sample_data=use_sample_data
      )
      self.logger = create_pbs_logger(__name__)
      
      # Cached data
      self._jobs: List[PBSJob] = []
      self._queues: List[PBSQueue] = []
      self._nodes: List[PBSNode] = []
      self._server_data: Optional[Dict[str, Any]] = None
      
      # Cache timestamps
      self._last_job_update: Optional[datetime] = None
      self._last_queue_update: Optional[datetime] = None
      self._last_node_update: Optional[datetime] = None
      self._last_server_update: Optional[datetime] = None
      self._last_auto_persist: Optional[datetime] = None
      
      # Job state caching for smart history tracking
      self._job_state_cache: Dict[str, JobStateInfo] = {}
      self._cache_populated = False
      
      # Threading for background updates
      self._update_lock = threading.Lock()
      self._background_update_thread: Optional[threading.Thread] = None
      self._stop_background_updates = False
      
      # Database integration
      self._database_enabled = enable_database and DATABASE_AVAILABLE
      if self._database_enabled:
         try:
            self._repository_factory = RepositoryFactory(self.config)
            self._model_converters = ModelConverters()
            self.logger.info("Database integration enabled")
         except Exception as e:
            self.logger.warning(f"Database integration failed: {str(e)}")
            self._database_enabled = False
      else:
         self._repository_factory = None
         self._model_converters = None
         if enable_database and not DATABASE_AVAILABLE:
            self.logger.warning("Database integration requested but not available")
   
   def test_connection(self) -> bool:
      """
      Test connection to PBS system
      
      Returns:
         True if PBS is accessible
      """
      try:
         return self.pbs_commands.test_connection()
      except Exception as e:
         self.logger.error(f"Failed to test PBS connection: {str(e)}")
         return False
   
   def test_database_connection(self) -> bool:
      """
      Test database connection
      
      Returns:
         True if database is accessible
      """
      if not self._database_enabled:
         return False
      
      try:
         db_manager = DatabaseManager(self.config)
         return db_manager.test_connection()
      except Exception as e:
         self.logger.error(f"Failed to test database connection: {str(e)}")
         return False
   
   def get_jobs(self, 
                user: Optional[str] = None,
                force_refresh: bool = False,
                include_historical: bool = False) -> List[PBSJob]:
      """
      Get job information
      
      Args:
         user: Filter by username (optional)
         force_refresh: Force refresh from PBS system
         include_historical: Include historical jobs from database
         
      Returns:
         List of PBSJob objects
      """
      should_refresh = (
         force_refresh or 
         self._last_job_update is None or
         (datetime.now() - self._last_job_update).total_seconds() > 
         self.config.pbs.job_refresh_interval
      )
      
      if should_refresh:
         self._refresh_jobs()
      
      # Start with current PBS jobs
      jobs = self._jobs.copy()
      
      # Add historical jobs if requested and database is available
      if include_historical and self._database_enabled:
         try:
            job_repo = self._repository_factory.get_job_repository()
            historical_jobs = job_repo.get_historical_jobs(user=user)
            historical_pbs_jobs = [
               self._model_converters.job.from_database(job) for job in historical_jobs
            ]
            
            # Merge with current jobs, avoiding duplicates
            current_job_ids = {job.job_id for job in jobs}
            jobs.extend([job for job in historical_pbs_jobs if job.job_id not in current_job_ids])
         except Exception as e:
            self.logger.warning(f"Failed to retrieve historical jobs: {str(e)}")
      
      # Filter by user if specified
      if user:
         return [job for job in jobs if job.owner == user]
      
      return jobs
   
   def get_completed_jobs(self, 
                         user: Optional[str] = None, 
                         include_pbs_history: bool = True,
                         days: int = 7) -> List[PBSJob]:
      """
      Get completed job information from both PBS history and database
      
      Args:
         user: Filter by username (optional)
         include_pbs_history: Include recent completed jobs from qstat -x
         days: Number of days to look back for PBS history
         
      Returns:
         List of completed PBSJob objects
      """
      completed_jobs = []
      job_ids_seen = set()
      
      # Get recent completed jobs from PBS if requested
      if include_pbs_history:
         try:
            pbs_completed = self.pbs_commands.qstat_completed_jobs(user=user, days=days)
            completed_jobs.extend(pbs_completed)
            job_ids_seen.update(job.job_id for job in pbs_completed)
            self.logger.debug(f"Retrieved {len(pbs_completed)} completed jobs from PBS")
         except Exception as e:
            self.logger.warning(f"Failed to get PBS completed jobs: {str(e)}")
      
      # Get completed jobs from database if available
      if self._database_enabled:
         try:
            job_repo = self._repository_factory.get_job_repository()
            
            # Get completed jobs from database
            db_jobs = job_repo.get_historical_jobs(user=user, days=days*2)  # Look back further in DB
            db_completed_jobs = [job for job in db_jobs if job.is_completed()]
            
            # Convert to PBSJob objects and add if not already seen
            for db_job in db_completed_jobs:
               if db_job.job_id not in job_ids_seen:
                  try:
                     pbs_job = self._model_converters.job.from_database(db_job)
                     completed_jobs.append(pbs_job)
                     job_ids_seen.add(pbs_job.job_id)
                  except Exception as e:
                     self.logger.warning(f"Failed to convert job {db_job.job_id}: {str(e)}")
            
            self.logger.debug(f"Retrieved {len(db_completed_jobs)} additional completed jobs from database")
         except Exception as e:
            self.logger.warning(f"Failed to retrieve completed jobs from database: {str(e)}")
      
      return completed_jobs
   
   def get_queues(self, force_refresh: bool = False) -> List[PBSQueue]:
      """
      Get queue information
      
      Args:
         force_refresh: Force refresh from PBS system
         
      Returns:
         List of PBSQueue objects
      """
      should_refresh = (
         force_refresh or 
         self._last_queue_update is None or
         (datetime.now() - self._last_queue_update).total_seconds() > 
         self.config.pbs.queue_refresh_interval
      )
      
      if should_refresh:
         self._refresh_queues()
      
      return self._queues.copy()
   
   def get_nodes(self, force_refresh: bool = False) -> List[PBSNode]:
      """
      Get node information
      
      Args:
         force_refresh: Force refresh from PBS system
         
      Returns:
         List of PBSNode objects
      """
      should_refresh = (
         force_refresh or 
         self._last_node_update is None or
         (datetime.now() - self._last_node_update).total_seconds() > 
         self.config.pbs.node_refresh_interval
      )
      
      if should_refresh:
         self._refresh_nodes()
      
      return self._nodes.copy()
   
   def get_job_by_id(self, job_id: str) -> Optional[PBSJob]:
      """
      Get specific job by ID
      
      Args:
         job_id: Job ID to find
         
      Returns:
         PBSJob object or None if not found
      """
      # First try current PBS data
      try:
         jobs = self.pbs_commands.qstat_jobs(job_id=job_id)
         if jobs:
            return jobs[0]
      except PBSCommandError as e:
         self.logger.error(f"Failed to get job {job_id} from PBS: {str(e)}")
      
      # Fall back to database if available
      if self._database_enabled:
         try:
            job_repo = self._repository_factory.get_job_repository()
            db_job = job_repo.get_job_by_id(job_id)
            if db_job:
               return self._model_converters.job.from_database(db_job)
         except Exception as e:
            self.logger.warning(f"Failed to get job {job_id} from database: {str(e)}")
      
      return None
   
   def get_system_summary(self) -> Dict[str, Any]:
      """
      Get system summary statistics
      
      Returns:
         Dictionary with system summary information
      """
      jobs = self.get_jobs()
      queues = self.get_queues()
      nodes = self.get_nodes()
      
      # Job statistics
      job_stats = {
         'total': len(jobs),
         'running': len([j for j in jobs if j.state == JobState.RUNNING]),
         'queued': len([j for j in jobs if j.state == JobState.QUEUED]),
         'held': len([j for j in jobs if j.state == JobState.HELD]),
         'other': len([j for j in jobs if j.state not in [JobState.RUNNING, JobState.QUEUED, JobState.HELD]])
      }
      
      # Queue depth statistics
      from .analytics.queue_depth import QueueDepthCalculator
      queue_calculator = QueueDepthCalculator()
      queue_depth = {
         'total_node_hours': queue_calculator.calculate_total_node_hours(jobs)
      }
      
      # Queue statistics
      queue_stats = {
         'total': len(queues),
         'enabled': len([q for q in queues if q.is_enabled()]),
         'disabled': len(queues) - len([q for q in queues if q.is_enabled()])
      }
      
      # Node statistics
      node_stats = {
         'total': len(nodes),
         'available': len([n for n in nodes if n.is_available()]),
         'busy': len([n for n in nodes if n.is_occupied()]),
         'offline': len([n for n in nodes if not n.is_available() and not n.is_occupied()])
      }
      
      # Resource statistics
      total_cores = sum(node.ncpus for node in nodes)
      used_cores = sum(len(node.jobs) for node in nodes)
      
      resource_stats = {
         'total_cores': total_cores,
         'used_cores': used_cores,
         'available_cores': total_cores - used_cores,
         'utilization': (used_cores / total_cores * 100) if total_cores > 0 else 0
      }
      
      return {
         'timestamp': datetime.now(),
         'jobs': job_stats,
         'queues': queue_stats,
         'nodes': node_stats,
         'resources': resource_stats,
         'queue_depth': queue_depth
      }
   
   def get_user_jobs(self, user: str) -> List[PBSJob]:
      """
      Get jobs for specific user
      
      Args:
         user: Username
         
      Returns:
         List of user's jobs
      """
      return self.get_jobs(user=user)
   
   def _populate_job_state_cache_if_needed(self) -> None:
      """Populate job state cache from database on first use"""
      if self._cache_populated or not self._database_enabled:
         return
      
      try:
         job_repo = self._repository_factory.get_job_repository()
         latest_states = job_repo.get_latest_job_states()
         
         with self._update_lock:
            self._job_state_cache.update(latest_states)
            self._cache_populated = True
            
         self.logger.debug(f"Populated job state cache with {len(latest_states)} entries")
      except Exception as e:
         self.logger.warning(f"Failed to populate job state cache: {str(e)}")
         # Mark as populated to avoid repeated failures
         self._cache_populated = True
   
   def _create_job_history_for_changes(self, current_jobs: List[PBSJob], 
                                      data_collection_id: Optional[int] = None) -> List['JobHistory']:
      """Create job history entries only for jobs that have changed"""
      if not self._database_enabled:
         return []
      
      # Import here to avoid circular import
      from .database.models import JobHistory
      
      # Ensure cache is populated
      self._populate_job_state_cache_if_needed()
      
      history_entries = []
      cache_updates = {}
      
      for job in current_jobs:
         cached_state = self._job_state_cache.get(job.job_id)
         
         # Create history entry if:
         # 1. Job is new (not in cache)
         # 2. Job has significant changes
         should_create_entry = (
            cached_state is None or 
            cached_state.has_changes(job)
         )
         
         if should_create_entry:
            # Create history entry using the existing converter method
            history_entry = self._model_converters.job.to_job_history(job, data_collection_id)
            history_entries.append(history_entry)
            
            # Prepare cache update
            cache_updates[job.job_id] = JobStateInfo.from_pbs_job(job)
            
            # Log the change for debugging
            change_reason = "new job" if cached_state is None else "state/attribute change"
            self.logger.debug(f"Creating history entry for job {job.job_id}: {change_reason}")
      
      # Update cache with new states
      with self._update_lock:
         self._job_state_cache.update(cache_updates)
      
      self.logger.debug(f"Created {len(history_entries)} job history entries from {len(current_jobs)} jobs")
      return history_entries
   
   def _cleanup_job_state_cache(self, current_job_ids: Set[str]) -> None:
      """Remove cache entries for jobs that no longer exist"""
      if not self._cache_populated:
         return
      
      with self._update_lock:
         # Find jobs that are in cache but not in current job list
         cached_job_ids = set(self._job_state_cache.keys())
         obsolete_job_ids = cached_job_ids - current_job_ids
         
         # Remove obsolete entries
         for job_id in obsolete_job_ids:
            del self._job_state_cache[job_id]
         
         if obsolete_job_ids:
            self.logger.debug(f"Cleaned up {len(obsolete_job_ids)} obsolete job entries from state cache")
   
   def get_queue_utilization(self) -> Dict[str, float]:
      """
      Get utilization percentage for each queue
      
      Returns:
         Dictionary mapping queue names to utilization percentages
      """
      queues = self.get_queues()
      return {queue.name: queue.utilization_percentage() for queue in queues}
   
   def collect_and_persist(self, collection_type: str = "manual") -> Dict[str, Any]:
      """
      Collect current PBS data and persist to database
      
      Args:
         collection_type: Type of collection ("manual", "daemon", "cli")
      
      Returns:
         Dictionary with collection results
      """
      if not self._database_enabled:
         raise RuntimeError("Database not available for persistence")
      
      collection_start = datetime.now()
      
      # Log collection start
      self.logger.info(f"Starting {collection_type} data collection and persistence")
      
      # Start data collection log
      collection_repo = self._repository_factory.get_data_collection_repository()
      log_id = collection_repo.log_collection_start(collection_type)
      
      try:
         # Collect all data
         self.refresh_all()
         
         # Also collect recently completed jobs to capture them before PBS purges them
         completed_jobs = []
         try:
            pbs_completed = self.pbs_commands.qstat_completed_jobs()
            completed_jobs.extend(pbs_completed)
            self.logger.debug(f"Collected {len(pbs_completed)} completed jobs from PBS history")
         except Exception as e:
            self.logger.warning(f"Failed to collect completed jobs from PBS: {str(e)}")
         
         # Combine current jobs with completed jobs for database storage
         all_jobs_for_db = self._jobs + completed_jobs
         
         # Convert to database models - but use smart job history creation
         db_data = {
            'jobs': [self._model_converters.job.to_database(job) for job in all_jobs_for_db],
            'queues': [self._model_converters.queue.to_database(queue) for queue in self._queues],
            'nodes': [self._model_converters.node.to_database(node) for node in self._nodes],
            'job_history': self._create_job_history_for_changes(all_jobs_for_db, log_id),
            'queue_snapshots': [self._model_converters.queue.to_queue_snapshot(queue) for queue in self._queues],
            'node_snapshots': [self._model_converters.node.to_node_snapshot(node) for node in self._nodes],
            'system_snapshot': self._model_converters.system.to_system_snapshot(self._jobs, self._queues, self._nodes)
         }
         
         # Clean up cache for jobs that no longer exist
         current_job_ids = {job.job_id for job in all_jobs_for_db}
         self._cleanup_job_state_cache(current_job_ids)
         
         # Add collection log ID to remaining entries (job_history already has it)
         for entry in db_data['queue_snapshots']:
            entry.data_collection_id = log_id
         for entry in db_data['node_snapshots']:
            entry.data_collection_id = log_id
         db_data['system_snapshot'].data_collection_id = log_id
         
         # Persist to database
         job_repo = self._repository_factory.get_job_repository()
         queue_repo = self._repository_factory.get_queue_repository()
         node_repo = self._repository_factory.get_node_repository()
         system_repo = self._repository_factory.get_system_repository()
         
         # Upsert current state
         job_repo.upsert_jobs(db_data['jobs'])
         queue_repo.upsert_queues(db_data['queues'])
         node_repo.upsert_nodes(db_data['nodes'])
         
         # Add historical snapshots
         job_repo.add_job_history_batch(db_data['job_history'])
         queue_repo.add_queue_snapshots(db_data['queue_snapshots'])
         node_repo.add_node_snapshots(db_data['node_snapshots'])
         system_repo.add_system_snapshot(db_data['system_snapshot'])
         
         # Log completion
         duration = (datetime.now() - collection_start).total_seconds()
         collection_repo.log_collection_complete(
            log_id, DataCollectionStatus.SUCCESS,
            jobs_collected=len(db_data['jobs']),
            queues_collected=len(db_data['queues']),
            nodes_collected=len(db_data['nodes']),
            duration=duration
         )
         
         # Log successful completion with summary
         self.logger.info(f"Completed {collection_type} data collection successfully: "
                         f"{len(db_data['jobs'])} jobs, {len(db_data['queues'])} queues, "
                         f"{len(db_data['nodes'])} nodes in {duration:.1f}s")
         
         return {
            'status': 'success',
            'jobs_collected': len(db_data['jobs']),
            'completed_jobs_collected': len(completed_jobs),
            'queues_collected': len(db_data['queues']),
            'nodes_collected': len(db_data['nodes']),
            'duration_seconds': duration,
            'collection_id': log_id
         }
         
      except Exception as e:
         # Log failure
         duration = (datetime.now() - collection_start).total_seconds()
         collection_repo.log_collection_complete(
            log_id, DataCollectionStatus.FAILED,
            duration=duration,
            error_message=str(e)
         )
         
         self.logger.error(f"Failed {collection_type} data collection after {duration:.1f}s: {str(e)}")
         raise
   
   def get_historical_job_data(self, job_id: str) -> Dict[str, Any]:
      """
      Get historical data for a specific job
      
      Args:
         job_id: Job ID to look up
         
      Returns:
         Dictionary with job history and state transitions
      """
      if not self._database_enabled:
         raise RuntimeError("Database not available for historical data")
      
      job_repo = self._repository_factory.get_job_repository()
      
      # Get job details
      job = job_repo.get_job_by_id(job_id)
      if not job:
         return {'error': f'Job {job_id} not found'}
      
      # Get job history
      history = job_repo.get_job_history(job_id)
      
      # Get state transitions
      transitions = []
      for i in range(1, len(history)):
         prev_state = history[i-1].state
         curr_state = history[i].state
         if prev_state != curr_state:
            transitions.append({
               'from_state': prev_state.value,
               'to_state': curr_state.value,
               'timestamp': history[i].timestamp,
               'duration_minutes': (history[i].timestamp - history[i-1].timestamp).total_seconds() / 60
            })
      
      return {
         'job': self._model_converters.job.from_database(job),
         'history_entries': len(history),
         'state_transitions': transitions,
         'first_seen': history[0].timestamp if history else None,
         'last_seen': history[-1].timestamp if history else None
      }
   
   def get_user_job_statistics(self, user: str, days: int = 30) -> Dict[str, Any]:
      """
      Get job statistics for a user over specified time period
      
      Args:
         user: Username
         days: Number of days to look back
         
      Returns:
         Dictionary with user job statistics
      """
      if not self._database_enabled:
         raise RuntimeError("Database not available for statistics")
      
      job_repo = self._repository_factory.get_job_repository()
      return job_repo.get_user_job_statistics(user, days)
   
   def _refresh_jobs(self) -> None:
      """Refresh job data from PBS system"""
      try:
         # Get cached server data and defaults BEFORE acquiring lock to avoid deadlock
         server_data = self.get_cached_server_data()
         server_defaults = None
         if server_data:
            # Extract server defaults from server data
            server_info = server_data.get("Server", {})
            for server_name, server_details in server_info.items():
               server_defaults = server_details.get("resources_default", {})
               break
         self.logger.debug(f"Server defaults: {server_defaults}")
         
         with self._update_lock:
            self.logger.debug("Refreshing job data")
            self._jobs = self.pbs_commands.qstat_jobs(
               server_defaults=server_defaults, 
               server_data=server_data
            )
            self._last_job_update = datetime.now()
            self.logger.debug(f"Updated {len(self._jobs)} jobs")
      except PBSCommandError as e:
         self.logger.error(f"Failed to refresh jobs: {str(e)}")
   
   def _refresh_queues(self) -> None:
      """Refresh queue data from PBS system"""
      try:
         with self._update_lock:
            self.logger.debug("Refreshing queue data")
            self._queues = self.pbs_commands.qstat_queues()
            self._last_queue_update = datetime.now()
            self.logger.debug(f"Updated {len(self._queues)} queues")
      except PBSCommandError as e:
         self.logger.error(f"Failed to refresh queues: {str(e)}")
   
   def _refresh_nodes(self) -> None:
      """Refresh node data from PBS system"""
      try:
         with self._update_lock:
            self.logger.debug("Refreshing node data")
            self._nodes = self.pbs_commands.pbsnodes()
            self._last_node_update = datetime.now()
            self.logger.debug(f"Updated {len(self._nodes)} nodes")
      except PBSCommandError as e:
         self.logger.error(f"Failed to refresh nodes: {str(e)}")
   
   def _refresh_server(self) -> None:
      """Refresh server data from PBS system"""
      try:
         with self._update_lock:
            self.logger.debug("Refreshing server data 2")
            self._server_data = self.pbs_commands.qstat_server()
            self.logger.debug("Retrieved server data")
            self._last_server_update = datetime.now()
            self.logger.debug("Updated server data")
      except PBSCommandError as e:
         self.logger.error(f"Failed to refresh server data: {str(e)}")
   
   def get_cached_server_defaults(self) -> Optional[Dict[str, Any]]:
      """
      Get cached server defaults, refreshing if needed
      
      Returns:
         Server defaults dictionary or None if not available
      """
      should_refresh = (
         self._last_server_update is None or
         (datetime.now() - self._last_server_update).total_seconds() > 
         self.config.pbs.server_refresh_interval
      )
      
      if should_refresh:
         self.logger.debug("Refreshing server data 1")
         self._refresh_server()
      
      if self._server_data:
         # Extract server defaults from server data
         server_info = self._server_data.get("Server", {})
         for server_name, server_details in server_info.items():
            return server_details.get("resources_default", {})
      
      return None
   
   def get_cached_server_data(self) -> Optional[Dict[str, Any]]:
      """
      Get cached server data, refreshing if needed
      
      Returns:
         Full server data dictionary or None if not available
      """
      should_refresh = (
         self._last_server_update is None or
         (datetime.now() - self._last_server_update).total_seconds() > 
         self.config.pbs.server_refresh_interval
      )
      
      if should_refresh:
         self._refresh_server()
      
      return self._server_data
   
   def refresh_all(self) -> None:
      """Refresh all data from PBS system"""
      self._refresh_server()
      self._refresh_jobs()
      self._refresh_queues()
      self._refresh_nodes()
   
   def start_background_updates(self) -> None:
      """Start background thread for automatic data updates"""
      if self._background_update_thread is not None:
         self.logger.warning("Background updates already running")
         return
      
      self._stop_background_updates = False
      self._background_update_thread = threading.Thread(
         target=self._background_update_loop,
         daemon=True
      )
      self._background_update_thread.start()
      self.logger.info("Started background updates")
   
   def stop_background_updates(self) -> None:
      """Stop background data updates"""
      if self._background_update_thread is None:
         return
      
      self._stop_background_updates = True
      self._background_update_thread.join(timeout=5)
      self._background_update_thread = None
      self.logger.info("Stopped background updates")
   
   def _background_update_loop(self) -> None:
      """Background update loop"""
      while not self._stop_background_updates:
         try:
            # Update jobs most frequently
            if (self._last_job_update is None or 
                (datetime.now() - self._last_job_update).total_seconds() > 
                self.config.pbs.job_refresh_interval):
               self._refresh_jobs()
            
            # Update nodes less frequently
            if (self._last_node_update is None or 
                (datetime.now() - self._last_node_update).total_seconds() > 
                self.config.pbs.node_refresh_interval):
               self._refresh_nodes()
            
            # Update queues least frequently
            if (self._last_queue_update is None or 
                (datetime.now() - self._last_queue_update).total_seconds() > 
                self.config.pbs.queue_refresh_interval):
               self._refresh_queues()
            
            # Optionally persist data if database is enabled and interval has elapsed
            if (self._database_enabled and 
                hasattr(self.config, 'database') and 
                self.config.database.auto_persist):
               
               # Check if auto_persist interval has elapsed
               should_persist = (
                  self._last_auto_persist is None or
                  (datetime.now() - self._last_auto_persist).total_seconds() > 
                  self.config.database.auto_persist_interval
               )
               
               if should_persist:
                  try:
                     self.logger.debug("Triggering periodic database collection from daemon")
                     result = self.collect_and_persist(collection_type="daemon")
                     self._last_auto_persist = datetime.now()
                     self.logger.debug(f"Periodic collection completed: {result['jobs_collected']} jobs, "
                                      f"{result['queues_collected']} queues, {result['nodes_collected']} nodes")
                  except Exception as e:
                     self.logger.error(f"Failed to persist data: {str(e)}")
            
            # Sleep for a short interval
            time.sleep(10)
            
         except Exception as e:
            self.logger.error(f"Error in background update loop: {str(e)}")
            time.sleep(30)  # Wait longer on error
   
   @property
   def database_enabled(self) -> bool:
      """Check if database functionality is enabled"""
      return self._database_enabled
   
   def __del__(self):
      """Cleanup on destruction"""
      self.stop_background_updates() 