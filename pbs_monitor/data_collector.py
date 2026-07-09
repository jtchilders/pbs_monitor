"""
Data Collector for PBS Monitor - Orchestrates data gathering from PBS system
"""

import logging
from typing import Dict, List, Optional, Set, Tuple, Any
from datetime import datetime, timedelta, timezone
import threading
import time

from .pbs_commands import PBSCommands, PBSCommandError
from .models.job import PBSJob, JobState
from .models.queue import PBSQueue
from .models.node import PBSNode
from .models.reservation import PBSReservation, ReservationState
from .config import Config
from .utils.logging_setup import create_pbs_logger

# Database integration (optional)
try:
   from .database import (
      RepositoryFactory, ModelConverters, DataCollectionStatus,
      DatabaseManager, initialize_database
   )
   DATABASE_AVAILABLE = True
   _DATABASE_IMPORT_ERROR = None
except ImportError as _db_import_exc:
   DATABASE_AVAILABLE = False
   # Preserve the real cause. A bare pass here masks genuine bugs — e.g. a
   # broken import *inside* the database package (missing analytics module,
   # syntax error) looks identical to "database extras not installed", and
   # surfaces later as a confusing NameError when ModelConverters is used.
   _DATABASE_IMPORT_ERROR = _db_import_exc
   import logging as _logging
   _logging.getLogger(__name__).warning(
      "Database integration unavailable — import failed: %s", _db_import_exc
   )

from .database.repositories import RepositoryFactory, JobStateInfo, ReservationStateInfo


class DataCollector:
   """Collects and manages PBS system data"""
   
   def __init__(self, config: Optional[Config] = None, use_sample_data: bool = False, 
                enable_database: bool = True):
      """
      Initialize data collector
      
      Args:
         config: Configuration object
         use_sample_data: Use sample JSON data instead of actual PBS commands
         enable_database: Enable database integration
      """
      self.config = config or Config()
      self.use_sample_data = use_sample_data
      self._database_enabled = enable_database
      
      # Initialize PBS commands wrapper
      self.pbs_commands = PBSCommands(timeout=self.config.pbs.command_timeout, 
                                     use_sample_data=use_sample_data)
      
      # Data storage
      self._jobs: List[PBSJob] = []
      self._queues: List[PBSQueue] = []
      self._nodes: List[PBSNode] = []
      self._reservations: List[PBSReservation] = []
      self._server_data: Optional[Dict[str, Any]] = None
      
      # Last update timestamps
      self._last_job_update: Optional[datetime] = None
      self._last_queue_update: Optional[datetime] = None
      self._last_node_update: Optional[datetime] = None
      self._last_reservation_update: Optional[datetime] = None
      self._last_server_update: Optional[datetime] = None
      self._last_auto_persist: Optional[datetime] = None
      self._last_snapshot_persist: Optional[datetime] = None
      
      # Job state tracking for history
      self._job_state_cache: Dict[str, JobStateInfo] = {}
      self._reservation_state_cache: Dict[str, 'ReservationStateInfo'] = {}
      
      # Threading support
      self._update_lock = threading.Lock()
      self._background_update_thread: Optional[threading.Thread] = None
      self._stop_background_updates = False
      
      # Database integration
      if self._database_enabled:
         if not DATABASE_AVAILABLE:
            raise RuntimeError(
               "Database integration was requested but the database package "
               "failed to import. Root cause: "
               f"{_DATABASE_IMPORT_ERROR!r}. "
               "This usually means a broken/partial deployment (e.g. the "
               "pbs_monitor.analytics.outcome_classifier module is missing or "
               "stale). Reinstall/redeploy the package and retry."
            )
         self._repository_factory = RepositoryFactory(config)
         self._model_converters = ModelConverters()
      else:
         self._repository_factory = None
         self._model_converters = None
      
      # Logging
      self.logger = logging.getLogger(__name__)
   
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
                project: Optional[str] = None,
                queue: Optional[str] = None,
                force_refresh: bool = False,
                include_historical: bool = False) -> List[PBSJob]:
      """
      Get job information
      
      Args:
         user: Filter by username (optional)
         project: Filter by project name using partial string matching (optional)
         queue: Filter by queue name using case-insensitive exact matching (optional)
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
         jobs = [job for job in jobs if job.owner == user]
      
      # Filter by project if specified (using partial string matching)
      if project:
         project_lower = project.lower()
         jobs = [job for job in jobs if job.project and project_lower in job.project.lower()]

      # Filter by queue if specified (case-insensitive exact match)
      if queue:
         queue_lower = queue.lower()
         jobs = [job for job in jobs if job.queue and job.queue.lower() == queue_lower]
      
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
            error_msg = str(e)
            if "utf-8" in error_msg.lower() and "decode" in error_msg.lower():
               self.logger.info("PBS history contains non-UTF-8 characters, using permissive encoding")
            else:
               self.logger.warning(f"Failed to get PBS completed jobs: {error_msg}")
      
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
   
   def get_nodes(self, force_refresh: bool = False, node_names: Optional[List[str]] = None) -> List[PBSNode]:
      """
      Get node information
      
      Args:
         force_refresh: Force refresh from PBS system
         node_names: Optional list of specific node names to retrieve
         
      Returns:
         List of PBSNode objects
      """
      # If specific nodes are requested, always refresh to get latest data
      should_refresh = (
         force_refresh or 
         node_names is not None or
         self._last_node_update is None or
         (datetime.now() - self._last_node_update).total_seconds() > 
         self.config.pbs.node_refresh_interval
      )
      
      if should_refresh:
         self._refresh_nodes(node_names)
      
      # If specific nodes were requested, filter cached results
      if node_names is not None:
         return [node for node in self._nodes if node.name in node_names]
      
      return self._nodes.copy()
   
   def get_reservations(self, force_refresh: bool = False, user: Optional[str] = None) -> List[PBSReservation]:
      """
      Get reservation information
      
      Args:
         force_refresh: Force refresh from PBS system
         user: Filter by username (optional)
         
      Returns:
         List of PBSReservation objects
      """
      should_refresh = (
         force_refresh or 
         self._last_reservation_update is None or
         (datetime.now() - self._last_reservation_update).total_seconds() > 
         self.config.database.job_collection_interval  # Use job collection interval as default
      )
      
      if should_refresh:
         self._refresh_reservations()
      
      reservations = self._reservations.copy()
      
      # Filter by user if specified
      if user:
         return [resv for resv in reservations if resv.owner == user]
      
      return reservations
   
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
   
   def get_jobs_by_numerical_id(self, numerical_id: str) -> List[PBSJob]:
      """
      Get jobs by numerical ID (e.g., "12345" matches "12345.pbs01", "12345.pbs02", etc.)
      
      Args:
         numerical_id: Numerical portion of job ID
         
      Returns:
         List of matching PBSJob objects
      """
      matching_jobs = []
      
      # Search in current PBS jobs
      try:
         all_jobs = self.get_jobs(force_refresh=True)
         for job in all_jobs:
            if job.job_id.startswith(f"{numerical_id}."):
               matching_jobs.append(job)
      except Exception as e:
         self.logger.error(f"Failed to search current jobs for {numerical_id}: {str(e)}")
      
      # Search in completed jobs if no matches found
      if not matching_jobs:
         try:
            completed_jobs = self.pbs_commands.qstat_completed_jobs()
            for job in completed_jobs:
               if job.job_id.startswith(f"{numerical_id}."):
                  matching_jobs.append(job)
         except Exception as e:
            self.logger.warning(f"Failed to search completed jobs for {numerical_id}: {str(e)}")
      
      # Search in database if available
      if self._database_enabled and not matching_jobs:
         try:
            job_repo = self._repository_factory.get_job_repository()
            # Get all jobs and filter by numerical ID
            all_db_jobs = job_repo.get_historical_jobs(days=365)  # Look back 1 year
            for db_job in all_db_jobs:
               if db_job.job_id.startswith(f"{numerical_id}."):
                  pbs_job = self._model_converters.job.from_database(db_job)
                  matching_jobs.append(pbs_job)
         except Exception as e:
            self.logger.warning(f"Failed to search database for {numerical_id}: {str(e)}")
      
      return matching_jobs
   
   def get_jobs_by_ids(self, job_ids: List[str]) -> List[PBSJob]:
      """
      Get multiple jobs by their full IDs
      
      Args:
         job_ids: List of full job IDs
         
      Returns:
         List of PBSJob objects (may be shorter than input if some jobs not found)
      """
      jobs = []
      
      for job_id in job_ids:
         job = self.get_job_by_id(job_id)
         if job:
            jobs.append(job)
         else:
            self.logger.warning(f"Job {job_id} not found")
      
      return jobs
   
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
      if not self._database_enabled:
         return
      
      try:
         job_repo = self._repository_factory.get_job_repository()
         latest_states = job_repo.get_latest_job_states()
         
         with self._update_lock:
            self._job_state_cache.update(latest_states)
            
         self.logger.debug(f"Populated job state cache with {len(latest_states)} entries")
      except Exception as e:
         self.logger.warning(f"Failed to populate job state cache: {str(e)}")
         # Mark as populated to avoid repeated failures
         # self._cache_populated = True # This line was removed from __init__
   
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
      if not self._database_enabled:
         return
      
      with self._update_lock:
         # Remove entries for jobs that no longer exist
         keys_to_remove = [job_id for job_id in self._job_state_cache.keys() 
                          if job_id not in current_job_ids]
         for job_id in keys_to_remove:
            del self._job_state_cache[job_id]
         
         if keys_to_remove:
            self.logger.debug(f"Cleaned up {len(keys_to_remove)} job state cache entries")
   
   def _populate_reservation_state_cache_if_needed(self) -> None:
      """Populate reservation state cache from database on first use"""
      if not self._database_enabled:
         return
      
      try:
         reservation_repo = self._repository_factory.get_reservation_repository()
         latest_states = reservation_repo.get_latest_reservation_states()
         
         with self._update_lock:
            self._reservation_state_cache.update(latest_states)
            
         self.logger.debug(f"Populated reservation state cache with {len(latest_states)} entries")
      except Exception as e:
         self.logger.warning(f"Failed to populate reservation state cache: {str(e)}")
   
   def _create_reservation_history_for_changes(self, current_reservations: List[PBSReservation], 
                                           data_collection_id: Optional[int] = None) -> List['ReservationHistory']:
      """Create reservation history entries for reservations with state changes"""
      if not self._database_enabled:
         return []
      
      # Populate cache if needed
      self._populate_reservation_state_cache_if_needed()
      
      history_entries = []
      
      with self._update_lock:
         for reservation in current_reservations:
            # Check if we have cached state for this reservation
            cached_state = self._reservation_state_cache.get(reservation.reservation_id)
            
            if cached_state is None:
               # New reservation - create history entry
               history_entry = self._model_converters.reservation.to_reservation_history(
                  reservation, data_collection_id
               )
               history_entries.append(history_entry)
               self.logger.debug(f"New reservation {reservation.reservation_id} - created history entry")
            elif cached_state.has_changes(reservation):
               # State changed - create history entry
               history_entry = self._model_converters.reservation.to_reservation_history(
                  reservation, data_collection_id
               )
               history_entries.append(history_entry)
               self.logger.debug(f"Reservation {reservation.reservation_id} state changed - created history entry")
            
            # Update cache with current state
            self._reservation_state_cache[reservation.reservation_id] = ReservationStateInfo(
               state=reservation.state,
               owner=reservation.owner,
               queue=reservation.queue,
               last_updated=datetime.now(timezone.utc)
            )
      
      return history_entries
   
   def _cleanup_reservation_state_cache(self, current_reservation_ids: Set[str]) -> None:
      """Remove cache entries for reservations that no longer exist or have reached final state"""
      if not self._database_enabled:
         return
      
      with self._update_lock:
         # Get reservations that have reached final state from database
         try:
            reservation_repo = self._repository_factory.get_reservation_repository()
            # We'll check each cached reservation to see if it's been marked as final
            keys_to_remove = []
            
            for resv_id in list(self._reservation_state_cache.keys()):
               # Remove if no longer in current PBS output
               if resv_id not in current_reservation_ids:
                  # Before removing, check if this reservation exists in DB with final state
                  try:
                     db_reservation = reservation_repo.get_reservation_by_id(resv_id)
                     if db_reservation and db_reservation.final_state_recorded:
                        # This reservation has been finalized, safe to remove from cache
                        keys_to_remove.append(resv_id)
                        self.logger.debug(f"Removing finalized reservation {resv_id} from cache")
                     elif db_reservation:
                        # Still in DB but not finalized - keep in cache for missing detection
                        self.logger.debug(f"Keeping reservation {resv_id} in cache (not finalized)")
                     else:
                        # Not in DB at all - safe to remove
                        keys_to_remove.append(resv_id)
                  except Exception as e:
                     self.logger.warning(f"Error checking final state for {resv_id}: {e}")
                     # If we can't check, remove to avoid cache bloat
                     keys_to_remove.append(resv_id)
            
            # Remove the identified keys
            for resv_id in keys_to_remove:
               del self._reservation_state_cache[resv_id]
            
            if keys_to_remove:
               self.logger.debug(f"Cleaned up {len(keys_to_remove)} reservation state cache entries")
               
         except Exception as e:
            self.logger.warning(f"Failed to check final states during cache cleanup: {e}")
            # Fallback to simple cleanup
            keys_to_remove = [resv_id for resv_id in self._reservation_state_cache.keys() 
                             if resv_id not in current_reservation_ids]
            for resv_id in keys_to_remove:
               del self._reservation_state_cache[resv_id]
            
            if keys_to_remove:
               self.logger.debug(f"Cleaned up {len(keys_to_remove)} reservation state cache entries (fallback)")

   def _snapshot_collection_due(self) -> bool:
      """Determine if it's time to collect node/queue/system snapshots."""
      interval_seconds = getattr(self.config.database, 'snapshot_interval', None)
      if not interval_seconds or interval_seconds <= 0:
         return True
      if self._last_snapshot_persist is None:
         return True
      return (datetime.now() - self._last_snapshot_persist) >= timedelta(seconds=interval_seconds)

   def _create_compact_node_snapshot(self, node_repo, data_collection_id: int) -> Optional['NodeSnapshot']:
      """Build a compact node snapshot string using the latest PBS node data."""
      if not self._database_enabled:
         return None
      node_index_map = node_repo.get_node_index_map()
      if not node_index_map:
         self.logger.debug("No node index map available; skipping node snapshot generation")
         return None
      snapshot = self._model_converters.node.to_compact_snapshot(
         self._nodes, node_index_map, data_collection_id=data_collection_id
      )
      if snapshot is None:
         self.logger.debug("Failed to build compact node snapshot; missing node index mapping?")
      return snapshot
   
   def _detect_and_handle_orphaned_jobs(self, current_pbs_job_ids: Set[str]) -> None:
      """
      Detect jobs that are active in database but missing from PBS and mark them as UNKNOWN_END
      
      Args:
         current_pbs_job_ids: Set of job IDs currently visible in PBS (current + completed)
      """
      if not self._database_enabled:
         return
      
      # Check if orphaned job detection is enabled (default: True)
      orphaned_detection_enabled = getattr(self.config.database, 'orphaned_job_detection', True)
      if not orphaned_detection_enabled:
         self.logger.debug("Orphaned job detection is disabled")
         return
      
      # Get orphaned job threshold (default: 60 minutes)
      threshold_minutes = getattr(self.config.database, 'orphaned_job_threshold_minutes', 60)
      threshold_delta = timedelta(minutes=threshold_minutes)
      
      try:
         job_repo = self._repository_factory.get_job_repository()
         
         # Get all active jobs from database
         active_db_jobs = job_repo.get_active_jobs()
         
         orphaned_job_ids = []
         current_time = datetime.now(timezone.utc)
         
         for db_job in active_db_jobs:
            # Check if job is missing from PBS
            if db_job.job_id not in current_pbs_job_ids:
               # Check if enough time has passed since last update
               time_since_update = current_time - db_job.last_updated
               if time_since_update > threshold_delta:
                  orphaned_job_ids.append(db_job.job_id)
         
         # Mark orphaned jobs as UNKNOWN_END
         if orphaned_job_ids:
            self.logger.warning(f"Found {len(orphaned_job_ids)} orphaned jobs, marking as UNKNOWN_END: {', '.join(orphaned_job_ids)}")
            
            for job_id in orphaned_job_ids:
               success = job_repo.mark_job_as_unknown_end(job_id)
               if not success:
                  self.logger.error(f"Failed to mark job {job_id} as UNKNOWN_END")
                  
            self.logger.info(f"Successfully marked {len(orphaned_job_ids)} orphaned jobs as UNKNOWN_END")
         else:
            self.logger.debug("No orphaned jobs detected")
            
      except Exception as e:
         self.logger.error(f"Failed to detect orphaned jobs: {str(e)}")
   
   def _detect_and_handle_missing_reservations(self, current_pbs_reservation_ids: Set[str]) -> None:
      """
      Detect reservations that are active in database but missing from PBS and infer final state
      
      Args:
         current_pbs_reservation_ids: Set of reservation IDs currently visible in PBS
      """
      if not self._database_enabled:
         return
      
      # Check if missing reservation detection is enabled (default: True)
      missing_detection_enabled = getattr(self.config.database, 'missing_reservation_detection', True)
      if not missing_detection_enabled:
         self.logger.debug("Missing reservation detection is disabled")
         return
      
      # Get missing reservation threshold (default: 30 minutes)
      threshold_minutes = getattr(self.config.database, 'missing_reservation_threshold_minutes', 30)
      
      try:
         reservation_repo = self._repository_factory.get_reservation_repository()
         
         # Get potentially missing reservations from database
         potentially_missing = reservation_repo.get_potentially_missing_reservations(threshold_minutes)
         
         # Filter to only those truly missing from current PBS output
         missing_reservations = [
            resv for resv in potentially_missing 
            if resv.reservation_id not in current_pbs_reservation_ids
         ]
         
         if not missing_reservations:
            self.logger.debug("No missing reservations detected")
            return
         
         self.logger.info(f"Detected {len(missing_reservations)} missing reservations")
         
         # Process each missing reservation
         for reservation in missing_reservations:
            try:
               final_state = self._infer_reservation_final_state(reservation)
               
               # Update reservation in database
               if reservation_repo.mark_reservation_final_state(reservation.reservation_id, final_state):
                  self.logger.info(f"Marked reservation {reservation.reservation_id} as {final_state.value}")
                  
                  # Create history entry for the final state transition
                  from .database.model_converters import ModelConverters
                  if not hasattr(self, '_model_converters') or self._model_converters is None:
                     self._model_converters = ModelConverters()
                  
                  # Create a PBSReservation object for history
                  from .models.reservation import PBSReservation
                  pbs_reservation = PBSReservation(
                     reservation_id=reservation.reservation_id,
                     reservation_name=reservation.reservation_name,
                     owner=reservation.owner,
                     state=final_state,
                     start_time=reservation.start_time,
                     end_time=reservation.end_time,
                     duration_seconds=reservation.duration_seconds,
                     queue=reservation.queue,
                     nodes=reservation.nodes,
                     ncpus=reservation.ncpus,
                     ngpus=reservation.ngpus,
                     walltime=reservation.walltime
                  )
                  
                  history_entry = self._model_converters.reservation.to_reservation_history(pbs_reservation)
                  reservation_repo.add_reservation_history_batch([history_entry])
                  
               else:
                  self.logger.warning(f"Failed to update missing reservation {reservation.reservation_id}")
                  
            except Exception as e:
               self.logger.error(f"Failed to handle missing reservation {reservation.reservation_id}: {str(e)}")
               
      except Exception as e:
         self.logger.error(f"Failed to detect missing reservations: {str(e)}")
   
   def _infer_reservation_final_state(self, reservation: 'Reservation') -> 'ReservationState':
      """
      Infer the final state of a missing reservation based on timing and context
      
      Args:
         reservation: Database reservation object
         
      Returns:
         Inferred final ReservationState
      """
      from .database.models import ReservationState
      
      now = datetime.now(timezone.utc)
      
      # If reservation has an end time
      if reservation.end_time:
         if now >= reservation.end_time:
            # Past the scheduled end time - likely completed normally
            return ReservationState.COMPLETED
         else:
            # Disappeared before end time - likely cancelled
            return ReservationState.CANCELLED
      
      # If no end time but has start time and duration
      elif reservation.start_time and reservation.duration_seconds:
         calculated_end = reservation.start_time + timedelta(seconds=reservation.duration_seconds)
         if now >= calculated_end:
            return ReservationState.COMPLETED
         else:
            return ReservationState.CANCELLED
      
      # For reservations without clear timing info, check if they ever started
      elif reservation.start_time and now > reservation.start_time:
         # Started but no clear end time - assume completed if missing for a while
         return ReservationState.COMPLETED
      else:
         # Never started and now missing - likely cancelled
         return ReservationState.CANCELLED
   
   def _check_for_orphaned_job_recovery(self, current_pbs_jobs: List[PBSJob]) -> None:
      """
      Check if any jobs previously marked as UNKNOWN_END now appear in PBS with final states
      
      Args:
         current_pbs_jobs: List of current PBS jobs (includes completed jobs from qstat -x)
      """
      if not self._database_enabled:
         return
      
      try:
         job_repo = self._repository_factory.get_job_repository()
         
         # Get all jobs currently marked as UNKNOWN_END
         with job_repo.get_session() as session:
            from .database.models import JobState as DBJobState, Job
            orphaned_jobs = session.query(Job).filter(Job.state == DBJobState.UNKNOWN_END).all()
            
            if not orphaned_jobs:
               return
            
            orphaned_job_ids = {job.job_id for job in orphaned_jobs}
            recovered_jobs = []
            
            # Check if any orphaned jobs now appear in PBS with final states
            for pbs_job in current_pbs_jobs:
               if (pbs_job.job_id in orphaned_job_ids and 
                   pbs_job.state.value in ['C', 'F', 'E']):  # Completed, Finished, Exiting
                  recovered_jobs.append(pbs_job)
            
            if recovered_jobs:
               self.logger.info(f"Found {len(recovered_jobs)} previously orphaned jobs that completed: {[j.job_id for j in recovered_jobs]}")
               
               # Update these jobs with their actual final states
               for pbs_job in recovered_jobs:
                  # Convert PBS job to database format and update
                  db_job_data = self._model_converters.job.to_database_dict(pbs_job)
                  job_repo.create_or_update_job(db_job_data)
                  self.logger.info(f"Updated orphaned job {pbs_job.job_id} to final state {pbs_job.state.value}")
                  
      except Exception as e:
         self.logger.error(f"Failed to check for orphaned job recovery: {str(e)}")
   
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
         
         # Check history for jobs that were active in DB but are no longer in the live PBS snapshot.
         # Query only those specific job IDs rather than the full history to avoid fetching
         # a very large JSON response that may fail to parse.
         completed_jobs = []
         history_check_succeeded = False
         try:
            current_pbs_job_ids = {job.job_id for job in self._jobs}
            job_repo = self._repository_factory.get_job_repository()
            active_db_jobs = job_repo.get_active_jobs()
            missing_job_ids = [j.job_id for j in active_db_jobs if j.job_id not in current_pbs_job_ids]
            if missing_job_ids:
               self.logger.debug(f"Checking history for {len(missing_job_ids)} jobs absent from live PBS snapshot")
               pbs_completed = self.pbs_commands.qstat_completed_jobs(job_ids=missing_job_ids)
               completed_jobs.extend(pbs_completed)
               self.logger.debug(f"Collected {len(pbs_completed)} completed jobs from PBS history")
            history_check_succeeded = True
         except Exception as e:
            error_msg = str(e)
            if "utf-8" in error_msg.lower() and "decode" in error_msg.lower():
               self.logger.info("PBS history contains non-UTF-8 characters, using permissive encoding")
            else:
               self.logger.warning(f"Failed to collect completed jobs from PBS: {error_msg}")
         
         # Combine current jobs with completed jobs for database storage
         all_jobs_for_db = self._jobs + completed_jobs
         
         should_collect_snapshots = self._snapshot_collection_due()
         queue_snapshots = []
         system_snapshot = None
         if should_collect_snapshots:
            queue_snapshots = [
               self._model_converters.queue.to_queue_snapshot(queue) for queue in self._queues
            ]
            system_snapshot = self._model_converters.system.to_system_snapshot(
               self._jobs, self._queues, self._nodes
            )
         
         db_nodes = [self._model_converters.node.to_database(node) for node in self._nodes]
         db_nodes.sort(key=lambda node: node.name)
         
         # Convert to database models - but use smart job history creation
         db_data = {
            'jobs': [self._model_converters.job.to_database(job) for job in all_jobs_for_db],
            'queues': [self._model_converters.queue.to_database(queue) for queue in self._queues],
            'nodes': db_nodes,
            'reservations': [self._model_converters.reservation.to_database(reservation) for reservation in self._reservations],
            'job_history': self._create_job_history_for_changes(all_jobs_for_db, log_id),
            'reservation_history': self._create_reservation_history_for_changes(self._reservations, log_id),
            'queue_snapshots': queue_snapshots,
            'system_snapshot': system_snapshot
         }
         
         # Clean up cache for jobs and reservations that no longer exist
         current_job_ids = {job.job_id for job in all_jobs_for_db}
         current_reservation_ids = {reservation.reservation_id for reservation in self._reservations}
         self._cleanup_job_state_cache(current_job_ids)
         self._cleanup_reservation_state_cache(current_reservation_ids)
         
         # Detect and handle orphaned jobs (active in DB but missing from PBS).
         # Skip if the history check failed: without qstat -x data the "seen" set
         # is incomplete and we risk prematurely marking jobs as UNKNOWN_END.
         if history_check_succeeded:
            self._detect_and_handle_orphaned_jobs(current_job_ids)
         else:
            self.logger.warning("Skipping orphaned job detection: qstat history check failed")
         
         # Check for recovery of previously orphaned jobs
         self._check_for_orphaned_job_recovery(all_jobs_for_db)
         
         # Detect and handle missing reservations (active in DB but missing from PBS)
         self._detect_and_handle_missing_reservations(current_reservation_ids)
         
         # Add collection log ID to remaining entries (job_history already has it)
         if should_collect_snapshots:
            for entry in db_data['queue_snapshots']:
               entry.data_collection_id = log_id
            if db_data['system_snapshot']:
               db_data['system_snapshot'].data_collection_id = log_id
         
         # Persist to database
         job_repo = self._repository_factory.get_job_repository()
         queue_repo = self._repository_factory.get_queue_repository()
         node_repo = self._repository_factory.get_node_repository()
         reservation_repo = self._repository_factory.get_reservation_repository()
         system_repo = self._repository_factory.get_system_repository()
         
         # Upsert current state
         job_repo.upsert_jobs(db_data['jobs'])
         queue_repo.upsert_queues(db_data['queues'])
         node_repo.upsert_nodes(db_data['nodes'])
         reservation_repo.upsert_reservations(db_data['reservations'])
         
         node_snapshot = None
         if should_collect_snapshots:
            node_snapshot = self._create_compact_node_snapshot(node_repo, log_id)
         
         # Add historical snapshots
         job_repo.add_job_history_batch(db_data['job_history'])
         reservation_repo.add_reservation_history_batch(db_data['reservation_history'])
         if should_collect_snapshots:
            if db_data['queue_snapshots']:
               queue_repo.add_queue_snapshots(db_data['queue_snapshots'])
            if node_snapshot is not None:
               node_repo.add_node_snapshot(node_snapshot)
            if db_data['system_snapshot'] is not None:
               system_repo.add_system_snapshot(db_data['system_snapshot'])
            self._last_snapshot_persist = datetime.now()
         
         # Log completion
         duration = (datetime.now() - collection_start).total_seconds()
         collection_repo.log_collection_complete(
            log_id, DataCollectionStatus.SUCCESS,
            jobs_collected=len(db_data['jobs']),
            queues_collected=len(db_data['queues']),
            nodes_collected=len(db_data['nodes']),
            reservations_collected=len(db_data['reservations']),
            duration=duration
         )
         
         # Log successful completion with summary
         self.logger.info(f"Completed {collection_type} data collection successfully: "
                         f"{len(db_data['jobs'])} jobs, {len(db_data['queues'])} queues, "
                         f"{len(db_data['nodes'])} nodes, {len(db_data['reservations'])} reservations "
                         f"in {duration:.1f}s")
         
         return {
            'status': 'success',
            'jobs_collected': len(db_data['jobs']),
            'completed_jobs_collected': len(completed_jobs),
            'queues_collected': len(db_data['queues']),
            'nodes_collected': len(db_data['nodes']),
            'reservations_collected': len(db_data['reservations']),
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
   
   def _refresh_nodes(self, node_names: Optional[List[str]] = None) -> None:
      """Refresh node data from PBS system"""
      try:
         with self._update_lock:
            if node_names:
               self.logger.debug(f"Refreshing specific node data: {node_names}")
               # For specific nodes, get fresh data and merge with existing cache
               specific_nodes = self.pbs_commands.pbsnodes(node_names)
               specific_node_names = {node.name for node in specific_nodes}
               
               # Keep existing nodes that weren't requested, replace requested ones
               existing_nodes = [node for node in self._nodes if node.name not in specific_node_names]
               self._nodes = existing_nodes + specific_nodes
            else:
               self.logger.debug("Refreshing all node data")
               self._nodes = self.pbs_commands.pbsnodes()
            
            self._last_node_update = datetime.now()
            self.logger.debug(f"Updated {len(self._nodes)} nodes")
      except PBSCommandError as e:
         self.logger.error(f"Failed to refresh nodes: {str(e)}")
   
   def _refresh_reservations(self) -> None:
      """Refresh reservation data from PBS system"""
      try:
         with self._update_lock:
            self.logger.debug("Refreshing reservation data")
            self._reservations = self.pbs_commands.pbs_rstat_all_detailed()
            self._last_reservation_update = datetime.now()
            self.logger.debug(f"Updated {len(self._reservations)} reservations")
      except PBSCommandError as e:
         self.logger.error(f"Failed to refresh reservations: {str(e)}")
   
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
      self._refresh_reservations()
   
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
