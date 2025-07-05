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


class DataCollector:
   """Collects and manages PBS system data"""
   
   def __init__(self, config: Optional[Config] = None, use_sample_data: bool = False):
      """
      Initialize data collector
      
      Args:
         config: Configuration object (optional)
         use_sample_data: Use sample JSON data instead of actual PBS commands
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
      
      # Cache timestamps
      self._last_job_update: Optional[datetime] = None
      self._last_queue_update: Optional[datetime] = None
      self._last_node_update: Optional[datetime] = None
      
      # Threading for background updates
      self._update_lock = threading.Lock()
      self._background_update_thread: Optional[threading.Thread] = None
      self._stop_background_updates = False
   
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
   
   def get_jobs(self, 
                user: Optional[str] = None,
                force_refresh: bool = False) -> List[PBSJob]:
      """
      Get job information
      
      Args:
         user: Filter by username (optional)
         force_refresh: Force refresh from PBS system
         
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
      
      # Filter by user if specified
      if user:
         return [job for job in self._jobs if job.owner == user]
      
      return self._jobs.copy()
   
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
      try:
         jobs = self.pbs_commands.qstat_jobs(job_id=job_id)
         return jobs[0] if jobs else None
      except PBSCommandError as e:
         self.logger.error(f"Failed to get job {job_id}: {str(e)}")
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
         'resources': resource_stats
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
   
   def get_queue_utilization(self) -> Dict[str, float]:
      """
      Get utilization percentage for each queue
      
      Returns:
         Dictionary mapping queue names to utilization percentages
      """
      queues = self.get_queues()
      return {queue.name: queue.utilization_percentage() for queue in queues}
   
   def _refresh_jobs(self) -> None:
      """Refresh job data from PBS system"""
      try:
         with self._update_lock:
            self.logger.debug("Refreshing job data")
            self._jobs = self.pbs_commands.qstat_jobs()
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
   
   def refresh_all(self) -> None:
      """Refresh all data from PBS system"""
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
            
            # Sleep for a short interval
            time.sleep(10)
            
         except Exception as e:
            self.logger.error(f"Error in background update loop: {str(e)}")
            time.sleep(30)  # Wait longer on error
   
   def __del__(self):
      """Cleanup on destruction"""
      self.stop_background_updates() 