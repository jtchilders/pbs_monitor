"""
PBS Queue data structure
"""

from typing import Dict, List, Optional, Any
from dataclasses import dataclass, field
from enum import Enum
import re


class QueueState(Enum):
   """PBS queue operational states"""
   ENABLED_STARTED = "enabled_started"
   ENABLED_STOPPED = "enabled_stopped"
   DISABLED = "disabled"


@dataclass
class PBSQueue:
   """Represents a PBS queue"""
   
   name: str
   state: QueueState
   queue_type: str = "execution"
   
   # Limits
   max_running: Optional[int] = None
   max_queued: Optional[int] = None
   max_user_run: Optional[int] = None
   max_user_queued: Optional[int] = None
   
   # Current job statistics (parsed from state_count)
   total_jobs: int = 0
   transit_jobs: int = 0
   queued_jobs: int = 0
   held_jobs: int = 0
   waiting_jobs: int = 0
   running_jobs: int = 0
   exiting_jobs: int = 0
   begun_jobs: int = 0
   
   # Resource limits
   max_walltime: Optional[str] = None
   max_nodes: Optional[int] = None
   max_ppn: Optional[int] = None
   
   # Priority and scheduling
   priority: int = 0
   
   # Raw PBS attributes
   raw_attributes: Dict[str, Any] = field(default_factory=dict)
   
   @classmethod
   def from_qstat_json(cls, queue_data: Dict[str, Any]) -> 'PBSQueue':
      """Create PBSQueue from qstat JSON output"""
      name = queue_data.get('Queue', '')
      
      # Parse queue state from enabled/started fields
      enabled = queue_data.get('enabled', 'True').lower() == 'true'
      started = queue_data.get('started', 'True').lower() == 'true'
      
      if not enabled:
         state = QueueState.DISABLED
      elif enabled and started:
         state = QueueState.ENABLED_STARTED
      else:
         state = QueueState.ENABLED_STOPPED
      
      queue_type = queue_data.get('queue_type', 'execution')
      
      # Parse limits
      max_running = cls._parse_int(queue_data.get('max_running'))
      max_queued = cls._parse_int(queue_data.get('max_queued'))
      max_user_run = cls._parse_int(queue_data.get('max_user_run'))
      max_user_queued = cls._parse_int(queue_data.get('max_user_queued'))
      
      # Parse job statistics from state_count
      total_jobs = cls._parse_int(queue_data.get('total_jobs', '0'), default=0)
      job_counts = cls._parse_state_count(queue_data.get('state_count', ''))
      
      # Parse resource limits
      max_walltime = queue_data.get('max_walltime')
      max_nodes = cls._parse_int(queue_data.get('max_nodes'))
      max_ppn = cls._parse_int(queue_data.get('max_ppn'))
      
      # Priority
      priority = cls._parse_int(queue_data.get('priority', '0'), default=0)
      
      return cls(
         name=name,
         state=state,
         queue_type=queue_type,
         max_running=max_running,
         max_queued=max_queued,
         max_user_run=max_user_run,
         max_user_queued=max_user_queued,
         total_jobs=total_jobs,
         transit_jobs=job_counts.get('transit', 0),
         queued_jobs=job_counts.get('queued', 0),
         held_jobs=job_counts.get('held', 0),
         waiting_jobs=job_counts.get('waiting', 0),
         running_jobs=job_counts.get('running', 0),
         exiting_jobs=job_counts.get('exiting', 0),
         begun_jobs=job_counts.get('begun', 0),
         max_walltime=max_walltime,
         max_nodes=max_nodes,
         max_ppn=max_ppn,
         priority=priority,
         raw_attributes=queue_data
      )
   
   @staticmethod
   def _parse_int(value: Optional[str], default: Optional[int] = None) -> Optional[int]:
      """Parse integer value from string"""
      if value is None:
         return default
      
      try:
         return int(value)
      except (ValueError, TypeError):
         return default
   
   @staticmethod
   def _parse_state_count(state_count: str) -> Dict[str, int]:
      """
      Parse state_count string into individual job state counts
      
      Args:
         state_count: String like "Transit:0 Queued:2 Held:0 Waiting:0 Running:1 Exiting:0 Begun:0 "
         
      Returns:
         Dictionary with job state counts
      """
      job_counts = {}
      
      if not state_count:
         return job_counts
      
      # Parse each state:count pair
      # Format: "Transit:0 Queued:2 Held:0 Waiting:0 Running:1 Exiting:0 Begun:0 "
      pairs = state_count.strip().split()
      for pair in pairs:
         if ':' in pair:
            try:
               state, count_str = pair.split(':', 1)
               count = int(count_str)
               job_counts[state.lower()] = count
            except (ValueError, TypeError):
               continue
      
      return job_counts
   
   def is_enabled(self) -> bool:
      """Check if queue is enabled"""
      return self.state in [QueueState.ENABLED_STARTED, QueueState.ENABLED_STOPPED]
   
   def is_started(self) -> bool:
      """Check if queue is started"""
      return self.state == QueueState.ENABLED_STARTED
   
   def status_description(self) -> str:
      """Get human-readable status description"""
      if self.state == QueueState.ENABLED_STARTED:
         return "Enabled"
      elif self.state == QueueState.ENABLED_STOPPED:
         return "Enabled/Stopped"
      else:
         return "Disabled"
   
   def utilization_percentage(self) -> float:
      """Calculate current utilization percentage"""
      if not self.max_running or self.max_running == 0:
         return 0.0
      
      return (self.running_jobs / self.max_running) * 100.0
   
   def can_accept_jobs(self) -> bool:
      """Check if queue can accept new jobs"""
      return (self.is_enabled() and self.is_started() and 
              (self.max_queued is None or self.queued_jobs < self.max_queued))
   
   def available_slots(self) -> Optional[int]:
      """Calculate available running slots"""
      if self.max_running is None:
         return None
      
      return max(0, self.max_running - self.running_jobs)
   
   def __str__(self) -> str:
      return (f"Queue {self.name}: {self.running_jobs}/{self.max_running or '∞'} running, "
              f"{self.queued_jobs} queued, {self.held_jobs} held ({self.status_description()})") 