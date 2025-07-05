"""
PBS Queue data structure
"""

from typing import Dict, List, Optional, Any
from dataclasses import dataclass, field
from enum import Enum


class QueueState(Enum):
   """PBS queue states"""
   ENABLED = "E"
   DISABLED = "D"


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
   
   # Current statistics
   total_jobs: int = 0
   running_jobs: int = 0
   queued_jobs: int = 0
   
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
      
      # Parse queue state
      state_str = queue_data.get('state_count', 'E')
      try:
         state = QueueState(state_str)
      except ValueError:
         state = QueueState.ENABLED
      
      queue_type = queue_data.get('queue_type', 'execution')
      
      # Parse limits
      max_running = cls._parse_int(queue_data.get('max_running'))
      max_queued = cls._parse_int(queue_data.get('max_queued'))
      max_user_run = cls._parse_int(queue_data.get('max_user_run'))
      max_user_queued = cls._parse_int(queue_data.get('max_user_queued'))
      
      # Parse current statistics
      total_jobs = cls._parse_int(queue_data.get('total_jobs', '0'), default=0)
      running_jobs = cls._parse_int(queue_data.get('running_jobs', '0'), default=0)
      queued_jobs = cls._parse_int(queue_data.get('queued_jobs', '0'), default=0)
      
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
         running_jobs=running_jobs,
         queued_jobs=queued_jobs,
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
   
   def is_enabled(self) -> bool:
      """Check if queue is enabled"""
      return self.state == QueueState.ENABLED
   
   def utilization_percentage(self) -> float:
      """Calculate current utilization percentage"""
      if not self.max_running or self.max_running == 0:
         return 0.0
      
      return (self.running_jobs / self.max_running) * 100.0
   
   def can_accept_jobs(self) -> bool:
      """Check if queue can accept new jobs"""
      return (self.is_enabled() and 
              (self.max_queued is None or self.queued_jobs < self.max_queued))
   
   def available_slots(self) -> Optional[int]:
      """Calculate available running slots"""
      if self.max_running is None:
         return None
      
      return max(0, self.max_running - self.running_jobs)
   
   def __str__(self) -> str:
      return (f"Queue {self.name}: {self.running_jobs}/{self.max_running or '∞'} running, "
              f"{self.queued_jobs} queued ({self.state.value})") 