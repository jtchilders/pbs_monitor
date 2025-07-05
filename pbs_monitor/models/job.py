"""
PBS Job data structure
"""

from datetime import datetime
from typing import Dict, List, Optional, Any
from dataclasses import dataclass, field
from enum import Enum


class JobState(Enum):
   """PBS job states"""
   QUEUED = "Q"
   RUNNING = "R"
   HELD = "H"
   WAITING = "W"
   TRANSITIONING = "T"
   EXITING = "E"
   SUSPENDED = "S"
   COMPLETED = "C"
   FINISHED = "F"


@dataclass
class PBSJob:
   """Represents a PBS job"""
   
   job_id: str
   job_name: str
   owner: str
   state: JobState
   queue: str
   
   # Resource requirements
   nodes: int = 1
   ppn: int = 1
   walltime: Optional[str] = None
   memory: Optional[str] = None
   
   # Timing information
   submit_time: Optional[datetime] = None
   start_time: Optional[datetime] = None
   end_time: Optional[datetime] = None
   
   # Additional attributes
   priority: int = 0
   execution_node: Optional[str] = None
   exit_status: Optional[int] = None
   
   # Raw PBS attributes
   raw_attributes: Dict[str, Any] = field(default_factory=dict)
   
   @classmethod
   def from_qstat_json(cls, job_data: Dict[str, Any]) -> 'PBSJob':
      """Create PBSJob from qstat JSON output"""
      job_id = job_data.get('Job_Id', '')
      job_name = job_data.get('Job_Name', '')
      owner = job_data.get('Job_Owner', '').split('@')[0]  # Remove @hostname
      
      # Parse job state
      state_str = job_data.get('job_state', 'Q')
      try:
         state = JobState(state_str)
      except ValueError:
         state = JobState.QUEUED
      
      queue = job_data.get('queue', '')
      
      # Parse resource requirements
      resources = job_data.get('Resource_List', {})
      nodes = int(resources.get('nodes', '1'))
      ppn = int(resources.get('ppn', '1'))
      walltime = resources.get('walltime')
      memory = resources.get('mem')
      
      # Parse timing
      submit_time = cls._parse_pbs_time(job_data.get('qtime'))
      start_time = cls._parse_pbs_time(job_data.get('start_time'))
      end_time = cls._parse_pbs_time(job_data.get('comp_time'))
      
      # Additional attributes
      priority = int(job_data.get('Priority', '0'))
      execution_node = job_data.get('exec_host')
      exit_status = job_data.get('exit_status')
      
      return cls(
         job_id=job_id,
         job_name=job_name,
         owner=owner,
         state=state,
         queue=queue,
         nodes=nodes,
         ppn=ppn,
         walltime=walltime,
         memory=memory,
         submit_time=submit_time,
         start_time=start_time,
         end_time=end_time,
         priority=priority,
         execution_node=execution_node,
         exit_status=exit_status,
         raw_attributes=job_data
      )
   
   @staticmethod
   def _parse_pbs_time(time_str: Optional[str]) -> Optional[datetime]:
      """Parse PBS timestamp format"""
      if not time_str:
         return None
      
      try:
         # PBS typically uses format like "Thu Oct 12 14:30:00 2023"
         return datetime.strptime(time_str, "%a %b %d %H:%M:%S %Y")
      except (ValueError, TypeError):
         return None
   
   def is_active(self) -> bool:
      """Check if job is currently active (running or queued)"""
      return self.state in [JobState.QUEUED, JobState.RUNNING, JobState.HELD]
   
   def estimated_total_cores(self) -> int:
      """Calculate total cores requested"""
      return self.nodes * self.ppn
   
   def runtime_duration(self) -> Optional[str]:
      """Calculate runtime duration if job has started"""
      if not self.start_time:
         return None
      
      end = self.end_time or datetime.now()
      duration = end - self.start_time
      
      total_seconds = int(duration.total_seconds())
      hours = total_seconds // 3600
      minutes = (total_seconds % 3600) // 60
      seconds = total_seconds % 60
      
      return f"{hours:02d}:{minutes:02d}:{seconds:02d}"
   
   def __str__(self) -> str:
      return f"Job {self.job_id}: {self.job_name} ({self.state.value}) - {self.owner}" 