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
   
   # Project and allocation information
   project: Optional[str] = None
   allocation_type: Optional[str] = None
   
   # Calculated fields
   total_cores: Optional[int] = None
   actual_runtime_seconds: Optional[int] = None
   queue_time_seconds: Optional[int] = None
   
   # Job score (calculated using server formula)
   score: Optional[float] = None
   
   # Raw PBS attributes
   raw_attributes: Dict[str, Any] = field(default_factory=dict)
   
   @classmethod
   def from_qstat_json(cls, job_data: Dict[str, Any], score: Optional[float] = None) -> 'PBSJob':
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
      
      # Handle nodes - prefer nodect; fallback to nodes; finally try select parsing
      nodes_val = resources.get('nodect')
      nodes = 1
      if nodes_val is None:
         nodes_val = resources.get('nodes')
      if nodes_val is not None:
         try:
            # Accept plain integers or strings like "2" or "2:ppn=4"
            nodes_str = str(nodes_val)
            if ':' in nodes_str:
               nodes_str = nodes_str.split(':', 1)[0]
            nodes = int(nodes_str)
         except (ValueError, TypeError):
            nodes = 1
      else:
         # Try parsing PBS select format if present
         sel = resources.get('select')
         if sel:
            try:
               total = 0
               sel_str = str(sel)
               if sel_str.isdigit():
                  total = int(sel_str)
               else:
                  for chunk in sel_str.split('+'):
                     part = chunk.strip()
                     if not part:
                        continue
                     count_str = part.split(':', 1)[0]
                     total += int(count_str)
               if total > 0:
                  nodes = total
            except Exception:
               nodes = 1
      
      ppn = int(resources.get('ppn', '1'))
      walltime = resources.get('walltime')
      memory = resources.get('mem')
      
      # Parse timing - handle different field names for completed vs running jobs
      submit_time = cls._parse_pbs_time(job_data.get('qtime'))
      # For start time: try 'stime' first (for completed jobs), then 'start_time'
      start_time = cls._parse_pbs_time(job_data.get('stime') or job_data.get('start_time'))
      # For end time: try 'obittime' first (for completed jobs), then 'comp_time'
      end_time = cls._parse_pbs_time(job_data.get('obittime') or job_data.get('comp_time'))
      
      # Additional attributes
      priority = int(job_data.get('Priority', '0'))
      execution_node = job_data.get('exec_host')
      # For exit status: try 'Exit_status' first (capital E), then 'exit_status'
      exit_status = job_data.get('Exit_status') or job_data.get('exit_status')
      if exit_status is not None:
         try:
            exit_status = int(exit_status)
         except (ValueError, TypeError):
            exit_status = None
      
      # Extract project and allocation type
      project = job_data.get('Account_Name') or job_data.get('project')
      allocation_type = None
      if resources:
         allocation_type = resources.get('award_category')
      
      # Calculate derived fields
      total_cores = nodes * ppn
      
      # Calculate actual runtime (for completed jobs)
      actual_runtime_seconds = None
      if start_time and end_time:
         duration = end_time - start_time
         actual_runtime_seconds = int(duration.total_seconds())
      
      # Calculate queue time
      queue_time_seconds = None
      if submit_time and start_time:
         duration = start_time - submit_time
         queue_time_seconds = int(duration.total_seconds())
      
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
         project=project,
         allocation_type=allocation_type,
         total_cores=total_cores,
         actual_runtime_seconds=actual_runtime_seconds,
         queue_time_seconds=queue_time_seconds,
         score=score,
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
      # Only show runtime for jobs that actually ran (have start time and finished)
      if self.state not in [JobState.FINISHED, JobState.COMPLETED] and not self.start_time:
         return None
      
      # For completed jobs, try to use actual walltime from resources_used first
      if self.state in [JobState.FINISHED, JobState.COMPLETED] and self.raw_attributes:
         resources_used = self.raw_attributes.get('resources_used', {})
         actual_walltime = resources_used.get('walltime')
         if actual_walltime:
            return actual_walltime
      
      # Fall back to calculating from timestamps (for running jobs or if walltime not available)
      if not self.start_time:
         return None
      
      end = self.end_time or datetime.now()
      duration = end - self.start_time
      
      total_seconds = int(duration.total_seconds())
      hours = total_seconds // 3600
      minutes = (total_seconds % 3600) // 60
      seconds = total_seconds % 60
      
      return f"{hours:02d}:{minutes:02d}:{seconds:02d}"

   def queue_duration(self) -> Optional[str]:
      """Calculate how long the job was queued before starting"""
      if not self.submit_time or not self.start_time:
         return None
      
      duration = self.start_time - self.submit_time
      total_seconds = int(duration.total_seconds())
      
      # Handle negative durations (shouldn't happen but just in case)
      if total_seconds < 0:
         return None
      
      hours = total_seconds // 3600
      minutes = (total_seconds % 3600) // 60
      seconds = total_seconds % 60
      
      return f"{hours:02d}:{minutes:02d}:{seconds:02d}"
   
   def format_score(self) -> str:
      """Format the job score for display"""
      if self.score is None:
         return "N/A"
      return f"{self.score:.2f}"
   
   def __str__(self) -> str:
      return f"Job {self.job_id}: {self.job_name} ({self.state.value}) - {self.owner}" 