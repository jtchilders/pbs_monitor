"""
PBS Node data structure
"""

from typing import Dict, List, Optional, Any
from dataclasses import dataclass, field
from enum import Enum


class NodeState(Enum):
   """PBS node states"""
   FREE = "free"
   OFFLINE = "offline"
   DOWN = "down"
   BUSY = "busy"
   JOB_EXCLUSIVE = "job-exclusive"
   JOB_SHARING = "job-sharing"
   RESERVE = "reserve"
   UNKNOWN = "unknown"


@dataclass
class PBSNode:
   """Represents a PBS compute node"""
   
   name: str
   state: NodeState
   
   # Hardware specifications
   ncpus: int = 0
   memory: Optional[str] = None
   
   # Current usage
   jobs: List[str] = field(default_factory=list)
   
   # Node properties
   properties: List[str] = field(default_factory=list)
   
   # Load and performance
   loadavg: Optional[float] = None
   
   # Raw PBS attributes
   raw_attributes: Dict[str, Any] = field(default_factory=dict)
   
   @classmethod
   def from_pbsnodes_json(cls, node_data: Dict[str, Any]) -> 'PBSNode':
      """Create PBSNode from pbsnodes JSON output"""
      name = node_data.get('name', '')
      
      # Parse node state
      state_str = node_data.get('state', 'unknown')
      try:
         state = NodeState(state_str)
      except ValueError:
         state = NodeState.UNKNOWN
      
      # Parse hardware specifications
      ncpus = cls._parse_int(node_data.get('ncpus', '0'), default=0)
      memory = node_data.get('memory')
      
      # Parse current jobs
      jobs = []
      jobs_data = node_data.get('jobs', [])
      if isinstance(jobs_data, list):
         jobs = jobs_data
      elif isinstance(jobs_data, str):
         jobs = [jobs_data]
      
      # Parse node properties
      properties = []
      prop_data = node_data.get('properties', [])
      if isinstance(prop_data, list):
         properties = prop_data
      elif isinstance(prop_data, str):
         properties = [prop_data]
      
      # Parse load average
      loadavg = cls._parse_float(node_data.get('loadavg'))
      
      return cls(
         name=name,
         state=state,
         ncpus=ncpus,
         memory=memory,
         jobs=jobs,
         properties=properties,
         loadavg=loadavg,
         raw_attributes=node_data
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
   def _parse_float(value: Optional[str]) -> Optional[float]:
      """Parse float value from string"""
      if value is None:
         return None
      
      try:
         return float(value)
      except (ValueError, TypeError):
         return None
   
   def is_available(self) -> bool:
      """Check if node is available for jobs"""
      return self.state in [NodeState.FREE, NodeState.JOB_SHARING]
   
   def is_occupied(self) -> bool:
      """Check if node is currently running jobs"""
      return len(self.jobs) > 0
   
   def cpu_utilization(self) -> Optional[float]:
      """Calculate CPU utilization percentage based on running jobs"""
      if self.ncpus == 0:
         return None
      
      return (len(self.jobs) / self.ncpus) * 100.0
   
   def available_cpus(self) -> int:
      """Calculate available CPUs"""
      return max(0, self.ncpus - len(self.jobs))
   
   def has_property(self, property_name: str) -> bool:
      """Check if node has specific property"""
      return property_name in self.properties
   
   def memory_gb(self) -> Optional[float]:
      """Parse memory to GB if available"""
      if not self.memory:
         return None
      
      try:
         # Handle formats like "32gb", "32768mb", "33554432kb"
         memory_str = self.memory.lower()
         if memory_str.endswith('gb'):
            return float(memory_str[:-2])
         elif memory_str.endswith('mb'):
            return float(memory_str[:-2]) / 1024.0
         elif memory_str.endswith('kb'):
            return float(memory_str[:-2]) / (1024.0 * 1024.0)
         else:
            # Assume bytes
            return float(memory_str) / (1024.0 * 1024.0 * 1024.0)
      except (ValueError, TypeError):
         return None
   
   def load_percentage(self) -> Optional[float]:
      """Calculate load percentage based on ncpus"""
      if self.loadavg is None or self.ncpus == 0:
         return None
      
      return (self.loadavg / self.ncpus) * 100.0
   
   def __str__(self) -> str:
      job_count = len(self.jobs)
      return (f"Node {self.name}: {self.state.value}, "
              f"{job_count}/{self.ncpus} CPUs, "
              f"{self.memory or 'N/A'} memory") 