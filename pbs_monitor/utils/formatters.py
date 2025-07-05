"""
Formatting utilities for PBS Monitor
"""

from datetime import datetime, timedelta
from typing import Optional, Union, Any
import re


def format_duration(seconds: Union[int, float, str, None]) -> str:
   """
   Format duration in seconds to human-readable string
   
   Args:
      seconds: Duration in seconds
      
   Returns:
      Formatted duration string (e.g., "1h 30m", "45m 30s", "2d 3h")
   """
   if seconds is None:
      return "N/A"
   
   try:
      if isinstance(seconds, str):
         # Try to parse walltime format like "01:30:00"
         if ':' in seconds:
            return _format_walltime(seconds)
         else:
            seconds = float(seconds)
      
      seconds = int(seconds)
      
      if seconds < 0:
         return "N/A"
      
      # Calculate time units
      days = seconds // 86400
      hours = (seconds % 86400) // 3600
      minutes = (seconds % 3600) // 60
      secs = seconds % 60
      
      # Format based on magnitude
      if days > 0:
         if hours > 0:
            return f"{days}d {hours}h"
         else:
            return f"{days}d"
      elif hours > 0:
         if minutes > 0:
            return f"{hours}h {minutes}m"
         else:
            return f"{hours}h"
      elif minutes > 0:
         if secs > 0:
            return f"{minutes}m {secs}s"
         else:
            return f"{minutes}m"
      else:
         return f"{secs}s"
      
   except (ValueError, TypeError):
      return "N/A"


def _format_walltime(walltime: str) -> str:
   """
   Format PBS walltime string (HH:MM:SS) to human-readable format
   
   Args:
      walltime: Walltime string in format HH:MM:SS
      
   Returns:
      Formatted walltime string
   """
   try:
      # Parse walltime format
      parts = walltime.split(':')
      if len(parts) != 3:
         return walltime
      
      hours = int(parts[0])
      minutes = int(parts[1])
      seconds = int(parts[2])
      
      # Convert to total seconds and format
      total_seconds = hours * 3600 + minutes * 60 + seconds
      return format_duration(total_seconds)
      
   except (ValueError, TypeError, IndexError):
      return walltime


def format_timestamp(
   timestamp: Optional[datetime],
   format_str: str = "%d-%m %H:%M"
) -> str:
   """
   Format timestamp to string
   
   Args:
      timestamp: Datetime object to format
      format_str: Format string (default: DD-MM HH:MM)
      
   Returns:
      Formatted timestamp string
   """
   if timestamp is None:
      return "N/A"
   
   try:
      return timestamp.strftime(format_str)
   except (ValueError, TypeError):
      return "N/A"


def format_memory(memory: Optional[str]) -> str:
   """
   Format memory specification to human-readable string
   
   Args:
      memory: Memory specification (e.g., "32gb", "1024mb")
      
   Returns:
      Formatted memory string
   """
   if not memory:
      return "N/A"
   
   try:
      memory_str = memory.lower()
      
      # Extract number and unit
      match = re.match(r'^(\d+(?:\.\d+)?)\s*([a-z]*)', memory_str)
      if not match:
         return memory
      
      value = float(match.group(1))
      unit = match.group(2)
      
      # Convert to appropriate unit
      if unit in ['kb', 'k']:
         if value >= 1024 * 1024:
            return f"{value / (1024 * 1024):.1f}GB"
         elif value >= 1024:
            return f"{value / 1024:.1f}MB"
         else:
            return f"{value:.0f}KB"
      elif unit in ['mb', 'm']:
         if value >= 1024:
            return f"{value / 1024:.1f}GB"
         else:
            return f"{value:.0f}MB"
      elif unit in ['gb', 'g']:
         return f"{value:.1f}GB"
      elif unit in ['tb', 't']:
         return f"{value:.1f}TB"
      else:
         # Assume bytes
         if value >= 1024 * 1024 * 1024:
            return f"{value / (1024 * 1024 * 1024):.1f}GB"
         elif value >= 1024 * 1024:
            return f"{value / (1024 * 1024):.1f}MB"
         elif value >= 1024:
            return f"{value / 1024:.1f}KB"
         else:
            return f"{value:.0f}B"
            
   except (ValueError, TypeError):
      return memory


def format_percentage(value: Optional[float], decimal_places: int = 1) -> str:
   """
   Format percentage value
   
   Args:
      value: Percentage value
      decimal_places: Number of decimal places
      
   Returns:
      Formatted percentage string
   """
   if value is None:
      return "N/A"
   
   try:
      return f"{value:.{decimal_places}f}%"
   except (ValueError, TypeError):
      return "N/A"


def format_number(value: Optional[Union[int, float]], 
                 decimal_places: int = 0) -> str:
   """
   Format numeric value
   
   Args:
      value: Numeric value
      decimal_places: Number of decimal places
      
   Returns:
      Formatted number string
   """
   if value is None:
      return "N/A"
   
   try:
      if decimal_places == 0:
         return f"{int(value)}"
      else:
         return f"{value:.{decimal_places}f}"
   except (ValueError, TypeError):
      return "N/A"


def truncate_string(text: str, max_length: int, suffix: str = "...") -> str:
   """
   Truncate string to maximum length
   
   Args:
      text: String to truncate
      max_length: Maximum length
      suffix: Suffix to add if truncated
      
   Returns:
      Truncated string
   """
   if len(text) <= max_length:
      return text
   
   return text[:max_length - len(suffix)] + suffix


def format_job_id(job_id: str) -> str:
   """
   Format job ID for display
   
   Args:
      job_id: Full job ID
      
   Returns:
      Formatted job ID
   """
   if not job_id:
      return "N/A"
   
   # Extract just the numeric part if it's a full PBS job ID
   if '.' in job_id:
      return job_id.split('.')[0]
   
   return job_id


def format_node_list(nodes: list, max_display: int = 3) -> str:
   """
   Format list of nodes for display
   
   Args:
      nodes: List of node names
      max_display: Maximum nodes to display
      
   Returns:
      Formatted node list string
   """
   if not nodes:
      return "N/A"
   
   if len(nodes) <= max_display:
      return ", ".join(nodes)
   
   displayed = nodes[:max_display]
   remaining = len(nodes) - max_display
   
   return f"{', '.join(displayed)} (+{remaining} more)"


def format_state(state: str) -> str:
   """
   Format state for display with colors/symbols
   
   Args:
      state: State string
      
   Returns:
      Formatted state string
   """
   state_map = {
      'R': 'Running',
      'Q': 'Queued',
      'H': 'Held',
      'W': 'Waiting',
      'T': 'Transitioning',
      'E': 'Exiting',
      'S': 'Suspended',
      'C': 'Completed',
      'F': 'Finished',
      'free': 'Free',
      'offline': 'Offline',
      'down': 'Down',
      'busy': 'Busy',
      'job-exclusive': 'Job-Exclusive',
      'job-sharing': 'Job-Sharing'
   }
   
   return state_map.get(state, state.capitalize()) 