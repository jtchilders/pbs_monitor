"""
Command implementations for PBS Monitor CLI
"""

import argparse
import logging
from typing import List, Dict, Any, Optional
from datetime import datetime
from abc import ABC, abstractmethod

from tabulate import tabulate
from rich.console import Console
from rich.table import Table
from rich.text import Text

from ..data_collector import DataCollector
from ..config import Config
from ..models.job import PBSJob, JobState
from ..models.queue import PBSQueue
from ..models.node import PBSNode, NodeState
from ..models.reservation import PBSReservation, ReservationState
from ..database.migrations import (
    initialize_database, migrate_database, validate_database,
    backup_database, restore_database, clean_old_data, get_database_info
)
from ..utils.formatters import (
   format_duration, format_timestamp, format_memory,
   format_percentage, format_number, format_job_id, format_state
)

import os
import signal
import sys
import time
import socket
import json
import getpass
from pathlib import Path

import pandas as pd


class BaseCommand(ABC):
   """Base class for CLI commands"""
   
   def __init__(self, collector: DataCollector, config: Config):
      self.collector = collector
      self.config = config
      self.logger = logging.getLogger(__name__)
      
      # Initialize console for rich output with better width handling
      console_width = None
      if config.display.auto_width:
         # Let Rich auto-detect terminal width
         console_width = None
      else:
         console_width = config.display.max_table_width
      
      self.console = Console(
         width=console_width,
         force_terminal=True if config.display.use_colors else False
      )
   
   @abstractmethod
   def execute(self, args: argparse.Namespace) -> int:
      """Execute the command"""
      pass
   
   def _create_table(self, title: str, headers: List[str], rows: List[List[str]]) -> Table:
      """Create a rich table with intelligent column sizing"""
      # Calculate optimal column widths
      column_widths = self._calculate_column_widths(headers, rows)
      
      # Create table with better sizing options
      table = Table(
         title=title, 
         show_header=True, 
         header_style="bold magenta",
         expand=self.config.display.expand_columns,
         width=None if self.config.display.auto_width else self.config.display.max_table_width
      )
      
      # Add columns with calculated widths
      for i, header in enumerate(headers):
         width = column_widths[i] if i < len(column_widths) else None
         table.add_column(
            header, 
            style="cyan",
            width=width,
            min_width=self.config.display.min_column_width,
            max_width=self.config.display.max_column_width,
            no_wrap=not self.config.display.word_wrap
         )
      
      for row in rows:
         table.add_row(*row)
      
      return table
   
   def _calculate_column_widths(self, headers: List[str], rows: List[List[str]]) -> List[int]:
      """Calculate optimal column widths based on content"""
      if not rows:
         return [len(header) + 2 for header in headers]
      
      column_widths = []
      for i, header in enumerate(headers):
         # Start with header length
         max_width = len(header)
         
         # Check content in this column
         for row in rows:
            if i < len(row) and row[i]:
               content_width = len(str(row[i]))
               max_width = max(max_width, content_width)
         
         # Apply constraints
         optimal_width = min(
            max(max_width + 2, self.config.display.min_column_width),
            self.config.display.max_column_width
         )
         
         column_widths.append(optimal_width)
      
      return column_widths
   
   def _print_table(self, title: str, headers: List[str], rows: List[List[str]]) -> None:
      """Print a formatted table with better width handling"""
      if self.config.display.use_colors:
         table = self._create_table(title, headers, rows)
         self.console.print(table)
      else:
         print(f"\n{title}")
         
         # For non-colored output, optionally truncate wide columns
         if not self.config.display.expand_columns:
            truncated_rows = []
            for row in rows:
               truncated_row = []
               for i, cell in enumerate(row):
                  if len(str(cell)) > self.config.display.max_column_width:
                     truncated_cell = str(cell)[:self.config.display.max_column_width-3] + "..."
                  else:
                     truncated_cell = str(cell)
                  truncated_row.append(truncated_cell)
               truncated_rows.append(truncated_row)
            rows = truncated_rows
         
         print(tabulate(rows, headers=headers, tablefmt="grid"))
   
   def _handle_collection_if_requested(self, args: argparse.Namespace) -> None:
      """Handle database collection if --collect flag is present"""
      if not hasattr(args, 'collect') or not args.collect:
         return
      
      if not self.collector.database_enabled:
         print("Warning: Database not enabled, skipping collection")
         return
      
      try:
         print("Collecting data to database...")
         result = self.collector.collect_and_persist(collection_type="cli")
         print(f"✓ Collection completed: {result['jobs_collected']} jobs, "
               f"{result['queues_collected']} queues, {result['nodes_collected']} nodes")
      except Exception as e:
         print(f"Warning: Database collection failed: {str(e)}")


class StatusCommand(BaseCommand):
   """Show PBS system status"""
   
   def execute(self, args: argparse.Namespace) -> int:
      """Execute status command"""
      
      try:
         # Get system summary
         summary = self.collector.get_system_summary()
         
         print(f"PBS System Status - {format_timestamp(summary['timestamp'])}")
         print("=" * 60)
         
         # Job statistics
         jobs = summary['jobs']
         print(f"\nJobs:")
         print(f"  Total: {jobs['total']}")
         print(f"  Running: {jobs['running']}")
         print(f"  Queued: {jobs['queued']}")
         print(f"  Held: {jobs['held']}")
         print(f"  Other: {jobs['other']}")
         
         # Queue statistics
         queues = summary['queues']
         print(f"\nQueues:")
         print(f"  Total: {queues['total']}")
         print(f"  Enabled: {queues['enabled']}")
         print(f"  Disabled: {queues['disabled']}")
         
         # Node statistics
         nodes = summary['nodes']
         print(f"\nNodes:")
         print(f"  Total: {nodes['total']}")
         print(f"  Available: {nodes['available']}")
         print(f"  Busy: {nodes['busy']}")
         print(f"  Offline: {nodes['offline']}")
         
         # Resource statistics
         resources = summary['resources']
         print(f"\nResources:")
         print(f"  Total Cores: {resources['total_cores']}")
         print(f"  Used Cores: {resources['used_cores']}")
         print(f"  Available Cores: {resources['available_cores']}")
         print(f"  Utilization: {format_percentage(resources['utilization'])}")
         
         # Queue depth statistics
         queue_depth = summary['queue_depth']
         print(f"\nQueue Depth:")
         print(f"  Total Node-Hours Waiting: {queue_depth['total_node_hours']:.1f}")
         
         # Show detailed queue depth breakdown if requested
         if args.queue_depth:
            self._show_detailed_queue_depth(args)
         
         # Handle database collection if requested
         self._handle_collection_if_requested(args)
         
         return 0
         
      except Exception as e:
         self.logger.error(f"Status command failed: {str(e)}")
         print(f"Error: {str(e)}")
         return 1
   
   def _show_detailed_queue_depth(self, args: argparse.Namespace) -> None:
      """Show detailed queue depth breakdown"""
      try:
         jobs = self.collector.get_jobs()
         from ..analytics.queue_depth import QueueDepthCalculator
         
         queue_calculator = QueueDepthCalculator()
         breakdown = queue_calculator.calculate_queue_depth_breakdown(jobs)
         
         print(f"\nDetailed Queue Depth Breakdown:")
         print("=" * 50)
         
         print(f"\nOverall Summary:")
         print(f"  Total Queued Jobs: {breakdown['total_jobs']}")
         print(f"  Total Node-Hours: {breakdown['total_node_hours']:.1f}")
         
         print(f"\nBy Node Count:")
         for category, data in breakdown['by_node_count'].items():
            if data['jobs'] > 0:
               print(f"  {category:>10} nodes: {data['jobs']:>3} jobs, {data['node_hours']:>8.1f} node-hours")
         
         print(f"\nBy Walltime:")
         for category, data in breakdown['by_walltime'].items():
            if data['jobs'] > 0:
               print(f"  {category:>6}: {data['jobs']:>3} jobs, {data['node_hours']:>8.1f} node-hours")
               
      except Exception as e:
         self.logger.error(f"Failed to show detailed queue depth: {str(e)}")
         print(f"Error showing queue depth details: {str(e)}")


class JobsCommand(BaseCommand):
   """Show job information"""
   
   def execute(self, args: argparse.Namespace) -> int:
      """Execute jobs command"""
      
      try:
         # Check if specific job IDs are provided
         if hasattr(args, 'job_ids') and args.job_ids:
            return self._show_job_details(args)
         
         # Original behavior for showing all jobs
         return self._show_job_summary(args)
         
      except Exception as e:
         self.logger.error(f"Jobs command failed: {str(e)}")
         print(f"Error: {str(e)}")
         return 1
   
   def _show_job_summary(self, args: argparse.Namespace) -> int:
      """Show job summary table (original behavior)"""
      
      # Get jobs
      jobs = self.collector.get_jobs(
         user=args.user,
         force_refresh=args.refresh
      )
      
      # Filter by state if specified
      if args.state:
         jobs = [job for job in jobs if job.state.value == args.state]
      
      if not jobs:
         print("No jobs found")
         # Handle database collection if requested
         self._handle_collection_if_requested(args)
         return 0
      
      # Sort jobs
      sort_key = args.sort if hasattr(args, 'sort') else 'score'
      
      # Determine sort direction
      if hasattr(args, 'reverse'):
         reverse_sort = not args.reverse  # Flip the reverse flag since we want opposite of what user specified
      else:
         # Default sort direction - descending for score, ascending for others
         reverse_sort = (sort_key == 'score')
      
      # Define sort key functions
      sort_functions = {
         'job_id': lambda j: j.job_id,
         'name': lambda j: j.job_name.lower(),
         'owner': lambda j: j.owner.lower(),
         'project': lambda j: (j.project or '').lower(),
         'allocation': lambda j: (j.allocation_type or '').lower(),
         'state': lambda j: j.state.value,
         'queue': lambda j: j.queue.lower(),
         'nodes': lambda j: j.nodes,
         'ppn': lambda j: j.ppn,
         'walltime': lambda j: j.walltime or '',
         'memory': lambda j: j.memory or '',
         'submit_time': lambda j: j.submit_time or datetime.min,
         'start_time': lambda j: j.start_time or datetime.min,
         'priority': lambda j: j.priority,
         'cores': lambda j: j.estimated_total_cores(),
         'score': lambda j: j.score if j.score is not None else -1  # Put jobs without scores at the end
      }
      
      if sort_key in sort_functions:
         try:
            jobs.sort(key=sort_functions[sort_key], reverse=reverse_sort)
         except Exception as e:
            self.logger.warning(f"Failed to sort by {sort_key}: {str(e)}")
      else:
         self.logger.warning(f"Unknown sort key: {sort_key}, using default (score)")
         jobs.sort(key=sort_functions['score'], reverse=True)
      
      # Determine columns
      columns = args.columns.split(',') if args.columns else self.config.display.default_job_columns
      
      # Create table data
      headers = []
      column_formatters = {
         'job_id': lambda j: format_job_id(j.job_id),
         'name': lambda j: j.job_name[:self.config.display.max_name_length] if self.config.display.truncate_long_names else j.job_name,
         'owner': lambda j: j.owner,
         'project': lambda j: j.project or "N/A",
         'allocation': lambda j: j.allocation_type or "N/A",
         'state': lambda j: format_state(j.state.value),
         'queue': lambda j: j.queue,
         'nodes': lambda j: format_number(j.nodes),
         'ppn': lambda j: format_number(j.ppn),
         'walltime': lambda j: format_duration(j.walltime),
         'memory': lambda j: format_memory(j.memory),
         'submit_time': lambda j: format_timestamp(j.submit_time),
         'start_time': lambda j: format_timestamp(j.start_time),
         'runtime': lambda j: j.runtime_duration() or 'N/A',
         'priority': lambda j: format_number(j.priority),
         'cores': lambda j: format_number(j.estimated_total_cores()),
                  'score': lambda j: j.format_score(),
         'queue_time': lambda j: self._format_queue_time(j)
      }

      # Build headers and rows
      for col in columns:
         if col in column_formatters:
            headers.append(col.replace('_', ' ').title())
      
      rows = []
      for job in jobs:
         row = []
         for col in columns:
            if col in column_formatters:
               row.append(column_formatters[col](job))
         rows.append(row)
      
      # Print table
      self._print_table(f"Jobs ({len(jobs)} total)", headers, rows)
      
      # Handle database collection if requested
      self._handle_collection_if_requested(args)
      
      return 0
   
   def _format_queue_time(self, job: PBSJob) -> str:
      """Format queue time, using current time for jobs still in queue"""
      from datetime import datetime
      
      if not job.submit_time:
         return "N/A"
      
      # For completed/running jobs, use queue_time_seconds if available
      if job.queue_time_seconds is not None:
         return format_duration(job.queue_time_seconds)
      
      # For jobs still in queue, calculate against current time
      if job.state.value in ['Q', 'H', 'W']:  # Queued, Held, or Waiting states
         now = datetime.now(job.submit_time.tzinfo)  # Use same timezone as submit_time
         queue_duration = now - job.submit_time
         return format_duration(int(queue_duration.total_seconds()))
      
      return "N/A"

   def _show_job_details(self, args: argparse.Namespace) -> int:
      """Show detailed information for specific jobs"""
      
      all_jobs = []
      unresolved_ids = []
      
      # Resolve each job ID
      for job_id in args.job_ids:
         # Check if it's a full job ID (contains a dot)
         if '.' in job_id:
            # Full job ID provided
            job = self.collector.get_job_by_id(job_id)
            if job:
               all_jobs.append(job)
            else:
               unresolved_ids.append(job_id)
         else:
            # Numerical ID provided - search for matches
            matching_jobs = self.collector.get_jobs_by_numerical_id(job_id)
            if len(matching_jobs) == 1:
               all_jobs.append(matching_jobs[0])
            elif len(matching_jobs) > 1:
               print(f"Multiple jobs found for ID {job_id}:")
               for job in matching_jobs:
                  print(f"  {job.job_id} ({job.state.value}) - {job.owner}")
               print(f"Please specify the full job ID (e.g., {matching_jobs[0].job_id})")
               return 1
            else:
               unresolved_ids.append(job_id)
      
      # Report unresolved job IDs
      if unresolved_ids:
         print(f"Could not find jobs: {', '.join(unresolved_ids)}")
         if not all_jobs:
            return 1
      
      if not all_jobs:
         print("No jobs found")
         return 0
      
      # Determine output format
      output_format = getattr(args, 'format', 'detailed')
      
      if output_format == 'json':
         return self._show_job_details_json(all_jobs, args)
      elif output_format == 'table':
         return self._show_job_details_table(all_jobs, args)
      else:  # detailed
         return self._show_job_details_detailed(all_jobs, args)
   
   def _show_job_details_detailed(self, jobs: List[PBSJob], args: argparse.Namespace) -> int:
      """Show detailed job information in formatted sections"""
      
      for i, job in enumerate(jobs):
         if i > 0:
            print("\n" + "="*80 + "\n")
         
         self._display_job_details(job, args)
      
      # Handle database collection if requested
      self._handle_collection_if_requested(args)
      
      return 0
   
   def _show_job_details_table(self, jobs: List[PBSJob], args: argparse.Namespace) -> int:
      """Show job details in table format"""
      
      # Determine columns - respect --columns argument or use config default
      columns = args.columns.split(',') if args.columns else self.config.display.default_job_columns
      
      # Create table data
      headers = []
      column_formatters = {
         'job_id': lambda j: format_job_id(j.job_id),
         'name': lambda j: j.job_name[:self.config.display.max_name_length] if self.config.display.truncate_long_names else j.job_name,
         'owner': lambda j: j.owner,
         'project': lambda j: j.project or "N/A",
         'allocation': lambda j: j.allocation_type or "N/A",
         'state': lambda j: format_state(j.state.value),
         'queue': lambda j: j.queue,
         'nodes': lambda j: format_number(j.nodes),
         'ppn': lambda j: format_number(j.ppn),
         'walltime': lambda j: format_duration(j.walltime),
         'walltime_actual': lambda j: self._format_walltime_usage(j.walltime, self._get_actual_walltime(j)),
         'memory': lambda j: format_memory(j.memory),
         'submit_time': lambda j: format_timestamp(j.submit_time),
         'start_time': lambda j: format_timestamp(j.start_time),
         'end_time': lambda j: format_timestamp(j.end_time),
         'runtime': lambda j: j.runtime_duration() or 'N/A',
         'priority': lambda j: format_number(j.priority),
         'cores': lambda j: format_number(j.total_cores or j.estimated_total_cores()),
         'score': lambda j: j.format_score(),
         'queue_time': lambda j: format_duration(j.queue_time_seconds) if j.queue_time_seconds else "N/A",
         'exit_status': lambda j: str(j.exit_status) if j.exit_status is not None else "N/A",
         'execution_node': lambda j: j.execution_node or "N/A"
      }
      
      # Build headers and rows
      for col in columns:
         if col in column_formatters:
            headers.append(col.replace('_', ' ').title())
      
      rows = []
      for job in jobs:
         row = []
         for col in columns:
            if col in column_formatters:
               row.append(column_formatters[col](job))
         rows.append(row)
      
      # Print table
      self._print_table(f"Job Details ({len(jobs)} jobs)", headers, rows)
      
      # Handle database collection if requested
      self._handle_collection_if_requested(args)
      
      return 0
   
   def _show_job_details_json(self, jobs: List[PBSJob], args: argparse.Namespace) -> int:
      """Show job details in JSON format"""
      
      import json
      
      job_data = []
      for job in jobs:
         job_info = {
            "job_id": job.job_id,
            "job_name": job.job_name,
            "owner": job.owner,
            "project": job.project,
            "allocation_type": job.allocation_type,
            "state": job.state.value,
            "queue": job.queue,
            "resources": {
               "nodes": job.nodes,
               "ppn": job.ppn,
               "total_cores": job.total_cores or job.estimated_total_cores(),
               "walltime": job.walltime,
               "actual_walltime": self._get_actual_walltime(job),
               "memory": job.memory
            },
            "timing": {
               "submit_time": job.submit_time.isoformat() if job.submit_time else None,
               "start_time": job.start_time.isoformat() if job.start_time else None,
               "end_time": job.end_time.isoformat() if job.end_time else None,
               "queue_time_seconds": job.queue_time_seconds,
               "actual_runtime_seconds": job.actual_runtime_seconds,
               "queue_duration": job.queue_duration(),
               "runtime_duration": job.runtime_duration()
            },
            "priority": job.priority,
            "score": job.score,
            "exit_status": job.exit_status,
            "execution_node": job.execution_node
         }
         
         if getattr(args, 'show_raw', False):
            job_info["raw_attributes"] = job.raw_attributes
         
         job_data.append(job_info)
      
      print(json.dumps(job_data, indent=2))
      
      # Handle database collection if requested
      self._handle_collection_if_requested(args)
      
      return 0
   
   def _display_job_details(self, job: PBSJob, args: argparse.Namespace) -> None:
      """Display detailed information for a single job"""
      
      # Job header
      print(f"Job Details: {format_job_id(job.job_id)}")
      print("=" * 60)
      
      # Basic Information
      print(f"\n📋 Basic Information:")
      print(f"  Name: {job.job_name}")
      print(f"  Owner: {job.owner}")
      if job.project:
         print(f"  Project: {job.project}")
      if job.allocation_type:
         print(f"  Allocation Type: {job.allocation_type}")
      print(f"  State: {format_state(job.state.value)}")
      print(f"  Queue: {job.queue}")
      print(f"  Priority: {format_number(job.priority)}")
      if job.score is not None:
         print(f"  Score: {job.format_score()}")
      
      # Resource Requirements
      print(f"\n💻 Resource Requirements:")
      print(f"  Nodes: {format_number(job.nodes)}")
      print(f"  Cores per Node: {format_number(job.ppn)}")
      print(f"  Total Cores: {format_number(job.total_cores or job.estimated_total_cores())}")
      print(f"  Walltime: {format_duration(job.walltime)}")
      if job.memory:
         print(f"  Memory: {format_memory(job.memory)}")
      
      # Timing Information
      print(f"\n⏰ Timing Information:")
      print(f"  Submit Time: {format_timestamp(job.submit_time)}")
      print(f"  Start Time: {format_timestamp(job.start_time)}")
      print(f"  End Time: {format_timestamp(job.end_time)}")
      
      if job.queue_time_seconds is not None:
         print(f"  Queue Time: {format_duration(job.queue_time_seconds)}")
      elif job.queue_duration():
         print(f"  Queue Duration: {job.queue_duration()}")
      
      if job.actual_runtime_seconds is not None:
         print(f"  Actual Runtime: {format_duration(job.actual_runtime_seconds)}")
      elif job.runtime_duration():
         print(f"  Runtime: {job.runtime_duration()}")
      
      # Resource Usage (for completed jobs)
      if job.state.value in ['C', 'F', 'E']:
         print(f"\n📊 Resource Usage:")
         actual_walltime = self._get_actual_walltime(job)
         if actual_walltime:
            walltime_usage = self._format_walltime_usage(job.walltime, actual_walltime)
            print(f"  Walltime Used: {walltime_usage}")
      
      # Execution Details
      print(f"\n🚀 Execution Details:")
      if job.execution_node:
         print(f"  Execution Node: {job.execution_node}")
      if job.exit_status is not None:
         print(f"  Exit Status: {job.exit_status}")
      
      # Job History (if requested and available)
      if getattr(args, 'history', False) and self.collector.database_enabled:
         self._display_job_history(job)
      
      # Raw Attributes (if requested)
      if getattr(args, 'show_raw', False):
         print(f"\n🔧 Raw PBS Attributes:")
         for key, value in job.raw_attributes.items():
            if isinstance(value, dict):
               print(f"  {key}:")
               for subkey, subvalue in value.items():
                  print(f"    {subkey}: {subvalue}")
            else:
               print(f"  {key}: {value}")
   
   def _get_actual_walltime(self, job: PBSJob) -> Optional[str]:
      """Get actual walltime used by the job"""
      if not job.raw_attributes:
         return None
      
      resources_used = job.raw_attributes.get('resources_used', {})
      return resources_used.get('walltime')
   
   def _format_walltime_usage(self, requested: Optional[str], actual: Optional[str]) -> str:
      """Format walltime usage with percentage"""
      if not actual:
         return "N/A"
      
      if not requested:
         return format_duration(actual)
      
      # Calculate percentage
      try:
         requested_seconds = self._parse_walltime_to_seconds(requested)
         actual_seconds = self._parse_walltime_to_seconds(actual)
         
         if requested_seconds > 0:
            percentage = (actual_seconds / requested_seconds) * 100
            return f"{format_duration(actual)} ({percentage:.1f}%)"
         else:
            return format_duration(actual)
      except (ValueError, TypeError):
         return format_duration(actual)
   
   def _parse_walltime_to_seconds(self, walltime_str: str) -> float:
      """Parse walltime string to seconds"""
      if not walltime_str:
         return 0
      
      try:
         parts = walltime_str.split(':')
         if len(parts) == 3:
            # HH:MM:SS format
            hours, minutes, seconds = map(int, parts)
            return hours * 3600 + minutes * 60 + seconds
         elif len(parts) == 4:
            # DD:HH:MM:SS format
            days, hours, minutes, seconds = map(int, parts)
            return days * 86400 + hours * 3600 + minutes * 60 + seconds
         else:
            return 0
      except (ValueError, TypeError):
         return 0
   
   def _display_job_history(self, job: PBSJob) -> None:
      """Display job history from database"""
      try:
         historical_data = self.collector.get_historical_job_data(job.job_id)
         
         if 'error' in historical_data:
            print(f"\n📚 Job History: {historical_data['error']}")
            return
         
         print(f"\n📚 Job History:")
         print(f"  History Entries: {historical_data['history_entries']}")
         
         if historical_data['first_seen']:
            print(f"  First Seen: {format_timestamp(historical_data['first_seen'])}")
         if historical_data['last_seen']:
            print(f"  Last Seen: {format_timestamp(historical_data['last_seen'])}")
         
         transitions = historical_data.get('state_transitions', [])
         if transitions:
            print(f"  State Transitions:")
            for transition in transitions:
               duration = transition.get('duration_minutes', 0)
               print(f"    {transition['from_state']} → {transition['to_state']} "
                     f"({format_timestamp(transition['timestamp'])}) "
                     f"[{duration:.1f} min]")
         
      except Exception as e:
         print(f"\n📚 Job History: Error retrieving history - {str(e)}")


class NodesCommand(BaseCommand):
   """Show node information"""
   
   def execute(self, args: argparse.Namespace) -> int:
      """Execute nodes command"""
      
      try:
         # Get nodes
         nodes = self.collector.get_nodes(force_refresh=args.refresh)
         
         # Filter by state if specified
         if args.state:
            nodes = [node for node in nodes if node.state.value == args.state]
         
         if not nodes:
            print("No nodes found")
            # Handle database collection if requested
            self._handle_collection_if_requested(args)
            return 0
         
         # Check if detailed mode is requested
         if args.detailed:
            return self._show_detailed_nodes(nodes, args)
         else:
            return self._show_node_summary(nodes, args)
         
      except Exception as e:
         self.logger.error(f"Nodes command failed: {str(e)}")
         print(f"Error: {str(e)}")
         return 1
   
   def _show_detailed_nodes(self, nodes: List[PBSNode], args: argparse.Namespace) -> int:
      """Show detailed node table (original behavior)"""
      
      # Determine columns
      columns = args.columns.split(',') if args.columns else self.config.display.default_node_columns
      
      # Create table data
      headers = []
      column_formatters = {
         'name': lambda n: n.name,
         'state': lambda n: format_state(n.state.value),
         'ncpus': lambda n: format_number(n.ncpus),
         'memory': lambda n: format_memory(n.memory),
         'jobs': lambda n: format_number(len(n.jobs)),
         'load': lambda n: format_percentage(n.load_percentage()),
         'utilization': lambda n: format_percentage(n.cpu_utilization()),
         'available': lambda n: format_number(n.available_cpus()),
         'properties': lambda n: ', '.join(n.properties[:3]) + ('...' if len(n.properties) > 3 else '')
      }
      
      # Build headers and rows
      for col in columns:
         if col in column_formatters:
            headers.append(col.replace('_', ' ').title())
      
      rows = []
      for node in nodes:
         row = []
         for col in columns:
            if col in column_formatters:
               row.append(column_formatters[col](node))
         rows.append(row)
      
      # Print table
      self._print_table(f"Nodes ({len(nodes)} total)", headers, rows)
      
      # Handle database collection if requested
      self._handle_collection_if_requested(args)
      
      return 0
   
   def _show_node_summary(self, nodes: List[PBSNode], args: argparse.Namespace) -> int:
      """Show node summary (new default behavior)"""
      
      # Calculate summary statistics
      summary_stats = self._calculate_node_summary(nodes)
      
      # Print overall summary
      print(f"Node Summary - {format_timestamp(datetime.now())}")
      print("=" * 50)
      print(f"Total Nodes: {summary_stats['total_nodes']}")
      
      # Print state breakdown with percentages
      state_stats = summary_stats['state_breakdown']
      for state, count in state_stats.items():
         percentage = (count / summary_stats['total_nodes']) * 100
         print(f"  └─ {state.replace('_', ' ').title()}: {count} ({percentage:.1f}%)")
      
      # Print resource summary
      resources = summary_stats['resources']
      print(f"\nResources:")
      print(f"  └─ Total CPUs: {format_number(resources['total_cpus'])}")
      print(f"  └─ Used CPUs: {format_number(resources['used_cpus'])}")
      print(f"  └─ Available CPUs: {format_number(resources['available_cpus'])}")
      print(f"  └─ CPU Utilization: {format_percentage(resources['cpu_utilization'])}")
      
      if resources['total_memory_gb']:
         print(f"  └─ Total Memory: {resources['total_memory_gb']:.1f} TB")
         print(f"  └─ Used Memory: {resources['used_memory_gb']:.1f} TB")
         print(f"  └─ Available Memory: {resources['available_memory_gb']:.1f} TB")
         print(f"  └─ Memory Utilization: {format_percentage(resources['memory_utilization'])}")
      
      # Print state breakdown table
      print(f"\nState Breakdown:")
      self._print_state_breakdown_table(summary_stats)
      
      # Print hardware types summary
      if summary_stats['hardware_types']:
         print(f"\nHardware Types:")
         self._print_hardware_types_table(summary_stats['hardware_types'])
      
      # Print attention items
      attention_items = self._get_attention_items(nodes, summary_stats)
      if attention_items:
         print(f"\nAttention Required:")
         for item in attention_items:
            print(f"  • {item}")
      
      # Handle database collection if requested
      self._handle_collection_if_requested(args)
      
      return 0
   
   def _calculate_node_summary(self, nodes: List[PBSNode]) -> Dict[str, Any]:
      """Calculate comprehensive node summary statistics"""
      
      # Initialize counters
      state_counts = {}
      total_cpus = 0
      used_cpus = 0
      total_memory_gb = 0.0
      used_memory_gb = 0.0
      hardware_types = {}
      
      # Process each node
      for node in nodes:
         # Count by state
         state_key = node.state.value.replace('-', '_')
         state_counts[state_key] = state_counts.get(state_key, 0) + 1
         
         # Resource calculations
         total_cpus += node.ncpus
         used_cpus += len(node.jobs)
         
         # Memory calculations
         memory_gb = node.memory_gb()
         if memory_gb:
            total_memory_gb += memory_gb
            if node.is_occupied():
               # Estimate used memory proportionally
               used_memory_gb += memory_gb * (len(node.jobs) / node.ncpus) if node.ncpus > 0 else 0
         
         # Hardware type classification
         cpu_type = node.raw_attributes.get('resources_available', {}).get('cputype', 'unknown')
         gpu_type = node.raw_attributes.get('resources_available', {}).get('gputype', 'none')
         hw_key = f"{cpu_type}/{gpu_type}"
         
         if hw_key not in hardware_types:
            hardware_types[hw_key] = {
               'count': 0,
               'cpus': 0,
               'memory_gb': 0.0,
               'used_cpus': 0
            }
         
         hardware_types[hw_key]['count'] += 1
         hardware_types[hw_key]['cpus'] += node.ncpus
         hardware_types[hw_key]['memory_gb'] += memory_gb or 0
         hardware_types[hw_key]['used_cpus'] += len(node.jobs)
      
      # Calculate utilization percentages
      cpu_utilization = (used_cpus / total_cpus * 100) if total_cpus > 0 else 0
      memory_utilization = (used_memory_gb / total_memory_gb * 100) if total_memory_gb > 0 else 0
      
      return {
         'total_nodes': len(nodes),
         'state_breakdown': state_counts,
         'resources': {
            'total_cpus': total_cpus,
            'used_cpus': used_cpus,
            'available_cpus': total_cpus - used_cpus,
            'cpu_utilization': cpu_utilization,
            'total_memory_gb': total_memory_gb / 1024 if total_memory_gb > 0 else None,  # Convert to TB
            'used_memory_gb': used_memory_gb / 1024 if used_memory_gb > 0 else None,    # Convert to TB
            'available_memory_gb': (total_memory_gb - used_memory_gb) / 1024 if total_memory_gb > 0 else None,
            'memory_utilization': memory_utilization
         },
         'hardware_types': hardware_types
      }
   
   def _print_state_breakdown_table(self, summary_stats: Dict[str, Any]) -> None:
      """Print state breakdown table"""
      
      state_data = []
      total_nodes = summary_stats['total_nodes']
      
      for state, count in summary_stats['state_breakdown'].items():
         # Calculate resources for this state
         state_cpus = 0
         state_memory = 0.0
         state_jobs = 0
         
         # We need to recalculate from original nodes for accurate per-state data
         # For now, use proportional estimates
         cpu_ratio = count / total_nodes
         state_cpus = int(summary_stats['resources']['total_cpus'] * cpu_ratio)
         
         if summary_stats['resources']['total_memory_gb']:
            state_memory = summary_stats['resources']['total_memory_gb'] * cpu_ratio
         
         state_data.append([
            state.replace('_', ' ').title(),
            format_number(count),
            format_number(state_cpus),
            f"{state_memory:.1f} TB" if state_memory > 0 else "N/A",
            "N/A"  # Jobs per state would need more complex calculation
         ])
      
      headers = ["State", "Count", "CPUs", "Memory", "Running Jobs"]
      
      if self.config.display.use_colors:
         table = Table(title="State Breakdown", show_header=True, header_style="bold magenta")
         for header in headers:
            table.add_column(header, style="cyan")
         for row in state_data:
            table.add_row(*row)
         self.console.print(table)
      else:
         print(tabulate(state_data, headers=headers, tablefmt="grid"))
   
   def _print_hardware_types_table(self, hardware_types: Dict[str, Any]) -> None:
      """Print hardware types table"""
      
      hw_data = []
      for hw_type, stats in hardware_types.items():
         utilization = (stats['used_cpus'] / stats['cpus'] * 100) if stats['cpus'] > 0 else 0
         hw_data.append([
            hw_type,
            format_number(stats['count']),
            format_number(stats['cpus']),
            f"{stats['memory_gb']/1024:.1f} TB" if stats['memory_gb'] > 0 else "N/A",
            format_percentage(utilization)
         ])
      
      headers = ["Type (CPU/GPU)", "Count", "CPUs", "Memory", "Utilization"]
      
      if self.config.display.use_colors:
         table = Table(title="Hardware Types", show_header=True, header_style="bold magenta")
         for header in headers:
            table.add_column(header, style="cyan")
         for row in hw_data:
            table.add_row(*row)
         self.console.print(table)
      else:
         print(tabulate(hw_data, headers=headers, tablefmt="grid"))
   
   def _get_attention_items(self, nodes: List[PBSNode], summary_stats: Dict[str, Any]) -> List[str]:
      """Generate list of items requiring attention"""
      
      attention_items = []
      
      # Check for high offline percentage
      offline_count = summary_stats['state_breakdown'].get('offline', 0)
      if offline_count > 0:
         offline_pct = (offline_count / summary_stats['total_nodes']) * 100
         if offline_pct > 20:  # More than 20% offline
            attention_items.append(f"{offline_count} nodes offline ({offline_pct:.1f}% of cluster)")
      
      # Check for nodes with high load
      high_load_nodes = [n for n in nodes if n.load_percentage() and n.load_percentage() > 90]
      if high_load_nodes:
         attention_items.append(f"{len(high_load_nodes)} nodes with high load (>90%)")
      
      # Check for nodes with job cleanup issues (nodes with comment containing cleanup info)
      cleanup_nodes = [n for n in nodes if n.raw_attributes.get('comment', '').lower().find('cleanup') >= 0 or 
                      n.raw_attributes.get('comment', '').lower().find('not cleaned') >= 0]
      if cleanup_nodes:
         attention_items.append(f"{len(cleanup_nodes)} nodes with job cleanup issues")
      
      # Check for down nodes
      down_count = summary_stats['state_breakdown'].get('down', 0)
      if down_count > 0:
         attention_items.append(f"{down_count} nodes down (hardware issues)")
      
      return attention_items


class QueuesCommand(BaseCommand):
   """Show queue information"""
   
   def execute(self, args: argparse.Namespace) -> int:
      """Execute queues command"""
      
      try:
         # Get queues
         queues = self.collector.get_queues(force_refresh=args.refresh)
         
         if not queues:
            print("No queues found")
            # Handle database collection if requested
            self._handle_collection_if_requested(args)
            return 0
         
         # Determine columns
         columns = args.columns.split(',') if args.columns else self.config.display.default_queue_columns
         
         # Create table data
         headers = []
         column_formatters = {
            'name': lambda q: q.name,
            'status': lambda q: q.status_description(),
            'type': lambda q: q.queue_type,
            'running': lambda q: format_number(q.running_jobs),
            'queued': lambda q: format_number(q.queued_jobs),
            'held': lambda q: format_number(q.held_jobs),
            'total': lambda q: format_number(q.total_jobs),
            'max_running': lambda q: format_number(q.max_running) if q.max_running is not None else "∞",
                        'max_queued': lambda q: format_number(q.max_queued) if q.max_queued is not None else "∞",
            'available': lambda q: format_number(q.available_slots()) if q.available_slots() is not None else "∞",
            'priority': lambda q: format_number(q.priority),
            'max_walltime': lambda q: format_duration(q.max_walltime),
            'max_nodes': lambda q: format_number(q.max_nodes) if q.max_nodes is not None else "∞"
         }
         
         # Build headers and rows
         for col in columns:
            if col in column_formatters:
               headers.append(col.replace('_', ' ').title())
         
         rows = []
         for queue in queues:
            row = []
            for col in columns:
               if col in column_formatters:
                  row.append(column_formatters[col](queue))
            rows.append(row)
         
         # Print table
         self._print_table(f"Queues ({len(queues)} total)", headers, rows)
         
         # Handle database collection if requested
         self._handle_collection_if_requested(args)
         
         return 0
         
      except Exception as e:
         self.logger.error(f"Queues command failed: {str(e)}")
         print(f"Error: {str(e)}")
         return 1


class DatabaseCommand(BaseCommand):
   """Database management commands"""
   
   def execute(self, args: argparse.Namespace) -> int:
      """Execute database command"""
      
      try:
         # Get database subcommand
         subcommand = args.database_action
         
         if subcommand is None:
            print("Error: No database action specified")
            print("\nAvailable database actions:")
            print("  init      Initialize database schema")
            print("  migrate   Migrate database to latest schema")
            print("  status    Show database status and information")
            print("  validate  Validate database schema and data")
            print("  backup    Create database backup")
            print("  restore   Restore database from backup")
            print("  cleanup   Clean up old data from database")
            print("\nExamples:")
            print("  pbs-monitor database init                    # Initialize database")
            print("  pbs-monitor database status                  # Show database status")
            print("  pbs-monitor database backup                  # Create backup")
            print("  pbs-monitor database cleanup --days 30       # Clean up old data")
            print("\nUse 'pbs-monitor database <action> --help' for more information about each action")
            return 1
         elif subcommand == 'init':
            return self._init_database(args)
         elif subcommand == 'migrate':
            return self._migrate_database(args)
         elif subcommand == 'status':
            return self._show_database_status(args)
         elif subcommand == 'validate':
            return self._validate_database(args)
         elif subcommand == 'backup':
            return self._backup_database(args)
         elif subcommand == 'restore':
            return self._restore_database(args)
         elif subcommand == 'cleanup':
            return self._cleanup_database(args)
         else:
            print(f"Unknown database subcommand: {subcommand}")
            print("\nAvailable actions: init, migrate, status, validate, backup, restore, cleanup")
            return 1
            
      except Exception as e:
         self.logger.error(f"Database command failed: {str(e)}")
         print(f"Error: {str(e)}")
         return 1
   
   def _init_database(self, args: argparse.Namespace) -> int:
      """Initialize database"""
      print("Initializing database...")
      
      if hasattr(args, 'force') and args.force:
         # Force initialization (drops existing tables)
         print("WARNING: This will drop all existing tables and data!")
         confirm = input("Are you sure? Type 'yes' to continue: ")
         if confirm.lower() != 'yes':
            print("Database initialization cancelled")
            return 0
      
      try:
         initialize_database(self.config)
         print("Database initialized successfully")
         return 0
      except Exception as e:
         print(f"Database initialization failed: {str(e)}")
         return 1
   
   def _migrate_database(self, args: argparse.Namespace) -> int:
      """Migrate database to latest schema"""
      print("Migrating database to latest schema...")
      
      try:
         migrate_database(self.config)
         print("Database migration completed successfully")
         return 0
      except Exception as e:
         print(f"Database migration failed: {str(e)}")
         return 1
   
   def _show_database_status(self, args: argparse.Namespace) -> int:
      """Show database status"""
      try:
         info = get_database_info(self.config)
         
         print("Database Information")
         print("=" * 50)
         print(f"Database URL: {info['database_url']}")
         print(f"Schema Version: {info['schema_version'] or 'Unknown'}")
         
         if info['database_size']:
            size_mb = info['database_size'] / (1024 * 1024)
            print(f"Database Size: {size_mb:.1f} MB")
         
         print(f"\nTables: {len(info['tables'])}")
         for table in sorted(info['tables']):
            count = info['table_counts'].get(table, 'N/A')
            print(f"  {table}: {count} records")
         
         # Validation results
         validation = info['validation']
         print(f"\nSchema Validation: {'PASS' if validation['valid'] else 'FAIL'}")
         
         if validation['errors']:
            print("Errors:")
            for error in validation['errors']:
               print(f"  - {error}")
         
         if validation['warnings']:
            print("Warnings:")
            for warning in validation['warnings']:
               print(f"  - {warning}")
         
         return 0
         
      except Exception as e:
         print(f"Failed to get database status: {str(e)}")
         return 1
   
   def _validate_database(self, args: argparse.Namespace) -> int:
      """Validate database schema"""
      print("Validating database schema...")
      
      try:
         validation = validate_database(self.config)
         
         if validation['valid']:
            print("✓ Database schema validation PASSED")
         else:
            print("✗ Database schema validation FAILED")
            
            if validation['errors']:
               print("\nErrors:")
               for error in validation['errors']:
                  print(f"  - {error}")
         
         if validation['warnings']:
            print("\nWarnings:")
            for warning in validation['warnings']:
               print(f"  - {warning}")
         
         # Table status
         print("\nTable Status:")
         for table, status in validation['table_status'].items():
            status_symbol = "✓" if status == "exists" else "✗"
            print(f"  {status_symbol} {table}: {status}")
         
         return 0 if validation['valid'] else 1
         
      except Exception as e:
         print(f"Database validation failed: {str(e)}")
         return 1
   
   def _backup_database(self, args: argparse.Namespace) -> int:
      """Backup database"""
      backup_path = getattr(args, 'backup_path', None)
      
      try:
         result_path = backup_database(backup_path, self.config)
         print(f"Database backed up to: {result_path}")
         return 0
      except Exception as e:
         print(f"Database backup failed: {str(e)}")
         return 1
   
   def _restore_database(self, args: argparse.Namespace) -> int:
      """Restore database from backup"""
      if not hasattr(args, 'backup_path') or not args.backup_path:
         print("Error: backup path is required for restore")
         return 1
      
      print(f"Restoring database from: {args.backup_path}")
      print("WARNING: This will overwrite the current database!")
      confirm = input("Are you sure? Type 'yes' to continue: ")
      if confirm.lower() != 'yes':
         print("Database restore cancelled")
         return 0
      
      try:
         restore_database(args.backup_path, self.config)
         print("Database restored successfully")
         return 0
      except Exception as e:
         print(f"Database restore failed: {str(e)}")
         return 1
   
   def _cleanup_database(self, args: argparse.Namespace) -> int:
      """Clean up old data from database"""
      job_history_days = getattr(args, 'job_history_days', 365)
      snapshot_days = getattr(args, 'snapshot_days', 90)
      
      print(f"Cleaning up data older than:")
      print(f"  Job history: {job_history_days} days")
      print(f"  Snapshots: {snapshot_days} days")
      
      if not getattr(args, 'force', False):
         confirm = input("Continue? Type 'yes' to proceed: ")
         if confirm.lower() != 'yes':
            print("Database cleanup cancelled")
            return 0
      
      try:
         results = clean_old_data(job_history_days, snapshot_days, self.config)
         
         print("Cleanup completed:")
         print(f"  Job history records deleted: {results['job_history_deleted']}")
         print(f"  Queue snapshots deleted: {results['queue_snapshots_deleted']}")
         print(f"  Node snapshots deleted: {results['node_snapshots_deleted']}")
         print(f"  System snapshots deleted: {results['system_snapshots_deleted']}")
         
         total_deleted = sum(results.values())
         print(f"  Total records deleted: {total_deleted}")
         
         return 0
      except Exception as e:
         print(f"Database cleanup failed: {str(e)}")
         return 1


class HistoryCommand(BaseCommand):
   """Show historical job information from database"""
   
   def execute(self, args: argparse.Namespace) -> int:
      """Execute history command"""
      
      try:
         # Check if database is available
         if not hasattr(self.collector, '_database_enabled') or not self.collector._database_enabled:
            print("Error: Database is not enabled. Historical data is not available.")
            print("Please run 'pbs-monitor database init' to set up the database.")
            return 1
         
         # Get historical jobs from database
         historical_jobs = self._get_historical_jobs(args)
         
         # Include PBS history if requested
         if args.include_pbs_history:
            try:
               pbs_completed_jobs = self.collector.pbs_commands.qstat_completed_jobs(user=args.user)
               # Merge with historical jobs, avoiding duplicates
               historical_job_ids = {job.job_id for job in historical_jobs}
               for pbs_job in pbs_completed_jobs:
                  if pbs_job.job_id not in historical_job_ids:
                     historical_jobs.append(pbs_job)
               if pbs_completed_jobs:
                  print(f"Added {len(pbs_completed_jobs)} jobs from recent PBS history")
            except Exception as e:
               error_msg = str(e)
               if "utf-8" in error_msg.lower() and "decode" in error_msg.lower():
                  print("Note: PBS history contains some non-UTF-8 characters (likely in job names or comments).")
                  print("This is normal and doesn't affect functionality - the data will be processed with character replacement.")
                  self.logger.info("PBS history contains non-UTF-8 characters, using permissive encoding")
               else:
                  self.logger.warning(f"Failed to get PBS completed jobs: {error_msg}")
                  print(f"Warning: Could not retrieve recent PBS history: {error_msg}")
         
         if not historical_jobs:
            print("No historical jobs found for the specified criteria")
            return 0
         
         # Filter by state if specified
         if args.state != "all":
            historical_jobs = [job for job in historical_jobs if job.state.value == args.state]
         
         # Sort jobs BEFORE applying limit to get the top N jobs by sort criteria
         historical_jobs = self._sort_jobs(historical_jobs, args.sort, args.reverse)
         
         # Apply limit after sorting to get the top N jobs
         if len(historical_jobs) > args.limit:
            historical_jobs = historical_jobs[:args.limit]
            print(f"Showing top {args.limit} jobs by {args.sort} (use --limit to adjust)")
         
         # Display jobs
         self._display_historical_jobs(historical_jobs, args)
         
         return 0
         
      except Exception as e:
         self.logger.error(f"History command failed: {str(e)}")
         print(f"Error: {str(e)}")
         return 1
   
   def _get_historical_jobs(self, args: argparse.Namespace) -> List[PBSJob]:
      """Get historical jobs from database"""
      from ..database.repositories import JobRepository
      from ..database.models import JobState as DBJobState
      
      job_repo = self.collector._repository_factory.get_job_repository()
      
      # Get jobs from database
      if args.state == "all":
         # Get all completed jobs
         db_jobs = job_repo.get_historical_jobs(user=args.user, days=args.days)
         # Filter to only completed states
         db_jobs = [job for job in db_jobs if job.is_completed()]
      else:
         # Get jobs by specific state
         state_map = {"C": DBJobState.COMPLETED, "F": DBJobState.FINISHED, "E": DBJobState.EXITING}
         db_state = state_map[args.state]
         db_jobs = job_repo.get_jobs_by_state(db_state)
         # Apply user filter if specified
         if args.user:
            db_jobs = [job for job in db_jobs if job.owner == args.user]
      
      # Convert to PBSJob objects
      historical_jobs = []
      for db_job in db_jobs:
         try:
            pbs_job = self.collector._model_converters.job.from_database(db_job)
            historical_jobs.append(pbs_job)
         except Exception as e:
            self.logger.warning(f"Failed to convert job {db_job.job_id}: {str(e)}")
      
      return historical_jobs
   
   def _sort_jobs(self, jobs: List[PBSJob], sort_key: str, reverse: bool) -> List[PBSJob]:
      """Sort jobs by specified key"""
      from datetime import datetime
      
      sort_functions = {
         'job_id': lambda j: j.job_id,
         'name': lambda j: j.job_name.lower(),
         'owner': lambda j: j.owner.lower(),
         'project': lambda j: (j.project or '').lower(),
         'allocation': lambda j: (j.allocation_type or '').lower(),
         'state': lambda j: j.state.value,
         'queue': lambda j: j.queue.lower(),
         'nodes': lambda j: j.nodes,
         'walltime': lambda j: self._parse_walltime_for_sort(j.walltime),
         'submit_time': lambda j: j.submit_time or datetime.min,
         'start_time': lambda j: j.start_time or datetime.min,
         'end_time': lambda j: j.end_time or datetime.min,
         'queued': lambda j: self._calculate_queue_seconds(j),
         'runtime': lambda j: self._calculate_runtime_seconds(j)
      }
      
      if sort_key in sort_functions:
         try:
            jobs.sort(key=sort_functions[sort_key], reverse=reverse)
         except Exception as e:
            self.logger.warning(f"Failed to sort by {sort_key}: {str(e)}")
      else:
         self.logger.warning(f"Unknown sort key: {sort_key}, using default (submit_time)")
         jobs.sort(key=sort_functions['submit_time'], reverse=reverse)
      
      return jobs
   
   def _calculate_runtime_seconds(self, job: PBSJob) -> int:
      """Calculate runtime in seconds for sorting"""
      if job.start_time and job.end_time:
         return int((job.end_time - job.start_time).total_seconds())
      return 0
   
   def _calculate_queue_seconds(self, job: PBSJob) -> int:
      """Calculate queue time in seconds for sorting"""
      if job.submit_time and job.start_time:
         queue_duration = job.start_time - job.submit_time
         return max(0, int(queue_duration.total_seconds()))  # Ensure non-negative
      return 0
   
   def _parse_walltime_for_sort(self, walltime: Optional[str]) -> int:
      """Parse walltime string to seconds for sorting"""
      if not walltime:
         return 0
      
      try:
         # Handle format like "HH:MM:SS" or "HHHHH:MM:SS"
         parts = walltime.split(':')
         if len(parts) == 3:
            hours, minutes, seconds = map(int, parts)
            return hours * 3600 + minutes * 60 + seconds
         elif len(parts) == 2:
            # Handle format like "MM:SS"
            minutes, seconds = map(int, parts)
            return minutes * 60 + seconds
         else:
            return 0
      except (ValueError, AttributeError):
         return 0
   
   def _display_historical_jobs(self, jobs: List[PBSJob], args: argparse.Namespace) -> None:
      """Display historical jobs in table format"""
      
      # Determine columns
      default_columns = ['job_id', 'name', 'owner', 'project', 'allocation', 'state', 'queue', 'nodes', 'walltime', 'submit_time', 'queued', 'runtime', 'exit_status']
      columns = args.columns.split(',') if args.columns else default_columns
      
      # Create table data
      headers = []
      column_formatters = {
         'job_id': lambda j: format_job_id(j.job_id),
         'name': lambda j: j.job_name[:30] + "..." if len(j.job_name) > 30 else j.job_name,
         'owner': lambda j: j.owner,
         'project': lambda j: j.project or "N/A",
         'allocation': lambda j: j.allocation_type or "N/A",
         'state': lambda j: format_state(j.state.value),
         'queue': lambda j: j.queue,
         'nodes': lambda j: format_number(j.nodes),
         'walltime': lambda j: format_duration(j.walltime),
         'submit_time': lambda j: format_timestamp(j.submit_time),
         'start_time': lambda j: format_timestamp(j.start_time),
         'end_time': lambda j: format_timestamp(j.end_time),
         'queued': lambda j: self._format_queue_duration(j),
         'runtime': lambda j: self._format_runtime(j),
         'exit_status': lambda j: self._format_exit_status(j),
         'cores': lambda j: format_number(j.estimated_total_cores())
      }
      
      # Build headers and rows
      for col in columns:
         if col in column_formatters:
            headers.append(col.replace('_', ' ').title())
      
      rows = []
      for job in jobs:
         row = []
         for col in columns:
            if col in column_formatters:
               row.append(column_formatters[col](job))
         rows.append(row)
      
      # Print table
      self._print_table(f"Historical Jobs ({len(jobs)} total)", headers, rows)
      
      # Check if any jobs have incomplete timing data and show explanation
      has_incomplete_data = any(
         (job.state.value in ['C', 'F', 'E'] and 
          (not job.start_time or not job.end_time or job.exit_status is None))
         for job in jobs
      )
      if has_incomplete_data:
         print("\n* Unknown: Job completed but timing/status data missing from database")
         print("  (These are likely old jobs collected before completion tracking was implemented)")
         print("  Note: Recent jobs may be updated by running: pbs-monitor jobs --collect")
   
   def _format_runtime(self, job: PBSJob) -> str:
      """Format job runtime for display"""
      if job.start_time and job.end_time:
         duration = job.end_time - job.start_time
         total_seconds = int(duration.total_seconds())
         hours = total_seconds // 3600
         minutes = (total_seconds % 3600) // 60
         seconds = total_seconds % 60
         return f"{hours:02d}:{minutes:02d}:{seconds:02d}"
      elif job.state.value in ['C', 'F', 'E']:  # Completed states
         return "Unknown*"  # Job completed but timing data missing
      else:
         return "N/A"  # Job not completed yet
   
   def _format_queue_duration(self, job: PBSJob) -> str:
      """Format queue duration for display with better error handling"""
      queue_duration = job.queue_duration()
      if queue_duration:
         return queue_duration
      elif job.state.value in ['C', 'F', 'E']:  # Completed states
         return "Unknown*"  # Job completed but timing data missing
      else:
         return "N/A"  # Job not completed yet
   
   def _format_exit_status(self, job: PBSJob) -> str:
      """Format exit status for display with better error handling"""
      if job.exit_status is not None:
         return str(job.exit_status)
      elif job.state.value in ['C', 'F', 'E']:  # Completed states
         return "Unknown*"  # Job completed but exit status missing
      else:
         return "N/A"  # Job not completed yet
         
   def _format_queue_time(self, job: PBSJob) -> str:
      """Format queue time, using current time for jobs still in queue"""
      from datetime import datetime
      
      if not job.submit_time:
         return "N/A"
      
      # For completed/running jobs, use queue_time_seconds if available
      if job.queue_time_seconds is not None:
         return format_duration(job.queue_time_seconds)
      
      # For jobs still in queue, calculate against current time
      if job.state.value in ['Q', 'H', 'W']:  # Queued, Held, or Waiting states
         now = datetime.now(job.submit_time.tzinfo)  # Use same timezone as submit_time
         queue_duration = now - job.submit_time
         return format_duration(int(queue_duration.total_seconds()))


class DaemonCommand(BaseCommand):
   """Daemon management commands"""
   
   def __init__(self, collector: DataCollector, config: Config):
      # For daemon commands, collector might be None
      self.config = config
      self.logger = logging.getLogger(__name__)
      
      # Initialize console for rich output if display config is available
      if hasattr(config, 'display'):
         from rich.console import Console
         self.console = Console(
            width=config.display.max_table_width,
            force_terminal=True if config.display.use_colors else False
         )
      else:
         self.console = None
   
   def execute(self, args: argparse.Namespace) -> int:
      """Execute daemon command"""
      
      try:
         # Get daemon subcommand
         subcommand = args.daemon_action
         
         if subcommand is None:
            print("Error: No daemon action specified")
            print("\nAvailable daemon actions:")
            print("  start     Start the PBS monitor daemon")
            print("  stop      Stop the PBS monitor daemon")
            print("  status    Show daemon status")
            print("\nExamples:")
            print("  pbs-monitor daemon start                    # Start the daemon")
            print("  pbs-monitor daemon stop                     # Stop the daemon")
            print("  pbs-monitor daemon status                   # Check daemon status")
            print("\nUse 'pbs-monitor daemon <action> --help' for more information about each action")
            return 1
         elif subcommand == 'start':
            return self._start_daemon(args)
         elif subcommand == 'stop':
            return self._stop_daemon(args)
         elif subcommand == 'status':
            return self._show_daemon_status(args)
         else:
            print(f"Unknown daemon subcommand: {subcommand}")
            print("\nAvailable actions: start, stop, status")
            return 1
            
      except Exception as e:
         self.logger.error(f"Daemon command failed: {str(e)}")
         print(f"Error: {str(e)}")
         return 1
   
   def _get_pid_file_path(self, args: argparse.Namespace) -> Path:
      """Get PID file path from args or default"""
      if hasattr(args, 'pid_file') and args.pid_file:
         return Path(args.pid_file)
      return Path.home() / ".pbs_monitor_daemon.pid"
   
   def _write_daemon_info(self, pid_file: Path, pid: int) -> None:
      """Write daemon information to JSON PID file"""
      daemon_info = {
         "hostname": socket.gethostname(),
         "pid": pid,
         "start_timestamp": datetime.now().isoformat(),
         "working_directory": str(Path.cwd()),
         "user": getpass.getuser()
      }
      
      try:
         with open(pid_file, 'w') as f:
            json.dump(daemon_info, f, indent=2)
      except Exception as e:
         raise Exception(f"Failed to write daemon info to {pid_file}: {str(e)}")
   
   def _read_daemon_info(self, pid_file: Path) -> Dict[str, Any]:
      """Read daemon information from PID file (JSON or legacy format)"""
      if not pid_file.exists():
         return None
      
      try:
         with open(pid_file, 'r') as f:
            content = f.read().strip()
         
         # Try to parse as JSON first
         try:
            daemon_info = json.loads(content)
            # Check if it's a dictionary with required fields
            if isinstance(daemon_info, dict) and 'hostname' in daemon_info and 'pid' in daemon_info:
               return daemon_info
         except json.JSONDecodeError:
            pass
         
         # Fall back to legacy PID-only format
         try:
            pid = int(content)
            return {
               "hostname": "unknown",  # Legacy files don't have hostname
               "pid": pid,
               "start_timestamp": None,
               "working_directory": None,
               "user": None,
               "legacy": True
            }
         except ValueError:
            raise Exception("Invalid PID file format")
            
      except Exception as e:
         raise Exception(f"Failed to read daemon info from {pid_file}: {str(e)}")
   
   def _check_hostname_match(self, daemon_info: Dict[str, Any]) -> bool:
      """Check if daemon is running on current hostname"""
      if daemon_info.get('legacy', False):
         # For legacy files, we can't determine hostname
         return True  # Assume local for backward compatibility
      
      current_hostname = socket.gethostname()
      daemon_hostname = daemon_info.get('hostname')
      
      return current_hostname == daemon_hostname
   
   def _format_daemon_location_message(self, daemon_info: Dict[str, Any]) -> str:
      """Format message about daemon location for user"""
      if daemon_info.get('legacy', False):
         return (f"Daemon is running with PID {daemon_info['pid']} "
                f"(legacy PID file - hostname unknown)")
      
      lines = []
      lines.append(f"Daemon is running on {daemon_info['hostname']} (PID {daemon_info['pid']})")
      
      if daemon_info.get('user'):
         lines.append(f"Started by: {daemon_info['user']}")
      
      if daemon_info.get('start_timestamp'):
         try:
            # Use strptime for better compatibility with older Python versions
            start_time = datetime.strptime(daemon_info['start_timestamp'][:19], '%Y-%m-%dT%H:%M:%S')
            lines.append(f"Started at: {format_timestamp(start_time)}")
         except (ValueError, TypeError):
            pass
      
      if daemon_info.get('working_directory'):
         lines.append(f"Working directory: {daemon_info['working_directory']}")
      
      lines.append(f"Please SSH to {daemon_info['hostname']} to manage the daemon")
      
      return "\n".join(lines)
   
   def _start_daemon(self, args: argparse.Namespace) -> int:
      """Start the daemon"""
      pid_file = self._get_pid_file_path(args)
      
      # Check if daemon is already running
      if pid_file.exists():
         try:
            daemon_info = self._read_daemon_info(pid_file)
            if daemon_info:
               pid = daemon_info['pid']
               
               # Check if process is still running
               try:
                  os.kill(pid, 0)  # Signal 0 just checks if process exists
                  
                  # Check if it's running on this host
                  if self._check_hostname_match(daemon_info):
                     print(f"Daemon already running with PID {pid}")
                  else:
                     print(self._format_daemon_location_message(daemon_info))
                  return 1
               except OSError:
                  # Process doesn't exist, remove stale PID file
                  pid_file.unlink()
                  print("Removed stale PID file")
         except Exception as e:
            # Invalid PID file, remove it
            self.logger.warning(f"Invalid PID file: {str(e)}")
            pid_file.unlink()
            print("Removed invalid PID file")
      
      print("Starting PBS Monitor daemon...")
      
      # Check database availability
      try:
         from ..data_collector import DataCollector
         collector = DataCollector(self.config)
         if not collector.database_enabled:
            print("Error: Database not enabled. Daemon requires database functionality.")
            print("Please run 'pbs-monitor database init' first.")
            return 1
      except Exception as e:
         print(f"Error: Failed to initialize data collector: {str(e)}")
         return 1
      
      if hasattr(args, 'detach') and args.detach:
         # Fork to background
         if os.fork() > 0:
            # Parent process exits
            print(f"Daemon started in background. PID file: {pid_file}")
            return 0
         
         # Child process continues
         os.setsid()  # Create new session
         os.chdir('/')  # Change to root directory
         
         # Redirect stdout/stderr to prevent issues
         sys.stdout.flush()
         sys.stderr.flush()
         
         # Close file descriptors
         with open('/dev/null', 'r') as devnull:
            os.dup2(devnull.fileno(), sys.stdin.fileno())
         with open('/dev/null', 'w') as devnull:
            os.dup2(devnull.fileno(), sys.stdout.fileno())
            os.dup2(devnull.fileno(), sys.stderr.fileno())
      
      # Write daemon info file
      try:
         self._write_daemon_info(pid_file, os.getpid())
      except Exception as e:
         print(f"Error: Failed to write daemon info: {str(e)}")
         return 1
      
      # Set up signal handlers for graceful shutdown
      def signal_handler(signum, frame):
         print(f"Received signal {signum}, shutting down...")
         if pid_file.exists():
            pid_file.unlink()
         collector.stop_background_updates()
         sys.exit(0)
      
      signal.signal(signal.SIGTERM, signal_handler)
      signal.signal(signal.SIGINT, signal_handler)
      
      try:
         # Enable auto-persist for daemon mode
         self.config.database.auto_persist = True
         
         # Start background updates
         collector.start_background_updates()
         
         if not (hasattr(args, 'detach') and args.detach):
            print("Daemon running in foreground. Press Ctrl+C to stop.")
            print(f"PID file: {pid_file}")
         
         # Keep the main thread alive
         try:
            while True:
               time.sleep(1)
         except KeyboardInterrupt:
            print("\nStopping daemon...")
            collector.stop_background_updates()
            
      finally:
         # Clean up PID file
         if pid_file.exists():
            pid_file.unlink()
      
      return 0
   
   def _stop_daemon(self, args: argparse.Namespace) -> int:
      """Stop the daemon"""
      pid_file = self._get_pid_file_path(args)
      
      if not pid_file.exists():
         print("Daemon is not running (no PID file found)")
         return 1
      
      try:
         daemon_info = self._read_daemon_info(pid_file)
         if not daemon_info:
            print("Daemon is not running (no PID file found)")
            return 1
         
         pid = daemon_info['pid']
         
         # Check if daemon is running on this host
         if not self._check_hostname_match(daemon_info):
            print(self._format_daemon_location_message(daemon_info))
            return 1
         
         print(f"Stopping daemon with PID {pid}...")
         
         # Send SIGTERM for graceful shutdown
         try:
            os.kill(pid, signal.SIGTERM)
            
            # Wait for process to exit
            for i in range(30):  # Wait up to 30 seconds
               try:
                  os.kill(pid, 0)  # Check if process still exists
                  time.sleep(1)
               except OSError:
                  # Process has exited
                  break
            else:
               # Process still running, force kill
               print("Process didn't exit gracefully, forcing shutdown...")
               os.kill(pid, signal.SIGKILL)
            
            print("Daemon stopped successfully")
            
            # Remove PID file
            if pid_file.exists():
               pid_file.unlink()
            
            return 0
            
         except OSError as e:
            if e.errno == 3:  # No such process
               print("Daemon was not running (stale PID file)")
               pid_file.unlink()
               return 0
            else:
               print(f"Error stopping daemon: {str(e)}")
               return 1
         
      except Exception as e:
         print(f"Error reading daemon info: {str(e)}")
         return 1
   
   def _show_daemon_status(self, args: argparse.Namespace) -> int:
      """Show daemon status"""
      pid_file = self._get_pid_file_path(args)
      
      print("PBS Monitor Daemon Status")
      print("=" * 50)
      
      # Check daemon process
      if pid_file.exists():
         try:
            daemon_info = self._read_daemon_info(pid_file)
            if daemon_info:
               pid = daemon_info['pid']
               
               try:
                  os.kill(pid, 0)  # Check if process exists
                  
                  # Check if daemon is running on this host
                  if self._check_hostname_match(daemon_info):
                     print(f"Status: Running (PID {pid})")
                     print(f"Hostname: {daemon_info.get('hostname', 'unknown')}")
                     if daemon_info.get('user'):
                        print(f"Started by: {daemon_info['user']}")
                     if daemon_info.get('start_timestamp'):
                        try:
                           # Use strptime for better compatibility with older Python versions
                           start_time = datetime.strptime(daemon_info['start_timestamp'][:19], '%Y-%m-%dT%H:%M:%S')
                           print(f"Started at: {format_timestamp(start_time)}")
                        except (ValueError, TypeError):
                           pass
                     if daemon_info.get('working_directory'):
                        print(f"Working directory: {daemon_info['working_directory']}")
                  else:
                     print("Status: Running on different host")
                     print(self._format_daemon_location_message(daemon_info))
                     
               except OSError:
                  print("Status: Not running (stale PID file)")
                  if not daemon_info.get('legacy', False):
                     print(f"Last known host: {daemon_info.get('hostname', 'unknown')}")
            else:
               print("Status: Not running (invalid PID file)")
         except Exception as e:
            print(f"Status: Not running (error reading PID file: {str(e)})")
      else:
         print("Status: Not running")
      
      print(f"PID file: {pid_file}")
      
      # Show configuration
      print(f"\nConfiguration:")
      print(f"  Database enabled: {hasattr(self.config, 'database')}")
      if hasattr(self.config, 'database'):
         print(f"  Database URL: {self.config.database.url}")
         print(f"  Auto-persist: {self.config.database.auto_persist}")
         print(f"  Daemon enabled: {self.config.database.daemon_enabled}")
         print(f"  Job collection interval: {self.config.database.job_collection_interval}s")
         print(f"  Node collection interval: {self.config.database.node_collection_interval}s")
         print(f"  Queue collection interval: {self.config.database.queue_collection_interval}s")
      
      # Show recent collection activity
      try:
         from ..data_collector import DataCollector
         collector = DataCollector(self.config)
         if collector.database_enabled:
            collection_repo = collector._repository_factory.get_data_collection_repository()
            recent_collections = collection_repo.get_recent_collections(hours=24)
            
            print(f"\nRecent Collection Activity (last 24 hours):")
            if recent_collections:
               for log_entry in recent_collections[:10]:  # Show last 10
                  # Now working with dictionaries instead of ORM objects
                  status = log_entry['status']
                  timestamp = log_entry['timestamp']
                  collection_type = log_entry['collection_type'] or "unknown"
                  duration = log_entry['duration_seconds']
                  jobs = log_entry['jobs_collected']
                  queues = log_entry['queues_collected']
                  nodes = log_entry['nodes_collected']
                  entities = jobs + queues + nodes
                  
                  status_symbol = "✓" if status == "SUCCESS" else "✗"
                  print(f"  {status_symbol} {format_timestamp(timestamp)} - "
                        f"{collection_type} - "
                        f"{entities} entities - "
                        f"{duration:.1f}s")
            else:
               print("  No recent collection activity")
      except Exception as e:
         print(f"\nError getting collection status: {str(e)}")
      
      return 0


class AnalyzeCommand(BaseCommand):
   """Command for running analytics analysis"""
   
   def execute(self, args: argparse.Namespace) -> int:
      """Execute the analyze command"""
      if args.analyze_action == "run-score":
         return self._analyze_run_score(args)
      else:
         self.logger.error(f"Unknown analyze action: {args.analyze_action}")
         return 1
   
   def _analyze_run_score(self, args: argparse.Namespace) -> int:
      """Analyze job scores at queue → run transitions"""
      try:
         from ..analytics import RunScoreAnalyzer
         
         # Initialize analyzer
         analyzer = RunScoreAnalyzer()
         
         # Get analysis period
         days = getattr(args, 'days', 30)
         
         # Perform analysis
         self.console.print(f"[bold blue]Analyzing job scores for queue → run transitions (last {days} days)...[/bold blue]")
         
         df = analyzer.analyze_transition_scores(days=days)
         
         if df.empty:
            self.console.print("[yellow]No transition data found for the specified period.[/yellow]")
            return 0
         
         # Get summary statistics
         summary = analyzer.get_analysis_summary(days=days)
         
         # Display results
         self._display_run_score_results(df, summary, args)
         
         return 0
         
      except Exception as e:
         self.logger.error(f"Error analyzing run scores: {str(e)}")
         self.console.print(f"[red]Error: {str(e)}[/red]")
         return 1
   
   def _display_run_score_results(self, df: pd.DataFrame, summary: Dict[str, Any], args: argparse.Namespace) -> None:
      """Display run score analysis results"""
      
            # Show summary
      self.console.print(f"\n[bold green]Job Score Analysis Summary[/bold green]")
      self.console.print(f"Analysis Period: {summary['analysis_period_days']} days")
      self.console.print(f"Total Finished Jobs: {summary['total_finished_jobs']}")
      self.console.print(f"Successful Score Calculations: {summary['successful_score_calculations']}")
      
      # Format output based on requested format
      output_format = getattr(args, 'format', 'table')
      
      if output_format == 'csv':
         self._display_csv_output(df)
      else:
         self._display_table_output(df)
   
   def _display_table_output(self, df: pd.DataFrame) -> None:
      """Display results in table format"""
      
      # Prepare table data
      headers = ['Node Count'] + [col for col in df.columns if col != 'node_count' and not col.endswith('_count')]
      rows = []
      
      for _, row in df.iterrows():
         table_row = [row['node_count']]
         for col in headers[1:]:  # Skip 'Node Count' header
            table_row.append(row[col])
         rows.append(table_row)
      
      # Create and display table
      table = self._create_table(
         title="Job Score Analysis: Queue → Run Transition",
         headers=headers,
         rows=rows
      )
      
      self.console.print(table)
      
      # Add note about data interpretation
      self.console.print(f"\n[dim]Note: Values show Average Score ± Standard Deviation. Sample sizes vary by bin.[/dim]")
   
   def _display_csv_output(self, df: pd.DataFrame) -> None:
      """Display results in CSV format"""
      
      # Remove count columns for CSV output
      csv_df = df.drop(columns=[col for col in df.columns if col.endswith('_count')])
      
      # Output CSV
      self.console.print(csv_df.to_csv(index=False))


class ReservationsCommand(BaseCommand):
   """Handle reservation listing and details"""
   
   def execute(self, args: argparse.Namespace) -> int:
      if args.reservation_action is None:
         print("Error: No reservation action specified")
         print("\nAvailable reservation actions:")
         print("  list         List reservations with summary information")
         print("  show         Show detailed reservation information")
         print("\nExamples:")
         print("  pbs-monitor resv list                    # List all reservations")
         print("  pbs-monitor resv list -u myuser          # List user's reservations")
         print("  pbs-monitor resv show S123456            # Show specific reservation")
         print("  pbs-monitor resv show                    # Show all reservations with details")
         print("\nUse 'pbs-monitor resv <action> --help' for more information about each action")
         return 1
      elif args.reservation_action == "list":
         return self._list_reservations(args)
      elif args.reservation_action == "show":
         return self._show_reservation_details(args)
      else:
         print(f"Unknown reservation action: {args.reservation_action}")
         print("\nAvailable actions: list, show")
         return 1
   
   def _list_reservations(self, args: argparse.Namespace) -> int:
      """List reservations with summary information"""
      try:
         # Force refresh if requested
         if args.refresh:
            self.collector.refresh_all()
         
         # Get reservations
         reservations = self.collector.get_reservations(force_refresh=args.refresh)
         
         if args.collect:
            # Collect and persist data to database
            try:
               result = self.collector.collect_and_persist("cli")
               self.logger.info(f"Collected {result.get('reservations_collected', 0)} reservations to database")
            except Exception as e:
               self.logger.error(f"Failed to collect reservation data: {str(e)}")
               print(f"Warning: Data collection failed: {str(e)}")
         
         # Apply filters
         filtered_reservations = self._filter_reservations(reservations, args)
         
         if not filtered_reservations:
            print("No reservations found matching criteria")
            return 0
         
         # Display table
         self._display_reservations_table(filtered_reservations, args)
         return 0
         
      except Exception as e:
         self.logger.error(f"Failed to list reservations: {str(e)}")
         print(f"Error: {str(e)}")
         return 1
   
   def _show_reservation_details(self, args: argparse.Namespace) -> int:
      """Show detailed information for specific reservation(s)"""
      try:
         if args.reservation_ids:
            # Show specific reservations
            all_reservations = self.collector.get_reservations()
            reservations = []
            seen_ids = set()
            for res_id in args.reservation_ids:
               # Find reservations that match (partial matches allowed) from live PBS
               matches = [r for r in all_reservations if res_id in r.reservation_id or r.reservation_id.startswith(res_id)]
               for m in matches:
                  if m.reservation_id not in seen_ids:
                     reservations.append(m)
                     seen_ids.add(m.reservation_id)
               
               # If not found via PBS, attempt database fallback for completed/archived reservations
               if not matches and getattr(self.collector, 'database_enabled', False):
                  try:
                     repo = self.collector._repository_factory.get_reservation_repository()
                     converters = self.collector._model_converters
                     db_found = []
                     # 1) Exact ID match
                     db_res = repo.get_reservation_by_id(res_id)
                     if db_res:
                        db_found = [db_res]
                     else:
                        # 2) Try recent reservations first, then broader historical window
                        recent = repo.get_recent_reservations(limit=1000)
                        db_found = [r for r in recent if res_id in r.reservation_id or r.reservation_id.startswith(res_id)]
                        if not db_found:
                           historical = repo.get_historical_reservations(days=365)
                           db_found = [r for r in historical if res_id in r.reservation_id or r.reservation_id.startswith(res_id)]
                     
                     if db_found:
                        for db_r in db_found:
                           pbs_r = converters.reservation.from_database(db_r)
                           if pbs_r.reservation_id not in seen_ids:
                              reservations.append(pbs_r)
                              seen_ids.add(pbs_r.reservation_id)
                     else:
                        print(f"Warning: Could not find reservation {res_id}")
                  except Exception as e:
                     self.logger.warning(f"DB fallback failed for reservation {res_id}: {e}")
                     print(f"Warning: Could not find reservation {res_id}")
               elif not matches:
                  print(f"Warning: Could not find reservation {res_id}")
         else:
            # Show all reservations with details
            reservations = self.collector.get_reservations()
         
         if not reservations:
            print("No reservations found")
            return 0
         
         # Display detailed information
         self._display_reservation_details(reservations, args)
         return 0
         
      except Exception as e:
         self.logger.error(f"Failed to show reservation details: {str(e)}")
         print(f"Error: {str(e)}")
         return 1
   
   def _filter_reservations(self, reservations: List[PBSReservation], args: argparse.Namespace) -> List[PBSReservation]:
      """Apply filters to reservations list"""
      filtered = reservations
      
      # Filter by user
      if hasattr(args, 'user') and args.user:
         filtered = [r for r in filtered if r.owner == args.user]
      
      # Filter by state
      if hasattr(args, 'state') and args.state:
         filtered = [r for r in filtered if args.state.upper() in [r.state.value, r.state.name]]
      
      return filtered
   
   def _display_reservations_table(self, reservations: List[PBSReservation], args: argparse.Namespace):
      """Display reservations in table format"""
      if args.format == "json":
         self._display_reservations_json(reservations)
         return
      
      # Default columns for reservation list
      default_columns = ["reservation_id", "name", "owner", "state", "type", "start_time", "duration", "nodes"]
      
      # Get columns from args or use defaults
      if hasattr(args, 'columns') and args.columns:
         columns = [col.strip() for col in args.columns.split(',')]
      else:
         columns = default_columns
      
      # Prepare table data
      headers = []
      for col in columns:
         if col == "reservation_id":
            headers.append("Reservation ID")
         elif col == "name":
            headers.append("Name")
         elif col == "owner":
            headers.append("Owner")
         elif col == "state":
            headers.append("State")
         elif col == "start_time":
            headers.append("Start Time")
         elif col == "duration":
            headers.append("Duration")
         elif col == "nodes":
            headers.append("Nodes")
         elif col == "queue":
            headers.append("Queue")
         elif col == "end_time":
            headers.append("End Time")
         elif col == "type":
            headers.append("Type")
         else:
            headers.append(col.title())
      
      rows = []
      for reservation in reservations:
         row = []
         for col in columns:
            if col == "reservation_id":
               # Truncate long reservation IDs for display
               res_id = reservation.reservation_id
               if len(res_id) > 25:
                  row.append(res_id[:22] + "...")
               else:
                  row.append(res_id)
            elif col == "name":
               row.append(reservation.reservation_name or "")
            elif col == "owner":
               row.append(reservation.owner or "")
            elif col == "state":
               # Use short form if available, otherwise full form
               if reservation.state in [ReservationState.RUNNING_SHORT, ReservationState.CONFIRMED_SHORT]:
                  row.append(reservation.state.value)
               else:
                  row.append(reservation.state.value.replace("RESV_", ""))
            elif col == "start_time":
               if reservation.start_time:
                  row.append(format_timestamp(reservation.start_time))
               else:
                  row.append("")
            elif col == "duration":
               if reservation.duration_seconds:
                  row.append(format_duration(reservation.duration_seconds))
               else:
                  row.append("")
            elif col == "nodes":
               row.append(str(reservation.nodes) if reservation.nodes else "")
            elif col == "queue":
               row.append(reservation.queue or "")
            elif col == "end_time":
               if reservation.end_time:
                  row.append(format_timestamp(reservation.end_time))
               else:
                  row.append("")
            elif col == "type":
               row.append(reservation.reservation_type)
            else:
               # Generic attribute access
               value = getattr(reservation, col, "")
               row.append(str(value) if value is not None else "")
         
         rows.append(row)
      
      # Create and display table
      table = self._create_table(
         title=f"PBS Reservations ({len(reservations)} found)",
         headers=headers,
         rows=rows
      )
      
      self.console.print(table)
   
   def _display_reservations_json(self, reservations: List[PBSReservation]):
      """Display reservations in JSON format"""
      import json
      reservation_data = []
      
      for reservation in reservations:
         data = {
            'reservation_id': reservation.reservation_id,
            'reservation_name': reservation.reservation_name,
            'owner': reservation.owner,
            'state': reservation.state.value,
            'queue': reservation.queue,
            'nodes': reservation.nodes,
            'ncpus': reservation.ncpus,
            'ngpus': reservation.ngpus,
            'start_time': reservation.start_time.isoformat() if reservation.start_time else None,
            'end_time': reservation.end_time.isoformat() if reservation.end_time else None,
            'duration_seconds': reservation.duration_seconds,
            'walltime': reservation.walltime,
            'authorized_users': reservation.authorized_users,
            'authorized_groups': reservation.authorized_groups,
            'server': reservation.server,
            'partition': reservation.partition
         }
         reservation_data.append(data)
      
      print(json.dumps(reservation_data, indent=2))
   
   def _display_reservation_details(self, reservations: List[PBSReservation], args: argparse.Namespace):
      """Display detailed reservation information"""
      if args.format == "json":
         self._display_reservations_json(reservations)
         return
      elif args.format == "yaml":
         self._display_reservations_yaml(reservations)
         return
      
      # Table format (default)
      for i, reservation in enumerate(reservations):
         if i > 0:
            print()  # Blank line between reservations
         
         self._display_single_reservation_details(reservation)
   
   def _display_single_reservation_details(self, reservation: PBSReservation):
      """Display detailed information for a single reservation"""
      
      # Main information table
      info_rows = [
         ["Reservation ID", reservation.reservation_id],
         ["Name", reservation.reservation_name or ""],
         ["Owner", reservation.owner or ""],
         ["State", reservation.state.value],
         ["Type", reservation.reservation_type],
         ["Queue", reservation.queue or ""],
      ]
      
      # Timing information
      if reservation.start_time:
         info_rows.append(["Start Time", format_timestamp(reservation.start_time)])
      if reservation.end_time:
         info_rows.append(["End Time", format_timestamp(reservation.end_time)])
      if reservation.duration_seconds:
         info_rows.append(["Duration", format_duration(reservation.duration_seconds)])
      
      # Resource information
      if reservation.nodes:
         info_rows.append(["Nodes", str(reservation.nodes)])
      if reservation.ncpus:
         info_rows.append(["CPUs", f"{reservation.ncpus:,}"])
      if reservation.ngpus:
         info_rows.append(["GPUs", f"{reservation.ngpus:,}"])
      if reservation.walltime:
         info_rows.append(["Walltime", reservation.walltime])
      
      # Additional metadata
      if reservation.authorized_users:
         info_rows.append(["Authorized Users", ", ".join(reservation.authorized_users)])
      if reservation.authorized_groups:
         info_rows.append(["Authorized Groups", ", ".join(reservation.authorized_groups)])
      if reservation.server:
         info_rows.append(["Server", reservation.server])
      if reservation.partition:
         info_rows.append(["Partition", reservation.partition])
      
      # Creation/modification times
      if reservation.creation_time:
         info_rows.append(["Created", format_timestamp(reservation.creation_time)])
      if reservation.modification_time:
         info_rows.append(["Modified", format_timestamp(reservation.modification_time)])
      
      # Create table
      table = self._create_table(
         title=f"Reservation Details: {reservation.reservation_name or reservation.reservation_id[:30] + '...'}",
         headers=["Property", "Value"],
         rows=info_rows
      )
      
      self.console.print(table)
      
      # Show recurring windows for recurring reservations
      if reservation.is_recurring:
         windows = reservation.get_recurring_windows()
         if windows:
            self.console.print(f"\n[bold]Recurring Reservation Windows:[/bold]")
            
            window_rows = []
            for window in windows:
               start_str = format_timestamp(window['start_time']) if window['start_time'] else ""
               end_str = format_timestamp(window['end_time']) if window['end_time'] else ""
               duration_str = format_duration(window['duration_seconds']) if window['duration_seconds'] else ""
               
               # Mark current window
               index_str = str(window['index'])
               if window.get('is_current'):
                  index_str += " (current)"
               
               window_rows.append([
                  index_str,
                  start_str,
                  end_str,
                  duration_str
               ])
            
            windows_table = self._create_table(
               title=f"All {len(windows)} Reservation Windows",
               headers=["Window", "Start Time", "End Time", "Duration"],
               rows=window_rows
            )
            self.console.print(windows_table)
      
      # Show reserved nodes if available (truncated for display)
      if reservation.reserved_nodes:
         nodes_display = reservation.reserved_nodes
         if len(nodes_display) > 200:
            nodes_display = nodes_display[:200] + "... (truncated)"
         
         self.console.print(f"\n[bold]Reserved Nodes:[/bold]")
         self.console.print(f"[dim]{nodes_display}[/dim]")
   
   def _display_reservations_yaml(self, reservations: List[PBSReservation]):
      """Display reservations in YAML format"""
      import yaml
      
      reservation_data = []
      for reservation in reservations:
         data = {
            'reservation_id': reservation.reservation_id,
            'reservation_name': reservation.reservation_name,
            'owner': reservation.owner,
            'state': reservation.state.value,
            'queue': reservation.queue,
            'resources': {
               'nodes': reservation.nodes,
               'ncpus': reservation.ncpus,
               'ngpus': reservation.ngpus,
               'walltime': reservation.walltime
            },
            'timing': {
               'start_time': reservation.start_time.isoformat() if reservation.start_time else None,
               'end_time': reservation.end_time.isoformat() if reservation.end_time else None,
               'duration_seconds': reservation.duration_seconds
            },
            'access_control': {
               'authorized_users': reservation.authorized_users,
               'authorized_groups': reservation.authorized_groups
            },
            'metadata': {
               'server': reservation.server,
               'partition': reservation.partition,
               'creation_time': reservation.creation_time.isoformat() if reservation.creation_time else None,
               'modification_time': reservation.modification_time.isoformat() if reservation.modification_time else None
            }
         }
         reservation_data.append(data)
      
      print(yaml.dump(reservation_data, default_flow_style=False))