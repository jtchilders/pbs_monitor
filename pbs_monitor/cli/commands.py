"""
Command implementations for PBS Monitor CLI
"""

import argparse
import logging
from typing import List, Dict, Any, Optional
from datetime import datetime, timedelta, timezone
from abc import ABC, abstractmethod

from sqlalchemy import and_, or_

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
from ..utils.json_helpers import load_json_safe
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
   
   def _parse_walltime_to_hours(self, walltime: str) -> float:
      """
      Parse walltime string to hours
      
      Args:
         walltime: Walltime string in format HH:MM:SS or DD:HH:MM:SS
         
      Returns:
         Walltime in hours as float
      """
      if not walltime:
         return 0.0
      
      try:
         parts = walltime.split(':')
         if len(parts) == 3:
            # HH:MM:SS format
            hours, minutes, seconds = map(int, parts)
            return hours + minutes / 60.0 + seconds / 3600.0
         elif len(parts) == 4:
            # DD:HH:MM:SS format
            days, hours, minutes, seconds = map(int, parts)
            return days * 24 + hours + minutes / 60.0 + seconds / 3600.0
         else:
            return 0.0
      except (ValueError, TypeError):
         return 0.0

   def _calculate_node_hours(self, job: PBSJob) -> float:
      """Calculate requested node-hours for a job"""
      if not job.nodes or not job.walltime:
         return 0.0
      
      walltime_hours = self._parse_walltime_to_hours(job.walltime)
      return job.nodes * walltime_hours
   
   def _calculate_current_queue_seconds(self, job: PBSJob) -> int:
      """Calculate current queue time in seconds for sorting"""
      from datetime import datetime
      
      if not job.submit_time:
         return -1
      
      # For completed/running jobs, use queue_time_seconds if available
      if job.queue_time_seconds is not None:
         return job.queue_time_seconds
      
      # For jobs still in queue, calculate against current time
      if job.state.value in ['Q', 'H', 'W']:  # Queued, Held, or Waiting states
         now = datetime.now(job.submit_time.tzinfo)  # Use same timezone as submit_time
         queue_duration = now - job.submit_time
         return int(queue_duration.total_seconds())
      
      return -1

   def _format_queue_time(self, job: PBSJob) -> str:
      """Format queue time, using current time for jobs still in queue"""
      seconds = self._calculate_current_queue_seconds(job)
      if seconds >= 0:
         return format_duration(seconds)
      return "N/A"


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
         project=getattr(args, 'project', None),
         queue=getattr(args, 'queue', None),
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
         'score': lambda j: j.score if j.score is not None else -1,  # Put jobs without scores at the end
         'queue_time': lambda j: self._calculate_current_queue_seconds(j),
         'node_hours': lambda j: self._calculate_node_hours(j)
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
         'queue_time': lambda j: self._format_queue_time(j),
         'node_hours': lambda j: f"{self._calculate_node_hours(j):.1f}"
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
      
      # Print summary statistics
      self._print_job_summary_statistics(jobs)
      
      # Handle database collection if requested
      self._handle_collection_if_requested(args)
      
      return 0
   
   def _print_job_summary_statistics(self, jobs: List[PBSJob]) -> None:
      """Print summary statistics for jobs"""
      from collections import defaultdict
      
      if not jobs:
         return
      
      print(f"\nJob Summary Statistics")
      print("=" * 50)
      
      # Calculate basic statistics
      total_jobs = len(jobs)
      total_node_hours = sum(self._calculate_node_hours(job) for job in jobs)
      
      # Group by queue
      queue_stats = defaultdict(lambda: {'count': 0, 'node_hours': 0.0})
      for job in jobs:
         queue_stats[job.queue]['count'] += 1
         queue_stats[job.queue]['node_hours'] += self._calculate_node_hours(job)
      
      # Group by state
      state_stats = defaultdict(lambda: {'count': 0, 'node_hours': 0.0})
      for job in jobs:
         state_stats[job.state.value]['count'] += 1
         state_stats[job.state.value]['node_hours'] += self._calculate_node_hours(job)
      
      # Print overall summary
      print(f"Total Jobs: {total_jobs}")
      print(f"Total Node-Hours Requested: {total_node_hours:.1f}")
      
      # Print jobs per queue
      print(f"\nJobs by Queue:")
      for queue, stats in sorted(queue_stats.items()):
         print(f"  {queue}: {stats['count']} jobs ({stats['node_hours']:.1f} node-hours)")
      
      # Print jobs per state  
      print(f"\nJobs by State:")
      for state, stats in sorted(state_stats.items()):
         print(f"  {state}: {stats['count']} jobs ({stats['node_hours']:.1f} node-hours)")

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
      
      # Determine output format based on flags
      # Priority: --json > -d/--detailed > table (default)
      if getattr(args, 'json', False):
         return self._show_job_details_json(all_jobs, args)
      elif getattr(args, 'detailed', False):
         return self._show_job_details_detailed(all_jobs, args)
      else:
         return self._show_job_details_table(all_jobs, args)
   
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
         'execution_node': lambda j: j.execution_node or "N/A",
         'node_hours': lambda j: f"{self._calculate_node_hours(j):.1f}"
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
               "memory": job.memory,
               "node_hours_requested": self._calculate_node_hours(job)
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
      print(f"  Node-Hours Requested: {self._calculate_node_hours(job):.1f}")
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
         # Get node names from arguments if provided
         node_names = args.node_ids if args.node_ids else None
         
         # Get nodes (optionally filtered by node names)
         nodes = self.collector.get_nodes(force_refresh=args.refresh, node_names=node_names)
         
         # Filter by state if specified
         if args.state:
            nodes = [node for node in nodes if node.state.value == args.state]
         
         if not nodes:
            if node_names:
               print(f"No nodes found matching: {', '.join(node_names)}")
            else:
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
      
      if resources['total_memory_gb'] > 0:
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
            'total_memory_gb': total_memory_gb / 1024 if total_memory_gb > 0 else 0.0,  # Convert to TB
            'used_memory_gb': used_memory_gb / 1024 if total_memory_gb > 0 else 0.0,    # Convert to TB
            'available_memory_gb': (total_memory_gb - used_memory_gb) / 1024 if total_memory_gb > 0 else 0.0,
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
         
         if summary_stats['resources']['total_memory_gb'] > 0:
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
            print("  init            Initialize database schema")
            print("  migrate         Migrate database to latest schema")
            print("  status          Show database status and information")
            print("  validate        Validate database schema and data")
            print("  backup          Create database backup")
            print("  restore         Restore database from backup")
            print("  cleanup         Clean up old data from database")
            print("  show            Show table data from database")
            print("  refresh-cache   Recalculate derived/analytical cache tables")
            print("\nExamples:")
            print("  pbs-monitor database init                              # Initialize database")
            print("  pbs-monitor database status                            # Show database status")
            print("  pbs-monitor database backup                            # Create backup")
            print("  pbs-monitor database cleanup --days 30                 # Clean up old data")
            print("  pbs-monitor database show -t jobs -a 10               # Show last 10 rows from jobs table")
            print("  pbs-monitor database refresh-cache                     # Refresh all cache tables")
            print("  pbs-monitor database refresh-cache -t utilization      # Refresh reservation utilization only")
            print("  pbs-monitor database refresh-cache -d 7 --dry-run     # Preview last 7 days without writing")
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
         elif subcommand == 'show':
            return self._show_table_data(args)
         elif subcommand == 'refresh-cache':
            return self._refresh_cache(args)
         else:
            print(f"Unknown database subcommand: {subcommand}")
            print("\nAvailable actions: init, migrate, status, validate, backup, restore, cleanup, show, refresh-cache")
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
   
   def _show_table_data(self, args: argparse.Namespace) -> int:
      """Show table data from database"""
      try:
         # Validate arguments
         if not self._validate_show_arguments(args):
            return 1
         
         # Get table data
         table_data = self._query_table_data(args)
         
         if not table_data:
            print(f"No data found in table '{args.table}'")
            return 0
         
         # Display data
         if args.format == "csv":
            self._display_csv_output(table_data)
         else:
            self._display_table_output(table_data)
         
         return 0
         
      except Exception as e:
         self.logger.error(f"Failed to show table data: {str(e)}")
         print(f"Error: {str(e)}")
         return 1
   
   def _validate_show_arguments(self, args: argparse.Namespace) -> bool:
      """Validate show command arguments"""
      # Check that only one query type is specified
      query_types = []
      if args.after is not None:
         query_types.append("after")
      if args.before is not None:
         query_types.append("before")
      if args.start is not None or args.num_rows is not None:
         query_types.append("range")
      
      if len(query_types) > 1:
         print("Error: Only one query type can be specified. Use either:")
         print("  -a/--after for last N rows")
         print("  -b/--before for first N rows")
         print("  -s/--start and -n/--num-rows for range")
         return False
      
      if len(query_types) == 0:
         print("Error: Must specify one of:")
         print("  -a/--after for last N rows")
         print("  -b/--before for first N rows")
         print("  -s/--start and -n/--num-rows for range")
         return False
      
      # Check that both start and num_rows are provided for range queries
      if "range" in query_types:
         if args.start is None or args.num_rows is None:
            print("Error: Both -s/--start and -n/--num-rows must be specified for range queries")
            return False
         
         if args.start < 0:
            print("Error: Start row must be non-negative")
            return False
         
         if args.num_rows <= 0:
            print("Error: Number of rows must be positive")
            return False
      
      # Check that after/before values are positive
      if args.after is not None and args.after <= 0:
         print("Error: After value must be positive")
         return False
      
      if args.before is not None and args.before <= 0:
         print("Error: Before value must be positive")
         return False
      
      return True
   
   def _query_table_data(self, args: argparse.Namespace) -> List[Dict[str, Any]]:
      """Query table data based on arguments"""
      from ..database.connection import get_database_manager
      from sqlalchemy import text
      
      db_manager = get_database_manager(self.config)
      
      # Check if table exists
      if not db_manager.table_exists(args.table):
         raise ValueError(f"Table '{args.table}' does not exist")
      
      # Build query based on arguments
      if args.after is not None:
         # Last N rows
         query = f"SELECT * FROM {args.table} ORDER BY rowid DESC LIMIT {args.after}"
      elif args.before is not None:
         # First N rows
         query = f"SELECT * FROM {args.table} ORDER BY rowid ASC LIMIT {args.before}"
      else:
         # Range query
         query = f"SELECT * FROM {args.table} ORDER BY rowid ASC LIMIT {args.num_rows} OFFSET {args.start}"
      
      # Execute query
      with db_manager.get_session() as session:
         result = session.execute(text(query))
         rows = result.fetchall()
         
         # Convert to list of dictionaries
         if rows:
            columns = result.keys()
            return [dict(zip(columns, row)) for row in rows]
         else:
            return []
   
   def _display_csv_output(self, data: List[Dict[str, Any]]) -> None:
      """Display data in CSV format"""
      if not data:
         return
      
      # Get column names from first row
      columns = list(data[0].keys())
      
      # Print header
      print(",".join(columns))
      
      # Print data rows
      for row in data:
         values = []
         for col in columns:
            value = row.get(col, "")
            # Handle None values and escape commas
            if value is None:
               value = ""
            elif isinstance(value, str) and "," in value:
               value = f'"{value}"'
            values.append(str(value))
         print(",".join(values))
   
   def _display_table_output(self, data: List[Dict[str, Any]]) -> None:
      """Display data in table format"""
      if not data:
         return
      
      # Get column names from first row
      columns = list(data[0].keys())
      
      # Prepare data for tabulate
      table_data = []
      for row in data:
         values = []
         for col in columns:
            value = row.get(col, "")
            if value is None:
               value = ""
            values.append(str(value))
         table_data.append(values)
      
      # Display table
      from tabulate import tabulate
      print(tabulate(table_data, headers=columns, tablefmt="grid"))


   # ---- refresh-cache tables ----
   # Maps user-facing --table names to the SQL tables they recalculate.
   _CACHE_TABLE_MAP = {
      "utilization": "reservation_utilization",
   }

   def _refresh_cache(self, args: argparse.Namespace) -> int:
      """Recalculate derived/analytical cache tables used by the web dashboard."""

      table_arg = getattr(args, 'table', 'all')
      days = getattr(args, 'days', 14)
      limit = getattr(args, 'limit', None)
      dry_run = getattr(args, 'dry_run', False)
      force = getattr(args, 'force', False)

      # Determine which tables to refresh
      if table_arg == 'all':
         targets = list(self._CACHE_TABLE_MAP.keys())
      else:
         if table_arg not in self._CACHE_TABLE_MAP:
            valid = ', '.join(list(self._CACHE_TABLE_MAP.keys()) + ['all'])
            print(f"Error: Invalid table '{table_arg}'. Valid targets: {valid}")
            return 1
         targets = [table_arg]

      total_errors = 0

      for target in targets:
         if target == 'utilization':
            errors = self._refresh_utilization_cache(days, limit, dry_run, force)
            total_errors += errors

      return 1 if total_errors > 0 else 0

   def _refresh_utilization_cache(self, days: int, limit: int | None,
                                   dry_run: bool, force: bool) -> int:
      """Refresh the reservation_utilization cache table.

      Returns the number of errors encountered.
      """
      from ..analytics.reservation_analysis import ReservationUtilizationAnalyzer
      from ..database.models import Reservation as ReservationModel
      from ..database.repositories import RepositoryFactory

      print(f"Refreshing reservation_utilization (last {days} days)...")
      if dry_run:
         print("  [dry-run] No database writes will be performed.")

      repo_factory = RepositoryFactory()
      analyzer = ReservationUtilizationAnalyzer(repository_factory=repo_factory)

      if dry_run:
         analyzer._readonly_mode = True
      elif force:
         analyzer._readonly_mode = False

      # Identify target reservations
      cutoff = datetime.now(timezone.utc) - timedelta(days=days)

      with repo_factory.get_job_repository().get_session() as session:
         query = session.query(ReservationModel).filter(
            or_(
               ReservationModel.start_time >= cutoff,
               and_(
                  ReservationModel.end_time >= cutoff,
                  ReservationModel.start_time < cutoff,
               ),
            )
         ).order_by(ReservationModel.start_time.desc())

         if limit is not None:
            query = query.limit(limit)

         reservations = query.all()
         reservation_ids = [r.reservation_id for r in reservations]

      if not reservation_ids:
         print(f"  No reservations found in the last {days} days.")
         return 0

      print(f"  Found {len(reservation_ids)} reservation(s) to process.")

      updated = 0
      errors = 0

      for res_id in reservation_ids:
         try:
            result = analyzer.analyze_reservation_utilization(res_id)

            util_pct = result.get('utilization_percentage', 0)
            nh_reserved = result.get('total_node_hours_reserved', 0)
            nh_used = result.get('total_node_hours_used', 0)
            jobs_sub = result.get('jobs_submitted', 0)

            if dry_run:
               name = result.get('reservation_name') or ''
               print(f"  [dry-run] {res_id}: {util_pct:.1f}% "
                     f"({nh_used:.1f}/{nh_reserved:.1f} node-hrs, "
                     f"{jobs_sub} jobs) {name}")
            else:
               updated += 1

         except Exception as e:
            print(f"  Error processing {res_id}: {e}")
            errors += 1

      # Summary
      if dry_run:
         print(f"\n  [dry-run] Would update {len(reservation_ids) - errors} rows "
               f"in reservation_utilization.")
      else:
         print(f"\n  ✓ Refreshed {updated} reservation(s) in reservation_utilization.")

      if errors:
         print(f"  ✗ {errors} error(s) encountered.")

      return errors


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
               pbs_completed_jobs = self.collector.pbs_commands.qstat_completed_jobs(user=args.user, project=getattr(args, 'project', None))
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
         state_map = {"C": DBJobState.COMPLETED, "F": DBJobState.FINISHED, "E": DBJobState.EXITING, "UNKNOWN_END": DBJobState.UNKNOWN_END}
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
      
      # Apply project filter if specified
      if hasattr(args, 'project') and args.project:
         project_filter = args.project.lower()
         historical_jobs = [job for job in historical_jobs if job.project and project_filter in job.project.lower()]
      
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
         'runtime': lambda j: self._calculate_runtime_seconds(j),
         'queue_time': lambda j: self._calculate_current_queue_seconds(j)
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
      default_columns = ['job_id', 'name', 'owner', 'project', 'allocation', 'state', 'queue', 'nodes', 'walltime', 'node_hours', 'submit_time', 'queued', 'runtime', 'exit_status']
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
         'node_hours': lambda j: f"{self._calculate_node_hours(j):.1f}",
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
      current_time = datetime.now().isoformat()
      daemon_info = {
         "hostname": socket.gethostname(),
         "pid": pid,
         "start_timestamp": current_time,
         "working_directory": str(Path.cwd()),
         "user": getpass.getuser(),
         "heartbeat": current_time,
         "stop_requested": False,
         "exited": False
      }
      
      try:
         with open(pid_file, 'w') as f:
            json.dump(daemon_info, f, indent=2)
      except Exception as e:
         raise Exception(f"Failed to write daemon info to {pid_file}: {str(e)}")
   
   def _update_daemon_heartbeat(self, pid_file: Path) -> bool:
      """Update daemon heartbeat and check for stop request
      
      Returns:
         True if daemon should continue running, False if stop was requested
      """
      try:
         # Read current daemon info
         daemon_info = self._read_daemon_info(pid_file)
         if not daemon_info:
            self.logger.warning("PID file disappeared, daemon will exit")
            return False
         
         # Check if stop was requested
         if daemon_info.get('stop_requested', False):
            self.logger.info("Stop request detected, daemon will exit")
            return False
         
         # Update heartbeat
         daemon_info['heartbeat'] = datetime.now().isoformat()
         
         # Write back to file
         with open(pid_file, 'w') as f:
            json.dump(daemon_info, f, indent=2)
         
         return True
         
      except Exception as e:
         self.logger.error(f"Failed to update daemon heartbeat: {str(e)}")
         # Continue running on heartbeat errors to avoid unnecessary shutdowns
         return True
   
   def _mark_daemon_exited(self, pid_file: Path) -> None:
      """Mark daemon as exited in PID file before terminating"""
      try:
         daemon_info = self._read_daemon_info(pid_file)
         if daemon_info:
            daemon_info['exited'] = True
            daemon_info['heartbeat'] = datetime.now().isoformat()
            
            with open(pid_file, 'w') as f:
               json.dump(daemon_info, f, indent=2)
            
            self.logger.info("Marked daemon as exited in PID file")
         
      except Exception as e:
         self.logger.error(f"Failed to mark daemon as exited: {str(e)}")
         # Don't raise exception here as we're shutting down anyway
   
   def _is_daemon_stale(self, daemon_info: Dict[str, Any], heartbeat_timeout_minutes: int = 30) -> bool:
      """Check if daemon is stale based on heartbeat timestamp
      
      Args:
         daemon_info: Daemon information dictionary
         heartbeat_timeout_minutes: Minutes after which daemon is considered stale
         
      Returns:
         True if daemon appears stale, False otherwise
      """
      if not daemon_info:
         return True
      
      # Check if daemon marked itself as exited
      if daemon_info.get('exited', False):
         return True
      
      # Check heartbeat timestamp
      heartbeat_str = daemon_info.get('heartbeat')
      if not heartbeat_str:
         # No heartbeat info, assume stale if it's a legacy format
         return daemon_info.get('legacy', False)
      
      try:
         # Parse heartbeat timestamp
         heartbeat_time = datetime.strptime(heartbeat_str[:19], '%Y-%m-%dT%H:%M:%S')
         current_time = datetime.now()
         
         # Check if heartbeat is too old
         time_diff = current_time - heartbeat_time
         timeout_seconds = heartbeat_timeout_minutes * 60
         
         return time_diff.total_seconds() > timeout_seconds
         
      except (ValueError, TypeError):
         # Invalid heartbeat timestamp, consider stale
         return True
   
   def _read_daemon_info(self, pid_file: Path) -> Dict[str, Any]:
      """Read daemon information from PID file (JSON or legacy format)"""
      if not pid_file.exists():
         return None
      
      try:
         with open(pid_file, 'r') as f:
            content = f.read().strip()
         
         # Try to parse as JSON first
         try:
            daemon_info = load_json_safe(content, f"daemon_info_pid_{pid_file}")
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
               
               # Check if daemon marked itself as exited
               if daemon_info.get('exited', False):
                  print("Previous daemon has exited, cleaning up PID file...")
                  pid_file.unlink()
               # Check if daemon is stale
               elif self._is_daemon_stale(daemon_info):
                  print("Found stale daemon PID file, cleaning up...")
                  if daemon_info.get('heartbeat'):
                     try:
                        last_heartbeat = datetime.strptime(daemon_info['heartbeat'][:19], '%Y-%m-%dT%H:%M:%S')
                        print(f"Last heartbeat was: {format_timestamp(last_heartbeat)}")
                     except (ValueError, TypeError):
                        pass
                  pid_file.unlink()
                  print("Removed stale PID file")
               else:
                  # Check if process is still running
                  try:
                     os.kill(pid, 0)  # Signal 0 just checks if process exists
                     
                     # Check if it's running on this host
                     if self._check_hostname_match(daemon_info):
                        if daemon_info.get('stop_requested', False):
                           print(f"Daemon with PID {pid} is shutting down (stop requested)")
                           print("Wait for it to exit or use 'pbs-monitor daemon status' to check progress")
                        else:
                           print(f"Daemon already running with PID {pid}")
                     else:
                        print(self._format_daemon_location_message(daemon_info))
                     return 1
                  except OSError:
                     # Process doesn't exist, remove stale PID file
                     pid_file.unlink()
                     print("Removed stale PID file (process not found)")
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
      
      # Write initial daemon info file before detaching
      try:
         self._write_daemon_info(pid_file, os.getpid())
      except Exception as e:
         print(f"Error: Failed to write daemon info: {str(e)}")
         return 1
      
      # Detach by default unless explicitly disabled
      should_detach = not (hasattr(args, 'foreground') and args.foreground)
      
      if should_detach:
         # Fork to background
         if os.fork() > 0:
            # Parent process exits
            print(f"Daemon started in background. PID file: {pid_file}")
            return 0
         
         # Child process continues
         os.setsid()  # Create new session
         os.chdir('/')  # Change to root directory
         
         # Update PID file with correct working directory after detachment
         try:
            self._write_daemon_info(pid_file, os.getpid())
         except Exception as e:
            # Log error but don't exit - daemon can still function
            self.logger.error(f"Failed to update daemon info after detachment: {str(e)}")
         
         # Redirect stdout/stderr to prevent issues
         sys.stdout.flush()
         sys.stderr.flush()
         
         # Close file descriptors
         with open('/dev/null', 'r') as devnull:
            os.dup2(devnull.fileno(), sys.stdin.fileno())
         with open('/dev/null', 'w') as devnull:
            os.dup2(devnull.fileno(), sys.stdout.fileno())
            os.dup2(devnull.fileno(), sys.stderr.fileno())
      
      # Set up signal handlers for graceful shutdown
      def signal_handler(signum, frame):
         print(f"Received signal {signum}, shutting down...")
         self._mark_daemon_exited(pid_file)
         collector.stop_background_updates()
         sys.exit(0)
      
      signal.signal(signal.SIGTERM, signal_handler)
      signal.signal(signal.SIGINT, signal_handler)
      
      try:
         # Enable auto-persist for daemon mode
         self.config.database.auto_persist = True
         
         # Start background updates
         collector.start_background_updates()
         
         if not should_detach:
            print("Daemon running in foreground. Press Ctrl+C to stop.")
            print(f"PID file: {pid_file}")
         
         # Main daemon loop with heartbeat and stop checking
         heartbeat_interval = 600  # 10 minutes in seconds
         last_heartbeat = time.time()
         
         try:
            while True:
               current_time = time.time()
               
               # Check if it's time for heartbeat update
               if current_time - last_heartbeat >= heartbeat_interval:
                  if not self._update_daemon_heartbeat(pid_file):
                     # Stop was requested
                     print("Stop request detected, shutting down...")
                     break
                  last_heartbeat = current_time
               
               # Sleep for a short interval before checking again
               time.sleep(30)  # Check every 30 seconds
               
         except KeyboardInterrupt:
            print("\nStopping daemon...")
            
         # Stop background updates
         collector.stop_background_updates()
            
      finally:
         # Mark daemon as exited and clean up PID file
         self._mark_daemon_exited(pid_file)
      
      return 0
   
   def _stop_daemon(self, args: argparse.Namespace) -> int:
      """Stop the daemon by setting stop flag"""
      pid_file = self._get_pid_file_path(args)
      
      if not pid_file.exists():
         print("Daemon is not running (no PID file found)")
         return 1
      
      try:
         daemon_info = self._read_daemon_info(pid_file)
         if not daemon_info:
            print("Daemon is not running (no PID file found)")
            return 1
         
         # Check if daemon is already marked as exited
         if daemon_info.get('exited', False):
            print("Daemon has already exited")
            return 0
         
         # Check if daemon is stale
         if self._is_daemon_stale(daemon_info):
            print("Daemon appears to be stale (old heartbeat or marked as exited)")
            print("Cleaning up stale PID file...")
            pid_file.unlink()
            return 0
         
         # Check if stop was already requested
         if daemon_info.get('stop_requested', False):
            print("Stop request already pending. Check 'pbs-monitor daemon status' to see when daemon exits.")
            return 0
         
         pid = daemon_info['pid']
         print(f"Requesting daemon shutdown (PID {pid})...")
         
         # Set stop_requested flag
         daemon_info['stop_requested'] = True
         daemon_info['heartbeat'] = datetime.now().isoformat()
         
         try:
            with open(pid_file, 'w') as f:
               json.dump(daemon_info, f, indent=2)
            
            print("Stop request sent successfully.")
            print("The daemon will shutdown within the next 10 minutes (at next heartbeat check).")
            print("Use 'pbs-monitor daemon status' to monitor shutdown progress.")
            
            return 0
            
         except Exception as e:
            print(f"Error setting stop flag: {str(e)}")
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
               
               # Check if daemon marked itself as exited
               if daemon_info.get('exited', False):
                  print(f"Status: Exited (PID {pid})")
                  print(f"Hostname: {daemon_info.get('hostname', 'unknown')}")
                  if daemon_info.get('heartbeat'):
                     try:
                        exit_time = datetime.strptime(daemon_info['heartbeat'][:19], '%Y-%m-%dT%H:%M:%S')
                        print(f"Exited at: {format_timestamp(exit_time)}")
                     except (ValueError, TypeError):
                        pass
               # Check if daemon is stale
               elif self._is_daemon_stale(daemon_info):
                  print(f"Status: Stale (PID {pid})")
                  print(f"Hostname: {daemon_info.get('hostname', 'unknown')}")
                  if daemon_info.get('heartbeat'):
                     try:
                        last_heartbeat = datetime.strptime(daemon_info['heartbeat'][:19], '%Y-%m-%dT%H:%M:%S')
                        print(f"Last heartbeat: {format_timestamp(last_heartbeat)}")
                        
                        # Calculate how long since last heartbeat
                        time_since = datetime.now() - last_heartbeat
                        hours = int(time_since.total_seconds() // 3600)
                        minutes = int((time_since.total_seconds() % 3600) // 60)
                        print(f"Time since heartbeat: {hours}h {minutes}m")
                     except (ValueError, TypeError):
                        pass
                  print("Daemon appears to have crashed or been killed")
               else:
                  # Check if process is actually running
                  try:
                     os.kill(pid, 0)  # Check if process exists
                     
                     # Check if daemon is running on this host
                     if self._check_hostname_match(daemon_info):
                        # Check if stop was requested
                        if daemon_info.get('stop_requested', False):
                           print(f"Status: Stopping (PID {pid})")
                           print("Stop request pending - daemon will exit at next heartbeat")
                        else:
                           print(f"Status: Running (PID {pid})")
                        
                        print(f"Hostname: {daemon_info.get('hostname', 'unknown')}")
                        if daemon_info.get('user'):
                           print(f"Started by: {daemon_info['user']}")
                        if daemon_info.get('start_timestamp'):
                           try:
                              start_time = datetime.strptime(daemon_info['start_timestamp'][:19], '%Y-%m-%dT%H:%M:%S')
                              print(f"Started at: {format_timestamp(start_time)}")
                           except (ValueError, TypeError):
                              pass
                        if daemon_info.get('heartbeat'):
                           try:
                              last_heartbeat = datetime.strptime(daemon_info['heartbeat'][:19], '%Y-%m-%dT%H:%M:%S')
                              print(f"Last heartbeat: {format_timestamp(last_heartbeat)}")
                              
                              # Calculate time since last heartbeat
                              time_since = datetime.now() - last_heartbeat
                              minutes = int(time_since.total_seconds() // 60)
                              print(f"Minutes since heartbeat: {minutes}")
                           except (ValueError, TypeError):
                              pass
                        if daemon_info.get('working_directory'):
                           print(f"Working directory: {daemon_info['working_directory']}")
                     else:
                        print("Status: Running on different host")
                        print(self._format_daemon_location_message(daemon_info))
                        
                  except OSError:
                     print(f"Status: Not running (process {pid} not found)")
                     if not daemon_info.get('legacy', False):
                        print(f"Last known host: {daemon_info.get('hostname', 'unknown')}")
                        if daemon_info.get('heartbeat'):
                           try:
                              last_heartbeat = datetime.strptime(daemon_info['heartbeat'][:19], '%Y-%m-%dT%H:%M:%S')
                              print(f"Last heartbeat: {format_timestamp(last_heartbeat)}")
                           except (ValueError, TypeError):
                              pass
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
         from pbs_monitor.utils.db_url import mask_db_url
         print(f"  Database URL: {mask_db_url(self.config.database.url)}")
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
   
   def _display_reservations_json(self, reservations: List[PBSReservation], args: argparse.Namespace):
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
         
         # Include reserved nodes if requested
         if getattr(args, 'show_nodes', False) and reservation.reserved_nodes:
            data['reserved_nodes'] = reservation.reserved_nodes
         
         reservation_data.append(data)
      
      print(json.dumps(reservation_data, indent=2))
   
   def _display_reservation_details(self, reservations: List[PBSReservation], args: argparse.Namespace):
      """Display detailed reservation information"""
      if args.format == "json":
         self._display_reservations_json(reservations, args)
         return
      elif args.format == "yaml":
         self._display_reservations_yaml(reservations, args)
         return
      
      # Table format (default)
      for i, reservation in enumerate(reservations):
         if i > 0:
            print()  # Blank line between reservations
         
         self._display_single_reservation_details(reservation, args)
   
   def _display_single_reservation_details(self, reservation: PBSReservation, args: argparse.Namespace):
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
      
      # Show reserved nodes if available
      if reservation.reserved_nodes:
         nodes_display = reservation.reserved_nodes
         
         # Check if we should show all nodes or truncate
         show_all_nodes = getattr(args, 'show_nodes', False)
         if not show_all_nodes and len(nodes_display) > 200:
            nodes_display = nodes_display[:200] + "... (truncated)"
            self.console.print(f"\n[bold]Reserved Nodes:[/bold]")
            self.console.print(f"[dim]{nodes_display}[/dim]")
            self.console.print(f"[dim]Use --show-nodes to see all {len(reservation.reserved_nodes)} characters[/dim]")
         else:
            self.console.print(f"\n[bold]Reserved Nodes:[/bold]")
            self.console.print(f"[dim]{nodes_display}[/dim]")
   
   def _display_reservations_yaml(self, reservations: List[PBSReservation], args: argparse.Namespace):
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
         
         # Include reserved nodes if requested
         if getattr(args, 'show_nodes', False) and reservation.reserved_nodes:
            data['reserved_nodes'] = reservation.reserved_nodes
         
         reservation_data.append(data)
      
      print(yaml.dump(reservation_data, default_flow_style=False))


class ScoreFormulaCommand(BaseCommand):
   """Display and explain the PBS job sort formula"""

   # Static documentation for formula variables
   VARIABLE_DOCS = {
      "base_score": ("Base priority score for the job", "server"),
      "score_boost": ("Additional priority boost", "server"),
      "enable_wfp": ("Enable Wait-time Factor Priority (0=off, 1=on)", "server"),
      "wfp_factor": ("Multiplier for WFP calculation", "server"),
      "enable_backfill": ("Enable backfill scheduling (0=off, 1=on)", "server"),
      "backfill_max": ("Maximum backfill bonus value", "server"),
      "backfill_factor": ("Divisor for backfill time calculation (seconds)", "server"),
      "enable_fifo": ("Enable FIFO ordering (0=off, 1=on)", "server"),
      "fifo_factor": ("Divisor for FIFO time calculation (seconds)", "server"),
      "project_priority": ("Project-level priority multiplier", "job"),
      "nodect": ("Number of nodes requested by the job", "job"),
      "total_cpus": ("Total CPUs available in the cluster", "server"),
      "walltime": ("Requested walltime in seconds", "job"),
      "eligible_time": ("Time since job became eligible to run (seconds)", "job"),
   }

   def execute(self, args: argparse.Namespace) -> int:
      """Execute the score-formula command"""
      try:
         # Get server data
         server_data = self.collector.pbs_commands.qstat_server()

         # Extract formula
         formula = self.collector.pbs_commands.get_job_sort_formula(server_data=server_data)

         if not formula:
            print("Error: No job sort formula available from PBS server")
            return 1

         # Get server defaults
         server_defaults = {}
         server_info = server_data.get("Server", {})
         for server_name, server_details in server_info.items():
            server_defaults = server_details.get("resources_default", {})
            break

         # Handle --raw option
         if getattr(args, 'raw', False):
            print(formula)
            return 0

         # Handle --plot-grid option
         if getattr(args, 'plot_grid', False):
            return self._generate_grid_plot(args, formula, server_defaults)

         # Handle --plot option
         if getattr(args, 'plot', False):
            return self._generate_plots(args, formula, server_defaults)

         # Fetch jobs if --job-ids provided (for display without --plot)
         jobs = []
         job_ids = getattr(args, 'job_ids', None)
         if job_ids:
            for job_id in job_ids:
               try:
                  job = self.collector.get_job_by_id(job_id)
                  if job:
                     jobs.append(job)
                  else:
                     self.logger.warning(f"Job {job_id} not found")
               except Exception as e:
                  self.logger.warning(f"Failed to get job {job_id}: {e}")

         # Display formatted output
         self._display_formula(formula, server_defaults, args, jobs)

         return 0

      except Exception as e:
         self.logger.error(f"Score formula command failed: {e}")
         print(f"Error: {e}")
         return 1

   def _display_formula(self, formula: str, server_defaults: Dict[str, Any], args: argparse.Namespace, jobs: List[Any] = None):
      """Display the formula and parameters in a formatted way"""

      # Header
      self.console.print()
      self.console.print("[bold cyan]PBS Job Sort Formula[/bold cyan]")
      self.console.print("=" * 60)
      self.console.print()

      # Raw formula
      self.console.print("[bold]Formula:[/bold]")
      self.console.print(f"  [green]{formula}[/green]")
      self.console.print()

      # Formula breakdown
      self._display_formula_breakdown(formula)

      # Parameters table (unless --no-defaults is specified)
      if not getattr(args, 'no_defaults', False):
         self._display_parameters_table(server_defaults)

      # Job-specific parameters table (if jobs provided)
      if jobs:
         self._display_job_parameters_table(jobs, server_defaults, formula)

   def _display_formula_breakdown(self, formula: str):
      """Display a breakdown of the formula components"""

      self.console.print("[bold]Formula Components:[/bold]")
      self.console.print()

      # Parse and identify the main components
      # The formula typically has these parts:
      # 1. Base: base_score + score_boost
      # 2. WFP: (enable_wfp * wfp_factor * (...))
      # 3. Backfill: (enable_backfill * min(backfill_max, ...))
      # 4. FIFO: (enable_fifo * eligible_time / fifo_factor)

      components = [
         ("1. Base Score", "base_score + score_boost",
          "Static priority values assigned to the job"),
         ("2. Wait-time Factor Priority (WFP)",
          "enable_wfp * wfp_factor * (eligible_time^2 / walltime^3 * project_priority * nodect / total_cpus)",
          "Rewards longer wait times, scaled by job size and project priority"),
         ("3. Backfill Bonus",
          "enable_backfill * min(backfill_max, eligible_time / backfill_factor)",
          "Bonus for jobs that can backfill into gaps"),
         ("4. FIFO Bonus",
          "enable_fifo * eligible_time / fifo_factor",
          "Simple time-in-queue priority boost"),
      ]

      for name, expr, description in components:
         self.console.print(f"  [bold yellow]{name}[/bold yellow]")
         self.console.print(f"    [dim]Expression:[/dim] [cyan]{expr}[/cyan]")
         self.console.print(f"    [dim]Purpose:[/dim] {description}")
         self.console.print()

   def _display_parameters_table(self, server_defaults: Dict[str, Any]):
      """Display a table of all parameters with their descriptions and defaults"""

      self.console.print("[bold]Parameters and Current Defaults:[/bold]")
      self.console.print()

      # Build rows for the table
      rows = []
      for var_name, (description, source) in self.VARIABLE_DOCS.items():
         # Get default value from server if available
         if var_name in server_defaults:
            default_value = str(server_defaults[var_name])
         elif var_name == "project_priority":
            default_value = "1"
         elif var_name == "nodect":
            default_value = "1"
         elif var_name == "walltime":
            default_value = "01:00:00 (3600s)"
         elif var_name == "eligible_time":
            default_value = "(dynamic)"
         else:
            default_value = "-"

         rows.append([var_name, description, default_value, source])

      # Create and display the table
      table = self._create_table(
         title="Formula Variables",
         headers=["Variable", "Description", "Default", "Source"],
         rows=rows
      )

      self.console.print(table)
      self.console.print()

      # Add notes
      self.console.print("[dim]Notes:[/dim]")
      self.console.print("[dim]  - Source 'server' = value from PBS server resources_default[/dim]")
      self.console.print("[dim]  - Source 'job' = value from job's Resource_List or computed at runtime[/dim]")
      self.console.print("[dim]  - Jobs can override server defaults via resource requests[/dim]")

   def _display_job_parameters_table(self, jobs: List[Any], server_defaults: Dict[str, Any], formula: str):
      """Display a table of formula parameter values for the specified jobs"""

      self.console.print()
      self.console.print("[bold]Parameters for Given Job IDs:[/bold]")
      self.console.print()

      # Build job data for each job
      job_data = []
      for job in jobs:
         resource_list = job.raw_attributes.get('Resource_List', {})

         # Get job-specific values, falling back to server defaults
         base_score = int(resource_list.get('base_score', server_defaults.get('base_score', 0)))
         score_boost = int(resource_list.get('score_boost', server_defaults.get('score_boost', 0)))
         enable_wfp = int(resource_list.get('enable_wfp', server_defaults.get('enable_wfp', 0)))
         wfp_factor = int(resource_list.get('wfp_factor', server_defaults.get('wfp_factor', 100000)))
         enable_backfill = int(resource_list.get('enable_backfill', server_defaults.get('enable_backfill', 0)))
         backfill_max = int(resource_list.get('backfill_max', server_defaults.get('backfill_max', 50)))
         backfill_factor = int(resource_list.get('backfill_factor', server_defaults.get('backfill_factor', 84600)))
         enable_fifo = int(resource_list.get('enable_fifo', server_defaults.get('enable_fifo', 1)))
         fifo_factor = int(resource_list.get('fifo_factor', server_defaults.get('fifo_factor', 1800)))
         total_cpus = int(server_defaults.get('total_cpus', 1))

         # Job-specific parameters
         project_priority = int(resource_list.get('project_priority', 1))
         nodect = job.nodes or 1
         walltime_str = job.walltime or '01:00:00'
         walltime_seconds = self._parse_walltime_to_seconds(walltime_str)
         eligible_time_raw = job.raw_attributes.get('eligible_time', '0')
         eligible_time_seconds = self._parse_walltime_to_seconds(str(eligible_time_raw)) if eligible_time_raw else 0

         # Calculate score terms
         base_term = base_score
         boost_term = score_boost

         # WFP term calculation (matching the formula structure)
         try:
            wfp_term = enable_wfp * wfp_factor * (
               eligible_time_seconds ** 2 /
               min(max(walltime_seconds, 21600.0), 43200.0) ** 3 *
               project_priority * nodect / total_cpus
            )
         except (ZeroDivisionError, ValueError):
            wfp_term = 0

         # Backfill term
         try:
            backfill_term = enable_backfill * min(backfill_max, eligible_time_seconds / backfill_factor)
         except (ZeroDivisionError, ValueError):
            backfill_term = 0

         # FIFO term
         try:
            fifo_term = enable_fifo * eligible_time_seconds / fifo_factor
         except (ZeroDivisionError, ValueError):
            fifo_term = 0

         total_score = base_term + boost_term + wfp_term + backfill_term + fifo_term

         # Truncate job ID for column header
         display_job_id = job.job_id.split('.')[0]  # Just the numeric part

         job_data.append({
            'job_id': display_job_id,
            'base_score': str(base_score),
            'score_boost': str(score_boost),
            'enable_wfp': str(enable_wfp),
            'wfp_factor': str(wfp_factor),
            'enable_backfill': str(enable_backfill),
            'backfill_max': str(backfill_max),
            'backfill_factor': str(backfill_factor),
            'enable_fifo': str(enable_fifo),
            'fifo_factor': str(fifo_factor),
            'project_priority': str(project_priority),
            'nodect': str(nodect),
            'total_cpus': str(total_cpus),
            'walltime': f'{walltime_str} ({int(walltime_seconds)}s)',
            'eligible_time': str(eligible_time_raw),
            # Score terms
            'base_term': f'{base_term:,.0f}',
            'boost_term': f'{boost_term:,.0f}',
            'wfp_term': f'{wfp_term:,.2f}',
            'backfill_term': f'{backfill_term:,.2f}',
            'fifo_term': f'{fifo_term:,.2f}',
            'total_score': f'{total_score:,.2f}',
         })

      # Build the table with jobs as columns
      from rich.table import Table

      table = Table(title="Job Formula Parameters", show_header=True, header_style="bold magenta")
      table.add_column("Variable", style="cyan")
      for jd in job_data:
         table.add_column(jd['job_id'], style="cyan")

      # Parameter rows
      params = [
         ('base_score', 'base_score'),
         ('score_boost', 'score_boost'),
         ('enable_wfp', 'enable_wfp'),
         ('wfp_factor', 'wfp_factor'),
         ('enable_backfill', 'enable_backfill'),
         ('backfill_max', 'backfill_max'),
         ('backfill_factor', 'backfill_factor'),
         ('enable_fifo', 'enable_fifo'),
         ('fifo_factor', 'fifo_factor'),
         ('project_priority', 'project_priority'),
         ('nodect', 'nodect'),
         ('total_cpus', 'total_cpus'),
         ('walltime', 'walltime'),
         ('eligible_time', 'eligible_time'),
      ]

      for label, key in params:
         row = [label] + [jd[key] for jd in job_data]
         table.add_row(*row)

      # Add separator
      table.add_section()

      # Score component rows
      score_rows = [
         ('base_term', 'base_term'),
         ('boost_term', 'boost_term'),
         ('wfp_term', 'wfp_term'),
         ('backfill_term', 'backfill_term'),
         ('fifo_term', 'fifo_term'),
         ('TOTAL SCORE', 'total_score'),
      ]

      for label, key in score_rows:
         row = [f'[bold]{label}[/bold]' if label == 'TOTAL SCORE' else label]
         for jd in job_data:
            val = jd[key]
            if label == 'TOTAL SCORE':
               row.append(f'[bold]{val}[/bold]')
            else:
               row.append(val)
         table.add_row(*row)

      self.console.print(table)
      self.console.print()

   def _generate_plots(self, args: argparse.Namespace, formula: str, server_defaults: Dict[str, Any]) -> int:
      """Generate score evolution plots"""
      try:
         import matplotlib.pyplot as plt
         import seaborn as sns
         import numpy as np
      except ImportError as e:
         print(f"Error: Plotting requires matplotlib, seaborn, and numpy. {e}")
         return 1

      import os
      from collections import defaultdict
      import random

      # Set up output directory
      output_dir = getattr(args, 'output_dir', 'plots/score_formula')
      os.makedirs(output_dir, exist_ok=True)

      # Get job configurations based on input mode
      configs = self._get_job_configs(args, server_defaults)

      if not configs:
         print("Error: No job configurations available to plot")
         return 1

      # Set up plotting style
      sns.set_context('talk')
      sns.set_style('whitegrid')

      self.console.print()
      self.console.print("[bold cyan]Generating Score Evolution Plots[/bold cyan]")
      self.console.print("=" * 60)
      self.console.print()
      self.console.print(f"[dim]Output directory: {output_dir}[/dim]")
      self.console.print(f"[dim]Job configurations: {len(configs)}[/dim]")
      self.console.print()

      # Generate plot types
      outputs = {}

      # Generate queue-aware score vs time plots (server defaults + per-queue)
      try:
         self.console.print("[bold]Score vs Time plots (by queue):[/bold]")
         result = self._plot_score_vs_time_by_queue(formula, server_defaults, args, output_dir)
         outputs.update(result)
      except Exception as e:
         self.logger.warning(f"Plot score_vs_time_by_queue failed: {e}")
         print(f"  Warning: Score vs Time (by queue) plots failed: {e}")

      try:
         result = self._plot_score_heatmap(formula, server_defaults, args, output_dir)
         outputs.update(result)
      except Exception as e:
         self.logger.warning(f"Plot score_heatmap failed: {e}")
         print(f"  Warning: Score heatmap plot failed: {e}")

      try:
         result = self._plot_job_shape_favorability(formula, server_defaults, output_dir)
         outputs.update(result)
      except Exception as e:
         self.logger.warning(f"Plot job_shape_favorability failed: {e}")
         print(f"  Warning: Job shape favorability plot failed: {e}")

      try:
         result = self._plot_walltime_cost(formula, server_defaults, output_dir)
         outputs.update(result)
      except Exception as e:
         self.logger.warning(f"Plot walltime_cost failed: {e}")
         print(f"  Warning: Walltime cost plot failed: {e}")

      try:
         sample_count = getattr(args, 'sample_count', 10)
         result = self._plot_sampled_jobs_score_vs_time(formula, server_defaults, args, output_dir, sample_count)
         outputs.update(result)
      except Exception as e:
         self.logger.warning(f"Plot sampled_jobs_score_vs_time failed: {e}")
         print(f"  Warning: Sampled jobs score vs time plot failed: {e}")

      # Display saved plot paths
      if outputs:
         self.console.print("[bold]Generated plots:[/bold]")
         for name, path in outputs.items():
            self.console.print(f"  [green]{name}[/green]: {path}")
      else:
         print("No plots were generated successfully")
         return 1

      return 0

   def _get_job_configs(self, args: argparse.Namespace, server_defaults: Dict[str, Any]) -> List[Dict[str, Any]]:
      """Get job configurations based on input mode"""
      configs = []

      # Mode 1: Interactive config from CLI flags
      nodes_arg = getattr(args, 'nodes', None)
      walltime_arg = getattr(args, 'walltime', None)
      if nodes_arg or walltime_arg:
         walltime_str = walltime_arg or '01:00:00'
         nodes = nodes_arg or 1
         configs.append({
            'label': f"{nodes} nodes, {walltime_str}",
            'nodect': nodes,
            'walltime_seconds': self._parse_walltime_to_seconds(walltime_str),
            'project_priority': getattr(args, 'project_priority', 1),
         })
         return configs

      # Mode 2: Extract configs from specific job IDs
      job_ids = getattr(args, 'job_ids', None)
      if job_ids:
         for job_id in job_ids:
            try:
               job = self.collector.get_job_by_id(job_id)
               if job:
                  resource_list = job.raw_attributes.get('Resource_List', {})
                  configs.append({
                     'label': f"{job_id[:20]} ({job.nodes}n, {job.queue})",
                     'nodect': job.nodes or 1,
                     'walltime_seconds': self._parse_walltime_to_seconds(job.walltime or '01:00:00'),
                     'project_priority': int(resource_list.get('project_priority', 1)),
                  })
               else:
                  self.logger.warning(f"Job {job_id} not found")
            except Exception as e:
               self.logger.warning(f"Failed to get job {job_id}: {e}")
         return configs if configs else self._get_predefined_configs(server_defaults)

      # Mode 3: Sample from current queue (default)
      sample_count = getattr(args, 'sample_count', 6)
      return self._sample_jobs_from_queue(sample_count, server_defaults)

   def _sample_jobs_from_queue(self, count: int, server_defaults: Dict[str, Any]) -> List[Dict[str, Any]]:
      """Sample representative jobs from current queue across queues and allocation types"""
      from collections import defaultdict
      import random

      try:
         jobs = self.collector.get_jobs()
         queued_jobs = [j for j in jobs if j.state.value == 'Q']
      except Exception as e:
         self.logger.warning(f"Failed to get jobs from queue: {e}")
         return self._get_predefined_configs(server_defaults)

      if not queued_jobs:
         self.console.print("[dim]No queued jobs found, using predefined configurations[/dim]")
         return self._get_predefined_configs(server_defaults)

      # Group by (queue, allocation_type) and sample
      groups = defaultdict(list)
      for job in queued_jobs:
         key = (job.queue, job.allocation_type or 'unknown')
         groups[key].append(job)

      # Sample up to `count` jobs, trying to get diversity
      sampled = []
      keys = list(groups.keys())
      random.shuffle(keys)

      for key in keys:
         if len(sampled) >= count:
            break
         job = random.choice(groups[key])
         resource_list = job.raw_attributes.get('Resource_List', {})
         sampled.append({
            'label': f"{job.queue}/{job.allocation_type or '?'} ({job.nodes}n)",
            'nodect': job.nodes or 1,
            'walltime_seconds': self._parse_walltime_to_seconds(job.walltime or '01:00:00'),
            'project_priority': int(resource_list.get('project_priority', 1)),
         })

      return sampled if sampled else self._get_predefined_configs(server_defaults)

   def _get_predefined_configs(self, server_defaults: Dict[str, Any]) -> List[Dict[str, Any]]:
      """Fallback predefined job configurations"""
      return [
         {'label': '1 node, 1hr', 'nodect': 1, 'walltime_seconds': 3600, 'project_priority': 1},
         {'label': '10 nodes, 6hr', 'nodect': 10, 'walltime_seconds': 21600, 'project_priority': 1},
         {'label': '100 nodes, 12hr', 'nodect': 100, 'walltime_seconds': 43200, 'project_priority': 1},
         {'label': '500 nodes, 24hr', 'nodect': 500, 'walltime_seconds': 86400, 'project_priority': 1},
      ]

   def _evaluate_formula(self, formula: str, nodect: int, walltime_seconds: float,
                         eligible_time_seconds: float, project_priority: int,
                         server_defaults: Dict[str, Any]) -> float:
      """Evaluate the job sort formula with given parameters"""
      variables = {
         "base_score": int(server_defaults.get("base_score", 0)),
         "score_boost": int(server_defaults.get("score_boost", 0)),
         "enable_wfp": int(server_defaults.get("enable_wfp", 0)),
         "wfp_factor": int(server_defaults.get("wfp_factor", 100000)),
         "enable_backfill": int(server_defaults.get("enable_backfill", 0)),
         "backfill_max": int(server_defaults.get("backfill_max", 50)),
         "backfill_factor": int(server_defaults.get("backfill_factor", 84600)),
         "enable_fifo": int(server_defaults.get("enable_fifo", 1)),
         "fifo_factor": int(server_defaults.get("fifo_factor", 1800)),
         "total_cpus": int(server_defaults.get("total_cpus", 1)),
         "project_priority": project_priority,
         "nodect": nodect,
         "walltime": walltime_seconds,
         "eligible_time": eligible_time_seconds,
         "min": min,
         "max": max,
      }
      return float(eval(formula, {"__builtins__": {}}, variables))

   def _parse_walltime_to_seconds(self, walltime_str: str) -> float:
      """Parse walltime string (HH:MM:SS or DD:HH:MM:SS) to seconds"""
      if not walltime_str:
         return 3600.0  # Default 1 hour

      try:
         parts = walltime_str.split(':')
         if len(parts) == 3:
            hours, minutes, seconds = map(int, parts)
            return hours * 3600 + minutes * 60 + seconds
         elif len(parts) == 4:
            days, hours, minutes, seconds = map(int, parts)
            return days * 86400 + hours * 3600 + minutes * 60 + seconds
         else:
            return 3600.0
      except (ValueError, TypeError):
         return 3600.0

   def _plot_score_vs_time(self, configs: List[Dict[str, Any]], formula: str,
                           server_defaults: Dict[str, Any], args: argparse.Namespace,
                           output_dir: str) -> Dict[str, str]:
      """Plot 1: Score vs eligible time for different configurations

      This is the legacy single-plot version. See _plot_score_vs_time_by_queue
      for the newer queue-aware multi-plot version.
      """
      import matplotlib.pyplot as plt
      import numpy as np

      max_time_hours = getattr(args, 'max_time_hours', 48.0)

      # Create time array (in seconds)
      time_hours = np.linspace(0.1, max_time_hours, 200)
      time_seconds = time_hours * 3600

      fig, ax = plt.subplots(figsize=(12, 7))

      # Calculate and plot score for each config
      for config in configs:
         scores = []
         for t in time_seconds:
            score = self._evaluate_formula(
               formula,
               nodect=config['nodect'],
               walltime_seconds=config['walltime_seconds'],
               eligible_time_seconds=t,
               project_priority=config['project_priority'],
               server_defaults=server_defaults
            )
            scores.append(score)
         ax.plot(time_hours, scores, label=config['label'], linewidth=2)

      ax.set_xlabel('Eligible Time (hours)')
      ax.set_ylabel('Score')
      ax.set_title('PBS Job Score vs Wait Time\n(Higher score = higher priority)')
      ax.legend(loc='upper left', bbox_to_anchor=(1.02, 1), borderaxespad=0, fontsize='small', frameon=True)
      ax.grid(True, alpha=0.3)

      # Add formula info as annotation
      ax.annotate(
         f"Formula components: Base + WFP + Backfill + FIFO",
         xy=(0.02, 0.02), xycoords='axes fraction',
         fontsize=8, color='gray'
      )

      plt.tight_layout()
      path = os.path.join(output_dir, 'score_vs_time.png')
      fig.savefig(path, bbox_inches='tight', dpi=120)
      plt.close(fig)

      return {'score_vs_time': path}

   def _get_standard_job_shapes(self) -> List[Dict[str, Any]]:
      """Get the standard set of job shapes for score vs time plots.

      Returns a list of job shape configurations with node counts and walltimes.
      Each shape includes metadata for color grouping and legend generation.
      """
      shapes = []

      # Define node count groups with their walltimes
      # Format: (node_count, list_of_walltime_hours)
      node_groups = [
         (1, [1]),                          # 1 node: only 1hr
         (32, [1, 3, 6, 12, 24]),           # 32 nodes: 1-24hr
         (128, [1, 3, 6, 12, 24]),          # 128 nodes: 1-24hr
         (512, [1, 3, 6, 12, 24]),          # 512 nodes: 1-24hr
         (1024, [1, 3, 6, 12, 24]),         # 1024 nodes: 1-24hr
         (4096, [1, 3, 6, 12, 24]),         # 4096 nodes: 1-24hr
         (8192, [1, 3, 6, 12, 24]),         # 8192 nodes: 1-24hr
      ]

      for node_count, walltimes in node_groups:
         for wt in walltimes:
            shapes.append({
               'nodect': node_count,
               'walltime_hours': wt,
               'walltime_seconds': wt * 3600,
               'project_priority': 1,
               'node_group_index': len([ng for ng in node_groups if ng[0] <= node_count]) - 1,
               'walltime_index': walltimes.index(wt),
               'num_walltimes': len(walltimes),
            })

      return shapes

   def _get_node_group_colors(self) -> List[str]:
      """Get base colors for each node count group.

      Returns 7 distinct colors for the 7 node count groups.
      """
      # Use a colorblind-friendly palette with good distinction
      return [
         '#1f77b4',  # Blue - 1 node
         '#ff7f0e',  # Orange - 32 nodes
         '#2ca02c',  # Green - 128 nodes
         '#d62728',  # Red - 512 nodes
         '#9467bd',  # Purple - 1024 nodes
         '#8c564b',  # Brown - 4096 nodes
         '#e377c2',  # Pink - 8192 nodes
      ]

   def _get_walltime_shade(self, base_color: str, walltime_index: int, num_walltimes: int) -> str:
      """Get a shade of the base color based on walltime position.

      Lighter shades for shorter walltimes, darker for longer.

      Args:
         base_color: Hex color string (e.g., '#1f77b4')
         walltime_index: Index of this walltime in the group (0 = shortest)
         num_walltimes: Total number of walltimes in the group

      Returns:
         Hex color string with adjusted brightness
      """
      import colorsys

      # Parse hex color to RGB
      hex_color = base_color.lstrip('#')
      r, g, b = tuple(int(hex_color[i:i+2], 16) / 255.0 for i in (0, 2, 4))

      # Convert to HLS (Hue, Lightness, Saturation)
      h, l, s = colorsys.rgb_to_hls(r, g, b)

      if num_walltimes == 1:
         # Single walltime, use base color
         return base_color

      # Adjust lightness: 0.7 (light) for shortest walltime to 0.3 (dark) for longest
      # Map walltime_index from [0, num_walltimes-1] to [0.65, 0.35] lightness range
      min_lightness = 0.35
      max_lightness = 0.65
      lightness_range = max_lightness - min_lightness
      l_new = max_lightness - (walltime_index / (num_walltimes - 1)) * lightness_range

      # Convert back to RGB
      r_new, g_new, b_new = colorsys.hls_to_rgb(h, l_new, s)

      # Convert to hex
      return '#{:02x}{:02x}{:02x}'.format(
         int(r_new * 255), int(g_new * 255), int(b_new * 255)
      )

   def _generate_legend_label(self, node_count: int, walltimes: List[int]) -> str:
      """Generate legend label for a node count group.

      Args:
         node_count: Number of nodes
         walltimes: List of walltime hours in this group

      Returns:
         Label string like "32 nodes (1-24hr)" or "1 node (1hr)"
      """
      node_word = "node" if node_count == 1 else "nodes"

      if len(walltimes) == 1:
         return f"{node_count} {node_word} ({walltimes[0]}hr)"
      else:
         return f"{node_count} {node_word} ({min(walltimes)}-{max(walltimes)}hr)"

   def _plot_score_vs_time_single(
      self,
      formula: str,
      defaults: Dict[str, Any],
      job_shapes: List[Dict[str, Any]],
      title: str,
      output_path: str,
      args: argparse.Namespace
   ) -> str:
      """Generate a single score vs time plot with the given job shapes.

      Args:
         formula: The PBS job_sort_formula string
         defaults: Merged defaults dict (server + queue)
         job_shapes: List of job shape dicts to plot
         title: Plot title
         output_path: Full path for output PNG
         args: CLI arguments (for max_time_hours, log_scale, etc.)

      Returns:
         Path to the generated plot
      """
      import matplotlib.pyplot as plt
      import numpy as np

      max_time_hours = getattr(args, 'max_time_hours', 48.0)
      use_log_scale = getattr(args, 'log_scale', False)

      # Create time array
      time_hours = np.linspace(0.1, max_time_hours, 200)
      time_seconds = time_hours * 3600

      fig, ax = plt.subplots(figsize=(14, 8))

      # Get colors
      base_colors = self._get_node_group_colors()

      # Group shapes by node count for legend
      from collections import OrderedDict
      node_groups = OrderedDict()
      for shape in job_shapes:
         nodect = shape['nodect']
         if nodect not in node_groups:
            node_groups[nodect] = []
         node_groups[nodect].append(shape)

      # Track legend handles (one per node group)
      legend_handles = []
      legend_labels = []

      # Plot each job shape
      for shape in job_shapes:
         nodect = shape['nodect']
         walltime_seconds = shape['walltime_seconds']
         node_group_index = shape['node_group_index']
         walltime_index = shape['walltime_index']
         num_walltimes = shape['num_walltimes']

         # Get color for this line
         base_color = base_colors[node_group_index]
         line_color = self._get_walltime_shade(base_color, walltime_index, num_walltimes)

         # Calculate scores
         scores = []
         for t in time_seconds:
            score = self._evaluate_formula(
               formula,
               nodect=nodect,
               walltime_seconds=walltime_seconds,
               eligible_time_seconds=t,
               project_priority=shape['project_priority'],
               server_defaults=defaults
            )
            scores.append(score)

         # Plot the line
         line, = ax.plot(time_hours, scores, color=line_color, linewidth=1.5, solid_capstyle='round')

         # Track first line of each node group for legend
         if walltime_index == 0:
            legend_handles.append(line)
            walltimes_in_group = [s['walltime_hours'] for s in node_groups[nodect]]
            legend_labels.append(self._generate_legend_label(nodect, walltimes_in_group))

      # Set up axes
      ax.set_xlabel('Eligible Time (hours)', fontsize=12)
      ax.set_ylabel('Score', fontsize=12)
      ax.set_title(title, fontsize=14, fontweight='bold')

      if use_log_scale:
         ax.set_xscale('log')
         ax.set_yscale('log')
         ax.set_xlim(left=1)  # Start at 1 hour for log scale

      # Add legend
      ax.legend(
         legend_handles, legend_labels,
         loc='upper left', bbox_to_anchor=(1.02, 1),
         borderaxespad=0, fontsize=10, frameon=True
      )

      ax.grid(True, alpha=0.3)

      # Add annotation about what's being shown
      enabled_components = []
      if defaults.get('enable_wfp', 0):
         enabled_components.append('WFP')
      if defaults.get('enable_backfill', 0):
         enabled_components.append('Backfill')
      if defaults.get('enable_fifo', 0):
         enabled_components.append('FIFO')
      if defaults.get('base_score', 0):
         enabled_components.insert(0, f"Base={defaults.get('base_score')}")

      components_str = ', '.join(enabled_components) if enabled_components else 'None configured'
      ax.annotate(
         f"Active components: {components_str}",
         xy=(0.02, 0.02), xycoords='axes fraction',
         fontsize=9, color='gray'
      )

      plt.tight_layout()
      fig.savefig(output_path, bbox_inches='tight', dpi=120)
      plt.close(fig)

      return output_path

   def _plot_score_vs_time_by_queue(
      self,
      formula: str,
      server_defaults: Dict[str, Any],
      args: argparse.Namespace,
      output_dir: str
   ) -> Dict[str, str]:
      """Generate a single score vs time plot with shaded envelopes for each queue.

      Creates a unified plot showing the score envelope (min to max possible scores)
      for each queue, including prod-fed queues and debug queues.

      The envelope for each queue is bounded by:
      - Maximum score curve: max nodes + min walltime
      - Minimum score curve: min nodes + max walltime

      Args:
         formula: The PBS job_sort_formula string
         server_defaults: Server-level resource defaults
         args: CLI arguments
         output_dir: Directory for output files

      Returns:
         Dict mapping plot names to file paths
      """
      import os
      import matplotlib.pyplot as plt
      import numpy as np

      outputs = {}

      # Get queue configs for prod-fed queues + debug queues
      queue_configs = self._get_target_queue_configs()

      if not queue_configs:
         self.console.print("  [yellow]No target queues detected, skipping envelope plot[/yellow]")
         return outputs

      self.console.print(f"  [dim]Generating envelope plot for {len(queue_configs)} queues...[/dim]")

      # Set up time array
      max_time_hours = getattr(args, 'max_time_hours', 48.0)
      time_hours = np.linspace(0.1, max_time_hours, 200)
      time_seconds = time_hours * 3600

      # Create figure
      fig, ax = plt.subplots(figsize=(14, 9))

      # Color palette for queues (colorblind-friendly)
      queue_colors = [
         '#1f77b4',  # Blue
         '#ff7f0e',  # Orange
         '#2ca02c',  # Green
         '#d62728',  # Red
         '#9467bd',  # Purple
         '#8c564b',  # Brown
         '#e377c2',  # Pink
         '#7f7f7f',  # Gray
         '#bcbd22',  # Olive
         '#17becf',  # Cyan
      ]

      legend_handles = []
      legend_labels = []

      for idx, qc in enumerate(queue_configs):
         queue_name = qc['name']
         color = queue_colors[idx % len(queue_colors)]

         # Merge queue defaults with server defaults
         queue_defaults = {**server_defaults, **qc.get('defaults', {})}

         # Get queue bounds
         min_nodes = qc.get('min_nodes', 1)
         max_nodes = qc.get('max_nodes', 10000)
         min_walltime_hours = qc.get('min_walltime_hours') or 1.0
         max_walltime_hours = qc.get('max_walltime_hours') or 24.0

         # Handle infinity for max_nodes
         if max_nodes == float('inf') or max_nodes > 10000:
            max_nodes = 10000  # Cap for visualization

         # Calculate max score curve: max nodes + min walltime (best case)
         max_scores = []
         for t in time_seconds:
            score = self._evaluate_formula(
               formula,
               nodect=int(max_nodes),
               walltime_seconds=min_walltime_hours * 3600,
               eligible_time_seconds=t,
               project_priority=1,
               server_defaults=queue_defaults
            )
            max_scores.append(score)
         max_scores = np.array(max_scores)

         # Calculate min score curve: min nodes + max walltime (worst case)
         min_scores = []
         for t in time_seconds:
            score = self._evaluate_formula(
               formula,
               nodect=int(min_nodes),
               walltime_seconds=max_walltime_hours * 3600,
               eligible_time_seconds=t,
               project_priority=1,
               server_defaults=queue_defaults
            )
            min_scores.append(score)
         min_scores = np.array(min_scores)

         # Plot shaded envelope
         ax.fill_between(time_hours, min_scores, max_scores,
                        alpha=0.25, color=color, linewidth=0)

         # Plot boundary lines
         line_max, = ax.plot(time_hours, max_scores, color=color, linewidth=1.5,
                            linestyle='-', alpha=0.9)
         ax.plot(time_hours, min_scores, color=color, linewidth=1.5,
                linestyle='--', alpha=0.9)

         # Build legend entry
         legend_handles.append(line_max)
         node_range = f"{min_nodes}-{max_nodes}" if min_nodes != max_nodes else str(min_nodes)
         wt_range = f"{min_walltime_hours:.0f}-{max_walltime_hours:.0f}hr"
         legend_labels.append(f"{queue_name} ({node_range}n, {wt_range})")

      # Set up axes
      ax.set_xlabel('Eligible Time (hours)', fontsize=12)
      ax.set_ylabel('Score', fontsize=12)
      ax.set_title('PBS Job Score Envelopes by Queue\n(Shaded region shows min-max score range for each queue)',
                  fontsize=14, fontweight='bold')

      # Add legend
      ax.legend(
         legend_handles, legend_labels,
         loc='upper left', bbox_to_anchor=(1.02, 1),
         borderaxespad=0, fontsize=9, frameon=True,
         title='Queue (nodes, walltime)'
      )

      ax.grid(True, alpha=0.3)

      # Apply log scale if requested
      use_log_scale = getattr(args, 'log_scale', False)
      if use_log_scale:
         ax.set_xscale('log')
         ax.set_yscale('log')
         ax.set_xlim(left=1)  # Start at 1 hour for log scale

      # Add annotation about line styles
      ax.annotate(
         'Solid line = max score (max nodes, min walltime)\n'
         'Dashed line = min score (min nodes, max walltime)',
         xy=(0.02, 0.02), xycoords='axes fraction',
         fontsize=9, color='gray',
         bbox=dict(boxstyle='round', facecolor='white', alpha=0.8)
      )

      plt.tight_layout()
      path = os.path.join(output_dir, 'score_vs_time_envelopes.png')
      fig.savefig(path, bbox_inches='tight', dpi=120)
      plt.close(fig)

      outputs['score_vs_time_envelopes'] = path
      return outputs

   def _plot_sampled_jobs_score_vs_time(
      self,
      formula: str,
      server_defaults: Dict[str, Any],
      args: argparse.Namespace,
      output_dir: str,
      sample_count: int = 10
   ) -> Dict[str, str]:
      """Generate a score vs time plot for randomly sampled jobs from production queues.

      Samples N jobs from the production queues and plots their individual score
      functions over time, using each job's actual parameters (nodes, walltime,
      project_priority, queue-specific defaults).

      Args:
         formula: The PBS job_sort_formula string
         server_defaults: Server-level resource defaults
         args: CLI arguments
         output_dir: Directory for output files
         sample_count: Number of jobs to sample (default 10)

      Returns:
         Dict mapping plot names to file paths
      """
      import os
      import random
      import matplotlib.pyplot as plt
      import numpy as np
      from collections import defaultdict

      outputs = {}

      self.console.print(f"  [dim]Sampling {sample_count} jobs from production queues...[/dim]")

      # Get all queued jobs
      try:
         jobs = self.collector.get_jobs()
         queued_jobs = [j for j in jobs if j.state.value == 'Q']
      except Exception as e:
         self.logger.warning(f"Failed to get jobs: {e}")
         self.console.print(f"  [yellow]Could not fetch jobs: {e}[/yellow]")
         return outputs

      if not queued_jobs:
         self.console.print("  [yellow]No queued jobs found, skipping sampled jobs plot[/yellow]")
         return outputs

      # Get target queue names (prod-fed + debug queues)
      target_queue_configs = self._get_target_queue_configs()
      target_queue_names = {qc['name'] for qc in target_queue_configs}
      queue_defaults_map = {qc['name']: qc.get('defaults', {}) for qc in target_queue_configs}

      # Filter to jobs in target queues
      target_jobs = [j for j in queued_jobs if j.queue in target_queue_names]

      if not target_jobs:
         self.console.print("  [yellow]No jobs found in production queues, skipping sampled jobs plot[/yellow]")
         return outputs

      # Group by (queue, allocation_type) for diverse sampling
      groups = defaultdict(list)
      for job in target_jobs:
         key = (job.queue, job.allocation_type or 'unknown')
         groups[key].append(job)

      # Sample jobs, trying to get diversity across groups
      sampled_jobs = []
      keys = list(groups.keys())
      random.shuffle(keys)

      # First pass: one job per group
      for key in keys:
         if len(sampled_jobs) >= sample_count:
            break
         job = random.choice(groups[key])
         sampled_jobs.append(job)

      # Second pass: fill remaining slots randomly from all target jobs
      remaining = sample_count - len(sampled_jobs)
      if remaining > 0:
         already_sampled_ids = {j.job_id for j in sampled_jobs}
         available = [j for j in target_jobs if j.job_id not in already_sampled_ids]
         if available:
            additional = random.sample(available, min(remaining, len(available)))
            sampled_jobs.extend(additional)

      if not sampled_jobs:
         self.console.print("  [yellow]Could not sample any jobs[/yellow]")
         return outputs

      self.console.print(f"  [dim]Plotting score curves for {len(sampled_jobs)} sampled jobs...[/dim]")

      # Set up time array
      max_time_hours = getattr(args, 'max_time_hours', 48.0)
      time_hours = np.linspace(0.1, max_time_hours, 200)
      time_seconds = time_hours * 3600

      # Create figure
      fig, ax = plt.subplots(figsize=(14, 9))

      # Color palette (colorblind-friendly)
      queue_colors_palette = [
         '#1f77b4',  # Blue
         '#ff7f0e',  # Orange
         '#2ca02c',  # Green
         '#d62728',  # Red
         '#9467bd',  # Purple
         '#8c564b',  # Brown
         '#e377c2',  # Pink
         '#7f7f7f',  # Gray
         '#bcbd22',  # Olive
         '#17becf',  # Cyan
      ]

      # Assign colors to queues (consistent coloring per queue)
      unique_queues = sorted(set(job.queue for job in sampled_jobs))
      queue_color_map = {q: queue_colors_palette[i % len(queue_colors_palette)] for i, q in enumerate(unique_queues)}

      # Track which queues we've added to the legend
      legend_handles = []
      legend_labels = []
      queues_in_legend = set()

      # Group sampled jobs by queue for plotting (so legend entries are in order)
      jobs_by_queue = defaultdict(list)
      for job in sampled_jobs:
         jobs_by_queue[job.queue].append(job)

      for queue_name in unique_queues:
         color = queue_color_map[queue_name]
         queue_defaults = queue_defaults_map.get(queue_name, {})
         merged_defaults = {**server_defaults, **queue_defaults}

         first_line = None
         for job in jobs_by_queue[queue_name]:
            # Get job parameters
            resource_list = job.raw_attributes.get('Resource_List', {})
            nodect = job.nodes or 1
            walltime_str = job.walltime or '01:00:00'
            walltime_seconds_job = self._parse_walltime_to_seconds(walltime_str)
            project_priority = int(resource_list.get('project_priority', 1))

            # Calculate scores over time
            scores = []
            for t in time_seconds:
               score = self._evaluate_formula(
                  formula,
                  nodect=nodect,
                  walltime_seconds=walltime_seconds_job,
                  eligible_time_seconds=t,
                  project_priority=project_priority,
                  server_defaults=merged_defaults
               )
               scores.append(score)
            scores = np.array(scores)

            # Plot the curve
            line, = ax.plot(time_hours, scores, color=color, linewidth=1.5, alpha=0.75)

            # Keep the first line for legend
            if first_line is None:
               first_line = line

         # Add one legend entry per queue
         if first_line is not None:
            legend_handles.append(first_line)
            job_count = len(jobs_by_queue[queue_name])
            legend_labels.append(f"{queue_name} ({job_count} jobs)")

      # Set up axes
      ax.set_xlabel('Eligible Time (hours)', fontsize=12)
      ax.set_ylabel('Score', fontsize=12)
      ax.set_title(
         f'PBS Job Score vs Time for {len(sampled_jobs)} Sampled Jobs\n'
         f'(Randomly sampled from production queues)',
         fontsize=14, fontweight='bold'
      )

      # Apply log scale if requested
      use_log_scale = getattr(args, 'log_scale', False)
      if use_log_scale:
         ax.set_xscale('log')
         ax.set_yscale('log')
         ax.set_xlim(left=1)  # Start at 1 hour for log scale

      # Add legend (outside plot on the right)
      ax.legend(
         legend_handles, legend_labels,
         loc='upper left', bbox_to_anchor=(1.02, 1),
         borderaxespad=0, fontsize=10, frameon=True,
         title='Queue'
      )

      ax.grid(True, alpha=0.3)

      plt.tight_layout()
      path = os.path.join(output_dir, 'sampled_jobs_score_vs_time.png')
      fig.savefig(path, bbox_inches='tight', dpi=120)
      plt.close(fig)

      outputs['sampled_jobs_score_vs_time'] = path
      self.console.print(f"  [green]Saved:[/green] {path}")
      return outputs

   def _get_target_queue_configs(self) -> List[Dict[str, Any]]:
      """Get queue configs for prod-fed queues + debug queues.

      Returns configs for:
      - All execution queues fed by the 'prod' routing queue
      - The 'debug' queue
      - The 'debug-scaling' queue
      """
      try:
         queues = self.collector.pbs_commands.qstat_queues()
         queues_by_name = {q.name: q for q in queues}

         target_queue_names = set()

         # 1. Get queues fed by 'prod' routing queue
         if 'prod' in queues_by_name:
            prod_q = queues_by_name['prod']
            route_destinations = prod_q.raw_attributes.get('route_destinations', '')
            if route_destinations:
               dest_names = [name.strip() for name in route_destinations.split(',')]
               # Filter out backfill queues
               for name in dest_names:
                  if not name.startswith('backfill-'):
                     target_queue_names.add(name)

         # Also check prod-large for larger node counts
         if 'prod-large' in queues_by_name:
            prod_large = queues_by_name['prod-large']
            route_destinations = prod_large.raw_attributes.get('route_destinations', '')
            if route_destinations:
               dest_names = [name.strip() for name in route_destinations.split(',')]
               for name in dest_names:
                  if not name.startswith('backfill-'):
                     target_queue_names.add(name)

         # 2. Add debug queues
         target_queue_names.add('debug')
         target_queue_names.add('debug-scaling')

         # 3. Build queue configs
         queue_configs = []
         for queue_name in target_queue_names:
            if queue_name not in queues_by_name:
               continue

            q = queues_by_name[queue_name]
            # Only include execution queues
            if q.raw_attributes.get('queue_type') != 'Execution':
               continue

            raw = q.raw_attributes
            resources_min = raw.get('resources_min', {})
            resources_max = raw.get('resources_max', {})
            resources_default = raw.get('resources_default', {})

            # Parse node limits
            min_nodes = resources_min.get('nodect', 1)
            max_nodes = resources_max.get('nodect', float('inf'))

            if isinstance(min_nodes, str):
               min_nodes = int(min_nodes)
            if isinstance(max_nodes, str):
               max_nodes = int(max_nodes)

            # Parse walltime limits
            max_walltime = resources_max.get('walltime')
            min_walltime = resources_min.get('walltime')

            max_walltime_hours = self._parse_walltime_to_hours(max_walltime) if max_walltime else None
            min_walltime_hours = self._parse_walltime_to_hours(min_walltime) if min_walltime else None

            queue_configs.append({
               'name': queue_name,
               'min_nodes': min_nodes,
               'max_nodes': max_nodes,
               'min_walltime_hours': min_walltime_hours,
               'max_walltime_hours': max_walltime_hours,
               'defaults': resources_default,
            })

         # Sort by min_nodes for consistent ordering
         queue_configs.sort(key=lambda x: (x['min_nodes'], x['name']))

         return queue_configs

      except Exception as e:
         self.logger.warning(f"Failed to get target queue configs: {e}")
         return []

   def _detect_all_execution_queues(self) -> List[Dict[str, Any]]:
      """Detect all execution queues and their settings.

      Returns a list of queue config dicts with:
      - name: Queue name
      - min_nodes, max_nodes: Node count limits
      - min_walltime_hours, max_walltime_hours: Walltime limits
      - defaults: Queue-specific resources_default dict
      """
      try:
         queues = self.collector.pbs_commands.qstat_queues()

         queue_configs = []
         for q in queues:
            # Only include execution queues
            if q.raw_attributes.get('queue_type') != 'Execution':
               continue

            raw = q.raw_attributes
            resources_min = raw.get('resources_min', {})
            resources_max = raw.get('resources_max', {})
            resources_default = raw.get('resources_default', {})

            # Parse node limits
            min_nodes = resources_min.get('nodect', 1)
            max_nodes = resources_max.get('nodect', float('inf'))

            if isinstance(min_nodes, str):
               min_nodes = int(min_nodes)
            if isinstance(max_nodes, str):
               max_nodes = int(max_nodes)

            # Parse walltime limits
            max_walltime = resources_max.get('walltime')
            min_walltime = resources_min.get('walltime')

            max_walltime_hours = self._parse_walltime_to_hours(max_walltime) if max_walltime else None
            min_walltime_hours = self._parse_walltime_to_hours(min_walltime) if min_walltime else None

            queue_configs.append({
               'name': q.name,
               'min_nodes': min_nodes,
               'max_nodes': max_nodes,
               'min_walltime_hours': min_walltime_hours,
               'max_walltime_hours': max_walltime_hours,
               'defaults': resources_default,
            })

         # Sort by min_nodes for consistent ordering
         queue_configs.sort(key=lambda x: (x['min_nodes'], x['name']))

         return queue_configs

      except Exception as e:
         self.logger.warning(f"Failed to detect execution queues: {e}")
         return []

   def _plot_score_heatmap(self, formula: str, server_defaults: Dict[str, Any],
                           args: argparse.Namespace, output_dir: str) -> Dict[str, str]:
      """Plot 3: 2D heatmap of score(nodes, walltime) at fixed eligible_time"""
      import matplotlib.pyplot as plt
      import numpy as np

      # Fixed eligible time for heatmap (6 hours is a reasonable wait)
      eligible_time_hours = 6.0
      eligible_time_seconds = eligible_time_hours * 3600

      # Create grid of nodes and walltime
      nodes_range = np.array([1, 2, 5, 10, 20, 50, 100, 200, 500, 1000])
      walltime_hours = np.array([1, 2, 4, 6, 12, 24, 48])
      walltime_seconds = walltime_hours * 3600

      # Calculate scores for grid
      scores = np.zeros((len(nodes_range), len(walltime_hours)))

      for i, nodes in enumerate(nodes_range):
         for j, wt in enumerate(walltime_seconds):
            score = self._evaluate_formula(
               formula,
               nodect=int(nodes),
               walltime_seconds=float(wt),
               eligible_time_seconds=eligible_time_seconds,
               project_priority=1,
               server_defaults=server_defaults
            )
            scores[i, j] = score

      fig, ax = plt.subplots(figsize=(10, 8))

      # Create heatmap
      import seaborn as sns
      heatmap = sns.heatmap(
         scores,
         xticklabels=[f'{h}h' for h in walltime_hours],
         yticklabels=nodes_range,
         annot=True,
         fmt='.0f',
         cmap='YlOrRd',
         ax=ax,
         cbar_kws={'label': 'Score'}
      )

      ax.set_xlabel('Walltime')
      ax.set_ylabel('Nodes')
      ax.set_title(f'Job Score Heatmap\n(eligible_time = {eligible_time_hours}h, project_priority = 1)')

      # Invert y-axis to have small nodes at bottom
      ax.invert_yaxis()

      plt.tight_layout()
      path = os.path.join(output_dir, 'score_heatmap.png')
      fig.savefig(path, bbox_inches='tight', dpi=120)
      plt.close(fig)

      return {'score_heatmap': path}

   def _parse_walltime_clamping(self, formula: str) -> Dict[str, Any]:
      """
      Parse the walltime clamping parameters from the job sort formula.

      Looks for patterns like: min(max(walltime,21600.0),43200.0) ** 3
      to extract floor, ceiling, and exponent values.

      Returns:
         Dict with keys: floor_seconds, ceiling_seconds, exponent, found
      """
      import re

      result = {
         'floor_seconds': 21600.0,   # Default: 6 hours
         'ceiling_seconds': 43200.0, # Default: 12 hours
         'exponent': 3,              # Default: cubic
         'found': False
      }

      # Pattern to match: min(max(walltime,FLOOR),CEILING) ** EXP
      # or: min(max(walltime,FLOOR),CEILING)**EXP  (no spaces)
      # The walltime variable might have different forms
      pattern = r'min\s*\(\s*max\s*\(\s*walltime\s*,\s*([\d.]+)\s*\)\s*,\s*([\d.]+)\s*\)\s*\*\*\s*(\d+)'

      match = re.search(pattern, formula, re.IGNORECASE)
      if match:
         result['floor_seconds'] = float(match.group(1))
         result['ceiling_seconds'] = float(match.group(2))
         result['exponent'] = int(match.group(3))
         result['found'] = True

      return result

   def _plot_job_shape_favorability(self, formula: str, server_defaults: Dict[str, Any],
                                     output_dir: str) -> Dict[str, str]:
      """
      Plot: Job Shape Favorability Heatmap

      A 2D contour plot visualizing priority score as a function of walltime (X) and
      node count (Y), at a fixed eligible_time. Shows how job shape affects WFP score.

      The clamping logic is derived from the formula to future-proof against changes.
      """
      import matplotlib.pyplot as plt
      import numpy as np

      # Parse clamping parameters from formula
      clamp = self._parse_walltime_clamping(formula)
      floor_hours = clamp['floor_seconds'] / 3600
      ceiling_hours = clamp['ceiling_seconds'] / 3600
      exponent = clamp['exponent']

      # Fixed eligible_time for visualization (24 hours as suggested)
      eligible_time_hours = 24.0
      eligible_time_seconds = eligible_time_hours * 3600

      # Get WFP parameters from server defaults
      wfp_factor = int(server_defaults.get('wfp_factor', 100000))
      total_cpus = int(server_defaults.get('total_cpus', 1))
      enable_wfp = int(server_defaults.get('enable_wfp', 1))

      # For visualization, always show WFP behavior even if disabled on server
      # The point of this plot is to understand the formula, not the current config
      wfp_enabled_note = ""
      if enable_wfp == 0:
         wfp_enabled_note = " (WFP currently disabled on server)"
         self.logger.info("WFP is disabled on server, but showing formula behavior for visualization")

      # Create grid
      # X-axis: Walltime from 1 to 24 hours
      walltime_hours = np.linspace(1, 24, 100)
      # Y-axis: Nodes from 1 to 1000
      nodes = np.linspace(1, 1000, 100)

      # Create meshgrid
      WT, N = np.meshgrid(walltime_hours, nodes)

      # Apply clamping logic derived from formula
      # Convert walltime to seconds for clamping
      wt_seconds = WT * 3600
      clamped_wt = np.clip(wt_seconds, clamp['floor_seconds'], clamp['ceiling_seconds'])

      # Calculate WFP term (the job-shape-dependent component)
      # Z = eligible_time^2 / clamped_time^exponent * nodes / total_cpus * wfp_factor
      # Note: We always calculate as if WFP is enabled (for visualization purposes)
      Z = wfp_factor * (
         eligible_time_seconds ** 2 /
         (clamped_wt ** exponent) *
         N / total_cpus
      )

      # Create figure
      fig, ax = plt.subplots(figsize=(12, 8))

      # Filled contour plot
      levels = 30
      cf = ax.contourf(WT, N, Z, levels=levels, cmap='viridis')

      # Add contour lines with labels
      contour_lines = ax.contour(WT, N, Z, levels=10, colors='white', linewidths=0.5, alpha=0.7)
      ax.clabel(contour_lines, inline=True, fontsize=8, fmt='%.0f')

      # Add colorbar
      cbar = fig.colorbar(cf, ax=ax, label='WFP Score Component')

      # Mark clamping transition points (derived from formula)
      ax.axvline(x=floor_hours, color='red', linestyle='--', linewidth=2,
                 label=f'Floor ({floor_hours:.0f}h): below this, score constant')
      ax.axvline(x=ceiling_hours, color='orange', linestyle='--', linewidth=2,
                 label=f'Ceiling ({ceiling_hours:.0f}h): above this, penalty maxed')

      # Labels and title
      ax.set_xlabel('Requested Walltime (hours)', fontsize=12)
      ax.set_ylabel('Node Count', fontsize=12)
      ax.set_title(
         f'Job Shape Favorability Heatmap{wfp_enabled_note}\n'
         f'(eligible_time = {eligible_time_hours:.0f}h, walltime clamped to [{floor_hours:.0f}h, {ceiling_hours:.0f}h], exponent = {exponent})',
         fontsize=12
      )

      ax.legend(loc='upper right', fontsize=9)
      ax.grid(True, alpha=0.3, color='white')

      # Add annotation explaining the visualization
      ax.annotate(
         f'Brighter = Higher priority\n'
         f'Walltime penalty: (clamped_time)^{exponent}',
         xy=(0.02, 0.02), xycoords='axes fraction',
         fontsize=9, color='white',
         bbox=dict(boxstyle='round', facecolor='black', alpha=0.5)
      )

      plt.tight_layout()
      path = os.path.join(output_dir, 'job_shape_favorability.png')
      fig.savefig(path, bbox_inches='tight', dpi=120)
      plt.close(fig)

      return {'job_shape_favorability': path}

   def _plot_walltime_cost(self, formula: str, server_defaults: Dict[str, Any],
                           output_dir: str) -> Dict[str, str]:
      """
      Plot: Cost of Walltime (Equivalence/Indifference Curve)

      Shows how many nodes are needed to maintain the same priority score
      as walltime increases. Answers: "To keep the same priority, how many
      nodes do I need if I increase my walltime?"

      The clamping logic is derived from the formula to future-proof against changes.
      """
      import matplotlib.pyplot as plt
      import numpy as np

      # Parse clamping parameters from formula
      clamp = self._parse_walltime_clamping(formula)
      floor_hours = clamp['floor_seconds'] / 3600
      ceiling_hours = clamp['ceiling_seconds'] / 3600
      exponent = clamp['exponent']

      # Baseline job configuration
      baseline_nodes = 50
      baseline_walltime_hours = 6.0  # At the floor
      baseline_wt_seconds = baseline_walltime_hours * 3600
      baseline_clamped = np.clip(baseline_wt_seconds, clamp['floor_seconds'], clamp['ceiling_seconds'])

      # Create walltime array (1 to 24 hours)
      walltime_hours = np.linspace(1, 24, 200)
      walltime_seconds = walltime_hours * 3600

      # Apply clamping logic
      clamped_wt = np.clip(walltime_seconds, clamp['floor_seconds'], clamp['ceiling_seconds'])

      # Calculate required nodes to maintain same score
      # Score ∝ nodes / clamped_time^exponent
      # For equal scores: N_new / clamped_new^exp = N_base / clamped_base^exp
      # Therefore: N_new = N_base * (clamped_new / clamped_base)^exp
      required_nodes = baseline_nodes * (clamped_wt / baseline_clamped) ** exponent

      # Create figure
      fig, ax = plt.subplots(figsize=(12, 7))

      # Main curve
      ax.plot(walltime_hours, required_nodes, 'b-', linewidth=2.5, label='Required nodes')

      # Mark baseline point
      ax.scatter([baseline_walltime_hours], [baseline_nodes], color='red', s=100, zorder=5,
                 label=f'Baseline: {baseline_nodes} nodes @ {baseline_walltime_hours:.0f}h')

      # Shade the three regions
      # Region 1: < floor (flat - no penalty increase)
      region1_mask = walltime_hours <= floor_hours
      ax.fill_between(walltime_hours[region1_mask], 0, required_nodes[region1_mask],
                      alpha=0.2, color='green', label=f'No penalty (< {floor_hours:.0f}h)')

      # Region 2: floor to ceiling (penalty zone - cubic growth)
      region2_mask = (walltime_hours > floor_hours) & (walltime_hours <= ceiling_hours)
      ax.fill_between(walltime_hours[region2_mask], 0, required_nodes[region2_mask],
                      alpha=0.2, color='yellow', label=f'Penalty zone ({floor_hours:.0f}h - {ceiling_hours:.0f}h)')

      # Region 3: > ceiling (maxed out - flat again)
      region3_mask = walltime_hours > ceiling_hours
      ax.fill_between(walltime_hours[region3_mask], 0, required_nodes[region3_mask],
                      alpha=0.2, color='red', label=f'Max penalty (> {ceiling_hours:.0f}h)')

      # Add vertical lines at transition points
      ax.axvline(x=floor_hours, color='green', linestyle='--', linewidth=1.5, alpha=0.7)
      ax.axvline(x=ceiling_hours, color='red', linestyle='--', linewidth=1.5, alpha=0.7)

      # Labels and title
      ax.set_xlabel('Requested Walltime (hours)', fontsize=12)
      ax.set_ylabel('Nodes Required (to match baseline priority)', fontsize=12)
      ax.set_title(
         f'Cost of Walltime: Nodes Needed to Maintain Equal Priority\n'
         f'(Clamping: [{floor_hours:.0f}h, {ceiling_hours:.0f}h], Exponent: {exponent})',
         fontsize=12
      )

      ax.legend(loc='upper left', fontsize=10)
      ax.grid(True, alpha=0.3)
      ax.set_xlim(0, 25)
      ax.set_ylim(0, max(required_nodes) * 1.1)

      # Add interpretive annotations
      # Annotation for flat region 1
      flat1_y = required_nodes[region1_mask][-1] if any(region1_mask) else baseline_nodes
      ax.annotate(
         f'Flat: requesting <{floor_hours:.0f}h\ncosts the same as {floor_hours:.0f}h',
         xy=(floor_hours / 2, flat1_y),
         xytext=(floor_hours / 2, flat1_y + 50),
         fontsize=9, ha='center',
         arrowprops=dict(arrowstyle='->', color='gray', alpha=0.7)
      )

      # Annotation for penalty zone
      mid_penalty = (floor_hours + ceiling_hours) / 2
      mid_idx = np.argmin(np.abs(walltime_hours - mid_penalty))
      ax.annotate(
         f'Cubic growth:\n{exponent}x penalty per\ndoubling of time',
         xy=(mid_penalty, required_nodes[mid_idx]),
         xytext=(mid_penalty + 2, required_nodes[mid_idx] + 100),
         fontsize=9, ha='center',
         arrowprops=dict(arrowstyle='->', color='gray', alpha=0.7)
      )

      # Annotation for flat region 3
      if any(region3_mask):
         flat3_y = required_nodes[region3_mask][0]
         ax.annotate(
            f'Flat: >{ceiling_hours:.0f}h has same\npenalty as {ceiling_hours:.0f}h',
            xy=(ceiling_hours + 2, flat3_y),
            xytext=(ceiling_hours + 4, flat3_y - 80),
            fontsize=9, ha='center',
            arrowprops=dict(arrowstyle='->', color='gray', alpha=0.7)
         )

      plt.tight_layout()
      path = os.path.join(output_dir, 'walltime_cost.png')
      fig.savefig(path, bbox_inches='tight', dpi=120)
      plt.close(fig)

      return {'walltime_cost': path}

   def _generate_grid_plot(self, args: argparse.Namespace, formula: str,
                           server_defaults: Dict[str, Any]) -> int:
      """Generate high-resolution grid plot showing score components across node/walltime combinations"""
      try:
         import matplotlib.pyplot as plt
         import numpy as np
      except ImportError as e:
         print(f"Error: Plotting requires matplotlib and numpy. {e}")
         return 1

      import os

      # Set up output directory
      output_dir = getattr(args, 'output_dir', 'plots/score_formula')
      os.makedirs(output_dir, exist_ok=True)

      # Get grid configuration
      grid_nodes = getattr(args, 'grid_nodes', [256, 512, 1024, 2048, 4096, 8192])
      grid_walltimes = getattr(args, 'grid_walltimes', [3, 6, 10, 12, 18, 24])
      max_time_hours = getattr(args, 'max_time_hours', 48.0)
      routing_queue = getattr(args, 'routing_queue', 'prod')

      # Detect execution queues from routing queue
      queue_configs = self._detect_queue_configs(routing_queue)

      if not queue_configs:
         print(f"Warning: Could not detect queues from routing queue '{routing_queue}'. Using server defaults.")
         queue_configs = [{'name': 'default', 'min_nodes': 0, 'max_nodes': float('inf'), 'defaults': server_defaults}]

      self.console.print()
      self.console.print("[bold cyan]Generating Score Component Grid Plot[/bold cyan]")
      self.console.print("=" * 60)
      self.console.print()
      self.console.print(f"[dim]Output directory: {output_dir}[/dim]")
      self.console.print(f"[dim]Grid dimensions: {len(grid_nodes)} columns (nodes) × {len(grid_walltimes)} rows (walltimes)[/dim]")
      self.console.print(f"[dim]Routing queue: {routing_queue}[/dim]")
      self.console.print(f"[dim]Detected execution queues: {', '.join(q['name'] for q in queue_configs)}[/dim]")
      self.console.print()

      # Generate the grid plot
      path = self._plot_score_grid(
         grid_nodes, grid_walltimes, max_time_hours,
         formula, queue_configs, server_defaults, output_dir, args
      )

      self.console.print("[bold]Generated plot:[/bold]")
      self.console.print(f"  [green]score_grid[/green]: {path}")

      return 0

   def _detect_queue_configs(self, routing_queue: str) -> List[Dict[str, Any]]:
      """Detect execution queues and their settings from a routing queue"""
      try:
         # Get queue data
         queues = self.collector.pbs_commands.qstat_queues()

         # Find the routing queue
         routing_q = None
         for q in queues:
            if q.name == routing_queue:
               routing_q = q
               break

         if not routing_q:
            self.logger.warning(f"Routing queue '{routing_queue}' not found")
            return []

         # Get route_destinations from raw_attributes
         route_destinations = routing_q.raw_attributes.get('route_destinations', '')
         if not route_destinations:
            self.logger.warning(f"No route_destinations found for queue '{routing_queue}'")
            return []

         # Parse destination queue names (comma-separated)
         dest_queue_names = [name.strip() for name in route_destinations.split(',')]

         # Filter out backfill queues - we only want the primary WFP queues
         primary_queues = [name for name in dest_queue_names if not name.startswith('backfill-')]

         # Build queue configurations
         queue_configs = []
         queues_by_name = {q.name: q for q in queues}

         for queue_name in primary_queues:
            if queue_name not in queues_by_name:
               continue

            q = queues_by_name[queue_name]
            raw = q.raw_attributes

            # Extract node limits from resources_min/max
            resources_min = raw.get('resources_min', {})
            resources_max = raw.get('resources_max', {})
            resources_default = raw.get('resources_default', {})

            min_nodes = resources_min.get('nodect', 1)
            max_nodes = resources_max.get('nodect', float('inf'))

            # Parse as int if string
            if isinstance(min_nodes, str):
               min_nodes = int(min_nodes)
            if isinstance(max_nodes, str):
               max_nodes = int(max_nodes)

            # Extract walltime limits
            max_walltime = resources_max.get('walltime')
            min_walltime = resources_min.get('walltime')

            max_walltime_hours = self._parse_walltime_to_hours(max_walltime) if max_walltime else None
            min_walltime_hours = self._parse_walltime_to_hours(min_walltime) if min_walltime else None

            queue_configs.append({
               'name': queue_name,
               'min_nodes': min_nodes,
               'max_nodes': max_nodes,
               'min_walltime_hours': min_walltime_hours,
               'max_walltime_hours': max_walltime_hours,
               'defaults': resources_default
            })

         # Sort by min_nodes
         queue_configs.sort(key=lambda x: x['min_nodes'])

         # Also check prod-large routing queue for large jobs
         if routing_queue == 'prod' and 'prod-large' in queues_by_name:
            prod_large = queues_by_name['prod-large']
            large_destinations = prod_large.raw_attributes.get('route_destinations', '')
            large_dest_names = [name.strip() for name in large_destinations.split(',')]
            large_primary = [name for name in large_dest_names if not name.startswith('backfill-')]

            for queue_name in large_primary:
               if queue_name not in queues_by_name:
                  continue
               if any(qc['name'] == queue_name for qc in queue_configs):
                  continue  # Already added

               q = queues_by_name[queue_name]
               raw = q.raw_attributes

               resources_min = raw.get('resources_min', {})
               resources_max = raw.get('resources_max', {})
               resources_default = raw.get('resources_default', {})

               min_nodes = resources_min.get('nodect', 1)
               max_nodes = resources_max.get('nodect', float('inf'))

               if isinstance(min_nodes, str):
                  min_nodes = int(min_nodes)
               if isinstance(max_nodes, str):
                  max_nodes = int(max_nodes)

               # Extract walltime limits
               max_walltime = resources_max.get('walltime')
               min_walltime = resources_min.get('walltime')

               max_walltime_hours = self._parse_walltime_to_hours(max_walltime) if max_walltime else None
               min_walltime_hours = self._parse_walltime_to_hours(min_walltime) if min_walltime else None

               queue_configs.append({
                  'name': queue_name,
                  'min_nodes': min_nodes,
                  'max_nodes': max_nodes,
                  'min_walltime_hours': min_walltime_hours,
                  'max_walltime_hours': max_walltime_hours,
                  'defaults': resources_default
               })

         # Re-sort after adding large queue
         queue_configs.sort(key=lambda x: x['min_nodes'])

         return queue_configs

      except Exception as e:
         self.logger.error(f"Error detecting queue configs: {e}")
         return []

   def _get_queue_for_nodes(self, nodect: int, queue_configs: List[Dict[str, Any]]) -> Dict[str, Any]:
      """Find the appropriate queue config for a given node count.

      When a node count is exactly at a boundary between queues (e.g., 2048 is both
      medium's max and large's min), we prefer the larger queue since PBS routing
      typically uses the minimum node count as the entry point.
      """
      # Check in reverse order (largest first) so boundary values go to the larger queue
      for qc in reversed(queue_configs):
         if qc['min_nodes'] <= nodect <= qc['max_nodes']:
            return qc
      # Fallback to last queue (largest)
      return queue_configs[-1] if queue_configs else None

   def _plot_score_grid(self, grid_nodes: List[int], grid_walltimes: List[float],
                        max_time_hours: float, formula: str,
                        queue_configs: List[Dict[str, Any]], server_defaults: Dict[str, Any],
                        output_dir: str, args: argparse.Namespace) -> str:
      """Generate the grid plot with score components"""
      import matplotlib.pyplot as plt
      import numpy as np

      n_cols = len(grid_nodes)
      n_rows = len(grid_walltimes)

      # Create figure with shared axes, tight layout
      fig, axes = plt.subplots(n_rows, n_cols, figsize=(20, 20), sharex=True, sharey=True)

      # Eliminate gaps between subplots
      fig.subplots_adjust(hspace=0, wspace=0)

      # Time array for x-axis
      time_hours = np.linspace(0.1, max_time_hours, 200)
      time_seconds = time_hours * 3600

      # Component colors (consistent across all plots)
      colors = {
         'base': '#888888',      # Gray
         'wfp': '#1f77b4',       # Blue
         'backfill': '#ff7f0e',  # Orange
         'fifo': '#2ca02c'       # Green
      }

      # Track global y-axis max for consistent scaling (only from valid configs)
      global_max_score = 0

      # First pass: calculate all scores to find global max, check validity
      all_scores = {}
      valid_configs = {}
      for row_idx, walltime_hours in enumerate(grid_walltimes):
         walltime_seconds = walltime_hours * 3600
         for col_idx, nodect in enumerate(grid_nodes):
            # Get queue config for this node count
            qc = self._get_queue_for_nodes(nodect, queue_configs)

            # Check if this config is valid for the queue
            is_valid = self._is_valid_job_config(nodect, walltime_hours, qc)
            valid_configs[(row_idx, col_idx)] = is_valid

            if is_valid:
               defaults = qc['defaults'] if qc else server_defaults

               # Calculate components
               components = self._calculate_score_components(
                  time_seconds, nodect, walltime_seconds, defaults, server_defaults, formula
               )
               all_scores[(row_idx, col_idx)] = components

               total = components['base'] + components['wfp'] + components['backfill'] + components['fifo']
               max_total = np.max(total)
               if max_total > global_max_score:
                  global_max_score = max_total

      # Add some headroom and set minimum for log scale
      use_log_scale = getattr(args, 'log_scale', False)
      y_min = 1.0 if use_log_scale else 0  # Minimum value for log scale
      y_max = global_max_score * (1.5 if use_log_scale else 1.1) if global_max_score > 0 else 100

      # Second pass: plot
      for row_idx, walltime_hours in enumerate(grid_walltimes):
         for col_idx, nodect in enumerate(grid_nodes):
            ax = axes[row_idx, col_idx]

            # Get queue config for this node count
            qc = self._get_queue_for_nodes(nodect, queue_configs)
            queue_name = qc['name'] if qc else 'default'

            # Check if this config is valid
            is_valid = valid_configs[(row_idx, col_idx)]

            # Set log scale for y-axis if requested
            if use_log_scale:
               ax.set_yscale('log')

            if not is_valid:
               # Show "Not Allowed" for invalid configurations
               ax.set_facecolor('#f5f5f5')  # Light gray background
               ax.text(0.5, 0.5, 'Not\nAllowed', transform=ax.transAxes,
                       ha='center', va='center', fontsize=14, fontweight='bold',
                       color='#999999', alpha=0.8)
               ax.set_ylim(y_min, y_max)
            else:
               components = all_scores[(row_idx, col_idx)]

               # Plot lines for each component (clip to minimum value for log scale if needed)
               if use_log_scale:
                  base_plot = np.maximum(components['base'], y_min)
                  wfp_plot = np.maximum(components['wfp'], y_min)
                  backfill_plot = np.maximum(components['backfill'], y_min)
                  fifo_plot = np.maximum(components['fifo'], y_min)
               else:
                  base_plot = components['base']
                  wfp_plot = components['wfp']
                  backfill_plot = components['backfill']
                  fifo_plot = components['fifo']

               ax.plot(time_hours, base_plot, color=colors['base'],
                       linewidth=1.5, label='Base', alpha=0.7)
               ax.plot(time_hours, wfp_plot, color=colors['wfp'],
                       linewidth=2, label='WFP')
               ax.plot(time_hours, backfill_plot, color=colors['backfill'],
                       linewidth=2, label='Backfill')
               ax.plot(time_hours, fifo_plot, color=colors['fifo'],
                       linewidth=2, label='FIFO')

               # Total score as dashed black line
               total = components['base'] + components['wfp'] + components['backfill'] + components['fifo']
               if use_log_scale:
                  total_plot = np.maximum(total, y_min)
               else:
                  total_plot = total
               ax.plot(time_hours, total_plot, color='black', linewidth=2,
                       linestyle='--', label='Total', alpha=0.8)

               # Set consistent y-axis
               ax.set_ylim(y_min, y_max)

            # Add queue name annotation in corner
            ax.annotate(queue_name, xy=(0.97, 0.97), xycoords='axes fraction',
                        fontsize=10, ha='right', va='top', color='gray')

            # Only show x-tick labels on bottom row
            if row_idx != n_rows - 1:
               ax.tick_params(axis='x', labelbottom=False)
            else:
               ax.tick_params(axis='x', labelsize=11)

            # Only show y-tick labels on leftmost column
            if col_idx != 0:
               ax.tick_params(axis='y', labelleft=False)
            else:
               ax.tick_params(axis='y', labelsize=11)

            # Light grid
            ax.grid(True, alpha=0.2, linewidth=0.5)

            # Remove individual subplot frames except outer edges
            ax.spines['top'].set_visible(row_idx == 0)
            ax.spines['bottom'].set_visible(row_idx == n_rows - 1)
            ax.spines['left'].set_visible(col_idx == 0)
            ax.spines['right'].set_visible(col_idx == n_cols - 1)

      # Add column headers (node counts) at top
      for col_idx, nodect in enumerate(grid_nodes):
         axes[0, col_idx].set_title(f'{nodect} nodes', fontsize=14, fontweight='bold', pad=12)

      # Add row labels (walltimes) on the left
      for row_idx, walltime_hours in enumerate(grid_walltimes):
         # Format walltime nicely
         if walltime_hours == int(walltime_hours):
            wt_label = f'{int(walltime_hours)}h'
         else:
            wt_label = f'{walltime_hours}h'
         axes[row_idx, 0].set_ylabel(wt_label, fontsize=14, fontweight='bold', rotation=0,
                                      ha='right', va='center', labelpad=20)

      # Add overall axis labels
      fig.text(0.5, 0.02, 'Eligible Time (hours)', ha='center', fontsize=16, fontweight='bold')
      fig.text(0.02, 0.5, 'Walltime', va='center', rotation='vertical', fontsize=16, fontweight='bold')

      # Add legend at top - find a valid axes for legend handles
      legend_handles = None
      legend_labels = None
      for row_idx in range(n_rows):
         for col_idx in range(n_cols):
            if valid_configs[(row_idx, col_idx)]:
               handles, labels = axes[row_idx, col_idx].get_legend_handles_labels()
               if handles:
                  legend_handles = handles
                  legend_labels = labels
                  break
         if legend_handles:
            break

      if legend_handles:
         fig.legend(legend_handles, legend_labels, loc='upper center', ncol=5, fontsize=12,
                    bbox_to_anchor=(0.5, 0.99), frameon=True)

      # Add title
      fig.suptitle('PBS Score Components by Job Configuration\n(Node Count × Walltime)',
                   fontsize=18, fontweight='bold', y=1.03)

      # Save
      path = os.path.join(output_dir, 'score_grid.png')
      fig.savefig(path, bbox_inches='tight', dpi=150)
      plt.close(fig)

      return path

   def _is_valid_job_config(self, nodect: int, walltime_hours: float, queue_config: Dict[str, Any]) -> bool:
      """Check if a job configuration is valid for its queue"""
      if not queue_config:
         return True  # No queue info, assume valid

      # Check node count bounds
      min_nodes = queue_config.get('min_nodes', 0)
      max_nodes = queue_config.get('max_nodes', float('inf'))

      if nodect < min_nodes or nodect > max_nodes:
         return False

      # Check walltime bounds
      max_walltime_hours = queue_config.get('max_walltime_hours')
      min_walltime_hours = queue_config.get('min_walltime_hours')

      if max_walltime_hours is not None and walltime_hours > max_walltime_hours:
         return False

      if min_walltime_hours is not None and walltime_hours < min_walltime_hours:
         return False

      return True

   def _parse_walltime_to_hours(self, walltime: str) -> float:
      """Parse walltime string (HH:MM:SS or D:HH:MM:SS) to hours"""
      if not walltime:
         return 0.0

      try:
         parts = walltime.split(':')
         if len(parts) == 3:
            # HH:MM:SS format
            hours = int(parts[0])
            minutes = int(parts[1])
            seconds = int(parts[2])
            return hours + minutes / 60.0 + seconds / 3600.0
         elif len(parts) == 4:
            # D:HH:MM:SS format
            days = int(parts[0])
            hours = int(parts[1])
            minutes = int(parts[2])
            seconds = int(parts[3])
            return days * 24 + hours + minutes / 60.0 + seconds / 3600.0
         else:
            return 0.0
      except (ValueError, TypeError):
         return 0.0

   def _calculate_score_components(self, time_seconds, nodect: int,
                                    walltime_seconds: float, queue_defaults: Dict[str, Any],
                                    server_defaults: Dict[str, Any], formula: str) -> Dict[str, Any]:
      """Calculate individual score components over time by evaluating the actual PBS formula.

      Args:
         time_seconds: numpy array of time values in seconds
         nodect: Number of nodes
         walltime_seconds: Walltime in seconds
         queue_defaults: Queue-specific default values
         server_defaults: Server-wide default values
         formula: The PBS job_sort_formula string from the server

      Returns:
         Dict with numpy arrays for each component: base, wfp, backfill, fifo
      """
      import numpy as np

      # Merge queue defaults with server defaults (queue takes precedence)
      defaults = {**server_defaults, **queue_defaults}

      # Split formula into individual terms
      terms = self._split_formula_terms(formula)

      # Classify each term
      classified_terms = {}
      for term in terms:
         component = self._classify_term(term)
         if component:
            classified_terms[component] = term

      # Build base variables dict (without eligible_time, which varies)
      base_variables = {
         "base_score": int(defaults.get("base_score", 0)),
         "score_boost": int(defaults.get("score_boost", 0)),
         "enable_wfp": int(defaults.get("enable_wfp", 0)),
         "wfp_factor": int(defaults.get("wfp_factor", 100000)),
         "enable_backfill": int(defaults.get("enable_backfill", 0)),
         "backfill_max": int(defaults.get("backfill_max", 50)),
         "backfill_factor": int(defaults.get("backfill_factor", 84600)),
         "enable_fifo": int(defaults.get("enable_fifo", 1)),
         "fifo_factor": int(defaults.get("fifo_factor", 1800)),
         "total_cpus": int(defaults.get("total_cpus", 10624)),
         "project_priority": 1,  # Default project priority
         "nodect": nodect,
         "walltime": walltime_seconds,
         "min": min,
         "max": max,
      }

      # Initialize result arrays
      base_values = np.zeros_like(time_seconds)
      wfp_values = np.zeros_like(time_seconds)
      backfill_values = np.zeros_like(time_seconds)
      fifo_values = np.zeros_like(time_seconds)

      # Evaluate each term for each time point
      for i, t in enumerate(time_seconds):
         variables = {**base_variables, "eligible_time": t}

         # Evaluate base terms (base_score and score_boost)
         if 'base' in classified_terms:
            try:
               base_values[i] += float(eval(classified_terms['base'], {"__builtins__": {}}, variables))
            except Exception:
               pass
         if 'boost' in classified_terms:
            try:
               base_values[i] += float(eval(classified_terms['boost'], {"__builtins__": {}}, variables))
            except Exception:
               pass

         # Evaluate WFP term
         if 'wfp' in classified_terms:
            try:
               wfp_values[i] = float(eval(classified_terms['wfp'], {"__builtins__": {}}, variables))
            except Exception:
               pass

         # Evaluate backfill term
         if 'backfill' in classified_terms:
            try:
               backfill_values[i] = float(eval(classified_terms['backfill'], {"__builtins__": {}}, variables))
            except Exception:
               pass

         # Evaluate FIFO term
         if 'fifo' in classified_terms:
            try:
               fifo_values[i] = float(eval(classified_terms['fifo'], {"__builtins__": {}}, variables))
            except Exception:
               pass

      return {
         'base': base_values,
         'wfp': wfp_values,
         'backfill': backfill_values,
         'fifo': fifo_values
      }

   def _split_formula_terms(self, formula: str) -> List[str]:
      """Split formula into top-level additive terms.

      Splits on '+' only when not inside parentheses, giving each
      top-level term as a string that can be eval()'d independently.
      """
      terms = []
      current: List[str] = []
      depth = 0

      for char in formula:
         if char == '(':
            depth += 1
            current.append(char)
         elif char == ')':
            depth -= 1
            current.append(char)
         elif char == '+' and depth == 0:
            term = ''.join(current).strip()
            if term:
               terms.append(term)
            current = []
         else:
            current.append(char)

      # Don't forget the last term
      if current:
         term = ''.join(current).strip()
         if term:
            terms.append(term)

      return terms

   def _classify_term(self, term: str) -> Optional[str]:
      """Classify a formula term into its component type.

      Returns one of: 'base', 'boost', 'wfp', 'backfill', 'fifo', or None if unrecognized.
      """
      term_lower = term.lower().strip()

      # Check for specific variable names that identify each component
      if term_lower == 'base_score' or 'base_score' in term_lower and 'enable' not in term_lower:
         # Simple base_score reference
         if 'enable' not in term_lower and 'factor' not in term_lower:
            return 'base'
      if term_lower == 'score_boost':
         return 'boost'
      if 'enable_wfp' in term_lower:
         return 'wfp'
      if 'enable_backfill' in term_lower:
         return 'backfill'
      if 'enable_fifo' in term_lower:
         return 'fifo'

      # Fallback: check if it's just a simple variable reference
      if term.strip() == 'base_score':
         return 'base'
      if term.strip() == 'score_boost':
         return 'boost'

      return None

