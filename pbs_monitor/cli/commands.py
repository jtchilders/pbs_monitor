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
from ..utils.formatters import (
   format_duration, format_timestamp, format_memory,
   format_percentage, format_number, format_job_id, format_state
)


class BaseCommand(ABC):
   """Base class for CLI commands"""
   
   def __init__(self, collector: DataCollector, config: Config):
      self.collector = collector
      self.config = config
      self.logger = logging.getLogger(__name__)
      
      # Initialize console for rich output
      self.console = Console(
         width=config.display.max_table_width,
         force_terminal=True if config.display.use_colors else False
      )
   
   @abstractmethod
   def execute(self, args: argparse.Namespace) -> int:
      """Execute the command"""
      pass
   
   def _create_table(self, title: str, headers: List[str], rows: List[List[str]]) -> Table:
      """Create a rich table"""
      table = Table(title=title, show_header=True, header_style="bold magenta")
      
      for header in headers:
         table.add_column(header, style="cyan")
      
      for row in rows:
         table.add_row(*row)
      
      return table
   
   def _print_table(self, title: str, headers: List[str], rows: List[List[str]]) -> None:
      """Print a formatted table"""
      if self.config.display.use_colors:
         table = self._create_table(title, headers, rows)
         self.console.print(table)
      else:
         print(f"\n{title}")
         print(tabulate(rows, headers=headers, tablefmt="grid"))


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
         
         return 0
         
      except Exception as e:
         self.logger.error(f"Status command failed: {str(e)}")
         print(f"Error: {str(e)}")
         return 1


class JobsCommand(BaseCommand):
   """Show job information"""
   
   def execute(self, args: argparse.Namespace) -> int:
      """Execute jobs command"""
      
      try:
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
            return 0
         
         # Determine columns
         columns = args.columns.split(',') if args.columns else self.config.display.default_job_columns
         
         # Create table data
         headers = []
         column_formatters = {
            'job_id': lambda j: format_job_id(j.job_id),
            'name': lambda j: j.job_name[:self.config.display.max_name_length] if self.config.display.truncate_long_names else j.job_name,
            'owner': lambda j: j.owner,
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
         self._print_table(f"Jobs ({len(jobs)} total)", headers, rows)
         
         return 0
         
      except Exception as e:
         self.logger.error(f"Jobs command failed: {str(e)}")
         print(f"Error: {str(e)}")
         return 1


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
            return 0
         
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
         
         return 0
         
      except Exception as e:
         self.logger.error(f"Nodes command failed: {str(e)}")
         print(f"Error: {str(e)}")
         return 1


class QueuesCommand(BaseCommand):
   """Show queue information"""
   
   def execute(self, args: argparse.Namespace) -> int:
      """Execute queues command"""
      
      try:
         # Get queues
         queues = self.collector.get_queues(force_refresh=args.refresh)
         
         if not queues:
            print("No queues found")
            return 0
         
         # Determine columns
         columns = args.columns.split(',') if args.columns else self.config.display.default_queue_columns
         
         # Create table data
         headers = []
         column_formatters = {
            'name': lambda q: q.name,
            'state': lambda q: format_state(q.state.value),
            'type': lambda q: q.queue_type,
            'running': lambda q: format_number(q.running_jobs),
            'queued': lambda q: format_number(q.queued_jobs),
            'total': lambda q: format_number(q.total_jobs),
            'max_running': lambda q: format_number(q.max_running) if q.max_running is not None else "∞",
            'max_queued': lambda q: format_number(q.max_queued) if q.max_queued is not None else "∞",
            'utilization': lambda q: format_percentage(q.utilization_percentage()),
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
         
         return 0
         
      except Exception as e:
         self.logger.error(f"Queues command failed: {str(e)}")
         print(f"Error: {str(e)}")
         return 1 