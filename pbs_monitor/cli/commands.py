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
            'score': lambda j: j.format_score()
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
         
         if subcommand == 'init':
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
               self.logger.warning(f"Failed to get PBS completed jobs: {str(e)}")
         
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
      default_columns = ['job_id', 'name', 'owner', 'state', 'queue', 'nodes', 'walltime', 'submit_time', 'queued', 'runtime', 'exit_status']
      columns = args.columns.split(',') if args.columns else default_columns
      
      # Create table data
      headers = []
      column_formatters = {
         'job_id': lambda j: format_job_id(j.job_id),
         'name': lambda j: j.job_name[:30] + "..." if len(j.job_name) > 30 else j.job_name,
         'owner': lambda j: j.owner,
         'state': lambda j: format_state(j.state.value),
         'queue': lambda j: j.queue,
         'nodes': lambda j: format_number(j.nodes),
         'walltime': lambda j: format_duration(j.walltime),
         'submit_time': lambda j: format_timestamp(j.submit_time),
         'start_time': lambda j: format_timestamp(j.start_time),
         'end_time': lambda j: format_timestamp(j.end_time),
         'queued': lambda j: j.queue_duration() or "N/A",
         'runtime': lambda j: self._format_runtime(j),
         'exit_status': lambda j: str(j.exit_status) if j.exit_status is not None else "N/A",
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
   
   def _format_runtime(self, job: PBSJob) -> str:
      """Format job runtime for display"""
      if job.start_time and job.end_time:
         duration = job.end_time - job.start_time
         total_seconds = int(duration.total_seconds())
         hours = total_seconds // 3600
         minutes = (total_seconds % 3600) // 60
         seconds = total_seconds % 60
         return f"{hours:02d}:{minutes:02d}:{seconds:02d}"
      return "N/A"


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
         
         if subcommand == 'start':
            return self._start_daemon(args)
         elif subcommand == 'stop':
            return self._stop_daemon(args)
         elif subcommand == 'status':
            return self._show_daemon_status(args)
         else:
            print(f"Unknown daemon subcommand: {subcommand}")
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
      self.console.print(f"Total Transitions Analyzed: {summary['total_transitions_analyzed']}")
      self.console.print(f"Unique Jobs with Scores: {summary['unique_jobs_with_scores']}")
      
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