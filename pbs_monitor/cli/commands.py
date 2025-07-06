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
         
         # Check if detailed mode is requested
         if args.detailed:
            return self._show_detailed_nodes(nodes, args)
         else:
            return self._show_node_summary(nodes)
         
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
      
      return 0
   
   def _show_node_summary(self, nodes: List[PBSNode]) -> int:
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