"""
Main CLI entry point for PBS Monitor
"""

import argparse
import sys
import os
import logging
from typing import List, Optional

from ..config import Config
from ..utils.logging_setup import setup_logging
from ..data_collector import DataCollector
from .commands import StatusCommand, JobsCommand, NodesCommand, QueuesCommand, DatabaseCommand, HistoryCommand, DaemonCommand


def create_parser() -> argparse.ArgumentParser:
   """Create argument parser for PBS Monitor CLI"""
   
   parser = argparse.ArgumentParser(
      prog="pbs-monitor",
      description="PBS scheduler monitoring and management tools",
      formatter_class=argparse.RawDescriptionHelpFormatter,
      epilog="""
Examples:
  pbs-monitor status              # Show system status
  pbs-monitor jobs                # Show all jobs
  pbs-monitor jobs -u myuser      # Show jobs for specific user
  pbs-monitor history             # Show completed jobs from database
  pbs-monitor history -u myuser   # Show user's completed jobs
  pbs-monitor nodes               # Show node information
  pbs-monitor queues              # Show queue information
  pbs-monitor config --create     # Create sample configuration
      """
   )
   
   # Global options
   parser.add_argument(
      "-c", "--config",
      help="Configuration file path",
      default=None
   )
   
   parser.add_argument(
      "-v", "--verbose",
      action="store_true",
      help="Enable verbose logging"
   )
   
   parser.add_argument(
      "-q", "--quiet",
      action="store_true",
      help="Suppress normal output"
   )
   
   parser.add_argument(
      "--log-file",
      help="Log file path",
      default=None
   )
   
   parser.add_argument(
      "--use-sample-data",
      action="store_true",
      help="Use sample JSON data instead of actual PBS commands (for testing)"
   )
   
   # Create subparsers
   subparsers = parser.add_subparsers(
      dest="command",
      help="Available commands"
   )
   
   # Status command
   status_parser = subparsers.add_parser(
      "status",
      help="Show PBS system status"
   )
   status_parser.add_argument(
      "-r", "--refresh",
      action="store_true",
      help="Force refresh of data"
   )
   status_parser.add_argument(
      "--collect",
      action="store_true",
      help="Collect and persist data to database after displaying"
   )
   
   # Jobs command
   jobs_parser = subparsers.add_parser(
      "jobs",
      help="Show job information"
   )
   jobs_parser.add_argument(
      "-u", "--user",
      help="Filter by username"
   )
   jobs_parser.add_argument(
      "-s", "--state",
      choices=["R", "Q", "H", "W", "T", "E", "S", "C", "F"],
      help="Filter by job state"
   )
   jobs_parser.add_argument(
      "-r", "--refresh",
      action="store_true",
      help="Force refresh of data"
   )
   jobs_parser.add_argument(
      "--columns",
      help="Comma-separated list of columns to display"
   )
   jobs_parser.add_argument(
      "--sort",
      default="score",
      help="Column to sort by: job_id, name, owner, state, queue, nodes, ppn, walltime, priority, cores, score (default: score)"
   )
   jobs_parser.add_argument(
      "--reverse",
      action="store_true",
      help="Sort in ascending order (default is descending for score, ascending for others)"
   )
   jobs_parser.add_argument(
      "--collect",
      action="store_true",
      help="Collect and persist data to database after displaying"
   )
   
   # Nodes command
   nodes_parser = subparsers.add_parser(
      "nodes",
      help="Show node information"
   )
   nodes_parser.add_argument(
      "-s", "--state",
      choices=["free", "offline", "down", "busy", "job-exclusive", "job-sharing"],
      help="Filter by node state"
   )
   nodes_parser.add_argument(
      "-r", "--refresh",
      action="store_true",
      help="Force refresh of data"
   )
   nodes_parser.add_argument(
      "--columns",
      help="Comma-separated list of columns to display"
   )
   nodes_parser.add_argument(
      "-d", "--detailed",
      action="store_true",
      help="Show detailed table format instead of summary"
   )
   nodes_parser.add_argument(
      "--collect",
      action="store_true",
      help="Collect and persist data to database after displaying"
   )
   
   # Queues command
   queues_parser = subparsers.add_parser(
      "queues",
      help="Show queue information"
   )
   queues_parser.add_argument(
      "-r", "--refresh",
      action="store_true",
      help="Force refresh of data"
   )
   queues_parser.add_argument(
      "--columns",
      help="Comma-separated list of columns to display"
   )
   queues_parser.add_argument(
      "--collect",
      action="store_true",
      help="Collect and persist data to database after displaying"
   )
   
   # History command
   history_parser = subparsers.add_parser(
      "history",
      help="Show historical job information from database"
   )
   history_parser.add_argument(
      "-u", "--user",
      help="Filter by username"
   )
   history_parser.add_argument(
      "-d", "--days",
      type=int,
      default=30,
      help="Number of days to look back (default: 30)"
   )
   history_parser.add_argument(
      "-s", "--state",
      choices=["C", "F", "E", "all"],
      default="all",
      help="Filter by completion state: C (completed), F (finished), E (exiting), all (default: all)"
   )
   history_parser.add_argument(
      "--columns",
      help="Comma-separated list of columns to display"
   )
   history_parser.add_argument(
      "--sort",
      default="submit_time",
      help="Column to sort by: job_id, name, owner, state, queue, submit_time, start_time, end_time, runtime (default: submit_time)"
   )
   history_parser.add_argument(
      "--reverse",
      action="store_true",
      help="Sort in reverse order"
   )
   history_parser.add_argument(
      "--limit",
      type=int,
      default=100,
      help="Maximum number of jobs to show (default: 100)"
   )
   history_parser.add_argument(
      "--include-pbs-history",
      action="store_true",
      help="Also include recent completed jobs from qstat -x"
   )
   
   # Config command
   config_parser = subparsers.add_parser(
      "config",
      help="Configuration management"
   )
   config_parser.add_argument(
      "--create",
      action="store_true",
      help="Create sample configuration file"
   )
   config_parser.add_argument(
      "--show",
      action="store_true",
      help="Show current configuration"
   )
   
   # Database command
   database_parser = subparsers.add_parser(
      "database",
      help="Database management"
   )
   database_subparsers = database_parser.add_subparsers(
      dest="database_action",
      help="Database management actions"
   )
   
   # Database init
   db_init_parser = database_subparsers.add_parser(
      "init",
      help="Initialize database with fresh schema"
   )
   db_init_parser.add_argument(
      "--force",
      action="store_true",
      help="Force initialization (drops existing tables)"
   )
   
   # Database migrate
   database_subparsers.add_parser(
      "migrate",
      help="Migrate database to latest schema"
   )
   
   # Database status
   database_subparsers.add_parser(
      "status",
      help="Show database status and information"
   )
   
   # Database validate
   database_subparsers.add_parser(
      "validate",
      help="Validate database schema"
   )
   
   # Database backup
   db_backup_parser = database_subparsers.add_parser(
      "backup",
      help="Create database backup"
   )
   db_backup_parser.add_argument(
      "backup_path",
      nargs="?",
      help="Backup file path (optional)"
   )
   
   # Database restore
   db_restore_parser = database_subparsers.add_parser(
      "restore",
      help="Restore database from backup"
   )
   db_restore_parser.add_argument(
      "backup_path",
      help="Backup file path to restore from"
   )
   
   # Database cleanup
   db_cleanup_parser = database_subparsers.add_parser(
      "cleanup",
      help="Clean up old data from database"
   )
   db_cleanup_parser.add_argument(
      "--job-history-days",
      type=int,
      default=365,
      help="Keep job history for N days (default: 365)"
   )
   db_cleanup_parser.add_argument(
      "--snapshot-days",
      type=int,
      default=90,
      help="Keep snapshots for N days (default: 90)"
   )
   db_cleanup_parser.add_argument(
      "--force",
      action="store_true",
      help="Skip confirmation prompt"
   )
   
   # Daemon command
   daemon_parser = subparsers.add_parser(
      "daemon",
      help="Background data collection daemon management"
   )
   daemon_subparsers = daemon_parser.add_subparsers(
      dest="daemon_action",
      help="Daemon management actions"
   )
   
   # Daemon start
   daemon_start_parser = daemon_subparsers.add_parser(
      "start",
      help="Start background data collection daemon"
   )
   daemon_start_parser.add_argument(
      "--detach",
      action="store_true",
      help="Run daemon in background (detached mode)"
   )
   daemon_start_parser.add_argument(
      "--pid-file",
      help="PID file path (default: ~/.pbs_monitor_daemon.pid)"
   )
   
   # Daemon stop
   daemon_stop_parser = daemon_subparsers.add_parser(
      "stop",
      help="Stop background data collection daemon"
   )
   daemon_stop_parser.add_argument(
      "--pid-file",
      help="PID file path (default: ~/.pbs_monitor_daemon.pid)"
   )
   
   # Daemon status
   daemon_status_parser = daemon_subparsers.add_parser(
      "status",
      help="Show daemon status and recent collection activity"
   )
   
   return parser


def setup_logging_from_args(args: argparse.Namespace, config: Config) -> None:
   """Setup logging based on command line arguments and configuration"""
   
   # Determine log level
   if args.verbose:
      level = logging.DEBUG
   elif args.quiet:
      level = logging.ERROR
   else:
      level = config.get_log_level()
   
   # Determine log file
   log_file = args.log_file or config.logging.log_file
   
   # Setup logging
   setup_logging(
      level=level,
      log_file=log_file,
      log_format=config.logging.log_format,
      date_format=config.logging.date_format,
      console_output=not args.quiet
   )


def handle_config_command(args: argparse.Namespace, config: Config) -> int:
   """Handle configuration management commands"""
   
   if args.create:
      config.create_sample_config()
      print(f"Sample configuration created at {config.config_file}")
      return 0
   
   if args.show:
      print(f"Configuration file: {config.config_file}")
      print(f"PBS command timeout: {config.pbs.command_timeout}s")
      print(f"Job refresh interval: {config.pbs.job_refresh_interval}s")
      print(f"Node refresh interval: {config.pbs.node_refresh_interval}s")
      print(f"Queue refresh interval: {config.pbs.queue_refresh_interval}s")
      print(f"Log level: {config.logging.level}")
      print(f"Use colors: {config.display.use_colors}")
      print(f"Max table width: {config.display.max_table_width}")
      return 0
   
   print("Use --create to create sample configuration or --show to display current settings")
   return 1


def main(argv: Optional[List[str]] = None) -> int:
   """
   Main entry point for PBS Monitor CLI
   
   Args:
      argv: Command line arguments (optional, for testing)
      
   Returns:
      Exit code
   """
   
   # Parse arguments
   parser = create_parser()
   args = parser.parse_args(argv)
   
   # Load configuration
   try:
      config = Config(config_file=args.config)
   except Exception as e:
      print(f"Error loading configuration: {str(e)}", file=sys.stderr)
      return 1
   
   # Setup logging
   setup_logging_from_args(args, config)
   logger = logging.getLogger(__name__)
   
   # Handle no command
   if not args.command:
      parser.print_help()
      return 1
   
   # Handle config command
   if args.command == "config":
      return handle_config_command(args, config)
   
   # Handle database command (doesn't need PBS connection)
   if args.command == "database":
      cmd = DatabaseCommand(None, config)  # No need for collector
      return cmd.execute(args)
   
   # Handle daemon command (doesn't need PBS connection)
   if args.command == "daemon":
      cmd = DaemonCommand(None, config)  # No need for collector
      return cmd.execute(args)
   
   # Initialize data collector for other commands
   try:
      collector = DataCollector(config, use_sample_data=args.use_sample_data)
      
      # Test PBS connection (skip if using sample data)
      if not args.use_sample_data and not collector.test_connection():
         print("Error: Unable to connect to PBS system", file=sys.stderr)
         print("Please ensure PBS commands are available in PATH", file=sys.stderr)
         return 1
      
   except Exception as e:
      logger.error(f"Failed to initialize data collector: {str(e)}")
      print(f"Error: {str(e)}", file=sys.stderr)
      return 1
   
   # Execute command
   try:
      if args.command == "status":
         cmd = StatusCommand(collector, config)
         return cmd.execute(args)
      
      elif args.command == "jobs":
         cmd = JobsCommand(collector, config)
         return cmd.execute(args)
      
      elif args.command == "nodes":
         cmd = NodesCommand(collector, config)
         return cmd.execute(args)
      
      elif args.command == "queues":
         cmd = QueuesCommand(collector, config)
         return cmd.execute(args)
      
      elif args.command == "history":
         cmd = HistoryCommand(collector, config)
         return cmd.execute(args)
      
      else:
         print(f"Unknown command: {args.command}", file=sys.stderr)
         return 1
   
   except KeyboardInterrupt:
      print("\nInterrupted by user", file=sys.stderr)
      return 130
   
   except Exception as e:
      logger.error(f"Command execution failed: {str(e)}")
      print(f"Error: {str(e)}", file=sys.stderr)
      return 1


if __name__ == "__main__":
   sys.exit(main()) 