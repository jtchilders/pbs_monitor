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
from .commands import StatusCommand, JobsCommand, NodesCommand, QueuesCommand, DatabaseCommand, HistoryCommand, DaemonCommand, ReservationsCommand, ScoreFormulaCommand
from .analyze_commands import AnalyzeCommand
from ..replay.cli import ReplayCommand


def create_parser() -> argparse.ArgumentParser:
   """Create argument parser for PBS Monitor CLI"""
   
   parser = argparse.ArgumentParser(
      prog="pbs-monitor",
      description="""PBS scheduler monitoring and management tools

Configuration file locations (searched in order):
  ~/.pbs_monitor.yaml
  ~/.config/pbs_monitor/config.yaml
  /etc/pbs_monitor/config.yaml
  pbs_monitor.yaml (current directory)

Many command options (columns, display settings, etc.) can be configured in the config file.""",
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
   
   # Table width control options
   parser.add_argument(
      "--max-width",
      type=int,
      help="Maximum table width (overrides config)"
   )
   
   parser.add_argument(
      "--auto-width",
      action="store_true",
      help="Auto-detect terminal width"
   )
   
   parser.add_argument(
      "--no-expand",
      action="store_true",
      help="Don't expand columns to fit content"
   )
   
   parser.add_argument(
      "--wrap",
      action="store_true",
      help="Enable word wrapping in table cells"
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
   status_parser.add_argument(
      "--queue-depth",
      action="store_true",
      help="Show detailed queue depth breakdown by job size"
   )
   
   # Jobs command
   jobs_parser = subparsers.add_parser(
      "jobs",
      help="Show job information"
   )
   jobs_parser.add_argument(
      "job_ids",
      nargs="*",
      help="Specific job IDs to show details for (numerical portion only, e.g., 12345)"
   )
   jobs_parser.add_argument(
      "-u", "--user",
      help="Filter by username"
   )
   jobs_parser.add_argument(
      "-p", "--project",
      help="Filter by project name (partial string matching)"
   )
   jobs_parser.add_argument(
      "-q", "--queue",
      help="Filter by queue name (case-insensitive exact match)"
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
      help="Comma-separated list of columns to display: job_id, name, owner, project, allocation, state, queue, nodes, ppn, walltime, walltime_actual, memory, submit_time, start_time, end_time, runtime, priority, cores, score, queue_time, exit_status, execution_node"
   )
   jobs_parser.add_argument(
      "--sort",
      default="score",
      help="Column to sort by: job_id, name, owner, project, allocation, state, queue, nodes, ppn, walltime, priority, cores, score (default: score)"
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
   jobs_parser.add_argument(
      "-d", "--detailed",
      action="store_true",
      help="Show detailed information for specific jobs (formatted sections instead of table)"
   )
   jobs_parser.add_argument(
      "--history",
      action="store_true",
      help="Include job history from database (for detailed view)"
   )
   jobs_parser.add_argument(
      "--json",
      action="store_true",
      help="Output job details in JSON format"
   )
   jobs_parser.add_argument(
      "--show-raw",
      action="store_true",
      help="Show raw PBS attributes (for detailed view)"
   )
   
   # Nodes command
   nodes_parser = subparsers.add_parser(
      "nodes",
      help="Show node information"
   )
   nodes_parser.add_argument(
      "node_ids",
      nargs="*",
      help="Optional node IDs to filter by (space separated)"
   )
   nodes_parser.add_argument(
      "-s", "--state",
      choices=["free", "offline", "down", "busy", "job-exclusive", "job-sharing", "resv-exclusive", "down,offline", "state-unknown,down", "state-unknown,down,offline"],
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
   
   # Reservations command
   reservations_parser = subparsers.add_parser(
      "resv",
      help="Reservation information and management",
      aliases=["reservations", "reserv"]
   )
   
   # Reservation subcommands
   resv_subparsers = reservations_parser.add_subparsers(
      dest="reservation_action",
      help="Reservation actions"
   )
   
   # List reservations
   list_parser = resv_subparsers.add_parser(
      "list",
      help="List reservations"
   )
   list_parser.add_argument("-u", "--user", help="Filter by user")
   list_parser.add_argument("-s", "--state", help="Filter by state")
   list_parser.add_argument("-r", "--refresh", action="store_true", help="Force refresh of data")
   list_parser.add_argument("--collect", action="store_true", help="Collect data to database")
   list_parser.add_argument("--format", choices=["table", "json"], default="table", help="Output format")
   list_parser.add_argument("--columns", help="Comma-separated list of columns to display. Available: reservation_id, name, owner, state, type, start_time, end_time, duration, nodes, queue")
   
   # Show reservation details  
   show_parser = resv_subparsers.add_parser(
      "show",
      help="Show detailed reservation information"
   )
   show_parser.add_argument("reservation_ids", nargs="*", help="Reservation IDs to show")
   show_parser.add_argument("--format", choices=["table", "json", "yaml"], default="table", help="Output format")
   show_parser.add_argument("--show-nodes", action="store_true", help="Show all reserved nodes (not truncated)")
   
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
      "-p", "--project",
      help="Filter by project name (partial string matching, case-sensitive)"
   )
   history_parser.add_argument(
      "-d", "--days",
      type=int,
      default=30,
      help="Number of days to look back (default: 30)"
   )
   history_parser.add_argument(
      "-s", "--state",
      choices=["C", "F", "E", "UNKNOWN_END", "all"],
      default="all",
      help="Filter by completion state: C (completed), F (finished), E (exiting), UNKNOWN_END (orphaned), all (default: all)"
   )
   history_parser.add_argument(
      "--columns",
      help="Comma-separated list of columns to display: job_id, name, owner, project, allocation, state, queue, nodes, walltime, submit_time, start_time, end_time, queued, runtime, exit_status, cores"
   )
   history_parser.add_argument(
      "--sort",
      default="submit_time",
      help="Column to sort by: job_id, name, owner, project, allocation, state, queue, nodes, walltime, submit_time, start_time, end_time, queued, runtime (default: submit_time)"
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
   
   # Analyze command
   analyze_parser = subparsers.add_parser(
      "analyze",
      help="Analytics and analysis commands"
   )
   analyze_subparsers = analyze_parser.add_subparsers(
      dest="analyze_action",
      help="Analysis actions"
   )
   
   # Analyze time-comparison
   time_comp_parser = analyze_subparsers.add_parser(
      "time-comparison",
      help="Compare throughput and metrics between two time periods"
   )
   time_comp_parser.add_argument(
      "--a-lower", required=True, help="Start of Period A (ISO 8601, e.g. YYYY-MM-DDTHH:MM)"
   )
   time_comp_parser.add_argument(
      "--a-upper", required=True, help="End of Period A (ISO 8601)"
   )
   time_comp_parser.add_argument(
      "--b-lower", required=True, help="Start of Period B (ISO 8601)"
   )
   time_comp_parser.add_argument(
      "--b-upper", required=True, help="End of Period B (ISO 8601)"
   )
   time_comp_parser.add_argument(
      "--group-by", choices=["queue", "project", "allocation_type"], default="queue",
      help="Group by category (default: queue)"
   )
   time_comp_parser.add_argument(
      "--output-dir", default="plots/comparison", help="Directory to save plots"
   )
   time_comp_parser.add_argument(
       "--format", choices=["table", "csv"], default="table", help="Metrics output format"
   )

   # Analyze run-now
   run_now_parser = analyze_subparsers.add_parser(
      "run-now",
      help="Suggest a job shape you can run right now safely"
   )
   run_now_parser.add_argument(
      "-b", "--buffer-minutes",
      type=int,
      default=8,
      help="Safety buffer before contention in minutes (default: 8)"
   )
   run_now_parser.add_argument(
      "--format",
      choices=["table", "json"],
      default="table",
      help="Output format (default: table)"
   )
   run_now_parser.add_argument(
      "-r", "--refresh",
      action="store_true",
      help="Force refresh of data"
   )
   
   # Analyze run-score
   run_score_parser = analyze_subparsers.add_parser(
      "run-score",
      help="Analyze job scores at queue → run transitions"
   )
   run_score_parser.add_argument(
      "-d", "--days",
      type=int,
      default=30,
      help="Number of days to analyze (default: 30)"
   )
   run_score_parser.add_argument(
      "--format",
      choices=["table", "csv"],
      default="table",
      help="Output format (default: table)"
   )
   
   # Analyze walltime-efficiency-by-user
   walltime_user_parser = analyze_subparsers.add_parser(
      "walltime-efficiency-by-user",
      help="Analyze walltime efficiency by user"
   )
   walltime_user_parser.add_argument(
      "-d", "--days",
      type=int,
      default=30,
      help="Number of days to analyze (default: 30)"
   )
   walltime_user_parser.add_argument(
      "-u", "--user",
      help="Filter to specific user (partial match, case-sensitive)"
   )
   walltime_user_parser.add_argument(
      "--min-jobs",
      type=int,
      default=3,
      help="Minimum number of jobs required for main table inclusion (default: 3)"
   )
   walltime_user_parser.add_argument(
      "-q", "--queue",
      help="Filter by queue name (partial match, case-sensitive)"
   )
   walltime_user_parser.add_argument(
      "--min-nodes",
      type=int,
      help="Minimum number of nodes required for job inclusion"
   )
   walltime_user_parser.add_argument(
      "--max-nodes",
      type=int,
      help="Maximum number of nodes allowed for job inclusion"
   )
   walltime_user_parser.add_argument(
      "--format",
      choices=["table", "csv"],
      default="table",
      help="Output format (default: table)"
   )
   
   # Analyze walltime-efficiency-by-project
   walltime_project_parser = analyze_subparsers.add_parser(
      "walltime-efficiency-by-project",
      help="Analyze walltime efficiency by project"
   )
   walltime_project_parser.add_argument(
      "-d", "--days",
      type=int,
      default=30,
      help="Number of days to analyze (default: 30)"
   )
   walltime_project_parser.add_argument(
      "-p", "--project",
      help="Filter to specific project (partial match, case-sensitive)"
   )
   walltime_project_parser.add_argument(
      "--min-jobs",
      type=int,
      default=3,
      help="Minimum number of jobs required for main table inclusion (default: 3)"
   )
   walltime_project_parser.add_argument(
      "-q", "--queue",
      help="Filter by queue name (partial match, case-sensitive)"
   )
   walltime_project_parser.add_argument(
      "--min-nodes",
      type=int,
      help="Minimum number of nodes required for job inclusion"
   )
   walltime_project_parser.add_argument(
      "--max-nodes",
      type=int,
      help="Maximum number of nodes allowed for job inclusion"
   )
   walltime_project_parser.add_argument(
      "--format",
      choices=["table", "csv"],
      default="table",
      help="Output format (default: table)"
   )
   
   # Analyze reservation-utilization
   reservation_util_parser = analyze_subparsers.add_parser(
      "reservation-utilization",
      help="Analyze reservation utilization efficiency"
   )
   reservation_util_parser.add_argument(
      "reservation_ids",
      nargs="*",
      help="Specific reservation IDs to analyze (if not provided, analyzes all)"
   )
   reservation_util_parser.add_argument(
      "--start-date",
      type=str,
      help="Start date for analysis period (YYYY-MM-DD format)"
   )
   reservation_util_parser.add_argument(
      "--end-date",
      type=str,
      help="End date for analysis period (YYYY-MM-DD format)"
   )
   reservation_util_parser.add_argument(
      "--format",
      choices=["table", "csv"],
      default="table",
      help="Output format (default: table)"
   )
   reservation_util_parser.add_argument(
      "--status",
      choices=["running", "future", "all"],
      default="all",
      help="Filter reservations by status: running (currently active), future (not yet started), or all (default: all)"
   )
   reservation_util_parser.add_argument(
      "-d", "--days",
      type=int,
      default=7,
      help="Number of days to look back from today for reservations (default: 7). Includes reservations starting in the last N days and future reservations"
   )
   
   # Analyze reservation-trends
   reservation_trends_parser = analyze_subparsers.add_parser(
      "reservation-trends",
      help="Analyze reservation utilization trends over time"
   )
   reservation_trends_parser.add_argument(
      "-d", "--days",
      type=int,
      default=30,
      help="Number of days to analyze (default: 30)"
   )
   reservation_trends_parser.add_argument(
      "-o", "--owner",
      help="Filter by reservation owner"
   )
   reservation_trends_parser.add_argument(
      "-q", "--queue",
      help="Filter by queue name"
   )
   reservation_trends_parser.add_argument(
      "--format",
      choices=["table", "csv"],
      default="table",
      help="Output format (default: table)"
   )
   
   # Analyze reservation-owner-ranking
   reservation_ranking_parser = analyze_subparsers.add_parser(
      "reservation-owner-ranking",
      help="Rank reservation owners by utilization efficiency"
   )
   reservation_ranking_parser.add_argument(
      "-d", "--days",
      type=int,
      default=30,
      help="Number of days to analyze (default: 30)"
   )
   reservation_ranking_parser.add_argument(
      "--format",
      choices=["table", "csv"],
      default="table",
      help="Output format (default: table)"
   )

   # Analyze usage-insights (Milestone 1)
   usage_insights_parser = analyze_subparsers.add_parser(
      "usage-insights",
      help="Usage insights derived metrics and initial plots"
   )
   usage_insights_parser.add_argument(
      "-d", "--days",
      type=int,
      default=30,
      help="Number of days to analyze (default: 30)"
   )
   usage_insights_parser.add_argument(
       "-m", "--min-queue-node-hours",
      type=float,
      default=100.0,
      help="Minimum requested node-hours per queue to include (default: 100)"
   )
   usage_insights_parser.add_argument(
       "-n", "--top-n-queues",
      type=int,
      help="Limit to top-N queues by requested node-hours"
   )
   usage_insights_parser.add_argument(
       "-R", "--incl-resv",
       action="store_true",
       help="Include reservation queues (names like M12345/R12345/S12345) in analysis"
    )
   usage_insights_parser.add_argument(
       "-a", "--allowlist-queues",
      nargs='+',
      help="Queues to always include regardless of thresholds"
   )
   usage_insights_parser.add_argument(
       "-x", "--ignore-queues",
      nargs='+',
      help="Queues to exclude from analysis and plots"
   )
   usage_insights_parser.add_argument(
       "-o", "--output-dir",
       default="plots",
      help="Directory to save plots"
   )
   usage_insights_parser.add_argument(
       "-P", "--no-plots",
      action="store_true",
      help="Do not generate plots; only compute metrics"
   )
   usage_insights_parser.add_argument(
       "-f", "--format",
      choices=["table", "csv"],
      default="table",
      help="Metrics output format (default: table)"
   )
   usage_insights_parser.add_argument(
       "-t", "--ts-freq",
      choices=["H", "D", "W"],
      default="D",
      help="Time-series frequency for advanced plots: H (hourly), D (daily), W (weekly)"
   )
   usage_insights_parser.add_argument(
      "--total-cluster-nodes",
      type=int,
      help=argparse.SUPPRESS
   )
   
   # Analyze leaderboard
   leaderboard_parser = analyze_subparsers.add_parser(
      "leaderboard",
      help="Show top users and projects by node-hours"
   )
   leaderboard_parser.add_argument(
      "-d", "--days",
      type=int,
      help="Number of days to analyze (default: 30 if neither --days nor --weeks specified)"
   )
   leaderboard_parser.add_argument(
      "-w", "--weeks",
      type=int,
      help="Number of weeks to analyze (shows weekly breakdown)"
   )
   leaderboard_parser.add_argument(
      "-n", "--top-n",
      type=int,
      default=10,
      help="Number of top entries to show (default: 10)"
   )
   leaderboard_parser.add_argument(
      "--min-node-hours",
      type=float,
      default=1.0,
      help="Minimum node-hours to be included (default: 1.0)"
   )
   leaderboard_parser.add_argument(
      "--include-running",
      action="store_true",
      default=True,
      help="Include currently running jobs (default: True)"
   )
   leaderboard_parser.add_argument(
      "--include-queued",
      action="store_true",
      default=False,
      help="Include queued jobs with estimated node-hours (default: False)"
   )
   leaderboard_parser.add_argument(
      "--format",
      choices=["table", "csv"],
      default="table",
      help="Output format (default: table)"
   )

   # Score formula command
   score_formula_parser = subparsers.add_parser(
      "score-formula",
      help="Display and explain the PBS job sort formula"
   )

   # General options
   score_formula_parser.add_argument(
      "--raw",
      action="store_true",
      help="Show only the raw formula string"
   )
   score_formula_parser.add_argument(
      "--no-defaults",
      action="store_true",
      help="Hide the parameters table with default values"
   )
   score_formula_parser.add_argument(
      "-r", "--refresh",
      action="store_true",
      help="Force refresh of server data"
   )
   score_formula_parser.add_argument(
      "--job-ids",
      nargs="+",
      help="Specific job IDs to display parameters for (also used for --plot sampling)"
   )

   # Plot options group
   plot_group = score_formula_parser.add_argument_group(
      "Plot Options (--plot)",
      "Options for generating score evolution plots"
   )
   plot_group.add_argument(
      "--plot",
      action="store_true",
      help="Generate score evolution plots"
   )
   plot_group.add_argument(
      "--output-dir",
      default="plots/score_formula",
      help="Directory to save plots (default: plots/score_formula)"
   )
   plot_group.add_argument(
      "--nodes",
      type=int,
      help="Number of nodes for interactive config"
   )
   plot_group.add_argument(
      "--walltime",
      help="Walltime for interactive config (HH:MM:SS)"
   )
   plot_group.add_argument(
      "--project-priority",
      type=int,
      default=1,
      help="Project priority for interactive config (default: 1)"
   )
   plot_group.add_argument(
      "--sample-count",
      type=int,
      default=6,
      help="Number of jobs to sample from queue (default: 6)"
   )
   plot_group.add_argument(
      "--max-time-hours",
      type=float,
      default=48.0,
      help="Maximum eligible time to plot in hours (default: 48)"
   )

   # Grid plot options group
   grid_group = score_formula_parser.add_argument_group(
      "Grid Plot Options (--plot-grid)",
      "Options for generating grid plots showing score components"
   )
   grid_group.add_argument(
      "--plot-grid",
      action="store_true",
      help="Generate high-resolution grid plot showing score components across node/walltime combinations"
   )
   grid_group.add_argument(
      "--grid-nodes",
      nargs="+",
      type=int,
      default=[256, 512, 1024, 2048, 4096, 8192],
      help="Node counts for grid columns (default: 256 512 1024 2048 4096 8192)"
   )
   grid_group.add_argument(
      "--grid-walltimes",
      nargs="+",
      type=float,
      default=[3, 6, 10, 12, 18, 24],
      help="Walltimes in hours for grid rows (default: 3 6 10 12 18 24)"
   )
   grid_group.add_argument(
      "--routing-queue",
      default="prod",
      help="Routing queue to detect execution queues from (default: prod)"
   )
   grid_group.add_argument(
      "--log-scale",
      action="store_true",
      help="Use logarithmic scale for y-axis in grid plot"
   )

   # Replay command
   replay_parser = subparsers.add_parser(
      "replay",
      help="Replay historical job timelines with split-panel display"
   )
   replay_parser.add_argument(
      "--start",
      help="Start time (ISO format like 2024-01-15 or relative like '24h ago')"
   )
   replay_parser.add_argument(
      "--end",
      help="End time (ISO format or 'now', default: now)"
   )
   replay_parser.add_argument(
      "-u", "--user",
      help="Filter by username"
   )
   replay_parser.add_argument(
      "-q", "--queue",
      help="Filter by queue name"
   )
   replay_parser.add_argument(
      "-p", "--project",
      help="Filter by project name"
   )
   replay_parser.add_argument(
      "--output-format",
      choices=["split-panel", "text", "timeline", "waffle"],
      default="split-panel",
      help="Output format (default: split-panel)"
   )
   replay_parser.add_argument(
      "--step",
      default="1h",
      help="Time step for stepping/animation (e.g., '5m', '1h', default: 1h)"
   )
   replay_parser.add_argument(
      "--top-n",
      type=int,
      default=None,
      help="Number of jobs to show per panel. If not set, auto-sizes to fit terminal height."
   )
   replay_parser.add_argument(
      "--live",
      action="store_true",
      help="Live mode: continuously update display"
   )
   # Waffle chart options
   replay_parser.add_argument(
      "--color-by",
      choices=["job", "user", "queue", "project", "allocation"],
      default="queue",
      help="How to color the waffle chart (default: queue)"
   )
   replay_parser.add_argument(
      "--grid-rows",
      type=int,
      default=72,
      help="Number of rows in waffle grid (default: 72)"
   )
   replay_parser.add_argument(
      "--grid-cols",
      type=int,
      default=144,
      help="Number of columns in waffle grid (default: 144)"
   )
   replay_parser.add_argument(
      "--small-job-threshold",
      type=int,
      default=None,
      help="Node threshold for bundling small jobs in 'job' color mode (default: nodes per cell)"
   )
   replay_parser.add_argument(
      "--output-dir",
      default=".",
      help="Output directory for waffle chart frames and GIF"
   )
   replay_parser.add_argument(
      "--frame-duration",
      type=int,
      default=1000,
      help="Duration of each frame in GIF in milliseconds (default: 1000)"
   )

   # Web dashboard command
   web_parser = subparsers.add_parser(
      "web",
      help="Start the web dashboard server"
   )
   web_parser.add_argument(
      "--port",
      type=int,
      default=8080,
      help="Port to bind to (default: 8080)"
   )
   web_parser.add_argument(
      "--host",
      type=str,
      default="127.0.0.1",
      help="Host to bind to (default: 127.0.0.1, localhost only)"
   )
   web_parser.add_argument(
      "--no-browser",
      action="store_true",
      help="Don't attempt to open browser automatically"
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
   
   # Database show
   db_show_parser = database_subparsers.add_parser(
      "show",
      help="Show table data from database"
   )
   db_show_parser.add_argument(
      "-t", "--table",
      required=True,
      help="Table name to show data from"
   )
   db_show_parser.add_argument(
      "-a", "--after",
      type=int,
      help="Show last N rows (mutually exclusive with -b and -s/-n)"
   )
   db_show_parser.add_argument(
      "-b", "--before",
      type=int,
      help="Show first N rows (mutually exclusive with -a and -s/-n)"
   )
   db_show_parser.add_argument(
      "-s", "--start",
      type=int,
      help="Starting row number (use with -n, mutually exclusive with -a and -b)"
   )
   db_show_parser.add_argument(
      "-n", "--num-rows",
      type=int,
      help="Number of rows to show (use with -s, mutually exclusive with -a and -b)"
   )
   db_show_parser.add_argument(
      "--format",
      choices=["table", "csv"],
      default="table",
      help="Output format (default: table)"
   )
   
   # Database refresh-cache
   db_refresh_cache_parser = database_subparsers.add_parser(
      "refresh-cache",
      help="Recalculate derived/analytical cache tables used by the web dashboard"
   )
   db_refresh_cache_parser.add_argument(
      "-t", "--table",
      choices=["utilization", "all"],
      default="all",
      help="Which cache table to refresh (default: all)"
   )
   db_refresh_cache_parser.add_argument(
      "-d", "--days",
      type=int,
      default=14,
      help="Recalculate reservations whose start_time is within the last N days (default: 14)"
   )
   db_refresh_cache_parser.add_argument(
      "--limit",
      type=int,
      default=None,
      help="Only process the most recent N reservations"
   )
   db_refresh_cache_parser.add_argument(
      "--dry-run",
      action="store_true",
      help="Calculate and print results without writing to the database"
   )
   db_refresh_cache_parser.add_argument(
      "--force",
      action="store_true",
      help="Bypass read-only checks and attempt writes regardless"
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
      "--foreground", "-f",
      action="store_true",
      help="Run daemon in foreground (don't detach)"
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


def apply_cli_overrides(args: argparse.Namespace, config: Config) -> None:
   """Apply command-line overrides to configuration"""
   
   # Apply table width overrides
   if hasattr(args, 'max_width') and args.max_width:
      config.display.max_table_width = args.max_width
   
   if hasattr(args, 'auto_width') and args.auto_width:
      config.display.auto_width = True
   
   if hasattr(args, 'no_expand') and args.no_expand:
      config.display.expand_columns = False
   
   if hasattr(args, 'wrap') and args.wrap:
      config.display.word_wrap = True


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
      print(f"Auto width: {config.display.auto_width}")
      print(f"Expand columns: {config.display.expand_columns}")
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
   
   # Apply command-line overrides to config
   apply_cli_overrides(args, config)
   
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

   # Handle web command (doesn't need PBS connection)
   if args.command == "web":
       try:
           import uvicorn
           from pbs_monitor.web.server import create_app
           
           app = create_app(config)
           host = args.host
           port = args.port
           
           print(f"Starting PBS Monitor Dashboard on http://{host}:{port}")
           print(f"Database: {config.database.url}")
           print(f"")
           print(f"Access via SSH tunnel:")
           print(f"  ssh -L {port}:localhost:{port} $(hostname)")
           print(f"  Then open http://localhost:{port} in your browser")
           print(f"")
           print("Press Ctrl+C to stop")
           
           uvicorn.run(app, host=host, port=port, log_level="info")
           return 0
       except ImportError as e:
           print(f"Error: Missing web dependencies. Install with: pip install fastapi uvicorn[standard]", file=sys.stderr)
           print(f"Details: {e}", file=sys.stderr)
           return 1
       except Exception as e:
           logger.error(f"Web server error: {str(e)}")
           print(f"Error: {str(e)}", file=sys.stderr)
           return 1
   
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
      
      elif args.command == "analyze":
         cmd = AnalyzeCommand(collector, config)
         return cmd.execute(args)
      
      elif args.command in ["resv", "reservations", "reserv"]:
         cmd = ReservationsCommand(collector, config)
         return cmd.execute(args)

      elif args.command == "score-formula":
         cmd = ScoreFormulaCommand(collector, config)
         return cmd.execute(args)

      elif args.command == "replay":
         cmd = ReplayCommand(collector, config)
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
