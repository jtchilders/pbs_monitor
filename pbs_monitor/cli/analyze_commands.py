"""
Analyze commands for PBS Monitor CLI

Provides analytics commands like run-score analysis.
"""

import argparse
import logging
from typing import List, Dict, Any, Optional
import pandas as pd
from datetime import datetime

from .commands import BaseCommand
from ..analytics import RunScoreAnalyzer, WalltimeEfficiencyAnalyzer, ReservationUtilizationAnalyzer, ReservationTrendAnalyzer


class AnalyzeCommand(BaseCommand):
   """Command for running analytics analysis"""
   
   def execute(self, args: argparse.Namespace) -> int:
      """Execute the analyze command"""
      if args.analyze_action is None:
         print("Error: No analyze action specified")
         print("\nAvailable analyze actions:")
         print("  run-now                      Suggest a job shape you can run right now safely")
         print("  run-score                    Analyze job scores at queue → run transitions")
         print("  walltime-efficiency-by-user  Analyze walltime efficiency by user")
         print("  walltime-efficiency-by-project Analyze walltime efficiency by project")
         print("  reservation-utilization      Analyze reservation utilization patterns")
         print("  reservation-trends           Analyze reservation usage trends over time")
         print("  reservation-owner-ranking    Analyze reservation usage by owner ranking")
         print("\nExamples:")
         print("  pbs-monitor analyze run-now                    # Get a run-now suggestion")
         print("  pbs-monitor analyze run-score                    # Analyze job scores")
         print("  pbs-monitor analyze walltime-efficiency-by-user  # Analyze user efficiency")
         print("  pbs-monitor analyze reservation-utilization      # Analyze reservation usage")
         print("\nUse 'pbs-monitor analyze <action> --help' for more information about each action")
         return 1
      elif args.analyze_action == "run-now":
          return self._analyze_run_now(args)
      elif args.analyze_action == "run-score":
         return self._analyze_run_score(args)
      elif args.analyze_action == "walltime-efficiency-by-user":
         return self._analyze_walltime_efficiency_by_user(args)
      elif args.analyze_action == "walltime-efficiency-by-project":
         return self._analyze_walltime_efficiency_by_project(args)
      elif args.analyze_action == "reservation-utilization":
         return self._analyze_reservation_utilization(args)
      elif args.analyze_action == "reservation-trends":
         return self._analyze_reservation_trends(args)
      elif args.analyze_action == "reservation-owner-ranking":
         return self._analyze_reservation_owner_ranking(args)
      else:
         self.logger.error(f"Unknown analyze action: {args.analyze_action}")
         print("\nAvailable actions: run-score, walltime-efficiency-by-user, walltime-efficiency-by-project, reservation-utilization, reservation-trends, reservation-owner-ranking")
         return 1

   def _analyze_run_now(self, args: argparse.Namespace) -> int:
       """Suggest a single best job shape that can run now without delaying queued jobs.

       Logic:
       - Greedily place queued jobs (by score desc) into current free nodes to find the leftover "hole" L.
       - Determine earliest contention time from either:
         a) the next queued job that cannot fit now but could after running-job releases, or
         b) the earliest upcoming CONFIRMED reservation start.
       - Apply safety buffer (default 8 minutes, configurable with --buffer-minutes).
       - Output a single suggestion: nodes=L and max walltime (or open-ended up to 24h if no contention found).
       """
       from datetime import datetime, timedelta
       try:
          buffer_minutes = getattr(args, 'buffer_minutes', 8)
          open_end_cap_hours = 24  # hard cap for "open-ended"

          now = datetime.now()

          # Gather data
          nodes = self.collector.get_nodes(force_refresh=getattr(args, 'refresh', False))
          jobs = self.collector.get_jobs(force_refresh=getattr(args, 'refresh', False))
          reservations = []
          try:
             reservations = self.collector.get_reservations(force_refresh=getattr(args, 'refresh', False))
          except Exception as e:
             # Reservations can fail on some systems; continue without blocking
             self.logger.warning(f"Reservations unavailable: {e}")

          # Count free nodes (full-node model)
          free_nodes_now = sum(1 for n in nodes if getattr(n.state, 'value', '').lower() == 'free')

          # Separate jobs, deriving required nodes robustly (nodect/select) when needed
          queued_jobs = []
          running_jobs = []
          for j in jobs:
             state_val = getattr(j.state, 'value', None)
             if state_val == 'Q':
                req = self._get_required_nodes(j)
                if req and req > 0:
                   queued_jobs.append((j, req))
             elif state_val == 'R':
                req = self._get_required_nodes(j)
                if req and req > 0 and getattr(j, 'start_time', None) and getattr(j, 'walltime', None):
                   running_jobs.append((j, req))

          # Sort queued by score desc (None -> very low)
          def score_key(job_and_req):
             job = job_and_req[0]
             s = getattr(job, 'score', None)
             return s if s is not None else float('-inf')
          queued_jobs.sort(key=score_key, reverse=True)

          # Release events from running jobs
          # Use PBSCommands walltime parser if available; else fallback parser
          def parse_walltime_seconds(wt: str) -> int:
             try:
                if hasattr(self.collector, 'pbs_commands') and hasattr(self.collector.pbs_commands, '_parse_walltime_to_seconds'):
                   return int(self.collector.pbs_commands._parse_walltime_to_seconds(wt))
             except Exception:
                pass
             # Fallback: HH:MM:SS or DD:HH:MM:SS
             try:
                parts = [int(x) for x in str(wt).split(':')]
                if len(parts) == 3:
                   h, m, s = parts
                   return h*3600 + m*60 + s
                if len(parts) == 4:
                   d, h, m, s = parts
                   return d*86400 + h*3600 + m*60 + s
             except Exception:
                return 0
             return 0

          release_events = []  # (datetime, nodes)
          for (j, req_nodes) in running_jobs:
             try:
                secs = parse_walltime_seconds(j.walltime)
                if secs > 0 and j.start_time:
                   release_at = j.start_time + timedelta(seconds=secs)
                   if release_at > now:
                      release_events.append((release_at, int(req_nodes)))
             except Exception:
                continue
          release_events.sort(key=lambda x: x[0])

          # Greedy place queued jobs into current free pool
          free = int(free_nodes_now)
          placed = []  # list of (job, req)
          remaining = []  # list of (job, req)
          for (job, req) in queued_jobs:
             if req <= free:
                placed.append((job, req))
                free -= req
             else:
                remaining.append((job, req))

          if free <= 0:
             self.console.print("[yellow]No immediate backfill window: all free nodes are consumed by queued jobs.[/yellow]")
             # Still show the next contention source for transparency
             next_info = self._compute_horizon(now, 0, remaining, release_events, reservations, buffer_minutes)
             if next_info.get('contention_time'):
                ts = next_info['contention_time']
                src = next_info['contention_source']
                bid = next_info.get('blocking_id') or 'N/A'
                self.console.print(f"[dim]Earliest contention at {ts} from {src} ({bid}).[/dim]")
             return 0

          # There is a hole L = free
          L = free

          # Determine contention horizon
          horizon_info = self._compute_horizon(now, L, remaining, release_events, reservations, buffer_minutes)

          # Build suggestion
          suggestion = {
             'nodes': L,
             'buffer_minutes': buffer_minutes,
          }

          reservations_in_window = []
          if horizon_info['open_ended']:
             # Cap open-ended at 24h
             T_eff = now + timedelta(hours=open_end_cap_hours)
             suggestion['max_walltime_seconds'] = int((T_eff - now).total_seconds())
             suggestion['max_walltime_display'] = self._format_seconds_hhmm(suggestion['max_walltime_seconds'])
             suggestion['contention_source'] = 'none'
             suggestion['earliest_contention'] = None
             # list next reservations up to cap window
             reservations_in_window = self._reservations_within_window(reservations, now, T_eff)
          else:
             T_eff = horizon_info['contention_time']
             # Apply buffer
             wall_secs = int(max(0, (T_eff - now).total_seconds() - buffer_minutes*60))
             if wall_secs <= 0:
                self.console.print("[yellow]No safe walltime window before contention after applying buffer.[/yellow]")
                return 0
             suggestion['max_walltime_seconds'] = wall_secs
             suggestion['max_walltime_display'] = self._format_seconds_hhmm(wall_secs)
             suggestion['contention_source'] = horizon_info['contention_source']
             suggestion['blocking_id'] = horizon_info.get('blocking_id')
             suggestion['earliest_contention'] = T_eff
             # reservations that start before contention
             reservations_in_window = self._reservations_within_window(reservations, now, T_eff)

          # Output
          output_format = getattr(args, 'format', 'table')
          if output_format == 'json':
             self._display_run_now_json(suggestion, reservations_in_window)
          else:
             self._display_run_now_table(suggestion, reservations_in_window)

          return 0
       except Exception as e:
          self.logger.error(f"Error computing run-now suggestion: {str(e)}")
          self.console.print(f"[red]Error: {str(e)}[/red]")
          return 1

   def _compute_horizon(self, now, hole_nodes: int, remaining_jobs, release_events, reservations, buffer_minutes: int) -> Dict[str, Any]:
       """Compute earliest contention time from next queued job or reservations.

       Returns dict with keys:
         - open_ended: bool
         - contention_time: datetime or None
         - contention_source: 'queued_job' | 'reservation' | 'none'
         - blocking_id: job_id or reservation_id when applicable
       """
        # Next queued job contention
       from datetime import datetime
       T_job = None
       blocking_job_id = None
       # Consider earliest time any remaining queued job could start
        # Limit scan to first N to keep this lightweight
       MAX_CHECK = 200
       for (job, req) in (remaining_jobs[:MAX_CHECK] if remaining_jobs else []):
          need = int(req) - int(hole_nodes)
          if need <= 0:
             candidate_T = now
          else:
             released = 0
             candidate_T = None
             for (t, n) in release_events:
                if t <= now:
                   continue
                released += int(n)
                if released >= need:
                   candidate_T = t
                   break
          if candidate_T is not None:
             if T_job is None or candidate_T < T_job:
                T_job = candidate_T
                blocking_job_id = getattr(job, 'job_id', None)

       # Reservation contention (confirmed-only, exclude running)
       T_resv = None
       blocking_resv_id = None
       confirmed_values = {"CONFIRMED", "RESV_CONFIRMED"}
       for r in sorted(reservations or [], key=lambda x: getattr(x, 'start_time', now) or now):
          try:
             state_val = getattr(r.state, 'value', '').upper()
             if state_val in confirmed_values:
                st = getattr(r, 'start_time', None)
                if st and st > now:
                   T_resv = st
                   blocking_resv_id = getattr(r, 'reservation_id', None)
                   break
          except Exception:
             continue

       # Decide horizon
       times = [t for t in [T_job, T_resv] if t is not None]
       if not times:
          return {
             'open_ended': True,
             'contention_time': None,
             'contention_source': 'none'
          }

       T_eff = min(times)
       if T_resv and T_eff == T_resv:
          return {
             'open_ended': False,
             'contention_time': T_eff,
             'contention_source': 'reservation',
             'blocking_id': blocking_resv_id
          }
       return {
          'open_ended': False,
          'contention_time': T_eff,
          'contention_source': 'queued_job',
          'blocking_id': blocking_job_id
       }

   def _reservations_within_window(self, reservations, start_dt, end_dt):
       """Return reservations starting within (start_dt, end_dt] with confirmed states."""
       confirmed_values = {"CONFIRMED", "RESV_CONFIRMED"}
       in_window = []
       for r in reservations or []:
          try:
             st = getattr(r, 'start_time', None)
             if not st:
                continue
             if not (start_dt < st <= end_dt):
                continue
             state_val = getattr(r.state, 'value', '').upper()
             if state_val in confirmed_values:
                in_window.append(r)
          except Exception:
             continue
       return in_window

   def _format_seconds_hhmm(self, total_seconds: int) -> str:
       if total_seconds <= 0:
          return "00:00"
       minutes = total_seconds // 60
       hours = minutes // 60
       mins = minutes % 60
       return f"{hours:02d}:{mins:02d}"

   def _get_required_nodes(self, job: Any) -> Optional[int]:
       """Derive required nodes from job fields.

       Priority:
       - job.nodes if set and >0
       - job.raw_attributes.Resource_List.nodect
       - parse job.raw_attributes.Resource_List.select strings like "select=2:ncpus=64+3:ncpus=32"
       """
       try:
          # 1) direct
          if getattr(job, 'nodes', None):
             return int(job.nodes)
          # 2) from Resource_List
          rl = None
          try:
             rl = job.raw_attributes.get('Resource_List') if hasattr(job, 'raw_attributes') else None
          except Exception:
             rl = None
          if isinstance(rl, dict):
             # nodect
             try:
                nd = rl.get('nodect')
                if nd is not None:
                   nd_int = int(str(nd))
                   if nd_int > 0:
                      return nd_int
             except Exception:
                pass
             # select
             try:
                sel = rl.get('select')
                if sel:
                   # Accept either a plain number or a full string with chunks
                   sel_str = str(sel)
                   if sel_str.isdigit():
                      return int(sel_str)
                   # Sum up counts in patterns like "2:ncpus=64+3:gpus=2"
                   total = 0
                   for chunk in sel_str.split('+'):
                      part = chunk.strip()
                      if not part:
                         continue
                      # Either "2:ncpus=64" or possibly just "2"
                      if ':' in part:
                         count_str = part.split(':', 1)[0]
                      else:
                         count_str = part
                      try:
                         total += int(count_str)
                      except Exception:
                         continue
                   if total > 0:
                      return total
             except Exception:
                pass
       except Exception:
          return None
       return None

   def _display_run_now_table(self, suggestion: Dict[str, Any], reservations_in_window) -> None:
       headers = [
          'Nodes', 'Max Walltime', 'Earliest Contention', 'Contention Source', 'Blocking ID', 'Buffer (min)'
       ]
       row = [
          str(suggestion.get('nodes')),
          suggestion.get('max_walltime_display') or 'open-ended',
          suggestion.get('earliest_contention').isoformat(sep=' ') if suggestion.get('earliest_contention') else 'none',
          suggestion.get('contention_source', 'none'),
          suggestion.get('blocking_id') or 'N/A',
          str(suggestion.get('buffer_minutes'))
       ]
       table = self._create_table(
          title="Run-Now Suggestion",
          headers=headers,
          rows=[row]
       )
       self.console.print(table)

       # Disclaimer: only shown when we have a suggestion
       self.console.print("[dim]Disclaimer: suggested windows are estimates based on current queue behavior and may not start immediately.[/dim]")

       # Reservations in window
       if reservations_in_window:
          from ..utils.formatters import format_timestamp
          resv_headers = ['Reservation ID', 'Owner', 'Start Time', 'Nodes']
          resv_rows = []
          for r in reservations_in_window:
             resv_rows.append([
                getattr(r, 'reservation_id', '')[:30],
                getattr(r, 'owner', '') or '',
                format_timestamp(getattr(r, 'start_time', None)) if getattr(r, 'start_time', None) else '',
                str(getattr(r, 'nodes', '') or '')
             ])
          resv_table = self._create_table(
             title="Reservations within Window",
             headers=resv_headers,
             rows=resv_rows
          )
          self.console.print(resv_table)

   def _display_run_now_json(self, suggestion: Dict[str, Any], reservations_in_window) -> None:
       import json
       out = {
          'suggestion': {
             'nodes': suggestion.get('nodes'),
             'max_walltime_seconds': suggestion.get('max_walltime_seconds'),
             'max_walltime_display': suggestion.get('max_walltime_display'),
             'earliest_contention': suggestion.get('earliest_contention').isoformat() if suggestion.get('earliest_contention') else None,
             'contention_source': suggestion.get('contention_source'),
             'blocking_id': suggestion.get('blocking_id'),
             'buffer_minutes': suggestion.get('buffer_minutes'),
             'disclaimer': 'suggested windows are estimates based on current queue behavior and may not start immediately.'
          },
          'reservations_in_window': [
             {
                'reservation_id': getattr(r, 'reservation_id', None),
                'owner': getattr(r, 'owner', None),
                'start_time': getattr(r, 'start_time', None).isoformat() if getattr(r, 'start_time', None) else None,
                'nodes': getattr(r, 'nodes', None),
             }
             for r in (reservations_in_window or [])
          ]
       }
       self.console.print(json.dumps(out, indent=2))
   
   def _analyze_run_score(self, args: argparse.Namespace) -> int:
      """Analyze job scores at queue → run transitions"""
      try:
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
   
   def _analyze_walltime_efficiency_by_user(self, args: argparse.Namespace) -> int:
      """Analyze walltime efficiency by user"""
      try:
         # Initialize analyzer
         analyzer = WalltimeEfficiencyAnalyzer()
         
         # Get analysis parameters
         days = getattr(args, 'days', 30)
         user = getattr(args, 'user', None)
         min_jobs = getattr(args, 'min_jobs', 3)
         queue = getattr(args, 'queue', None)
         min_nodes = getattr(args, 'min_nodes', None)
         max_nodes = getattr(args, 'max_nodes', None)
         
         # Perform analysis
         filter_desc = self._build_filter_description(queue=queue, min_nodes=min_nodes, max_nodes=max_nodes)
         if user:
            self.console.print(f"[bold blue]Analyzing walltime efficiency for user '{user}' (last {days} days){filter_desc}...[/bold blue]")
         else:
            self.console.print(f"[bold blue]Analyzing walltime efficiency by user (last {days} days){filter_desc}...[/bold blue]")
         
         df = analyzer.analyze_efficiency_by_user(days=days, user=user, min_jobs=min_jobs, 
                                                 queue=queue, min_nodes=min_nodes, max_nodes=max_nodes)
         
         if df.empty:
            self.console.print("[yellow]No efficiency data found for the specified period.[/yellow]")
            return 0
         
         # Get summary statistics
         summary = analyzer.get_analysis_summary(days=days, analysis_type="user")
         
         # Display results
         self._display_walltime_efficiency_results(df, summary, args, "User Walltime Efficiency Analysis")
         
         return 0
         
      except Exception as e:
         self.logger.error(f"Error analyzing walltime efficiency by user: {str(e)}")
         self.console.print(f"[red]Error: {str(e)}[/red]")
         return 1
   
   def _analyze_walltime_efficiency_by_project(self, args: argparse.Namespace) -> int:
      """Analyze walltime efficiency by project"""
      try:
         # Initialize analyzer
         analyzer = WalltimeEfficiencyAnalyzer()
         
         # Get analysis parameters
         days = getattr(args, 'days', 30)
         project = getattr(args, 'project', None)
         min_jobs = getattr(args, 'min_jobs', 3)
         queue = getattr(args, 'queue', None)
         min_nodes = getattr(args, 'min_nodes', None)
         max_nodes = getattr(args, 'max_nodes', None)
         
         # Perform analysis
         filter_desc = self._build_filter_description(queue=queue, min_nodes=min_nodes, max_nodes=max_nodes)
         if project:
            self.console.print(f"[bold blue]Analyzing walltime efficiency for project '{project}' (last {days} days){filter_desc}...[/bold blue]")
         else:
            self.console.print(f"[bold blue]Analyzing walltime efficiency by project (last {days} days){filter_desc}...[/bold blue]")
         
         df = analyzer.analyze_efficiency_by_project(days=days, project=project, min_jobs=min_jobs,
                                                    queue=queue, min_nodes=min_nodes, max_nodes=max_nodes)
         
         if df.empty:
            self.console.print("[yellow]No efficiency data found for the specified period.[/yellow]")
            return 0
         
         # Get summary statistics
         summary = analyzer.get_analysis_summary(days=days, analysis_type="project")
         
         # Display results
         self._display_walltime_efficiency_results(df, summary, args, "Project Walltime Efficiency Analysis")
         
         return 0
         
      except Exception as e:
         self.logger.error(f"Error analyzing walltime efficiency by project: {str(e)}")
         self.console.print(f"[red]Error: {str(e)}[/red]")
         return 1
   
   def _display_walltime_efficiency_results(self, df: pd.DataFrame, summary: Dict[str, Any], 
                                          args: argparse.Namespace, title: str) -> None:
      """Display walltime efficiency analysis results"""
      
      # Show summary
      self.console.print(f"\n[bold green]{title} Summary[/bold green]")
      self.console.print(f"Analysis Period: {summary['analysis_period_days']} days")
      self.console.print(f"Total Completed Jobs: {summary['total_completed_jobs']}")
      self.console.print(f"Jobs with Efficiency Data: {summary['jobs_with_efficiency_data']}")
      
      # Format output based on requested format
      output_format = getattr(args, 'format', 'table')
      min_jobs = getattr(args, 'min_jobs', 3)
      
      if output_format == 'csv':
         self._display_efficiency_csv_output(df)
      else:
         self._display_efficiency_table_output(df, title, min_jobs)
   
   def _display_efficiency_table_output(self, df: pd.DataFrame, title: str, min_jobs: int) -> None:
      """Display efficiency results in table format"""
      
      if df.empty:
         self.console.print("[yellow]No data to display.[/yellow]")
         return
      
      # Prepare table data - all rows from DataFrame
      headers = list(df.columns)
      rows = []
      
      # Track where insufficient data starts (if any)
      insufficient_data_start = None
      
      for i, (_, row) in enumerate(df.iterrows()):
         table_row = [str(row[col]) for col in headers]
         
         # Check if this is the first row with insufficient jobs
         if insufficient_data_start is None and int(row['Jobs']) < min_jobs:
            insufficient_data_start = i
         
         rows.append(table_row)
      
      # Create and display table
      table = self._create_table(
         title=title,
         headers=headers,
         rows=rows
      )
      
      self.console.print(table)
      
      # Add explanatory notes
      if insufficient_data_start is not None and insufficient_data_start > 0:
         self.console.print(f"\n[dim]Note: Entries with fewer than {min_jobs} jobs are shown at the end for completeness but may not represent reliable statistics.[/dim]")
      elif insufficient_data_start == 0:
         self.console.print(f"\n[dim]Note: All entries have fewer than {min_jobs} jobs and may not represent reliable statistics.[/dim]")
      
      self.console.print(f"\n[dim]Efficiency is calculated as (actual runtime / requested walltime) × 100%, capped at 100%.[/dim]")
   
   def _display_efficiency_csv_output(self, df: pd.DataFrame) -> None:
      """Display efficiency results in CSV format"""
      
      # Output CSV
      self.console.print(df.to_csv(index=False))
   
   def _build_filter_description(self, queue: Optional[str] = None, 
                               min_nodes: Optional[int] = None, max_nodes: Optional[int] = None) -> str:
      """Build a description of active filters for display"""
      filters = []
      
      if queue:
         filters.append(f"queue '{queue}'")
      
      if min_nodes is not None and max_nodes is not None:
         filters.append(f"nodes {min_nodes}-{max_nodes}")
      elif min_nodes is not None:
         filters.append(f"nodes ≥{min_nodes}")
      elif max_nodes is not None:
         filters.append(f"nodes ≤{max_nodes}")
      
      if filters:
         return f" with filters: {', '.join(filters)}"
      return ""
   
   def _parse_date(self, date_str: Optional[str]) -> Optional[datetime]:
      """Parse date string in YYYY-MM-DD format"""
      if not date_str:
         return None
      try:
         return datetime.strptime(date_str, "%Y-%m-%d")
      except ValueError:
         raise ValueError(f"Invalid date format: {date_str}. Use YYYY-MM-DD format.")
   
   def _analyze_reservation_utilization(self, args: argparse.Namespace) -> int:
      """Analyze reservation utilization efficiency"""
      try:
         # Initialize analyzer
         analyzer = ReservationUtilizationAnalyzer()
         
         # Get reservation ID(s)
         reservation_ids = getattr(args, 'reservation_ids', None)
         start_date = self._parse_date(getattr(args, 'start_date', None))
         end_date = self._parse_date(getattr(args, 'end_date', None))
         
         if reservation_ids:
            # Analyze specific reservations
            self.console.print(f"[bold blue]Analyzing utilization for reservations: {', '.join(reservation_ids)}[/bold blue]")
            
            utilizations = []
            for res_id in reservation_ids:
               try:
                  utilization = analyzer.analyze_reservation_utilization(
                     res_id, start_date, end_date
                  )
                  utilizations.append(utilization)
               except Exception as e:
                  self.console.print(f"[red]Error analyzing reservation {res_id}: {str(e)}[/red]")
         else:
            # Analyze all reservations in time period
            self.console.print(f"[bold blue]Analyzing utilization for all reservations[/bold blue]")
            utilizations = analyzer.analyze_multiple_reservations(
               start_date=start_date, end_date=end_date
            )
         
         if not utilizations:
            self.console.print("[yellow]No reservation utilization data found.[/yellow]")
            return 0
         
         # Get summary statistics
         summary = analyzer.get_utilization_summary(start_date, end_date)
         
         # Display results
         self._display_reservation_utilization_results(utilizations, summary, args)
         
         return 0
         
      except Exception as e:
         self.logger.error(f"Error analyzing reservation utilization: {str(e)}")
         self.console.print(f"[red]Error: {str(e)}[/red]")
         return 1
   
   def _analyze_reservation_trends(self, args: argparse.Namespace) -> int:
      """Analyze reservation utilization trends over time"""
      try:
         # Initialize analyzer with database manager
         analyzer = ReservationTrendAnalyzer()
         
         # Get analysis parameters
         days = getattr(args, 'days', 30)
         owner = getattr(args, 'owner', None)
         queue = getattr(args, 'queue', None)
         
         self.console.print(f"[bold blue]Analyzing reservation utilization trends (last {days} days)...[/bold blue]")
         
         # Perform analysis
         df = analyzer.analyze_utilization_trends(days=days, owner=owner, queue=queue)
         
         if df.empty:
            self.console.print("[yellow]No trend data found for the specified period.[/yellow]")
            return 0
         
         # Display results
         self._display_reservation_trends_results(df, args, days, owner, queue)
         
         return 0
         
      except Exception as e:
         self.logger.error(f"Error analyzing reservation trends: {str(e)}")
         self.console.print(f"[red]Error: {str(e)}[/red]")
         return 1
   
   def _analyze_reservation_owner_ranking(self, args: argparse.Namespace) -> int:
      """Analyze reservation owner efficiency rankings"""
      try:
         # Initialize analyzer with database manager
         analyzer = ReservationTrendAnalyzer()
         
         # Get analysis parameters
         days = getattr(args, 'days', 30)
         
         self.console.print(f"[bold blue]Analyzing reservation owner efficiency rankings (last {days} days)...[/bold blue]")
         
         # Perform analysis
         df = analyzer.get_owner_efficiency_ranking(days=days)
         
         if df.empty:
            self.console.print("[yellow]No owner ranking data found for the specified period.[/yellow]")
            return 0
         
         # Display results
         self._display_reservation_owner_ranking_results(df, args, days)
         
         return 0
         
      except Exception as e:
         self.logger.error(f"Error analyzing reservation owner rankings: {str(e)}")
         self.console.print(f"[red]Error: {str(e)}[/red]")
         return 1
   
   def _display_reservation_utilization_results(self, utilizations: List, summary: Dict[str, Any], args: argparse.Namespace) -> None:
      """Display reservation utilization analysis results"""
      
      # Show summary
      self.console.print(f"\n[bold green]Reservation Utilization Analysis Summary[/bold green]")
      self.console.print(f"Total Reservations Analyzed: {summary['total_reservations']}")
      self.console.print(f"Average Utilization: {summary['avg_utilization']:.1f}%")
      self.console.print(f"Median Utilization: {summary['median_utilization']:.1f}%")
      self.console.print(f"Underutilized (<50%): {summary['underutilized_count']}")
      self.console.print(f"Well Utilized (≥80%): {summary['well_utilized_count']}")
      
      # Format output based on requested format
      output_format = getattr(args, 'format', 'table')
      
      if output_format == 'csv':
         self._display_reservation_utilization_csv(utilizations)
      else:
         self._display_reservation_utilization_table(utilizations)
   
   def _display_reservation_utilization_table(self, utilizations: List) -> None:
      """Display reservation utilization results in table format"""
      
      if not utilizations:
         self.console.print("[yellow]No utilization data to display.[/yellow]")
         return
      
      # Prepare table data
      headers = [
         'Reservation ID', 'Reservation Name', 'Owner', 'Queue', 'Utilization %', 
         'Node Hours Reserved', 'Node Hours Used', 'Jobs Submitted', 'Jobs Completed'
      ]
      rows = []
      
      for util in utilizations:
         rows.append([
            util['reservation_id'][:20] + '...' if len(util['reservation_id']) > 20 else util['reservation_id'],
            util.get('reservation_name', '') or '',
            util['owner'],
            util['queue'],
            f"{util['utilization_percentage']:.1f}%",
            f"{util['total_node_hours_reserved']:.1f}",
            f"{util['total_node_hours_used']:.1f}",
            str(util['jobs_submitted']),
            str(util['jobs_completed'])
         ])
      
      # Create and display table
      table = self._create_table(
         title="Reservation Utilization Analysis",
         headers=headers,
         rows=rows
      )
      
      self.console.print(table)
   
   def _display_reservation_utilization_csv(self, utilizations: List) -> None:
      """Display reservation utilization results in CSV format"""
      
      if not utilizations:
         return
      
      # Convert to DataFrame for CSV output
      data = []
      for util in utilizations:
         data.append({
            'reservation_id': util['reservation_id'],
            'reservation_name': util.get('reservation_name', '') or '',
            'owner': util['owner'],
            'queue': util['queue'],
            'utilization_percentage': util['utilization_percentage'],
            'node_hours_reserved': util['total_node_hours_reserved'],
            'node_hours_used': util['total_node_hours_used'],
            'jobs_submitted': util['jobs_submitted'],
            'jobs_completed': util['jobs_completed'],
            'cpu_utilization_percentage': util['cpu_utilization_percentage'],
            'gpu_utilization_percentage': util['gpu_utilization_percentage']
         })
      
      df = pd.DataFrame(data)
      self.console.print(df.to_csv(index=False))
   
   def _display_reservation_trends_results(self, df: pd.DataFrame, args: argparse.Namespace, 
                                         days: int, owner: Optional[str], queue: Optional[str]) -> None:
      """Display reservation trends analysis results"""
      
      # Build filter description
      filter_desc = ""
      if owner:
         filter_desc += f" for owner '{owner}'"
      if queue:
         filter_desc += f" in queue '{queue}'"
      
      self.console.print(f"\n[bold green]Reservation Utilization Trends{filter_desc}[/bold green]")
      self.console.print(f"Analysis Period: Last {days} days")
      
      # Format output based on requested format
      output_format = getattr(args, 'format', 'table')
      
      if output_format == 'csv':
         self.console.print(df.to_csv(index=False))
      else:
         # Display as table
         if not df.empty:
            headers = list(df.columns)
            rows = []
            for _, row in df.iterrows():
               rows.append([str(row[col]) for col in headers])
            
            table = self._create_table(
               title="Daily Reservation Utilization Trends",
               headers=headers,
               rows=rows
            )
            self.console.print(table)
         else:
            self.console.print("[yellow]No trend data to display.[/yellow]")
   
   def _display_reservation_owner_ranking_results(self, df: pd.DataFrame, args: argparse.Namespace, days: int) -> None:
      """Display reservation owner ranking results"""
      
      self.console.print(f"\n[bold green]Reservation Owner Efficiency Rankings[/bold green]")
      self.console.print(f"Analysis Period: Last {days} days")
      
      # Format output based on requested format
      output_format = getattr(args, 'format', 'table')
      
      if output_format == 'csv':
         self.console.print(df.to_csv(index=False))
      else:
         # Display as table
         if not df.empty:
            headers = list(df.columns)
            rows = []
            for _, row in df.iterrows():
               rows.append([str(row[col]) for col in headers])
            
            table = self._create_table(
               title="Reservation Owner Efficiency Rankings",
               headers=headers,
               rows=rows
            )
            self.console.print(table)
         else:
            self.console.print("[yellow]No ranking data to display.[/yellow]") 