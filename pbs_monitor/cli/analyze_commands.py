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
         print("  run-score                    Analyze job scores at queue → run transitions")
         print("  walltime-efficiency-by-user  Analyze walltime efficiency by user")
         print("  walltime-efficiency-by-project Analyze walltime efficiency by project")
         print("  reservation-utilization      Analyze reservation utilization patterns")
         print("  reservation-trends           Analyze reservation usage trends over time")
         print("  reservation-owner-ranking    Analyze reservation usage by owner ranking")
         print("\nExamples:")
         print("  pbs-monitor analyze run-score                    # Analyze job scores")
         print("  pbs-monitor analyze walltime-efficiency-by-user  # Analyze user efficiency")
         print("  pbs-monitor analyze reservation-utilization      # Analyze reservation usage")
         print("\nUse 'pbs-monitor analyze <action> --help' for more information about each action")
         return 1
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
         'Reservation ID', 'Owner', 'Queue', 'Utilization %', 
         'Node Hours Reserved', 'Node Hours Used', 'Jobs Submitted', 'Jobs Completed'
      ]
      rows = []
      
      for util in utilizations:
         rows.append([
            util['reservation_id'][:20] + '...' if len(util['reservation_id']) > 20 else util['reservation_id'],
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