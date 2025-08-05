"""
Analyze commands for PBS Monitor CLI

Provides analytics commands like run-score analysis.
"""

import argparse
import logging
from typing import List, Dict, Any, Optional
import pandas as pd

from .commands import BaseCommand
from ..analytics import RunScoreAnalyzer, WalltimeEfficiencyAnalyzer


class AnalyzeCommand(BaseCommand):
   """Command for running analytics analysis"""
   
   def execute(self, args: argparse.Namespace) -> int:
      """Execute the analyze command"""
      if args.analyze_action == "run-score":
         return self._analyze_run_score(args)
      elif args.analyze_action == "walltime-efficiency-by-user":
         return self._analyze_walltime_efficiency_by_user(args)
      elif args.analyze_action == "walltime-efficiency-by-project":
         return self._analyze_walltime_efficiency_by_project(args)
      else:
         self.logger.error(f"Unknown analyze action: {args.analyze_action}")
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