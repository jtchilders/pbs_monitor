"""
Analyze commands for PBS Monitor CLI

Provides analytics commands like run-score analysis.
"""

import argparse
import logging
from typing import List, Dict, Any, Optional
import pandas as pd

from .commands import BaseCommand
from ..analytics import RunScoreAnalyzer


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