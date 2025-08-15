"""
Usage Insights analytics and plotting

Milestone 1 implements:
- Derived metrics DataFrame for jobs in a time window
- Initial plots:
  - Score at start vs wait time (by queue)
  - Score at start vs requested node-hours (by queue)
  - Start-score distribution by queue
  - ECDF of wait time by queue
  - Rolling median start-score over time (per queue)

Outputs can be saved to disk and/or returned to callers (e.g., notebooks).
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple
from datetime import datetime, timedelta

import logging
import math
import os
import subprocess
import shutil
import re
from functools import partial

import numpy as np
import pandas as pd

try:
   import matplotlib.pyplot as plt
   import seaborn as sns
except Exception:  # pragma: no cover - plotting is optional for headless testing
   plt = None
   sns = None

from sqlalchemy.orm import Session
from sqlalchemy import and_, func, or_

from ..database.repositories import RepositoryFactory
from ..database.models import Job, JobHistory, JobState


_LOGGER = logging.getLogger(__name__)


@dataclass
class QueueFilter:
   days: int = 30
   min_queue_node_hours: float = 100.0
   top_n_queues: Optional[int] = None
   allowlist_queues: Optional[List[str]] = None
   ignore_queues: Optional[List[str]] = None
   include_reservations: bool = False
   reservation_queue_regex: str = r'^[MRS]\d+$'


class UsageInsights:
   """Compute usage insight metrics and generate plots."""

   def __init__(self, repository_factory: Optional[RepositoryFactory] = None):
      self.repo_factory = repository_factory or RepositoryFactory()
      self.logger = logging.getLogger(__name__)

   # --------- Public API ---------
   def build_job_metrics(
      self,
      queue_filter: QueueFilter,
   ) -> pd.DataFrame:
      """
      Build a DataFrame of job-level derived metrics for jobs that:
      1) Started within the window
      2) Are currently queued

      Columns include:
      - job_id, owner, project, queue
      - nodes, walltime_hours
      - submit_time, start_time, end_time
      - wait_time_hours, run_time_hours, requested_node_hours
      - start_score
      - start_score_quantile (within queue over the window)
      - state (job state)
      """
      with self.repo_factory.get_job_repository().get_session() as session:
         cutoff_start = datetime.now() - timedelta(days=queue_filter.days)

         # Get both started and queued jobs
         started_jobs = self._query_started_jobs(session, cutoff_start)
         queued_jobs = self._query_queued_jobs(session)
         jobs = started_jobs + queued_jobs

         if not jobs:
            return pd.DataFrame(columns=[
               'job_id', 'owner', 'project', 'queue', 'nodes', 'walltime_hours',
               'submit_time', 'start_time', 'end_time', 'wait_time_hours',
               'run_time_hours', 'requested_node_hours', 'start_score',
               'start_score_quantile', 'state'
            ])

         # Build raw records with derived metrics
         records: List[Dict[str, object]] = []
         for job in jobs:
            try:
               walltime_hours = self._parse_walltime_to_hours(job.walltime)
               wait_h = self._compute_wait_hours(job.submit_time, job.start_time)
               run_h = self._compute_run_hours(job.start_time, job.end_time)
               start_score = self._find_start_score(session, job)
               requested_node_hours = (job.nodes or 0) * walltime_hours
               records.append({
                  'job_id': job.job_id,
                  'owner': job.owner,
                  'project': job.project,
                  'queue': job.queue,
                  'nodes': int(job.nodes or 0),
                  'walltime_hours': float(walltime_hours),
                  'submit_time': job.submit_time,
                  'start_time': job.start_time,
                  'end_time': job.end_time,
                  'wait_time_hours': float(wait_h),
                  'run_time_hours': float(run_h) if run_h is not None else np.nan,
                  'requested_node_hours': float(requested_node_hours),
                  'start_score': float(start_score) if start_score is not None else np.nan,
                  'state': str(job.state) if job.state else None,
               })
            except Exception as e:  # robust to malformed rows
               self.logger.debug(f"Skipping job {getattr(job, 'job_id', '?')}: {e}")
               continue

         if not records:
            return pd.DataFrame()

         df = pd.DataFrame.from_records(records)

         # Queue filtering based on node-hours
         df = self._filter_queues(df, queue_filter)

         # Add start_score_quantile within queue for the window
         df['start_score_quantile'] = (
            df.groupby('queue')['start_score']
              .transform(lambda s: s.rank(pct=True, method='average'))
         )

         # Slowdown: (wait + run)/max(run, eps). Use eps=1 minute in hours for stability
         eps_hours = 1.0 / 60.0
         if 'run_time_hours' in df.columns:
            denom = df['run_time_hours'].fillna(eps_hours).clip(lower=eps_hours)
            df['slowdown'] = (df['wait_time_hours'].fillna(0) + df['run_time_hours'].fillna(0)) / denom
         else:
            df['slowdown'] = np.nan

         return df

   def generate_plots(
      self,
      df: pd.DataFrame,
      save_dir: Optional[str] = None,
      dpi: int = 120
   ) -> Dict[str, str]:
      """
      Generate milestone-1 plots. Returns mapping of plot name to saved file path when saved.
      If save_dir is None or plotting backends unavailable, returns an empty dict.
      """
      outputs: Dict[str, str] = {}
      if df.empty:
         self.logger.warning("No data to plot - dataframe is empty")
         return outputs
      if plt is None or sns is None:
         self.logger.warning("Plotting libraries not available - matplotlib or seaborn import failed")
         return outputs

      os.makedirs(save_dir, exist_ok=True) if save_dir else None

      # Common aesthetics
      sns.set_context('talk')
      sns.set_style('whitegrid')

      # Build a consistent palette for queues across plots
      try:
         queues = sorted(df['queue'].dropna().astype(str).unique().tolist())
      except Exception:
         queues = []
      queue_palette = self._build_queue_palette(queues)

      # 1) Score at start vs wait time (by queue)
      try:
         g = sns.FacetGrid(df, col='queue', col_wrap=3, sharex=False, sharey=False)
         g.map_dataframe(partial(self._hex_or_scatter, palette=queue_palette), 'wait_time_hours', 'start_score')
         g.set_axis_labels('Wait time (hours, log)', 'Score at start')
         for ax in g.axes.ravel():
            ax.set_xscale('log')
         g.fig.suptitle('Score at start vs Wait time (by queue)', y=1.02)
         if save_dir:
            pth = os.path.join(save_dir, 'score_vs_wait_by_queue.png')
            g.fig.savefig(pth, bbox_inches='tight', dpi=dpi)
            outputs['score_vs_wait_by_queue'] = pth
         plt.close(g.fig)
      except Exception as e:
         self.logger.debug(f"Plot score_vs_wait_by_queue failed: {e}")

      # 2) Score at start vs requested node-hours (by queue)
      try:
         g = sns.FacetGrid(df, col='queue', col_wrap=3, sharex=False, sharey=False)
         g.map_dataframe(partial(self._hex_or_scatter, palette=queue_palette), 'requested_node_hours', 'start_score')
         g.set_axis_labels('Requested node-hours (log)', 'Score at start')
         for ax in g.axes.ravel():
            ax.set_xscale('log')
         g.fig.suptitle('Score at start vs Requested node-hours (by queue)', y=1.02)
         if save_dir:
            pth = os.path.join(save_dir, 'score_vs_node_hours_by_queue.png')
            g.fig.savefig(pth, bbox_inches='tight', dpi=dpi)
            outputs['score_vs_node_hours_by_queue'] = pth
         plt.close(g.fig)
      except Exception as e:
         self.logger.debug(f"Plot score_vs_node_hours_by_queue failed: {e}")

      # 3) Start-score distribution by queue (violin + box)
      try:
         fig, ax = plt.subplots(figsize=(10, 6))
         sns.violinplot(data=df, x='queue', y='start_score', inner=None, ax=ax, cut=0, palette=queue_palette)
         sns.boxplot(data=df, x='queue', y='start_score', ax=ax, width=0.25, showcaps=True, boxprops={'facecolor':'none'})
         ax.set_title('Start-score distribution by queue')
         ax.set_xlabel('Queue')
         ax.set_ylabel('Score at start')
         fig.autofmt_xdate(rotation=30)
         if save_dir:
            pth = os.path.join(save_dir, 'start_score_distribution_by_queue.png')
            fig.savefig(pth, bbox_inches='tight', dpi=dpi)
            outputs['start_score_distribution_by_queue'] = pth
         plt.close(fig)
      except Exception as e:
         self.logger.debug(f"Plot start_score_distribution_by_queue failed: {e}")

      # 4) ECDF of wait time by queue
      try:
         fig, ax = plt.subplots(figsize=(14, 6))
         for q, sub in df.groupby('queue'):
            x = np.sort(sub['wait_time_hours'].dropna().values)
            if x.size == 0:
               continue
            y = np.arange(1, x.size + 1) / x.size
            ax.step(x, y, where='post', label=str(q), color=queue_palette.get(str(q)))
         ax.set_xscale('log')
         ax.set_xlabel('Wait time (hours, log)')
         ax.set_ylabel('ECDF')
         ax.set_title('ECDF of wait time by queue')
         ax.legend(loc='upper left', bbox_to_anchor=(1.02, 1), borderaxespad=0, fontsize='small', title='Queue', frameon=False)
         if save_dir:
            pth = os.path.join(save_dir, 'ecdf_wait_by_queue.png')
            fig.savefig(pth, bbox_inches='tight', dpi=dpi)
            outputs['ecdf_wait_by_queue'] = pth
         plt.close(fig)
      except Exception as e:
         self.logger.debug(f"Plot ecdf_wait_by_queue failed: {e}")

      # 5) Rolling median start-score over time (per queue)
      try:
         fig, ax = plt.subplots(figsize=(14, 6))
         # Resample per queue on start_time, rolling median 7 days
         if not pd.api.types.is_datetime64_any_dtype(df['start_time']):
            df['start_time'] = pd.to_datetime(df['start_time'])
         for q, sub in df[['start_time','queue','start_score']].dropna().groupby('queue'):
            sub_sorted = sub.sort_values('start_time').set_index('start_time')
            med = sub_sorted['start_score'].rolling('7D', min_periods=3).median()
            ax.plot(med.index, med.values, label=str(q), color=queue_palette.get(str(q)))
         ax.set_title('Rolling 7-day median of start-score (per queue)')
         ax.set_xlabel('Time')
         ax.set_ylabel('Start-score (median)')
         ax.legend(loc='upper left', bbox_to_anchor=(1.02, 1), borderaxespad=0, fontsize='small', title='Queue', frameon=False)
         if save_dir:
            pth = os.path.join(save_dir, 'rolling_median_start_score_by_queue.png')
            fig.savefig(pth, bbox_inches='tight', dpi=dpi)
            outputs['rolling_median_start_score_by_queue'] = pth
         plt.close(fig)
      except Exception as e:
         self.logger.debug(f"Plot rolling_median_start_score_by_queue failed: {e}")

      return outputs

   def generate_plots_extended(
      self,
      df: pd.DataFrame,
      days: int = 30,
      save_dir: Optional[str] = None,
      dpi: int = 120,
      per_user_top_n: int = 20,
      per_user_min_jobs: int = 3,
      ts_freq: str = 'D'
   ) -> Dict[str, str]:
      """
      Generate advanced plot suite:
      - Throughput over time (stacked area of node-hours started per day) by queue
      - Backlog over time (node-hours queued) by queue
      - Active nodes over time by queue (stacked area)
      - Per-user distributions and summaries

      Returns mapping of plot name to saved file path when saved.
      """
      outputs: Dict[str, str] = {}
      if df.empty:
         self.logger.warning("No data to plot - dataframe is empty")
         return outputs
      if plt is None or sns is None:
         self.logger.warning("Plotting libraries not available - matplotlib or seaborn import failed")
         return outputs

      try:
         if not pd.api.types.is_datetime64_any_dtype(df['submit_time']):
            df['submit_time'] = pd.to_datetime(df['submit_time'], errors='coerce')
         if not pd.api.types.is_datetime64_any_dtype(df['start_time']):
            df['start_time'] = pd.to_datetime(df['start_time'], errors='coerce')
         if 'end_time' in df.columns and not pd.api.types.is_datetime64_any_dtype(df['end_time']):
            df['end_time'] = pd.to_datetime(df['end_time'], errors='coerce')
      except Exception:
         pass

      window_start = pd.Timestamp.now(tz=None) - pd.Timedelta(days=int(days))

      os.makedirs(save_dir, exist_ok=True) if save_dir else None

      sns.set_context('talk')
      sns.set_style('whitegrid')

      # Consistent palette across all extended plots
      try:
         queues = sorted(df['queue'].dropna().astype(str).unique().tolist())
      except Exception:
         queues = []
      queue_palette = self._build_queue_palette(queues)

      # ---- Throughput over time (node-hours started per period) by queue ----
      try:
         th_df = self._compute_throughput_timeseries(df, window_start, freq=ts_freq)
         if not th_df.empty:
            pivot = th_df.pivot_table(index='timestamp', columns='queue', values='node_hours', aggfunc='sum').fillna(0.0)
            fig, ax = plt.subplots(figsize=(14, 6))
            color_order = [queue_palette.get(str(c)) for c in pivot.columns]
            pivot.plot.area(ax=ax, color=color_order)
            ax.set_title(f'Throughput over time (requested node-hours started per {ts_freq})')
            ax.set_xlabel('Time')
            ax.set_ylabel('Requested node-hours started')
            ax.legend(loc='upper left', bbox_to_anchor=(1.02, 1), borderaxespad=0, fontsize='small', title='Queue', frameon=False)
            if save_dir:
               pth = os.path.join(save_dir, f'throughput_node_hours_per_{ts_freq}.png')
               fig.savefig(pth, bbox_inches='tight', dpi=dpi)
               outputs['throughput_node_hours'] = pth
            plt.close(fig)
      except Exception as e:
         self.logger.debug(f"Plot throughput_node_hours failed: {e}")

      # ---- Queue depth over time (machine-hours queued) by queue ----
      try:
         bl_df = self._compute_backlog_timeseries(df, window_start, freq=ts_freq)
         if not bl_df.empty:
            pivot = bl_df.pivot_table(index='timestamp', columns='queue', values='machine_hours', aggfunc='sum').fillna(0.0)
            fig, ax = plt.subplots(figsize=(14, 6))
            color_order = [queue_palette.get(str(c)) for c in pivot.columns]
            pivot.plot.area(ax=ax, color=color_order)
            ax.set_title(f'Queue depth over time (machine-hours queued per {ts_freq})')
            ax.set_xlabel('Time')
            ax.set_ylabel('Machine-hours queued')
            ax.legend(loc='upper left', bbox_to_anchor=(1.02, 1), borderaxespad=0, fontsize='small', title='Queue', frameon=False)
            if save_dir:
               pth = os.path.join(save_dir, f'backlog_node_hours_per_{ts_freq}.png')
               fig.savefig(pth, bbox_inches='tight', dpi=dpi)
               outputs['backlog_node_hours'] = pth
            plt.close(fig)
      except Exception as e:
         self.logger.debug(f"Plot backlog_node_hours failed: {e}")

      # ---- Current wait time distribution by queue ----
      try:
         self.logger.debug("Computing current wait bins...")
         wait_bins = self._compute_current_wait_bins(df)
         self.logger.debug(f"Wait bins result: {len(wait_bins)} rows")
         if not wait_bins.empty:
            # Aggregate across all queues to get total count per wait bin
            total_by_bin = wait_bins.groupby('wait_bin')['count'].sum().reset_index()
            
            # Ensure all bins are present (even with 0 count)
            all_bins = ['<1hr', '1-6hrs', '6-12hrs', '12-24hrs', 
                       '1-2days', '2-7days', '7-14days', '2-3weeks', '3-5weeks', '>1month']
            total_by_bin = total_by_bin.set_index('wait_bin').reindex(all_bins, fill_value=0).reset_index()

            # Plot simple bar chart
            fig, ax = plt.subplots(figsize=(12, 6))
            bars = ax.bar(total_by_bin['wait_bin'], total_by_bin['count'], width=0.8)
            
            # Add value labels on top of bars
            for bar in bars:
               height = bar.get_height()
               if height > 0:
                  ax.text(bar.get_x() + bar.get_width()/2., height,
                         f'{int(height)}',
                         ha='center', va='bottom')
            
            ax.set_title('Current wait time distribution of queued jobs')
            ax.set_xlabel('Wait time')
            ax.set_ylabel('Number of jobs')
            plt.xticks(rotation=45)
            if save_dir:
               pth = os.path.join(save_dir, 'current_wait_distribution.png')
               fig.savefig(pth, bbox_inches='tight', dpi=dpi)
               outputs['current_wait_distribution'] = pth
            plt.close(fig)
         else:
            self.logger.warning("No data to plot - current_wait_distribution is empty")
      except Exception as e:
         self.logger.debug(f"Plot current_wait_distribution failed: {e}")

      # ---- Active nodes over time by queue ----
      try:
         an_df = self._compute_active_nodes_timeseries(df, window_start, freq=ts_freq)
         if not an_df.empty:
            pivot = an_df.pivot_table(index='timestamp', columns='queue', values='nodes', aggfunc='sum').fillna(0.0)
            fig, ax = plt.subplots(figsize=(14, 6))
            color_order = [queue_palette.get(str(c)) for c in pivot.columns]
            pivot.plot.area(ax=ax, color=color_order)
            ax.set_title(f'Active nodes over time by queue (per {ts_freq})')
            ax.set_xlabel('Time')
            ax.set_ylabel('Active nodes')
            ax.legend(loc='upper left', bbox_to_anchor=(1.02, 1), borderaxespad=0, fontsize='small', title='Queue', frameon=False)
            if save_dir:
               pth = os.path.join(save_dir, f'active_nodes_per_{ts_freq}.png')
               fig.savefig(pth, bbox_inches='tight', dpi=dpi)
               outputs['active_nodes'] = pth
            plt.close(fig)
      except Exception as e:
         self.logger.debug(f"Plot active_nodes failed: {e}")

      # ---- Utilization: percent of capacity used per period ----
      try:
         total_nodes = self._detect_total_cluster_nodes()
         if total_nodes and int(total_nodes) > 0:
            used_df = self._compute_used_node_hours_timeseries(df, window_start, freq=ts_freq)
            if not used_df.empty:
               # Build full timeline to include zero-usage bins
               now_ts = pd.Timestamp.now(tz=None)
               start_bin = window_start.to_period(ts_freq).to_timestamp()
               end_bin = now_ts.to_period(ts_freq).to_timestamp()
               full_idx = pd.date_range(start=start_bin, end=end_bin, freq=ts_freq)
               used_series = used_df.set_index('timestamp')['used_node_hours'].reindex(full_idx, fill_value=0.0)

               # Compute capacity hours per bin
               offset = pd.tseries.frequencies.to_offset(ts_freq)
               cap_hours = []
               for t in used_series.index:
                  candidate_next = t + offset
                  # Clip last bin to now
                  next_t = min(candidate_next, now_ts)
                  hours = max(0.0, (next_t - t).total_seconds() / 3600.0)
                  cap_hours.append(hours)
               cap_hours = pd.Series(cap_hours, index=used_series.index)
               capacity_node_hours = cap_hours.astype(float) * float(int(total_nodes))
               eps = 1e-9
               utilization_pct = (used_series.astype(float) / capacity_node_hours.clip(lower=eps)) * 100.0

               fig, ax = plt.subplots(figsize=(14, 6))
               ax.plot(utilization_pct.index, utilization_pct.values, label='Utilization')
               ax.set_title(f'Utilization over time (% of capacity used per {ts_freq})')
               ax.set_xlabel('Time')
               ax.set_ylabel('Utilization (%)')
               ax.set_ylim(0, 100)
               if save_dir:
                  pth = os.path.join(save_dir, f'utilization_percent_per_{ts_freq}.png')
                  fig.savefig(pth, bbox_inches='tight', dpi=dpi)
                  outputs['utilization_percent'] = pth
               plt.close(fig)
      except Exception as e:
         self.logger.debug(f"Plot utilization_percent failed: {e}")

      # ---- Per-user distributions ----
      try:
         # Focus on users with at least per_user_min_jobs, take top N by job count
         counts = df.groupby('owner')['job_id'].count().sort_values(ascending=False)
         selected_users = counts[counts >= int(per_user_min_jobs)].head(int(per_user_top_n)).index.tolist()
         sub = df[df['owner'].isin(selected_users)].copy()

         if not sub.empty:
            # 1) Slowdown distributions by user (violin)
            try:
               fig, ax = plt.subplots(figsize=(12, 6))
               sns.violinplot(data=sub, x='owner', y='slowdown', inner=None, cut=0, ax=ax)
               sns.boxplot(data=sub, x='owner', y='slowdown', width=0.25, showcaps=True, boxprops={'facecolor':'none'}, ax=ax)
               ax.set_title('User slowdown distributions (selected users)')
               ax.set_xlabel('User')
               ax.set_ylabel('Slowdown')
               fig.autofmt_xdate(rotation=30)
               if save_dir:
                  pth = os.path.join(save_dir, 'user_slowdown_distributions.png')
                  fig.savefig(pth, bbox_inches='tight', dpi=dpi)
                  outputs['user_slowdown_distributions'] = pth
               plt.close(fig)
            except Exception as e:
               self.logger.debug(f"Plot user_slowdown_distributions failed: {e}")

            # 2) Median wait vs average requested node-hours per user (scatter)
            try:
               agg = sub.groupby('owner').agg(
                  median_wait_hours=pd.NamedAgg(column='wait_time_hours', aggfunc='median'),
                  avg_req_node_hours=pd.NamedAgg(column='requested_node_hours', aggfunc='mean'),
                  jobs=pd.NamedAgg(column='job_id', aggfunc='count')
               ).reset_index()
               fig, ax = plt.subplots(figsize=(10, 6))
               sizes = (50 + 5 * agg['jobs'].astype(float).clip(upper=500.0)).values
               scatter = ax.scatter(agg['avg_req_node_hours'], agg['median_wait_hours'], s=sizes, alpha=0.7)
               ax.set_xscale('log')
               ax.set_xlabel('Average requested node-hours (log)')
               ax.set_ylabel('Median wait time (hours)')
               ax.set_title('Per-user median wait vs average requested node-hours')
               if save_dir:
                  pth = os.path.join(save_dir, 'user_median_wait_vs_avg_node_hours.png')
                  fig.savefig(pth, bbox_inches='tight', dpi=dpi)
                  outputs['user_median_wait_vs_avg_node_hours'] = pth
               plt.close(fig)
            except Exception as e:
               self.logger.debug(f"Plot user_median_wait_vs_avg_node_hours failed: {e}")
      except Exception as e:
         self.logger.debug(f"Per-user plots failed: {e}")

      return outputs

   # --------- Internals ---------
   def _query_started_jobs(self, session: Session, cutoff_start: datetime) -> List[Job]:
      """Jobs that started within the window with required fields."""
      jobs = session.query(Job).filter(
         and_(
            Job.start_time.isnot(None),
            Job.submit_time.isnot(None),
            Job.nodes.isnot(None),
            Job.walltime.isnot(None),
            Job.start_time >= cutoff_start,
         )
      ).all()
      return jobs

   def _query_queued_jobs(self, session: Session) -> List[Job]:
      """Get currently queued jobs (submitted but not started or in Q state)."""
      now = datetime.now()
      jobs = session.query(Job).filter(
         and_(
            Job.submit_time.isnot(None),
            Job.nodes.isnot(None),
            Job.walltime.isnot(None),
            Job.start_time.is_(None),
            Job.state == JobState.QUEUED
         )
      ).all()
      self.logger.debug(f"Found {len(jobs)} queued jobs in database")
      return jobs

   def _find_start_score(self, session: Session, job: Job) -> Optional[float]:
      """
      Find score at start from `job_history` by taking the last recorded score
      at or before `start_time`. If none, try the first score after start.
      """
      if not job.start_time:
         return None

      hist = session.query(JobHistory).filter(
         JobHistory.job_id == job.job_id,
         JobHistory.timestamp <= job.start_time,
         JobHistory.score.isnot(None)
      ).order_by(JobHistory.timestamp.desc()).first()
      if hist and hist.score is not None:
         return float(hist.score)

      # Fallback: nearest after start
      hist2 = session.query(JobHistory).filter(
         JobHistory.job_id == job.job_id,
         JobHistory.timestamp > job.start_time,
         JobHistory.score.isnot(None)
      ).order_by(JobHistory.timestamp.asc()).first()
      if hist2 and hist2.score is not None:
         return float(hist2.score)
      return None

   def _parse_walltime_to_hours(self, walltime: Optional[str]) -> float:
      if not walltime:
         return 1.0
      try:
         parts = [int(x) for x in str(walltime).split(':')]
         if len(parts) == 3:
            h, m, s = parts
            return float(h) + m / 60.0 + s / 3600.0
         if len(parts) == 4:
            d, h, m, s = parts
            return float(d * 24 + h) + m / 60.0 + s / 3600.0
      except Exception:
         pass
      return 1.0

   def _compute_wait_hours(self, submit_time: Optional[datetime], start_time: Optional[datetime]) -> float:
      if not submit_time or not start_time:
         return float('nan')
      try:
         return max(0.0, (start_time - submit_time).total_seconds() / 3600.0)
      except Exception:
         return float('nan')

   def _compute_run_hours(self, start_time: Optional[datetime], end_time: Optional[datetime]) -> Optional[float]:
      if not start_time or not end_time:
         return None
      try:
         v = (end_time - start_time).total_seconds() / 3600.0
         return max(0.0, v)
      except Exception:
         return None

   def _filter_queues(self, df: pd.DataFrame, qf: QueueFilter) -> pd.DataFrame:
      if df.empty:
         return df
      # Exclude queues explicitly ignored
      if getattr(qf, 'ignore_queues', None):
         df = df[~df['queue'].isin(set(qf.ignore_queues))].copy()
      # Exclude reservation queues by default unless allowlisted or inclusion requested
      try:
         pattern = re.compile(getattr(qf, 'reservation_queue_regex', r'^[MRS]\\d+$'))
      except Exception:
         pattern = re.compile(r'^[MRS]\\d+$')
      allowlist_set = set(getattr(qf, 'allowlist_queues', []) or [])
      if not getattr(qf, 'include_reservations', False):
         is_resv = df['queue'].astype(str).map(lambda q: bool(pattern.match(q)))
         df = df[(~is_resv) | (df['queue'].isin(allowlist_set))].copy()
      # Compute per-queue total requested node-hours in window
      per_q = (
         df.groupby('queue')['requested_node_hours']
           .sum()
           .sort_values(ascending=False)
      )

      # Determine inclusion set
      include = set(per_q[per_q >= float(qf.min_queue_node_hours)].index.tolist())
      if qf.allowlist_queues:
         include.update(qf.allowlist_queues)
      if qf.top_n_queues is not None and qf.top_n_queues > 0:
         top = per_q.head(qf.top_n_queues).index.tolist()
         include.update(top)

      if include:
         out = df[df['queue'].isin(sorted(include))].copy()
         return out
      return df

   def _hex_or_scatter(self, data: pd.DataFrame, x: str, y: str, color=None, **kwargs) -> None:
      ax = plt.gca()
      x_values = data[x].values
      y_values = data[y].values
      try:
         # If a palette was provided, prefer a scatter with the designated queue color for consistency
         palette = kwargs.pop('palette', None)
         qname = None
         try:
            uq = data['queue'].dropna().astype(str).unique()
            qname = uq[0] if len(uq) > 0 else None
         except Exception:
            qname = None
         chosen_color = None
         if palette and qname is not None:
            chosen_color = palette.get(str(qname))

         if chosen_color is not None:
            ax.scatter(x_values, y_values, s=8, alpha=0.6, color=chosen_color)
            return

         hb = ax.hexbin(x_values, y_values, gridsize=30, mincnt=1, xscale='linear', cmap='viridis')
         cb = plt.colorbar(hb, ax=ax)
         cb.set_label('Count')
      except Exception:
         ax.scatter(x_values, y_values, s=8, alpha=0.6, color=color)

   def _build_queue_palette(self, queues: List[str]) -> Dict[str, str]:
      """Return a deterministic mapping from queue name to color."""
      palette: Dict[str, str] = {}
      if not queues:
         return palette
      try:
         base_colors = sns.color_palette('tab20', n_colors=max(3, len(queues)))
      except Exception:
         # Fallback basic colors if seaborn unavailable
         base_colors = [
            (0.121, 0.466, 0.705), (1.0, 0.498, 0.054), (0.172, 0.627, 0.172),
            (0.839, 0.153, 0.157), (0.580, 0.404, 0.741), (0.549, 0.337, 0.294),
            (0.890, 0.467, 0.761), (0.498, 0.498, 0.498), (0.737, 0.741, 0.133),
            (0.090, 0.745, 0.811)
         ]
         # Repeat if necessary
         if len(base_colors) < len(queues):
            k = int(math.ceil(len(queues) / float(len(base_colors))))
            base_colors = (base_colors * k)[:len(queues)]
      for idx, q in enumerate(queues):
         color = base_colors[idx % len(base_colors)]
         try:
            color = sns.utils.hex_color(color) if hasattr(sns.utils, 'hex_color') else color
         except Exception:
            pass
         palette[str(q)] = color
      return palette

   # --------- Time series helpers for Milestone 2 ---------
   def _compute_throughput_timeseries(self, df: pd.DataFrame, window_start: pd.Timestamp, freq: str = 'D') -> pd.DataFrame:
      """Aggregate requested node-hours started per period by queue."""
      if df.empty:
         return pd.DataFrame(columns=['timestamp', 'queue', 'node_hours'])
      dfx = df.dropna(subset=['start_time']).copy()
      dfx = dfx[dfx['start_time'] >= window_start]
      if dfx.empty:
         return pd.DataFrame(columns=['timestamp', 'queue', 'node_hours'])
      dfx['timestamp'] = dfx['start_time'].dt.to_period(freq).dt.to_timestamp()
      out = (
         dfx.groupby(['timestamp', 'queue'])['requested_node_hours']
            .sum()
            .reset_index(name='node_hours')
            .sort_values('timestamp')
      )
      return out

   def _compute_backlog_timeseries(self, df: pd.DataFrame, window_start: pd.Timestamp, freq: str = 'D') -> pd.DataFrame:
      """
      Estimate queue depth as sum of machine-hours for jobs queued at each timestamp.
      Machine-hours are computed as (nodes × hours in the period defined by `freq`).
      Each job contributes its nodes × period-hours from submit_time until start_time (exclusive).
      Returns columns: ['timestamp', 'queue', 'machine_hours']
      """
      if df.empty:
         return pd.DataFrame(columns=['timestamp', 'queue', 'machine_hours'])

      rows: List[Tuple[pd.Timestamp, str, float]] = []
      for _, row in df.iterrows():
         try:
            sub = row.get('submit_time')
            st = row.get('start_time')
            q = row.get('queue')
            nodes = int(row.get('nodes') or 0)
            if pd.isna(sub) or pd.isna(st) or nodes <= 0:
               continue
            if st <= window_start:
               continue
            start_bin = max(pd.Timestamp(sub).to_period(freq).to_timestamp(), window_start)
            end_bin = pd.Timestamp(st).to_period(freq).to_timestamp()
            if end_bin <= start_bin:
               continue
            timeline = pd.date_range(start=start_bin, end=end_bin, freq=freq, inclusive='left')
            for idx, t in enumerate(timeline):
               # compute hours in this period [t, next_t)
               next_t_candidates = pd.date_range(start=t, periods=2, freq=freq)
               next_t = next_t_candidates[-1] if len(next_t_candidates) == 2 else (t + pd.Timedelta(hours=24))
               if next_t > end_bin:
                  next_t = end_bin
               hours = max(0.0, (next_t - t).total_seconds() / 3600.0)
               mh = float(nodes) * float(hours)
               if mh > 0:
                  rows.append((t, str(q), mh))
         except Exception:
            continue

      if not rows:
         return pd.DataFrame(columns=['timestamp', 'queue', 'machine_hours'])
      out = pd.DataFrame(rows, columns=['timestamp', 'queue', 'machine_hours'])
      out = (
         out.groupby(['timestamp', 'queue'])['machine_hours']
            .sum()
            .reset_index()
            .sort_values('timestamp')
      )
      return out

   def _compute_active_nodes_timeseries(self, df: pd.DataFrame, window_start: pd.Timestamp, freq: str = 'D') -> pd.DataFrame:
      """Sum active nodes per timestamp by queue based on job run intervals."""
      if df.empty:
         return pd.DataFrame(columns=['timestamp', 'queue', 'nodes'])

      rows: List[Tuple[pd.Timestamp, str, int]] = []
      for _, row in df.iterrows():
         try:
            st = row.get('start_time')
            en = row.get('end_time')
            q = row.get('queue')
            nodes = int(row.get('nodes') or 0)
            if pd.isna(st) or pd.isna(en) or nodes <= 0:
               continue
            if en <= window_start:
               continue
            start_bin = max(pd.Timestamp(st).to_period(freq).to_timestamp(), window_start)
            end_bin = pd.Timestamp(en).to_period(freq).to_timestamp()
            if end_bin < start_bin:
               end_bin = start_bin
            timeline = pd.date_range(start=start_bin, end=end_bin, freq=freq)
            if len(timeline) == 0:
               timeline = pd.DatetimeIndex([start_bin])
            for t in timeline:
               rows.append((t, str(q), nodes))
         except Exception:
            continue

      if not rows:
         return pd.DataFrame(columns=['timestamp', 'queue', 'nodes'])
      out = pd.DataFrame(rows, columns=['timestamp', 'queue', 'nodes'])
      out = (
         out.groupby(['timestamp', 'queue'])['nodes']
            .sum()
            .reset_index()
            .sort_values('timestamp')
      )
      return out

   def _compute_used_node_hours_timeseries(self, df: pd.DataFrame, window_start: pd.Timestamp, freq: str = 'D') -> pd.DataFrame:
      """
      Aggregate actual used node-hours per period across all queues.
      Computes sum over jobs of nodes × overlap_hours between job [start,end) and each period [t, next_t).
      Returns columns: ['timestamp', 'used_node_hours']
      """
      if df.empty:
         return pd.DataFrame(columns=['timestamp', 'used_node_hours'])

      rows: List[Tuple[pd.Timestamp, float]] = []
      for _, row in df.iterrows():
         try:
            st = row.get('start_time')
            en = row.get('end_time')
            nodes = int(row.get('nodes') or 0)
            if pd.isna(st) or pd.isna(en) or nodes <= 0:
               continue
            if en <= window_start:
               continue
            start_bin = max(pd.Timestamp(st).to_period(freq).to_timestamp(), window_start)
            end_bin = pd.Timestamp(en).to_period(freq).to_timestamp()
            if end_bin < start_bin:
               end_bin = start_bin
            timeline = pd.date_range(start=start_bin, end=end_bin, freq=freq)
            if len(timeline) == 0:
               timeline = pd.DatetimeIndex([start_bin])
            for t in timeline:
               next_t_candidates = pd.date_range(start=t, periods=2, freq=freq)
               next_t = next_t_candidates[-1] if len(next_t_candidates) == 2 else (t + pd.Timedelta(hours=24))
               # overlap within [t, next_t)
               seg_start = max(pd.Timestamp(st), t)
               seg_end = min(pd.Timestamp(en), next_t)
               hours = max(0.0, (seg_end - seg_start).total_seconds() / 3600.0)
               if hours > 0.0:
                  rows.append((t, float(nodes) * float(hours)))
         except Exception:
            continue

      if not rows:
         return pd.DataFrame(columns=['timestamp', 'used_node_hours'])
      out = pd.DataFrame(rows, columns=['timestamp', 'used_node_hours'])
      out = (
         out.groupby(['timestamp'])['used_node_hours']
            .sum()
            .reset_index()
            .sort_values('timestamp')
      )
      return out

   def _compute_current_wait_bins(self, df: pd.DataFrame) -> pd.DataFrame:
      """
      Compute wait time bins for currently queued jobs.
      Returns DataFrame with columns: ['queue', 'wait_bin', 'count']
      """
      if df.empty:
         self.logger.debug("Input DataFrame is empty")
         return pd.DataFrame(columns=['queue', 'wait_bin', 'count'])

      # Debug: Log the DataFrame info
      self.logger.debug(f"Input DataFrame has {len(df)} rows")
      if 'state' in df.columns:
         state_counts = df['state'].value_counts()
         self.logger.debug(f"State distribution: {state_counts.to_dict()}")
      else:
         self.logger.debug("No 'state' column in DataFrame")

      # Define bin edges in hours
      bins = [0, 1, 6, 12, 24, 48, 24*7, 24*14, 24*21, 24*35, float('inf')]
      labels = [
         '<1hr', '1-6hrs', '6-12hrs', '12-24hrs',
         '1-2days', '2-7days', '7-14days', '2-3weeks', '3-5weeks', '>1month'
      ]

      # Get currently queued jobs - only those in QUEUED state
      now = pd.Timestamp.now(tz=None)
      
      # More flexible filtering to debug what's happening
      if 'state' not in df.columns:
         self.logger.warning("No 'state' column found in DataFrame")
         return pd.DataFrame(columns=['queue', 'wait_bin', 'count'])
      
      # Check various conditions separately
      has_submit = df['submit_time'].notna()
      # JobState.QUEUED has value "Q", so check for both
      is_queued = df['state'].astype(str).isin(['Q', 'QUEUED', 'JobState.QUEUED'])
      no_start = df['start_time'].isna()
      
      self.logger.debug(f"Jobs with submit_time: {has_submit.sum()}")
      self.logger.debug(f"Jobs in QUEUED state: {is_queued.sum()}")
      self.logger.debug(f"Jobs without start_time: {no_start.sum()}")
      self.logger.debug(f"Unique state values: {df['state'].unique()}")
      
      queued = df[has_submit & is_queued & no_start].copy()
      
      self.logger.debug(f"Final queued jobs after filtering: {len(queued)}")
      
      if queued.empty:
         self.logger.warning("No queued jobs found after filtering")
         return pd.DataFrame(columns=['queue', 'wait_bin', 'count'])

      # Compute current wait time in hours
      queued['wait_hours'] = (now - queued['submit_time']).dt.total_seconds() / 3600.0
      
      # Bin the wait times
      queued['wait_bin'] = pd.cut(
         queued['wait_hours'],
         bins=bins,
         labels=labels,
         right=False
      )
      
      # Group by queue and wait bin
      counts = (
         queued.groupby(['queue', 'wait_bin'])
         .size()
         .reset_index(name='count')
         .sort_values(['queue', 'wait_bin'])
      )
      
      return counts

   def _detect_total_cluster_nodes(self) -> Optional[int]:
      """
      Detect total number of cluster nodes by invoking 'pbsnodes'.
      Prefers: pbsnodes -a -F dsv (1 line per node). Falls back to 'pbsnodes -a'.
      Returns None if detection fails.
      """
      try:
         if shutil.which('pbsnodes') is None:
            return None
         # Preferred: one line per node
         try:
            result = subprocess.run(
               ['pbsnodes', '-a', '-F', 'dsv'],
               check=True,
               capture_output=True,
               text=True,
               timeout=15
            )
            count = sum(1 for line in (result.stdout or '').splitlines() if line.strip())
            if count > 0:
               return int(count)
         except Exception:
            pass
         # Fallback: generic output; approximate by counting non-empty lines that look like node records
         try:
            result2 = subprocess.run(
               ['pbsnodes', '-a'],
               check=True,
               capture_output=True,
               text=True,
               timeout=15
            )
            # Heuristic: count lines that start new node sections, commonly like: 'Node: <name>'
            lines = (result2.stdout or '').splitlines()
            count2 = sum(1 for line in lines if line.strip().lower().startswith('node:'))
            if count2 > 0:
               return int(count2)
            # As last resort, count blocks separated by blank lines
            blocks = [b for b in (result2.stdout or '').split('\n\n') if b.strip()]
            if blocks:
               return int(len(blocks))
         except Exception:
            pass
      except Exception:
         return None
      return None



