"""
Walltime Efficiency Analyzer for PBS Monitor Analytics

Analyzes job walltime usage efficiency by user or project to help identify
optimization opportunities and usage patterns.
"""

from typing import List, Dict, Any, Tuple, Optional
from datetime import datetime, timedelta
import pandas as pd
import numpy as np
from sqlalchemy import and_, func, desc, or_
from sqlalchemy.orm import Session

from ..database.repositories import RepositoryFactory
from ..database.models import Job, JobState


class WalltimeEfficiencyAnalyzer:
   """Analyzer for job walltime usage efficiency patterns"""
   
   def __init__(self, repository_factory: Optional[RepositoryFactory] = None):
      self.repo_factory = repository_factory or RepositoryFactory()
   
   def analyze_efficiency_by_user(self, days: int = 30, user: Optional[str] = None, 
                                min_jobs: int = 3, queue: Optional[str] = None,
                                min_nodes: Optional[int] = None, max_nodes: Optional[int] = None) -> pd.DataFrame:
      """
      Analyze walltime efficiency by user
      
      Args:
         days: Number of days to look back for analysis
         user: Filter to specific user (partial match, case-sensitive)
         min_jobs: Minimum number of jobs required for main table inclusion
         queue: Filter by queue name (partial match, case-sensitive)
         min_nodes: Minimum number of nodes required for job inclusion
         max_nodes: Maximum number of nodes allowed for job inclusion
         
      Returns:
         DataFrame with efficiency statistics by user
      """
      cutoff_date = datetime.now() - timedelta(days=days)
      
      with self.repo_factory.get_job_repository().get_session() as session:
         # Get completed jobs with walltime efficiency data
         efficiency_data = self._get_user_efficiency_data(session, cutoff_date, user, queue, min_nodes, max_nodes)
         
         if not efficiency_data:
            return self._create_empty_user_dataframe()
         
         # Convert to DataFrame for analysis
         df = pd.DataFrame(efficiency_data)
         
         # Calculate efficiency statistics grouped by user
         result_data = []
         insufficient_data = []
         
         for user_name in sorted(df['owner'].unique()):
            user_data = df[df['owner'] == user_name]
            job_count = len(user_data)
            
            stats = self._calculate_efficiency_statistics(user_data['efficiency'])
            
            row_data = {
               'User': user_name,
               'Jobs': job_count,
               'Mean Efficiency': f"{stats['mean']:.1f}%",
               'Std Dev': f"{stats['std']:.1f}%",
               'Min Efficiency': f"{stats['min']:.1f}%", 
               'Max Efficiency': f"{stats['max']:.1f}%"
            }
            
            if job_count >= min_jobs:
               result_data.append(row_data)
            else:
               insufficient_data.append(row_data)
         
         # Combine main results with insufficient data at the end
         all_data = result_data + insufficient_data
         
         return pd.DataFrame(all_data)
   
   def analyze_efficiency_by_project(self, days: int = 30, project: Optional[str] = None,
                                   min_jobs: int = 3, queue: Optional[str] = None,
                                   min_nodes: Optional[int] = None, max_nodes: Optional[int] = None) -> pd.DataFrame:
      """
      Analyze walltime efficiency by project
      
      Args:
         days: Number of days to look back for analysis
         project: Filter to specific project (partial match, case-sensitive)
         min_jobs: Minimum number of jobs required for main table inclusion
         queue: Filter by queue name (partial match, case-sensitive)
         min_nodes: Minimum number of nodes required for job inclusion
         max_nodes: Maximum number of nodes allowed for job inclusion
         
      Returns:
         DataFrame with efficiency statistics by project
      """
      cutoff_date = datetime.now() - timedelta(days=days)
      
      with self.repo_factory.get_job_repository().get_session() as session:
         # Get completed jobs with walltime efficiency data, excluding NULL projects
         efficiency_data = self._get_project_efficiency_data(session, cutoff_date, project, queue, min_nodes, max_nodes)
         
         if not efficiency_data:
            return self._create_empty_project_dataframe()
         
         # Convert to DataFrame for analysis  
         df = pd.DataFrame(efficiency_data)
         
         # Calculate efficiency statistics grouped by project
         result_data = []
         insufficient_data = []
         
         for project_name in sorted(df['project'].unique()):
            project_data = df[df['project'] == project_name]
            job_count = len(project_data)
            
            stats = self._calculate_efficiency_statistics(project_data['efficiency'])
            
            row_data = {
               'Project': project_name,
               'Jobs': job_count,
               'Mean Efficiency': f"{stats['mean']:.1f}%",
               'Std Dev': f"{stats['std']:.1f}%",
               'Min Efficiency': f"{stats['min']:.1f}%",
               'Max Efficiency': f"{stats['max']:.1f}%"
            }
            
            if job_count >= min_jobs:
               result_data.append(row_data)
            else:
               insufficient_data.append(row_data)
         
         # Combine main results with insufficient data at the end
         all_data = result_data + insufficient_data
         
         return pd.DataFrame(all_data)
   
   def _get_user_efficiency_data(self, session: Session, cutoff_date: datetime, 
                               user: Optional[str] = None, queue: Optional[str] = None,
                               min_nodes: Optional[int] = None, max_nodes: Optional[int] = None) -> List[Dict[str, Any]]:
      """
      Get job efficiency data for user analysis from database
      
      Args:
         session: Database session
         cutoff_date: Cutoff date for analysis
         user: Optional user filter (partial match)
         queue: Optional queue filter (partial match)
         min_nodes: Optional minimum nodes filter
         max_nodes: Optional maximum nodes filter
         
      Returns:
         List of job efficiency data dictionaries
      """
      # Query for completed jobs with required data
      query = session.query(Job).filter(
         and_(
            Job.state.in_([JobState.COMPLETED, JobState.FINISHED, JobState.UNKNOWN_END]),
            Job.last_updated >= cutoff_date,
            Job.walltime.isnot(None),
            or_(
               Job.actual_runtime_seconds.isnot(None),
               and_(Job.start_time.isnot(None), Job.end_time.isnot(None))
            )
         )
      )
      
      # Apply user filter if specified (partial match, case-sensitive)
      if user:
         query = query.filter(Job.owner.contains(user))
      
      # Apply queue filter if specified (partial match, case-sensitive)
      if queue:
         query = query.filter(Job.queue.contains(queue))
      
      # Apply node filters if specified
      if min_nodes is not None:
         query = query.filter(Job.nodes >= min_nodes)
      if max_nodes is not None:
         query = query.filter(Job.nodes <= max_nodes)
      
      jobs = query.all()
      
      efficiency_data = []
      for job in jobs:
         eff = self._compute_job_efficiency(job)
         if eff is None:
            continue
         efficiency_data.append({
            'job_id': job.job_id,
            'owner': job.owner,
            'project': job.project if job.project else 'No Project',
            'efficiency': eff['efficiency'],
            'actual_runtime': eff['actual_runtime'],
            'requested_walltime': eff['requested_walltime'],
            'run_count': eff['run_count']
         })
      
      return efficiency_data
   
   def _get_project_efficiency_data(self, session: Session, cutoff_date: datetime,
                                  project: Optional[str] = None, queue: Optional[str] = None,
                                  min_nodes: Optional[int] = None, max_nodes: Optional[int] = None) -> List[Dict[str, Any]]:
      """
      Get job efficiency data for project analysis from database
      
      Args:
         session: Database session
         cutoff_date: Cutoff date for analysis
         project: Optional project filter (partial match)
         queue: Optional queue filter (partial match)
         min_nodes: Optional minimum nodes filter
         max_nodes: Optional maximum nodes filter
         
      Returns:
         List of job efficiency data dictionaries
      """
      # Query for completed jobs with required data
      # Note: If Job model doesn't have project field, this will filter out all jobs
      query = session.query(Job).filter(
         and_(
            Job.state.in_([JobState.COMPLETED, JobState.FINISHED, JobState.UNKNOWN_END]),
            Job.last_updated >= cutoff_date,
            Job.walltime.isnot(None),
            or_(
               Job.actual_runtime_seconds.isnot(None),
               and_(Job.start_time.isnot(None), Job.end_time.isnot(None))
            )
         )
      )
      
      # Exclude NULL projects for project analysis
      query = query.filter(Job.project.isnot(None))
      
      # Apply project filter if specified (partial match, case-sensitive)
      if project:
         query = query.filter(Job.project.contains(project))
      
      # Apply queue filter if specified (partial match, case-sensitive)
      if queue:
         query = query.filter(Job.queue.contains(queue))
      
      # Apply node filters if specified
      if min_nodes is not None:
         query = query.filter(Job.nodes >= min_nodes)
      if max_nodes is not None:
         query = query.filter(Job.nodes <= max_nodes)
      
      jobs = query.all()
      
      efficiency_data = []
      for job in jobs:
         eff = self._compute_job_efficiency(job)
         if eff is None:
            continue
         efficiency_data.append({
            'job_id': job.job_id,
            'owner': job.owner,
            'project': job.project if job.project else 'Unknown',
            'efficiency': eff['efficiency'],
            'actual_runtime': eff['actual_runtime'],
            'requested_walltime': eff['requested_walltime'],
            'run_count': eff['run_count']
         })
      
      return efficiency_data
   
   def _compute_job_efficiency(self, job: Job) -> Optional[Dict[str, Any]]:
      """Compute rerun-aware walltime efficiency for a single job.

      Efficiency = occupied_seconds / (requested_walltime * run_count), so that
      a job requeued N times is measured against the N reservations it consumed,
      not a single one. This keeps the ratio interpretable (<=100%) for
      requeued jobs instead of relying on a blind cap that laundered a failed,
      never-useful job into "100% efficient".

      Returns None (job excluded from the distribution) when:
        - the job never actually occupied nodes (no occupied_seconds / no run
          record) — e.g. a job held after too many failed launch attempts. Such
          a job has NO meaningful run/request efficiency; folding it in as 0% or
          100% both distort the picture, so it is tracked by the repeated-rerun
          alert instead.
        - the requested walltime cannot be parsed / is non-positive.

      Args:
         job: Job database model

      Returns:
         Dict with keys efficiency, actual_runtime, requested_walltime,
         run_count — or None to exclude the job.
      """
      # Actual node-occupancy time. _get_actual_runtime_seconds prefers
      # occupied_seconds (PBS-measured), which is None/0 for jobs that never ran.
      actual_runtime_seconds = self._get_actual_runtime_seconds(job)
      if actual_runtime_seconds is None or actual_runtime_seconds <= 0:
         # Never occupied nodes (held/failed-to-launch job). Exclude — no
         # meaningful efficiency; surfaced via the repeated-rerun alert.
         return None

      # Requested walltime for a single reservation.
      requested_walltime_seconds = self._parse_walltime_to_seconds(job.walltime)
      if requested_walltime_seconds <= 0:
         return None

      # Rerun-aware denominator: each run attempt consumed a full reservation.
      # run_count may be NULL on rows collected before v1.4 / not yet
      # backfilled; treat unknown as a single attempt so we never inflate the
      # denominator on missing data.
      run_count = getattr(job, 'run_count', None)
      if run_count is None or run_count < 1:
         run_count = 1

      denom = requested_walltime_seconds * run_count
      efficiency = (actual_runtime_seconds / denom) * 100.0

      # Safety cap for residual outliers (e.g. occupied_seconds slightly past a
      # single walltime on a run_count==1 job that ran a hair past the wall).
      # With the rerun-normalized denominator this now only trims genuine small
      # overruns rather than masking multi-attempt failures.
      efficiency = min(efficiency, 100.0)

      return {
         'efficiency': efficiency,
         'actual_runtime': actual_runtime_seconds,
         'requested_walltime': requested_walltime_seconds,
         'run_count': run_count,
      }

   def _get_actual_runtime_seconds(self, job: Job) -> Optional[int]:
      """
      Get actual node-occupancy runtime in seconds.

      Preference order:
        1. occupied_seconds — PBS-measured node-occupancy time
           (resources_used.walltime), which excludes HELD/QUEUED gaps for
           requeued/preempted jobs. This is the correct basis for efficiency.
        2. actual_runtime_seconds — elapsed start..end span (legacy; overstates
           for requeued jobs). Used only when occupied_seconds is not populated.
        3. end_time - start_time — computed fallback.

      Args:
         job: Job database model

      Returns:
         Actual runtime in seconds or None if unavailable
      """
      # Prefer PBS-measured node-occupancy time
      occupied = getattr(job, 'occupied_seconds', None)
      if occupied is not None and occupied > 0:
         return occupied

      # Fall back to stored elapsed span (legacy field)
      if job.actual_runtime_seconds is not None and job.actual_runtime_seconds > 0:
         return job.actual_runtime_seconds

      # Final fallback: compute from start/end times
      if job.start_time and job.end_time:
         duration = job.end_time - job.start_time
         return int(duration.total_seconds())

      return None
   
   def _parse_walltime_to_seconds(self, walltime_str: str) -> int:
      """
      Parse walltime string to seconds
      
      Args:
         walltime_str: Walltime in format HH:MM:SS or DD:HH:MM:SS
         
      Returns:
         Walltime in seconds
      """
      try:
         parts = walltime_str.split(':')
         if len(parts) == 3:
            # HH:MM:SS format
            hours, minutes, seconds = map(int, parts)
            return hours * 3600 + minutes * 60 + seconds
         elif len(parts) == 4:
            # DD:HH:MM:SS format
            days, hours, minutes, seconds = map(int, parts)
            return days * 86400 + hours * 3600 + minutes * 60 + seconds
         else:
            return 0
      except (ValueError, TypeError):
         return 0
   
   def _calculate_efficiency_statistics(self, efficiency_series: pd.Series) -> Dict[str, float]:
      """
      Calculate efficiency statistics
      
      Args:
         efficiency_series: Pandas series of efficiency values
         
      Returns:
         Dictionary with mean, std, min, max statistics
      """
      if len(efficiency_series) == 0:
         return {'mean': 0.0, 'std': 0.0, 'min': 0.0, 'max': 0.0}
      
      return {
         'mean': float(efficiency_series.mean()),
         'std': float(efficiency_series.std()) if len(efficiency_series) > 1 else 0.0,
         'min': float(efficiency_series.min()),
         'max': float(efficiency_series.max())
      }
   
   def _create_empty_user_dataframe(self) -> pd.DataFrame:
      """Create empty dataframe with user analysis columns"""
      return pd.DataFrame(columns=[
         'User', 'Jobs', 'Mean Efficiency', 'Std Dev', 'Min Efficiency', 'Max Efficiency'
      ])
   
   def _create_empty_project_dataframe(self) -> pd.DataFrame:
      """Create empty dataframe with project analysis columns"""
      return pd.DataFrame(columns=[
         'Project', 'Jobs', 'Mean Efficiency', 'Std Dev', 'Min Efficiency', 'Max Efficiency'
      ])
   
   def get_analysis_summary(self, days: int = 30, analysis_type: str = "user") -> Dict[str, Any]:
      """
      Get summary statistics for the analysis period
      
      Args:
         days: Number of days analyzed
         analysis_type: Type of analysis ("user" or "project")
         
      Returns:
         Summary statistics dictionary
      """
      cutoff_date = datetime.now() - timedelta(days=days)
      
      with self.repo_factory.get_job_repository().get_session() as session:
         # Count total completed jobs in period
         total_jobs = session.query(Job).filter(
            and_(
               Job.state.in_([JobState.COMPLETED, JobState.FINISHED, JobState.UNKNOWN_END]),
               Job.last_updated >= cutoff_date
            )
         ).count()
         
         # Count jobs with efficiency data
         if analysis_type == "project":
            # For project analysis, exclude NULL projects (if project field exists)
            project_query = session.query(Job).filter(
               and_(
                  Job.state.in_([JobState.COMPLETED, JobState.FINISHED, JobState.UNKNOWN_END]),
                  Job.last_updated >= cutoff_date,
                  Job.walltime.isnot(None),
                  or_(
                     Job.actual_runtime_seconds.isnot(None),
                     and_(Job.start_time.isnot(None), Job.end_time.isnot(None))
                  )
               )
            )
            # Filter by project field
            project_query = project_query.filter(Job.project.isnot(None))
            jobs_with_data = project_query.count()
         else:
            # For user analysis, include all jobs (including NULL projects)
            jobs_with_data = session.query(Job).filter(
               and_(
                  Job.state.in_([JobState.COMPLETED, JobState.FINISHED, JobState.UNKNOWN_END]),
                  Job.last_updated >= cutoff_date,
                  Job.walltime.isnot(None),
                  or_(
                     Job.actual_runtime_seconds.isnot(None),
                     and_(Job.start_time.isnot(None), Job.end_time.isnot(None))
                  )
               )
            ).count()
         
         return {
            'analysis_period_days': days,
            'total_completed_jobs': total_jobs,
            'jobs_with_efficiency_data': jobs_with_data,
            'analysis_type': analysis_type
         }