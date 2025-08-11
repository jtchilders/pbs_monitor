"""
Reservation Analysis Module for PBS Monitor

Provides analytics features for analyzing PBS reservation utilization and efficiency.
"""

import logging
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional, Tuple
import pandas as pd
from sqlalchemy.orm import Session
from sqlalchemy import and_, func, desc

from ..database.models import (
    Reservation, ReservationUtilization, Job, JobState, 
    ReservationState, DataCollectionLog
)
from ..database.repositories import RepositoryFactory


class ReservationUtilizationAnalyzer:
    """Analyze reservation utilization efficiency"""
    
    def __init__(self, repository_factory: Optional[RepositoryFactory] = None):
        self.repo_factory = repository_factory or RepositoryFactory()
        self.logger = logging.getLogger(__name__)
    
    def analyze_reservation_utilization(self, 
                                       reservation_id: str,
                                       start_date: Optional[datetime] = None,
                                       end_date: Optional[datetime] = None,
                                       analysis_method: str = "job_queue_analysis") -> Dict[str, Any]:
        """
        Analyze how well a reservation was utilized by examining:
        1. Jobs submitted to the reservation's queue
        2. Actual node-hours used vs. reserved
        3. Resource efficiency metrics
        
        Args:
            reservation_id: The reservation ID to analyze
            start_date: Optional start date for analysis period
            end_date: Optional end date for analysis period
            analysis_method: Method used for analysis ("job_queue_analysis")
            
        Returns:
            Dictionary with analysis results
        """
        try:
            with self.repo_factory.get_job_repository().get_session() as session:
                # Get reservation details
                reservation = self._get_reservation(session, reservation_id)
                if not reservation:
                    raise ValueError(f"Reservation {reservation_id} not found")
                
                # Determine analysis window (respect overrides)
                window_start = start_date or reservation.start_time
                window_end = end_date or reservation.end_time

                # Find jobs whose run interval overlaps the reservation window
                reservation_jobs = self._find_reservation_jobs(
                    session, reservation, window_start, window_end
                )
                
                # Calculate utilization metrics
                metrics = self._calculate_utilization_metrics(
                    reservation, reservation_jobs, analysis_method, window_start, window_end
                )
                
                # Convert reservation data to dictionary
                result = {
                    'reservation_id': reservation.reservation_id,
                    'reservation_name': reservation.reservation_name,
                    'owner': reservation.owner,
                    'queue': reservation.queue,
                    'state': reservation.state.value if reservation.state else 'unknown',
                    **metrics
                }
                
                # Store in database
                utilization = ReservationUtilization(
                    reservation_id=reservation_id,
                    analysis_timestamp=datetime.now(),
                    **metrics
                )
                session.add(utilization)
                session.commit()
                
                return result
                
        except Exception as e:
            self.logger.error(f"Failed to analyze reservation {reservation_id}: {str(e)}")
            raise
    
    def analyze_multiple_reservations(self,
                                    reservation_ids: Optional[List[str]] = None,
                                    start_date: Optional[datetime] = None,
                                    end_date: Optional[datetime] = None,
                                    states: Optional[List[ReservationState]] = None) -> List[Dict[str, Any]]:
        """
        Analyze utilization for multiple reservations
        
        Args:
            reservation_ids: List of specific reservation IDs to analyze (None = all)
            start_date: Optional start date for analysis period
            end_date: Optional end date for analysis period
            states: Optional list of reservation states to filter by
            
        Returns:
            List of dictionaries with analysis results
        """
        results = []
        
        with self.repo_factory.get_job_repository().get_session() as session:
            # Get reservations to analyze
            reservations = self._get_reservations_to_analyze(
                session, reservation_ids, start_date, end_date, states
            )
            
            for reservation in reservations:
                try:
                    result = self.analyze_reservation_utilization(
                        reservation.reservation_id, start_date, end_date
                    )
                    results.append(result)
                except Exception as e:
                    self.logger.warning(f"Failed to analyze reservation {reservation.reservation_id}: {e}")
                    continue
        
        return results
    
    def get_utilization_summary(self,
                               start_date: Optional[datetime] = None,
                               end_date: Optional[datetime] = None,
                               min_utilization: Optional[float] = None) -> Dict[str, Any]:
        """
        Get summary statistics for reservation utilization
        
        Args:
            start_date: Optional start date for analysis period
            end_date: Optional end date for analysis period
            min_utilization: Optional minimum utilization percentage to include
            
        Returns:
            Dictionary with summary statistics
        """
        with self.repo_factory.get_job_repository().get_session() as session:
            query = session.query(ReservationUtilization)
            
            if start_date:
                query = query.filter(ReservationUtilization.analysis_timestamp >= start_date)
            if end_date:
                query = query.filter(ReservationUtilization.analysis_timestamp <= end_date)
            if min_utilization is not None:
                query = query.filter(ReservationUtilization.utilization_percentage >= min_utilization)
            
            utilizations = query.all()
            
            # Convert to simple Python objects
            utilization_data = [
                {
                    'utilization_percentage': u.utilization_percentage,
                    'total_node_hours_reserved': u.total_node_hours_reserved,
                    'total_node_hours_used': u.total_node_hours_used,
                    'jobs_submitted': u.jobs_submitted,
                    'jobs_completed': u.jobs_completed
                }
                for u in utilizations
            ]
        
        if not utilization_data:
            return {
                'total_reservations': 0,
                'avg_utilization': 0.0,
                'median_utilization': 0.0,
                'min_utilization': 0.0,
                'max_utilization': 0.0,
                'underutilized_count': 0,
                'well_utilized_count': 0
            }
        
        # Calculate statistics
        utilization_percentages = [u['utilization_percentage'] for u in utilization_data]
        
        summary = {
            'total_reservations': len(utilization_data),
            'avg_utilization': sum(utilization_percentages) / len(utilization_percentages),
            'median_utilization': sorted(utilization_percentages)[len(utilization_percentages) // 2],
            'min_utilization': min(utilization_percentages),
            'max_utilization': max(utilization_percentages),
            'underutilized_count': len([u for u in utilization_percentages if u < 50.0]),
            'well_utilized_count': len([u for u in utilization_percentages if u >= 80.0])
        }
        
        return summary
    
    def _get_reservation(self, session: Session, reservation_id: str) -> Optional[Reservation]:
        """Get reservation from database"""
        return session.query(Reservation).filter(
            Reservation.reservation_id == reservation_id
        ).first()
    
    def _get_reservations_to_analyze(self,
                                   session: Session,
                                   reservation_ids: Optional[List[str]] = None,
                                   start_date: Optional[datetime] = None,
                                   end_date: Optional[datetime] = None,
                                   states: Optional[List[ReservationState]] = None) -> List[Reservation]:
        """Get list of reservations to analyze"""
        query = session.query(Reservation)
        
        if reservation_ids:
            query = query.filter(Reservation.reservation_id.in_(reservation_ids))
        
        if start_date:
            query = query.filter(Reservation.start_time >= start_date)
        if end_date:
            query = query.filter(Reservation.end_time <= end_date)
        if states:
            query = query.filter(Reservation.state.in_(states))
        
        return query.all()
    
    def _find_reservation_jobs(self, 
                               session: Session,
                               reservation: Reservation,
                               start_date: Optional[datetime],
                               end_date: Optional[datetime]) -> List[Dict[str, Any]]:
        """
        Find jobs that submitted to the reservation's queue during the reservation period
        
        Args:
            session: Database session
            reservation: The reservation to analyze
            start_date: Optional start date override
            end_date: Optional end date override
            
        Returns:
            List of job data dictionaries
        """
        # Use reservation period if no dates specified
        query_start = start_date or reservation.start_time
        query_end = end_date or reservation.end_time
        
        if not query_start or not query_end:
            self.logger.warning(f"Reservation {reservation.reservation_id} has no start/end time")
            return []
        
        # Select jobs by run-interval overlap with the analysis window, not submit time
        # Only include jobs with a non-zero runtime
        jobs = session.query(Job).filter(
            Job.queue == reservation.queue,
            Job.start_time.isnot(None),
            Job.end_time.isnot(None),
            Job.actual_runtime_seconds.isnot(None),
            Job.actual_runtime_seconds > 0,
            Job.start_time <= query_end,
            Job.end_time >= query_start
        ).all()
        
        # Convert to simple Python objects
        job_data = [
            {
                'job_id': job.job_id,
                'nodes': job.nodes,
                'total_cores': job.total_cores or job.nodes,
                'ngpus': getattr(job, 'ngpus', None),
                'actual_runtime_seconds': job.actual_runtime_seconds,
                'state': job.state,
                'start_time': job.start_time,
                'end_time': job.end_time
            }
            for job in jobs
        ]
        
        self.logger.debug(f"Found {len(job_data)} jobs for reservation {reservation.reservation_id}")
        return job_data
    
    def _calculate_utilization_metrics(self,
                                     reservation: Reservation,
                                     jobs: List[Dict[str, Any]],
                                     analysis_method: str,
                                     window_start: Optional[datetime] = None,
                                     window_end: Optional[datetime] = None) -> Dict[str, Any]:
        """
        Calculate detailed utilization metrics
        
        Args:
            reservation: The reservation being analyzed
            jobs: List of job data dictionaries
            analysis_method: Method used for analysis
            
        Returns:
            Dictionary of utilization metrics
        """
        # Determine effective analysis window
        effective_start = window_start or reservation.start_time
        effective_end = window_end or reservation.end_time

        if not effective_start or not effective_end or effective_end <= effective_start:
            duration_hours = 0
        else:
            duration_hours = (effective_end - effective_start).total_seconds() / 3600

        # Calculate reserved resources over the effective window
        total_node_hours_reserved = (reservation.nodes or 0) * duration_hours if reservation.nodes else 0
        total_cpu_hours_reserved = (reservation.ncpus or 0) * duration_hours if reservation.ncpus else 0
        total_gpu_hours_reserved = (reservation.ngpus * duration_hours) if reservation.ngpus else None
        
        # Calculate used resources (from jobs)
        total_node_hours_used = 0
        total_cpu_hours_used = 0
        total_gpu_hours_used = 0
        jobs_completed = 0
        jobs_failed = 0
        peak_nodes_used = 0
        peak_usage_timestamp = None
        
        for job in jobs:
            if job['actual_runtime_seconds'] and job['nodes'] and job.get('start_time') and job.get('end_time'):
                # Calculate overlap between job run interval and effective reservation window
                overlap_start = max(job['start_time'], effective_start) if effective_start else job['start_time']
                overlap_end = min(job['end_time'], effective_end) if effective_end else job['end_time']

                if overlap_end and overlap_start and overlap_end > overlap_start:
                    overlap_hours = (overlap_end - overlap_start).total_seconds() / 3600
                else:
                    overlap_hours = 0

                if overlap_hours > 0:
                    job_node_hours = job['nodes'] * overlap_hours
                    total_node_hours_used += job_node_hours
                    
                    # CPU usage
                    job_cpus = job['total_cores']
                    total_cpu_hours_used += job_cpus * overlap_hours
                    
                    # GPU usage (if job used GPUs)
                    if job['ngpus']:
                        total_gpu_hours_used += job['ngpus'] * overlap_hours
                    
                    # Track peak usage (approximate: max nodes of any overlapping job)
                    if job['nodes'] > peak_nodes_used:
                        peak_nodes_used = job['nodes']
                        peak_usage_timestamp = overlap_start
            
            # Job completion status
            if job['state'] == JobState.COMPLETED:
                jobs_completed += 1
            elif job['state'] == JobState.FINISHED:
                jobs_completed += 1
        
        # Calculate utilization percentages
        node_utilization = (total_node_hours_used / total_node_hours_reserved * 100) if total_node_hours_reserved > 0 else 0
        cpu_utilization = (total_cpu_hours_used / total_cpu_hours_reserved * 100) if total_cpu_hours_reserved > 0 else 0
        gpu_utilization = (total_gpu_hours_used / total_gpu_hours_reserved * 100) if total_gpu_hours_reserved and total_gpu_hours_reserved > 0 else None
        
        return {
            'total_node_hours_reserved': total_node_hours_reserved,
            'total_node_hours_used': total_node_hours_used,
            'utilization_percentage': node_utilization,
            'jobs_submitted': len(jobs),
            'jobs_completed': jobs_completed,
            'jobs_failed': jobs_failed,
            'cpu_hours_reserved': total_cpu_hours_reserved,
            'cpu_hours_used': total_cpu_hours_used,
            'cpu_utilization_percentage': cpu_utilization,
            'gpu_hours_reserved': total_gpu_hours_reserved,
            'gpu_hours_used': total_gpu_hours_used if total_gpu_hours_reserved else None,
            'gpu_utilization_percentage': gpu_utilization,
            'peak_nodes_used': peak_nodes_used,
            'peak_usage_timestamp': peak_usage_timestamp,
            'analysis_method': analysis_method,
            'jobs_analyzed': len(jobs)
        }


class ReservationTrendAnalyzer:
    """Analyze reservation trends and patterns over time"""
    
    def __init__(self, repository_factory: Optional[RepositoryFactory] = None):
        self.repo_factory = repository_factory or RepositoryFactory()
        self.logger = logging.getLogger(__name__)
    
    def analyze_utilization_trends(self,
                                 days: int = 30,
                                 owner: Optional[str] = None,
                                 queue: Optional[str] = None) -> pd.DataFrame:
        """
        Analyze reservation utilization trends over time
        
        Args:
            days: Number of days to analyze
            owner: Optional owner filter
            queue: Optional queue filter
            
        Returns:
            DataFrame with trend data
        """
        end_date = datetime.now()
        start_date = end_date - timedelta(days=days)
        
        with self.repo_factory.get_job_repository().get_session() as session:
            query = session.query(ReservationUtilization).join(Reservation).filter(
                ReservationUtilization.analysis_timestamp >= start_date,
                ReservationUtilization.analysis_timestamp <= end_date
            )
            
            if owner:
                query = query.filter(Reservation.owner == owner)
            if queue:
                query = query.filter(Reservation.queue == queue)
            
            utilizations = query.all()
            
            # Convert to simple Python objects
            data = [
                {
                    'date': util.analysis_timestamp.date(),
                    'reservation_id': util.reservation_id,
                    'owner': util.reservation.owner,
                    'queue': util.reservation.queue,
                    'utilization_percentage': util.utilization_percentage,
                    'node_hours_reserved': util.total_node_hours_reserved,
                    'node_hours_used': util.total_node_hours_used,
                    'jobs_submitted': util.jobs_submitted,
                    'jobs_completed': util.jobs_completed
                }
                for util in utilizations
            ]
        
        if not data:
            return pd.DataFrame()
        
        df = pd.DataFrame(data)
        
        # Add daily aggregations
        daily_stats = df.groupby('date').agg({
            'utilization_percentage': ['mean', 'median', 'count'],
            'node_hours_reserved': 'sum',
            'node_hours_used': 'sum',
            'jobs_submitted': 'sum',
            'jobs_completed': 'sum'
        }).round(2)
        
        # Flatten column names
        daily_stats.columns = ['_'.join(col).strip() for col in daily_stats.columns]
        daily_stats.reset_index(inplace=True)
        
        return daily_stats
    
    def get_owner_efficiency_ranking(self, days: int = 30) -> pd.DataFrame:
        """
        Rank reservation owners by utilization efficiency
        
        Args:
            days: Number of days to analyze
            
        Returns:
            DataFrame with owner rankings
        """
        end_date = datetime.now()
        start_date = end_date - timedelta(days=days)
        
        with self.repo_factory.get_job_repository().get_session() as session:
            # Get utilization data with owner info
            utilizations = session.query(ReservationUtilization).join(Reservation).filter(
                ReservationUtilization.analysis_timestamp >= start_date,
                ReservationUtilization.analysis_timestamp <= end_date
            ).all()
            
            # Convert to simple Python objects
            data = [
                {
                    'owner': util.reservation.owner,
                    'utilization_percentage': util.utilization_percentage,
                    'node_hours_reserved': util.total_node_hours_reserved,
                    'node_hours_used': util.total_node_hours_used,
                    'jobs_submitted': util.jobs_submitted,
                    'jobs_completed': util.jobs_completed
                }
                for util in utilizations
            ]
        
        if not data:
            return pd.DataFrame()
        
        df = pd.DataFrame(data)
        
        # Group by owner and calculate statistics
        owner_stats = []
        for owner in df['owner'].unique():
            owner_data = df[df['owner'] == owner]
            
            stats = {
                'owner': owner,
                'reservations': len(owner_data),
                'avg_utilization_percentage': owner_data['utilization_percentage'].mean(),
                'overall_utilization_percentage': (owner_data['node_hours_used'].sum() / owner_data['node_hours_reserved'].sum() * 100) if owner_data['node_hours_reserved'].sum() > 0 else 0,
                'total_node_hours_reserved': owner_data['node_hours_reserved'].sum(),
                'total_node_hours_used': owner_data['node_hours_used'].sum(),
                'jobs_submitted': owner_data['jobs_submitted'].sum(),
                'jobs_completed': owner_data['jobs_completed'].sum(),
                'completion_rate_percentage': (owner_data['jobs_completed'].sum() / owner_data['jobs_submitted'].sum() * 100) if owner_data['jobs_submitted'].sum() > 0 else 0
            }
            
            owner_stats.append(stats)
        
        result_df = pd.DataFrame(owner_stats)
        
        # Round numeric columns
        numeric_cols = ['avg_utilization_percentage', 'overall_utilization_percentage', 
                       'total_node_hours_reserved', 'total_node_hours_used', 'completion_rate_percentage']
        result_df[numeric_cols] = result_df[numeric_cols].round(2)
        
        # Sort by overall utilization percentage
        result_df = result_df.sort_values('overall_utilization_percentage', ascending=False)
        
        return result_df