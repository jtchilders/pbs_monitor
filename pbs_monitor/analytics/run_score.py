"""
Run Score Analyzer for PBS Monitor Analytics

Analyzes historical job scores at queue → run transitions to help users
understand what scores are typically needed for different job configurations.
"""

from typing import List, Dict, Any, Tuple, Optional
from datetime import datetime, timedelta
import pandas as pd
import numpy as np
from sqlalchemy import and_, func, desc
from sqlalchemy.orm import Session

from ..database.repositories import RepositoryFactory
from ..database.models import JobHistory, Job, JobState
from ..pbs_commands import PBSCommands


class RunScoreAnalyzer:
    """Analyzer for job score patterns at queue → run transitions"""
    
    def __init__(self, repository_factory: Optional[RepositoryFactory] = None):
        self.repo_factory = repository_factory or RepositoryFactory()
        self.pbs_commands = PBSCommands()
        
        # Node count bins (as defined in the plan)
        self.node_bins = [
            (1, 31, "1-31"),
            (32, 127, "32-127"), 
            (128, 255, "128-255"),
            (256, 999, "256-999"),
            (1000, 1999, "1000-1999"),
            (2000, 2999, "2000-2999"),
            (3000, 3999, "3000-3999"),
            (4000, 4999, "4000-4999"),
            (5000, 5999, "5000-5999"),
            (6000, 6999, "6000-6999"),
            (7000, 7999, "7000-7999"),
            (8000, 8999, "8000-8999"),
            (9000, 9999, "9000-9999"),
            (10000, float('inf'), "10000+")
        ]
        
        # Walltime bins (in hours)
        self.walltime_bins = [
            (0, 1, "0-60min"),
            (1, 3, "1-3hrs"),
            (3, 6, "3-6hrs"), 
            (6, 12, "6-12hrs"),
            (12, 18, "12-18hrs"),
            (18, 24, "18-24hrs"),
            (24, float('inf'), "24hrs+")
        ]
    
    def analyze_transition_scores(self, days: int = 30) -> pd.DataFrame:
        """
        Analyze job scores at queue→run transition
        
        Args:
            days: Number of days to look back for analysis
            
        Returns:
            DataFrame with score statistics by node count and walltime bins
        """
        cutoff_date = datetime.now() - timedelta(days=days)
        
        with self.repo_factory.get_job_repository().get_session() as session:
            # Query for Q→R transitions with scores
            transitions = self._get_transition_data(session, cutoff_date)
            
            if not transitions:
                return self._create_empty_dataframe()
            
            # Convert to DataFrame for analysis
            df = pd.DataFrame(transitions)
            
            # Add binning columns
            df['node_bin'] = df['nodes'].apply(self._categorize_by_nodes)
            df['walltime_bin'] = df['walltime_hours'].apply(self._categorize_by_walltime)
            
            # Calculate statistics for each bin combination
            result_data = []
            
            for node_bin in [bin_info[2] for bin_info in self.node_bins]:
                row_data = {'node_count': node_bin}
                
                for walltime_bin in [bin_info[2] for bin_info in self.walltime_bins]:
                    # Filter data for this bin combination
                    mask = (df['node_bin'] == node_bin) & (df['walltime_bin'] == walltime_bin)
                    bin_data = df[mask]['score']
                    
                    if len(bin_data) > 0:
                        stats = self._calculate_score_statistics(bin_data)
                        row_data[walltime_bin] = f"{stats['mean']:.0f} ± {stats['std']:.0f}"
                        row_data[f"{walltime_bin}_count"] = stats['count']
                    else:
                        row_data[walltime_bin] = "No data"
                        row_data[f"{walltime_bin}_count"] = 0
                
                result_data.append(row_data)
            
            return pd.DataFrame(result_data)
    
    def _get_transition_data(self, session: Session, cutoff_date: datetime) -> List[Dict[str, Any]]:
        """
        Get job transition data from database
        
        Args:
            session: Database session
            cutoff_date: Cutoff date for analysis
            
        Returns:
            List of transition records with job metadata and recalculated scores
        """
        # Query for FINISHED jobs within the time window that have raw PBS data
        finished_jobs = session.query(Job).filter(
            and_(
                Job.state == JobState.FINISHED,
                Job.end_time >= cutoff_date,
                Job.raw_pbs_data.isnot(None),
                Job.nodes.isnot(None),
                Job.walltime.isnot(None)
            )
        ).all()
        
        transitions = []
        
        # Get server data and defaults for score calculation
        try:
            server_data = self.pbs_commands.qstat_server()
            server_info = server_data.get("Server", {})
            server_defaults = None
            for server_name, server_details in server_info.items():
                server_defaults = server_details.get("resources_default", {})
                break
        except Exception as e:
            # If we can't get current server data, skip score calculation
            return []
        
        if not server_defaults:
            return []
        
        for job in finished_jobs:
            if not job.raw_pbs_data:
                continue
                
            try:
                # Recalculate the score using the job's raw PBS data
                # This gives us the score as it was at the Q→R transition
                score = self.pbs_commands.calculate_job_score(
                    job.raw_pbs_data, 
                    server_defaults, 
                    server_data
                )
                
                if score is not None:
                    walltime_hours = self._parse_walltime_to_hours(job.walltime)
                    
                    transitions.append({
                        'job_id': job.job_id,
                        'nodes': job.nodes,
                        'walltime_hours': walltime_hours,
                        'score': score,
                        'transition_time': job.start_time or job.end_time
                    })
                    
            except Exception as e:
                # Skip jobs where score calculation fails
                continue
        
        return transitions
    
    def _categorize_by_nodes(self, nodes: int) -> str:
        """Categorize job by node count into bins"""
        for min_nodes, max_nodes, label in self.node_bins:
            if min_nodes <= nodes <= max_nodes:
                return label
        return "1024+"  # Default fallback
    
    def _categorize_by_walltime(self, hours: float) -> str:
        """Categorize job by walltime into bins"""
        for min_hours, max_hours, label in self.walltime_bins:
            if min_hours <= hours < max_hours:
                return label
        return "24hrs+"  # Default fallback
    
    def _calculate_score_statistics(self, scores: pd.Series) -> Dict[str, float]:
        """Calculate mean, std dev, and count for score list"""
        return {
            'mean': scores.mean(),
            'std': scores.std(),
            'count': len(scores)
        }
    
    def _parse_walltime_to_hours(self, walltime: str) -> float:
        """
        Convert walltime string to hours
        
        Args:
            walltime: Walltime string in format HH:MM:SS or DD:HH:MM:SS
            
        Returns:
            Walltime in hours as float
        """
        if not walltime:
            return 1.0  # Default to 1 hour
        
        try:
            parts = walltime.split(':')
            if len(parts) == 3:  # HH:MM:SS
                hours = int(parts[0])
                minutes = int(parts[1])
                seconds = int(parts[2])
                return hours + minutes / 60.0 + seconds / 3600.0
            elif len(parts) == 4:  # DD:HH:MM:SS
                days = int(parts[0])
                hours = int(parts[1])
                minutes = int(parts[2])
                seconds = int(parts[3])
                return days * 24 + hours + minutes / 60.0 + seconds / 3600.0
            else:
                return 1.0  # Default fallback
        except (ValueError, IndexError):
            return 1.0  # Default fallback
    
    def _create_empty_dataframe(self) -> pd.DataFrame:
        """Create empty DataFrame with correct structure"""
        columns = ['node_count'] + [bin_info[2] for bin_info in self.walltime_bins]
        return pd.DataFrame(columns=columns)
    
    def get_analysis_summary(self, days: int = 30) -> Dict[str, Any]:
        """
        Get summary statistics for the analysis
        
        Args:
            days: Number of days analyzed
            
        Returns:
            Dictionary with summary information
        """
        cutoff_date = datetime.now() - timedelta(days=days)
        
        with self.repo_factory.get_job_repository().get_session() as session:
            # Count finished jobs that can be analyzed
            finished_jobs_total = session.query(Job).filter(
                and_(
                    Job.state == JobState.FINISHED,
                    Job.end_time >= cutoff_date,
                    Job.raw_pbs_data.isnot(None),
                    Job.nodes.isnot(None),
                    Job.walltime.isnot(None)
                )
            ).count()
            
            # Get the actual transition data to count successful score calculations
            transitions = self._get_transition_data(session, cutoff_date)
            successful_calculations = len(transitions)
            
            return {
                'analysis_period_days': days,
                'cutoff_date': cutoff_date,
                'total_finished_jobs': finished_jobs_total,
                'successful_score_calculations': successful_calculations,
                'node_bins': [bin_info[2] for bin_info in self.node_bins],
                'walltime_bins': [bin_info[2] for bin_info in self.walltime_bins]
            } 