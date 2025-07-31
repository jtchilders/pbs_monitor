"""
Queue Depth Calculator for PBS Monitor Analytics

Calculates total node-hours currently waiting in the queue to provide
insight into queue depth and resource demand.
"""

from typing import List, Dict, Any
from ..models.job import PBSJob, JobState


class QueueDepthCalculator:
    """Calculator for queue depth metrics"""
    
    def calculate_total_node_hours(self, jobs: List[PBSJob]) -> float:
        """
        Calculate total node-hours waiting in queue
        
        Args:
            jobs: List of PBS jobs
            
        Returns:
            Total node-hours for all queued jobs
        """
        total_node_hours = 0.0
        
        for job in jobs:
            if job.state == JobState.QUEUED:  # Only queued jobs
                nodes = job.nodes or 1
                walltime_hours = self._parse_walltime_to_hours(job.walltime)
                total_node_hours += nodes * walltime_hours
                
        return total_node_hours
    
    def calculate_queue_depth_breakdown(self, jobs: List[PBSJob]) -> Dict[str, Any]:
        """
        Calculate detailed queue depth breakdown by job size categories
        
        Args:
            jobs: List of PBS jobs
            
        Returns:
            Dictionary with queue depth breakdown by node count ranges
        """
        breakdown = {
            'total_node_hours': 0.0,
            'total_jobs': 0,
            'by_node_count': {
                '1-31': {'jobs': 0, 'node_hours': 0.0},
                '32-127': {'jobs': 0, 'node_hours': 0.0},
                '128-255': {'jobs': 0, 'node_hours': 0.0},
                '256-1023': {'jobs': 0, 'node_hours': 0.0},
                '1024+': {'jobs': 0, 'node_hours': 0.0}
            },
            'by_walltime': {
                '0-1h': {'jobs': 0, 'node_hours': 0.0},
                '1-3h': {'jobs': 0, 'node_hours': 0.0},
                '3-6h': {'jobs': 0, 'node_hours': 0.0},
                '6-12h': {'jobs': 0, 'node_hours': 0.0},
                '12-24h': {'jobs': 0, 'node_hours': 0.0},
                '24h+': {'jobs': 0, 'node_hours': 0.0}
            }
        }
        
        for job in jobs:
            if job.state == JobState.QUEUED:
                nodes = job.nodes or 1
                walltime_hours = self._parse_walltime_to_hours(job.walltime)
                node_hours = nodes * walltime_hours
                
                breakdown['total_jobs'] += 1
                breakdown['total_node_hours'] += node_hours
                
                # Categorize by node count
                node_category = self._categorize_by_nodes(nodes)
                if node_category in breakdown['by_node_count']:
                    breakdown['by_node_count'][node_category]['jobs'] += 1
                    breakdown['by_node_count'][node_category]['node_hours'] += node_hours
                
                # Categorize by walltime
                walltime_category = self._categorize_by_walltime(walltime_hours)
                if walltime_category in breakdown['by_walltime']:
                    breakdown['by_walltime'][walltime_category]['jobs'] += 1
                    breakdown['by_walltime'][walltime_category]['node_hours'] += node_hours
        
        return breakdown
    
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
            if len(parts) == 3:
                # HH:MM:SS format
                hours, minutes, seconds = map(int, parts)
                return hours + minutes / 60.0 + seconds / 3600.0
            elif len(parts) == 4:
                # DD:HH:MM:SS format  
                days, hours, minutes, seconds = map(int, parts)
                return days * 24 + hours + minutes / 60.0 + seconds / 3600.0
            else:
                return 1.0  # Default to 1 hour
        except (ValueError, TypeError):
            return 1.0  # Default to 1 hour
    
    def _categorize_by_nodes(self, nodes: int) -> str:
        """Categorize job by node count"""
        if 1 <= nodes <= 31:
            return '1-31'
        elif 32 <= nodes <= 127:
            return '32-127'
        elif 128 <= nodes <= 255:
            return '128-255'
        elif 256 <= nodes <= 1023:
            return '256-1023'
        else:
            return '1024+'
    
    def _categorize_by_walltime(self, hours: float) -> str:
        """Categorize job by walltime in hours"""
        if hours <= 1:
            return '0-1h'
        elif hours <= 3:
            return '1-3h'
        elif hours <= 6:
            return '3-6h'
        elif hours <= 12:
            return '6-12h'
        elif hours <= 24:
            return '12-24h'
        else:
            return '24h+' 