"""
Analytics module for PBS Monitor

Provides analytics features like queue depth analysis, job score analysis,
run-now opportunities, and system trends.
"""

from .queue_depth import QueueDepthCalculator

__all__ = ['QueueDepthCalculator'] 