"""
PBS Reservation data structure
"""

from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any
from dataclasses import dataclass, field
from enum import Enum
import re
import logging


class ReservationState(Enum):
    """PBS reservation states"""
    CONFIRMED = "RESV_CONFIRMED"      # Scheduled but not started
    RUNNING = "RESV_RUNNING"          # Currently active (RN in summary)
    FINISHED = "RESV_FINISHED"        # Completed
    DELETED = "RESV_DELETED"          # Cancelled
    DEGRADED = "RESV_DEGRADED"        # Some nodes unavailable
    CONFIRMED_SHORT = "CO"            # Confirmed state in summary output
    RUNNING_SHORT = "RN"              # Running state in summary output
    UNKNOWN = "unknown"

    @classmethod
    def from_pbs_state(cls, state_str: str) -> 'ReservationState':
        """Convert PBS state string to ReservationState enum"""
        # Map short forms from summary output
        short_state_map = {
            "RN": cls.RUNNING_SHORT,
            "CO": cls.CONFIRMED_SHORT,
        }
        
        # Try short form first
        if state_str in short_state_map:
            return short_state_map[state_str]
        
        # Try full state names
        for state in cls:
            if state.value == state_str:
                return state
        
        return cls.UNKNOWN


@dataclass
class PBSReservation:
    """Represents a PBS reservation"""
    
    # Core identifiers
    reservation_id: str                    # e.g., "S6703362.aurora-pbs-0001..."
    reservation_name: Optional[str] = None # e.g., "HACC-DAOS-Dbg"
    owner: Optional[str] = None            # Username without hostname
    
    # State and timing
    state: ReservationState = ReservationState.UNKNOWN
    start_time: Optional[datetime] = None
    end_time: Optional[datetime] = None
    duration_seconds: Optional[int] = None
    
    # Resources
    queue: Optional[str] = None            # Associated queue
    nodes: Optional[int] = None            # Node count
    ncpus: Optional[int] = None            # Total CPUs
    ngpus: Optional[int] = None            # Total GPUs
    walltime: Optional[str] = None         # HH:MM:SS format
    
    # Access control
    authorized_users: List[str] = field(default_factory=list)
    authorized_groups: List[str] = field(default_factory=list)
    
    # Additional metadata
    server: Optional[str] = None
    creation_time: Optional[datetime] = None
    modification_time: Optional[datetime] = None
    partition: Optional[str] = None
    
    # Recurring reservation attributes
    reserve_rrule: Optional[str] = None        # RFC 2445 RRULE (e.g., "FREQ=WEEKLY;COUNT=3")
    reserve_index: Optional[int] = None        # Current occurrence index (1-based)
    reserve_count: Optional[int] = None        # Total number of occurrences
    
    # Reserved nodes (if available)
    reserved_nodes: Optional[str] = None   # Formatted node list
    
    # Raw PBS attributes
    raw_attributes: Dict[str, Any] = field(default_factory=dict)
    
    @property
    def is_recurring(self) -> bool:
        """Check if this is a recurring reservation"""
        return self.reserve_rrule is not None
    
    @property
    def reservation_type(self) -> str:
        """Get reservation type as string for display"""
        if self.is_recurring:
            if self.reserve_index and self.reserve_count:
                return f"Recurring ({self.reserve_index}/{self.reserve_count})"
            else:
                return "Recurring"
        return "Single"
    
    def get_recurring_windows(self) -> List[Dict[str, Any]]:
        """
        Parse RRULE and generate all reservation windows for recurring reservations
        Returns list of dictionaries with 'index', 'start_time', 'end_time', 'duration_seconds'
        """
        if not self.is_recurring or not self.reserve_rrule or not self.start_time:
            return []
        
        try:
            windows = []
            
            # Parse RRULE (simplified for common PBS patterns)
            # Example: "FREQ=WEEKLY;COUNT=3" or "FREQ=DAILY;COUNT=5"
            rrule_parts = {}
            for part in self.reserve_rrule.split(';'):
                if '=' in part:
                    key, value = part.split('=', 1)
                    rrule_parts[key] = value
            
            freq = rrule_parts.get('FREQ', '').upper()
            count = int(rrule_parts.get('COUNT', 1))
            
            if not freq or count <= 0:
                return []
            
            # Calculate the interval between windows based on frequency
            if freq == 'DAILY':
                interval_days = 1
            elif freq == 'WEEKLY':
                interval_days = 7
            elif freq == 'MONTHLY':
                interval_days = 30  # Approximate
            else:
                # Unsupported frequency
                return []
            
            # Calculate base start time (for index 1)
            # Current reservation is at reserve_index, so we need to calculate backwards
            current_index = self.reserve_index or 1
            base_start = self.start_time
            
            # Go back to the first occurrence
            days_to_subtract = (current_index - 1) * interval_days
            if days_to_subtract > 0:
                base_start = self.start_time - timedelta(days=days_to_subtract)
            
            # Generate all windows
            duration = self.duration_seconds or 3600  # Default 1 hour if unknown
            
            for i in range(1, count + 1):
                window_start = base_start
                if i > 1:
                    window_start = base_start + timedelta(days=(i-1) * interval_days)
                
                window_end = window_start + timedelta(seconds=duration)
                
                windows.append({
                    'index': i,
                    'start_time': window_start,
                    'end_time': window_end,
                    'duration_seconds': duration,
                    'is_current': i == current_index
                })
            
            return windows
            
        except Exception as e:
            # If parsing fails, return empty list
            logging.getLogger(__name__).warning(f"Failed to parse RRULE '{self.reserve_rrule}': {e}")
            return []
    
    @classmethod
    def from_detailed_output(cls, reservation_text: str) -> 'PBSReservation':
        """Parse detailed pbs_rstat -f output into PBSReservation object"""
        logger = logging.getLogger(__name__)
        
        # Split into lines and parse key-value pairs
        lines = reservation_text.strip().split('\n')
        if not lines:
            raise ValueError("Empty reservation text")
        
        # First line should be "Resv ID: ..."
        first_line = lines[0].strip()
        if not first_line.startswith("Resv ID:"):
            raise ValueError(f"Expected 'Resv ID:' at start, got: {first_line}")
        
        reservation_id = first_line.split(":", 1)[1].strip()
        
        # Parse the rest of the attributes
        attributes = {}
        for line in lines[1:]:
            line = line.strip()
            if not line:
                continue
            
            if " = " in line:
                key, value = line.split(" = ", 1)
                attributes[key.strip()] = value.strip()
        
        # Extract core fields
        reservation_name = attributes.get("Reserve_Name")
        
        # Extract owner (remove hostname part)
        owner_full = attributes.get("Reserve_Owner", "")
        owner = owner_full.split("@")[0] if "@" in owner_full else owner_full
        
        # Parse state
        state_str = attributes.get("reserve_state", "unknown")
        state = ReservationState.from_pbs_state(state_str)
        
        # Parse timing
        start_time = cls._parse_pbs_datetime(attributes.get("reserve_start"))
        end_time = cls._parse_pbs_datetime(attributes.get("reserve_end"))
        duration_seconds = cls._parse_duration(attributes.get("reserve_duration"))
        
        # Parse resources
        queue = attributes.get("queue")
        nodes = cls._parse_int(attributes.get("Resource_List.nodect"))
        ncpus = cls._parse_int(attributes.get("Resource_List.ncpus"))
        ngpus = cls._parse_int(attributes.get("Resource_List.ngpus"))
        walltime = attributes.get("Resource_List.walltime")
        
        # Parse access control
        authorized_users = cls._parse_list(attributes.get("Authorized_Users", ""))
        authorized_groups = cls._parse_list(attributes.get("Authorized_Groups", ""))
        
        # Additional metadata
        server = attributes.get("server")
        creation_time = cls._parse_pbs_datetime(attributes.get("ctime"))
        modification_time = cls._parse_pbs_datetime(attributes.get("mtime"))
        partition = attributes.get("partition")
        reserved_nodes = attributes.get("resv_nodes")
        
        # Recurring reservation attributes
        reserve_rrule = attributes.get("reserve_rrule")
        reserve_index = cls._parse_int(attributes.get("reserve_index"))
        reserve_count = cls._parse_int(attributes.get("reserve_count"))
        
        return cls(
            reservation_id=reservation_id,
            reservation_name=reservation_name,
            owner=owner,
            state=state,
            start_time=start_time,
            end_time=end_time,
            duration_seconds=duration_seconds,
            queue=queue,
            nodes=nodes,
            ncpus=ncpus,
            ngpus=ngpus,
            walltime=walltime,
            authorized_users=authorized_users,
            authorized_groups=authorized_groups,
            server=server,
            creation_time=creation_time,
            modification_time=modification_time,
            partition=partition,
            reserved_nodes=reserved_nodes,
            reserve_rrule=reserve_rrule,
            reserve_index=reserve_index,
            reserve_count=reserve_count,
            raw_attributes=attributes
        )
    
    @classmethod
    def from_summary_line(cls, summary_line: str) -> 'PBSReservation':
        """Parse single line from pbs_rstat summary into PBSReservation object"""
        logger = logging.getLogger(__name__)
        
        # Pattern: ResID | Queue | User | State | Start/Duration/End
        # Example: S6703362.aurora S6703362      richp@au RN          Today 10:00 / 14400 / Today 14:00
        
        # Use regex for flexible parsing of fixed-width columns
        # This pattern accounts for varying column spacing
        pattern = r'^(\S+)\s+(\S+)\s+(\S+)\s+(\S+)\s+(.+)$'
        match = re.match(pattern, summary_line.strip())
        
        if not match:
            raise ValueError(f"Could not parse reservation line: {summary_line}")
        
        resv_id, queue, user, state_str, timing = match.groups()
        
        # Parse timing field (e.g., "Today 10:00 / 14400 / Today 14:00")
        start_time, duration_seconds, end_time = cls._parse_timing_field(timing)
        
        # Clean up user (remove hostname)
        owner = user.split('@')[0] if '@' in user else user
        
        # Parse state
        state = ReservationState.from_pbs_state(state_str)
        
        # Extract base reservation ID (everything before the first dot, if any)
        # This is needed because PBS summary shows truncated IDs like "S6703362.aurora"
        # but the detailed command expects just the base ID like "S6703362"
        base_resv_id = resv_id.split('.')[0] if '.' in resv_id else resv_id
        
        return cls(
            reservation_id=base_resv_id,
            owner=owner,
            state=state,
            queue=queue,
            start_time=start_time,
            end_time=end_time,
            duration_seconds=duration_seconds,
            raw_attributes={"source": "summary", "full_id": resv_id}
        )
    
    @staticmethod
    def _parse_pbs_datetime(datetime_str: Optional[str]) -> Optional[datetime]:
        """Parse PBS datetime format"""
        if not datetime_str:
            return None
        
        try:
            # PBS format: "Wed Aug 06 10:00:00 2025"
            return datetime.strptime(datetime_str, "%a %b %d %H:%M:%S %Y")
        except ValueError:
            logging.getLogger(__name__).warning(f"Could not parse datetime: {datetime_str}")
            return None
    
    @staticmethod
    def _parse_duration(duration_str: Optional[str]) -> Optional[int]:
        """Parse duration string to seconds"""
        if not duration_str:
            return None
        
        try:
            return int(duration_str)
        except ValueError:
            logging.getLogger(__name__).warning(f"Could not parse duration: {duration_str}")
            return None
    
    @staticmethod
    def _parse_int(value_str: Optional[str]) -> Optional[int]:
        """Parse integer value with error handling"""
        if not value_str:
            return None
        
        try:
            return int(value_str)
        except ValueError:
            return None
    
    @staticmethod
    def _parse_list(list_str: str) -> List[str]:
        """Parse comma-separated list"""
        if not list_str:
            return []
        
        # Handle both comma and space separation
        items = re.split(r'[,\s]+', list_str.strip())
        return [item.strip() for item in items if item.strip()]
    
    @staticmethod
    def _parse_timing_field(timing_str: str) -> tuple[Optional[datetime], Optional[int], Optional[datetime]]:
        """Parse timing field from summary format"""
        # Example: "Today 10:00 / 14400 / Today 14:00"
        # Example: "Mon Jul 28 16:00 / 1411200 / Thu Aug 14 00:00"
        
        parts = timing_str.split(" / ")
        if len(parts) != 3:
            logging.getLogger(__name__).warning(f"Unexpected timing format: {timing_str}")
            return None, None, None
        
        start_str, duration_str, end_str = [part.strip() for part in parts]
        
        # Parse duration (always in seconds)
        try:
            duration_seconds = int(duration_str)
        except ValueError:
            duration_seconds = None
        
        # Parse start and end times
        start_time = PBSReservation._parse_summary_datetime(start_str)
        end_time = PBSReservation._parse_summary_datetime(end_str)
        
        return start_time, duration_seconds, end_time
    
    @staticmethod
    def _parse_summary_datetime(datetime_str: str) -> Optional[datetime]:
        """Parse datetime from summary format"""
        if not datetime_str:
            return None
        
        try:
            # Handle "Today HH:MM" format
            if datetime_str.startswith("Today "):
                time_part = datetime_str.replace("Today ", "")
                # For simplicity, use current date - in real usage this would need proper date handling
                from datetime import date
                today = date.today()
                time_obj = datetime.strptime(time_part, "%H:%M").time()
                return datetime.combine(today, time_obj)
            
            # Handle "Thu HH:MM" format  
            elif len(datetime_str.split()) == 2:
                # Simple day + time, assume current week
                # This is a simplified parser - real implementation would need better date logic
                time_part = datetime_str.split()[1]
                from datetime import date
                today = date.today()
                time_obj = datetime.strptime(time_part, "%H:%M").time()
                return datetime.combine(today, time_obj)
            
            # Handle full format "Mon Jul 28 16:00"
            elif len(datetime_str.split()) >= 4:
                # Add current year if not present
                if not any(part.isdigit() and len(part) == 4 for part in datetime_str.split()):
                    datetime_str += f" {datetime.now().year}"
                return datetime.strptime(datetime_str, "%a %b %d %H:%M %Y")
            
        except ValueError as e:
            logging.getLogger(__name__).warning(f"Could not parse summary datetime '{datetime_str}': {e}")
        
        return None
    
    def __str__(self) -> str:
        """String representation of reservation"""
        return f"PBSReservation({self.reservation_id}, {self.reservation_name}, {self.state.value})"
    
    def __repr__(self) -> str:
        """Detailed string representation"""
        return (f"PBSReservation(id='{self.reservation_id}', "
                f"name='{self.reservation_name}', state='{self.state.value}', "
                f"owner='{self.owner}', nodes={self.nodes})")