"""
Configuration management for PBS Monitor
"""

import os
import re
import stat
import yaml
import logging
from typing import Dict, Any, Optional, List
from pathlib import Path
from dataclasses import dataclass, field


class InsecureConfigError(Exception):
   """Raised when a config file containing credentials has overly
   permissive filesystem permissions (group or other can read).

   Bypass with ``allow_insecure=True`` only for debugging.
   """


# Matches a SQLAlchemy URL that embeds inline credentials, e.g.
# postgresql://user:password@host/db. We treat any URL with a non-empty
# password segment as credential-bearing.
_CREDENTIAL_URL_RE = re.compile(
   r"^[a-zA-Z][a-zA-Z0-9+.-]*://[^:/?#@]+:[^@/?#]+@",
)


def _config_contains_credentials(config_data: Optional[Dict[str, Any]]) -> bool:
   """Return True if the loaded YAML contains a credential-bearing DB URL."""
   if not config_data:
      return False
   db_section = config_data.get("database") or {}
   url = db_section.get("url") if isinstance(db_section, dict) else None
   if not isinstance(url, str):
      return False
   return bool(_CREDENTIAL_URL_RE.match(url))


def _check_config_permissions(path: str) -> None:
   """Refuse to load if ``path`` is readable by group or other.

   Only invoked when the file contains inline credentials. Raises
   :class:`InsecureConfigError` with a remediation hint.
   """
   try:
      mode = os.stat(path).st_mode
   except OSError:
      return  # No file → nothing to check; caller handles absence.
   # Reject ANY group/other permission bits.
   if mode & (stat.S_IRWXG | stat.S_IRWXO):
      perms = oct(mode & 0o777)
      raise InsecureConfigError(
         f"{path} contains database credentials but has permissions "
         f"{perms} (group/other can read). Run: chmod 600 {path}"
      )


@dataclass
class PBSConfig:
   """PBS system configuration"""

   # Logical name of the PBS system this daemon is collecting from.
   # Used to tag every row written to the database so that a single
   # multi-system database can host data from Polaris, Aurora, etc.
   # without identifier collisions. Required; no sensible default.
   system: Optional[str] = None

   # PBS command paths (if not in PATH)
   qstat_path: str = "qstat"
   qsub_path: str = "qsub"
   qdel_path: str = "qdel"
   qhold_path: str = "qhold"
   qrls_path: str = "qrls"
   pbsnodes_path: str = "pbsnodes"
   
   # Command timeouts
   command_timeout: int = 30
   
   # Default queue settings
   default_queue: Optional[str] = None
   
   # Refresh intervals (seconds)
   job_refresh_interval: int = 30
   node_refresh_interval: int = 60
   queue_refresh_interval: int = 300
   server_refresh_interval: int = 3600  # 1 hour - server info changes infrequently


@dataclass
class DisplayConfig:
   """Display and output configuration"""
   
   # Table display options
   max_table_width: int = 120
   auto_width: bool = True  # Auto-detect terminal width
   min_column_width: int = 8
   max_column_width: int = 50
   truncate_long_names: bool = True
   max_name_length: int = 20
   
   # Column width behavior
   expand_columns: bool = True  # Allow columns to expand to fit content
   word_wrap: bool = False  # Enable word wrapping in columns
   
   # Color output
   use_colors: bool = True
   
   # Time format
   time_format: str = "%d-%m %H:%M"
   
   # Default columns to show
   default_job_columns: List[str] = field(default_factory=lambda: [
      "job_id", "state", "queue", "owner", "project", "allocation", "nodes", "walltime", "node_hours", "score", "queue_time"
   ])
   
   default_node_columns: List[str] = field(default_factory=lambda: [
      "name", "state", "ncpus", "memory", "jobs", "load"
   ])
   
   default_queue_columns: List[str] = field(default_factory=lambda: [
      "name", "status", "running", "queued", "held"
   ])


@dataclass
class DatabaseConfig:
   """Database configuration"""
   
   # Database URL
   url: str = f"sqlite:///{Path.home()}/.pbs_monitor.db"
   
   # Database schema (for multi-system Postgres deployments)
   # Each monitored system should use its own schema (e.g. "polaris", "aurora")
   schema: str = "public"
   
   # Connection settings
   pool_size: int = 5
   max_overflow: int = 10
   echo_sql: bool = False
   
   # Collection intervals (seconds)
   job_collection_interval: int = 900      # 15 minutes
   node_collection_interval: int = 1800    # 30 minutes
   queue_collection_interval: int = 3600   # 60 minutes
   snapshot_interval: int = 1800           # 30 minutes
   
   # Data retention settings
   job_history_days: int = 365             # Keep job history for 1 year
   snapshot_retention_days: int = 90       # Keep snapshots for 90 days
   
   # Collection settings
   daemon_enabled: bool = True
   auto_persist: bool = False
   auto_persist_interval: int = 300        # 5 minutes (300 seconds)
   batch_size: int = 1000
   
   # Orphaned job detection settings
   orphaned_job_detection: bool = True     # Enable detection of orphaned jobs
   orphaned_job_threshold_minutes: int = 60  # Minutes before marking job as orphaned
   
   # Missing reservation detection settings
   missing_reservation_detection: bool = True  # Enable detection of missing reservations
   missing_reservation_threshold_minutes: int = 30  # Minutes before marking reservation as missing


@dataclass
class LoggingConfig:
   """Logging configuration"""
   
   # Log level
   level: str = "INFO"
   
   # Log file path
   log_file: Optional[str] = None
   
   # Log format
   log_format: str = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
   
   # Date format (following workspace rules)
   date_format: str = "%d-%m %H:%M"


class Config:
   """Main configuration manager"""

   def __init__(
      self,
      config_file: Optional[str] = None,
      allow_insecure_config: bool = False,
   ):
      """
      Initialize configuration

      Args:
         config_file: Path to configuration file.
         allow_insecure_config: If True, skip the permission check that
            normally refuses to load a credential-bearing config file
            with group/other read permissions. Intended for debugging
            only; production deployments should use ``chmod 600``.
      """
      self.config_file = config_file or self._get_default_config_path()
      self.allow_insecure_config = allow_insecure_config
      self.logger = logging.getLogger(__name__)

      # Initialize default configurations
      self.pbs = PBSConfig()
      self.display = DisplayConfig()
      self.logging = LoggingConfig()
      self.database = DatabaseConfig()

      # Load configuration from file
      self._load_config()
   
   def _get_default_config_path(self) -> str:
      """Get default configuration file path"""
      # Try these locations in order
      config_paths = [
         os.path.expanduser("~/.pbs_monitor.yaml"),
         os.path.expanduser("~/.config/pbs_monitor/config.yaml"),
         "/etc/pbs_monitor/config.yaml",
         "pbs_monitor.yaml"
      ]
      
      for path in config_paths:
         if os.path.exists(path):
            return path
      
      # Return first path as default
      return config_paths[0]
   
   def _load_config(self) -> None:
      """Load configuration from file

      Raises:
         InsecureConfigError: if the file embeds DB credentials and is
            readable by group or other (and ``allow_insecure_config``
            is False).
      """
      if not os.path.exists(self.config_file):
         self.logger.debug(f"Configuration file not found: {self.config_file}")
         return

      try:
         with open(self.config_file, 'r') as f:
            config_data = yaml.safe_load(f)

         if not config_data:
            return

         # Refuse to use a credential-bearing config that's world/group readable.
         if _config_contains_credentials(config_data) and not self.allow_insecure_config:
            _check_config_permissions(self.config_file)

         # Update PBS configuration
         if 'pbs' in config_data:
            self._update_config_object(self.pbs, config_data['pbs'])
         
         # Update display configuration
         if 'display' in config_data:
            self._update_config_object(self.display, config_data['display'])
         
         # Update logging configuration
         if 'logging' in config_data:
            self._update_config_object(self.logging, config_data['logging'])
         
         # Update database configuration
         if 'database' in config_data:
            self._update_config_object(self.database, config_data['database'])
         
         self.logger.info(f"Configuration loaded from {self.config_file}")

      except InsecureConfigError:
         # Surface the security failure to the caller instead of
         # silently degrading to defaults. Logged at error level too.
         raise
      except Exception as e:
         self.logger.error(f"Failed to load configuration: {str(e)}")
   
   def _update_config_object(self, config_obj: Any, config_data: Dict[str, Any]) -> None:
      """Update configuration object with data from file"""
      for key, value in config_data.items():
         if hasattr(config_obj, key):
            setattr(config_obj, key, value)
   
   def save_config(self) -> None:
      """Save current configuration to file"""
      try:
         # Create directory if it doesn't exist
         config_dir = os.path.dirname(self.config_file)
         if config_dir:
            os.makedirs(config_dir, exist_ok=True)
         
         config_data = {
            'pbs': self._config_to_dict(self.pbs),
            'display': self._config_to_dict(self.display),
            'logging': self._config_to_dict(self.logging),
            'database': self._config_to_dict(self.database)
         }
         
         with open(self.config_file, 'w') as f:
            yaml.dump(config_data, f, default_flow_style=False, indent=2)
         
         self.logger.info(f"Configuration saved to {self.config_file}")
         
      except Exception as e:
         self.logger.error(f"Failed to save configuration: {str(e)}")
   
   def _config_to_dict(self, config_obj: Any) -> Dict[str, Any]:
      """Convert configuration object to dictionary"""
      if hasattr(config_obj, '__dict__'):
         return {k: v for k, v in config_obj.__dict__.items() if not k.startswith('_')}
      return {}
   
   def create_sample_config(self) -> None:
      """Create a sample configuration file"""
      sample_config = {
         'pbs': {
            # REQUIRED: logical name of the PBS system this daemon
            # collects from (e.g. "polaris", "aurora"). Used to tag
            # every row written to the database.
            'system': 'polaris',
            'command_timeout': 30,
            'default_queue': 'default',
            'job_refresh_interval': 30,
            'node_refresh_interval': 60,
            'queue_refresh_interval': 300,
            'server_refresh_interval': 3600
         },
         'display': {
            'max_table_width': 120,
            'auto_width': True,
            'min_column_width': 8,
            'max_column_width': 50,
            'truncate_long_names': True,
            'max_name_length': 20,
            'expand_columns': True,
            'word_wrap': False,
            'use_colors': True,
            'time_format': '%d-%m %H:%M',
            'default_job_columns': [
               'job_id', 'state', 'queue', 'owner', 'project', 'allocation', 'nodes', 'walltime', 'node_hours', 'score', 'queue_time'
            ],
            'default_node_columns': [
               'name', 'state', 'ncpus', 'memory', 'jobs', 'load'
            ],
            'default_queue_columns': [
               'name', 'status', 'running', 'queued', 'held'
            ]
         },
         'logging': {
            'level': 'INFO',
            'log_file': None,
            'log_format': '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            'date_format': '%d-%m %H:%M'
         },
         'database': {
            'url': 'sqlite:///~/.pbs_monitor.db',  # or: postgresql://pbs_monitor:PASSWORD@localhost:5432/pbs_monitor
            'schema': 'public',  # Change per system: 'polaris', 'aurora', etc.
            'pool_size': 5,
            'max_overflow': 10,
            'echo_sql': False,
            'job_collection_interval': 900,
            'node_collection_interval': 1800,
            'queue_collection_interval': 3600,
            'snapshot_interval': 1800,
            'job_history_days': 365,
            'snapshot_retention_days': 90,
            'daemon_enabled': True,
            'auto_persist': False,
            'auto_persist_interval': 300,
            'batch_size': 1000,
            'orphaned_job_detection': True,
            'orphaned_job_threshold_minutes': 60,
            'missing_reservation_detection': True,
            'missing_reservation_threshold_minutes': 30
         }
      }
      
      try:
         config_dir = os.path.dirname(self.config_file)
         if config_dir:
            os.makedirs(config_dir, exist_ok=True)
         
         with open(self.config_file, 'w') as f:
            yaml.dump(sample_config, f, default_flow_style=False, indent=2)
         
         print(f"Sample configuration created at {self.config_file}")
         
      except Exception as e:
         print(f"Failed to create sample configuration: {str(e)}")
   
   def get_log_level(self) -> int:
      """Get numeric log level"""
      level_map = {
         'DEBUG': logging.DEBUG,
         'INFO': logging.INFO,
         'WARNING': logging.WARNING,
         'ERROR': logging.ERROR,
         'CRITICAL': logging.CRITICAL
      }
      
      return level_map.get(self.logging.level.upper(), logging.INFO)
   
   def __str__(self) -> str:
      return f"Config(file={self.config_file})" 