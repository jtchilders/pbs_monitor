"""
Configuration management for PBS Monitor
"""

import os
import yaml
import logging
from typing import Dict, Any, Optional, List
from pathlib import Path
from dataclasses import dataclass, field


@dataclass
class PBSConfig:
   """PBS system configuration"""
   
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
      "job_id", "name", "owner", "project", "allocation", "state", "queue", "nodes", "walltime", "score"
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
   url: str = "sqlite:///~/.pbs_monitor.db"
   
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
   
   def __init__(self, config_file: Optional[str] = None):
      """
      Initialize configuration
      
      Args:
         config_file: Path to configuration file
      """
      self.config_file = config_file or self._get_default_config_path()
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
      """Load configuration from file"""
      if not os.path.exists(self.config_file):
         self.logger.debug(f"Configuration file not found: {self.config_file}")
         return
      
      try:
         with open(self.config_file, 'r') as f:
            config_data = yaml.safe_load(f)
         
         if not config_data:
            return
         
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
               'job_id', 'name', 'owner', 'state', 'queue', 'nodes', 'walltime', 'score'
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
            'url': 'sqlite:///~/.pbs_monitor.db',
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
            'batch_size': 1000
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