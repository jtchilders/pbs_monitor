"""
Logging setup utility for PBS Monitor
"""

import logging
import logging.handlers
import sys
from typing import Optional
from pathlib import Path


def setup_logging(
   level: int = logging.INFO,
   log_file: Optional[str] = None,
   log_format: Optional[str] = None,
   date_format: Optional[str] = None,
   console_output: bool = True
) -> None:
   """
   Set up logging configuration for PBS Monitor
   
   Args:
      level: Logging level (default: INFO)
      log_file: Path to log file (optional)
      log_format: Custom log format (optional)
      date_format: Date format for timestamps (default: DD-MM HH:MM)
      console_output: Whether to output to console (default: True)
   """
   
   # Default formats following workspace rules
   if date_format is None:
      date_format = "%d-%m %H:%M"
   
   if log_format is None:
      log_format = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
   
   # Create formatter
   formatter = logging.Formatter(
      fmt=log_format,
      datefmt=date_format
   )
   
   # Get root logger
   logger = logging.getLogger()
   logger.setLevel(level)
   
   # Clear existing handlers
   logger.handlers.clear()
   
   # Console handler
   if console_output:
      console_handler = logging.StreamHandler(sys.stdout)
      console_handler.setLevel(level)
      console_handler.setFormatter(formatter)
      logger.addHandler(console_handler)
   
   # File handler
   if log_file:
      try:
         # Create directory if it doesn't exist
         log_path = Path(log_file)
         log_path.parent.mkdir(parents=True, exist_ok=True)
         
         # Create rotating file handler (10MB max, 5 backups)
         file_handler = logging.handlers.RotatingFileHandler(
            log_file,
            maxBytes=10 * 1024 * 1024,  # 10MB
            backupCount=5
         )
         file_handler.setLevel(level)
         file_handler.setFormatter(formatter)
         logger.addHandler(file_handler)
         
      except Exception as e:
         # If file logging fails, log to console
         console_logger = logging.getLogger(__name__)
         console_logger.error(f"Failed to set up file logging: {str(e)}")


def get_logger(name: str) -> logging.Logger:
   """
   Get a logger instance with the specified name
   
   Args:
      name: Logger name
      
   Returns:
      Logger instance
   """
   return logging.getLogger(name)


class PBSLoggerAdapter(logging.LoggerAdapter):
   """
   Custom logger adapter for PBS Monitor that adds context
   """
   
   def __init__(self, logger: logging.Logger, extra: dict = None):
      super().__init__(logger, extra or {})
   
   def process(self, msg, kwargs):
      """Process log message with additional context"""
      if self.extra:
         context_parts = []
         for key, value in self.extra.items():
            if value is not None:
               context_parts.append(f"{key}={value}")
         
         if context_parts:
            context_str = "[" + ", ".join(context_parts) + "]"
            msg = f"{context_str} {msg}"
      
      return msg, kwargs


def create_pbs_logger(name: str, **context) -> PBSLoggerAdapter:
   """
   Create a PBS logger with context
   
   Args:
      name: Logger name
      **context: Additional context to include in logs
      
   Returns:
      PBSLoggerAdapter instance
   """
   logger = logging.getLogger(name)
   return PBSLoggerAdapter(logger, context)


def set_log_level(level: int) -> None:
   """
   Set log level for all loggers
   
   Args:
      level: New log level
   """
   root_logger = logging.getLogger()
   root_logger.setLevel(level)
   
   # Update all handlers
   for handler in root_logger.handlers:
      handler.setLevel(level)


def enable_debug_logging() -> None:
   """Enable debug logging for troubleshooting"""
   set_log_level(logging.DEBUG)
   
   # Add debug information
   logger = logging.getLogger(__name__)
   logger.debug("Debug logging enabled")


def disable_logging() -> None:
   """Disable all logging output"""
   logging.disable(logging.CRITICAL)


def enable_logging() -> None:
   """Re-enable logging after disabling"""
   logging.disable(logging.NOTSET) 