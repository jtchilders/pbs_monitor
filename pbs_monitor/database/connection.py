"""
Database Connection Management for PBS Monitor

This module provides database connection management, session handling,
and table creation utilities for the PBS Monitor database.
"""

import os
import logging
from contextlib import contextmanager
from typing import Dict, Optional, Any, Generator
from pathlib import Path

from sqlalchemy import create_engine, MetaData, inspect
from sqlalchemy.orm import sessionmaker, Session
from sqlalchemy.engine import Engine
from sqlalchemy.pool import StaticPool

from .models import Base
from ..config import Config
from ..utils.logging_setup import create_pbs_logger

logger = create_pbs_logger(__name__)

class DatabaseManager:
    """
    Database connection and session management
    
    Provides centralized database connection handling with connection pooling,
    session management, and configuration integration.
    """
    
    def __init__(self, config: Optional[Config] = None):
        """
        Initialize database manager
        
        Args:
            config: Configuration object (optional)
        """
        self.config = config or Config()
        self.engine: Optional[Engine] = None
        self.session_factory: Optional[sessionmaker] = None
        self._initialized = False
    
    def initialize(self) -> None:
        """Initialize database engine and session factory"""
        if self._initialized:
            return
        
        database_url = self._get_database_url()
        engine_options = self._get_engine_options()
        
        logger.info(f"Initializing database connection to: {self._mask_url(database_url)}")
        
        self.engine = create_engine(database_url, **engine_options)
        self.session_factory = sessionmaker(bind=self.engine)
        self._initialized = True
        
        logger.info("Database connection initialized successfully")
    
    def _get_database_url(self) -> str:
        """Get database URL from configuration"""
        url = None
        
        # Check environment variable first
        if 'PBS_MONITOR_DB_URL' in os.environ:
            url = os.environ['PBS_MONITOR_DB_URL']
        
        # Check for database configuration in config
        elif hasattr(self.config, 'database') and hasattr(self.config.database, 'url'):
            url = self.config.database.url
        
        # Default to SQLite in user home directory
        else:
            db_path = Path.home() / '.pbs_monitor.db'
            url = f"sqlite:///{db_path}"
        
        # Expand ~ in path for SQLite URLs
        if url.startswith('sqlite:') and '~' in url:
            path_part = url.replace('sqlite:///', '')
            expanded_path = os.path.expanduser(path_part)
            url = f"sqlite:///{expanded_path}"
        
        return url
    
    def _get_engine_options(self) -> Dict[str, Any]:
        """Get SQLAlchemy engine options"""
        options = {
            'pool_pre_ping': True,
            'pool_recycle': 3600,
            'echo': False,
        }
        
        # Add configuration options if available
        if hasattr(self.config, 'database'):
            db_config = self.config.database
            
            if hasattr(db_config, 'echo_sql'):
                options['echo'] = db_config.echo_sql
                
            if hasattr(db_config, 'pool_size'):
                options['pool_size'] = db_config.pool_size
                
            if hasattr(db_config, 'max_overflow'):
                options['max_overflow'] = db_config.max_overflow
        
        # Special handling for SQLite
        database_url = self._get_database_url()
        if database_url.startswith('sqlite:'):
            options.update({
                'poolclass': StaticPool,
                'connect_args': {
                    'check_same_thread': False,
                    'timeout': 30
                }
            })
            # Remove pool size options for SQLite
            options.pop('pool_size', None)
            options.pop('max_overflow', None)
        
        return options
    
    def _mask_url(self, url: str) -> str:
        """Mask password in database URL for logging"""
        if '://' in url and '@' in url:
            # Format: postgresql://user:password@host:port/database
            parts = url.split('://', 1)
            if len(parts) == 2:
                scheme, rest = parts
                if '@' in rest:
                    auth_part, host_part = rest.split('@', 1)
                    if ':' in auth_part:
                        user, _ = auth_part.split(':', 1)
                        return f"{scheme}://{user}:***@{host_part}"
        return url
    
    @contextmanager
    def get_session(self) -> Generator[Session, None, None]:
        """
        Get database session with automatic cleanup
        
        Yields:
            SQLAlchemy session
        """
        if not self._initialized:
            self.initialize()
        
        session = self.session_factory()
        try:
            yield session
            session.commit()
        except Exception as e:
            session.rollback()
            logger.error(f"Database session error: {str(e)}")
            raise
        finally:
            session.close()
    
    def test_connection(self) -> bool:
        """Test database connection"""
        try:
            with self.get_session() as session:
                # Simple test query
                from sqlalchemy import text
                session.execute(text("SELECT 1"))
                return True
        except Exception:
            return False
    
    def create_tables(self) -> None:
        """Create all database tables"""
        if not self._initialized:
            self.initialize()
        
        logger.info("Creating database tables...")
        Base.metadata.create_all(self.engine)
        logger.info("Database tables created successfully")
    
    def drop_tables(self) -> None:
        """Drop all database tables"""
        if not self._initialized:
            self.initialize()
        
        logger.warning("Dropping all database tables...")
        Base.metadata.drop_all(self.engine)
        logger.info("Database tables dropped successfully")
    
    def table_exists(self, table_name: str) -> bool:
        """Check if a table exists in the database"""
        if not self._initialized:
            self.initialize()
        
        inspector = inspect(self.engine)
        return table_name in inspector.get_table_names()
    
    def get_table_names(self) -> list:
        """Get list of all table names"""
        if not self._initialized:
            self.initialize()
        
        inspector = inspect(self.engine)
        return inspector.get_table_names()
    
    def vacuum_database(self) -> None:
        """Vacuum/optimize database (SQLite only)"""
        if not self._initialized:
            self.initialize()
        
        database_url = self._get_database_url()
        if database_url.startswith('sqlite:'):
            logger.info("Vacuuming SQLite database...")
            with self.engine.connect() as conn:
                conn.execute("VACUUM")
            logger.info("Database vacuum completed")
        else:
            logger.info("Vacuum not supported for this database type")
    
    def get_database_size(self) -> Optional[int]:
        """Get database size in bytes (SQLite only)"""
        database_url = self._get_database_url()
        if database_url.startswith('sqlite:'):
            # Extract file path from SQLite URL
            if '///' in database_url:
                db_path = database_url.split('///', 1)[1]
                try:
                    return Path(db_path).stat().st_size
                except (OSError, FileNotFoundError):
                    return None
        return None
    
    def close(self) -> None:
        """Close database connections"""
        if self.engine:
            self.engine.dispose()
            logger.info("Database connections closed")

# Global database manager instance
_database_manager: Optional[DatabaseManager] = None

def get_database_manager(config: Optional[Config] = None) -> DatabaseManager:
    """
    Get global database manager instance
    
    Args:
        config: Configuration object (optional)
        
    Returns:
        DatabaseManager instance
    """
    global _database_manager
    
    if _database_manager is None:
        _database_manager = DatabaseManager(config)
    
    return _database_manager

def create_tables(config: Optional[Config] = None) -> None:
    """
    Create all database tables
    
    Args:
        config: Configuration object (optional)
    """
    db_manager = get_database_manager(config)
    db_manager.create_tables()

def drop_tables(config: Optional[Config] = None) -> None:
    """
    Drop all database tables
    
    Args:
        config: Configuration object (optional)
    """
    db_manager = get_database_manager(config)
    db_manager.drop_tables()

def initialize_database(config: Optional[Config] = None) -> None:
    """
    Initialize database with tables if they don't exist
    
    Args:
        config: Configuration object (optional)
    """
    db_manager = get_database_manager(config)
    db_manager.initialize()
    
    # Create tables if they don't exist
    required_tables = [
        'jobs', 'job_history', 'queues', 'queue_snapshots', 
        'nodes', 'node_snapshots', 'system_snapshots', 'data_collection_log'
    ]
    
    missing_tables = []
    for table_name in required_tables:
        if not db_manager.table_exists(table_name):
            missing_tables.append(table_name)
    
    if missing_tables:
        logger.info(f"Creating missing tables: {', '.join(missing_tables)}")
        db_manager.create_tables()
    else:
        logger.info("All database tables already exist")

@contextmanager
def get_db_session(config: Optional[Config] = None) -> Generator[Session, None, None]:
    """
    Context manager for database sessions
    
    Args:
        config: Configuration object (optional)
        
    Yields:
        SQLAlchemy session
    """
    db_manager = get_database_manager(config)
    with db_manager.get_session() as session:
        yield session 