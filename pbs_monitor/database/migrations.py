"""
Database Migration Utilities for PBS Monitor

This module provides utilities for database initialization, schema updates,
and data migrations for the PBS Monitor database.
"""

import os
import sys
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional, Dict, Any, List
from sqlalchemy import inspect, text
from sqlalchemy.engine import Engine
from sqlalchemy.exc import OperationalError

from .models import Base, Job, JobHistory, Queue, QueueSnapshot, Node, NodeSnapshot, SystemSnapshot, DataCollectionLog
from .connection import get_database_manager, DatabaseManager
from ..config import Config
from ..utils.logging_setup import create_pbs_logger

logger = create_pbs_logger(__name__)

class DatabaseMigration:
    """Database migration manager"""
    
    def __init__(self, config: Optional[Config] = None):
        self.config = config or Config()
        self.db_manager = get_database_manager(config)
        self.db_manager.initialize()
        
    def check_database_exists(self) -> bool:
        """Check if database exists and is accessible"""
        try:
            with self.db_manager.get_session() as session:
                session.execute(text("SELECT 1"))
                return True
        except Exception as e:
            logger.error(f"Database check failed: {str(e)}")
            return False
    
    def get_existing_tables(self) -> List[str]:
        """Get list of existing tables in database"""
        try:
            inspector = inspect(self.db_manager.engine)
            return inspector.get_table_names()
        except Exception as e:
            logger.error(f"Failed to get table names: {str(e)}")
            return []
    
    def get_required_tables(self) -> List[str]:
        """Get list of required tables from models"""
        return [
            'jobs',
            'job_history',
            'queues',
            'queue_snapshots',
            'nodes',
            'node_snapshots',
            'system_snapshots',
            'data_collection_log'
        ]
    
    def check_schema_version(self) -> Optional[str]:
        """Check current schema version"""
        try:
            with self.db_manager.get_session() as session:
                # Try to query a table that would exist in our schema
                result = session.execute(text("SELECT COUNT(*) FROM data_collection_log"))
                result.fetchone()
                return "1.0.0"  # Current schema version
        except Exception:
            return None
    
    def create_fresh_database(self) -> None:
        """Create a fresh database with all tables"""
        logger.info("Creating fresh database...")
        
        try:
            # Create all tables
            Base.metadata.create_all(self.db_manager.engine)
            
            # Log the creation
            logger.info("Database tables created successfully")
            
            # Create initial data if needed
            self._create_initial_data()
            
        except Exception as e:
            logger.error(f"Failed to create database: {str(e)}")
            raise
    
    def _create_initial_data(self) -> None:
        """Create initial data for the database"""
        logger.info("Creating initial data...")
        
        try:
            with self.db_manager.get_session() as session:
                # Create an initial data collection log entry
                from .models import DataCollectionStatus
                initial_log = DataCollectionLog(
                    collection_type="database_init",
                    status=DataCollectionStatus.SUCCESS,
                    jobs_collected=0,
                    queues_collected=0,
                    nodes_collected=0,
                    duration_seconds=0,
                    timestamp=datetime.now()
                )
                session.add(initial_log)
                session.commit()
                
                logger.info("Initial data created successfully")
                
        except Exception as e:
            logger.error(f"Failed to create initial data: {str(e)}")
            raise
    
    def migrate_to_latest(self) -> None:
        """Migrate database to latest schema version"""
        current_version = self.check_schema_version()
        
        if current_version is None:
            logger.info("No existing schema detected, creating fresh database")
            self.create_fresh_database()
            return
        
        logger.info(f"Current schema version: {current_version}")
        
        # Add migration logic here for future schema updates
        # For now, we only have version 1.0.0
        if current_version == "1.0.0":
            logger.info("Database schema is up to date")
            return
        
        # Future migrations would go here
        logger.warning(f"Unknown schema version: {current_version}")
    
    def validate_schema(self) -> Dict[str, Any]:
        """Validate database schema"""
        validation_results = {
            'valid': True,
            'errors': [],
            'warnings': [],
            'table_status': {}
        }
        
        try:
            existing_tables = self.get_existing_tables()
            required_tables = self.get_required_tables()
            
            # Check for missing tables
            missing_tables = set(required_tables) - set(existing_tables)
            if missing_tables:
                validation_results['valid'] = False
                validation_results['errors'].append(f"Missing tables: {', '.join(missing_tables)}")
            
            # Check for extra tables
            extra_tables = set(existing_tables) - set(required_tables)
            if extra_tables:
                validation_results['warnings'].append(f"Extra tables found: {', '.join(extra_tables)}")
            
            # Check each required table
            for table in required_tables:
                if table in existing_tables:
                    validation_results['table_status'][table] = 'exists'
                else:
                    validation_results['table_status'][table] = 'missing'
            
            # Check table structures
            if validation_results['valid']:
                self._validate_table_structures(validation_results)
                
        except Exception as e:
            validation_results['valid'] = False
            validation_results['errors'].append(f"Schema validation error: {str(e)}")
        
        return validation_results
    
    def _validate_table_structures(self, validation_results: Dict[str, Any]) -> None:
        """Validate table structures against models"""
        try:
            inspector = inspect(self.db_manager.engine)
            
            # Check key columns for each table
            table_checks = {
                'jobs': ['job_id', 'job_name', 'owner', 'state', 'queue'],
                'job_history': ['id', 'job_id', 'timestamp', 'state'],
                'queues': ['name', 'queue_type', 'max_running'],
                'queue_snapshots': ['id', 'queue_name', 'timestamp', 'state'],
                'nodes': ['name', 'ncpus', 'memory_gb'],
                'node_snapshots': ['id', 'node_name', 'timestamp', 'state'],
                'system_snapshots': ['id', 'timestamp', 'total_jobs'],
                'data_collection_log': ['id', 'timestamp', 'collection_type', 'status']
            }
            
            for table_name, required_columns in table_checks.items():
                if table_name in inspector.get_table_names():
                    existing_columns = [col['name'] for col in inspector.get_columns(table_name)]
                    missing_columns = set(required_columns) - set(existing_columns)
                    
                    if missing_columns:
                        validation_results['valid'] = False
                        validation_results['errors'].append(
                            f"Table '{table_name}' missing columns: {', '.join(missing_columns)}"
                        )
                    
        except Exception as e:
            validation_results['errors'].append(f"Table structure validation error: {str(e)}")
    
    def backup_database(self, backup_path: Optional[str] = None) -> str:
        """Create database backup (SQLite only)"""
        database_url = self.db_manager._get_database_url()
        
        if not database_url.startswith('sqlite:'):
            raise ValueError("Database backup only supported for SQLite databases")
        
        # Extract database file path
        db_path = database_url.replace('sqlite:///', '')
        db_path = os.path.expanduser(db_path)
        
        if not os.path.exists(db_path):
            raise FileNotFoundError(f"Database file not found: {db_path}")
        
        # Create backup path
        if backup_path is None:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            backup_path = f"{db_path}.backup_{timestamp}"
        
        # Copy database file
        import shutil
        shutil.copy2(db_path, backup_path)
        
        logger.info(f"Database backed up to: {backup_path}")
        return backup_path
    
    def restore_database(self, backup_path: str) -> None:
        """Restore database from backup (SQLite only)"""
        database_url = self.db_manager._get_database_url()
        
        if not database_url.startswith('sqlite:'):
            raise ValueError("Database restore only supported for SQLite databases")
        
        if not os.path.exists(backup_path):
            raise FileNotFoundError(f"Backup file not found: {backup_path}")
        
        # Extract database file path
        db_path = database_url.replace('sqlite:///', '')
        db_path = os.path.expanduser(db_path)
        
        # Close existing connections
        self.db_manager.close()
        
        # Restore database file
        import shutil
        shutil.copy2(backup_path, db_path)
        
        # Reinitialize database manager
        self.db_manager.initialize()
        
        logger.info(f"Database restored from: {backup_path}")
    
    def clean_old_data(self, job_history_days: int = 365, 
                      snapshot_days: int = 90) -> Dict[str, int]:
        """Clean old data according to retention policies"""
        logger.info("Cleaning old data...")
        
        cleanup_results = {
            'job_history_deleted': 0,
            'queue_snapshots_deleted': 0,
            'node_snapshots_deleted': 0,
            'system_snapshots_deleted': 0
        }
        
        try:
            with self.db_manager.get_session() as session:
                # Clean old job history
                job_history_cutoff = datetime.now() - timedelta(days=job_history_days)
                job_history_deleted = session.query(JobHistory).filter(
                    JobHistory.timestamp < job_history_cutoff
                ).delete()
                cleanup_results['job_history_deleted'] = job_history_deleted
                
                # Clean old snapshots
                snapshot_cutoff = datetime.now() - timedelta(days=snapshot_days)
                
                queue_snapshots_deleted = session.query(QueueSnapshot).filter(
                    QueueSnapshot.timestamp < snapshot_cutoff
                ).delete()
                cleanup_results['queue_snapshots_deleted'] = queue_snapshots_deleted
                
                node_snapshots_deleted = session.query(NodeSnapshot).filter(
                    NodeSnapshot.timestamp < snapshot_cutoff
                ).delete()
                cleanup_results['node_snapshots_deleted'] = node_snapshots_deleted
                
                system_snapshots_deleted = session.query(SystemSnapshot).filter(
                    SystemSnapshot.timestamp < snapshot_cutoff
                ).delete()
                cleanup_results['system_snapshots_deleted'] = system_snapshots_deleted
                
                session.commit()
                
                logger.info(f"Cleanup completed: {cleanup_results}")
                
        except Exception as e:
            logger.error(f"Data cleanup failed: {str(e)}")
            raise
        
        return cleanup_results
    
    def get_database_info(self) -> Dict[str, Any]:
        """Get database information"""
        info = {
            'database_url': self.db_manager._mask_url(self.db_manager._get_database_url()),
            'schema_version': self.check_schema_version(),
            'tables': self.get_existing_tables(),
            'database_size': self.db_manager.get_database_size(),
            'validation': self.validate_schema()
        }
        
        # Add table row counts
        try:
            with self.db_manager.get_session() as session:
                info['table_counts'] = {}
                for table in self.get_required_tables():
                    if table in info['tables']:
                        count = session.execute(text(f"SELECT COUNT(*) FROM {table}")).scalar()
                        info['table_counts'][table] = count
        except Exception as e:
            logger.error(f"Failed to get table counts: {str(e)}")
            info['table_counts'] = {}
        
        return info

# Convenience functions for CLI and scripts

def initialize_database(config: Optional[Config] = None) -> None:
    """Initialize database with fresh schema"""
    migration = DatabaseMigration(config)
    migration.create_fresh_database()

def migrate_database(config: Optional[Config] = None) -> None:
    """Migrate database to latest schema version"""
    migration = DatabaseMigration(config)
    migration.migrate_to_latest()

def validate_database(config: Optional[Config] = None) -> Dict[str, Any]:
    """Validate database schema"""
    migration = DatabaseMigration(config)
    return migration.validate_schema()

def backup_database(backup_path: Optional[str] = None, config: Optional[Config] = None) -> str:
    """Backup database"""
    migration = DatabaseMigration(config)
    return migration.backup_database(backup_path)

def restore_database(backup_path: str, config: Optional[Config] = None) -> None:
    """Restore database from backup"""
    migration = DatabaseMigration(config)
    migration.restore_database(backup_path)

def clean_old_data(job_history_days: int = 365, snapshot_days: int = 90, 
                   config: Optional[Config] = None) -> Dict[str, int]:
    """Clean old data from database"""
    migration = DatabaseMigration(config)
    return migration.clean_old_data(job_history_days, snapshot_days)

def get_database_info(config: Optional[Config] = None) -> Dict[str, Any]:
    """Get database information"""
    migration = DatabaseMigration(config)
    return migration.get_database_info() 