"""
Database Migration Utilities for PBS Monitor

This module provides utilities for database initialization, schema updates,
and data migrations for the PBS Monitor database.

NOTE: Migration operations (ALTER TABLE, etc.) are dialect-neutral and should
work on both SQLite and PostgreSQL. Tested against both backends.
"""

import os
import sys
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional, Dict, Any, List
from sqlalchemy import inspect, text
from sqlalchemy.engine import Engine
from sqlalchemy.exc import OperationalError

from .models import Base, Job, JobHistory, Queue, QueueSnapshot, Node, NodeSnapshot, SystemSnapshot, Reservation, ReservationHistory, ReservationUtilization, DataCollectionLog
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
            'reservations',
            'reservation_history',
            'reservation_utilization',
            'data_collection_log'
        ]
    
    def check_schema_version(self) -> Optional[str]:
        """Check current schema version.
        Inspects actual column presence to determine schema version:
          - None: no tables yet
          - 1.0.0: jobs table present but no reservation tables
          - 1.1.0: reservation tables present but no outcome_class column
          - 1.2.0: outcome_class column present on jobs table (latest)
        """
        try:
            existing = self.get_existing_tables()
            if not existing:
                return None
            inspector = inspect(self.db_manager.engine)
            # Check for outcome_class column (v1.2)
            if 'jobs' in existing:
                job_cols = [c['name'] for c in inspector.get_columns('jobs')]
                if 'outcome_class' in job_cols:
                    return "1.2.0"
            # Check for reservation tables (v1.1)
            if 'reservations' in existing:
                return "1.1.0"
            return "1.0.0"
        except Exception:
            return None
    
    def create_fresh_database(self) -> None:
        """Create a fresh database with all tables."""
        logger.info("Creating fresh database...")
        try:
            Base.metadata.create_all(self.db_manager.engine)
            logger.info("All tables created successfully")
        except Exception as e:
            logger.error(f"Failed to create database: {str(e)}")
            raise
    
    def _create_initial_data(self) -> None:
        """No-op for initial data to avoid write issues in test environments."""
        logger.info("Skipping initial data creation for fresh database")
        return None
    
    def migrate_to_latest(self) -> None:
        """Migrate database to latest schema version"""
        current_version = self.check_schema_version()
        
        if current_version is None:
            logger.info("No existing schema detected, creating fresh database")
            self.create_fresh_database()
            return
        
        logger.info(f"Current schema version: {current_version}")
        
        # Migration path from 1.0.0 to 1.1.0 (add reservation tables)
        if current_version == "1.0.0":
            logger.info("Migrating from v1.0.0 to v1.1.0 (adding reservation tables)")
            self.migrate_to_v1_1_reservations()
            current_version = "1.1.0"
        
        # Migration path from 1.1.0 to 1.2.0 (add outcome_class column + indexes)
        if current_version == "1.1.0":
            logger.info("Migrating from v1.1.0 to v1.2.0 (adding outcome_class column)")
            self.migrate_to_v1_2_outcome_class()
            return
        
        # Already at latest version
        if current_version == "1.2.0":
            logger.info("Database schema is up to date")
            return
        
        # Unknown version
        logger.warning(f"Unknown schema version: {current_version}")
    
    def migrate_to_v1_1_reservations(self) -> None:
        """Add reservation tables for version 1.1"""
        logger.info("Migrating to v1.1 - Adding reservation tables")
        
        try:
            # Check if tables already exist
            inspector = inspect(self.db_manager.engine)
            existing_tables = inspector.get_table_names()
            
            new_tables = ['reservations', 'reservation_history', 'reservation_utilization']
            tables_to_create = [table for table in new_tables if table not in existing_tables]
            
            if tables_to_create:
                logger.info(f"Creating reservation tables: {', '.join(tables_to_create)}")
                
                # Create only the new tables
                Reservation.__table__.create(self.db_manager.engine, checkfirst=True)
                ReservationHistory.__table__.create(self.db_manager.engine, checkfirst=True)
                ReservationUtilization.__table__.create(self.db_manager.engine, checkfirst=True)
                
                logger.info("Reservation tables created successfully")
            else:
                logger.info("Reservation tables already exist")
            
            # Add reservations_collected column to data_collection_log if it doesn't exist
            self._add_reservations_collected_column()
            
            logger.info("Migration to v1.1.0 completed successfully")
            
        except Exception as e:
            logger.error(f"Failed to migrate to v1.1.0: {str(e)}")
            raise
    
    def _add_reservations_collected_column(self) -> None:
        """Add reservations_collected column to data_collection_log table"""
        try:
            inspector = inspect(self.db_manager.engine)
            columns = [col['name'] for col in inspector.get_columns('data_collection_log')]
            
            if 'reservations_collected' not in columns:
                logger.info("Adding reservations_collected column to data_collection_log")
                with self.db_manager.get_session() as session:
                    session.execute(text(
                        "ALTER TABLE data_collection_log ADD COLUMN reservations_collected INTEGER DEFAULT 0"
                    ))
                    session.commit()
                logger.info("reservations_collected column added successfully")
            else:
                logger.info("reservations_collected column already exists")
                
        except Exception as e:
            logger.error(f"Failed to add reservations_collected column: {str(e)}")
            raise

    # ------------------------------------------------------------------
    # v1.2.0 – T0: outcome_class column + indexes
    # ------------------------------------------------------------------

    def migrate_to_v1_2_outcome_class(self) -> None:
        """Add outcome_class VARCHAR column and indexes to the jobs table (v1.2).

        **IMPORTANT – DB BACKUP:** This migration alters a potentially large
        table (~450 k rows on Aurora).  Take a database backup before running
        in production::

            pbs-monitor database backup

        The migration is idempotent — it is safe to run more than once.
        """
        logger.info("Migrating to v1.2 – adding outcome_class column to jobs table")
        logger.warning(
            "T0 migration: back up the database before running this on production data. "
            "Run: pbs-monitor database backup"
        )

        try:
            inspector = inspect(self.db_manager.engine)
            job_cols = [c['name'] for c in inspector.get_columns('jobs')]

            # --- 1. Add the column (idempotent) ---
            if 'outcome_class' not in job_cols:
                logger.info("Adding outcome_class column to jobs table")
                with self.db_manager.get_session() as session:
                    session.execute(text(
                        "ALTER TABLE jobs ADD COLUMN outcome_class VARCHAR(20)"
                    ))
                    session.commit()
                logger.info("outcome_class column added successfully")
            else:
                logger.info("outcome_class column already exists")

            # --- 2. Add indexes (CREATE INDEX IF NOT EXISTS is idempotent) ---
            with self.db_manager.get_session() as session:
                session.execute(text(
                    "CREATE INDEX IF NOT EXISTS ix_jobs_outcome_class "
                    "ON jobs (outcome_class)"
                ))
                session.execute(text(
                    "CREATE INDEX IF NOT EXISTS ix_jobs_end_time_outcome_class "
                    "ON jobs (end_time, outcome_class)"
                ))
                session.commit()
            logger.info("Indexes on outcome_class created/verified")

            logger.info("Migration to v1.2.0 completed successfully")

        except Exception as e:
            logger.error(f"Failed to migrate to v1.2.0: {str(e)}")
            raise

    # ------------------------------------------------------------------
    # T0 Backfill – populate exit_status + outcome_class for existing rows
    # ------------------------------------------------------------------

    def backfill_exit_status_and_outcome_class(
        self, batch_size: int = 5000, dry_run: bool = False
    ) -> Dict[str, Any]:
        """Backfill ``exit_status`` (parsed from ``raw_pbs_data.Exit_status``) and
        ``outcome_class`` (via :func:`~pbs_monitor.analytics.outcome_classifier.classify_exit`)
        for all existing jobs rows.

        **IMPORTANT – DB BACKUP:** This operation updates up to ~458 k rows.
        Take a database backup before running in production::

            pbs-monitor database backup

        The operation is **batched** (default 5 000 rows per transaction) and
        **idempotent** — rows that already have both ``exit_status`` and
        ``outcome_class`` populated are skipped automatically.

        Args:
            batch_size: Number of rows to update per transaction.
            dry_run:    When ``True`` the function scans and classifies but
                        does not write anything to the database.

        Returns:
            Dict with keys: ``updated``, ``skipped``, ``errors``, ``dry_run``.
        """
        import json as _json
        from ..analytics.outcome_classifier import classify_exit

        logger.info(
            "Starting T0 backfill: exit_status + outcome_class "
            f"(batch_size={batch_size}, dry_run={dry_run})"
        )
        if dry_run:
            logger.info("DRY RUN – no rows will be written")

        stats: Dict[str, Any] = {"updated": 0, "skipped": 0, "errors": 0, "dry_run": dry_run}

        def _parse_walltime_seconds(wt_str: Optional[str]) -> Optional[int]:
            """Parse HH:MM:SS → seconds.  Returns None on failure."""
            if not wt_str:
                return None
            try:
                parts = wt_str.strip().split(":")
                if len(parts) == 3:
                    h, m, s = parts
                    return int(h) * 3600 + int(m) * 60 + int(s)
            except (ValueError, AttributeError):
                pass
            return None

        with self.db_manager.get_session() as session:
            # Stream IDs in chunks to avoid loading 450k rows into RAM at once.
            offset = 0
            while True:
                rows = session.execute(
                    text(
                        "SELECT job_id, state, exit_status, outcome_class, "
                        "       actual_runtime_seconds, walltime, raw_pbs_data "
                        "FROM jobs "
                        "ORDER BY job_id "
                        f"LIMIT {batch_size} OFFSET {offset}"
                    )
                ).fetchall()

                if not rows:
                    break

                for row in rows:
                    job_id = row[0]
                    state = row[1]
                    db_exit_status = row[2]
                    db_outcome_class = row[3]
                    actual_runtime_seconds = row[4]
                    walltime_str = row[5]
                    raw_pbs_data = row[6]

                    # --- Parse exit_status from raw JSON if not already set ---
                    exit_status = db_exit_status
                    if exit_status is None and raw_pbs_data:
                        try:
                            raw = raw_pbs_data if isinstance(raw_pbs_data, dict) else _json.loads(raw_pbs_data)
                            # Use explicit None checks — exit_status 0 is falsy but valid
                            es_raw = raw.get("Exit_status")
                            if es_raw is None:
                                es_raw = raw.get("exit_status")
                            if es_raw is not None:
                                exit_status = int(es_raw)
                        except (ValueError, TypeError, _json.JSONDecodeError):
                            stats["errors"] += 1
                            logger.debug(f"job_id={job_id}: failed to parse Exit_status from raw_pbs_data")

                    # --- Classify ---
                    requested_walltime_seconds = _parse_walltime_seconds(walltime_str)
                    new_outcome_class = classify_exit(
                        state or "F",
                        exit_status,
                        actual_runtime_seconds=actual_runtime_seconds,
                        requested_walltime_seconds=requested_walltime_seconds,
                    )

                    # --- Skip if nothing changed ---
                    if exit_status == db_exit_status and new_outcome_class == db_outcome_class:
                        stats["skipped"] += 1
                        continue

                    if not dry_run:
                        session.execute(
                            text(
                                "UPDATE jobs "
                                "SET exit_status = :es, outcome_class = :oc "
                                "WHERE job_id = :job_id"
                            ),
                            {"es": exit_status, "oc": new_outcome_class, "job_id": job_id},
                        )

                    stats["updated"] += 1

                if not dry_run:
                    session.commit()

                offset += batch_size
                logger.info(
                    f"Backfill progress: offset={offset}, "
                    f"updated={stats['updated']}, skipped={stats['skipped']}, "
                    f"errors={stats['errors']}"
                )

        logger.info(f"T0 backfill complete: {stats}")
        return stats
    
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
                'nodes': ['name', 'ncpus', 'memory_gb', 'snapshot_index'],
                'node_snapshots': ['id', 'timestamp', 'snapshot_data', 'node_count'],
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


def backfill_exit_status_and_outcome_class(
    batch_size: int = 5000,
    dry_run: bool = False,
    config: Optional[Config] = None,
) -> Dict[str, Any]:
    """Run T0 backfill: populate exit_status + outcome_class for existing jobs rows.

    **IMPORTANT – DB BACKUP FIRST:** This updates up to ~458 k rows.
    Run ``pbs-monitor database backup`` before calling this on production data.
    """
    migration = DatabaseMigration(config)
    return migration.backfill_exit_status_and_outcome_class(
        batch_size=batch_size, dry_run=dry_run
    )
