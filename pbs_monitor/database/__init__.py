"""
Database module for PBS Monitor

Provides database connectivity, models, and data access layer.
"""

from .models import (
    Job, Queue, Node, JobHistory, QueueSnapshot, NodeSnapshot, 
    SystemSnapshot, DataCollectionLog, JobState, QueueState, NodeState, DataCollectionStatus
)
from .connection import DatabaseManager, get_database_manager, create_tables, drop_tables
from .repositories import (
    JobRepository, QueueRepository, NodeRepository, 
    SystemRepository, DataCollectionRepository, RepositoryFactory
)
from .migrations import (
    initialize_database, validate_database, migrate_database,
    backup_database, restore_database, clean_old_data, get_database_info
)
from .model_converters import (
    JobConverter, QueueConverter, NodeConverter, SystemConverter, ModelConverters
)

__all__ = [
    # Models
    'Job', 'Queue', 'Node', 'JobHistory', 'QueueSnapshot', 'NodeSnapshot',
    'SystemSnapshot', 'DataCollectionLog', 'JobState', 'QueueState', 'NodeState', 'DataCollectionStatus',
    
    # Connection
    'DatabaseManager', 'get_database_manager', 'create_tables', 'drop_tables',
    
    # Repositories
    'JobRepository', 'QueueRepository', 'NodeRepository', 
    'SystemRepository', 'DataCollectionRepository', 'RepositoryFactory',
    
    # Migrations
    'initialize_database', 'validate_database', 'migrate_database',
    'backup_database', 'restore_database', 'clean_old_data', 'get_database_info',
    
    # Model Converters
    'JobConverter', 'QueueConverter', 'NodeConverter', 'SystemConverter', 'ModelConverters',
    'get_repository_factory'
] 


def get_repository_factory(config=None):
    """Convenience function expected by tests/CLI to get a repository factory."""
    return RepositoryFactory(config)