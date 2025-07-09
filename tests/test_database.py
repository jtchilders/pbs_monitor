"""
Test database functionality for PBS Monitor
"""

import pytest
import tempfile
import os
from datetime import datetime, timedelta
from pathlib import Path

from pbs_monitor.config import Config
from pbs_monitor.database import (
    get_database_manager, create_tables, initialize_database,
    Job, JobHistory, Queue, QueueSnapshot, Node, NodeSnapshot,
    JobState, QueueState, NodeState, DataCollectionStatus,
    JobRepository, QueueRepository, NodeRepository, SystemRepository,
    get_repository_factory
)
from pbs_monitor.database.migrations import DatabaseMigration


@pytest.fixture
def temp_db_config():
    """Create a temporary database configuration for testing"""
    # Create temporary database file
    temp_db = tempfile.NamedTemporaryFile(delete=False, suffix='.db')
    temp_db.close()
    
    # Create config with temporary database
    config = Config()
    config.database.url = f"sqlite:///{temp_db.name}"
    
    yield config
    
    # Cleanup
    try:
        os.unlink(temp_db.name)
    except OSError:
        pass


@pytest.fixture
def initialized_db(temp_db_config):
    """Create and initialize a test database"""
    # Initialize database
    initialize_database(temp_db_config)
    
    yield temp_db_config
    
    # Database will be cleaned up by temp_db_config fixture


class TestDatabaseModels:
    """Test database models"""
    
    def test_job_model_creation(self, initialized_db):
        """Test Job model creation and methods"""
        repo = JobRepository(initialized_db)
        
        # Create a test job
        job_data = {
            'job_id': '12345.pbs01',
            'job_name': 'test_job',
            'owner': 'testuser',
            'state': JobState.RUNNING,
            'queue': 'default',
            'nodes': 2,
            'ppn': 4,
            'walltime': '01:00:00',
            'memory': '8gb',
            'submit_time': datetime.now() - timedelta(minutes=30),
            'start_time': datetime.now() - timedelta(minutes=20),
            'priority': 100
        }
        
        job = repo.create_or_update_job(job_data)
        
        assert job.job_id == '12345.pbs01'
        assert job.job_name == 'test_job'
        assert job.owner == 'testuser'
        assert job.state == JobState.RUNNING
        assert job.queue == 'default'
        assert job.nodes == 2
        assert job.ppn == 4
        assert job.is_active() == True
        assert job.is_completed() == False
        assert job.estimated_total_cores() == 8
        
        # Test derived fields calculation
        assert job.total_cores == 8
        assert job.queue_time_seconds == 600  # 10 minutes = 600 seconds
    
    def test_job_history_tracking(self, initialized_db):
        """Test job history tracking"""
        repo = JobRepository(initialized_db)
        
        # Create job
        job_data = {
            'job_id': '12346.pbs01',
            'job_name': 'history_test',
            'owner': 'testuser',
            'state': JobState.QUEUED,
            'queue': 'default'
        }
        
        job = repo.create_or_update_job(job_data)
        
        # Add history entries
        repo.add_job_history('12346.pbs01', JobState.QUEUED)
        repo.add_job_history('12346.pbs01', JobState.RUNNING)
        
        # Get history
        history = repo.get_job_history('12346.pbs01')
        
        assert len(history) == 2
        assert history[0].state == JobState.QUEUED
        assert history[1].state == JobState.RUNNING
    
    def test_queue_model_creation(self, initialized_db):
        """Test Queue model creation"""
        repo = QueueRepository(initialized_db)
        
        queue_data = {
            'name': 'test_queue',
            'queue_type': 'execution',
            'max_running': 100,
            'max_queued': 500,
            'priority': 50
        }
        
        queue = repo.create_or_update_queue(queue_data)
        
        assert queue.name == 'test_queue'
        assert queue.queue_type == 'execution'
        assert queue.max_running == 100
        assert queue.max_queued == 500
        assert queue.priority == 50
    
    def test_node_model_creation(self, initialized_db):
        """Test Node model creation"""
        repo = NodeRepository(initialized_db)
        
        node_data = {
            'name': 'node001',
            'ncpus': 24,
            'memory_gb': 128.0,
            'properties': ['gpu', 'large_mem']
        }
        
        node = repo.create_or_update_node(node_data)
        
        assert node.name == 'node001'
        assert node.ncpus == 24
        assert node.memory_gb == 128.0
        assert node.properties == ['gpu', 'large_mem']


class TestDatabaseRepositories:
    """Test repository functionality"""
    
    def test_job_repository_queries(self, initialized_db):
        """Test job repository query methods"""
        repo = JobRepository(initialized_db)
        
        # Create test jobs
        jobs_data = [
            {
                'job_id': '100.pbs01',
                'job_name': 'job1',
                'owner': 'user1',
                'state': JobState.RUNNING,
                'queue': 'default'
            },
            {
                'job_id': '101.pbs01',
                'job_name': 'job2',
                'owner': 'user1',
                'state': JobState.QUEUED,
                'queue': 'default'
            },
            {
                'job_id': '102.pbs01',
                'job_name': 'job3',
                'owner': 'user2',
                'state': JobState.COMPLETED,
                'queue': 'gpu'
            }
        ]
        
        for job_data in jobs_data:
            repo.create_or_update_job(job_data)
        
        # Test queries
        user1_jobs = repo.get_jobs_by_user('user1')
        assert len(user1_jobs) == 2
        
        active_jobs = repo.get_active_jobs()
        assert len(active_jobs) == 2  # RUNNING and QUEUED
        
        running_jobs = repo.get_jobs_by_state(JobState.RUNNING)
        assert len(running_jobs) == 1
        
        queue_jobs = repo.get_jobs_by_queue('default')
        assert len(queue_jobs) == 2
        
        # Test statistics
        stats = repo.get_job_statistics()
        assert stats['total_jobs'] == 3
        assert stats['active_jobs'] == 2
        assert stats['R_count'] == 1
        assert stats['Q_count'] == 1
        assert stats['C_count'] == 1
    
    def test_queue_snapshots(self, initialized_db):
        """Test queue snapshot functionality"""
        queue_repo = QueueRepository(initialized_db)
        
        # Create queue
        queue_data = {
            'name': 'snapshot_queue',
            'max_running': 50
        }
        queue_repo.create_or_update_queue(queue_data)
        
        # Add snapshots
        snapshot_data = {
            'state': QueueState.ENABLED_STARTED,
            'running_jobs': 25,
            'queued_jobs': 10,
            'utilization_percent': 50.0
        }
        
        snapshot = queue_repo.add_queue_snapshot('snapshot_queue', snapshot_data)
        
        assert snapshot.queue_name == 'snapshot_queue'
        assert snapshot.running_jobs == 25
        assert snapshot.utilization_percent == 50.0
        
        # Get snapshots
        snapshots = queue_repo.get_queue_snapshots('snapshot_queue')
        assert len(snapshots) == 1
    
    def test_node_snapshots(self, initialized_db):
        """Test node snapshot functionality"""
        node_repo = NodeRepository(initialized_db)
        
        # Create node
        node_data = {
            'name': 'snapshot_node',
            'ncpus': 16
        }
        node_repo.create_or_update_node(node_data)
        
        # Add snapshot
        snapshot_data = {
            'state': NodeState.BUSY,
            'jobs_running': 8,
            'jobs_list': ['100.pbs01', '101.pbs01'],
            'cpu_utilization_percent': 75.0
        }
        
        snapshot = node_repo.add_node_snapshot('snapshot_node', snapshot_data)
        
        assert snapshot.node_name == 'snapshot_node'
        assert snapshot.jobs_running == 8
        assert snapshot.cpu_utilization_percent == 75.0
        
        # Get snapshots
        snapshots = node_repo.get_node_snapshots('snapshot_node')
        assert len(snapshots) == 1


class TestDatabaseMigrations:
    """Test database migration functionality"""
    
    def test_database_initialization(self, temp_db_config):
        """Test database initialization"""
        migration = DatabaseMigration(temp_db_config)
        
        # Check that database doesn't exist initially
        assert migration.check_schema_version() is None
        
        # Initialize database
        migration.create_fresh_database()
        
        # Check that database now exists with correct schema
        assert migration.check_schema_version() == "1.0.0"
        
        # Check that all required tables exist
        existing_tables = migration.get_existing_tables()
        required_tables = migration.get_required_tables()
        
        for table in required_tables:
            assert table in existing_tables
    
    def test_database_validation(self, initialized_db):
        """Test database validation"""
        migration = DatabaseMigration(initialized_db)
        
        validation = migration.validate_schema()
        
        assert validation['valid'] == True
        assert len(validation['errors']) == 0
        
        # Check table status
        for table in migration.get_required_tables():
            assert validation['table_status'][table] == 'exists'
    
    def test_database_info(self, initialized_db):
        """Test database info retrieval"""
        migration = DatabaseMigration(initialized_db)
        
        info = migration.get_database_info()
        
        assert 'database_url' in info
        assert info['schema_version'] == "1.0.0"
        assert len(info['tables']) >= 8  # Should have all required tables
        assert 'validation' in info
        assert info['validation']['valid'] == True


class TestRepositoryFactory:
    """Test repository factory"""
    
    def test_repository_factory(self, initialized_db):
        """Test repository factory functionality"""
        factory = get_repository_factory(initialized_db)
        
        # Test that all repositories can be created
        job_repo = factory.get_job_repository()
        queue_repo = factory.get_queue_repository()
        node_repo = factory.get_node_repository()
        system_repo = factory.get_system_repository()
        data_repo = factory.get_data_collection_repository()
        
        assert isinstance(job_repo, JobRepository)
        assert isinstance(queue_repo, QueueRepository)
        assert isinstance(node_repo, NodeRepository)
        assert isinstance(system_repo, SystemRepository)
        
        # Test that repositories work
        job_data = {
            'job_id': 'factory_test.pbs01',
            'job_name': 'factory_job',
            'owner': 'factory_user',
            'state': JobState.RUNNING,
            'queue': 'default'
        }
        
        job = job_repo.create_or_update_job(job_data)
        assert job.job_id == 'factory_test.pbs01'
        
        retrieved_job = job_repo.get_job_by_id('factory_test.pbs01')
        assert retrieved_job is not None
        assert retrieved_job.job_id == 'factory_test.pbs01'


def test_basic_database_functionality():
    """Test basic database functionality without fixtures"""
    # Create temporary database
    temp_db = tempfile.NamedTemporaryFile(delete=False, suffix='.db')
    temp_db.close()
    
    try:
        # Create config
        config = Config()
        config.database.url = f"sqlite:///{temp_db.name}"
        
        # Initialize database
        initialize_database(config)
        
        # Test basic operations
        factory = get_repository_factory(config)
        job_repo = factory.get_job_repository()
        
        # Create a job
        job_data = {
            'job_id': 'basic_test.pbs01',
            'job_name': 'basic_job',
            'owner': 'test_user',
            'state': JobState.QUEUED,
            'queue': 'default'
        }
        
        job = job_repo.create_or_update_job(job_data)
        job_id = job.job_id  # Access attribute while session is still active
        assert job_id == 'basic_test.pbs01'
        
        # Retrieve the job
        retrieved_job = job_repo.get_job_by_id('basic_test.pbs01')
        assert retrieved_job is not None
        assert retrieved_job.job_name == 'basic_job'
        
        print("✓ Basic database functionality test passed")
        
    finally:
        # Cleanup
        try:
            os.unlink(temp_db.name)
        except OSError:
            pass


if __name__ == "__main__":
    # Run basic test if called directly
    test_basic_database_functionality() 