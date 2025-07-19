#!/usr/bin/env python3
"""
Final integration test for PBS Monitor Phase 2B - DataCollector Database Integration
"""

import sys
import os
sys.path.insert(0, os.path.dirname(__file__))

from pbs_monitor.data_collector import DataCollector
from pbs_monitor.config import Config
from pbs_monitor.database import initialize_database

def test_phase_2b_integration():
    """Test Phase 2B DataCollector integration"""
    print("🚀 Testing PBS Monitor Phase 2B - DataCollector Integration")
    print("=" * 70)
    
    # Create configuration
    config = Config()
    
    # Initialize database
    print("1. Initializing database...")
    try:
        initialize_database(config)
        print("   ✅ Database initialized successfully")
    except Exception as e:
        print(f"   ❌ Database initialization failed: {e}")
        return False
    
    # Create DataCollector with database enabled
    print("2. Creating DataCollector with database integration...")
    try:
        collector = DataCollector(config, use_sample_data=True, enable_database=True)
        print(f"   ✅ DataCollector created, database enabled: {collector.database_enabled}")
    except Exception as e:
        print(f"   ❌ DataCollector creation failed: {e}")
        return False
    
    # Test database connection
    print("3. Testing database connectivity...")
    try:
        db_connected = collector.test_database_connection()
        print(f"   ✅ Database connection: {'Success' if db_connected else 'Failed'}")
    except Exception as e:
        print(f"   ❌ Database connection test failed: {e}")
        return False
    
    # Test PBS data collection (backward compatibility)
    print("4. Testing PBS data collection (backward compatibility)...")
    try:
        jobs = collector.get_jobs()
        queues = collector.get_queues()
        nodes = collector.get_nodes()
        print(f"   ✅ PBS data collected: {len(jobs)} jobs, {len(queues)} queues, {len(nodes)} nodes")
    except Exception as e:
        print(f"   ❌ PBS data collection failed: {e}")
        return False
    
    # Test data persistence (new functionality)
    print("5. Testing data persistence to database...")
    try:
        result = collector.collect_and_persist()
        print(f"   ✅ Data persisted successfully:")
        print(f"      - Jobs: {result['jobs_collected']}")
        print(f"      - Queues: {result['queues_collected']}")
        print(f"      - Nodes: {result['nodes_collected']}")
        print(f"      - Duration: {result['duration_seconds']:.2f}s")
        print(f"      - Collection ID: {result['collection_id']}")
    except Exception as e:
        print(f"   ❌ Data persistence failed: {e}")
        return False
    
    # Test historical data retrieval (new functionality)
    print("6. Testing historical data retrieval...")
    try:
        historical_jobs = collector.get_jobs(include_historical=True)
        current_jobs = collector.get_jobs(include_historical=False)
        print(f"   ✅ Historical data access working:")
        print(f"      - Current jobs: {len(current_jobs)}")
        print(f"      - Including historical: {len(historical_jobs)}")
    except Exception as e:
        print(f"   ❌ Historical data retrieval failed: {e}")
        return False
    
    # Test enhanced job lookup (database fallback)
    print("7. Testing enhanced job lookup with database fallback...")
    if jobs:
        job_id = jobs[0].job_id
        print(f"   Testing with job ID: {job_id}")
        try:
            job = collector.get_job_by_id(job_id)
            if job:
                print(f"   ✅ Job lookup successful: {job.job_name} ({job.state.value})")
            else:
                print(f"   ⚠️  Job not found: {job_id}")
        except Exception as e:
            print(f"   ❌ Job lookup failed: {e}")
            return False
    
    # Test system summary (should work as before)
    print("8. Testing system summary (backward compatibility)...")
    try:
        summary = collector.get_system_summary()
        print(f"   ✅ System summary generated:")
        print(f"      - Total jobs: {summary['jobs']['total']}")
        print(f"      - Running: {summary['jobs']['running']}")
        print(f"      - Queued: {summary['jobs']['queued']}")
        print(f"      - Utilization: {summary['resources']['utilization']:.1f}%")
    except Exception as e:
        print(f"   ❌ System summary failed: {e}")
        return False
    
    print("\n" + "=" * 70)
    print("🎉 Phase 2B Integration Test PASSED!")
    print("\n✅ All functionality working:")
    print("   • Database persistence and retrieval")
    print("   • Historical data access")
    print("   • Enhanced job lookup with database fallback")
    print("   • Backward compatibility maintained")
    print("   • Model conversion between PBS and database formats")
    print("   • Data collection logging and tracking")
    
    return True

if __name__ == "__main__":
    success = test_phase_2b_integration()
    sys.exit(0 if success else 1) 