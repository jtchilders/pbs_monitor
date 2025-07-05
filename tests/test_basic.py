"""
Basic tests for PBS Monitor components
"""

import pytest
from unittest.mock import Mock, patch
from datetime import datetime

from pbs_monitor.models.job import PBSJob, JobState
from pbs_monitor.models.queue import PBSQueue, QueueState
from pbs_monitor.models.node import PBSNode, NodeState
from pbs_monitor.config import Config
from pbs_monitor.utils.formatters import format_duration, format_memory, format_timestamp


class TestPBSJob:
   """Test PBSJob model"""
   
   def test_job_creation(self):
      """Test job creation"""
      job = PBSJob(
         job_id="12345.pbs01",
         job_name="test_job",
         owner="testuser",
         state=JobState.RUNNING,
         queue="default"
      )
      
      assert job.job_id == "12345.pbs01"
      assert job.job_name == "test_job"
      assert job.owner == "testuser"
      assert job.state == JobState.RUNNING
      assert job.queue == "default"
   
   def test_job_from_qstat_json(self):
      """Test job creation from qstat JSON"""
      job_data = {
         "Job_Id": "12345.pbs01",
         "Job_Name": "test_job",
         "Job_Owner": "testuser@hostname",
         "job_state": "R",
         "queue": "default",
         "Resource_List": {
            "nodes": "2",
            "ppn": "4",
            "walltime": "01:00:00",
            "mem": "8gb"
         },
         "qtime": "Mon Oct 30 14:30:00 2023",
         "start_time": "Mon Oct 30 14:35:00 2023",
         "Priority": "100"
      }
      
      job = PBSJob.from_qstat_json(job_data)
      
      assert job.job_id == "12345.pbs01"
      assert job.job_name == "test_job"
      assert job.owner == "testuser"
      assert job.state == JobState.RUNNING
      assert job.queue == "default"
      assert job.nodes == 2
      assert job.ppn == 4
      assert job.walltime == "01:00:00"
      assert job.memory == "8gb"
      assert job.priority == 100
   
   def test_job_active_status(self):
      """Test job active status"""
      running_job = PBSJob("1", "test", "user", JobState.RUNNING, "default")
      queued_job = PBSJob("2", "test", "user", JobState.QUEUED, "default")
      completed_job = PBSJob("3", "test", "user", JobState.COMPLETED, "default")
      
      assert running_job.is_active() == True
      assert queued_job.is_active() == True
      assert completed_job.is_active() == False
   
   def test_job_total_cores(self):
      """Test total cores calculation"""
      job = PBSJob("1", "test", "user", JobState.RUNNING, "default", nodes=4, ppn=8)
      assert job.estimated_total_cores() == 32


class TestPBSQueue:
   """Test PBSQueue model"""
   
   def test_queue_creation(self):
      """Test queue creation"""
      queue = PBSQueue(
         name="default",
         state=QueueState.ENABLED,
         max_running=10,
         running_jobs=5,
         queued_jobs=3
      )
      
      assert queue.name == "default"
      assert queue.state == QueueState.ENABLED
      assert queue.max_running == 10
      assert queue.running_jobs == 5
      assert queue.queued_jobs == 3
   
   def test_queue_utilization(self):
      """Test queue utilization calculation"""
      queue = PBSQueue(
         name="default",
         state=QueueState.ENABLED,
         max_running=10,
         running_jobs=5
      )
      
      assert queue.utilization_percentage() == 50.0
   
   def test_queue_available_slots(self):
      """Test available slots calculation"""
      queue = PBSQueue(
         name="default",
         state=QueueState.ENABLED,
         max_running=10,
         running_jobs=3
      )
      
      assert queue.available_slots() == 7


class TestPBSNode:
   """Test PBSNode model"""
   
   def test_node_creation(self):
      """Test node creation"""
      node = PBSNode(
         name="node01",
         state=NodeState.FREE,
         ncpus=16,
         memory="32gb"
      )
      
      assert node.name == "node01"
      assert node.state == NodeState.FREE
      assert node.ncpus == 16
      assert node.memory == "32gb"
   
   def test_node_availability(self):
      """Test node availability"""
      free_node = PBSNode("node01", NodeState.FREE)
      busy_node = PBSNode("node02", NodeState.BUSY)
      down_node = PBSNode("node03", NodeState.DOWN)
      
      assert free_node.is_available() == True
      assert busy_node.is_available() == False
      assert down_node.is_available() == False
   
   def test_node_cpu_utilization(self):
      """Test CPU utilization calculation"""
      node = PBSNode(
         name="node01",
         state=NodeState.BUSY,
         ncpus=16,
         jobs=["job1", "job2", "job3", "job4"]
      )
      
      assert node.cpu_utilization() == 25.0
   
   def test_node_memory_parsing(self):
      """Test memory parsing"""
      node_gb = PBSNode("node01", NodeState.FREE, memory="32gb")
      node_mb = PBSNode("node02", NodeState.FREE, memory="32768mb")
      
      assert node_gb.memory_gb() == 32.0
      assert node_mb.memory_gb() == 32.0


class TestConfig:
   """Test configuration management"""
   
   def test_config_creation(self):
      """Test config creation"""
      config = Config()
      
      assert config.pbs.command_timeout == 30
      assert config.display.use_colors == True
      assert config.logging.level == "INFO"
   
   def test_config_log_level(self):
      """Test log level conversion"""
      config = Config()
      config.logging.level = "DEBUG"
      
      import logging
      assert config.get_log_level() == logging.DEBUG


class TestFormatters:
   """Test formatting utilities"""
   
   def test_format_duration(self):
      """Test duration formatting"""
      assert format_duration(3600) == "1h"
      assert format_duration(3660) == "1h 1m"
      assert format_duration(90) == "1m 30s"
      assert format_duration(30) == "30s"
      assert format_duration(None) == "N/A"
   
   def test_format_memory(self):
      """Test memory formatting"""
      assert format_memory("1024mb") == "1.0GB"
      assert format_memory("32gb") == "32.0GB"
      assert format_memory("512kb") == "512KB"
      assert format_memory(None) == "N/A"
   
   def test_format_timestamp(self):
      """Test timestamp formatting"""
      dt = datetime(2023, 10, 30, 14, 30, 0)
      assert format_timestamp(dt) == "30-10 14:30"
      assert format_timestamp(None) == "N/A"


class TestCLI:
   """Test CLI components"""
   
   def test_cli_import(self):
      """Test CLI module imports"""
      from pbs_monitor.cli.main import main
      from pbs_monitor.cli.commands import StatusCommand
      
      assert callable(main)
      assert StatusCommand is not None
   
   @patch('pbs_monitor.cli.main.DataCollector')
   def test_cli_help(self, mock_collector):
      """Test CLI help output"""
      from pbs_monitor.cli.main import main
      
      # Mock collector to avoid PBS connection
      mock_collector.return_value.test_connection.return_value = True
      
      # Test help output
      result = main(['--help'])
      # Should exit with code 0 for help
      assert result == 0 or result is None  # argparse may not return exit code


if __name__ == '__main__':
   pytest.main([__file__]) 