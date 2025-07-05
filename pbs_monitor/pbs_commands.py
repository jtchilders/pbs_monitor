"""
PBS Commands wrapper - Interface to PBS command line tools
"""

import json
import subprocess
import logging
from typing import Dict, List, Optional, Any, Union
from pathlib import Path

from .models.job import PBSJob
from .models.queue import PBSQueue
from .models.node import PBSNode


class PBSCommandError(Exception):
   """Exception raised when PBS command fails"""
   pass


class PBSCommands:
   """Wrapper for PBS command line tools"""
   
   def __init__(self, timeout: int = 30):
      """
      Initialize PBS commands wrapper
      
      Args:
         timeout: Timeout for PBS commands in seconds
      """
      self.timeout = timeout
      self.logger = logging.getLogger(__name__)
   
   def _run_command(self, command: List[str], timeout: Optional[int] = None) -> str:
      """
      Execute a command and return output
      
      Args:
         command: Command and arguments to execute
         timeout: Command timeout override
         
      Returns:
         Command output as string
         
      Raises:
         PBSCommandError: If command fails
      """
      cmd_timeout = timeout or self.timeout
      
      try:
         self.logger.debug(f"Executing command: {' '.join(command)}")
         
         result = subprocess.run(
            command,
            capture_output=True,
            text=True,
            timeout=cmd_timeout,
            check=False
         )
         
         if result.returncode != 0:
            error_msg = f"Command failed: {' '.join(command)}\n"
            error_msg += f"Exit code: {result.returncode}\n"
            error_msg += f"Stderr: {result.stderr}"
            raise PBSCommandError(error_msg)
         
         return result.stdout
         
      except subprocess.TimeoutExpired:
         raise PBSCommandError(f"Command timed out after {cmd_timeout} seconds: {' '.join(command)}")
      except FileNotFoundError:
         raise PBSCommandError(f"Command not found: {command[0]}")
      except Exception as e:
         raise PBSCommandError(f"Command execution failed: {str(e)}")
   
   def _parse_json_output(self, output: str) -> Dict[str, Any]:
      """
      Parse JSON output from PBS commands
      
      Args:
         output: Raw command output
         
      Returns:
         Parsed JSON data
         
      Raises:
         PBSCommandError: If JSON parsing fails
      """
      if not output.strip():
         return {}
      
      try:
         return json.loads(output)
      except json.JSONDecodeError as e:
         raise PBSCommandError(f"Failed to parse JSON output: {str(e)}")
   
   def qstat_jobs(self, user: Optional[str] = None, job_id: Optional[str] = None) -> List[PBSJob]:
      """
      Get job information using qstat
      
      Args:
         user: Filter by username
         job_id: Get specific job ID
         
      Returns:
         List of PBSJob objects
      """
      command = ["qstat", "-f", "-F", "json"]
      
      if job_id:
         command.append(job_id)
      elif user:
         command.extend(["-u", user])
      
      try:
         output = self._run_command(command)
         data = self._parse_json_output(output)
         
         jobs = []
         jobs_data = data.get("Jobs", {})
         
         for job_id, job_info in jobs_data.items():
            job_info["Job_Id"] = job_id  # Ensure job ID is in the data
            try:
               job = PBSJob.from_qstat_json(job_info)
               jobs.append(job)
            except Exception as e:
               self.logger.warning(f"Failed to parse job {job_id}: {str(e)}")
         
         return jobs
         
      except PBSCommandError:
         raise
      except Exception as e:
         raise PBSCommandError(f"Failed to get job information: {str(e)}")
   
   def qstat_queues(self) -> List[PBSQueue]:
      """
      Get queue information using qstat
      
      Returns:
         List of PBSQueue objects
      """
      command = ["qstat", "-Q", "-f", "-F", "json"]
      
      try:
         output = self._run_command(command)
         data = self._parse_json_output(output)
         
         queues = []
         queues_data = data.get("Queues", {})
         
         for queue_name, queue_info in queues_data.items():
            queue_info["Queue"] = queue_name  # Ensure queue name is in the data
            try:
               queue = PBSQueue.from_qstat_json(queue_info)
               queues.append(queue)
            except Exception as e:
               self.logger.warning(f"Failed to parse queue {queue_name}: {str(e)}")
         
         return queues
         
      except PBSCommandError:
         raise
      except Exception as e:
         raise PBSCommandError(f"Failed to get queue information: {str(e)}")
   
   def pbsnodes(self, node_name: Optional[str] = None) -> List[PBSNode]:
      """
      Get node information using pbsnodes
      
      Args:
         node_name: Get specific node information
         
      Returns:
         List of PBSNode objects
      """
      command = ["pbsnodes", "-a", "-F", "json"]
      
      if node_name:
         command.append(node_name)
      
      try:
         output = self._run_command(command)
         data = self._parse_json_output(output)
         
         nodes = []
         nodes_data = data.get("nodes", {})
         
         for node_name, node_info in nodes_data.items():
            node_info["name"] = node_name  # Ensure node name is in the data
            try:
               node = PBSNode.from_pbsnodes_json(node_info)
               nodes.append(node)
            except Exception as e:
               self.logger.warning(f"Failed to parse node {node_name}: {str(e)}")
         
         return nodes
         
      except PBSCommandError:
         raise
      except Exception as e:
         raise PBSCommandError(f"Failed to get node information: {str(e)}")
   
   def qsub(self, script_path: str, **kwargs) -> str:
      """
      Submit a job using qsub
      
      Args:
         script_path: Path to job script
         **kwargs: Additional qsub options
         
      Returns:
         Job ID
      """
      command = ["qsub"]
      
      # Add common options
      for key, value in kwargs.items():
         if key.startswith("_"):
            continue
         
         option = f"-{key}"
         if value is not None:
            command.extend([option, str(value)])
         else:
            command.append(option)
      
      command.append(script_path)
      
      try:
         output = self._run_command(command)
         job_id = output.strip()
         
         self.logger.info(f"Job submitted successfully: {job_id}")
         return job_id
         
      except PBSCommandError:
         raise
      except Exception as e:
         raise PBSCommandError(f"Failed to submit job: {str(e)}")
   
   def qdel(self, job_id: str) -> bool:
      """
      Delete a job using qdel
      
      Args:
         job_id: Job ID to delete
         
      Returns:
         True if successful
      """
      command = ["qdel", job_id]
      
      try:
         self._run_command(command)
         self.logger.info(f"Job deleted successfully: {job_id}")
         return True
         
      except PBSCommandError:
         raise
      except Exception as e:
         raise PBSCommandError(f"Failed to delete job {job_id}: {str(e)}")
   
   def qhold(self, job_id: str) -> bool:
      """
      Hold a job using qhold
      
      Args:
         job_id: Job ID to hold
         
      Returns:
         True if successful
      """
      command = ["qhold", job_id]
      
      try:
         self._run_command(command)
         self.logger.info(f"Job held successfully: {job_id}")
         return True
         
      except PBSCommandError:
         raise
      except Exception as e:
         raise PBSCommandError(f"Failed to hold job {job_id}: {str(e)}")
   
   def qrls(self, job_id: str) -> bool:
      """
      Release a job using qrls
      
      Args:
         job_id: Job ID to release
         
      Returns:
         True if successful
      """
      command = ["qrls", job_id]
      
      try:
         self._run_command(command)
         self.logger.info(f"Job released successfully: {job_id}")
         return True
         
      except PBSCommandError:
         raise
      except Exception as e:
         raise PBSCommandError(f"Failed to release job {job_id}: {str(e)}")
   
   def test_connection(self) -> bool:
      """
      Test if PBS commands are available
      
      Returns:
         True if PBS is available
      """
      try:
         # Try a simple qstat command
         self._run_command(["qstat", "--version"])
         return True
      except PBSCommandError:
         return False 