"""
PBS Commands wrapper - Interface to PBS command line tools
"""

import json
import subprocess
import logging
import os
import re
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
   
   def __init__(self, timeout: int = 30, use_sample_data: bool = False):
      """
      Initialize PBS commands wrapper
      
      Args:
         timeout: Timeout for PBS commands in seconds
         use_sample_data: Use sample JSON data instead of actual PBS commands
      """
      self.timeout = timeout
      self.use_sample_data = use_sample_data
      self.sample_data_dir = Path(__file__).parent / "sample_json"
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
         
         # Log both stdout and stderr for debugging
         if result.stdout:
            self.logger.debug(f"Command stdout: {result.stdout[:500]}...")
         if result.stderr:
            self.logger.debug(f"Command stderr: {result.stderr[:500]}...")
         
         if result.returncode != 0:
            error_msg = f"Command failed: {' '.join(command)}\n"
            error_msg += f"Exit code: {result.returncode}\n"
            error_msg += f"Stdout: {result.stdout}\n"
            error_msg += f"Stderr: {result.stderr}"
            
            # Log the full output for debugging
            self.logger.error(f"PBS command failed with full output:\n{error_msg}")
            
            raise PBSCommandError(error_msg)
         
         return result.stdout
         
      except subprocess.TimeoutExpired:
         raise PBSCommandError(f"Command timed out after {cmd_timeout} seconds: {' '.join(command)}")
      except FileNotFoundError:
         raise PBSCommandError(f"Command not found: {command[0]}")
      except Exception as e:
         raise PBSCommandError(f"Command execution failed: {str(e)}")
   
   def _preprocess_json(self, output: str) -> str:
      """
      Preprocess JSON output to fix common PBS command formatting issues
      
      Args:
         output: Raw JSON output from PBS commands
         
      Returns:
         Cleaned JSON output
      """
      # Fix unquoted large numeric values that start with 0
      # Pattern: "field_name":0000000000000000000000000000000000000000,
      pattern = r'"([^"]+)":([0-9]{30,}),'
      
      def fix_numeric_value(match):
         field_name = match.group(1)
         numeric_value = match.group(2)
         # Quote the numeric value to make it a string
         return f'"{field_name}":"{numeric_value}",'
      
      cleaned_output = re.sub(pattern, fix_numeric_value, output)
      
      # Log if any fixes were applied
      if cleaned_output != output:
         fixes_count = len(re.findall(pattern, output))
         self.logger.debug(f"Applied {fixes_count} JSON preprocessing fixes for malformed numeric values")
      
      return cleaned_output
   
   def _parse_json_output(self, output: str, command_description: str = "") -> Dict[str, Any]:
      """
      Parse JSON output from PBS commands
      
      Args:
         output: Raw command output
         command_description: Description of the command for error logging
         
      Returns:
         Parsed JSON data
         
      Raises:
         PBSCommandError: If JSON parsing fails
      """
      if not output.strip():
         return {}
      
      try:
         # Preprocess the JSON to fix common formatting issues
         cleaned_output = self._preprocess_json(output)
         return json.loads(cleaned_output)
      except json.JSONDecodeError as e:
         # Log the raw output for debugging
         self.logger.error(f"JSON parsing failed for {command_description}")
         self.logger.error(f"JSON decode error: {str(e)}")
         self.logger.error(f"Raw output length: {len(output)} characters")
         
         # Log a portion of the raw output around the error position
         if hasattr(e, 'pos') and e.pos:
            start = max(0, e.pos - 200)
            end = min(len(output), e.pos + 200)
            self.logger.error(f"Raw output around error position {e.pos}:")
            self.logger.error(f"'{output[start:end]}'")
         else:
            # Log beginning and end of output
            self.logger.error(f"Raw output (first 1000 chars): {output[:1000]}")
            self.logger.error(f"Raw output (last 1000 chars): {output[-1000:]}")
         
         raise PBSCommandError(f"Failed to parse JSON output for {command_description}: {str(e)}")
   
   def _load_sample_data(self, filename: str) -> Dict[str, Any]:
      """
      Load sample JSON data from file
      
      Args:
         filename: Name of the sample JSON file
         
      Returns:
         Parsed JSON data
      """
      file_path = self.sample_data_dir / filename
      if not file_path.exists():
         raise PBSCommandError(f"Sample data file not found: {file_path}")
      
      try:
         with open(file_path, 'r') as f:
            raw_content = f.read()
         
         # Use the same preprocessing and parsing logic as for command output
         return self._parse_json_output(raw_content, f"sample data {filename}")
      except Exception as e:
         raise PBSCommandError(f"Failed to load sample data from {filename}: {str(e)}")
   
   def qstat_jobs(self, user: Optional[str] = None, job_id: Optional[str] = None) -> List[PBSJob]:
      """
      Get job information using qstat
      
      Args:
         user: Filter by username
         job_id: Get specific job ID
         
      Returns:
         List of PBSJob objects
      """
      if self.use_sample_data:
         try:
            data = self._load_sample_data("qstat_f_F_json-output.json")
         except PBSCommandError:
            self.logger.warning("Failed to load sample job data, returning empty list")
            return []
      else:
         command = ["qstat", "-f", "-F", "json"]
         
         if job_id:
            command.append(job_id)
         elif user:
            command.extend(["-u", user])
         
         try:
            output = self._run_command(command)
            data = self._parse_json_output(output, "qstat jobs")
            
         except PBSCommandError:
            raise
         except Exception as e:
            raise PBSCommandError(f"Failed to get job information: {str(e)}")
      
      jobs = []
      jobs_data = data.get("Jobs", {})
      
      for job_id, job_info in jobs_data.items():
         job_info["Job_Id"] = job_id  # Ensure job ID is in the data
         try:
            job = PBSJob.from_qstat_json(job_info)
            # Apply user filter if specified and using sample data
            if user and self.use_sample_data and job.owner != user:
               continue
            jobs.append(job)
         except Exception as e:
            self.logger.warning(f"Failed to parse job {job_id}: {str(e)}")
      
      return jobs
   
   def qstat_queues(self) -> List[PBSQueue]:
      """
      Get queue information using qstat
      
      Returns:
         List of PBSQueue objects
      """
      if self.use_sample_data:
         try:
            data = self._load_sample_data("qstat_Q_f_F_json-output.json")
         except PBSCommandError:
            self.logger.warning("Failed to load sample queue data, returning empty list")
            return []
      else:
         command = ["qstat", "-Q", "-f", "-F", "json"]
         
         try:
            output = self._run_command(command)
            data = self._parse_json_output(output, "qstat queues")
            
         except PBSCommandError:
            raise
         except Exception as e:
            raise PBSCommandError(f"Failed to get queue information: {str(e)}")
      
      queues = []
      queues_data = data.get("Queue", {})  # Note: "Queue" not "Queues"
      
      for queue_name, queue_info in queues_data.items():
         queue_info["Queue"] = queue_name  # Ensure queue name is in the data
         try:
            queue = PBSQueue.from_qstat_json(queue_info)
            queues.append(queue)
         except Exception as e:
            self.logger.warning(f"Failed to parse queue {queue_name}: {str(e)}")
      
      return queues
   
   def pbsnodes(self, node_name: Optional[str] = None) -> List[PBSNode]:
      """
      Get node information using pbsnodes
      
      Args:
         node_name: Get specific node information
         
      Returns:
         List of PBSNode objects
      """
      if self.use_sample_data:
         try:
            data = self._load_sample_data("pbsnodes_a_f_json-output.json")
         except PBSCommandError:
            self.logger.warning("Failed to load sample node data, returning empty list")
            return []
      else:
         command = ["pbsnodes", "-a", "-F", "json"]
         
         if node_name:
            command.append(node_name)
         
         try:
            output = self._run_command(command)
            data = self._parse_json_output(output, "pbsnodes")
            
         except PBSCommandError:
            raise
         except Exception as e:
            raise PBSCommandError(f"Failed to get node information: {str(e)}")
      
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