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
from .models.reservation import PBSReservation, ReservationState


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
         
         # First try with UTF-8 encoding
         try:
            result = subprocess.run(
               command,
               capture_output=True,
               text=True,
               encoding='utf-8',
               errors='strict',
               timeout=cmd_timeout,
               check=False
            )
         except UnicodeDecodeError as e:
            # If UTF-8 fails, try with more permissive encoding
            self.logger.warning(f"UTF-8 decoding failed for command {' '.join(command)}: {str(e)}")
            self.logger.info("Retrying with permissive encoding (latin-1)...")
            
            result = subprocess.run(
               command,
               capture_output=True,
               text=True,
               encoding='latin-1',  # More permissive encoding
               errors='replace',    # Replace invalid characters with replacement character
               timeout=cmd_timeout,
               check=False
            )
            
            self.logger.info("Command completed with permissive encoding (some characters may be replaced)")
         
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
      # Remove or replace invalid control characters that break JSON parsing
      # This includes characters like \x00-\x1f except for \t, \n, \r which are valid in JSON
      import string
      
      # Define valid control characters for JSON
      valid_controls = {'\t', '\n', '\r'}
      
      # Replace invalid control characters with spaces
      cleaned_output = ""
      for char in output:
         if char in string.whitespace or ord(char) >= 32 or char in valid_controls:
            cleaned_output += char
         else:
            # Replace invalid control characters with space
            cleaned_output += " "
      
      # Fix unquoted large numeric values that start with 0
      # Pattern: "field_name":0000000000000000000000000000000000000000,
      pattern = r'"([^"]+)":([0-9]{30,}),'
      
      def fix_numeric_value(match):
         field_name = match.group(1)
         numeric_value = match.group(2)
         # Quote the numeric value to make it a string
         return f'"{field_name}":"{numeric_value}",'
      
      cleaned_output = re.sub(pattern, fix_numeric_value, cleaned_output)
      
      # Log if any fixes were applied
      if cleaned_output != output:
         self.logger.debug(f"Applied JSON preprocessing fixes for control characters and malformed numeric values")
      
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
   
   def qstat_jobs(self, user: Optional[str] = None, job_id: Optional[str] = None, 
                  server_defaults: Optional[Dict[str, Any]] = None, 
                  server_data: Optional[Dict[str, Any]] = None) -> List[PBSJob]:
      """
      Get job information using qstat
      
      Args:
         user: Filter by username
         job_id: Get specific job ID
         server_defaults: Pre-fetched server defaults (optional, will fetch if not provided)
         
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
         command = ["/opt/pbs/bin/qstat", "-f", "-F", "json"]
         
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
      
      # Get server data for score calculation
      server_data_for_scoring = server_data
      if server_defaults is None:
         try:
            if server_data_for_scoring is None:
               server_data_for_scoring = self.qstat_server()
            server_info = server_data_for_scoring.get("Server", {})
            for server_name, server_details in server_info.items():
               server_defaults = server_details.get("resources_default", {})
               break
         except Exception as e:
            self.logger.warning(f"Failed to get server defaults for score calculation: {str(e)}")
      elif server_data_for_scoring is None:
         # If server_defaults is provided but no server_data, get it for formula calculation
         try:
            server_data_for_scoring = self.qstat_server()
         except Exception as e:
            self.logger.warning(f"Failed to get server data for score calculation: {str(e)}")
      
      jobs = []
      jobs_data = data.get("Jobs", {})
      
      for job_id, job_info in jobs_data.items():
         job_info["Job_Id"] = job_id  # Ensure job ID is in the data
         try:
            # Calculate job score
            score = None
            if server_defaults:
               score = self.calculate_job_score(job_info, server_defaults, server_data_for_scoring)
            
            job = PBSJob.from_qstat_json(job_info, score=score)
            # Apply user filter if specified and using sample data
            if user and self.use_sample_data and job.owner != user:
               continue
            jobs.append(job)
         except Exception as e:
            self.logger.warning(f"Failed to parse job {job_id}: {str(e)}")
      
      return jobs
   
   def qstat_completed_jobs(self, user: Optional[str] = None, days: int = 7) -> List[PBSJob]:
      """
      Get completed job information using qstat -x
      
      Args:
         user: Filter by username
         days: Number of days back to look for completed jobs
         
      Returns:
         List of PBSJob objects representing completed jobs
      """
      if self.use_sample_data:
         try:
            data = self._load_sample_data("qstat_x_f_F_json-output.json")
         except PBSCommandError:
            self.logger.warning("Failed to load sample completed job data, returning empty list")
            return []
      else:
         # Note: We don't use -u option because it causes PBS to return tabular format instead of JSON
         # User filtering is done in Python after parsing the JSON
         command = ["/opt/pbs/bin/qstat", "-x", "-f", "-F", "json"]
         
         try:
            output = self._run_command(command)
            data = self._parse_json_output(output, "qstat completed jobs")
            
         except PBSCommandError:
            raise
         except Exception as e:
            raise PBSCommandError(f"Failed to get completed job information: {str(e)}")
      
      jobs = []
      jobs_data = data.get("Jobs", {})
      
      for job_id, job_info in jobs_data.items():
         job_info["Job_Id"] = job_id  # Ensure job ID is in the data
         try:
            # For completed jobs, we don't calculate scores since they're no longer in queue
            job = PBSJob.from_qstat_json(job_info, score=None)
            
            # Apply user filter if specified (works for both real PBS and sample data)
            if user and job.owner != user:
               continue
               
            # Only include completed jobs (should be all of them from qstat -x, but double-check)
            if job.state.value in ['C', 'F', 'E']:  # Completed, Finished, or Exiting
               jobs.append(job)
         except Exception as e:
            self.logger.warning(f"Failed to parse completed job {job_id}: {str(e)}")
      
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
         command = ["/opt/pbs/bin/qstat", "-Q", "-f", "-F", "json"]
         
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
   
   def qstat_server(self) -> Dict[str, Any]:
      """
      Get server information using qstat -B -f -F json
      
      Returns:
         Dictionary containing server information including job_sort_formula
      """
      self.logger.debug("Retrieving server data")
      if self.use_sample_data:
         try:
            data = self._load_sample_data("qstat_B_f_F_json-output.json")
         except PBSCommandError:
            self.logger.warning("Failed to load sample server data, returning empty dict")
            return {}
      else:
         command = ["/opt/pbs/bin/qstat", "-B", "-f", "-F", "json"]
         
         try:
            output = self._run_command(command)
            data = self._parse_json_output(output, "qstat server")
            
         except PBSCommandError:
            raise
         except Exception as e:
            raise PBSCommandError(f"Failed to get server information: {str(e)}")
      
      return data
   
   def get_job_sort_formula(self, server_data: Optional[Dict[str, Any]] = None) -> Optional[str]:
      """
      Get the job sort formula from the PBS server
      
      Args:
         server_data: Pre-fetched server data (optional, will fetch if not provided)
      
      Returns:
         Job sort formula string or None if not available
      """
      try:
         if server_data is None:
            server_data = self.qstat_server()
         
         # Navigate to the server information in the JSON structure
         server_info = server_data.get("Server", {})
         
         # Get the first server entry (there should be only one)
         for server_name, server_details in server_info.items():
            formula = server_details.get("job_sort_formula")
            if formula:
               return formula
         
         return None
         
      except Exception as e:
         self.logger.error(f"Failed to get job sort formula: {str(e)}")
         return None
   
   def calculate_job_score(self, job_data: Dict[str, Any], server_defaults: Optional[Dict[str, Any]] = None, 
                          server_data: Optional[Dict[str, Any]] = None) -> Optional[float]:
      """
      Calculate job score using the server's job sort formula
      
      Args:
         job_data: Job data dictionary from qstat JSON
         server_defaults: Server resource defaults (optional, will fetch if not provided)
         server_data: Pre-fetched server data (optional, will fetch if not provided)
         
      Returns:
         Calculated job score or None if calculation fails
      """
      try:
         # Get server data if not provided (for both formula and defaults)
         if server_data is None:
            server_data = self.qstat_server()
         
         # Get the job sort formula
         formula = self.get_job_sort_formula(server_data=server_data)
         if not formula:
            self.logger.warning("No job sort formula available")
            return None
         
         # Get server defaults if not provided
         if server_defaults is None:
            server_info = server_data.get("Server", {})
            for server_name, server_details in server_info.items():
               server_defaults = server_details.get("resources_default", {})
               break
         
         if not server_defaults:
            self.logger.warning("No server defaults available")
            return None
         
         # Extract parameters from job data
         resource_list = job_data.get("Resource_List", {})
         
         # Build the variables dictionary for the formula
         variables = {
            # From job data
            "base_score": int(resource_list.get("base_score", server_defaults.get("base_score", 0))),
            "score_boost": int(resource_list.get("score_boost", server_defaults.get("score_boost", 0))),
            "enable_wfp": int(resource_list.get("enable_wfp", server_defaults.get("enable_wfp", 0))),
            "wfp_factor": int(resource_list.get("wfp_factor", server_defaults.get("wfp_factor", 100000))),
            "enable_backfill": int(resource_list.get("enable_backfill", server_defaults.get("enable_backfill", 0))),
            "backfill_max": int(resource_list.get("backfill_max", server_defaults.get("backfill_max", 50))),
            "backfill_factor": int(resource_list.get("backfill_factor", server_defaults.get("backfill_factor", 84600))),
            "enable_fifo": int(resource_list.get("enable_fifo", server_defaults.get("enable_fifo", 1))),
            "fifo_factor": int(resource_list.get("fifo_factor", server_defaults.get("fifo_factor", 1800))),
            "project_priority": int(resource_list.get("project_priority", 1)),
            "nodect": int(resource_list.get("nodect", 1)),
            "total_cpus": int(resource_list.get("total_cpus", server_defaults.get("total_cpus", 1))),
            "walltime": self._parse_walltime_to_seconds(resource_list.get("walltime", "01:00:00")),
         }
         
         # Calculate eligible_time (time since job was queued)
         eligible_time_str = job_data.get("eligible_time", "00:00:00")
         variables["eligible_time"] = self._parse_eligible_time_to_seconds(eligible_time_str)
         
         # Add math functions for the formula
         variables["min"] = min
         variables["max"] = max
         
         # Evaluate the formula
         try:
            score = eval(formula, {"__builtins__": {}}, variables)
            return float(score)
         except Exception as e:
            self.logger.error(f"Error evaluating job sort formula: {str(e)}")
            self.logger.error(f"Formula: {formula}")
            self.logger.error(f"Variables: {variables}")
            return None
         
      except Exception as e:
         self.logger.error(f"Failed to calculate job score: {str(e)}")
         return None
   
   def _parse_walltime_to_seconds(self, walltime_str: str) -> float:
      """
      Parse walltime string to seconds
      
      Args:
         walltime_str: Walltime in format HH:MM:SS or DD:HH:MM:SS
         
      Returns:
         Walltime in seconds
      """
      try:
         parts = walltime_str.split(':')
         if len(parts) == 3:
            # HH:MM:SS format
            hours, minutes, seconds = map(int, parts)
            return hours * 3600 + minutes * 60 + seconds
         elif len(parts) == 4:
            # DD:HH:MM:SS format
            days, hours, minutes, seconds = map(int, parts)
            return days * 86400 + hours * 3600 + minutes * 60 + seconds
         else:
            return 3600  # Default to 1 hour
      except (ValueError, TypeError):
         return 3600  # Default to 1 hour
   
   def _parse_eligible_time_to_seconds(self, eligible_time_str: str) -> float:
      """
      Parse eligible time string to seconds
      
      Args:
         eligible_time_str: Eligible time in format HH:MM:SS or DDDD:HH:MM
         
      Returns:
         Eligible time in seconds
      """
      try:
         parts = eligible_time_str.split(':')
         if len(parts) == 3:
            # HH:MM:SS format
            hours, minutes, seconds = map(int, parts)
            return hours * 3600 + minutes * 60 + seconds
         elif len(parts) == 2:
            # DDDD:HH:MM format
            hours, minutes = map(int, parts)
            return hours * 3600 + minutes * 60
         else:
            return 0  # Default to 0
      except (ValueError, TypeError):
         return 0  # Default to 0

   def test_connection(self) -> bool:
      """
      Test if PBS commands are available
      
      Returns:
         True if PBS is available
      """
      try:
         # Try a simple qstat command
         self._run_command(["/opt/pbs/bin/qstat", "--version"])
         return True
      except PBSCommandError:
         return False

   # Reservation methods
   
   def pbs_rstat_summary(self) -> List[PBSReservation]:
      """Get reservation summary list"""
      if self.use_sample_data:
         return self._load_sample_reservations_summary()
      
      try:
         output = self._run_command(["/opt/pbs/bin/pbs_rstat"])
         return self._parse_rstat_summary(output)
      except Exception as e:
         raise PBSCommandError(f"Failed to get reservation summary: {str(e)}")
   
   def pbs_rstat_detailed(self, reservation_id: str) -> PBSReservation:
      """Get detailed reservation information"""
      if self.use_sample_data:
         return self._load_sample_reservation_detail(reservation_id)
      
      try:
         output = self._run_command(["/opt/pbs/bin/pbs_rstat", "-f", reservation_id])
         return self._parse_rstat_detailed(output)
      except Exception as e:
         raise PBSCommandError(f"Failed to get reservation details for {reservation_id}: {str(e)}")
   
   def pbs_rstat_all_detailed(self) -> List[PBSReservation]:
      """Get detailed information for all reservations"""
      # Strategy: Get summary first, then detailed for each ID
      summary_reservations = self.pbs_rstat_summary()
      detailed_reservations = []
      
      for reservation in summary_reservations:
         try:
            detailed = self.pbs_rstat_detailed(reservation.reservation_id)
            detailed_reservations.append(detailed)
         except Exception as e:
            self.logger.warning(f"Failed to get details for {reservation.reservation_id}: {e}")
            # Fall back to summary data
            detailed_reservations.append(reservation)
      
      return detailed_reservations
   
   def _parse_rstat_summary(self, output: str) -> List[PBSReservation]:
      """Parse pbs_rstat summary output"""
      reservations = []
      lines = output.strip().split('\n')
      
      # Skip header lines
      data_lines = [line for line in lines if not line.startswith('Resv ID') and not line.startswith('---')]
      
      for line in data_lines:
         if line.strip():
            try:
               reservation = PBSReservation.from_summary_line(line)
               reservations.append(reservation)
            except Exception as e:
               self.logger.warning(f"Failed to parse reservation line: {line[:50]}... Error: {e}")
      
      return reservations
   
   def _parse_rstat_detailed(self, output: str) -> PBSReservation:
      """Parse pbs_rstat -f detailed output"""
      return PBSReservation.from_detailed_output(output)
   
   def _load_sample_reservations_summary(self) -> List[PBSReservation]:
      """Load sample reservation summary data"""
      try:
         with open(self.sample_data_dir / "pbs_rstat.txt", 'r') as f:
            output = f.read()
         return self._parse_rstat_summary(output)
      except Exception as e:
         self.logger.error(f"Failed to load sample reservation summary data: {str(e)}")
         return []
   
   def _load_sample_reservation_detail(self, reservation_id: str) -> PBSReservation:
      """Load sample detailed reservation data"""
      try:
         with open(self.sample_data_dir / "pbs_rstat_f.txt", 'r') as f:
            content = f.read()
         
         # Split by reservation entries (each starts with "Resv ID:")
         reservations = content.split("Resv ID: ")[1:]  # Skip empty first part
         
         # Create a mapping from short ID to full ID
         id_mapping = {}
         for resv_text in reservations:
            first_line = resv_text.split('\n')[0]
            full_id = first_line.strip()
            # Extract short ID (everything before the first dot after the main ID)
            if '.' in full_id:
               short_id = full_id.split('.')[0] + '.' + full_id.split('.')[1]  # e.g., "S6703362.aurora"
               id_mapping[short_id] = full_id
         
         # Try to find the reservation by various ID formats
         target_full_id = None
         
         # 1. Direct match with full ID
         if reservation_id in [resv_text.split('\n')[0].strip() for resv_text in reservations]:
            target_full_id = reservation_id
         
         # 2. Match via short ID mapping
         elif reservation_id in id_mapping:
            target_full_id = id_mapping[reservation_id]
         
         # 3. Partial match - check if reservation_id is contained in any full ID
         else:
            for resv_text in reservations:
               full_id = resv_text.split('\n')[0].strip()
               if reservation_id in full_id or full_id.startswith(reservation_id):
                  target_full_id = full_id
                  break
         
         # Find and return the matching reservation
         if target_full_id:
            for resv_text in reservations:
               first_line = resv_text.split('\n')[0]
               full_id = first_line.strip()
               if full_id == target_full_id:
                  resv_text = "Resv ID: " + resv_text.strip()
                  return PBSReservation.from_detailed_output(resv_text)
         
         # If not found, return the first reservation as fallback
         if reservations:
            resv_text = "Resv ID: " + reservations[0].strip()
            return PBSReservation.from_detailed_output(resv_text)
         
         raise PBSCommandError(f"No sample reservation data found for {reservation_id}")
         
      except Exception as e:
         self.logger.error(f"Failed to load sample reservation detail for {reservation_id}: {str(e)}")
         raise PBSCommandError(f"Failed to load sample reservation data: {str(e)}") 