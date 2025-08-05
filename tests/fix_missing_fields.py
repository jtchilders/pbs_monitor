#!/usr/bin/env python3
"""
Script to retroactively fill missing fields in the Job table using JSON data.
This script identifies jobs with missing fields and populates them from the raw PBS data.
"""

import argparse
import logging
import sys
import json
from datetime import datetime
from sqlalchemy import create_engine, text, update
from sqlalchemy.orm import sessionmaker
from typing import Dict, Any, Optional

# Setup logging
logging.basicConfig(
   level=logging.INFO,
   format='%(asctime)s - %(levelname)s - %(message)s',
   datefmt='%d-%m %H:%M'
)
logger = logging.getLogger(__name__)

def connect_to_database(db_path):
   """Connect to the SQLite database"""
   try:
      engine = create_engine(f"sqlite:///{db_path}")
      Session = sessionmaker(bind=engine)
      return Session()
   except Exception as e:
      logger.error(f"Failed to connect to database: {e}")
      sys.exit(1)

def extract_project_from_json(raw_data: Dict[str, Any]) -> Optional[str]:
   """Extract project from raw PBS data"""
   if not raw_data:
      return None
   
   # Try multiple possible sources for project information
   project = raw_data.get('project')
   if project:
      return project
   
   # Try Account_Name as project
   account_name = raw_data.get('Account_Name')
   if account_name:
      return account_name
   
   return None

def extract_allocation_type_from_json(raw_data: Dict[str, Any]) -> Optional[str]:
   """Extract allocation type from raw PBS data"""
   if not raw_data:
      return None
   
   resource_list = raw_data.get('Resource_List', {})
   if not resource_list:
      return None
   
   # Try award_category first
   award_category = resource_list.get('award_category')
   if award_category:
      return award_category
   
   # Try award_type as fallback
   award_type = resource_list.get('award_type')
   if award_type:
      return award_type
   
   return None

def extract_memory_from_json(raw_data: Dict[str, Any]) -> Optional[str]:
   """Extract memory from raw PBS data"""
   if not raw_data:
      return None
   
   resource_list = raw_data.get('Resource_List', {})
   if not resource_list:
      return None
   
   # Try different memory field names
   memory = resource_list.get('mem')
   if memory:
      return memory
   
   memory = resource_list.get('memory')
   if memory:
      return memory
   
   return None

def extract_exit_status_from_json(raw_data: Dict[str, Any]) -> Optional[int]:
   """Extract exit status from raw PBS data"""
   if not raw_data:
      return None
   
   # Try different exit status field names
   exit_status = raw_data.get('Exit_status')
   if exit_status is not None:
      try:
         return int(exit_status)
      except (ValueError, TypeError):
         pass
   
   exit_status = raw_data.get('exit_status')
   if exit_status is not None:
      try:
         return int(exit_status)
      except (ValueError, TypeError):
         pass
   
   return None

def calculate_total_cores(nodes: Optional[int], ppn: Optional[int]) -> Optional[int]:
   """Calculate total cores from nodes and ppn"""
   if nodes is None or ppn is None:
      return None
   return nodes * ppn

def calculate_actual_runtime_seconds(start_time, end_time) -> Optional[int]:
   """Calculate actual runtime in seconds"""
   if not start_time or not end_time:
      return None
   
   # Handle both string and datetime objects
   if isinstance(start_time, str):
      # Try to parse as ISO format first, then PBS format
      try:
         start_time = datetime.fromisoformat(start_time.replace('Z', '+00:00'))
      except ValueError:
         start_time = parse_pbs_time(start_time)
   elif hasattr(start_time, 'replace'):  # SQLAlchemy datetime object
      start_time = start_time.replace(tzinfo=None)
   
   if isinstance(end_time, str):
      # Try to parse as ISO format first, then PBS format
      try:
         end_time = datetime.fromisoformat(end_time.replace('Z', '+00:00'))
      except ValueError:
         end_time = parse_pbs_time(end_time)
   elif hasattr(end_time, 'replace'):  # SQLAlchemy datetime object
      end_time = end_time.replace(tzinfo=None)
   
   if not start_time or not end_time:
      return None
   
   duration = end_time - start_time
   return int(duration.total_seconds())

def calculate_queue_time_seconds(submit_time, start_time) -> Optional[int]:
   """Calculate queue time in seconds"""
   if not submit_time or not start_time:
      return None
   
   # Handle both string and datetime objects
   if isinstance(submit_time, str):
      # Try to parse as ISO format first, then PBS format
      try:
         submit_time = datetime.fromisoformat(submit_time.replace('Z', '+00:00'))
      except ValueError:
         submit_time = parse_pbs_time(submit_time)
   elif hasattr(submit_time, 'replace'):  # SQLAlchemy datetime object
      submit_time = submit_time.replace(tzinfo=None)
   
   if isinstance(start_time, str):
      # Try to parse as ISO format first, then PBS format
      try:
         start_time = datetime.fromisoformat(start_time.replace('Z', '+00:00'))
      except ValueError:
         start_time = parse_pbs_time(start_time)
   elif hasattr(start_time, 'replace'):  # SQLAlchemy datetime object
      start_time = start_time.replace(tzinfo=None)
   
   if not submit_time or not start_time:
      return None
   
   duration = start_time - submit_time
   return int(duration.total_seconds())

def parse_pbs_time(time_str: Optional[str]) -> Optional[datetime]:
   """Parse PBS timestamp format"""
   if not time_str:
      return None
   
   try:
      # PBS typically uses format like "Thu Oct 12 14:30:00 2023"
      return datetime.strptime(time_str, "%a %b %d %H:%M:%S %Y")
   except (ValueError, TypeError):
      return None

def get_jobs_with_missing_fields(session, limit=None):
   """Get jobs that have missing fields that can be filled"""
   query = """
      SELECT 
         job_id, job_name, owner, project, allocation_type,
         state, queue, nodes, ppn, walltime, memory,
         submit_time, start_time, end_time, priority, exit_status,
         execution_node, total_cores, actual_runtime_seconds,
         queue_time_seconds, first_seen, last_updated,
         final_state_recorded, raw_pbs_data
      FROM jobs 
      WHERE 
         project IS NULL OR 
         allocation_type IS NULL OR 
         memory IS NULL OR 
         exit_status IS NULL OR 
         total_cores IS NULL OR 
         actual_runtime_seconds IS NULL OR 
         queue_time_seconds IS NULL
      ORDER BY last_updated DESC
   """
   
   if limit:
      query += f" LIMIT {limit}"
   
   result = session.execute(text(query))
   return result.fetchall()

def update_job_fields(session, job_id: str, updates: Dict[str, Any]):
   """Update job fields in the database"""
   try:
      # Build the SET clause
      set_clause = ", ".join([f"{key} = :{key}" for key in updates.keys()])
      query = f"UPDATE jobs SET {set_clause} WHERE job_id = :job_id"
      
      # Execute the update
      session.execute(text(query), {"job_id": job_id, **updates})
      return True
   except Exception as e:
      logger.error(f"Failed to update job {job_id}: {e}")
      return False

def process_job_entry(entry) -> Dict[str, Any]:
   """Process a job entry and return updates needed"""
   updates = {}
   
   # Parse raw PBS data
   raw_data = entry.raw_pbs_data
   if isinstance(raw_data, str):
      try:
         raw_data = json.loads(raw_data)
      except json.JSONDecodeError:
         logger.warning(f"Failed to parse JSON for job {entry.job_id}")
         raw_data = {}
   
   # Extract project
   if entry.project is None:
      project = extract_project_from_json(raw_data)
      if project:
         updates['project'] = project
   
   # Extract allocation type
   if entry.allocation_type is None:
      allocation_type = extract_allocation_type_from_json(raw_data)
      if allocation_type:
         updates['allocation_type'] = allocation_type
   
   # Extract memory
   if entry.memory is None:
      memory = extract_memory_from_json(raw_data)
      if memory:
         updates['memory'] = memory
   
   # Extract exit status
   if entry.exit_status is None:
      exit_status = extract_exit_status_from_json(raw_data)
      if exit_status is not None:
         updates['exit_status'] = exit_status
   
   # Calculate total cores
   if entry.total_cores is None:
      total_cores = calculate_total_cores(entry.nodes, entry.ppn)
      if total_cores is not None:
         updates['total_cores'] = total_cores
   
   # Calculate actual runtime
   if entry.actual_runtime_seconds is None:
      actual_runtime = calculate_actual_runtime_seconds(entry.start_time, entry.end_time)
      if actual_runtime is not None:
         updates['actual_runtime_seconds'] = actual_runtime
   
   # Calculate queue time
   if entry.queue_time_seconds is None:
      queue_time = calculate_queue_time_seconds(entry.submit_time, entry.start_time)
      if queue_time is not None:
         updates['queue_time_seconds'] = queue_time
   
   return updates

def main():
   parser = argparse.ArgumentParser(description="Fix missing fields in PBS monitor database")
   parser.add_argument("-d", "--database", default="../.pbs_monitor.db",
                      help="Path to SQLite database file (default: ../.pbs_monitor.db)")
   parser.add_argument("-l", "--limit", type=int, default=None,
                      help="Limit number of jobs to process (default: all)")
   parser.add_argument("--dry-run", action="store_true",
                      help="Show what would be updated without making changes")
   
   args = parser.parse_args()
   
   logger.info(f"Connecting to database: {args.database}")
   session = connect_to_database(args.database)
   
   logger.info("Finding jobs with missing fields...")
   jobs = get_jobs_with_missing_fields(session, args.limit)
   
   if not jobs:
      logger.info("No jobs with missing fields found.")
      return
   
   logger.info(f"Found {len(jobs)} jobs with missing fields")
   
   total_updates = 0
   successful_updates = 0
   
   for i, job in enumerate(jobs, 1):
      logger.info(f"Processing job {i}/{len(jobs)}: {job.job_id}")
      
      updates = process_job_entry(job)
      
      if updates:
         total_updates += len(updates)
         logger.info(f"  Updates for {job.job_id}: {updates}")
         
         if not args.dry_run:
            if update_job_fields(session, job.job_id, updates):
               successful_updates += len(updates)
               session.commit()
               logger.info(f"  Successfully updated {job.job_id}")
            else:
               logger.error(f"  Failed to update {job.job_id}")
      else:
         logger.info(f"  No updates needed for {job.job_id}")
   
   if args.dry_run:
      logger.info(f"DRY RUN: Would update {total_updates} fields across {len(jobs)} jobs")
   else:
      logger.info(f"Successfully updated {successful_updates} fields across {len(jobs)} jobs")
   
   session.close()
   logger.info("Database connection closed.")

if __name__ == "__main__":
   main() 