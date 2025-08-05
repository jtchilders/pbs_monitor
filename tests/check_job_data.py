#!/usr/bin/env python3
"""
Script to verify that fields are being filled in the Job table of the database.
Displays N entries with all columns for verification.
"""

import argparse
import logging
import sys
from datetime import datetime
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
import json

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

def get_job_entries(session, limit=10):
   """Retrieve job entries from the database"""
   try:
      # Query to get all columns from jobs table
      query = text("""
         SELECT 
            job_id, job_name, owner, project, allocation_type,
            state, queue, nodes, ppn, walltime, memory,
            submit_time, start_time, end_time, priority, exit_status,
            execution_node, total_cores, actual_runtime_seconds,
            queue_time_seconds, first_seen, last_updated,
            final_state_recorded, raw_pbs_data
         FROM jobs 
         ORDER BY last_updated DESC 
         LIMIT :limit
      """)
      
      result = session.execute(query, {"limit": limit})
      return result.fetchall()
   except Exception as e:
      logger.error(f"Failed to query jobs table: {e}")
      return []

def format_timestamp(timestamp):
   """Format timestamp for display"""
   if timestamp is None:
      return "None"
   if isinstance(timestamp, str):
      return timestamp
   return timestamp.strftime('%Y-%m-%d %H:%M:%S')

def format_json_data(data):
   """Format JSON data for display - no truncation"""
   if data is None:
      return "None"
   try:
      if isinstance(data, str):
         parsed = json.loads(data)
      else:
         parsed = data
      return json.dumps(parsed, indent=2)
   except:
      return str(data)

def display_job_entries(entries):
   """Display job entries in a formatted table"""
   if not entries:
      logger.info("No job entries found in the database.")
      return
   
   logger.info(f"Found {len(entries)} job entries:")
   logger.info("=" * 120)
   
   for i, entry in enumerate(entries, 1):
      logger.info(f"Entry {i}:")
      logger.info("-" * 80)
      
      # Display all columns with proper formatting
      columns = [
         ("Job ID", entry.job_id),
         ("Job Name", entry.job_name),
         ("Owner", entry.owner),
         ("Project", entry.project),
         ("Allocation Type", entry.allocation_type),
         ("State", entry.state),
         ("Queue", entry.queue),
         ("Nodes", entry.nodes),
         ("PPN", entry.ppn),
         ("Walltime", entry.walltime),
         ("Memory", entry.memory),
         ("Submit Time", format_timestamp(entry.submit_time)),
         ("Start Time", format_timestamp(entry.start_time)),
         ("End Time", format_timestamp(entry.end_time)),
         ("Priority", entry.priority),
         ("Exit Status", entry.exit_status),
         ("Execution Node", entry.execution_node),
         ("Total Cores", entry.total_cores),
         ("Actual Runtime (seconds)", entry.actual_runtime_seconds),
         ("Queue Time (seconds)", entry.queue_time_seconds),
         ("First Seen", format_timestamp(entry.first_seen)),
         ("Last Updated", format_timestamp(entry.last_updated)),
         ("Final State Recorded", entry.final_state_recorded),
         ("Raw PBS Data", format_json_data(entry.raw_pbs_data))
      ]
      
      for col_name, value in columns:
         logger.info(f"  {col_name:<25}: {value}")
      
      logger.info("")

def main():
   parser = argparse.ArgumentParser(description="Check job data in PBS monitor database")
   parser.add_argument("-n", "--num-entries", type=int, default=5,
                      help="Number of job entries to display (default: 5)")
   parser.add_argument("-d", "--database", default="../.pbs_monitor.db",
                      help="Path to SQLite database file (default: ../.pbs_monitor.db)")
   
   args = parser.parse_args()
   
   logger.info(f"Connecting to database: {args.database}")
   session = connect_to_database(args.database)
   
   logger.info(f"Retrieving {args.num_entries} job entries...")
   entries = get_job_entries(session, args.num_entries)
   
   display_job_entries(entries)
   
   session.close()
   logger.info("Database connection closed.")

if __name__ == "__main__":
   main() 