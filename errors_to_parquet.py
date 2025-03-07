#!/usr/bin/env python3
import sqlite3
import pandas as pd
import logging
import os
import sys
from datetime import datetime

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[logging.FileHandler("errors_to_parquet.log"), logging.StreamHandler()]
)
logger = logging.getLogger("errors_to_parquet")

def connect_to_db(db_path="logs.db"):
    """Connect to the SQLite database"""
    try:
        logger.info(f"Connecting to database: {db_path}")
        conn = sqlite3.connect(db_path)
        return conn
    except sqlite3.Error as e:
        logger.error(f"Error connecting to database: {e}")
        sys.exit(1)

def query_parsing_errors(conn, limit=None):
    """Query parsing errors from the database"""
    try:
        logger.info("Querying parsing_errors table")
        query = "SELECT * FROM parsing_errors"
        if limit:
            query += f" LIMIT {limit}"
        
        # Read directly into pandas DataFrame
        df = pd.read_sql_query(query, conn)
        logger.info(f"Retrieved {len(df)} parsing error records")
        return df
    except Exception as e:
        logger.error(f"Error querying parsing_errors: {e}")
        return pd.DataFrame()

def query_error_logs(conn, limit=None):
    """Query error and critical logs from the logs table"""
    try:
        logger.info("Querying error logs from logs table")
        query = "SELECT * FROM logs WHERE level IN ('ERROR', 'CRITICAL')"
        if limit:
            query += f" LIMIT {limit}"
        
        # Read directly into pandas DataFrame
        df = pd.read_sql_query(query, conn)
        logger.info(f"Retrieved {len(df)} error log records")
        return df
    except Exception as e:
        logger.error(f"Error querying logs: {e}")
        return pd.DataFrame()

def query_stack_traces(conn, log_ids=None, limit=None):
    """Query stack traces for the given log IDs"""
    try:
        if log_ids is not None and len(log_ids) > 0:
            logger.info(f"Querying stack traces for {len(log_ids)} log IDs")
            # Convert log_ids to a comma-separated string for the IN clause
            log_ids_str = ','.join(str(id) for id in log_ids)
            query = f"SELECT * FROM stack_traces WHERE log_id IN ({log_ids_str})"
            if limit:
                query += f" LIMIT {limit}"
            
            # Read directly into pandas DataFrame
            df = pd.read_sql_query(query, conn)
            logger.info(f"Retrieved {len(df)} stack trace records")
            return df
        else:
            logger.info("No log IDs provided for stack traces query")
            return pd.DataFrame()
    except Exception as e:
        logger.error(f"Error querying stack_traces: {e}")
        return pd.DataFrame()

def query_all_logs(conn, limit=None):
    """Query all logs from the logs table"""
    try:
        logger.info("Querying all logs from logs table")
        query = "SELECT * FROM logs"
        if limit:
            query += f" LIMIT {limit}"
        
        # Read directly into pandas DataFrame
        df = pd.read_sql_query(query, conn)
        logger.info(f"Retrieved {len(df)} log records")
        return df
    except Exception as e:
        logger.error(f"Error querying logs: {e}")
        return pd.DataFrame()

def save_to_parquet(df, output_path=None, prefix="errors"):
    """Save DataFrame to parquet file"""
    if output_path is None:
        # Generate default filename with timestamp
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_path = f"{prefix}_{timestamp}.parquet"
    
    try:
        logger.info(f"Saving {len(df)} records to {output_path}")
        df.to_parquet(output_path, index=False)
        logger.info(f"Successfully saved parquet file to {output_path}")
        return output_path
    except Exception as e:
        logger.error(f"Error saving parquet file: {e}")
        sys.exit(1)

def get_db_stats(conn):
    """Get statistics about the database tables"""
    stats = {}
    try:
        cursor = conn.cursor()
        
        # Get table names
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
        tables = [row[0] for row in cursor.fetchall()]
        
        # Get row counts for each table
        for table in tables:
            cursor.execute(f"SELECT COUNT(*) FROM {table}")
            count = cursor.fetchone()[0]
            stats[table] = count
            
        # Get error log count
        cursor.execute("SELECT COUNT(*) FROM logs WHERE level IN ('ERROR', 'CRITICAL')")
        stats['error_logs'] = cursor.fetchone()[0]
        
        return stats
    except Exception as e:
        logger.error(f"Error getting database stats: {e}")
        return {}

def main():
    # Parse command line arguments
    import argparse
    parser = argparse.ArgumentParser(description="Export errors from SQLite database to parquet file")
    parser.add_argument("--db", default="logs.db", help="Path to SQLite database file")
    parser.add_argument("--output", help="Output parquet file path")
    parser.add_argument("--limit", type=int, help="Limit number of records to export per table")
    parser.add_argument("--type", choices=["parsing", "logs", "stack_traces", "all", "full_db"], 
                       default="all", help="Type of errors to export (default: all)")
    parser.add_argument("--verbose", "-v", action="store_true", 
                       help="Show detailed database statistics")
    args = parser.parse_args()
    
    # Connect to database
    conn = connect_to_db(args.db)
    
    # Initialize empty DataFrames
    parsing_errors_df = pd.DataFrame()
    error_logs_df = pd.DataFrame()
    stack_traces_df = pd.DataFrame()
    
    # Print database statistics
    if args.verbose:
        stats = get_db_stats(conn)
        print("\nDatabase Statistics:")
        for table, count in stats.items():
            print(f"  {table}: {count} records")
        print("")
    
    # Query data based on the requested type
    if args.type in ["parsing", "all"]:
        parsing_errors_df = query_parsing_errors(conn, args.limit)
        if not parsing_errors_df.empty:
            save_to_parquet(parsing_errors_df, 
                           args.output if args.type == "parsing" else None,
                           "parsing_errors")
    
    if args.type in ["logs", "all"]:
        error_logs_df = query_error_logs(conn, args.limit)
        if not error_logs_df.empty:
            save_to_parquet(error_logs_df, 
                           args.output if args.type == "logs" else None,
                           "error_logs")
    
    if args.type in ["stack_traces", "all"] and not error_logs_df.empty:
        # Get log IDs with stack traces
        log_ids_with_traces = error_logs_df[error_logs_df['has_stack_trace'] == 1]['id'].tolist()
        if log_ids_with_traces:
            stack_traces_df = query_stack_traces(conn, log_ids_with_traces, args.limit)
            if not stack_traces_df.empty:
                save_to_parquet(stack_traces_df, 
                               args.output if args.type == "stack_traces" else None,
                               "stack_traces")
    
    if args.type == "full_db":
        # Export entire logs table
        all_logs_df = query_all_logs(conn, args.limit)
        if not all_logs_df.empty:
            save_to_parquet(all_logs_df, args.output, "all_logs")
            print(f"  All logs exported: {len(all_logs_df)}")
    
    # Close database connection
    conn.close()
    
    # Print summary
    print(f"\nExport Summary:")
    if not parsing_errors_df.empty:
        print(f"  Parsing errors exported: {len(parsing_errors_df)}")
    if not error_logs_df.empty:
        print(f"  Error logs exported: {len(error_logs_df)}")
    if not stack_traces_df.empty:
        print(f"  Stack traces exported: {len(stack_traces_df)}")
    
    if parsing_errors_df.empty and error_logs_df.empty and stack_traces_df.empty:
        print("  No error records found or exported")

if __name__ == "__main__":
    main() 