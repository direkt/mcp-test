#!/usr/bin/env python3
import sqlite3
import sys
import csv
import io
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[logging.FileHandler("query_logs.log"), logging.StreamHandler()]
)
logger = logging.getLogger("query_logs")

def connect_to_db():
    """Connect to the SQLite database"""
    try:
        conn = sqlite3.connect("logs.db")
        conn.row_factory = sqlite3.Row  # This enables column access by name
        return conn
    except sqlite3.Error as e:
        logger.error(f"Error connecting to database: {e}")
        sys.exit(1)

def execute_query(conn, query):
    """Execute a SQL query and return the results"""
    try:
        cursor = conn.cursor()
        cursor.execute(query)
        return cursor.fetchall()
    except sqlite3.Error as e:
        logger.error(f"Error executing query: {e}")
        return None

def output_as_csv(rows):
    """Output query results as CSV"""
    if not rows:
        return
    
    # Create a CSV string
    output = io.StringIO()
    writer = csv.writer(output)
    
    # Write header
    writer.writerow(rows[0].keys())
    
    # Write data rows
    for row in rows:
        writer.writerow(row)
    
    # Print the CSV
    print(output.getvalue())

def main():
    conn = connect_to_db()
    
    if len(sys.argv) > 1:
        # If a query is provided as an argument, execute it
        query = " ".join(sys.argv[1:])
        logger.info(f"Executing query: {query}")
        results = execute_query(conn, query)
        
        if results:
            logger.info(f"Query returned {len(results)} rows")
            output_as_csv(results)
    else:
        # Get database info
        cursor = conn.cursor()
        
        # Get list of tables
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
        tables = cursor.fetchall()
        
        print("Database Tables:")
        for table in tables:
            table_name = table['name']
            
            # Get column info
            cursor.execute(f"PRAGMA table_info({table_name})")
            columns = cursor.fetchall()
            
            # Get row count
            cursor.execute(f"SELECT COUNT(*) as count FROM {table_name}")
            count = cursor.fetchone()['count']
            
            print(f"\n{table_name}:")
            print(f"  Columns: {', '.join([col['name'] for col in columns])}")
            print(f"  Row count: {count}")
    
    conn.close()

if __name__ == "__main__":
    main() 