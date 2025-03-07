#!/usr/bin/env python3
import sqlite3
import sys
import json

def connect_to_db():
    """Connect to the SQLite database"""
    try:
        conn = sqlite3.connect("logs.db")
        conn.row_factory = sqlite3.Row  # This enables column access by name
        return conn
    except sqlite3.Error as e:
        print(f"Error connecting to database: {e}")
        sys.exit(1)

def execute_query(conn, query):
    """Execute a SQL query and return the results"""
    try:
        cursor = conn.cursor()
        cursor.execute(query)
        return cursor.fetchall()
    except sqlite3.Error as e:
        print(f"Error executing query: {e}")
        return None

def print_table_info(conn):
    """Print information about the database tables"""
    cursor = conn.cursor()
    
    # Get list of tables
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
    tables = cursor.fetchall()
    
    print("Database Tables:")
    for table in tables:
        table_name = table['name']
        print(f"\n{table_name}:")
        
        # Get column info
        cursor.execute(f"PRAGMA table_info({table_name})")
        columns = cursor.fetchall()
        
        print("  Columns:")
        for col in columns:
            print(f"    {col['name']} ({col['type']})")
        
        # Get row count
        cursor.execute(f"SELECT COUNT(*) as count FROM {table_name}")
        count = cursor.fetchone()['count']
        print(f"  Total rows: {count}")

def print_sample_data(conn, table_name, limit=5):
    """Print sample data from a table"""
    cursor = conn.cursor()
    cursor.execute(f"SELECT * FROM {table_name} LIMIT {limit}")
    rows = cursor.fetchall()
    
    print(f"\nSample data from {table_name} (first {limit} rows):")
    for row in rows:
        print("\n  Row:")
        for key in row.keys():
            value = row[key]
            # Truncate long values
            if isinstance(value, str) and len(value) > 100:
                value = value[:100] + "..."
            print(f"    {key}: {value}")

def main():
    conn = connect_to_db()
    
    if len(sys.argv) > 1:
        # If a query is provided as an argument, execute it
        query = " ".join(sys.argv[1:])
        print(f"Executing query: {query}\n")
        results = execute_query(conn, query)
        
        if results:
            print("Results:")
            for row in results:
                print("\n  Row:")
                for key in row.keys():
                    value = row[key]
                    # Truncate long values
                    if isinstance(value, str) and len(value) > 100:
                        value = value[:100] + "..."
                    print(f"    {key}: {value}")
            print(f"\nTotal rows returned: {len(results)}")
    else:
        # Otherwise, print database info
        print_table_info(conn)
        
        # Print sample data from each table
        cursor = conn.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
        tables = cursor.fetchall()
        
        for table in tables:
            table_name = table['name']
            print_sample_data(conn, table_name)
    
    conn.close()

if __name__ == "__main__":
    main() 