#!/usr/bin/env python3
import gzip
import sqlite3
import os
import re
import json
import glob
from datetime import datetime

# Define the database file
DB_FILE = "logs.db"

# Define the log files - this will be replaced with dynamic file finding
# LOG_FILES = [
#     "server.log.gz",
#     "server.2025-03-05.0.log.gz",
#     "server.2025-03-04.0.log.gz"
# ]

# Regular expression pattern for the log format we observed
# Format: 2025-03-06 00:00:00,024 [UserServer-2] INFO  c.d.s.r.user.EnterpriseUserRPCServer - [USER]: Channel ...
LOG_PATTERN = r'^(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2},\d{3}) \[([^\]]+)\] (\w+)\s+([^\s-]+) - (.+)$'

def create_database():
    """Create the SQLite database and tables"""
    # Remove existing database if it exists
    if os.path.exists(DB_FILE):
        os.remove(DB_FILE)
        print(f"Removed existing {DB_FILE}")
    
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    
    # Create logs table
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS logs (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        timestamp TEXT,
        thread TEXT,
        level TEXT,
        module TEXT,
        message TEXT,
        source_file TEXT,
        raw_log TEXT
    )
    ''')
    
    # Create a table for JSON logs if they exist
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS json_logs (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        timestamp TEXT,
        log_data TEXT,
        source_file TEXT
    )
    ''')
    
    conn.commit()
    return conn, cursor

def parse_log_line(line, source_file):
    """Parse a log line and return a dictionary of values"""
    # Try to parse as structured log
    match = re.match(LOG_PATTERN, line)
    if match:
        timestamp, thread, level, module, message = match.groups()
        return {
            "timestamp": timestamp,
            "thread": thread,
            "level": level,
            "module": module,
            "message": message,
            "source_file": source_file,
            "raw_log": line
        }
    
    # Try to parse as JSON
    try:
        data = json.loads(line)
        if isinstance(data, dict):
            # Extract timestamp if it exists
            timestamp = data.get("timestamp", data.get("time", data.get("@timestamp", "")))
            return {
                "is_json": True,
                "timestamp": timestamp,
                "log_data": line,
                "source_file": source_file
            }
    except json.JSONDecodeError:
        pass
    
    # If we can't parse it in a structured way, just store the raw log
    return {
        "timestamp": "",
        "thread": "",
        "level": "",
        "module": "",
        "message": line,
        "source_file": source_file,
        "raw_log": line
    }

def find_gz_files():
    """Find all .gz files in the current directory"""
    gz_files = glob.glob("*.gz")
    print(f"Found {len(gz_files)} .gz files: {', '.join(gz_files)}")
    return gz_files

def process_log_files(conn, cursor):
    """Process all log files and insert into database"""
    # Find all .gz files in the current directory
    log_files = find_gz_files()
    
    if not log_files:
        print("No .gz files found in the current directory.")
        return
    
    for log_file in log_files:
        if not os.path.exists(log_file):
            print(f"Warning: Log file {log_file} not found, skipping.")
            continue
        
        print(f"Processing {log_file}...")
        
        # Open and decompress the gzip file
        with gzip.open(log_file, 'rt', encoding='utf-8', errors='replace') as f:
            batch = []
            json_batch = []
            batch_size = 1000
            
            for line in f:
                line = line.strip()
                if not line:
                    continue
                
                parsed = parse_log_line(line, log_file)
                
                if parsed.get("is_json"):
                    json_batch.append((
                        parsed["timestamp"],
                        parsed["log_data"],
                        parsed["source_file"]
                    ))
                else:
                    batch.append((
                        parsed["timestamp"],
                        parsed.get("thread", ""),
                        parsed["level"],
                        parsed["module"],
                        parsed["message"],
                        parsed["source_file"],
                        parsed["raw_log"]
                    ))
                
                # Insert in batches for better performance
                if len(batch) >= batch_size:
                    cursor.executemany(
                        "INSERT INTO logs (timestamp, thread, level, module, message, source_file, raw_log) VALUES (?, ?, ?, ?, ?, ?, ?)",
                        batch
                    )
                    batch = []
                
                if len(json_batch) >= batch_size:
                    cursor.executemany(
                        "INSERT INTO json_logs (timestamp, log_data, source_file) VALUES (?, ?, ?)",
                        json_batch
                    )
                    json_batch = []
            
            # Insert any remaining logs
            if batch:
                cursor.executemany(
                    "INSERT INTO logs (timestamp, thread, level, module, message, source_file, raw_log) VALUES (?, ?, ?, ?, ?, ?, ?)",
                    batch
                )
            
            if json_batch:
                cursor.executemany(
                    "INSERT INTO json_logs (timestamp, log_data, source_file) VALUES (?, ?, ?)",
                    json_batch
                )
            
            conn.commit()
            print(f"Finished processing {log_file}")

def create_indexes(conn, cursor):
    """Create indexes for better query performance"""
    print("Creating indexes...")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_logs_timestamp ON logs (timestamp)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_logs_level ON logs (level)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_logs_module ON logs (module)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_logs_thread ON logs (thread)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_json_logs_timestamp ON json_logs (timestamp)")
    conn.commit()

def main():
    print(f"Creating log database: {DB_FILE}")
    conn, cursor = create_database()
    
    try:
        process_log_files(conn, cursor)
        create_indexes(conn, cursor)
        
        # Print some stats
        cursor.execute("SELECT COUNT(*) FROM logs")
        log_count = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(*) FROM json_logs")
        json_log_count = cursor.fetchone()[0]
        
        # Print level distribution
        cursor.execute("SELECT level, COUNT(*) as count FROM logs GROUP BY level ORDER BY count DESC")
        level_counts = cursor.fetchall()
        
        print(f"\nDatabase created successfully!")
        print(f"Total regular logs: {log_count}")
        print(f"Total JSON logs: {json_log_count}")
        
        if level_counts:
            print("\nLog level distribution:")
            for level, count in level_counts:
                if level:  # Only show non-empty levels
                    print(f"  {level}: {count}")
        
        print(f"Database file: {os.path.abspath(DB_FILE)}")
        
    finally:
        conn.close()

if __name__ == "__main__":
    main() 