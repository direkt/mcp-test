#!/usr/bin/env python3
import gzip
import sqlite3
import os
import re
import glob
from datetime import datetime
import sys
import traceback
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[logging.FileHandler("create_log_db.log"), logging.StreamHandler()]
)
logger = logging.getLogger("create_log_db")

# Define the database file
DB_FILE = "logs.db"

# Improved regular expression pattern for the log format
# Format: 2025-03-06 00:00:00,024 [UserServer-2] INFO  c.d.s.r.user.EnterpriseUserRPCServer - [USER]: Channel ...
LOG_PATTERN = r'^(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2},\d{3}) \[([^\]]+)\] (\w+)\s+([^\s-]+) - (.+)$'

# Alternative pattern for logs that might not match the primary pattern
ALT_LOG_PATTERN = r'^(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2},\d{3}) \[([^\]]+)\] (\w+)\s+(.+)$'

# Pattern to detect the start of a Java stack trace
STACK_TRACE_START_PATTERN = r'^(java\.\w+\.\w+Exception|Caused by:|at [\w\.]+\()'

def create_database():
    """Create the SQLite database and tables"""
    # Remove existing database if it exists
    if os.path.exists(DB_FILE):
        os.remove(DB_FILE)
        logger.info(f"Removed existing {DB_FILE}")
    
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
        raw_log TEXT,
        has_stack_trace INTEGER DEFAULT 0
    )
    ''')
    
    # Create a table for parsing errors
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS parsing_errors (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        line TEXT,
        source_file TEXT,
        error_message TEXT,
        timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
    )
    ''')
    
    # Create a table for stack traces
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS stack_traces (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        log_id INTEGER,
        stack_trace TEXT,
        FOREIGN KEY (log_id) REFERENCES logs (id)
    )
    ''')
    
    conn.commit()
    return conn, cursor

def is_stack_trace_line(line):
    """Check if a line is part of a stack trace"""
    return re.match(STACK_TRACE_START_PATTERN, line) is not None or line.strip().startswith("at ") or "Exception" in line

def parse_log_line(line, source_file):
    """Parse a log line and return a dictionary of values"""
    # Try to parse as structured log with primary pattern
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
            "raw_log": line,
            "parsed": True
        }
    
    # Try alternative pattern
    match = re.match(ALT_LOG_PATTERN, line)
    if match:
        timestamp, thread, level, message = match.groups()
        # In this case, we don't have a separate module, so we'll use an empty string
        return {
            "timestamp": timestamp,
            "thread": thread,
            "level": level,
            "module": "",
            "message": message,
            "source_file": source_file,
            "raw_log": line,
            "parsed": True
        }
    
    # Check if it's a stack trace line
    if is_stack_trace_line(line):
        return {
            "is_stack_trace": True,
            "stack_trace_line": line,
            "parsed": True
        }
    
    # If we can't parse it in a structured way, just store the raw log
    # and record it as a parsing error
    return {
        "timestamp": "",
        "thread": "",
        "level": "",
        "module": "",
        "message": line,
        "source_file": source_file,
        "raw_log": line,
        "parsed": False,
        "error": "Could not parse log line with any pattern"
    }

def find_gz_files():
    """Find all .gz files in the current directory"""
    gz_files = glob.glob("*.gz")
    logger.info(f"Found {len(gz_files)} .gz files: {', '.join(gz_files)}")
    return gz_files

def process_log_files(conn, cursor):
    """Process all log files and insert into database"""
    # Find all .gz files in the current directory
    log_files = find_gz_files()
    
    if not log_files:
        logger.warning("No .gz files found in the current directory.")
        return
    
    total_logs = 0
    total_stack_traces = 0
    total_errors = 0
    
    for log_file in log_files:
        if not os.path.exists(log_file):
            logger.warning(f"Log file {log_file} not found, skipping.")
            continue
        
        logger.info(f"Processing {log_file}...")
        file_logs = 0
        file_stack_traces = 0
        file_errors = 0
        
        try:
            # Open and decompress the gzip file
            with gzip.open(log_file, 'rt', encoding='utf-8', errors='replace') as f:
                batch = []
                error_batch = []
                stack_trace_batch = []
                batch_size = 1000
                line_count = 0
                
                # Variables to track multi-line log entries
                current_log_entry = None
                current_stack_trace = []
                last_log_id = None
                
                for line in f:
                    line_count += 1
                    line = line.strip()
                    if not line:
                        continue
                    
                    try:
                        # Check if this is a new log entry or continuation of previous
                        is_new_log = re.match(LOG_PATTERN, line) is not None or re.match(ALT_LOG_PATTERN, line) is not None
                        
                        # If we have a stack trace and this is a new log, save the stack trace
                        if current_stack_trace and is_new_log and last_log_id is not None:
                            stack_trace_text = "\n".join(current_stack_trace)
                            stack_trace_batch.append((last_log_id, stack_trace_text))
                            current_stack_trace = []
                            file_stack_traces += 1
                        
                        # Parse the line
                        parsed = parse_log_line(line, log_file)
                        
                        if is_new_log:
                            # This is a new log entry
                            if parsed.get("parsed", False):
                                has_stack_trace = 0
                                batch.append((
                                    parsed["timestamp"],
                                    parsed.get("thread", ""),
                                    parsed["level"],
                                    parsed["module"],
                                    parsed["message"],
                                    parsed["source_file"],
                                    parsed["raw_log"],
                                    has_stack_trace
                                ))
                                file_logs += 1
                                current_log_entry = parsed
                            else:
                                error_batch.append((
                                    line,
                                    log_file,
                                    parsed.get("error", "Unknown parsing error")
                                ))
                                file_errors += 1
                                current_log_entry = None
                        elif parsed.get("is_stack_trace", False):
                            # This is a stack trace line
                            current_stack_trace.append(parsed["stack_trace_line"])
                            
                            # If we have a current log entry, mark it as having a stack trace
                            if current_log_entry and batch:
                                # Update the last entry in the batch to indicate it has a stack trace
                                last_entry = list(batch[-1])
                                last_entry[7] = 1  # Set has_stack_trace to 1
                                batch[-1] = tuple(last_entry)
                        else:
                            # This is a continuation of the previous log or an error
                            if current_log_entry:
                                # Append to the message of the current log entry
                                if batch:
                                    last_entry = list(batch[-1])
                                    last_entry[4] += "\n" + line  # Append to message
                                    last_entry[6] += "\n" + line  # Append to raw_log
                                    batch[-1] = tuple(last_entry)
                            else:
                                error_batch.append((
                                    line,
                                    log_file,
                                    "Continuation line without a parent log entry"
                                ))
                                file_errors += 1
                        
                        # Insert in batches for better performance
                        if len(batch) >= batch_size:
                            cursor.executemany(
                                "INSERT INTO logs (timestamp, thread, level, module, message, source_file, raw_log, has_stack_trace) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                                batch
                            )
                            # Get the ID of the last inserted log for stack traces
                            if current_stack_trace:
                                last_log_id = cursor.lastrowid
                            batch = []
                            
                        if len(error_batch) >= batch_size:
                            cursor.executemany(
                                "INSERT INTO parsing_errors (line, source_file, error_message) VALUES (?, ?, ?)",
                                error_batch
                            )
                            error_batch = []
                            
                        if len(stack_trace_batch) >= batch_size:
                            cursor.executemany(
                                "INSERT INTO stack_traces (log_id, stack_trace) VALUES (?, ?)",
                                stack_trace_batch
                            )
                            stack_trace_batch = []
                            
                        # Commit every 100,000 lines to avoid transaction getting too large
                        if line_count % 100000 == 0:
                            conn.commit()
                            logger.info(f"  Processed {line_count} lines...")
                    
                    except Exception as e:
                        error_batch.append((
                            line,
                            log_file,
                            f"Exception: {str(e)}"
                        ))
                        file_errors += 1
                
                # Insert any remaining logs
                if batch:
                    cursor.executemany(
                        "INSERT INTO logs (timestamp, thread, level, module, message, source_file, raw_log, has_stack_trace) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                        batch
                    )
                    # Get the ID of the last inserted log for stack traces
                    if current_stack_trace:
                        last_log_id = cursor.lastrowid
                
                # Insert the final stack trace if there is one
                if current_stack_trace and last_log_id is not None:
                    stack_trace_text = "\n".join(current_stack_trace)
                    stack_trace_batch.append((last_log_id, stack_trace_text))
                    file_stack_traces += 1
                
                if error_batch:
                    cursor.executemany(
                        "INSERT INTO parsing_errors (line, source_file, error_message) VALUES (?, ?, ?)",
                        error_batch
                    )
                    
                if stack_trace_batch:
                    cursor.executemany(
                        "INSERT INTO stack_traces (log_id, stack_trace) VALUES (?, ?)",
                        stack_trace_batch
                    )
                
                conn.commit()
                
                total_logs += file_logs
                total_stack_traces += file_stack_traces
                total_errors += file_errors
                
                logger.info(f"Finished processing {log_file}: {file_logs} logs, {file_stack_traces} stack traces, {file_errors} errors")
        
        except Exception as e:
            logger.error(f"Error processing {log_file}: {str(e)}")
            traceback.print_exc()
            conn.rollback()
    
    return total_logs, total_stack_traces, total_errors

def create_indexes(conn, cursor):
    """Create indexes for better query performance"""
    logger.info("Creating indexes...")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_logs_timestamp ON logs (timestamp)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_logs_level ON logs (level)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_logs_module ON logs (module)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_logs_thread ON logs (thread)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_logs_has_stack_trace ON logs (has_stack_trace)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_parsing_errors_source_file ON parsing_errors (source_file)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_stack_traces_log_id ON stack_traces (log_id)")
    conn.commit()

def main():
    logger.info(f"Creating log database: {DB_FILE}")
    
    try:
        conn, cursor = create_database()
        
        try:
            total_logs, total_stack_traces, total_errors = process_log_files(conn, cursor)
            create_indexes(conn, cursor)
            
            # Get stats
            cursor.execute("SELECT COUNT(*) FROM logs")
            log_count = cursor.fetchone()[0]
            
            cursor.execute("SELECT COUNT(*) FROM parsing_errors")
            error_count = cursor.fetchone()[0]
            
            cursor.execute("SELECT COUNT(*) FROM stack_traces")
            stack_trace_count = cursor.fetchone()[0]
            
            # Get level distribution
            cursor.execute("SELECT level, COUNT(*) as count FROM logs GROUP BY level ORDER BY count DESC")
            level_counts = cursor.fetchall()
            
            logger.info(f"\nDatabase created successfully!")
            logger.info(f"Total regular logs: {log_count}")
            logger.info(f"Total stack traces: {stack_trace_count}")
            logger.info(f"Total parsing errors: {error_count}")
            
            if level_counts:
                level_distribution = "\nLog level distribution:"
                for level, count in level_counts:
                    if level:  # Only show non-empty levels
                        level_distribution += f"\n  {level}: {count}"
                logger.info(level_distribution)
            
            logger.info(f"Database file: {os.path.abspath(DB_FILE)}")
            
        finally:
            conn.close()
    
    except Exception as e:
        logger.error(f"Error: {str(e)}")
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    main() 