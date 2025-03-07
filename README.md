# Log Analysis with SQLite MCP Server

This project provides tools to create an SQLite database from compressed log files and interact with it using the Model Context Protocol (MCP) SQLite server.

## Contents

- `create_log_db.py`: Script to extract and parse log files into an SQLite database
- `query_logs.py`: Script to directly query the SQLite database
- `logs.db`: SQLite database containing parsed log data

## Database Structure

The database contains the following tables:

### `logs` Table

- `id`: Unique identifier for each log entry
- `timestamp`: Timestamp of the log entry
- `thread`: Thread that generated the log
- `level`: Log level (INFO, WARN, ERROR, DEBUG)
- `module`: Module that generated the log
- `message`: Log message content
- `source_file`: Source log file
- `raw_log`: Raw log entry

### `json_logs` Table

- `id`: Unique identifier for each JSON log entry
- `timestamp`: Timestamp of the JSON log entry
- `log_data`: Full JSON log data
- `source_file`: Source log file

## Log Statistics

- Total log entries: 1,002,326
- Log level distribution:
  - INFO: 851,068
  - WARN: 13,890
  - ERROR: 1,141
  - DEBUG: 10

## Using the Database Directly

You can query the database directly using the `query_logs.py` script:

```bash
# View database information
python3 query_logs.py

# Run a custom query
python3 query_logs.py "SELECT * FROM logs WHERE level = 'ERROR' LIMIT 10"

MCP SQL Lite Server in Cursor

npx -y @smithery/cli@latest run mcp-server-sqlite-npx --config "{\"databasePath\":\"/path/to/thedatbase/logs.db\"}"

