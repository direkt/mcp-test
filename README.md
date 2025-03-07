# Log Analysis with SQLite MCP Server

This project provides tools to create an SQLite database from compressed log files and interact with it using the Model Context Protocol (MCP) SQLite server.

## Contents

- `create_log_db.py`: Script to extract and parse log files into an SQLite database
- `query_logs.py`: Script to directly query the SQLite database
- `setup_mcp_server.sh`: Script to set up the MCP SQLite server
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
```

## Setting Up the MCP SQLite Server

To set up the MCP SQLite server:

1. Run the setup script:
   ```bash
   ./setup_mcp_server.sh
   ```

2. Add the server configuration to your `claude_desktop_config.json`:
   ```json
   "mcpServers": {
     "sqlite": {
       "command": "docker",
       "args": [
         "run",
         "--rm",
         "-i",
         "-v",
         "mcp-logs:/mcp",
         "mcp/sqlite",
         "--db-path",
         "/mcp/logs.db"
       ]
     }
   }
   ```

3. Start Claude Desktop with the MCP server enabled

## Using the MCP SQLite Server

Once the server is set up, you can use the following tools in Claude Desktop:

### Query Tools

- `read_query`: Execute SELECT queries to read data
  ```sql
  SELECT timestamp, level, module, message FROM logs WHERE level = 'ERROR' LIMIT 10
  ```

- `write_query`: Execute INSERT, UPDATE, or DELETE queries
  ```sql
  INSERT INTO logs (timestamp, level, message) VALUES ('2025-03-07 12:00:00', 'INFO', 'Test message')
  ```

### Schema Tools

- `list_tables`: Get a list of all tables in the database
- `describe-table`: View schema information for a specific table
  ```
  describe-table logs
  ```

### Example Queries

#### Find Error Logs
```sql
SELECT timestamp, module, message FROM logs WHERE level = 'ERROR' LIMIT 20
```

#### Count Logs by Level
```sql
SELECT level, COUNT(*) as count FROM logs GROUP BY level ORDER BY count DESC
```

#### Find Most Active Modules
```sql
SELECT module, COUNT(*) as count FROM logs GROUP BY module ORDER BY count DESC LIMIT 10
```

#### Find Logs by Time Range
```sql
SELECT * FROM logs WHERE timestamp BETWEEN '2025-03-05 00:00:00' AND '2025-03-05 01:00:00' LIMIT 20
```

## Troubleshooting

If you encounter issues with the MCP server:

1. Make sure Docker is running
2. Check that the database file is correctly copied to the Docker volume
3. Verify the MCP server configuration in your `claude_desktop_config.json` 