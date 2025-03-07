# Log Analysis with SQLite

This project provides tools to create an SQLite database from compressed log files and interact with it programmatically.

## Install instructions

```bash
python3 -m venv venv
source venv/bin/activate
pip3 install -r requirements.txt
```

Place log files in the folder as .gz files, then run:
```bash
python3 create_log_db.py 
```

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

### `stack_traces` Table

- `id`: Unique identifier for each stack trace
- `log_id`: Reference to the log entry this stack trace belongs to
- `stack_trace`: Full stack trace text

### `parsing_errors` Table

- `id`: Unique identifier for each parsing error
- `line`: The line that couldn't be parsed
- `source_file`: Source log file
- `error_message`: Error message explaining why parsing failed
- `timestamp`: When the parsing error occurred

## Log Statistics

- Total log entries: 1,002,326
- Log level distribution:
  - INFO: 851,068
  - WARN: 13,890
  - ERROR: 1,141
  - DEBUG: 10

## Using the Database Programmatically

You can query the database directly using the `query_logs.py` script:

```bash
# View database information
python3 query_logs.py

# Run a custom query
python3 query_logs.py "SELECT * FROM logs WHERE level = 'ERROR' LIMIT 10"
```

The script outputs results in CSV format for easy parsing and further processing.
