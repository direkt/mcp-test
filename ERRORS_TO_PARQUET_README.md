# Errors to Parquet Exporter

This script extracts error data from a SQLite database and exports it to Parquet files for further analysis.

## Features

- Extracts error data from multiple tables:
  - `parsing_errors`: Parsing-related errors
  - `logs`: Error and critical level log entries
  - `stack_traces`: Stack traces associated with error logs
- Exports data to Parquet format for efficient storage and analysis
- Supports filtering by error type and limiting the number of records
- Provides detailed logging and summary information

## Requirements

- Python 3.6+
- Required packages (see requirements.txt):
  - pandas
  - pyarrow (for Parquet support)
  - sqlite3 (standard library)

## Installation

1. Ensure you have Python 3.6 or higher installed
2. Install required packages:
   ```
   pip install -r requirements.txt
   ```

## Usage

Basic usage:

```bash
python errors_to_parquet.py
```

This will export all error data from the default `logs.db` database to separate Parquet files.

### Command-line Options

- `--db PATH`: Specify the path to the SQLite database (default: logs.db)
- `--output PATH`: Specify the output file path (default: auto-generated with timestamp)
- `--limit N`: Limit the number of records exported per table
- `--type TYPE`: Specify the type of errors to export:
  - `parsing`: Only export parsing errors
  - `logs`: Only export error logs
  - `stack_traces`: Only export stack traces
  - `all`: Export all error types (default)

### Examples

Export only error logs:
```bash
python errors_to_parquet.py --type logs
```

Export a limited number of records:
```bash
python errors_to_parquet.py --limit 100
```

Specify a custom output file (only works when exporting a single type):
```bash
python errors_to_parquet.py --type logs --output my_error_logs.parquet
```

Use a different database file:
```bash
python errors_to_parquet.py --db my_logs.db
```

## Output

The script generates Parquet files with the following naming convention:
- `parsing_errors_YYYYMMDD_HHMMSS.parquet`: Parsing errors
- `error_logs_YYYYMMDD_HHMMSS.parquet`: Error logs
- `stack_traces_YYYYMMDD_HHMMSS.parquet`: Stack traces

Each file contains all columns from the corresponding database table.

## Logging

The script logs its operations to both the console and a log file (`errors_to_parquet.log`). 