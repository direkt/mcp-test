#!/bin/bash

# Setup script for MCP SQLite server

# Check if the logs.db file exists
if [ ! -f "logs.db" ]; then
    echo "Error: logs.db file not found. Please run create_log_db.py first."
    exit 1
fi

echo "Found logs.db database file."

# Check if Docker is installed
if ! command -v docker &> /dev/null; then
    echo "Docker is not installed. Please install Docker to continue."
    exit 1
fi

echo "Docker is installed."

# Build the MCP SQLite server Docker image
echo "Building MCP SQLite server Docker image..."
echo "This requires cloning the MCP servers repository first."

# Clone the repository if it doesn't exist
if [ ! -d "servers" ]; then
    echo "Cloning MCP servers repository..."
    git clone https://github.com/modelcontextprotocol/servers.git
    if [ $? -ne 0 ]; then
        echo "Failed to clone the repository."
        exit 1
    fi
fi

# Build the Docker image
echo "Building Docker image..."
cd servers/src/sqlite
docker build -t mcp/sqlite .
cd ../../..

if [ $? -ne 0 ]; then
    echo "Failed to build the Docker image."
    exit 1
fi

echo "Docker image built successfully."

# Create a volume for the database
echo "Creating Docker volume for the database..."
docker volume create mcp-logs

# Copy the database to the volume
echo "Copying logs.db to Docker volume..."
# Create a temporary container to copy the file
docker run --rm -v mcp-logs:/mcp -v $(pwd):/source alpine sh -c "cp /source/logs.db /mcp/"

echo "Database copied to Docker volume."

# Print instructions for running the server
echo ""
echo "=== MCP SQLite Server Setup Complete ==="
echo ""
echo "To run the MCP SQLite server, add the following to your claude_desktop_config.json:"
echo ""
echo '"mcpServers": {'
echo '  "sqlite": {'
echo '    "command": "docker",'
echo '    "args": ['
echo '      "run",'
echo '      "--rm",'
echo '      "-i",'
echo '      "-v",'
echo '      "mcp-logs:/mcp",'
echo '      "mcp/sqlite",'
echo '      "--db-path",'
echo '      "/mcp/logs.db"'
echo '    ]'
echo '  }'
echo '}'
echo ""
echo "You can then use the MCP SQLite server in Claude Desktop to interact with your logs database."
echo "Available tools include: read_query, write_query, list_tables, describe-table, etc."
echo ""
echo "Example query: SELECT * FROM logs LIMIT 10" 