# ScyllaDB MCP Server Developer Setup Guide

This guide helps developers quickly set up the ScyllaDB MCP server for local development and testing.

## Prerequisites

- Python 3.8 or higher
- Docker Desktop installed and running
- Claude Desktop application

## Quick Setup (5 minutes)

### 1. Clone and Install

```bash
cd /Users/andrewbrown/Sites/scylladb-mcp-server
cd src

# Create virtual environment
python3 -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt
```

### 2. Configure Claude

You have three options for configuration:

#### Option A: Project-specific `.claude` directory (Recommended)
Create `.claude/config.json` in the project root:

```bash
cp .claude/config.example.json .claude/config.json
# Edit .claude/config.json with your credentials
```

#### Option B: Claude Desktop global config
**macOS**: `~/Library/Application Support/Claude/claude_desktop_config.json`

```json
{
  "mcpServers": {
    "scylladb": {
      "command": "/Users/andrewbrown/Sites/scylladb-mcp-server/src/venv/bin/python",
      "args": ["/Users/andrewbrown/Sites/scylladb-mcp-server/src/scylladb_mcp_server.py"],
      "env": {
        "SCYLLA_IS_DOCKER": "true"
      }
    }
  }
}
```

#### Option C: Environment variables
Create `src/.env` file with your credentials

### 3. Restart Claude Desktop

After saving the configuration, completely quit and restart Claude Desktop.

## What This Gives You

As a developer, the MCP server provides:

1. **Zero-Config Local Development**: Automatically starts a local ScyllaDB container
2. **Technical Documentation**: Engineering-focused explanations of ScyllaDB internals
3. **DynamoDB Migration Path**: Test your DynamoDB code with ScyllaDB's Alternator API
4. **Cost Planning**: Understand infrastructure costs before going to production
5. **Performance Testing**: Real benchmarks to validate your architecture choices

## Getting Started - Example Queries

### 1. Cost Analysis for Your Workload
```
"What would this DynamoDB workload cost on ScyllaDB?
- 100,000 reads/sec
- 20,000 writes/sec
- 10TB storage
- Bursty traffic patterns"
```

### 2. Migration Assessment
```
"Check if this DynamoDB code works with ScyllaDB:
boto3.resource('dynamodb').Table('users').put_item(
    Item={'user_id': '123', 'data': 'test'}
)"
```

### 3. Performance Comparison
```
"Compare read performance between DynamoDB and ScyllaDB"
```

### 4. Architecture Discussion
```
"Explain ScyllaDB's shard-per-core architecture vs DynamoDB"
```

## Docker Container Details

The MCP server automatically manages a Docker container named `scylladb-mcp` with:
- CQL port: 9042
- Alternator (DynamoDB API) port: 8000
- Auto-starts if not running
- Persists between demos

## Troubleshooting

### MCP Server Not Appearing
1. Check Python path in Claude config is absolute
2. Verify virtual environment is activated
3. Test manually: `python scylladb_mcp_server.py`

### Docker Issues
```bash
# Check if container is running
docker ps | grep scylladb-mcp

# View logs
docker logs scylladb-mcp

# Restart container
docker restart scylladb-mcp
```

### Connection Errors
- Ensure Docker Desktop is running
- Check ports 9042 and 8000 are free
- Try removing and recreating container:
  ```bash
  docker stop scylladb-mcp
  docker rm scylladb-mcp
  ```

## Advanced Configuration

### Using ScyllaDB Cloud
Create a `.env` file in the src directory:
```env
SCYLLA_IS_DOCKER=false
SCYLLA_HOST=your-cluster.scylladb.com
SCYLLA_USERNAME=your-username
SCYLLA_PASSWORD=your-password
```

### Custom Docker Settings
```env
SCYLLA_CONTAINER_NAME=my-scylla
SCYLLA_CQL_PORT=9042
SCYLLA_ALTERNATOR_PORT=8000
```

## Key Benefits

1. **Mechanical Sympathy**: Thread-per-core, no locks, hardware-aware
2. **No Artificial Limits**: Use all your hardware, no throttling
3. **Real Benchmarks**: P99 latencies in single-digit milliseconds
4. **Cost Efficiency**: 5-10x lower TCO than DynamoDB
5. **Drop-in Compatible**: Alternator API for easy migration

## Support

- ScyllaDB Docs: https://docs.scylladb.com
- GitHub Issues: https://github.com/scylladb/scylladb-mcp/issues
- Internal Slack: #scylladb-mcp