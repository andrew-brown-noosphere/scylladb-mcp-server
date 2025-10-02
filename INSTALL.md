# ScyllaDB MCP Server - Quick Install Guide

## 1. Prerequisites
- Claude Desktop with MCP support
- Python 3.11+ installed
- Docker Desktop (for local demos)

## 2. Quick Install

### Option A: One-Line Install (Recommended)
```bash
curl -sSL https://raw.githubusercontent.com/scylladb/scylladb-mcp-server/main/install.sh | bash
```

### Option B: Manual Install
```bash
# Clone the repository
git clone https://github.com/scylladb/scylladb-mcp-server.git
cd scylladb-mcp-server

# Run setup
./setup.sh
```

## 3. Configure Claude Desktop

The installer will automatically add the configuration to Claude Desktop. If needed, manually add to:

**macOS**: `~/Library/Application Support/Claude/claude_desktop_config.json`
**Windows**: `%APPDATA%\Claude\claude_desktop_config.json`

```json
{
  "mcpServers": {
    "scylladb": {
      "command": "python3",
      "args": ["<PATH_TO_REPO>/src/scylladb_mcp_server.py"],
      "env": {
        "SCYLLA_CONNECTION_MODE": "docker"
      }
    }
  }
}
```

## 4. Test the Installation

1. Restart Claude Desktop
2. Open a new conversation
3. Type: "Connect to ScyllaDB and analyze my workload"
4. You should see the MCP tools available

## 5. Demo Scripts

For live demos, run these in terminal:
```bash
# Quick performance comparison
python3 src/quick_demo.py

# Full benchmark
python3 src/ycsb_benchmark.py

# Cost analysis demo
python3 src/test_mcp_server.py
```

## Troubleshooting
- If MCP tools don't appear, check Claude Desktop logs
- Ensure Python 3.11+ is in PATH
- Docker Desktop must be running for local demos