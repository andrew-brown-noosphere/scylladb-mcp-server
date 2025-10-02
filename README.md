# ScyllaDB MCP Server

A Model Context Protocol (MCP) server for ScyllaDB featuring a technical engineering advisor. Helps developers migrate from DynamoDB with honest analysis about costs, performance, and architectural decisions.

## Features

- 🎭 **Technical Engineering Voice** - Direct analysis, no marketing
- 🐳 **Zero-Friction Docker Setup** - Auto-manages local ScyllaDB container
- 💰 **Workload-Specific Cost Analysis** - Real pricing calculations
- 🚀 **Benchmark-Backed Performance Claims** - Data from scylladb.com
- 🔍 **DynamoDB Compatibility Checking** - Via Alternator API
- 📊 **Pattern Detection** - Identifies hot partitions, GSI proliferation
- 🛠️ **Migration Complexity Assessment** - Honest effort estimates

## Quick Start

### Prerequisites

- Python 3.8+
- Docker Desktop (running)
- Claude Desktop with MCP support

### Installation

1. Clone this repository:
```bash
git clone https://github.com/scylladb/scylladb-mcp-server.git
cd scylladb-mcp-server
```

2. Install Python dependencies:
```bash
# Create virtual environment (recommended)
python3 -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install requirements
pip install -r src/requirements.txt
```

3. Configure Claude Desktop by editing the config file:

**macOS**: `~/Library/Application Support/Claude/claude_desktop_config.json`  
**Windows**: `%APPDATA%\Claude\claude_desktop_config.json`  
**Linux**: `~/.config/Claude/claude_desktop_config.json`

Add the ScyllaDB MCP server configuration:

```json
{
  "mcpServers": {
    "scylladb": {
      "command": "python3",
      "args": ["/absolute/path/to/scylladb-mcp-server/src/scylladb_mcp_server.py"],
      "env": {
        "SCYLLA_IS_DOCKER": "true"
      }
    }
  }
}
```

4. Restart Claude Desktop

## Usage Examples

### Connect to ScyllaDB
```
Connect to ScyllaDB
```
Automatically provisions and connects to a local ScyllaDB Docker container.

### Cost Analysis
```
What would my DynamoDB workload cost on ScyllaDB?
- 50,000 reads/sec
- 10,000 writes/sec  
- 5TB storage
```

### Check Code Compatibility
```python
Check if this DynamoDB code will work with ScyllaDB:

table.query(
    IndexName='GSI1',
    KeyConditionExpression='GSI1PK = :pk',
    ExpressionAttributeValues={':pk': 'CATEGORY#featured'}
)
```

### Analyze Workload
```
Analyze my DynamoDB code in ./src for ScyllaDB migration
```

## Available MCP Tools

- **connect** - Connect to ScyllaDB (auto-provisions Docker if needed)
- **costEstimate** - Compare costs between DynamoDB and ScyllaDB
- **checkMigration** - Validate DynamoDB code compatibility
- **analyzeWorkload** - Deep analysis of DynamoDB patterns
- **comparePerformance** - Show benchmark comparisons
- **populateData** - Create test data with realistic patterns
- **analyzeDynamoDBModel** - Analyze AWS data models

## Project Structure

```
scylladb-mcp-server/
├── src/
│   ├── scylladb_mcp_server.py   # Main MCP server
│   ├── technical_advisor.py      # Technical analysis voice
│   ├── query_analyzer.py         # DynamoDB pattern analysis
│   ├── scylladb_advisor.py       # ScyllaDB-specific insights
│   └── requirements.txt          # Python dependencies
├── test/
│   └── dynamodb_online_shop_example.py  # AWS example code
├── docs/
│   └── SYSTEMS_DESIGN_CONTEXT.md  # Architecture philosophy
├── demo_scenarios.md             # Demo walkthrough scripts
└── README.md                     # This file
```

## Technical Notes

- **Pattern Detection**: Static code analysis identifies common anti-patterns
- **Cost Estimates**: Based on current AWS pricing and typical EC2 costs
- **Performance Data**: From published ScyllaDB benchmarks
- **Compatibility**: Uses Alternator (DynamoDB-compatible API)

## The Technical Philosophy

Every response provides engineering analysis:
- "Thread-per-core eliminates lock contention"
- "GSIs multiply write costs by (N+1)"
- "Hardware limits, not artificial throttling"

No marketing. Just technical reality.

## Contributing

PRs welcome for:
- Additional cost analysis patterns
- More pattern detection rules
- Real migration case studies
- Performance benchmark updates

## License

Apache 2.0

## Support

- ScyllaDB Docs: [docs.scylladb.com](https://docs.scylladb.com)
- ScyllaDB University: [university.scylladb.com](https://university.scylladb.com)