# ScyllaDB MCP Server

A Model Context Protocol (MCP) server for ScyllaDB featuring a technical engineering advisor. Enables live A/B testing between DynamoDB and ScyllaDB with honest analysis about costs, performance, and architectural decisions.

## Features

- 🎭 **Technical Engineering Voice** - Direct analysis, no marketing fluff
- ☁️ **Multi-Cloud Support** - ScyllaDB Cloud, AWS DynamoDB, and local Docker
- 💰 **Advanced Cost Calculator** - Matches official ScyllaDB calculator with workload profiles
- 🚀 **Live A/B Testing** - Real-time performance comparison between platforms
- 🔍 **DynamoDB Code Analysis** - Pattern detection and migration assessment
- 📊 **Workload Templates** - Discord-scale, AdTech, FinTech, and more
- 🛠️ **Migration Complexity Assessment** - Honest effort estimates

## Supported Connections

### 1. ScyllaDB Cloud
Connect to managed ScyllaDB instances with full CQL and Alternator (DynamoDB API) support.

### 2. AWS DynamoDB
Native DynamoDB connections for live A/B testing and cost comparison.

### 3. Local Docker
Auto-provisions ScyllaDB containers for development and testing.

## Demo Applications

The MCP server includes several demo applications for real-world testing:

### Performance Benchmarks
- **`quick_demo.py`** - Connection speed comparison (ScyllaDB vs DynamoDB)
- **`simple_ab_demo.py`** - Side-by-side write performance test
- **`live_ab_test.py`** - Comprehensive performance comparison with metrics
- **`ycsb_benchmark.py`** - Industry-standard YCSB workload testing

### Cost Analysis Tools
- **`test_mcp_server.py`** - Test the advisor with real workload scenarios
- **Advanced Calculator** - Matches ScyllaDB's official pricing calculator
- **Workload Templates** - Pre-configured profiles for common applications

### DynamoDB Analysis
The server includes AWS's official DynamoDB sample application for analysis:
- **Source**: Based on AWS's DynamoDB online shop example
- **Location**: `test/dynamodb_online_shop_example.py`
- **Purpose**: Analyze real DynamoDB patterns from AWS's own code
- **Features**:
  - Query pattern inspection
  - Access pattern analysis
  - Hot partition detection
  - GSI proliferation identification
  - Migration complexity assessment

## Quick Start

### Prerequisites

- Python 3.8+ (3.11 recommended)
- Docker Desktop (for local testing)
- Claude Desktop with MCP support
- AWS credentials (for DynamoDB comparison)

### Installation

1. Clone this repository:
```bash
git clone https://github.com/scylladb/scylladb-mcp-server.git
cd scylladb-mcp-server
```

2. Run the setup script:
```bash
./setup.sh
```

3. Configure your environment:
```bash
# Copy the example environment file
cp .env.example .env

# Edit .env with your credentials:
# - AWS_ACCESS_KEY_ID
# - AWS_SECRET_ACCESS_KEY
# - SCYLLA_ALTERNATOR_ENDPOINT (for ScyllaDB Cloud)
```

4. Restart Claude Desktop

## Configuration

### Claude Desktop Configuration

The setup script automatically configures Claude Desktop. For manual configuration:

**macOS**: `~/Library/Application Support/Claude/claude_desktop_config.json`  
**Windows**: `%APPDATA%\Claude\claude_desktop_config.json`  
**Linux**: `~/.config/Claude/claude_desktop_config.json`

```json
{
  "mcpServers": {
    "scylladb": {
      "command": "python3",
      "args": ["/path/to/scylladb-mcp-server/src/scylladb_mcp_server.py"],
      "env": {
        "SCYLLA_CONNECTION_MODE": "alternator",
        "SCYLLA_ALTERNATOR_ENDPOINT": "http://your-endpoint:8000",
        "AWS_ACCESS_KEY_ID": "your-key",
        "AWS_SECRET_ACCESS_KEY": "your-secret"
      }
    }
  }
}
```

### Connection Modes

- **`docker`** - Local ScyllaDB container (default)
- **`cloud`** - ScyllaDB Cloud with CQL
- **`alternator`** - ScyllaDB or DynamoDB via Alternator API

### DynamoDB Configuration

To connect to AWS DynamoDB for A/B testing, add these environment variables to your configuration:

```json
{
  "mcpServers": {
    "scylladb": {
      "command": "python3",
      "args": ["/path/to/scylladb-mcp-server/src/scylladb_mcp_server.py"],
      "env": {
        "SCYLLA_CONNECTION_MODE": "alternator",
        "SCYLLA_ALTERNATOR_ENDPOINT": "http://your-scylla-endpoint:8000",
        "AWS_ACCESS_KEY_ID": "your-aws-key",
        "AWS_SECRET_ACCESS_KEY": "your-aws-secret",
        "AWS_REGION": "us-east-1",
        "DYNAMODB_ENDPOINT": ""  // Leave empty for real AWS DynamoDB
      }
    }
  }
}
```

For local DynamoDB testing, set `DYNAMODB_ENDPOINT` to your local DynamoDB URL (e.g., `http://localhost:8001`).

## Usage Examples

### Live A/B Testing
```
Compare performance between DynamoDB and ScyllaDB for 1000 write operations
```

### Cost Analysis with Real Workloads
```
Calculate costs for a FinTech platform processing 50,000 transactions/second
```

### Analyze Existing DynamoDB Code
```
Analyze the DynamoDB patterns in test/dynamodb_online_shop_example.py
```

### Migration Assessment
```python
Check if this DynamoDB code will work with ScyllaDB:

response = table.transact_write_items(
    TransactItems=[
        {'Put': {'TableName': 'Orders', 'Item': order}},
        {'Update': {'TableName': 'Inventory', 'Key': {'id': item_id}}}
    ]
)
```

## Running Demo Scripts

### Quick Performance Test
```bash
python3 src/quick_demo.py
```
Shows connection latency differences between platforms.

### A/B Write Performance
```bash
python3 src/simple_ab_demo.py
```
Demonstrates identical code running on both platforms with performance metrics.

### Full YCSB Benchmark
```bash
python3 src/ycsb_benchmark.py
```
Runs industry-standard benchmarks showing ~29% better P99 latency.

### Cost Calculator Test
```bash
python3 src/test_mcp_server.py
```
Tests the advisor with various workload scenarios and cost calculations.

## Available MCP Tools

- **connect** - Connect to ScyllaDB/DynamoDB (supports all modes)
- **costEstimate** - Advanced cost comparison with workload templates
- **checkMigration** - Validate DynamoDB code compatibility
- **analyzeWorkload** - Deep analysis of DynamoDB patterns
- **comparePerformance** - Live A/B testing between platforms
- **populateData** - Create test data with realistic patterns
- **analyzeDynamoDBModel** - Analyze AWS data models and access patterns

## Workload Templates

Pre-configured profiles for accurate cost analysis:

- **Discord-scale messaging** - 50K-200K ops/sec
- **AdTech RTB** - Sub-10ms latency requirements
- **Cybersecurity analytics** - Write-heavy, 100K-500K ops/sec
- **FinTech platform** - Payment processing scale
- **Growing e-commerce** - $100M+ revenue scale

## Project Structure

```
scylladb-mcp-server/
├── src/
│   ├── scylladb_mcp_server.py      # Main MCP server
│   ├── technical_advisor.py         # Engineering analysis engine
│   ├── advanced_cost_calculator.py  # Matches official calculator
│   ├── workload_templates.py        # Real-world profiles
│   ├── query_analyzer.py            # DynamoDB pattern analysis
│   ├── quick_demo.py               # Performance demos
│   ├── simple_ab_demo.py           # A/B testing demo
│   ├── live_ab_test.py             # Advanced comparison
│   ├── ycsb_benchmark.py           # YCSB implementation
│   └── requirements.txt            # Python dependencies
├── test/
│   └── dynamodb_online_shop_example.py  # AWS sample for analysis
├── docs/
│   └── SYSTEMS_DESIGN_CONTEXT.md   # Architecture philosophy
├── .env.example                    # Environment template
├── setup.sh                        # Automated setup
└── README.md                       # This file
```

## Key Results

Based on real benchmarks and customer data:

- **Cost Reduction**: 86-99% depending on workload
- **P99 Latency**: ~29% better under load
- **No Throttling**: Hardware limits only
- **Same API**: Drop-in replacement via Alternator

## Technical Philosophy

Every response is backed by:
- Actual benchmarks from scylladb.com
- Real customer workloads (Discord, Palo Alto Networks, Comcast)
- Transparent architecture explanations
- No marketing claims without data

## Contributing

PRs welcome for:
- Additional workload templates
- More DynamoDB pattern detection
- Real migration case studies
- Performance benchmark updates

## License

[TBD]

## Support

- ScyllaDB Docs: [docs.scylladb.com](https://docs.scylladb.com)
- ScyllaDB University: [university.scylladb.com](https://university.scylladb.com)
- DynamoDB Migration Guide: [scylladb.com/dynamodb](https://scylladb.com/dynamodb)