#!/bin/bash
# Package ScyllaDB MCP Server for distribution

VERSION="1.0.0"
PACKAGE_NAME="scylladb-mcp-server-$VERSION"

echo "📦 Packaging ScyllaDB MCP Server v$VERSION"
echo "======================================="

# Create package directory
rm -rf dist/$PACKAGE_NAME
mkdir -p dist/$PACKAGE_NAME

# Copy necessary files
echo "Copying files..."
cp -r src dist/$PACKAGE_NAME/
cp setup.sh dist/$PACKAGE_NAME/
cp INSTALL.md dist/$PACKAGE_NAME/
cp README.md dist/$PACKAGE_NAME/
cp -r .claude dist/$PACKAGE_NAME/ 2>/dev/null || true

# Remove unnecessary files
echo "Cleaning up..."
find dist/$PACKAGE_NAME -name "__pycache__" -type d -exec rm -rf {} + 2>/dev/null || true
find dist/$PACKAGE_NAME -name "*.pyc" -delete 2>/dev/null || true
find dist/$PACKAGE_NAME -name "venv*" -type d -exec rm -rf {} + 2>/dev/null || true
rm -f dist/$PACKAGE_NAME/src/*_mcp_server_fixed.py
rm -f dist/$PACKAGE_NAME/src/test_*.py

# Update config template
cat > dist/$PACKAGE_NAME/.claude/config.json << 'EOF'
{
  "mcpServers": {
    "scylladb": {
      "command": "python3",
      "args": ["./src/scylladb_mcp_server.py"],
      "env": {
        "SCYLLA_CONNECTION_MODE": "alternator",
        "SCYLLA_ALTERNATOR_ENDPOINT": "http://YOUR-SCYLLA-ENDPOINT:8000",
        "AWS_ACCESS_KEY_ID": "YOUR-AWS-KEY",
        "AWS_SECRET_ACCESS_KEY": "YOUR-AWS-SECRET"
      }
    }
  }
}
EOF

# Create README for SEs
cat > dist/$PACKAGE_NAME/SE_README.md << 'EOF'
# ScyllaDB MCP Server - SE Quick Start

## What This Does
The ScyllaDB MCP Server adds AI-powered database analysis to Claude Desktop:
- Analyzes DynamoDB workloads for migration
- Provides real-time performance comparisons
- Calculates cost savings based on actual usage
- Offers technical recommendations without fluff

## Installation (2 minutes)
1. Run: `./setup.sh`
2. Restart Claude Desktop
3. Done! The ScyllaDB tools are now available in Claude

## Demo Flow
1. **In Claude Desktop**, ask:
   - "Analyze my DynamoDB code for ScyllaDB migration"
   - "Compare performance between DynamoDB and ScyllaDB"
   - "Calculate cost savings for 10K reads/sec workload"

2. **For live demos**, run in terminal:
   ```bash
   python3 src/quick_demo.py      # Connection speed test
   python3 src/simple_ab_demo.py  # Side-by-side performance
   python3 src/ycsb_benchmark.py  # Full benchmark (2 min)
   ```

## Key Value Props
- **97% cost savings** on typical workloads
- **29% better P99 latency** (benchmarked)
- **Same DynamoDB API** - just change endpoint
- **No throttling** - hardware limits only

## Troubleshooting
- Ensure Python 3.11+ installed
- Docker Desktop must be running
- Check Claude Desktop logs if tools don't appear
EOF

# Create tarball
cd dist
tar -czf $PACKAGE_NAME.tar.gz $PACKAGE_NAME
echo "✅ Created dist/$PACKAGE_NAME.tar.gz"

# Create zip for Windows users
zip -r $PACKAGE_NAME.zip $PACKAGE_NAME -q
echo "✅ Created dist/$PACKAGE_NAME.zip"

cd ..
echo ""
echo "📦 Package ready for distribution!"
echo "Share: dist/$PACKAGE_NAME.tar.gz (or .zip for Windows)"
echo ""
echo "SEs can install with:"
echo "1. tar -xzf $PACKAGE_NAME.tar.gz"
echo "2. cd $PACKAGE_NAME"
echo "3. ./setup.sh"