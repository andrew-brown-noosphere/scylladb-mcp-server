#!/bin/bash

# Developer Setup Script for ScyllaDB MCP Server
# Get up and running with local ScyllaDB development in under 2 minutes

set -e

echo "🚀 ScyllaDB MCP Quick Setup"
echo "=========================="

# Colors for output
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m' # No Color

# Check prerequisites
echo -e "\n${YELLOW}Checking prerequisites...${NC}"

# Python check
if ! python3 --version &> /dev/null; then
    echo -e "${RED}❌ Python 3 is required${NC}"
    echo "   Install from: https://www.python.org/downloads/"
    exit 1
else
    echo -e "${GREEN}✓ Python 3 found${NC}"
fi

# Docker check
if ! docker --version &> /dev/null; then
    echo -e "${RED}❌ Docker is required${NC}"
    echo "   Install Docker Desktop from: https://www.docker.com/products/docker-desktop/"
    exit 1
else
    echo -e "${GREEN}✓ Docker found${NC}"
fi

# Check if Docker daemon is running
if ! docker info &> /dev/null; then
    echo -e "${RED}❌ Docker daemon is not running${NC}"
    echo "   Please start Docker Desktop and try again"
    exit 1
else
    echo -e "${GREEN}✓ Docker daemon running${NC}"
fi

# Navigate to src directory
cd "$(dirname "$0")/src"

# Create virtual environment
echo -e "\n${YELLOW}Setting up Python environment...${NC}"
if [ ! -d "venv" ]; then
    python3 -m venv venv
    echo -e "${GREEN}✓ Virtual environment created${NC}"
else
    echo -e "${GREEN}✓ Virtual environment exists${NC}"
fi

# Activate virtual environment
source venv/bin/activate

# Install dependencies
echo -e "\n${YELLOW}Installing dependencies...${NC}"
pip install --quiet --upgrade pip
pip install --quiet -r requirements.txt

# Check if mcp is installed
if ! pip show mcp &> /dev/null; then
    echo -e "${YELLOW}Installing MCP from GitHub...${NC}"
    pip install --quiet git+https://github.com/anthropics/mcp-python-sdk.git
fi

echo -e "${GREEN}✓ All dependencies installed${NC}"

# Create Claude config
echo -e "\n${YELLOW}Creating Claude Desktop configuration...${NC}"
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"
PYTHON_PATH="${SCRIPT_DIR}/src/venv/bin/python"
SERVER_PATH="${SCRIPT_DIR}/src/scylladb_mcp_server.py"

cat > claude-config.json <<EOF
{
  "mcpServers": {
    "scylladb": {
      "command": "${PYTHON_PATH}",
      "args": ["${SERVER_PATH}"],
      "env": {
        "SCYLLA_IS_DOCKER": "true"
      }
    }
  }
}
EOF

echo -e "${GREEN}✓ Configuration created${NC}"

# Test the server
echo -e "\n${YELLOW}Testing MCP server...${NC}"
if timeout 5 python scylladb_mcp_server.py 2>&1 | grep -q "scylladb-mcp"; then
    echo -e "${GREEN}✓ Server starts successfully${NC}"
else
    echo -e "${YELLOW}⚠ Could not verify server (this might be normal)${NC}"
fi

# Pre-pull ScyllaDB Docker image
echo -e "\n${YELLOW}Pre-pulling ScyllaDB Docker image...${NC}"
if docker pull scylladb/scylla:latest > /dev/null 2>&1; then
    echo -e "${GREEN}✓ ScyllaDB image ready${NC}"
else
    echo -e "${YELLOW}⚠ Could not pull image (will download on first use)${NC}"
fi

# Show next steps
echo -e "\n${GREEN}✅ Setup Complete!${NC}"
echo -e "\n${YELLOW}Next Steps:${NC}"
echo "1. Copy the configuration to Claude Desktop:"

if [[ "$OSTYPE" == "darwin"* ]]; then
    echo "   cp claude-config.json ~/Library/Application\\ Support/Claude/claude_desktop_config.json"
elif [[ "$OSTYPE" == "msys" || "$OSTYPE" == "win32" ]]; then
    echo "   copy claude-config.json %APPDATA%\\Claude\\claude_desktop_config.json"
else
    echo "   cp claude-config.json ~/.config/Claude/claude_desktop_config.json"
fi

echo ""
echo "2. Restart Claude Desktop completely (Quit and reopen)"
echo ""
echo -e "${YELLOW}Demo Examples:${NC}"
echo "• 'Compare DynamoDB vs ScyllaDB costs for 100k ops/sec'"
echo "• 'Show me ScyllaDB performance vs DynamoDB'"
echo "• 'Check if my DynamoDB code works with ScyllaDB'"
echo ""
echo -e "${GREEN}Happy demoing! 🚀${NC}"