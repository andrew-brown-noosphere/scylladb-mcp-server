#!/bin/bash
# Skip AWS's terrible UI - use DynamoDB Local!

echo "🚀 Setting up DynamoDB Local (no AWS account needed!)"

# Pull and run DynamoDB Local
docker run -d \
  --name dynamodb-local \
  -p 8001:8000 \
  amazon/dynamodb-local

echo "✅ DynamoDB Local running on port 8001"
echo ""
echo "Use these fake credentials (DynamoDB Local doesn't check them):"
echo "AWS_ACCESS_KEY_ID=fake"
echo "AWS_SECRET_ACCESS_KEY=fake"
echo "AWS_REGION=us-east-1"
echo ""
echo "Endpoint: http://localhost:8001"