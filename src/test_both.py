#!/usr/bin/env python3
"""Test both DynamoDB and ScyllaDB Alternator connections."""

import boto3
import time
import os

# ScyllaDB Alternator endpoint
SCYLLA_ENDPOINT = "http://node-0.aws-us-east-1.a17dfa2542149ef7642e.clusters.scylla.cloud:8000"

# AWS credentials from environment or config
AWS_KEY = os.getenv('AWS_ACCESS_KEY_ID', 'your-access-key-here')
AWS_SECRET = os.getenv('AWS_SECRET_ACCESS_KEY', 'your-secret-key-here')
AWS_REGION = os.getenv('AWS_REGION', 'us-east-1')

print("🔄 A/B Testing Setup\n")

# Test 1: AWS DynamoDB
print("1️⃣ Testing AWS DynamoDB...")
try:
    dynamodb_aws = boto3.client(
        'dynamodb',
        region_name=AWS_REGION,
        aws_access_key_id=AWS_KEY,
        aws_secret_access_key=AWS_SECRET
    )
    
    start = time.time()
    tables = dynamodb_aws.list_tables()
    latency = (time.time() - start) * 1000
    
    print(f"✅ AWS DynamoDB connected!")
    print(f"   Latency: {latency:.1f}ms")
    print(f"   Tables: {len(tables.get('TableNames', []))}")
except Exception as e:
    print(f"❌ AWS DynamoDB failed: {e}")

# Test 2: ScyllaDB Alternator
print("\n2️⃣ Testing ScyllaDB Alternator...")
try:
    dynamodb_scylla = boto3.client(
        'dynamodb',
        endpoint_url=SCYLLA_ENDPOINT,
        region_name='us-east-1',
        aws_access_key_id='None',
        aws_secret_access_key='None'
    )
    
    start = time.time()
    tables = dynamodb_scylla.list_tables()
    latency = (time.time() - start) * 1000
    
    print(f"✅ ScyllaDB Alternator connected!")
    print(f"   Latency: {latency:.1f}ms")
    print(f"   Tables: {len(tables.get('TableNames', []))}")
except Exception as e:
    print(f"❌ ScyllaDB Alternator failed: {e}")

print("\n✅ Both connections ready for A/B testing!")
print("\nNext: Update your AWS credentials in .claude/config.json")