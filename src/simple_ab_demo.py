#!/usr/bin/env python3
"""Simple A/B Demo: Same code, different endpoints"""

import boto3
import time
import os

# Credentials
import os
AWS_KEY = os.getenv('AWS_ACCESS_KEY_ID', 'your-aws-key')
AWS_SECRET = os.getenv('AWS_SECRET_ACCESS_KEY', 'your-aws-secret')

print("🔄 Live A/B Test: AWS DynamoDB vs ScyllaDB\n")

# 1. AWS DynamoDB Client
dynamodb_aws = boto3.resource('dynamodb',
    region_name='us-east-1',
    aws_access_key_id=AWS_KEY,
    aws_secret_access_key=AWS_SECRET
)

# 2. ScyllaDB Alternator Client (exact same API!)
dynamodb_scylla = boto3.resource('dynamodb',
    endpoint_url='http://node-0.aws-us-east-1.a17dfa2542149ef7642e.clusters.scylla.cloud:8000',
    region_name='us-east-1',
    aws_access_key_id='fake',  # Alternator doesn't check these
    aws_secret_access_key='fake'
)

# Create a simple test table
table_name = 'ab-test-demo'

print("📊 Creating test tables...")
for db, name in [(dynamodb_aws, 'AWS'), (dynamodb_scylla, 'ScyllaDB')]:
    try:
        table = db.create_table(
            TableName=table_name,
            KeySchema=[{'AttributeName': 'id', 'KeyType': 'HASH'}],
            AttributeDefinitions=[{'AttributeName': 'id', 'AttributeType': 'S'}],
            BillingMode='PAY_PER_REQUEST'
        )
        print(f"✅ Created on {name}")
    except:
        print(f"ℹ️  Table exists on {name}")

print("\n⏱️  Performance Test: 100 writes")
print("-" * 40)

# Test writes
for db, name in [(dynamodb_aws, 'AWS DynamoDB'), (dynamodb_scylla, 'ScyllaDB')]:
    table = db.Table(table_name)
    
    start = time.time()
    for i in range(100):
        table.put_item(Item={
            'id': f'{name}-{i}',
            'data': 'x' * 1024,  # 1KB
            'timestamp': int(time.time())
        })
    elapsed = time.time() - start
    
    print(f"{name:15} {elapsed:.2f}s ({100/elapsed:.0f} ops/sec)")

print("\n💡 Insight: Same code, same API, different performance!")
print("   ScyllaDB is typically 3-10x faster with lower latency")