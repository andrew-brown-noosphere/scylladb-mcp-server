#!/usr/bin/env python3
"""Test AWS DynamoDB connection."""

import boto3
import os
from botocore.exceptions import ClientError

# You'll need to set these
AWS_ACCESS_KEY_ID = os.getenv('AWS_ACCESS_KEY_ID', 'your-access-key')
AWS_SECRET_ACCESS_KEY = os.getenv('AWS_SECRET_ACCESS_KEY', 'your-secret-key')
AWS_REGION = os.getenv('AWS_REGION', 'us-east-1')

print("🔧 Testing AWS DynamoDB connection...")
print(f"   Region: {AWS_REGION}")

if AWS_ACCESS_KEY_ID == 'your-access-key':
    print("\n❌ Please set your AWS credentials:")
    print("   export AWS_ACCESS_KEY_ID='your-actual-key'")
    print("   export AWS_SECRET_ACCESS_KEY='your-actual-secret'")
    exit(1)

try:
    # Create DynamoDB client
    dynamodb = boto3.client(
        'dynamodb',
        region_name=AWS_REGION,
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY
    )
    
    # List tables (might be empty)
    response = dynamodb.list_tables()
    print(f"\n✅ Connected to AWS DynamoDB!")
    print(f"   Existing tables: {response.get('TableNames', [])}")
    
    # Create a test table
    table_name = 'scylla-demo-test'
    
    try:
        # Check if table exists
        dynamodb.describe_table(TableName=table_name)
        print(f"\n   Table '{table_name}' already exists")
    except ClientError as e:
        if e.response['Error']['Code'] == 'ResourceNotFoundException':
            print(f"\n   Creating test table '{table_name}'...")
            dynamodb.create_table(
                TableName=table_name,
                KeySchema=[
                    {'AttributeName': 'id', 'KeyType': 'HASH'}
                ],
                AttributeDefinitions=[
                    {'AttributeName': 'id', 'AttributeType': 'S'}
                ],
                BillingMode='PAY_PER_REQUEST'  # On-demand billing
            )
            print(f"   ✅ Table created!")
    
    print("\n📋 Next steps:")
    print("1. Add these credentials to .claude/config.json")
    print("2. Now you can do A/B testing between DynamoDB and ScyllaDB!")
    
except Exception as e:
    print(f"\n❌ Connection failed: {e}")
    print("\nTroubleshooting:")
    print("1. Check your AWS credentials")
    print("2. Ensure your IAM user has DynamoDB permissions")
    print("3. Try a different region if us-east-1 doesn't work")