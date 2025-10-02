#!/usr/bin/env python3
"""Test ScyllaDB Cloud Alternator (DynamoDB API) connection."""

import boto3
import json

# Test both HTTP and HTTPS endpoints
endpoints = {
    "HTTP": "http://node-0.aws-us-east-1.a17dfa2542149ef7642e.clusters.scylla.cloud:8000",
    "HTTPS": "https://node-0.aws-us-east-1.a17dfa2542149ef7642e.clusters.scylla.cloud:8043"
}

for protocol, endpoint in endpoints.items():
    print(f"\nTesting {protocol} endpoint: {endpoint}")
    
    try:
        # Create DynamoDB client
        dynamodb = boto3.client(
            'dynamodb',
            endpoint_url=endpoint,
            region_name='us-east-1',  # ScyllaDB Cloud uses real region
            aws_access_key_id='None',
            aws_secret_access_key='None'
        )
        
        # Try to list tables
        response = dynamodb.list_tables()
        print(f"✅ Connected via {protocol}!")
        print(f"   Tables: {response.get('TableNames', [])}")
        
        # Try the resource interface too
        dynamodb_resource = boto3.resource(
            'dynamodb',
            endpoint_url=endpoint,
            region_name='us-east-1',
            aws_access_key_id='None',
            aws_secret_access_key='None'
        )
        
        # If no tables exist, this confirms we're connected
        print(f"   Connection successful - Alternator API is working!")
        
        # We found a working endpoint, let's use it
        if protocol == "HTTP":
            print(f"\n📌 Use this endpoint in your config: {endpoint}")
        
    except Exception as e:
        print(f"❌ Failed via {protocol}: {type(e).__name__}: {e}")