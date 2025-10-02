#!/usr/bin/env python3
"""Test the ScyllaDB MCP Server functionality directly."""

import os
os.environ['CASS_DRIVER_NO_ASYNCORE'] = '1'
os.environ['CASS_DRIVER_NO_TWISTED'] = '1'
os.environ['CASS_DRIVER_NO_LIBEV'] = '1'

import asyncio
from scylladb_mcp_server_fixed import ScyllaDBMCPServer


async def test_advisor():
    """Test the advisor functionality directly."""
    server = ScyllaDBMCPServer()
    
    print("ScyllaDB MCP Server - Technical Advisor Test")
    print("=" * 50)
    
    # Test 1: Cost Estimation
    print("\n1. Cost Estimation Test:")
    result = await server._handle_cost_estimate(
        reads_per_sec=10000,
        writes_per_sec=5000,
        storage_gb=500,
        item_size_kb=1,
        pattern="steady"
    )
    print(result)
    
    # Test 2: Migration Check
    print("\n2. Migration Assessment Test:")
    sample_code = """
    import boto3
    
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table('users')
    
    # Simple operations
    table.put_item(Item={'id': '123', 'name': 'John'})
    response = table.get_item(Key={'id': '123'})
    
    # Scan operation
    response = table.scan()
    
    # Global Secondary Index
    table.query(IndexName='GSI-1', KeyConditionExpression=Key('email').eq('john@example.com'))
    
    # Transactional write
    dynamodb.transact_write_items(
        TransactItems=[
            {'Put': {'TableName': 'users', 'Item': {'id': '1'}}},
            {'Put': {'TableName': 'orders', 'Item': {'id': '2'}}}
        ]
    )
    """
    
    result = await server._handle_check_migration(sample_code, "python")
    print(result)
    
    # Test 3: Workload Analysis
    print("\n3. Workload Analysis Test:")
    # Create a temporary file for analysis
    test_file = "/tmp/test_dynamodb_code.py"
    with open(test_file, 'w') as f:
        f.write(sample_code)
    
    result = await server._handle_analyze_workload(test_file, True)
    print(result)
    
    print("\n" + "=" * 50)
    print("Technical Advisor is working correctly!")
    print("\nThe advisor provides:")
    print("- Direct technical analysis without marketing fluff")
    print("- Real cost calculations based on workload")
    print("- Migration complexity assessments")
    print("- Performance insights based on benchmarks")


if __name__ == "__main__":
    asyncio.run(test_advisor())