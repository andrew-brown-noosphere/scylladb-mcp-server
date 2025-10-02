#!/usr/bin/env python3
"""Test ScyllaDB Cloud connection."""

import os
import json
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider

# Load config from .claude directory
config_path = '.claude/config.json'
if os.path.exists(config_path):
    with open(config_path, 'r') as f:
        config = json.load(f)
        env = config['mcpServers']['scylladb']['env']
        
        # Extract credentials
        host = env.get('SCYLLA_HOST', env.get('// SCYLLA_HOST'))
        username = env.get('SCYLLA_USERNAME', env.get('// SCYLLA_USERNAME'))
        password = env.get('SCYLLA_PASSWORD', env.get('// SCYLLA_PASSWORD'))
        datacenter = env.get('SCYLLA_DC', env.get('// SCYLLA_DC', 'AWS_US_EAST_1'))
else:
    print(f"❌ Config file not found: {config_path}")
    exit(1)

print(f"🔧 Testing ScyllaDB Cloud connection...")
print(f"   Host: {host}")
print(f"   Username: {username}")
print(f"   Datacenter: {datacenter}")
print()

try:
    # Create connection
    auth_provider = PlainTextAuthProvider(username=username, password=password)
    cluster = Cluster(
        [host],
        auth_provider=auth_provider,
        port=9042
    )
    
    session = cluster.connect()
    
    # Test query
    result = session.execute("SELECT cluster_name, release_version FROM system.local")
    for row in result:
        print(f"✅ Connected to ScyllaDB Cloud!")
        print(f"   Cluster: {row.cluster_name}")
        print(f"   Version: {row.release_version}")
    
    # List keyspaces
    print(f"\n📊 Available keyspaces:")
    result = session.execute("SELECT keyspace_name FROM system_schema.keyspaces")
    for row in result:
        print(f"   - {row.keyspace_name}")
    
    cluster.shutdown()
    print(f"\n✅ Connection test successful!")
    
except Exception as e:
    print(f"\n❌ Connection failed: {e}")
    print(f"\nTroubleshooting:")
    print(f"1. Check your credentials in .claude/config.json")
    print(f"2. Ensure your IP is whitelisted in ScyllaDB Cloud")
    print(f"3. Verify the cluster is running")