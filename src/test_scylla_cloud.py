#!/usr/bin/env python3
"""Test ScyllaDB Cloud connection with Python 3.13 compatibility."""

import os
import sys
import json

# Load config from .claude directory
config_path = '../.claude/config.json'
if os.path.exists(config_path):
    with open(config_path, 'r') as f:
        config = json.load(f)
        env = config['mcpServers']['scylladb']['env']
        
        # Set environment variables
        for key, value in env.items():
            if not key.startswith('//'):
                os.environ[key] = value

# Must set BEFORE importing cassandra
os.environ.setdefault('CASS_DRIVER_NO_CYTHON', '1')

# Import cassandra with specific connection class
from cassandra.cluster import Cluster, ExecutionProfile, EXEC_PROFILE_DEFAULT
from cassandra.auth import PlainTextAuthProvider
from cassandra.policies import WhiteListRoundRobinPolicy

print(f"🔧 Testing ScyllaDB Cloud connection...")
print(f"   Host: {os.getenv('SCYLLA_HOST', 'Not set')}")
print(f"   Username: {os.getenv('SCYLLA_USERNAME', 'Not set')}")
print(f"   DC: {os.getenv('SCYLLA_DC', 'Not set')}")
print()

if os.getenv('SCYLLA_IS_DOCKER', 'true').lower() != 'true':
    try:
        # Cloud connection
        host = os.getenv('SCYLLA_HOST')
        username = os.getenv('SCYLLA_USERNAME')
        password = os.getenv('SCYLLA_PASSWORD')
        datacenter = os.getenv('SCYLLA_DC', 'AWS_US_EAST_1')
        
        auth_provider = PlainTextAuthProvider(username=username, password=password)
        
        # Create profile with specific policies
        profile = ExecutionProfile(
            load_balancing_policy=WhiteListRoundRobinPolicy([host])
        )
        
        # Try with SSL first
        from ssl import CERT_REQUIRED, PROTOCOL_TLS, SSLContext
        ssl_context = SSLContext(PROTOCOL_TLS)
        ssl_context.check_hostname = False
        ssl_context.verify_mode = CERT_REQUIRED
        
        cluster = Cluster(
            contact_points=[host],
            auth_provider=auth_provider,
            protocol_version=4,
            port=9042,
            ssl_context=ssl_context,
            execution_profiles={EXEC_PROFILE_DEFAULT: profile}
        )
        
        session = cluster.connect()
        
        # Test query
        result = session.execute("SELECT cluster_name, release_version FROM system.local")
        for row in result:
            print(f"✅ Connected to ScyllaDB Cloud!")
            print(f"   Cluster: {row.cluster_name}")
            print(f"   Version: {row.release_version}")
        
        cluster.shutdown()
        
    except Exception as e:
        print(f"❌ Connection failed: {e}")
        print(f"\nError type: {type(e).__name__}")
        import traceback
        traceback.print_exc()
else:
    print("📦 Docker mode is enabled. Switch SCYLLA_IS_DOCKER to 'false' to test cloud.")