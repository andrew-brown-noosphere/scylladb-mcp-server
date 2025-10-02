#!/usr/bin/env python3
"""Test all ScyllaDB Cloud nodes."""

import os
import json
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider

# Load config
with open('../.claude/config.json', 'r') as f:
    config = json.load(f)
    env = config['mcpServers']['scylladb']['env']
    for k, v in env.items():
        if not k.startswith('//'):
            os.environ[k] = v

# All three nodes
nodes = [
    "node-0.aws-us-east-1.a17dfa2542149ef7642e.clusters.scylla.cloud",
    "node-1.aws-us-east-1.a17dfa2542149ef7642e.clusters.scylla.cloud", 
    "node-2.aws-us-east-1.a17dfa2542149ef7642e.clusters.scylla.cloud"
]

username = "scylla"
password = "mfjVd0No5c3Opik"

print("Testing all ScyllaDB Cloud nodes...")

auth_provider = PlainTextAuthProvider(username=username, password=password)

try:
    # Try with all nodes
    print(f"\nTrying all nodes together...")
    cluster = Cluster(
        contact_points=nodes,
        auth_provider=auth_provider,
        port=9042
    )
    session = cluster.connect()
    print("✅ Connected successfully!")
    
    result = session.execute("SELECT cluster_name, release_version FROM system.local")
    for row in result:
        print(f"   Cluster: {row.cluster_name}")
        print(f"   Version: {row.release_version}")
    
    cluster.shutdown()
    
except Exception as e:
    print(f"❌ Failed: {e}")
    
    # Try each node individually
    for node in nodes:
        print(f"\nTrying {node}...")
        try:
            cluster = Cluster([node], auth_provider=auth_provider, port=9042)
            session = cluster.connect()
            print(f"✅ Connected to {node}")
            cluster.shutdown()
            break
        except Exception as e:
            print(f"❌ Failed: {e}")

print("\nWhat does the Python tab in ScyllaDB Cloud show for connection code?")