#!/usr/bin/env python3
"""Simple ScyllaDB Cloud connection test."""

import os
import json

# Load config
with open('../.claude/config.json', 'r') as f:
    config = json.load(f)
    env = config['mcpServers']['scylladb']['env']
    for k, v in env.items():
        if not k.startswith('//'):
            os.environ[k] = v

from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider

host = os.getenv('SCYLLA_HOST')
username = os.getenv('SCYLLA_USERNAME')
password = os.getenv('SCYLLA_PASSWORD')

print(f"Testing connection to: {host}")

auth_provider = PlainTextAuthProvider(username=username, password=password)

# Simple connection without SSL first
try:
    cluster = Cluster([host], auth_provider=auth_provider, port=9042)
    session = cluster.connect()
    print("✅ Connected without SSL!")
    result = session.execute("SELECT now() FROM system.local")
    for row in result:
        print(f"   Server time: {row[0]}")
    cluster.shutdown()
except Exception as e:
    print(f"❌ No SSL failed: {e}")

# Try with connection bundle approach
print("\nDo you have a connection bundle (cqlshrc file) from ScyllaDB Cloud?")
print("If yes, it might contain SSL certificates and connection details.")