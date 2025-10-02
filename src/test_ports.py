#!/usr/bin/env python3
"""Test different ports for ScyllaDB Cloud."""

import socket
import os
import json

# Load config
with open('../.claude/config.json', 'r') as f:
    config = json.load(f)
    env = config['mcpServers']['scylladb']['env']

host = "node-0.aws-us-east-1.a17dfa2542149ef7642e.clusters.scylla.cloud"
print(f"Testing ports for: {host}")

# Common CQL and ScyllaDB Cloud ports
ports_to_test = [9042, 9142, 443, 9043, 10000]

for port in ports_to_test:
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.settimeout(5)
    try:
        result = sock.connect_ex((host, port))
        if result == 0:
            print(f"✅ Port {port} is OPEN")
        else:
            print(f"❌ Port {port} is closed/filtered")
    except Exception as e:
        print(f"❌ Port {port} error: {e}")
    finally:
        sock.close()

print("\nNote: ScyllaDB Cloud might use:")
print("- Port 9042 for standard CQL")
print("- Port 9142 for CQL with SSL/TLS")
print("- Port 443 for proxy connections")
print("- Custom ports for managed clusters")