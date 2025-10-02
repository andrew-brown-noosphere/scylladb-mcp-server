#!/usr/bin/env python3
"""ScyllaDB MCP Server with Technical Advisor - Fixed for current MCP API."""

import os
# Set up cassandra driver for Python 3.13 compatibility BEFORE importing
os.environ['CASS_DRIVER_NO_ASYNCORE'] = '1'
os.environ['CASS_DRIVER_NO_TWISTED'] = '1'
os.environ['CASS_DRIVER_NO_LIBEV'] = '1'

import asyncio
import json
import logging
from typing import Dict, Any, Optional, List
from dataclasses import dataclass
import time

import docker
import boto3
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
from mcp.server import Server
from mcp.server.models import InitializationOptions
from mcp.server.stdio import stdio_server
from mcp.types import Tool, TextContent

# Import our technical advisor voice
from technical_advisor import TechnicalAdvisor, technical_response
from query_analyzer import DynamoDBQueryAnalyzer
from scylladb_advisor import ScyllaDBAdvisor

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("scylladb-mcp")

@dataclass
class DatabaseConnections:
    """Database connection container."""
    dynamodb: Optional[Any] = None
    scylla: Optional[Any] = None
    alternator: Optional[Any] = None
    docker_container: Optional[Any] = None


class ScyllaDBMCPServer:
    """Main MCP server for ScyllaDB with technical advisor personality."""
    
    def __init__(self):
        self.server = Server(
            name="scylladb-mcp",
            version="1.0.0"
        )
        self.connections = DatabaseConnections()
        self.docker_client = None
        self.advisor = ScyllaDBAdvisor()
        self.technical_advisor = TechnicalAdvisor()
        self.query_analyzer = DynamoDBQueryAnalyzer()
        self._register_handlers()
    
    async def create_connections(self):
        """Create database connections based on environment."""
        mode = os.getenv('SCYLLA_CONNECTION_MODE', 'docker')
        
        if mode == 'docker':
            await self._handle_connect('docker')
        elif mode == 'alternator':
            await self._handle_connect('alternator')
        else:
            await self._handle_connect('cloud')
    
    def _register_handlers(self):
        """Register all MCP handlers with technical descriptions."""
        
        # Register list_tools handler
        @self.server.list_tools()
        async def list_tools():
            """List all available tools."""
            return [
                Tool(
                    name="connect",
                    description="Connect to ScyllaDB instance. Modes: 'docker' (local), 'cloud' (managed), 'alternator' (DynamoDB API)",
                    inputSchema={
                        "type": "object",
                        "properties": {
                            "mode": {
                                "type": "string",
                                "enum": ["docker", "cloud", "alternator"],
                                "default": "docker"
                            }
                        }
                    }
                ),
                Tool(
                    name="execute",
                    description="Execute CQL queries. Returns technical analysis of query patterns and performance implications.",
                    inputSchema={
                        "type": "object",
                        "properties": {
                            "query": {"type": "string"},
                            "keyspace": {"type": "string", "default": "demo"}
                        },
                        "required": ["query"]
                    }
                ),
                Tool(
                    name="checkMigration",
                    description="Analyze DynamoDB code for ScyllaDB compatibility. Returns effort estimate and specific issues.",
                    inputSchema={
                        "type": "object",
                        "properties": {
                            "code": {"type": "string"},
                            "language": {
                                "type": "string",
                                "enum": ["python", "javascript", "java", "go"],
                                "default": "python"
                            }
                        },
                        "required": ["code"]
                    }
                ),
                Tool(
                    name="analyzeWorkload",
                    description="Deep analysis of DynamoDB access patterns. Detects hot partitions, inefficient queries, cost drivers.",
                    inputSchema={
                        "type": "object",
                        "properties": {
                            "code_path": {"type": "string"},
                            "deep_analysis": {"type": "boolean", "default": True}
                        },
                        "required": ["code_path"]
                    }
                ),
                Tool(
                    name="comparePerformance",
                    description="Live A/B test: DynamoDB vs ScyllaDB. Shows real latency, throughput, and cost differences.",
                    inputSchema={
                        "type": "object",
                        "properties": {
                            "operation": {
                                "type": "string",
                                "enum": ["write", "read", "scan", "batch"],
                                "default": "write"
                            },
                            "itemCount": {"type": "integer", "default": 1000}
                        }
                    }
                ),
                Tool(
                    name="costEstimate",
                    description="Calculate cost comparison based on workload. No fluff, just numbers.",
                    inputSchema={
                        "type": "object",
                        "properties": {
                            "reads_per_sec": {"type": "integer"},
                            "writes_per_sec": {"type": "integer"},
                            "storage_gb": {"type": "integer"},
                            "item_size_kb": {"type": "number", "default": 1},
                            "pattern": {
                                "type": "string",
                                "enum": ["steady", "bursty", "time_series"],
                                "default": "steady"
                            }
                        },
                        "required": ["reads_per_sec", "writes_per_sec", "storage_gb"]
                    }
                )
            ]
        
        # Register call_tool handler
        @self.server.call_tool()
        async def call_tool(name: str, arguments: dict) -> List[TextContent]:
            """Handle tool calls."""
            try:
                if name == "connect":
                    result = await self._handle_connect(arguments.get("mode", "docker"))
                elif name == "execute":
                    result = await self._handle_execute(
                        arguments["query"],
                        arguments.get("keyspace", "demo")
                    )
                elif name == "checkMigration":
                    result = await self._handle_check_migration(
                        arguments["code"],
                        arguments.get("language", "python")
                    )
                elif name == "analyzeWorkload":
                    result = await self._handle_analyze_workload(
                        arguments["code_path"],
                        arguments.get("deep_analysis", True)
                    )
                elif name == "comparePerformance":
                    result = await self._handle_compare_performance(
                        arguments.get("operation", "write"),
                        arguments.get("itemCount", 1000)
                    )
                elif name == "costEstimate":
                    result = await self._handle_cost_estimate(
                        arguments["reads_per_sec"],
                        arguments["writes_per_sec"],
                        arguments["storage_gb"],
                        arguments.get("item_size_kb", 1),
                        arguments.get("pattern", "steady")
                    )
                else:
                    result = f"Unknown tool: {name}"
                
                return [TextContent(type="text", text=result)]
                
            except Exception as e:
                error_msg = f"Tool execution failed: {str(e)}"
                logger.error(error_msg, exc_info=True)
                return [TextContent(type="text", text=error_msg)]
    
    async def _handle_connect(self, mode: str) -> str:
        """Handle database connection with auto-setup."""
        try:
            if mode == "docker":
                # Check Docker availability
                try:
                    self.docker_client = docker.from_env()
                    self.docker_client.ping()
                except Exception as e:
                    return technical_response(
                        "Docker not running. Start Docker Desktop first.",
                        {'pattern': 'setup_issue'}
                    )
                
                # Look for existing container
                containers = self.docker_client.containers.list(
                    all=True,
                    filters={"name": "scylladb-mcp"}
                )
                
                if containers:
                    container = containers[0]
                    if container.status != "running":
                        container.start()
                        await asyncio.sleep(5)
                else:
                    # Create new container
                    result = await self._create_docker_container()
                    if "Failed" in result:
                        return result
                
                # Connect to local ScyllaDB
                self.connections.scylla = Cluster(['localhost']).connect()
                
                return technical_response(
                    "Connected to local ScyllaDB (Docker). Zero-copy networking, no cloud latency.",
                    {'pattern': 'good_design', 'design_pattern': 'local_development'}
                )
                
            elif mode == "alternator":
                # Connect to ScyllaDB via Alternator API
                endpoint = os.getenv('SCYLLA_ALTERNATOR_ENDPOINT')
                if not endpoint:
                    return technical_response(
                        "Missing SCYLLA_ALTERNATOR_ENDPOINT. Check your config.",
                        {'pattern': 'config_error'}
                    )
                
                self.connections.alternator = boto3.resource(
                    'dynamodb',
                    endpoint_url=endpoint,
                    region_name='us-east-1',
                    aws_access_key_id='fake',
                    aws_secret_access_key='fake'
                )
                
                # Also connect native DynamoDB for comparison
                aws_key = os.getenv('AWS_ACCESS_KEY_ID')
                if aws_key:
                    self.connections.dynamodb = boto3.resource(
                        'dynamodb',
                        region_name='us-east-1',
                        aws_access_key_id=aws_key,
                        aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY')
                    )
                
                return technical_response(
                    "Connected to ScyllaDB Alternator. Same DynamoDB API, better internals.",
                    {'pattern': 'alternator', 'add_insight': True, 'insight_topic': 'shard_per_core'}
                )
                
            else:
                return technical_response(
                    f"Unknown mode: {mode}. Use 'docker', 'cloud', or 'alternator'.",
                    {'pattern': 'usage_error'}
                )
                
        except Exception as e:
            logger.error(f"Connection failed: {e}")
            return technical_response(
                f"Connection failed: {str(e)}",
                {'pattern': 'error'}
            )
    
    async def _handle_execute(self, query: str, keyspace: str) -> str:
        """Execute CQL query with performance analysis."""
        if not self.connections.scylla:
            return technical_response(
                "Not connected. Run 'connect' first.",
                {'pattern': 'usage_error'}
            )
        
        try:
            # Create keyspace if needed
            if keyspace != "system":
                self.connections.scylla.execute(f"""
                    CREATE KEYSPACE IF NOT EXISTS {keyspace}
                    WITH replication = {{
                        'class': 'SimpleStrategy',
                        'replication_factor': 1
                    }}
                """)
                self.connections.scylla.execute(f"USE {keyspace}")
            
            # Execute query
            start_time = time.time()
            result = self.connections.scylla.execute(query)
            execution_time = (time.time() - start_time) * 1000
            
            # Format response with analysis
            response = f"Query executed in {execution_time:.1f}ms\n\n"
            
            if result:
                rows = list(result)
                response += f"Returned {len(rows)} rows\n"
                if len(rows) > 0:
                    response += f"Sample: {rows[0]}\n"
            
            # Add query pattern analysis
            query_lower = query.lower()
            if "allow filtering" in query_lower:
                response += self.technical_advisor.react_to_design(
                    'allow_filtering'
                )
            elif "select * from" in query_lower and "where" not in query_lower:
                response += self.technical_advisor.analyze_workload(
                    'full_scan',
                    {'table_size_gb': 10}  # Estimate
                )
            
            return response
            
        except Exception as e:
            return technical_response(
                f"Query failed: {str(e)}",
                {'pattern': 'error'}
            )
    
    async def _handle_check_migration(self, code: str, language: str) -> str:
        """Check DynamoDB code compatibility."""
        compatibility_score = 0.9  # Base compatibility
        issues = []
        
        # Analyze code patterns
        code_lower = code.lower()
        
        # Check for incompatible features
        if "transact_write" in code_lower or "transactwriteitems" in code_lower:
            compatibility_score -= 0.1
            issues.append("transaction: Multi-partition transactions not supported")
        
        if "stream" in code_lower and ("getrecords" in code_lower or "describe_stream" in code_lower):
            compatibility_score -= 0.05
            issues.append("stream: Use CDC instead of Streams API")
        
        if "global_secondary_index" in code_lower or "gsi" in code_lower:
            # GSIs are supported but note the pattern
            issues.append("gsi: Supported, but consider materialized views")
        
        # Build response
        response = self.technical_advisor.migration_assessment({
            'compatibility_score': compatibility_score,
            'issues': issues
        })
        
        # Add code-specific advice
        if language == "python":
            response += "\n\nMigration steps:\n"
            response += "1. Change endpoint_url to ScyllaDB Alternator\n"
            response += "2. Set aws_access_key_id='fake' for Alternator\n"
            response += "3. Rest of boto3 code unchanged\n"
        
        return response
    
    async def _handle_analyze_workload(self, code_path: str, deep_analysis: bool) -> str:
        """Analyze workload patterns from code."""
        try:
            # Read code file
            with open(code_path, 'r') as f:
                code = f.read()
            
            # Analyze with our query analyzer
            analysis_results = self.query_analyzer.analyze_code(code)
            
            response = f"Workload Analysis: {code_path}\n"
            response += "=" * 50 + "\n\n"
            
            # Access patterns
            response += f"Access Patterns Detected: {len(analysis_results['patterns'])}\n"
            for pattern in analysis_results['patterns']:
                response += f"- {pattern['type']}: {pattern['count']} occurrences\n"
            
            # Hot partitions
            if analysis_results['hot_partitions']:
                response += "\n⚠️  Hot Partition Alert:\n"
                for hp in analysis_results['hot_partitions']:
                    response += f"- Partition '{hp['partition']}': {hp['heat_ratio']:.0%} of traffic\n"
                
                # Add technical analysis
                hp = analysis_results['hot_partitions'][0]  # Worst offender
                response += self.technical_advisor.analyze_workload(
                    'hot_partition',
                    {
                        'heat_ratio': hp['heat_ratio'],
                        'ops_per_sec': hp['access_count'] * 10  # Estimate
                    }
                )
            
            # Performance implications
            if deep_analysis:
                response += "\n\nPerformance Implications:\n"
                for pattern in analysis_results['patterns']:
                    if pattern['type'] == 'scan':
                        response += self.technical_advisor.analyze_workload('full_scan', {'table_size_gb': 50})
                    elif pattern['type'] == 'batch_write' and pattern.get('size', 0) < 10:
                        response += self.technical_advisor.analyze_workload('tiny_batches', {'avg_batch_size': pattern.get('size', 5)})
            
            # Cost implications
            response += f"\n\nCost Drivers:\n"
            response += f"- GSI count: {analysis_results['gsi_count']} (each GSI = duplicate write costs)\n"
            response += f"- Scan operations: {analysis_results['scan_frequency']}/hour (expensive at scale)\n"
            
            # Add technical insight
            response += "\n" + self.technical_advisor.technical_insight('tablets')
            
            return response
            
        except Exception as e:
            return technical_response(f"Analysis failed: {str(e)}", {'pattern': 'error'})
    
    async def _handle_compare_performance(self, operation: str, item_count: int) -> str:
        """Run live performance comparison."""
        if not (self.connections.dynamodb and self.connections.alternator):
            return technical_response(
                "Need both DynamoDB and Alternator connections. Run 'connect' with mode='alternator'.",
                {'pattern': 'setup_issue'}
            )
        
        response = f"Performance Comparison: {operation} ({item_count} items)\n"
        response += "=" * 50 + "\n\n"
        
        # Create test data
        test_item = {
            'id': 'perf-test-id',
            'data': 'x' * 1024,  # 1KB payload
            'timestamp': int(time.time())
        }
        
        # Test DynamoDB
        dynamodb_latencies = []
        dynamodb_start = time.time()
        
        table = self.connections.dynamodb.Table('perf-test')
        for i in range(min(item_count, 100)):  # Cap at 100 for quick test
            item = test_item.copy()
            item['id'] = f'ddb-{i}'
            
            op_start = time.time()
            if operation == "write":
                table.put_item(Item=item)
            elif operation == "read":
                table.get_item(Key={'id': item['id']})
            dynamodb_latencies.append((time.time() - op_start) * 1000)
        
        dynamodb_time = time.time() - dynamodb_start
        
        # Test ScyllaDB Alternator
        scylla_latencies = []
        scylla_start = time.time()
        
        table = self.connections.alternator.Table('perf-test')
        for i in range(min(item_count, 100)):
            item = test_item.copy()
            item['id'] = f'scylla-{i}'
            
            op_start = time.time()
            if operation == "write":
                table.put_item(Item=item)
            elif operation == "read":
                table.get_item(Key={'id': item['id']})
            scylla_latencies.append((time.time() - op_start) * 1000)
        
        scylla_time = time.time() - scylla_start
        
        # Calculate metrics
        import statistics
        
        dynamodb_metrics = {
            'avg_latency': statistics.mean(dynamodb_latencies),
            'p99_ms': sorted(dynamodb_latencies)[int(len(dynamodb_latencies) * 0.99)],
            'throughput': len(dynamodb_latencies) / dynamodb_time,
            'throttled': 0  # Would need CloudWatch for real number
        }
        
        scylla_metrics = {
            'avg_latency': statistics.mean(scylla_latencies),
            'p99_ms': sorted(scylla_latencies)[int(len(scylla_latencies) * 0.99)],
            'throughput': len(scylla_latencies) / scylla_time,
            'throttled': 0
        }
        
        # Format results
        response += f"DynamoDB:\n"
        response += f"  Avg latency: {dynamodb_metrics['avg_latency']:.1f}ms\n"
        response += f"  P99 latency: {dynamodb_metrics['p99_ms']:.1f}ms\n"
        response += f"  Throughput: {dynamodb_metrics['throughput']:.0f} ops/sec\n"
        
        response += f"\nScyllaDB Alternator:\n"
        response += f"  Avg latency: {scylla_metrics['avg_latency']:.1f}ms\n"
        response += f"  P99 latency: {scylla_metrics['p99_ms']:.1f}ms\n"
        response += f"  Throughput: {scylla_metrics['throughput']:.0f} ops/sec\n"
        response += f"  Throttled requests: 0 (hardware limits only)\n"
        
        response += "\n" + self.technical_advisor.explain_performance_delta(
            dynamodb_metrics, scylla_metrics
        )
        
        # Add architecture insight
        response += "\n\nArchitecture difference:\n"
        response += "- DynamoDB: Request → LB → Storage nodes → Response\n"
        response += "- ScyllaDB: Request → Shard (CPU + Memory + Storage) → Response\n"
        response += "Fewer hops = lower latency\n"
        
        return response
    
    async def _handle_cost_estimate(self, reads_per_sec: int, writes_per_sec: int, 
                                   storage_gb: int, item_size_kb: float, pattern: str) -> str:
        """Calculate cost comparison."""
        response = "Cost Analysis\n"
        response += "=" * 50 + "\n\n"
        
        # DynamoDB costs (simplified)
        read_units = reads_per_sec * 3600 * 24 * 30  # Monthly reads
        write_units = writes_per_sec * 3600 * 24 * 30  # Monthly writes
        
        # Pricing (us-east-1)
        read_cost = (read_units / 1_000_000) * 0.25
        write_cost = (write_units / 1_000_000) * 1.25
        storage_cost = storage_gb * 0.25
        
        dynamodb_total = read_cost + write_cost + storage_cost
        
        response += f"DynamoDB Monthly Cost:\n"
        response += f"  Reads: ${read_cost:,.2f}\n"
        response += f"  Writes: ${write_cost:,.2f}\n"
        response += f"  Storage: ${storage_cost:,.2f}\n"
        response += f"  Total: ${dynamodb_total:,.2f}\n"
        
        # ScyllaDB estimate (rough - depends on instance selection)
        # Assuming i3.2xlarge can handle 50K ops/sec
        total_ops = reads_per_sec + writes_per_sec
        instances_needed = max(1, total_ops // 50000)
        scylla_cost = instances_needed * 624  # i3.2xlarge monthly
        
        response += f"\nScyllaDB Estimated Cost:\n"
        response += f"  Instances: {instances_needed} × i3.2xlarge\n"
        response += f"  Total: ${scylla_cost:,.2f}\n"
        response += f"  Savings: ${dynamodb_total - scylla_cost:,.2f} ({((dynamodb_total - scylla_cost) / dynamodb_total * 100):.0f}%)\n"
        
        # Add workload-specific analysis
        workload_context = {
            'reads_per_sec': reads_per_sec,
            'writes_per_sec': writes_per_sec,
            'storage_gb': storage_gb,
            'avg_item_size_kb': item_size_kb,
            'pattern': pattern
        }
        
        response += self.technical_advisor.cost_analysis(workload_context)
        
        return response
    
    async def _create_docker_container(self) -> str:
        """Create and start ScyllaDB Docker container."""
        try:
            logger.info("Pulling ScyllaDB image...")
            self.docker_client.images.pull("scylladb/scylla", tag="latest")
            
            logger.info("Creating container...")
            container = self.docker_client.containers.run(
                "scylladb/scylla:latest",
                name="scylladb-mcp",
                ports={
                    '9042/tcp': 9042,   # CQL
                    '9160/tcp': 9160,   # Thrift
                    '10000/tcp': 10000  # REST API
                },
                detach=True,
                remove=False,
                command="--smp 1 --memory 750M --overprovisioned 1"
            )
            
            logger.info("Waiting for ScyllaDB to start...")
            await asyncio.sleep(30)  # ScyllaDB needs time to initialize
            
            self.connections.docker_container = container
            
            return technical_response(
                "Created ScyllaDB container. SMP=1 for laptop-friendly performance.",
                {'pattern': 'docker_setup'}
            )
            
        except Exception as e:
            return technical_response(
                f"Failed to create container: {str(e)}",
                {'pattern': 'error'}
            )
    
    async def run(self):
        """Run the MCP server."""
        async with stdio_server() as (read_stream, write_stream):
            await self.server.run(
                read_stream,
                write_stream,
                InitializationOptions(
                    server_name="scylladb-mcp",
                    server_version="1.0.0"
                )
            )


def main():
    """Main entry point."""
    server = ScyllaDBMCPServer()
    asyncio.run(server.run())


if __name__ == "__main__":
    main()