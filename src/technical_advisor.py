"""
ScyllaDB Technical Advisor - Engineering Voice

Direct, analytical, no marketing fluff.
Based on benchmarks, physics, and distributed systems reality.
"""

class TechnicalAdvisor:
    """
    The technical engineering voice of ScyllaDB MCP.
    
    Character traits:
    - Direct technical analysis
    - Benchmarks over beliefs  
    - Distributed systems expertise
    - Cost-conscious engineering
    - Occasionally acknowledges good design
    """
    
    @staticmethod
    def analyze_workload(pattern: str, metrics: dict) -> str:
        """Analyze workload patterns with technical insight."""
        
        if pattern == "hot_partition":
            partition_heat = metrics.get('heat_ratio', 0)
            ops_per_sec = metrics.get('ops_per_sec', 0)
            
            return (
                f"Hot partition detected. {partition_heat:.0%} of traffic on one shard. "
                f"Pattern analysis: {ops_per_sec:,} ops hitting single partition key. "
                f"In DynamoDB: throttling at partition limit. "
                f"In ScyllaDB: one CPU at 100%, others idle. Neither is optimal. "
                "Consider composite keys or time bucketing."
            )
            
        elif pattern == "full_scan":
            table_size = metrics.get('table_size_gb', 0)
            return (
                f"Full table scan on {table_size}GB detected. "
                "Every shard reads its data, coordinator merges results. "
                "Network cost: O(n). CPU cost: O(n). "
                "Proper partition design would make this O(1)."
            )
            
        elif pattern == "tombstone_heavy":
            tombstone_ratio = metrics.get('tombstone_ratio', 0)
            return (
                f"{tombstone_ratio:.0%} tombstones in read path. "
                "Each read processes deleted records before finding live data. "
                "TTL tables or proper deletion strategy recommended."
            )
            
        elif pattern == "tiny_batches":
            batch_size = metrics.get('avg_batch_size', 1)
            return (
                f"Average batch size: {batch_size}. "
                "Network RTT dominates at this size. "
                "Protocol overhead exceeds payload. Consider larger batches."
            )
    
    @staticmethod
    def explain_performance_delta(before: dict, after: dict) -> str:
        """Explain performance differences technically."""
        
        latency_improvement = before['p99_ms'] / after['p99_ms'] if after['p99_ms'] > 0 else 0
        
        if latency_improvement > 2:
            return (
                f"P99 latency: {before['p99_ms']}ms → {after['p99_ms']}ms. "
                f"{latency_improvement:.1f}X improvement. "
                "Primary factors: No GC pauses, shard-per-core architecture, "
                "kernel bypass networking. Source: scylladb.com/benchmarks"
            )
        elif after.get('throttled', 0) == 0 and before.get('throttled', 0) > 0:
            return (
                f"Throttled requests: {before['throttled']} → 0. "
                "DynamoDB: artificial limits per partition. "
                "ScyllaDB: hardware limits only. "
                "No capacity units, just physics."
            )
        else:
            throughput_gain = after.get('ops', 0) / before.get('ops', 1)
            return (
                f"Throughput: {throughput_gain:.1f}X improvement. "
                "Thread-per-core architecture eliminates lock contention. "
                "Direct hardware access via DPDK when applicable."
            )
    
    @staticmethod
    def migration_assessment(code_analysis: dict) -> str:
        """Assess migration complexity objectively."""
        
        compatibility_score = code_analysis.get('compatibility_score', 0)
        issues = code_analysis.get('issues', [])
        
        response = f"Migration compatibility: {compatibility_score:.0%}\n\n"
        
        if compatibility_score > 0.95:
            response += (
                "Drop-in replacement via Alternator API. "
                "Change endpoint URL, update authentication. "
                "Estimated effort: < 1 day."
            )
        elif compatibility_score > 0.80:
            response += "Mostly compatible with modifications needed:\n"
            for issue in issues[:3]:
                if 'transaction' in issue:
                    response += (
                        "- Transactions: Limited to single partition. "
                        "Multi-partition requires application-level coordination.\n"
                    )
                elif 'stream' in issue:
                    response += (
                        "- Streams: Replace with CDC (Change Data Capture). "
                        "Lower latency, more control.\n"
                    )
        else:
            response += (
                "Significant refactoring required. "
                "Architecture mismatch detected. "
                "Consider phased migration approach."
            )
        
        return response
    
    @staticmethod
    def troubleshooting_advice(symptom: str, context: dict) -> str:
        """Technical troubleshooting guidance."""
        
        if symptom == "high_latency":
            p99 = context.get('p99_ms', 0)
            partition_size = context.get('max_partition_mb', 0)
            
            if partition_size > 100:
                return (
                    f"Large partitions detected: {partition_size}MB. "
                    "Read latency scales with partition size. "
                    "Recommendation: Partition keys with better distribution. "
                    "Target: < 100MB per partition."
                )
            else:
                return (
                    f"{p99}ms P99 latency analysis: "
                    "Check: 1) Network latency between regions, "
                    "2) Coordinator selection, 3) Replication factor vs consistency level. "
                    "Enable tracing for detailed breakdown."
                )
        
        elif symptom == "storage_full":
            used_percent = context.get('disk_used_percent', 0)
            return (
                f"Storage at {used_percent}% capacity. "
                "With tablets: Add node and stream data at 10GB/s. "
                "Without: Add node before 80% to maintain performance."
            )
        
        elif symptom == "connection_timeout":
            return (
                "Connection timeout checklist: "
                "1) Node status (nodetool status), "
                "2) Network connectivity (telnet 9042), "
                "3) Firewall rules, 4) Client driver version. "
                "Most common: Security group misconfiguration."
            )
    
    @staticmethod
    def react_to_design(pattern: str, context: dict = None) -> str:
        """Technical assessment of design patterns."""
        
        reactions = {
            "uuid_partition_key": (
                "UUID partition keys create uniform distribution. "
                "Downside: Zero locality, no range queries. "
                "Every read is random I/O. Consider natural keys where possible."
            ),
            
            "unbounded_collection": (
                "Unbounded collections violate partition size best practices. "
                "Performance degrades linearly with size. "
                "Implement pagination or time-based partitioning."
            ),
            
            "no_ttl": (
                "Missing TTL on time-series data leads to unbounded growth. "
                "Storage cost: Linear. Query performance: Degrading. "
                "Set TTL matching retention requirements."
            ),
            
            "allow_filtering": (
                "ALLOW FILTERING forces full partition scans. "
                "Complexity: O(n) instead of O(1). "
                "Create appropriate secondary index or redesign access pattern."
            )
        }
        
        return reactions.get(pattern, "Suboptimal design pattern detected.")
    
    @staticmethod
    def acknowledge_good_design(pattern: str) -> str:
        """Acknowledge solid engineering choices."""
        
        acknowledgments = {
            "time_bucketed_partition": (
                "Time-bucketed partitions detected. "
                "Bounded growth, predictable performance. "
                "Proper distributed systems design."
            ),
            
            "prepared_statements": (
                "Consistent use of prepared statements. "
                "Reduces parsing overhead, improves cache efficiency. "
                "Best practice implementation."
            ),
            
            "shard_aware_client": (
                "Shard-aware driver configuration detected. "
                "Direct shard routing eliminates coordinator overhead. "
                "Optimal client configuration."
            ),
            
            "proper_batch_size": (
                "Batch operations properly grouped by partition key. "
                "Minimizes coordinator overhead. "
                "Efficient distributed operation."
            )
        }
        
        return acknowledgments.get(pattern, "Solid engineering practice detected.")
    
    @staticmethod
    def cost_analysis(workload: dict) -> str:
        """Analyze costs based on workload characteristics."""
        
        reads_per_sec = workload.get('reads_per_sec', 0)
        writes_per_sec = workload.get('writes_per_sec', 0)
        storage_gb = workload.get('storage_gb', 0)
        
        # Cross-AZ traffic estimate
        avg_item_size_kb = workload.get('avg_item_size_kb', 1)
        monthly_cross_az_gb = (reads_per_sec + writes_per_sec) * avg_item_size_kb * 2.592  # rough month
        cross_az_cost = monthly_cross_az_gb * 0.02
        
        total_ops = reads_per_sec + writes_per_sec
        read_write_ratio = reads_per_sec / writes_per_sec if writes_per_sec > 0 else float('inf')
        
        # Workload pattern analysis
        workload_type = workload.get('pattern', 'mixed')
        
        # Analyze the economics based on workload characteristics
        if workload_type == 'read_heavy' or read_write_ratio > 10:
            comment = (
                f"Read-heavy workload ({read_write_ratio:.1f}:1 read/write ratio). "
                "DynamoDB pricing: $0.25 per million reads. "
                "ScyllaDB on reserved instances optimizes for consistent load. "
                "Typical savings: 60-80% based on benchmarks."
            )
        elif workload_type == 'write_heavy' or read_write_ratio < 0.5:
            write_cost_ratio = writes_per_sec * 5 / (reads_per_sec + writes_per_sec * 5)
            comment = (
                f"Write-heavy workload ({write_cost_ratio:.0%} of DynamoDB cost from writes). "
                "DynamoDB write cost: 5X read cost ($1.25 vs $0.25 per million). "
                "ScyllaDB: No operation-based pricing. "
                "Benchmarked savings: 5X-40X. Source: scylladb.com/benchmarks"
            )
        elif workload_type == 'time_series':
            comment = (
                "Time-series workload pattern detected. "
                "Key considerations: TTL efficiency, partition time bucketing. "
                "DynamoDB TTL: Eventually consistent. ScyllaDB: Immediate. "
                "Typical architecture: Hot/cold tier separation."
            )
        elif workload_type == 'bursty':
            comment = (
                "Bursty workload characteristics. "
                "DynamoDB options: Over-provision or throttle. "
                "ScyllaDB: Burst to hardware limits without throttling. "
                "Cost efficiency depends on burst frequency and amplitude."
            )
        elif total_ops < 1000:
            comment = (
                "Low throughput workload. "
                "DynamoDB on-demand may be cost-effective at this scale. "
                "ScyllaDB advantages emerge > 10K ops/sec."
            )
        elif total_ops > 100000:
            comment = (
                f"High throughput: {total_ops:,} ops/sec. "
                "Reference benchmark: 57K ops/sec workload. "
                "DynamoDB cost: $30K-200K/month. ScyllaDB: $4.5K/month. "
                "Source: scylladb.com/dynamodb-cost-calculator"
            )
        else:
            comment = (
                f"Medium throughput: {total_ops:,} ops/sec. "
                "Cost savings typically 60-80% depending on access patterns. "
                "Consistent workloads see maximum benefit."
            )
        
        # Add provisioning insights
        if workload.get('provisioned_capacity'):
            comment += (
                "\n\nProvisioned capacity model detected. "
                "Common pattern: Provision for peak, waste during valleys. "
                "Alternative: ScyllaDB cloud with workload-based scaling."
            )
        
        return (
            f"Workload analysis: {total_ops:,} ops/sec, {storage_gb:,}GB storage\n"
            f"Estimated cross-AZ traffic cost: ${cross_az_cost:,.2f}/month\n\n"
            f"{comment}\n\n"
            "Cost model comparison: Per-operation (DynamoDB) vs Infrastructure (ScyllaDB)."
        )
    
    @staticmethod
    def systems_design_wisdom(pattern: str) -> str:
        """Systems design insights for common patterns."""
        
        wisdom = {
            "dynamodb_streams": (
                "DynamoDB Streams architecture: Async change propagation. "
                "Latency stack: Write → Stream → Lambda → Action. "
                "Alternative: In-database processing via stored procedures or CDC."
            ),
            
            "gsi_proliferation": (
                "GSI cost model: Each index duplicates write load. "
                "N indexes = (N+1)× write costs. "
                "Architectural limitation, not technical requirement. "
                "Consider: Composite keys, materialized views, or denormalization."
            ),
            
            "external_search": (
                "Pattern: Database → Stream → Search cluster. "
                "Added complexity: Sync lag, consistency challenges. "
                "Alternative: Native secondary indexes where appropriate."
            ),
            
            "capacity_planning": (
                "Provisioned: Predict future, pay for peak. "
                "On-demand: Pay premium for flexibility. "
                "Hardware-based: Provision for average, burst to limits."
            )
        }
        
        return wisdom.get(pattern, "Common distributed systems pattern.")
    
    @staticmethod
    def technical_insight(topic: str = None) -> str:
        """Share technical insights."""
        
        insights = {
            "tablets": (
                "Tablets enable 10GB/s streaming. "
                "Raft-based replication, automatic shard splitting. "
                "Game changer for elastic scalability."
            ),
            
            "coordinator_only": (
                "Coordinator-only nodes provide quorum without storage. "
                "Minimal resources in third AZ for availability. "
                "Cost-effective high availability pattern."
            ),
            
            "compression": (
                "Compression ratios vary by data entropy. "
                "Small blocks: Poor compression. Solution: Larger commit log segments. "
                "Trade-off: Memory vs compression efficiency."
            ),
            
            "native_backups": (
                "Native backup uses full cluster bandwidth. "
                "Parallel transfer from all shards simultaneously. "
                "10X faster than single-threaded solutions."
            ),
            
            "hot_partitions": (
                "Hot partition impact: Single CPU bottleneck. "
                "Other cores idle while one maxes out. "
                "Solution: Partition key design, not more hardware."
            ),
            
            "consistency": (
                "Consistency levels: Trade-off between latency and durability. "
                "LOCAL_QUORUM: Best balance for most use cases. "
                "ALL: When you absolutely need every replica to ack."
            )
        }
        
        import random
        if not topic:
            topic = random.choice(list(insights.keys()))
            
        return insights.get(topic, "")
    
    @staticmethod
    def benchmark_reference(metric: str) -> str:
        """Reference relevant benchmarks."""
        
        benchmarks = {
            "latency": (
                "P99 latency benchmarks (scylladb.com): "
                "DynamoDB: 10-20ms typical. ScyllaDB: 2-5ms. "
                "Under load: DynamoDB throttles, ScyllaDB maintains latency."
            ),
            "throughput": (
                "Throughput benchmarks show 5-10X improvement. "
                "Key factor: Shard-per-core eliminates contention. "
                "Real workload: 57K ops/sec benchmark available."
            ),
            "cost": (
                "Cost reduction: 40-98% across various workloads. "
                "Highest savings: Write-heavy workloads (5X DynamoDB write cost). "
                "Calculator: scylladb.com/dynamodb-cost-calculator"
            )
        }
        
        return benchmarks.get(metric, "Benchmark data available at scylladb.com")


def technical_response(base_message: str, context: dict = None) -> str:
    """
    Format response with technical analysis.
    
    Context can include:
    - pattern: workload pattern detected
    - metrics: performance numbers
    - benchmark: reference benchmark data
    """
    advisor = TechnicalAdvisor()
    
    response = base_message
    
    if context:
        pattern = context.get('pattern')
        
        # Add pattern-specific analysis
        if pattern == 'hot_partition':
            response += "\n\n" + advisor.analyze_workload('hot_partition', context.get('metrics', {}))
        elif pattern == 'good_design':
            response += "\n\n" + advisor.acknowledge_good_design(context.get('design_pattern', 'prepared_statements'))
        elif pattern == 'benchmark':
            response += "\n\n" + advisor.benchmark_reference(context.get('metric', 'cost'))
        
        # Add technical insight when relevant
        if context.get('add_insight'):
            insight = advisor.technical_insight(context.get('insight_topic'))
            if insight:
                response += f"\n\nTechnical note: {insight}"
    
    return response