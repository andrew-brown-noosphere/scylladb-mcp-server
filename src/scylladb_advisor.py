"""
ScyllaDB Technical Advisor

Direct, pragmatic, performance-obsessed. Thinks in terms of memory layout,
CPU cycles, and distributed systems fundamentals. No marketing fluff.
"""

class ScyllaDBAdvisor:
    """
    The engineering voice of ScyllaDB MCP.
    
    Character traits:
    - Direct and pragmatic
    - Explains the "why" at a systems level
    - Cost-conscious (knows cloud pricing)
    - Performance metrics matter
    - Honest about tradeoffs
    - Occasional dry humor
    """
    
    @staticmethod
    def analyze_workload(pattern: str, metrics: dict) -> str:
        """Analyze workload patterns with engineering precision."""
        
        if pattern == "hot_partition":
            partition_heat = metrics.get('heat_ratio', 0)
            ops_per_sec = metrics.get('ops_per_sec', 0)
            
            return (
                f"You have a hot partition taking {partition_heat:.0%} of traffic. "
                f"That's {ops_per_sec:,} ops hitting one shard while others sit idle. "
                "This isn't just inefficient - at 2 cents per GB for cross-AZ traffic, "
                "the coordinator overhead is costing real money. "
                "Consider sharding by time buckets or hash prefixes."
            )
            
        elif pattern == "full_scan":
            table_size = metrics.get('table_size_gb', 0)
            return (
                f"Full table scan on {table_size}GB. Every shard has to wake up, "
                "scan its ranges, serialize results, ship to coordinator. "
                "You're turning a parallel architecture into a sequential bottleneck. "
                "Use the partition key - that's what it's for."
            )
            
        elif pattern == "tombstone_heavy":
            tombstone_ratio = metrics.get('tombstone_ratio', 0)
            return (
                f"Tombstone ratio is {tombstone_ratio:.0%}. You're reading more dead data "
                "than live data. Until repair runs and garbage collection happens, "
                "every query pays this tax. Consider TTL tables or rethink your delete patterns."
            )
    
    @staticmethod
    def explain_performance_delta(before: dict, after: dict) -> str:
        """Explain performance differences with technical depth."""
        
        latency_improvement = before['p99_ms'] / after['p99_ms'] if after['p99_ms'] > 0 else 0
        
        if latency_improvement > 2:
            return (
                f"P99 latency dropped from {before['p99_ms']}ms to {after['p99_ms']}ms. "
                "That's what happens when you eliminate the JVM GC pauses. "
                "No more stop-the-world collections. The CPU keeps working on YOUR requests, "
                "not garbage collection. ScyllaDB benchmarks consistently show 2X-4X better P99."
            )
        elif after.get('throttled', 0) == 0 and before.get('throttled', 0) > 0:
            return (
                f"Zero throttled requests vs {before['throttled']} before. "
                "ScyllaDB doesn't throttle - if the hardware can handle it, we process it. "
                "No more ProvisionedThroughputExceeded at 3am."
            )
        else:
            throughput_gain = after.get('ops', 0) / before.get('ops', 1)
            return (
                f"{throughput_gain:.1f}x throughput. Shard-per-core architecture - "
                "each CPU owns its data, no locks, no coordination. "
                "This is mechanical sympathy at work."
            )
    
    @staticmethod
    def migration_assessment(code_analysis: dict) -> str:
        """Assess DynamoDB to Alternator migration with engineering rigor."""
        
        compatibility_score = code_analysis.get('compatibility_score', 0)
        issues = code_analysis.get('issues', [])
        
        response = f"Migration assessment: {compatibility_score:.0%} compatible.\n\n"
        
        if compatibility_score > 0.95:
            response += (
                "Straightforward port. Change the endpoint, you're done. "
                "Alternator speaks DynamoDB's protocol but with better internals. "
                "30% better performance from LTO alone."
            )
        elif compatibility_score > 0.80:
            response += "Mostly compatible with some caveats:\n"
            for issue in issues[:3]:  # Top 3 issues
                if 'transaction' in issue:
                    response += (
                        "- Transactions: Limited to single partition. It's Raft-based "
                        "consensus - crossing partitions means distributed coordination. "
                        "Keep transactions partition-local.\n"
                    )
                elif 'stream' in issue:
                    response += (
                        "- Streams: Use CDC instead. It's more efficient anyway - "
                        "captures changes at the storage layer, not the API layer.\n"
                    )
        else:
            response += (
                "Significant redesign needed. Your access patterns fight the architecture. "
                "In DynamoDB, you pay to make it work. In ScyllaDB, you design it right. "
                "Let's look at your partition strategy..."
            )
        
        return response
    
    @staticmethod
    def explain_architectural_advantage(feature: str) -> str:
        """Explain ScyllaDB advantages at a technical level."""
        
        explanations = {
            "shard_per_core": (
                "Thread-per-core architecture. No thread context switches, no locks. "
                "Each core runs one thread that owns its data exclusively. "
                "Cache stays hot, CPU stays busy. It's that simple."
            ),
            
            "userspace_tcp": (
                "Kernel bypass with DPDK. Why waste cycles on kernel/user transitions? "
                "Network packets go straight from NIC to userspace. "
                "Saves microseconds per request. At a million requests/sec, that's real latency."
            ),
            
            "tablets": (
                "Tablets give us fast streaming - up to 10GB/s on i3en.metal. "
                "Schema-independent, incremental. Launch instances just-in-time "
                "when you hit 90% disk. No more keeping 30% free 'just in case'."
            ),
            
            "memory_management": (
                "We monitor bloom filter memory pressure. If it gets too high, "
                "we evict them. Trade some read performance for stability. "
                "When memory frees up, reload them. No OOMs, no crashes."
            ),
            
            "raft_consistency": (
                "Moving system tables to Raft. No more eventually consistent auth tables. "
                "Full replication to every node. No more hot spots during connection storms. "
                "Learned this one the hard way."
            )
        }
        
        return explanations.get(feature, "Solid engineering. No magic, just better implementation.")
    
    @staticmethod
    def cost_analysis(workload: dict) -> str:
        """Analyze costs with real numbers."""
        
        reads_per_sec = workload.get('reads_per_sec', 0)
        writes_per_sec = workload.get('writes_per_sec', 0)
        storage_gb = workload.get('storage_gb', 0)
        
        # Cross-AZ traffic estimate
        avg_item_size_kb = workload.get('avg_item_size_kb', 1)
        cross_az_percentage = 0.66  # Assuming 3 AZs, 2/3 traffic crosses AZ
        
        monthly_cross_az_gb = (
            (reads_per_sec + writes_per_sec) * 
            avg_item_size_kb / 1024 * 
            cross_az_percentage * 
            86400 * 30  # seconds per month
        )
        
        cross_az_cost = monthly_cross_az_gb * 0.02  # 2 cents per GB
        
        total_ops = reads_per_sec + writes_per_sec
        
        # Workload-specific insights
        if total_ops < 1000:
            savings_estimate = "For light workloads, consider growth trajectory."
        elif total_ops < 10000:
            savings_estimate = "Expect 40-60% savings on medium workloads."
        elif total_ops < 100000:
            savings_estimate = "High-throughput workloads typically see 60-80% savings."
        else:
            savings_estimate = "At extreme scale, savings can exceed 80%."
        
        return (
            f"Your workload: {total_ops:,} ops/sec, {storage_gb:,}GB\n"
            f"Cross-AZ traffic alone: ${cross_az_cost:,.2f}/month\n"
            "With ScyllaDB's shard-aware drivers, you cut coordinator hops. "
            "Plus no throttling charges, no over-provisioning.\n"
            f"{savings_estimate} Do the math."
        )
    
    @staticmethod
    def troubleshooting_advice(symptom: str, context: dict) -> str:
        """Direct troubleshooting advice."""
        
        if symptom == "high_latency":
            p99 = context.get('p99_ms', 0)
            partition_size = context.get('max_partition_mb', 0)
            
            if partition_size > 100:
                return (
                    f"{partition_size}MB partitions. That's your problem. "
                    "Large partitions = more data to scan = higher latency. "
                    "Physics still applies. Split those partitions."
                )
            else:
                return (
                    f"Check your coordinators. With {p99}ms P99, something's wrong. "
                    "Could be cross-DC queries, could be GC if you're comparing to Cassandra. "
                    "Run 'nodetool cfstats' and look for pending compactions."
                )
        
        elif symptom == "storage_full":
            used_percent = context.get('disk_used_percent', 0)
            return (
                f"At {used_percent}% disk usage. With tablets, stream at 10GB/s. "
                "Launch a new node now, it'll rebalance before you hit 100%. "
                "No need for the old '30% free space' rule anymore."
            )
    
    @staticmethod
    def acknowledge_good_design(pattern: str) -> str:
        """Recognize good engineering (sparingly)."""
        
        acknowledgments = {
            "time_series_partition": (
                "Good partition design. Time buckets prevent infinite partition growth. "
                "Someone's been reading the docs."
            ),
            "prepared_statements": (
                "All prepared statements. That's how you do it. "
                "Parse once, execute many. Saves CPU on both sides."
            ),
            "shard_aware_client": (
                "Using shard-aware drivers. Smart. "
                "Skip the coordinator, go straight to the replica. "
                "30% latency reduction right there."
            )
        }
        
        return acknowledgments.get(pattern, "Solid approach.")
    
    @staticmethod
    def technical_insight(topic: str = None) -> str:
        """Drop technical insights Avi-style."""
        
        insights = {
            "bloom_filters": (
                "Bloom filters can eat memory with small partitions. "
                "We monitor and evict when needed. "
                "Better to lose some read performance than OOM."
            ),
            "coordinator_only": (
                "Coordinator-only nodes solve the split-brain problem. "
                "Tiny instance in a third AZ, just for quorum. "
                "Cheap insurance against data center failure."
            ),
            "dictionary_compression": (
                "Small compression blocks don't compress well. "
                "Dictionary compression gives context. "
                "Sample the data, build dictionary, compress against it. "
                "Works for RPC and SSTables."
            ),
            "native_backups": (
                "Native backup can use full bandwidth. "
                "Scheduler throttles when needed. "
                "No more babysitting rclone at 10% speed."
            )
        }
        
        import random
        if not topic:
            topic = random.choice(list(insights.keys()))
            
        return insights.get(topic, "")
    

def advisor_response(base_message: str, context: dict = None) -> str:
    """
    Format response in ScyllaDB engineering style.
    
    Context can include:
    - pattern: workload pattern detected
    - metrics: performance metrics
    - operation: what user is trying to do
    """
    advisor = ScyllaDBAdvisor()
    
    response = base_message
    
    if context:
        pattern = context.get('pattern')
        metrics = context.get('metrics', {})
        
        # Add pattern-specific insights
        if pattern:
            if pattern == 'hot_partition':
                response += "\n\n" + advisor.analyze_workload('hot_partition', metrics)
            elif pattern == 'good_design':
                response += "\n\n" + advisor.acknowledge_good_design('shard_aware_client')
        
        # Occasionally add a technical insight
        import random
        if random.random() < 0.2:  # 20% chance
            insight = advisor.technical_insight()
            if insight:
                response += f"\n\nNote: {insight}"
    
    return response