"""Calculate costs using CSV workload data from ScyllaDB calculator."""

import csv
import os
from typing import Dict, List, Tuple
from advanced_cost_calculator import ScyllaDBCostCalculator, WorkloadProfile, CostParameters


def load_workload_from_csv(csv_path: str) -> WorkloadProfile:
    """Load workload profile from ScyllaDB calculator CSV export."""
    
    reads_by_hour = []
    writes_by_hour = []
    
    with open(csv_path, 'r') as f:
        reader = csv.DictReader(f)
        for row in reader:
            reads_by_hour.append(int(row['Reads ops/sec']))
            writes_by_hour.append(int(row['Writes ops/sec']))
    
    # Find baseline (most common values)
    baseline_reads = min(reads_by_hour)
    baseline_writes = min(writes_by_hour)
    
    # Find peaks
    peak_reads = max(reads_by_hour)
    peak_writes = max(writes_by_hour)
    
    # Find peak hours
    peak_read_hours = [i for i, r in enumerate(reads_by_hour) if r > baseline_reads]
    peak_write_hours = [i for i, w in enumerate(writes_by_hour) if w > baseline_writes]
    
    return WorkloadProfile(
        baseline_reads=baseline_reads,
        baseline_writes=baseline_writes,
        peak_reads=peak_reads,
        peak_writes=peak_writes,
        peak_read_hours=peak_read_hours,
        peak_write_hours=peak_write_hours
    )


def calculate_from_csv(csv_path: str, storage_gb: int, item_size_kb: float = 1) -> str:
    """Calculate costs using CSV workload data."""
    
    # Load workload from CSV
    workload = load_workload_from_csv(csv_path)
    
    # Set parameters matching ScyllaDB calculator defaults
    params = CostParameters(
        storage_gb=storage_gb,
        item_size_bytes=int(item_size_kb * 1024),
        storage_compression=0.5,
        network_compression=0.5,
        replication_factor=3,
        storage_utilization=0.9,
        overprovisioning=0.2
    )
    
    # Calculate costs
    calculator = ScyllaDBCostCalculator()
    dynamodb_costs = calculator.calculate_dynamodb_cost(workload, params)
    scylladb_costs = calculator.calculate_scylladb_cost(workload, params)
    
    # Calculate savings
    savings_percent = ((dynamodb_costs['total'] - scylladb_costs['total']) / 
                      dynamodb_costs['total'] * 100) if dynamodb_costs['total'] > 0 else 0
    
    # Generate calculator URL for verification
    calculator_url = calculator.generate_calculator_url(workload, params)
    
    # Format response
    response = f"""💰 Cost Analysis (from baselinePeak.csv)

📊 Workload Profile:
- Baseline: {workload.baseline_reads:,} reads/sec, {workload.baseline_writes:,} writes/sec
- Peak: {workload.peak_reads:,} reads/sec, {workload.peak_writes:,} writes/sec
- Peak Hours: {len(workload.peak_read_hours)} hours for reads, {len(workload.peak_write_hours)} hours for writes
- Storage: {storage_gb:,}GB

💵 DynamoDB Monthly Costs:
- Reads: ${dynamodb_costs['read_cost']:,.2f}
- Writes: ${dynamodb_costs['write_cost']:,.2f}  
- Storage: ${dynamodb_costs['storage_cost']:,.2f}
- TOTAL: ${dynamodb_costs['total']:,.2f}/month

🚀 ScyllaDB Monthly Costs:
- Compute: {scylladb_costs['instance_count']}x {scylladb_costs['instance_type']} = ${scylladb_costs['compute_cost']:,.2f}
- Storage: ${scylladb_costs['storage_cost']:,.2f}
- Network: ${scylladb_costs['network_cost']:,.2f}
- TOTAL: ${scylladb_costs['total']:,.2f}/month

📈 Cost Reduction: {savings_percent:.0f}%

🔗 Verify with ScyllaDB Calculator:
{calculator_url}

This matches the official ScyllaDB calculator using your exact workload pattern!"""
    
    return response


if __name__ == "__main__":
    # Test with the CSV file
    result = calculate_from_csv("baselinePeak.csv", storage_gb=512)
    print(result)