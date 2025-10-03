#!/usr/bin/env python3
"""Test calculator accuracy against known ScyllaDB calculator results."""

from advanced_cost_calculator import ScyllaDBCostCalculator, WorkloadProfile, CostParameters
from calculator_from_csv import calculate_from_csv


def test_against_baseline_csv():
    """Test our calculator against the baselinePeak.csv from ScyllaDB."""
    
    print("Testing against baselinePeak.csv...")
    print("=" * 60)
    
    # Expected values from ScyllaDB calculator (you should verify these)
    # URL: https://calculator.scylladb.com/?pricing=demand&storageGB=512...
    expected = {
        'dynamodb_monthly': 897743.00,  # From the calculator
        'scylladb_monthly': None,  # Need actual value from calculator
        'instance_type': 'i3en.large',
        'instance_count': 36
    }
    
    result = calculate_from_csv('../baselinePeak.csv', storage_gb=512)
    print(result)
    
    # Extract numbers from result
    import re
    dynamodb_match = re.search(r'TOTAL: \$([0-9,]+\.[0-9]+)/month', result)
    scylladb_match = re.search(r'ScyllaDB Monthly Costs:.*?TOTAL: \$([0-9,]+\.[0-9]+)/month', result, re.DOTALL)
    
    if dynamodb_match:
        calculated_dynamodb = float(dynamodb_match.group(1).replace(',', ''))
        print(f"\nDynamoDB Monthly:")
        print(f"  Expected: ${expected['dynamodb_monthly']:,.2f}")
        print(f"  Calculated: ${calculated_dynamodb:,.2f}")
        print(f"  Difference: ${abs(expected['dynamodb_monthly'] - calculated_dynamodb):,.2f}")
        
    if scylladb_match:
        calculated_scylladb = float(scylladb_match.group(1).replace(',', ''))
        print(f"\nScyllaDB Monthly:")
        print(f"  Calculated: ${calculated_scylladb:,.2f}")


def test_simple_workload():
    """Test a simple workload we can manually verify."""
    
    print("\n\nTesting simple workload...")
    print("=" * 60)
    
    # Simple: 1000 reads/sec, 500 writes/sec, steady (no peaks)
    calculator = ScyllaDBCostCalculator()
    
    workload = WorkloadProfile(
        baseline_reads=1000,
        baseline_writes=500,
        peak_reads=1000,
        peak_writes=500,
        peak_read_hours=[],
        peak_write_hours=[]
    )
    
    params = CostParameters(
        storage_gb=100,
        item_size_bytes=1024
    )
    
    # Calculate DynamoDB
    dynamodb_costs = calculator.calculate_dynamodb_cost(workload, params)
    
    # Manual verification:
    # 1000 reads/sec * 86400 sec/day * 30.5 days = 2,635,200,000 reads/month
    # 500 writes/sec * 86400 sec/day * 30.5 days = 1,317,600,000 writes/month
    # Read cost: 2,635,200,000 * $0.25/1M = $658.80
    # Write cost: 1,317,600,000 * $1.25/1M = $1,647.00
    # Storage: 100GB * $0.25 = $25.00
    # Total: $2,330.80
    
    print(f"DynamoDB Costs:")
    print(f"  Reads: ${dynamodb_costs['read_cost']:,.2f} (expected: $658.80)")
    print(f"  Writes: ${dynamodb_costs['write_cost']:,.2f} (expected: $1,647.00)")
    print(f"  Storage: ${dynamodb_costs['storage_cost']:,.2f} (expected: $25.00)")
    print(f"  Total: ${dynamodb_costs['total']:,.2f} (expected: $2,330.80)")
    
    # Calculate ScyllaDB
    scylladb_costs = calculator.calculate_scylladb_cost(workload, params)
    
    print(f"\nScyllaDB Costs:")
    print(f"  Instance: {scylladb_costs['instance_count']}x {scylladb_costs['instance_type']}")
    print(f"  Compute: ${scylladb_costs['compute_cost']:,.2f}")
    print(f"  Storage: ${scylladb_costs['storage_cost']:,.2f}")
    print(f"  Network: ${scylladb_costs['network_cost']:,.2f}")
    print(f"  Total: ${scylladb_costs['total']:,.2f}")


def test_calculator_url():
    """Test that our generated URLs match the expected format."""
    
    print("\n\nTesting URL generation...")
    print("=" * 60)
    
    calculator = ScyllaDBCostCalculator()
    
    workload = WorkloadProfile(
        baseline_reads=100000,
        baseline_writes=200000,
        peak_reads=250000,
        peak_writes=500000,
        peak_read_hours=[11, 12],
        peak_write_hours=[10, 11, 12, 13]
    )
    
    params = CostParameters(storage_gb=512)
    
    url = calculator.generate_calculator_url(workload, params)
    print(f"Generated URL: {url[:200]}...")
    
    # Check key parameters in URL
    assert 'baselineReads=100000' in url
    assert 'baselineWrites=200000' in url
    assert 'peakReads=250000' in url
    assert 'peakWrites=500000' in url
    assert 'storageGB=512' in url
    print("✓ URL parameters correct")


if __name__ == "__main__":
    test_against_baseline_csv()
    test_simple_workload()
    test_calculator_url()