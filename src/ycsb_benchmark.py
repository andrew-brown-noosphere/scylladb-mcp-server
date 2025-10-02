#!/usr/bin/env python3
"""YCSB Benchmark: DynamoDB vs ScyllaDB"""

import boto3
import time
import random
import string
import statistics
from concurrent.futures import ThreadPoolExecutor
import threading

# Configuration
import os
AWS_KEY = os.getenv('AWS_ACCESS_KEY_ID', 'your-aws-key')
AWS_SECRET = os.getenv('AWS_SECRET_ACCESS_KEY', 'your-aws-secret')
SCYLLA_ENDPOINT = os.getenv('SCYLLA_ALTERNATOR_ENDPOINT', 'http://your-scylla-endpoint:8000')

# YCSB Workload parameters
RECORD_COUNT = 1000
OPERATION_COUNT = 1000
READ_PROPORTION = 0.5  # 50% reads, 50% writes
FIELD_COUNT = 10
FIELD_LENGTH = 100

print("🔬 YCSB Benchmark: DynamoDB vs ScyllaDB")
print("=" * 50)
print(f"Records: {RECORD_COUNT}")
print(f"Operations: {OPERATION_COUNT}")
print(f"Read/Write: {int(READ_PROPORTION*100)}%/{int((1-READ_PROPORTION)*100)}%")
print(f"Fields per record: {FIELD_COUNT}")
print(f"Field size: {FIELD_LENGTH} bytes\n")

# Create clients
aws_client = boto3.resource('dynamodb',
    region_name='us-east-1',
    aws_access_key_id=AWS_KEY,
    aws_secret_access_key=AWS_SECRET
)

scylla_client = boto3.resource('dynamodb',
    endpoint_url=SCYLLA_ENDPOINT,
    region_name='us-east-1',
    aws_access_key_id='fake',
    aws_secret_access_key='fake'
)

def create_table(client, table_name):
    """Create YCSB table."""
    try:
        table = client.create_table(
            TableName=table_name,
            KeySchema=[
                {'AttributeName': 'id', 'KeyType': 'HASH'}
            ],
            AttributeDefinitions=[
                {'AttributeName': 'id', 'AttributeType': 'S'}
            ],
            BillingMode='PAY_PER_REQUEST'
        )
        return table
    except:
        return client.Table(table_name)

def generate_value(length):
    """Generate random string of specified length."""
    return ''.join(random.choices(string.ascii_letters + string.digits, k=length))

def generate_record(key):
    """Generate YCSB record."""
    record = {'id': f'user{key}'}
    for i in range(FIELD_COUNT):
        record[f'field{i}'] = generate_value(FIELD_LENGTH)
    return record

def load_data(table, start_key, end_key, results):
    """Load data into table."""
    latencies = []
    for key in range(start_key, end_key):
        record = generate_record(key)
        start = time.time()
        table.put_item(Item=record)
        latencies.append((time.time() - start) * 1000)
    results.extend(latencies)

def run_workload(table, operation_count, read_proportion, results):
    """Run YCSB workload."""
    latencies = []
    for _ in range(operation_count):
        key = random.randint(0, RECORD_COUNT - 1)
        
        if random.random() < read_proportion:
            # Read operation
            start = time.time()
            table.get_item(Key={'id': f'user{key}'})
            latencies.append((time.time() - start) * 1000)
        else:
            # Update operation
            record = generate_record(key)
            start = time.time()
            table.put_item(Item=record)
            latencies.append((time.time() - start) * 1000)
    results.extend(latencies)

def run_benchmark(client, platform_name):
    """Run complete benchmark for a platform."""
    print(f"\n📊 {platform_name} Benchmark")
    print("-" * 40)
    
    # Create table
    table_name = f'ycsb-{int(time.time())}'
    print(f"Creating table: {table_name}")
    table = create_table(client, table_name)
    
    # Wait for table to be ready
    print("Waiting for table to be active...")
    if platform_name == "AWS DynamoDB":
        try:
            table.wait_until_exists()
        except:
            pass
    else:
        time.sleep(5)
    
    # Load phase
    print(f"\n🔄 Load Phase: Inserting {RECORD_COUNT} records...")
    load_latencies = []
    load_start = time.time()
    
    # Use multiple threads for loading
    with ThreadPoolExecutor(max_workers=10) as executor:
        chunk_size = RECORD_COUNT // 10
        futures = []
        for i in range(10):
            start = i * chunk_size
            end = start + chunk_size if i < 9 else RECORD_COUNT
            thread_results = []
            future = executor.submit(load_data, table, start, end, thread_results)
            futures.append((future, thread_results))
        
        for future, thread_results in futures:
            future.result()
            load_latencies.extend(thread_results)
    
    load_time = time.time() - load_start
    
    # Run phase
    print(f"\n⚡ Run Phase: Executing {OPERATION_COUNT} operations...")
    run_latencies = []
    run_start = time.time()
    
    # Single thread for run phase (YCSB default)
    run_workload(table, OPERATION_COUNT, READ_PROPORTION, run_latencies)
    
    run_time = time.time() - run_start
    
    # Calculate statistics
    all_latencies = load_latencies + run_latencies
    
    results = {
        'load_time': load_time,
        'load_throughput': RECORD_COUNT / load_time,
        'run_time': run_time,
        'run_throughput': OPERATION_COUNT / run_time,
        'avg_latency': statistics.mean(all_latencies),
        'p50_latency': statistics.median(all_latencies),
        'p95_latency': sorted(all_latencies)[int(len(all_latencies) * 0.95)],
        'p99_latency': sorted(all_latencies)[int(len(all_latencies) * 0.99)],
    }
    
    # Print results
    print(f"\n📈 Results:")
    print(f"  Load throughput: {results['load_throughput']:.0f} ops/sec")
    print(f"  Run throughput: {results['run_throughput']:.0f} ops/sec")
    print(f"  Average latency: {results['avg_latency']:.1f} ms")
    print(f"  P50 latency: {results['p50_latency']:.1f} ms")
    print(f"  P95 latency: {results['p95_latency']:.1f} ms")
    print(f"  P99 latency: {results['p99_latency']:.1f} ms")
    
    return results

# Run benchmarks
aws_results = run_benchmark(aws_client, "AWS DynamoDB")
scylla_results = run_benchmark(scylla_client, "ScyllaDB Alternator")

# Compare results
print("\n" + "=" * 50)
print("🏆 COMPARISON SUMMARY")
print("=" * 50)

metrics = [
    ('Load Throughput', 'load_throughput', 'ops/sec'),
    ('Run Throughput', 'run_throughput', 'ops/sec'),
    ('Avg Latency', 'avg_latency', 'ms'),
    ('P95 Latency', 'p95_latency', 'ms'),
    ('P99 Latency', 'p99_latency', 'ms'),
]

for name, metric, unit in metrics:
    aws_val = aws_results[metric]
    scylla_val = scylla_results[metric]
    
    if 'throughput' in metric:
        improvement = (scylla_val / aws_val - 1) * 100
        better = scylla_val > aws_val
    else:  # Latency - lower is better
        improvement = (aws_val / scylla_val - 1) * 100
        better = scylla_val < aws_val
    
    print(f"\n{name}:")
    print(f"  AWS DynamoDB: {aws_val:.1f} {unit}")
    print(f"  ScyllaDB: {scylla_val:.1f} {unit}")
    if better:
        print(f"  🚀 ScyllaDB is {improvement:.0f}% better")
    else:
        print(f"  📉 ScyllaDB is {abs(improvement):.0f}% slower")

print("\n💡 Key Insights:")
print("- ScyllaDB typically shows better P99 latencies")
print("- Consistent performance without throttling")
print("- Same DynamoDB API, better price/performance")