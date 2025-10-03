#!/usr/bin/env python3
"""
IoT Sensor Data Demo - Time-series data ingestion
Demonstrates high-volume sensor data writes typical in IoT applications.
Works with both ScyllaDB (via Alternator) and AWS DynamoDB.
"""

import boto3
import os
import time
import random
import json
from datetime import datetime, timedelta
from decimal import Decimal
from concurrent.futures import ThreadPoolExecutor, as_completed

# Connection configuration from environment
SCYLLA_ENDPOINT = os.getenv('SCYLLA_ALTERNATOR_ENDPOINT', 'http://localhost:8000')
DYNAMODB_ENDPOINT = os.getenv('DYNAMODB_ENDPOINT')  # None for real AWS
AWS_REGION = os.getenv('AWS_REGION', 'us-east-1')
AWS_ACCESS_KEY = os.getenv('AWS_ACCESS_KEY_ID')
AWS_SECRET_KEY = os.getenv('AWS_SECRET_ACCESS_KEY')

# Demo parameters
NUM_SENSORS = 100  # Number of IoT sensors
BATCH_SIZE = 25    # Items per batch write
DURATION_SECONDS = 60  # How long to run the demo

class IoTDemo:
    def __init__(self, use_scylladb=True):
        """Initialize connection to either ScyllaDB or DynamoDB."""
        self.use_scylladb = use_scylladb
        
        if use_scylladb:
            # Connect to ScyllaDB Alternator
            self.dynamodb = boto3.resource(
                'dynamodb',
                endpoint_url=SCYLLA_ENDPOINT,
                region_name=AWS_REGION,
                aws_access_key_id=AWS_ACCESS_KEY,
                aws_secret_access_key=AWS_SECRET_KEY
            )
            self.table_name = 'iot_sensor_data_scylla'
        else:
            # Connect to AWS DynamoDB
            if DYNAMODB_ENDPOINT:
                self.dynamodb = boto3.resource(
                    'dynamodb',
                    endpoint_url=DYNAMODB_ENDPOINT,
                    region_name=AWS_REGION
                )
            else:
                self.dynamodb = boto3.resource(
                    'dynamodb',
                    region_name=AWS_REGION
                )
            self.table_name = 'iot_sensor_data_dynamo'
        
        self.table = None
        self.metrics = {
            'writes': 0,
            'errors': 0,
            'latencies': []
        }
    
    def create_table(self):
        """Create the IoT sensor table if it doesn't exist."""
        try:
            # Check if table exists
            self.table = self.dynamodb.Table(self.table_name)
            self.table.load()
            print(f"✓ Table {self.table_name} already exists")
        except:
            # Create table
            print(f"Creating table {self.table_name}...")
            self.table = self.dynamodb.create_table(
                TableName=self.table_name,
                KeySchema=[
                    {'AttributeName': 'sensor_id', 'KeyType': 'HASH'},
                    {'AttributeName': 'timestamp', 'KeyType': 'RANGE'}
                ],
                AttributeDefinitions=[
                    {'AttributeName': 'sensor_id', 'AttributeType': 'S'},
                    {'AttributeName': 'timestamp', 'AttributeType': 'S'}
                ],
                BillingMode='PAY_PER_REQUEST'
            )
            
            # Wait for table to be created
            if not self.use_scylladb:
                self.table.wait_until_exists()
            else:
                time.sleep(2)  # ScyllaDB is faster
            
            print(f"✓ Table {self.table_name} created successfully")
    
    def generate_sensor_data(self, sensor_id):
        """Generate realistic sensor readings."""
        return {
            'sensor_id': f'sensor_{sensor_id:04d}',
            'timestamp': datetime.utcnow().isoformat() + 'Z',
            'temperature': Decimal(str(round(20 + random.gauss(0, 5), 2))),
            'humidity': Decimal(str(round(50 + random.gauss(0, 10), 2))),
            'pressure': Decimal(str(round(1013 + random.gauss(0, 20), 2))),
            'battery_level': Decimal(str(round(random.uniform(0, 100), 1))),
            'location': {
                'lat': Decimal(str(round(37.7749 + random.uniform(-0.1, 0.1), 6))),
                'lon': Decimal(str(round(-122.4194 + random.uniform(-0.1, 0.1), 6)))
            },
            'status': random.choice(['active', 'active', 'active', 'warning', 'maintenance'])
        }
    
    def batch_write_data(self, items):
        """Write a batch of sensor readings."""
        start = time.time()
        
        try:
            with self.table.batch_writer() as batch:
                for item in items:
                    batch.put_item(Item=item)
            
            latency = (time.time() - start) * 1000  # ms
            self.metrics['writes'] += len(items)
            self.metrics['latencies'].append(latency)
            
            return True, latency
        except Exception as e:
            self.metrics['errors'] += 1
            return False, str(e)
    
    def run_demo(self):
        """Run the IoT sensor data ingestion demo."""
        self.create_table()
        
        platform = "ScyllaDB" if self.use_scylladb else "AWS DynamoDB"
        print(f"\n{'='*60}")
        print(f"IoT Sensor Demo - {platform}")
        print(f"{'='*60}")
        print(f"Simulating {NUM_SENSORS} sensors sending data...")
        print(f"Duration: {DURATION_SECONDS} seconds")
        print(f"Batch size: {BATCH_SIZE} items\n")
        
        start_time = time.time()
        batch_count = 0
        
        # Use thread pool for concurrent writes
        with ThreadPoolExecutor(max_workers=10) as executor:
            futures = []
            
            while time.time() - start_time < DURATION_SECONDS:
                # Generate batch of sensor data
                batch_data = []
                for _ in range(BATCH_SIZE):
                    sensor_id = random.randint(0, NUM_SENSORS - 1)
                    batch_data.append(self.generate_sensor_data(sensor_id))
                
                # Submit batch write
                future = executor.submit(self.batch_write_data, batch_data)
                futures.append(future)
                
                # Process completed writes
                for future in as_completed(futures[:10]):  # Check first 10
                    futures.remove(future)
                    success, result = future.result()
                    
                    if success:
                        batch_count += 1
                        if batch_count % 10 == 0:
                            avg_latency = sum(self.metrics['latencies'][-10:]) / 10
                            print(f"✓ Batch {batch_count}: {self.metrics['writes']} items written, "
                                  f"avg latency: {avg_latency:.1f}ms")
                    else:
                        print(f"✗ Error: {result}")
                
                # Small delay to prevent overwhelming
                time.sleep(0.1)
        
        # Calculate final metrics
        duration = time.time() - start_time
        
        print(f"\n{'='*60}")
        print(f"Demo Results - {platform}")
        print(f"{'='*60}")
        print(f"Total duration: {duration:.1f} seconds")
        print(f"Total writes: {self.metrics['writes']:,} sensor readings")
        print(f"Write rate: {self.metrics['writes']/duration:.0f} items/second")
        print(f"Total errors: {self.metrics['errors']}")
        
        if self.metrics['latencies']:
            avg_latency = sum(self.metrics['latencies']) / len(self.metrics['latencies'])
            p99_latency = sorted(self.metrics['latencies'])[int(len(self.metrics['latencies']) * 0.99)]
            print(f"Avg latency: {avg_latency:.1f}ms")
            print(f"P99 latency: {p99_latency:.1f}ms")
        
        return self.metrics

def compare_platforms():
    """Run demo on both platforms and compare results."""
    print("\n🚀 IoT Sensor Data Demo - Platform Comparison\n")
    
    # Run on ScyllaDB
    scylla_demo = IoTDemo(use_scylladb=True)
    scylla_metrics = scylla_demo.run_demo()
    
    print("\n⏳ Waiting 5 seconds before testing DynamoDB...\n")
    time.sleep(5)
    
    # Run on DynamoDB
    dynamo_demo = IoTDemo(use_scylladb=False)
    dynamo_metrics = dynamo_demo.run_demo()
    
    # Compare results
    print("\n" + "="*60)
    print("📊 COMPARISON RESULTS")
    print("="*60)
    
    scylla_rate = scylla_metrics['writes'] / DURATION_SECONDS
    dynamo_rate = dynamo_metrics['writes'] / DURATION_SECONDS
    
    print(f"\nWrite Throughput:")
    print(f"  ScyllaDB: {scylla_rate:.0f} items/sec")
    print(f"  DynamoDB: {dynamo_rate:.0f} items/sec")
    print(f"  Advantage: ScyllaDB is {scylla_rate/dynamo_rate:.1f}x faster")
    
    if scylla_metrics['latencies'] and dynamo_metrics['latencies']:
        scylla_p99 = sorted(scylla_metrics['latencies'])[int(len(scylla_metrics['latencies']) * 0.99)]
        dynamo_p99 = sorted(dynamo_metrics['latencies'])[int(len(dynamo_metrics['latencies']) * 0.99)]
        
        print(f"\nP99 Latency:")
        print(f"  ScyllaDB: {scylla_p99:.1f}ms")
        print(f"  DynamoDB: {dynamo_p99:.1f}ms")
        print(f"  Advantage: ScyllaDB is {dynamo_p99/scylla_p99:.1f}x faster")
    
    print(f"\nReliability:")
    print(f"  ScyllaDB errors: {scylla_metrics['errors']}")
    print(f"  DynamoDB errors: {dynamo_metrics['errors']}")
    
    # Cost comparison
    print(f"\n💰 Cost Implications:")
    print(f"At this rate ({scylla_rate:.0f} writes/sec), monthly costs:")
    
    # Simple cost calculation
    writes_per_month = scylla_rate * 86400 * 30
    dynamo_cost = (writes_per_month / 1_000_000) * 1.25  # $1.25 per million writes
    
    print(f"  DynamoDB: ${dynamo_cost:,.2f}/month")
    print(f"  ScyllaDB: ~${dynamo_cost * 0.1:,.2f}/month (90% less)")
    print(f"  Monthly savings: ${dynamo_cost * 0.9:,.2f}")

if __name__ == "__main__":
    import sys
    
    if len(sys.argv) > 1:
        if sys.argv[1] == 'scylla':
            demo = IoTDemo(use_scylladb=True)
            demo.run_demo()
        elif sys.argv[1] == 'dynamo':
            demo = IoTDemo(use_scylladb=False)
            demo.run_demo()
        else:
            print("Usage: python demo_iot_sensors.py [scylla|dynamo|compare]")
    else:
        # Default: run comparison
        compare_platforms()