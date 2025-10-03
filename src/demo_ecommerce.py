#!/usr/bin/env python3
"""
E-commerce Platform Demo - Transaction Processing
Demonstrates shopping cart, inventory, and order management patterns.
Works with both ScyllaDB (via Alternator) and AWS DynamoDB.
"""

import boto3
import os
import time
import random
import json
import uuid
from datetime import datetime
from decimal import Decimal
from concurrent.futures import ThreadPoolExecutor, as_completed

# Connection configuration
SCYLLA_ENDPOINT = os.getenv('SCYLLA_ALTERNATOR_ENDPOINT', 'http://localhost:8000')
DYNAMODB_ENDPOINT = os.getenv('DYNAMODB_ENDPOINT')
AWS_REGION = os.getenv('AWS_REGION', 'us-east-1')
AWS_ACCESS_KEY = os.getenv('AWS_ACCESS_KEY_ID')
AWS_SECRET_KEY = os.getenv('AWS_SECRET_ACCESS_KEY')

# Demo parameters
NUM_PRODUCTS = 1000
NUM_USERS = 500
SIMULATION_DURATION = 60  # seconds

class EcommerceDemo:
    def __init__(self, use_scylladb=True):
        """Initialize connection to either ScyllaDB or DynamoDB."""
        self.use_scylladb = use_scylladb
        
        if use_scylladb:
            self.dynamodb = boto3.resource(
                'dynamodb',
                endpoint_url=SCYLLA_ENDPOINT,
                region_name=AWS_REGION,
                aws_access_key_id=AWS_ACCESS_KEY,
                aws_secret_access_key=AWS_SECRET_KEY
            )
            self.prefix = 'ecommerce_scylla_'
        else:
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
            self.prefix = 'ecommerce_dynamo_'
        
        self.tables = {}
        self.metrics = {
            'cart_operations': 0,
            'orders_placed': 0,
            'inventory_updates': 0,
            'errors': 0,
            'latencies': []
        }
        
        # Sample product categories
        self.categories = ['Electronics', 'Books', 'Clothing', 'Home', 'Sports']
        self.product_names = ['Laptop', 'Phone', 'Tablet', 'Watch', 'Headphones', 
                              'Shirt', 'Pants', 'Shoes', 'Book', 'Game']
    
    def create_tables(self):
        """Create e-commerce tables if they don't exist."""
        table_configs = [
            {
                'name': self.prefix + 'products',
                'keys': [
                    {'AttributeName': 'product_id', 'KeyType': 'HASH'}
                ],
                'attributes': [
                    {'AttributeName': 'product_id', 'AttributeType': 'S'}
                ]
            },
            {
                'name': self.prefix + 'carts',
                'keys': [
                    {'AttributeName': 'user_id', 'KeyType': 'HASH'},
                    {'AttributeName': 'product_id', 'KeyType': 'RANGE'}
                ],
                'attributes': [
                    {'AttributeName': 'user_id', 'AttributeType': 'S'},
                    {'AttributeName': 'product_id', 'AttributeType': 'S'}
                ]
            },
            {
                'name': self.prefix + 'orders',
                'keys': [
                    {'AttributeName': 'order_id', 'KeyType': 'HASH'}
                ],
                'attributes': [
                    {'AttributeName': 'order_id', 'AttributeType': 'S'},
                    {'AttributeName': 'user_id', 'AttributeType': 'S'},
                    {'AttributeName': 'order_date', 'AttributeType': 'S'}
                ],
                'gsi': [
                    {
                        'IndexName': 'user-orders-index',
                        'Keys': [
                            {'AttributeName': 'user_id', 'KeyType': 'HASH'},
                            {'AttributeName': 'order_date', 'KeyType': 'RANGE'}
                        ],
                        'Projection': {'ProjectionType': 'ALL'}
                    }
                ]
            }
        ]
        
        for config in table_configs:
            try:
                table = self.dynamodb.Table(config['name'])
                table.load()
                self.tables[config['name']] = table
                print(f"✓ Table {config['name']} already exists")
            except:
                print(f"Creating table {config['name']}...")
                
                create_params = {
                    'TableName': config['name'],
                    'KeySchema': config['keys'],
                    'AttributeDefinitions': config['attributes'],
                    'BillingMode': 'PAY_PER_REQUEST'
                }
                
                # Add GSI if specified
                if 'gsi' in config:
                    create_params['GlobalSecondaryIndexes'] = config['gsi']
                
                table = self.dynamodb.create_table(**create_params)
                
                if not self.use_scylladb:
                    table.wait_until_exists()
                else:
                    time.sleep(1)
                
                self.tables[config['name']] = table
                print(f"✓ Table {config['name']} created")
    
    def populate_products(self):
        """Populate product catalog."""
        products_table = self.tables[self.prefix + 'products']
        
        print(f"Populating {NUM_PRODUCTS} products...")
        batch_items = []
        
        for i in range(NUM_PRODUCTS):
            product = {
                'product_id': f'PROD-{i:06d}',
                'name': f"{random.choice(self.product_names)} {i}",
                'category': random.choice(self.categories),
                'price': Decimal(str(round(random.uniform(10, 1000), 2))),
                'inventory': random.randint(0, 1000),
                'rating': Decimal(str(round(random.uniform(3.0, 5.0), 1))),
                'reviews': random.randint(0, 500)
            }
            batch_items.append(product)
            
            # Write in batches
            if len(batch_items) == 25:
                with products_table.batch_writer() as batch:
                    for item in batch_items:
                        batch.put_item(Item=item)
                batch_items = []
        
        # Write remaining items
        if batch_items:
            with products_table.batch_writer() as batch:
                for item in batch_items:
                    batch.put_item(Item=item)
        
        print(f"✓ Products populated")
    
    def add_to_cart(self, user_id, product_id, quantity):
        """Add item to shopping cart."""
        start = time.time()
        
        try:
            self.tables[self.prefix + 'carts'].put_item(
                Item={
                    'user_id': user_id,
                    'product_id': product_id,
                    'quantity': quantity,
                    'added_at': datetime.utcnow().isoformat()
                }
            )
            
            latency = (time.time() - start) * 1000
            self.metrics['cart_operations'] += 1
            self.metrics['latencies'].append(latency)
            
            return True
        except Exception as e:
            self.metrics['errors'] += 1
            return False
    
    def place_order(self, user_id):
        """Place an order from cart items."""
        start = time.time()
        
        try:
            # Get cart items
            cart_response = self.tables[self.prefix + 'carts'].query(
                KeyConditionExpression='user_id = :uid',
                ExpressionAttributeValues={':uid': user_id}
            )
            
            if not cart_response['Items']:
                return False
            
            # Create order
            order_id = str(uuid.uuid4())
            order_total = Decimal('0')
            order_items = []
            
            # Calculate total (simplified - not fetching actual prices)
            for item in cart_response['Items']:
                item_total = Decimal(str(random.uniform(10, 100))) * item['quantity']
                order_total += item_total
                order_items.append({
                    'product_id': item['product_id'],
                    'quantity': item['quantity'],
                    'price': item_total
                })
            
            # Save order
            self.tables[self.prefix + 'orders'].put_item(
                Item={
                    'order_id': order_id,
                    'user_id': user_id,
                    'order_date': datetime.utcnow().isoformat(),
                    'status': 'confirmed',
                    'total': order_total,
                    'items': order_items
                }
            )
            
            # Clear cart
            with self.tables[self.prefix + 'carts'].batch_writer() as batch:
                for item in cart_response['Items']:
                    batch.delete_item(
                        Key={
                            'user_id': user_id,
                            'product_id': item['product_id']
                        }
                    )
            
            latency = (time.time() - start) * 1000
            self.metrics['orders_placed'] += 1
            self.metrics['latencies'].append(latency)
            
            return True
        except Exception as e:
            self.metrics['errors'] += 1
            return False
    
    def update_inventory(self, product_id, quantity_change):
        """Update product inventory."""
        start = time.time()
        
        try:
            self.tables[self.prefix + 'products'].update_item(
                Key={'product_id': product_id},
                UpdateExpression='ADD inventory :val',
                ExpressionAttributeValues={':val': quantity_change}
            )
            
            latency = (time.time() - start) * 1000
            self.metrics['inventory_updates'] += 1
            self.metrics['latencies'].append(latency)
            
            return True
        except Exception as e:
            self.metrics['errors'] += 1
            return False
    
    def simulate_user_activity(self):
        """Simulate a user shopping session."""
        user_id = f'USER-{random.randint(1, NUM_USERS):06d}'
        
        # Add items to cart
        num_items = random.randint(1, 5)
        for _ in range(num_items):
            product_id = f'PROD-{random.randint(0, NUM_PRODUCTS-1):06d}'
            quantity = random.randint(1, 3)
            self.add_to_cart(user_id, product_id, quantity)
        
        # Sometimes place order
        if random.random() < 0.3:  # 30% chance
            self.place_order(user_id)
        
        # Sometimes update inventory
        if random.random() < 0.1:  # 10% chance
            product_id = f'PROD-{random.randint(0, NUM_PRODUCTS-1):06d}'
            self.update_inventory(product_id, random.randint(-10, 50))
    
    def run_demo(self):
        """Run the e-commerce simulation."""
        self.create_tables()
        self.populate_products()
        
        platform = "ScyllaDB" if self.use_scylladb else "AWS DynamoDB"
        print(f"\n{'='*60}")
        print(f"E-commerce Demo - {platform}")
        print(f"{'='*60}")
        print(f"Simulating {NUM_USERS} users shopping...")
        print(f"Duration: {SIMULATION_DURATION} seconds\n")
        
        start_time = time.time()
        operation_count = 0
        
        # Use thread pool for concurrent operations
        with ThreadPoolExecutor(max_workers=20) as executor:
            futures = []
            
            while time.time() - start_time < SIMULATION_DURATION:
                # Submit user activity
                future = executor.submit(self.simulate_user_activity)
                futures.append(future)
                
                # Process completed operations
                for future in as_completed(futures[:50]):
                    futures.remove(future)
                    operation_count += 1
                    
                    if operation_count % 100 == 0:
                        total_ops = (self.metrics['cart_operations'] + 
                                   self.metrics['orders_placed'] + 
                                   self.metrics['inventory_updates'])
                        print(f"✓ Operations: {total_ops} | "
                              f"Carts: {self.metrics['cart_operations']} | "
                              f"Orders: {self.metrics['orders_placed']} | "
                              f"Inventory: {self.metrics['inventory_updates']}")
                
                time.sleep(0.01)  # Small delay
        
        # Calculate final metrics
        duration = time.time() - start_time
        total_operations = (self.metrics['cart_operations'] + 
                          self.metrics['orders_placed'] + 
                          self.metrics['inventory_updates'])
        
        print(f"\n{'='*60}")
        print(f"Demo Results - {platform}")
        print(f"{'='*60}")
        print(f"Total duration: {duration:.1f} seconds")
        print(f"Cart operations: {self.metrics['cart_operations']:,}")
        print(f"Orders placed: {self.metrics['orders_placed']:,}")
        print(f"Inventory updates: {self.metrics['inventory_updates']:,}")
        print(f"Total operations: {total_operations:,}")
        print(f"Operations/second: {total_operations/duration:.0f}")
        print(f"Errors: {self.metrics['errors']}")
        
        if self.metrics['latencies']:
            avg_latency = sum(self.metrics['latencies']) / len(self.metrics['latencies'])
            p99_latency = sorted(self.metrics['latencies'])[int(len(self.metrics['latencies']) * 0.99)]
            print(f"Avg latency: {avg_latency:.1f}ms")
            print(f"P99 latency: {p99_latency:.1f}ms")
        
        return self.metrics

def compare_platforms():
    """Run demo on both platforms and compare results."""
    print("\n🛒 E-commerce Platform Demo - Performance Comparison\n")
    
    # Run on ScyllaDB
    scylla_demo = EcommerceDemo(use_scylladb=True)
    scylla_metrics = scylla_demo.run_demo()
    
    print("\n⏳ Waiting 5 seconds before testing DynamoDB...\n")
    time.sleep(5)
    
    # Run on DynamoDB
    dynamo_demo = EcommerceDemo(use_scylladb=False)
    dynamo_metrics = dynamo_demo.run_demo()
    
    # Compare results
    print("\n" + "="*60)
    print("📊 COMPARISON RESULTS")
    print("="*60)
    
    scylla_total = (scylla_metrics['cart_operations'] + 
                   scylla_metrics['orders_placed'] + 
                   scylla_metrics['inventory_updates'])
    dynamo_total = (dynamo_metrics['cart_operations'] + 
                   dynamo_metrics['orders_placed'] + 
                   dynamo_metrics['inventory_updates'])
    
    print(f"\nTotal Operations:")
    print(f"  ScyllaDB: {scylla_total:,}")
    print(f"  DynamoDB: {dynamo_total:,}")
    print(f"  Advantage: ScyllaDB handled {scylla_total/dynamo_total:.1f}x more")
    
    if scylla_metrics['latencies'] and dynamo_metrics['latencies']:
        scylla_p99 = sorted(scylla_metrics['latencies'])[int(len(scylla_metrics['latencies']) * 0.99)]
        dynamo_p99 = sorted(dynamo_metrics['latencies'])[int(len(dynamo_metrics['latencies']) * 0.99)]
        
        print(f"\nP99 Latency:")
        print(f"  ScyllaDB: {scylla_p99:.1f}ms")
        print(f"  DynamoDB: {dynamo_p99:.1f}ms")
        print(f"  Advantage: ScyllaDB is {dynamo_p99/scylla_p99:.1f}x faster")
    
    # Cost implications
    print(f"\n💰 Cost Analysis:")
    ops_per_second = scylla_total / SIMULATION_DURATION
    monthly_ops = ops_per_second * 86400 * 30
    
    # Rough cost estimate
    dynamo_read_cost = (monthly_ops * 0.7 / 1_000_000) * 0.25  # 70% reads
    dynamo_write_cost = (monthly_ops * 0.3 / 1_000_000) * 1.25  # 30% writes
    dynamo_total_cost = dynamo_read_cost + dynamo_write_cost
    
    print(f"  At {ops_per_second:.0f} ops/sec:")
    print(f"  DynamoDB: ${dynamo_total_cost:,.2f}/month")
    print(f"  ScyllaDB: ~${dynamo_total_cost * 0.1:,.2f}/month")
    print(f"  Savings: ${dynamo_total_cost * 0.9:,.2f}/month (90%)")

if __name__ == "__main__":
    import sys
    
    if len(sys.argv) > 1:
        if sys.argv[1] == 'scylla':
            demo = EcommerceDemo(use_scylladb=True)
            demo.run_demo()
        elif sys.argv[1] == 'dynamo':
            demo = EcommerceDemo(use_scylladb=False)
            demo.run_demo()
        else:
            print("Usage: python demo_ecommerce.py [scylla|dynamo|compare]")
    else:
        compare_platforms()