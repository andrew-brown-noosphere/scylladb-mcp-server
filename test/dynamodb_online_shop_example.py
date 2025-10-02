#!/usr/bin/env python3
"""
DynamoDB Online Shop Example
Based on AWS DynamoDB Design Patterns repository
https://github.com/aws-samples/amazon-dynamodb-design-patterns

This represents a typical e-commerce application with:
- Users
- Products
- Orders
- Shopping Cart
- Inventory tracking
"""

import boto3
from datetime import datetime
from decimal import Decimal
import json

# Initialize DynamoDB client
dynamodb = boto3.resource('dynamodb', region_name='us-east-1')

# Table design following single-table pattern
TABLE_NAME = 'OnlineShop'

def create_table():
    """Create the single table with GSIs for access patterns"""
    table = dynamodb.create_table(
        TableName=TABLE_NAME,
        KeySchema=[
            {'AttributeName': 'PK', 'KeyType': 'HASH'},  # Partition key
            {'AttributeName': 'SK', 'KeyType': 'RANGE'}  # Sort key
        ],
        AttributeDefinitions=[
            {'AttributeName': 'PK', 'AttributeType': 'S'},
            {'AttributeName': 'SK', 'AttributeType': 'S'},
            {'AttributeName': 'GSI1PK', 'AttributeType': 'S'},
            {'AttributeName': 'GSI1SK', 'AttributeType': 'S'},
            {'AttributeName': 'GSI2PK', 'AttributeType': 'S'},
            {'AttributeName': 'GSI2SK', 'AttributeType': 'S'},
        ],
        GlobalSecondaryIndexes=[
            {
                'IndexName': 'GSI1',
                'KeySchema': [
                    {'AttributeName': 'GSI1PK', 'KeyType': 'HASH'},
                    {'AttributeName': 'GSI1SK', 'KeyType': 'RANGE'}
                ],
                'Projection': {'ProjectionType': 'ALL'},
                'ProvisionedThroughput': {'ReadCapacityUnits': 5, 'WriteCapacityUnits': 5}
            },
            {
                'IndexName': 'GSI2',
                'KeySchema': [
                    {'AttributeName': 'GSI2PK', 'KeyType': 'HASH'},
                    {'AttributeName': 'GSI2SK', 'KeyType': 'RANGE'}
                ],
                'Projection': {'ProjectionType': 'ALL'},
                'ProvisionedThroughput': {'ReadCapacityUnits': 5, 'WriteCapacityUnits': 5}
            }
        ],
        ProvisionedThroughput={'ReadCapacityUnits': 10, 'WriteCapacityUnits': 10}
    )
    return table

def add_user(user_id, email, name):
    """Add a user to the system"""
    table = dynamodb.Table(TABLE_NAME)
    table.put_item(
        Item={
            'PK': f'USER#{user_id}',
            'SK': f'USER#{user_id}',
            'GSI1PK': f'USER#{email}',
            'GSI1SK': f'USER#{user_id}',
            'Type': 'User',
            'UserID': user_id,
            'Email': email,
            'Name': name,
            'CreatedAt': datetime.utcnow().isoformat()
        }
    )

def add_product(product_id, name, price, category, inventory):
    """Add a product to the catalog"""
    table = dynamodb.Table(TABLE_NAME)
    table.put_item(
        Item={
            'PK': f'PRODUCT#{product_id}',
            'SK': f'PRODUCT#{product_id}',
            'GSI1PK': f'CATEGORY#{category}',
            'GSI1SK': f'PRODUCT#{product_id}',
            'Type': 'Product',
            'ProductID': product_id,
            'Name': name,
            'Price': Decimal(str(price)),
            'Category': category,
            'Inventory': inventory,
            'CreatedAt': datetime.utcnow().isoformat()
        }
    )

def create_order(user_id, order_id, items):
    """Create an order with multiple items (transaction)"""
    table = dynamodb.Table(TABLE_NAME)
    
    # Calculate total
    total = sum(Decimal(str(item['price'])) * item['quantity'] for item in items)
    
    # Use transaction to ensure consistency
    with table.batch_writer() as batch:
        # Order header
        batch.put_item(
            Item={
                'PK': f'USER#{user_id}',
                'SK': f'ORDER#{order_id}',
                'GSI1PK': f'ORDER#{order_id}',
                'GSI1SK': f'ORDER#{order_id}',
                'Type': 'Order',
                'OrderID': order_id,
                'UserID': user_id,
                'Status': 'PENDING',
                'Total': total,
                'CreatedAt': datetime.utcnow().isoformat()
            }
        )
        
        # Order items
        for item in items:
            batch.put_item(
                Item={
                    'PK': f'ORDER#{order_id}',
                    'SK': f'ITEM#{item["product_id"]}',
                    'Type': 'OrderItem',
                    'ProductID': item['product_id'],
                    'ProductName': item['name'],
                    'Price': Decimal(str(item['price'])),
                    'Quantity': item['quantity']
                }
            )

def add_to_cart(user_id, product_id, quantity):
    """Add item to shopping cart"""
    table = dynamodb.Table(TABLE_NAME)
    table.put_item(
        Item={
            'PK': f'USER#{user_id}',
            'SK': f'CART#{product_id}',
            'Type': 'CartItem',
            'ProductID': product_id,
            'Quantity': quantity,
            'AddedAt': datetime.utcnow().isoformat()
        }
    )

def get_user_orders(user_id):
    """Get all orders for a user"""
    table = dynamodb.Table(TABLE_NAME)
    response = table.query(
        KeyConditionExpression='PK = :pk AND begins_with(SK, :sk)',
        ExpressionAttributeValues={
            ':pk': f'USER#{user_id}',
            ':sk': 'ORDER#'
        }
    )
    return response['Items']

def get_products_by_category(category):
    """Get all products in a category using GSI"""
    table = dynamodb.Table(TABLE_NAME)
    response = table.query(
        IndexName='GSI1',
        KeyConditionExpression='GSI1PK = :pk',
        ExpressionAttributeValues={
            ':pk': f'CATEGORY#{category}'
        }
    )
    return response['Items']

def update_inventory(product_id, quantity_change):
    """Update product inventory (atomic counter)"""
    table = dynamodb.Table(TABLE_NAME)
    response = table.update_item(
        Key={
            'PK': f'PRODUCT#{product_id}',
            'SK': f'PRODUCT#{product_id}'
        },
        UpdateExpression='SET Inventory = Inventory + :change',
        ExpressionAttributeValues={
            ':change': quantity_change
        },
        ReturnValues='UPDATED_NEW'
    )
    return response

def get_hot_products():
    """Simulate hot partition - everyone queries for featured products"""
    table = dynamodb.Table(TABLE_NAME)
    # This creates a hot partition on CATEGORY#featured
    response = table.query(
        IndexName='GSI1',
        KeyConditionExpression='GSI1PK = :pk',
        ExpressionAttributeValues={
            ':pk': 'CATEGORY#featured'
        }
    )
    return response['Items']

def bulk_update_prices(category, percentage_change):
    """Bulk update prices for a category - inefficient pattern"""
    table = dynamodb.Table(TABLE_NAME)
    
    # First, get all products in category
    products = get_products_by_category(category)
    
    # Then update each one individually (anti-pattern)
    for product in products:
        table.update_item(
            Key={
                'PK': product['PK'],
                'SK': product['SK']
            },
            UpdateExpression='SET Price = Price * :factor',
            ExpressionAttributeValues={
                ':factor': Decimal(1 + percentage_change/100)
            }
        )

# Example usage patterns
if __name__ == "__main__":
    # Sample data
    print("Creating sample e-commerce data...")
    
    # Users
    add_user("user123", "john@example.com", "John Doe")
    add_user("user456", "jane@example.com", "Jane Smith")
    
    # Products
    add_product("prod001", "Laptop", 999.99, "electronics", 50)
    add_product("prod002", "Mouse", 29.99, "electronics", 200)
    add_product("prod003", "T-Shirt", 19.99, "clothing", 100)
    add_product("prod004", "iPhone", 1199.99, "featured", 25)  # Hot item
    
    # Shopping cart
    add_to_cart("user123", "prod001", 1)
    add_to_cart("user123", "prod002", 2)
    
    # Order
    create_order("user123", "order001", [
        {"product_id": "prod001", "name": "Laptop", "price": 999.99, "quantity": 1},
        {"product_id": "prod002", "name": "Mouse", "price": 29.99, "quantity": 2}
    ])
    
    # Queries
    print("\nUser orders:", get_user_orders("user123"))
    print("\nElectronics:", get_products_by_category("electronics"))
    
    # Anti-patterns
    print("\nHot partition access (featured products)...")
    for _ in range(100):  # Simulate many requests
        get_hot_products()
    
    print("\nBulk price update (inefficient)...")
    bulk_update_prices("electronics", 10)  # 10% increase