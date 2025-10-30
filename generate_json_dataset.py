import json
import pandas as pd
import boto3
from datetime import datetime, timedelta
import random
import uuid
from io import BytesIO

def generate_sample_json_data(num_records=1000):
    """Generate sample JSON data similar to the structure in sample-view.sql"""
    
    data = []
    
    for i in range(num_records):
        # Generate random timestamps
        created_at = datetime.now() - timedelta(days=random.randint(1, 365))
        updated_at = created_at + timedelta(hours=random.randint(1, 24))
        purchase_date_ms = int(created_at.timestamp() * 1000)
        
        # Sample JSON structure
        json_data = {
            "_id": str(uuid.uuid4()),
            "gnId": f"gn_{random.randint(100000, 999999)}",
            "status": random.choice(["completed", "pending", "refunded", "cancelled"]),
            "type": random.choice(["purchase", "subscription", "renewal", "refund"]),
            "platform": random.choice(["ios", "android", "web"]),
            "appType": random.choice(["ios", "android", None]),
            "currency": random.choice(["USD", "EUR", "GBP", "JPY", "CAD"]),
            "amount": round(random.uniform(0.99, 99.99), 2),
            "purchaseDateMs": str(purchase_date_ms),
            "completedAt": created_at.strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
            "createdAt": created_at.strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
            "updatedAt": updated_at.strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
            "platformProductIdentifier": f"com.example.product_{random.randint(1, 100)}",
            "transactionId": f"txn_{uuid.uuid4().hex[:16]}",
            "ownerType": random.choice(["user", "family", "organization"]),
            "customerSubscription": f"sub_{uuid.uuid4().hex[:12]}",
            "marketplaceProductId": f"mp_{random.randint(1000, 9999)}",
            "expireDateMs": str(purchase_date_ms + random.randint(86400000, 31536000000)),  # 1 day to 1 year
            "originalTransaction": {
                "product_id": f"prod_{random.randint(1, 50)}",
                "original_transaction_id": f"orig_{uuid.uuid4().hex[:16]}",
                "transaction_id": f"txn_{uuid.uuid4().hex[:16]}",
                "is_trial_period": str(random.choice([True, False])).lower(),
                "is_in_intro_offer_period": str(random.choice([True, False])).lower(),
                "in_app_ownership_type": random.choice(["PURCHASED", "FAMILY_SHARED"]),
                "is_family_share": str(random.choice([True, False])).lower(),
                "purchase_date": created_at.strftime("%Y-%m-%d %H:%M:%S Etc/GMT"),
                "purchase_date_pst": created_at.strftime("%Y-%m-%d %H:%M:%S America/Los_Angeles"),
                "purchase_date_ms": str(purchase_date_ms),
                "original_purchase_date": created_at.strftime("%Y-%m-%d %H:%M:%S Etc/GMT"),
                "original_purchase_date_ms": str(purchase_date_ms),
                "original_purchase_date_pst": created_at.strftime("%Y-%m-%d %H:%M:%S America/Los_Angeles"),
                "type": random.choice(["Auto-Renewable Subscription", "Non-Consumable", "Consumable"]),
                "subscription_group_identifier": f"sub_group_{random.randint(1, 10)}",
                "expires_date_ms": str(purchase_date_ms + random.randint(86400000, 31536000000)),
                "expires_date": (created_at + timedelta(days=30)).strftime("%Y-%m-%d %H:%M:%S Etc/GMT"),
                "expires_date_pst": (created_at + timedelta(days=30)).strftime("%Y-%m-%d %H:%M:%S America/Los_Angeles"),
                "web_order_line_item_id": f"woli_{random.randint(100000, 999999)}",
                "transactionReason": random.choice(["PURCHASE", "RENEWAL", "UPGRADE"]),
                "signedDate": created_at.strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
                "quantity": str(random.randint(1, 5)),
                "price": str(round(random.uniform(0.99, 99.99), 2)),
                "currency": random.choice(["USD", "EUR", "GBP", "JPY", "CAD"]),
                "price_in_purchased_currency": str(round(random.uniform(0.99, 99.99), 2)),
                "offerDiscountType": random.choice(["INTRODUCTORY", "PROMOTIONAL", None]),
                "offerPeriod": random.choice(["P1W", "P1M", "P3M", None]),
                "offerType": random.choice(["1", "2", "3", None]),
                "store": random.choice(["app_store", "play_store", "web_store"]),
                "storefront": random.choice(["USA", "GBR", "JPN", "DEU", "FRA"]),
                "storefrontId": str(random.randint(143441, 143465)),
                "environment": random.choice(["Production", "Sandbox"]),
                "productId": f"prod_{random.randint(1, 50)}",
                "app_account_token": f"aat_{uuid.uuid4().hex[:20]}",
                "appTransactionId": f"app_txn_{uuid.uuid4().hex[:16]}",
                "app_user_id": f"user_{random.randint(100000, 999999)}",
                "purchaseDate": created_at.strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
                "originalPurchaseDate": created_at.strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
                "expiresDate": (created_at + timedelta(days=30)).strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
                "bundleId": f"com.example.app{random.randint(1, 10)}",
                "id": str(uuid.uuid4()),
                "event_timestamp_ms": str(purchase_date_ms),
                "extras": {
                    "isLifetimePurchase": str(random.choice([True, False])).lower()
                }
            },
            "extraInfo": {
                "countryCode": random.choice(["US", "GB", "JP", "DE", "FR", "CA", "AU"]),
                "transactionSource": random.choice(["app_store", "play_store", "direct"]),
                "isSandbox": str(random.choice([True, False])).lower(),
                "bundleTransactionId": f"bundle_{uuid.uuid4().hex[:16]}",
                "productPurchaseType": random.choice(["purchase", "rental", "subscription"]),
                "priceTier": str(random.randint(1, 10)),
                "trackInfo": {
                    "productName": f"Product {random.randint(1, 100)}",
                    "creatorName": f"Creator {random.randint(1, 50)}",
                    "productType": random.choice(["music", "video", "app", "book", "game"]),
                    "experiments": [{
                        "name": f"experiment_{random.randint(1, 20)}",
                        "userGroup": random.choice(["default", "true", "false"])
                    }]
                }
            }
        }
        
        # Convert to JSON string for the data column
        record = {
            "id": i + 1,
            "data": json.dumps(json_data),
            "created_date": created_at.strftime("%Y-%m-%d"),
            "year": created_at.year,
            "month": created_at.month
        }
        
        data.append(record)
    
    return data

def upload_to_s3_and_register_table():
    """Generate data, upload to S3, and register Glue table"""
    
    # Generate sample data
    print("Generating sample JSON data...")
    sample_data = generate_sample_json_data(1000)
    
    # Create DataFrame
    df = pd.DataFrame(sample_data)
    
    # Convert to Parquet
    print("Converting to Parquet...")
    parquet_buffer = BytesIO()
    df.to_parquet(parquet_buffer, index=False, engine='pyarrow')
    parquet_buffer.seek(0)
    
    # Upload to S3
    s3_client = boto3.client('s3')
    bucket_name = "aws-dbt-glue-datalake-020254235468-us-east-1"
    s3_key = "json-dataset/sample_json_data.parquet"
    
    print(f"Uploading to S3: s3://{bucket_name}/{s3_key}")
    s3_client.upload_fileobj(parquet_buffer, bucket_name, s3_key)
    
    # Create Glue database if it doesn't exist
    glue_client = boto3.client('glue')
    
    try:
        glue_client.create_database(
            DatabaseInput={
                'Name': 'dbt-glue-json-db',
                'Description': 'Database for JSON dataset testing'
            }
        )
        print("Created Glue database: dbt-glue-json-db")
    except glue_client.exceptions.AlreadyExistsException:
        print("Glue database dbt-glue-json-db already exists")
    
    # Register table in Glue catalog
    table_input = {
        'Name': 'sample_json_data',
        'Description': 'Sample dataset with JSON string column',
        'StorageDescriptor': {
            'Columns': [
                {'Name': 'id', 'Type': 'bigint'},
                {'Name': 'data', 'Type': 'string'},
                {'Name': 'created_date', 'Type': 'string'},
                {'Name': 'year', 'Type': 'bigint'},
                {'Name': 'month', 'Type': 'bigint'}
            ],
            'Location': f's3://{bucket_name}/json-dataset/',
            'InputFormat': 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat',
            'OutputFormat': 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat',
            'SerdeInfo': {
                'SerializationLibrary': 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
            }
        },
        'TableType': 'EXTERNAL_TABLE'
    }
    
    try:
        glue_client.create_table(
            DatabaseName='dbt-glue-json-db',
            TableInput=table_input
        )
        print("Created Glue table: dbt-glue-json-db.sample_json_data")
    except glue_client.exceptions.AlreadyExistsException:
        print("Glue table already exists, updating...")
        glue_client.update_table(
            DatabaseName='dbt-glue-json-db',
            TableInput=table_input
        )
        print("Updated Glue table: dbt-glue-json-db.sample_json_data")
    
    print(f"Dataset successfully created with {len(sample_data)} records")
    print(f"S3 Location: s3://{bucket_name}/json-dataset/")
    print("Glue Table: dbt-glue-json-db.sample_json_data")

if __name__ == "__main__":
    upload_to_s3_and_register_table()
