"""
MinIO / S3 Client Module.

Handles interactions with MinIO, including bucket creation and
transforming Python dictionaries into Parquet files for cloud storage.
"""

import os
import boto3
import logging
import tempfile
import pandas as pd
from datetime import datetime
from typing import List, Dict, Any


def get_s3_client(endpoint_url: str, access_key: str, secret_key: str) -> boto3.client:
    """Initializes and returns a boto3 S3 client for MinIO."""
    return boto3.client(
        's3',
        endpoint_url=endpoint_url,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key
    )


def ensure_bucket_exists(s3_client: boto3.client, bucket_name: str) -> None:
    """
    Checks if the target bucket exists and creates it if it does not.

    Args:
        s3_client (boto3.client): The active S3 client.
        bucket_name (str): The name of the bucket to verify/create.
    """
    existing_buckets = [b['Name'] for b in s3_client.list_buckets().get('Buckets', [])]
    if bucket_name not in existing_buckets:
        logging.info(f"Bucket '{bucket_name}' not found. Creating it...")
        s3_client.create_bucket(Bucket=bucket_name)


def upload_records_as_parquet(s3_client: boto3.client, bucket_name: str, table_name: str,
                              records: List[Dict[str, Any]]) -> None:
    """
    Converts a list of dictionary records to a Parquet file and uploads it to MinIO.

    Uses Hive-style partitioning (date=YYYY-MM-DD) for optimized querying later (e.g., via Athena or Trino).

    Args:
        s3_client (boto3.client): The active S3 client.
        bucket_name (str): Target MinIO bucket.
        table_name (str): The logical name of the table (used for S3 prefixing).
        records (List[Dict[str, Any]]): The batch of CDC records to upload.
    """
    if not records:
        return

    df = pd.DataFrame(records)

    # Generate timestamp components for partitioning and uniqueness
    now = datetime.now()
    date_str = now.strftime('%Y-%m-%d')
    timestamp_str = now.strftime('%H%M%S%f')

    # Target S3 path with Hive-style partitioning
    s3_key = f'{table_name}/date={date_str}/{table_name}_{timestamp_str}.parquet'

    # Use a secure temporary directory to write the file before uploading
    with tempfile.TemporaryDirectory() as temp_dir:
        temp_file_path = os.path.join(temp_dir, f"{table_name}.parquet")

        # Write to local temp file
        df.to_parquet(temp_file_path, engine='fastparquet', index=False)

        # Upload to MinIO
        s3_client.upload_file(temp_file_path, bucket_name, s3_key)

    logging.info(f"Uploaded {len(records)} records to s3://{bucket_name}/{s3_key}")