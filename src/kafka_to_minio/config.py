"""
Configuration module for the Kafka-to-MinIO data pipeline.

This module centralizes all environment variables and constant settings
required for consuming messages from Kafka and writing them to MinIO.
"""

import os
from dotenv import load_dotenv

# Load environment variables
dotenv_path = os.path.join(os.path.dirname(__file__), "..", "..", ".env")
load_dotenv(dotenv_path, verbose=True)

# -----------------------------
# Kafka Configuration
# -----------------------------
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
KAFKA_GROUP = os.getenv("KAFKA_GROUP", "minio_writer_group")

# List of Kafka topics to subscribe to
KAFKA_TOPICS = [
    'banking_server.raw.customers',
    'banking_server.raw.accounts',
    'banking_server.raw.transactions'
]

# Number of messages to buffer per topic before triggering an upload
BATCH_SIZE: int = 50

# -----------------------------
# MinIO / S3 Configuration
# -----------------------------
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://localhost:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")
MINIO_BUCKET = os.getenv("MINIO_BUCKET", "banking-data-lake")