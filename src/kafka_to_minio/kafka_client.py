"""
Kafka Client Module.

Handles the initialization and configuration of the Kafka consumer.
"""

import json
from kafka import KafkaConsumer
from typing import List

def get_kafka_consumer(topics: List[str], bootstrap_servers: str, group_id: str) -> KafkaConsumer:
    """
    Initializes a KafkaConsumer configured for Debezium JSON payloads.

    Args:
        topics (List[str]): List of topics to subscribe to.
        bootstrap_servers (str): Kafka broker connection string.
        group_id (str): Consumer group ID for offset management.

    Returns:
        KafkaConsumer: A configured Kafka consumer instance.
    """
    return KafkaConsumer(
        *topics,
        bootstrap_servers=bootstrap_servers,
        auto_offset_reset='earliest',  # Start from the beginning if no offset exists
        enable_auto_commit=True,       # Let Kafka handle offset tracking automatically
        group_id=group_id,
        value_deserializer=lambda x: json.loads(x.decode('utf-8')) # Decode Debezium JSON
    )