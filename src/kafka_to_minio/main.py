"""
Main Orchestrator for the Kafka-to-MinIO Pipeline.

Subscribes to Debezium CDC topics, buffers incoming records in memory,
and periodically flushes them to MinIO as Parquet files (Micro-batching).
"""

import logging
import sys

import config
import kafka_client
import minio_client

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s: %(message)s")


def main() -> None:
    """
    Main execution loop for consuming Kafka messages and writing to MinIO.
    """
    # 1. Initialize Clients
    s3_client = minio_client.get_s3_client(
        config.MINIO_ENDPOINT,
        config.MINIO_ACCESS_KEY,
        config.MINIO_SECRET_KEY
    )
    minio_client.ensure_bucket_exists(s3_client, config.MINIO_BUCKET)

    consumer = kafka_client.get_kafka_consumer(
        config.KAFKA_TOPICS,
        config.KAFKA_BOOTSTRAP,
        config.KAFKA_GROUP
    )

    # 2. Initialize In-Memory Buffers
    # Creates a dictionary with topic names as keys and empty lists as values
    buffer = {topic: [] for topic in config.KAFKA_TOPICS}

    logging.info("Connected to Kafka. Listening for messages...")

    try:
        # 3. Continuous Consumption Loop
        for message in consumer:
            print(f"DEBUG: Received message from topic {message.topic}")
            topic = message.topic
            event = message.value

            # Debezium wraps the actual row data inside payload -> after
            payload = event.get("payload", {})
            record = payload.get("after")

            # CDC Delete operations often have a null 'after' block
            if record:
                buffer[topic].append(record)
                logging.debug(f"[{topic}] Buffered 1 record. (Total: {len(buffer[topic])})")

            # Trigger upload if batch size is reached
            if len(buffer[topic]) >= config.BATCH_SIZE:
                # Extract the actual table name from the topic string (e.g., 'customers')
                table_name = topic.split('.')[-1]

                logging.info(f"Batch limit reached for {table_name}. Uploading...")
                minio_client.upload_records_as_parquet(
                    s3_client, config.MINIO_BUCKET, table_name, buffer[topic]
                )

                # Clear the buffer for this topic after successful upload
                buffer[topic] = []

    except KeyboardInterrupt:
        logging.warning("Process interrupted by user. Initiating graceful shutdown...")
    except Exception as e:
        logging.error(f"Unexpected error occurred: {e}")
    finally:
        # 4. Graceful Shutdown: Flush remaining buffers
        logging.info("Flushing remaining records in buffers to MinIO...")
        for topic, records in buffer.items():
            if records:
                table_name = topic.split('.')[-1]
                minio_client.upload_records_as_parquet(
                    s3_client, config.MINIO_BUCKET, table_name, records
                )

        # Close Kafka consumer to release socket connections
        consumer.close()
        logging.info("Graceful shutdown complete. Exiting.")
        sys.exit(0)


if __name__ == "__main__":
    main()