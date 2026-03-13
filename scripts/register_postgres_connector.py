"""
Debezium PostgreSQL Connector Registration Script.

This module loads environment variables, constructs the configuration for a
Debezium PostgreSQL source connector, and registers it with a local Kafka Connect REST API.
"""

import os
import json
import logging
import requests
from typing import Dict, Any
from dotenv import load_dotenv

# Configure basic logging for better observability
logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")


def get_connector_config() -> Dict[str, Any]:
    """
    Constructs the JSON payload required to register the Debezium Postgres connector.

    Reads database connection parameters from environment variables and sets
    specific Debezium properties for capturing changes (CDC).

    Returns:
        Dict[str, Any]: A dictionary representing the connector configuration payload.
    """
    target_tables = "raw.customers,raw.accounts,raw.transactions"

    return {
        "name": "postgres-connector",
        "config": {
            "connector.class": "io.debezium.connector.postgresql.PostgresConnector",

            # Database connection details
            "database.hostname": os.getenv("POSTGRES_HOST_DOCKER"),
            "database.port": os.getenv("POSTGRES_PORT"),
            "database.user": os.getenv("POSTGRES_USER"),
            "database.password": os.getenv("POSTGRES_PASSWORD"),
            "database.dbname": os.getenv("POSTGRES_DB"),

            # Kafka topic routing and table selection
            "topic.prefix": "banking_server",
            "table.include.list": target_tables,

            # PostgreSQL specific CDC configurations
            "plugin.name": "pgoutput",  # Use standard Postgres logical decoding plugin
            "slot.name": "banking_slot",  # Unique replication slot name
            "publication.autocreate.mode": "filtered",  # Automatically create publication for included tables

            # Data transformation configurations
            "tombstones.on.delete": "false",  # Do not generate a tombstone event after a delete event
            "decimal.handling.mode": "double",  # Convert Postgres numeric/decimal to Kafka double
        },
    }


def register_connector(connect_url: str, config_payload: Dict[str, Any]) -> None:
    """
    Sends a POST request to the Kafka Connect REST API to register the connector.

    Args:
        connect_url (str): The endpoint URL of the Kafka Connect REST API.
        config_payload (Dict[str, Any]): The JSON configuration for the connector.

    Raises:
        requests.exceptions.RequestException: If the HTTP request fails entirely.
    """
    headers = {"Content-Type": "application/json"}

    try:
        response = requests.post(
            connect_url,
            headers=headers,
            data=json.dumps(config_payload),
            timeout=10  # Best practice: always set a timeout for network requests
        )

        # Handle specific HTTP status codes returned by Kafka Connect
        if response.status_code == 201:
            logging.info("Connector created successfully!")
        elif response.status_code == 409:
            logging.warning("Connector already exists. Skipping creation.")
        else:
            logging.error(f"Failed to create connector ({response.status_code}): {response.text}")

    except requests.exceptions.RequestException as e:
        logging.error(f"Network error while connecting to Kafka Connect: {e}")


def main() -> None:
    """
    Main entry point for the script.
    """
    # 1. Load environment variables
    dotenv_path = os.path.join(os.path.dirname(__file__), "..", ".env")
    load_dotenv(dotenv_path)

    # 2. Build configuration
    config = get_connector_config()

    # 3. Register connector
    kafka_connect_url = "http://localhost:8083/connectors"
    logging.info(f"Attempting to register connector at {kafka_connect_url}...")
    register_connector(kafka_connect_url, config)


if __name__ == "__main__":
    main()