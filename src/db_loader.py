"""
Database Loader Module.

This module handles all database connections and data insertion logic.
It uses psycopg2's `execute_values` for high-performance batch inserts.
"""

import os
import psycopg2
from psycopg2.extras import execute_values
from typing import List, Tuple, Optional
from decimal import Decimal


def get_connection() -> psycopg2.extensions.connection:
    """
    Establishes a connection to the PostgreSQL database using environment variables.

    Returns:
        psycopg2.extensions.connection: An active database connection object.
    """
    return psycopg2.connect(
        host=os.getenv("POSTGRES_HOST"),
        port=os.getenv("POSTGRES_PORT"),
        dbname=os.getenv("POSTGRES_DB"),
        user=os.getenv("POSTGRES_USER"),
        password=os.getenv("POSTGRES_PASSWORD"),
    )


def insert_customers(cur: psycopg2.extensions.cursor, customer_data: List[Tuple[str, str, str]]) -> List[int]:
    """
    Batch inserts customers into the 'raw.customers' table.

    Args:
        cur (psycopg2.extensions.cursor): The active database cursor.
        customer_data (List[Tuple]): The customer records to insert.

    Returns:
        List[int]: A list of newly auto-generated Customer IDs from the database.
    """
    # The RETURNING clause is crucial here to retrieve the auto-incremented IDs
    query = """
            INSERT INTO raw.customers (first_name, last_name, email)
            VALUES %s RETURNING id \
            """
    records = execute_values(cur, query, customer_data, fetch=True)
    return [row[0] for row in records]


def insert_accounts(cur: psycopg2.extensions.cursor, account_data: List[Tuple[int, str, Decimal, str]]) -> List[int]:
    """
    Batch inserts accounts into the 'raw.accounts' table.

    Args:
        cur (psycopg2.extensions.cursor): The active database cursor.
        account_data (List[Tuple]): The account records to insert.

    Returns:
        List[int]: A list of newly auto-generated Account IDs from the database.
    """
    query = """
            INSERT INTO raw.accounts (customer_id, account_type, balance, currency)
            VALUES %s RETURNING id \
            """
    records = execute_values(cur, query, account_data, fetch=True)
    return [row[0] for row in records]


def insert_transactions(cur: psycopg2.extensions.cursor,
                        transaction_data: List[Tuple[int, str, Decimal, Optional[int], str]]) -> None:
    """
    Batch inserts transactions into the 'raw.transactions' table.

    Args:
        cur (psycopg2.extensions.cursor): The active database cursor.
        transaction_data (List[Tuple]): The transaction records to insert.

    Note:
        Does not return IDs since no downstream tables depend on transactions.
    """
    query = """
            INSERT INTO raw.transactions (account_id, txn_type, amount, related_account_id, status)
            VALUES %s \
            """
    execute_values(cur, query, transaction_data)