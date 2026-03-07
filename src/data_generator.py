"""
Data Generation Module.

This module is responsible for creating fake but structurally valid banking data.
It does not interact with the database, ensuring strict separation of concerns.
All monetary values are strictly handled as Decimals to prevent floating-point errors.
"""

import random
from decimal import Decimal, ROUND_DOWN
from typing import List, Tuple, Optional
from faker import Faker

import config


def generate_random_money(min_val: Decimal, max_val: Decimal) -> Decimal:
    """
    Generates a random monetary value within a specified range.

    Python's random.uniform only works with floats. To maintain precision for
    financial data, we cast to float, randomize, and safely cast back to Decimal.

    Args:
        min_val (Decimal): The minimum possible amount.
        max_val (Decimal): The maximum possible amount.

    Returns:
        Decimal: A random amount quantized to exactly two decimal places.
    """
    val = Decimal(str(random.uniform(float(min_val), float(max_val))))
    return val.quantize(Decimal("0.01"), rounding=ROUND_DOWN)


def generate_customers(fake: Faker, num_customers: int) -> List[Tuple[str, str, str]]:
    """
    Generates a list of fake customer records.

    Args:
        fake (Faker): An instance of the Faker library for data generation.
        num_customers (int): The number of customer records to generate.

    Returns:
        List[Tuple[str, str, str]]: A list containing tuples of 
        (first_name, last_name, email).
    """
    # Using fake.unique ensures no duplicate emails are generated in the same run
    return [
        (fake.first_name(), fake.last_name(), fake.unique.email())
        for _ in range(num_customers)
    ]


def generate_accounts(customer_ids: List[int], accounts_per_customer: int) -> List[Tuple[int, str, Decimal, str]]:
    """
    Generates bank account records linked to a given list of customer IDs.

    Args:
        customer_ids (List[int]): Valid customer IDs retrieved from the database.
        accounts_per_customer (int): Number of accounts to create per customer.

    Returns:
        List[Tuple[int, str, Decimal, str]]: A list containing tuples of 
        (customer_id, account_type, initial_balance, currency).
    """
    account_data = []
    for cid in customer_ids:
        for _ in range(accounts_per_customer):
            account_type = random.choice(["SAVINGS", "CHECKING"])
            initial_balance = generate_random_money(
                config.INITIAL_BALANCE_MIN,
                config.INITIAL_BALANCE_MAX
            )
            account_data.append((cid, account_type, initial_balance, config.CURRENCY))

    return account_data


def generate_transactions(account_ids: List[int], num_transactions: int) -> List[
    Tuple[int, str, Decimal, Optional[int], str]]:
    """
    Generates transaction records linked to a given list of account IDs.

    Args:
        account_ids (List[int]): Valid account IDs retrieved from the database.
        num_transactions (int): Total number of transactions to generate.

    Returns:
        List[Tuple[int, str, Decimal, Optional[int], str]]: A list of tuples containing
        (account_id, txn_type, amount, related_account_id, status).
    """
    txn_types = ["DEPOSIT", "WITHDRAWAL", "TRANSFER"]
    transaction_data = []

    for _ in range(num_transactions):
        # Randomly select a source account
        account_id = random.choice(account_ids)
        txn_type = random.choice(txn_types)
        amount = generate_random_money(Decimal("1.00"), config.MAX_TXN_AMOUNT)
        related_account = None

        # If it's a transfer, we must pick a distinct destination account
        if txn_type == "TRANSFER" and len(account_ids) > 1:
            available_targets = [a for a in account_ids if a != account_id]
            related_account = random.choice(available_targets)

        transaction_data.append((account_id, txn_type, amount, related_account, 'COMPLETED'))

    return transaction_data