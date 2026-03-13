"""
Configuration module for the Fake Banking Data Generator.

This module stores all the constant values and configuration settings used
across the data generation and database loading processes. Centralizing
these settings ensures consistency and easy tweaking without altering logic.
"""

from decimal import Decimal

# -----------------------------
# Data Generation Volumes
# -----------------------------
# Number of unique customers to generate per iteration
NUM_CUSTOMERS: int = 10

# Number of bank accounts assigned to EACH customer
ACCOUNTS_PER_CUSTOMER: int = 2

# Total number of transactions generated per iteration
NUM_TRANSACTIONS: int = 50

# -----------------------------
# Financial Constraints
# -----------------------------
# Maximum amount for a single transaction (deposit, withdrawal, transfer)
MAX_TXN_AMOUNT: Decimal = Decimal("1000.00")

# Standard currency used for all accounts
CURRENCY: str = "USD"

# Range for the initial balance when a new account is created
INITIAL_BALANCE_MIN: Decimal = Decimal("10.00")
INITIAL_BALANCE_MAX: Decimal = Decimal("1000.00")

# -----------------------------
# Execution Flow Constraints
# -----------------------------
# Determines if the main script runs in an infinite loop by default
DEFAULT_LOOP: bool = True

# Wait time (in seconds) between each generation iteration
SLEEP_SECONDS: int = 2