"""
Main Orchestrator Module for the Fake Banking Data Generator.

This script acts as the orchestrator. It fetches generated data from `data_generator.py`,
passes it to `db_loader.py` for insertion, retrieves the generated primary keys,
and uses them to generate dependent downstream data (Accounts and Transactions).
"""

import argparse
import sys
import time
import psycopg2
from dotenv import load_dotenv
from faker import Faker
import os

# Import custom modules
from src.data_generator import data_generator, config, db_loader


def parse_arguments() -> argparse.Namespace:
    """
    Parses command-line arguments for the application.

    Returns:
        argparse.Namespace: Parsed arguments (e.g., --once flag).
    """
    parser = argparse.ArgumentParser(description="Run fake banking data generator")
    parser.add_argument(
        "--once",
        action="store_true",
        help="Run a single iteration and exit instead of looping continuously"
    )
    return parser.parse_args()


def main() -> None:
    """
    Main execution loop.

    Handles the database connection context, manages the continuous generation
    loop, coordinates the data flow between modules, and commits transactions.
    """
    # 1. Initialization and Setup
    dotenv_path = os.path.join(os.path.dirname(__file__), "..", "..", ".env")
    load_dotenv(dotenv_path, verbose=True)
    args = parse_arguments()
    loop_active = not args.once and config.DEFAULT_LOOP
    fake = Faker()

    try:
        print("Connecting to PostgreSQL...")
        conn = db_loader.get_connection()

        # Context manager ensures connection resources are handled cleanly
        with conn:
            with conn.cursor() as cur:
                iteration = 0

                while True:
                    iteration += 1
                    print(f"\n--- Iteration {iteration} started ---")

                    # Step 1: Generate & Insert Customers
                    # We must do this first to obtain the new Customer IDs for the Accounts.
                    customer_data = data_generator.generate_customers(fake, config.NUM_CUSTOMERS)
                    customer_ids = db_loader.insert_customers(cur, customer_data)

                    # Step 2: Generate & Insert Accounts
                    # Using the IDs from Step 1, generate Accounts and get Account IDs.
                    account_data = data_generator.generate_accounts(customer_ids, config.ACCOUNTS_PER_CUSTOMER)
                    account_ids = db_loader.insert_accounts(cur, account_data)

                    # Step 3: Generate & Insert Transactions
                    # Using the IDs from Step 2, simulate transactions between them.
                    transaction_data = data_generator.generate_transactions(account_ids, config.NUM_TRANSACTIONS)
                    db_loader.insert_transactions(cur, transaction_data)

                    # Step 4: Commit transaction to Database
                    # This safely writes the entire batch to disk.
                    conn.commit()

                    print(
                        f"Generated {len(customer_ids)} customers, {len(account_ids)} accounts, {config.NUM_TRANSACTIONS} transactions.")
                    print(f"--- Iteration {iteration} finished ---")

                    # Clear Faker's uniqueness registry to avoid MemoryErrors over long runs
                    fake.unique.clear()

                    # Check exit condition
                    if not loop_active:
                        break

                    # Pause before the next iteration
                    time.sleep(config.SLEEP_SECONDS)


    except psycopg2.OperationalError as e:
        print(f"Database connection failed: {e}")
        sys.exit(1)
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
        sys.exit(1)
    except KeyboardInterrupt:
        print("\nInterrupted by user. Exiting gracefully...")
    finally:
        # Cleanup: Ensure connection is closed even if an error occurs
        if 'conn' in locals() and not conn.closed:
            conn.close()
            print("Database connection closed.")

if __name__ == "__main__":
    main()