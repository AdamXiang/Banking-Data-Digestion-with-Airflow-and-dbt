"""
MinIO to Snowflake Ingestion Pipeline.

This DAG extracts Parquet files from a MinIO object storage (Data Lake) 
and loads them into Snowflake RAW tables. It utilizes the Airflow TaskFlow API 
for clean dependency management and implicit XCom data passing.
"""

import os
import logging
import boto3
import snowflake.connector
from typing import Dict, List
from datetime import datetime, timedelta

from airflow.decorators import dag, task

# ---------------
# Configuration |
# ---------------

# MinIO Config
MINIO_ENDPOINT = os.getenv("MINIO_AIRFLOW_ENDPOINT")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY")
BUCKET = os.getenv("MINIO_BUCKET")
LOCAL_DIR = os.getenv("MINIO_LOCAL_DIR", "/tmp/minio_downloads")

# Snowflake Config
SNOWFLAKE_USER = os.getenv("SNOWFLAKE_USER")
SNOWFLAKE_PASSWORD = os.getenv("SNOWFLAKE_PASSWORD")
SNOWFLAKE_ACCOUNT = os.getenv("SNOWFLAKE_ACCOUNT")
SNOWFLAKE_WAREHOUSE = os.getenv("SNOWFLAKE_WAREHOUSE")
SNOWFLAKE_DB = os.getenv("SNOWFLAKE_DB")
SNOWFLAKE_SCHEMA = os.getenv("SNOWFLAKE_SCHEMA")

TABLES = ["customers", "accounts", "transactions"]

# Setup Airflow standard logger
logger = logging.getLogger("airflow.task")

# -----------------------------
# DAG Definition
# -----------------------------
default_args = {
    "owner": "data_engineer",
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}


@dag(
    dag_id="minio_to_snowflake_banking",
    default_args=default_args,
    description="Load MinIO parquet into Snowflake RAW tables using TaskFlow API",
    schedule_interval="*/10 * * * *",
    start_date=datetime(2026, 3, 7),
    catchup=False,
    tags=["banking", "ingestion", "snowflake"],
)
def minio_to_snowflake_etl():
    """
    Main DAG function defining the workflow for MinIO to Snowflake ingestion.
    """

    @task(multiple_outputs=False)
    def download_from_minio() -> Dict[str, List[str]]:
        """
        Connects to MinIO, lists all Parquet files for the configured tables,
        and downloads them to a local temporary directory.

        Returns:
            Dict[str, List[str]]: A mapping of table names to their respective 
            downloaded local file paths.
        """
        logger.info(f"🔍 DEBUG ENDPOINT: {MINIO_ENDPOINT}")
        logger.info(f"🔍 DEBUG ACCESS_KEY: '{MINIO_ACCESS_KEY}'")
        logger.info(f"🔍 DEBUG SECRET_KEY: '{MINIO_SECRET_KEY}'")

        os.makedirs(LOCAL_DIR, exist_ok=True)

        s3 = boto3.client(
            "s3",
            endpoint_url=MINIO_ENDPOINT,
            aws_access_key_id=MINIO_ACCESS_KEY,
            aws_secret_access_key=MINIO_SECRET_KEY
        )

        local_files: Dict[str, List[str]] = {}

        for table in TABLES:
            prefix = f"{table}/"
            resp = s3.list_objects_v2(Bucket=BUCKET, Prefix=prefix)
            objects = resp.get("Contents", [])
            local_files[table] = []

            for obj in objects:
                key = obj["Key"]
                local_file = os.path.join(LOCAL_DIR, os.path.basename(key))

                s3.download_file(BUCKET, key, local_file)
                logger.info(f"Successfully downloaded {key} -> {local_file}")

                local_files[table].append(local_file)

        return local_files

    @task
    def load_to_snowflake(local_files: Dict[str, List[str]]) -> None:
        """
        Takes the dictionary of local file paths, uploads them to Snowflake 
        internal stages, and executes the COPY INTO command to load the data.

        Args:
            local_files (Dict[str, List[str]]): The output from `download_from_minio`.
        """
        if not local_files:
            logger.info("No files found in MinIO. Skipping load task.")
            return

        try:
            logger.info("Connecting to Snowflake...")
            conn = snowflake.connector.connect(
                user=SNOWFLAKE_USER,
                password=SNOWFLAKE_PASSWORD,
                account=SNOWFLAKE_ACCOUNT,
                warehouse=SNOWFLAKE_WAREHOUSE,
                database=SNOWFLAKE_DB,
                schema=SNOWFLAKE_SCHEMA,
            )

            # Use context manager for cursor to ensure it closes automatically
            with conn.cursor() as cur:
                for table, files in local_files.items():
                    if not files:
                        logger.info(f"No files for table '{table}', skipping.")
                        continue

                    # Step 1: Stage the files into Snowflake's internal stage
                    for f in files:
                        # Ensure path is compatible across OS
                        safe_path = f.replace('\\', '/')
                        cur.execute(f"PUT file://{safe_path} @%{table}")
                        logger.info(f"Uploaded {safe_path} to @%{table} stage.")

                    # Step 2: Load data from stage into the actual table
                    copy_sql = f"""
                    COPY INTO {table}
                    FROM @%{table}
                    FILE_FORMAT=(TYPE=PARQUET)
                    ON_ERROR='CONTINUE'
                    """
                    cur.execute(copy_sql)
                    logger.info(f"Data successfully loaded into table: {table}")

        except snowflake.connector.errors.Error as e:
            logger.error(f"Snowflake query failed: {e}")
            raise
        finally:
            # Always close the connection even if an error occurs
            if 'conn' in locals() and not conn.is_closed():
                conn.close()
                logger.info("Snowflake connection closed.")

    # -----------------------------
    # Task Dependencies
    # -----------------------------
    # In TaskFlow API, dependency is set implicitly by passing the output 
    # of one task directly as the input to the next.
    downloaded_files_dict = download_from_minio()
    load_to_snowflake(downloaded_files_dict)


# Instantiate the DAG
minio_to_snowflake_dag = minio_to_snowflake_etl()