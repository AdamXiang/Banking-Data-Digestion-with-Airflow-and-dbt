"""
DBT SCD2 Snapshot and Mart Processing Pipeline.

This DAG orchestrates dbt commands to capture Slowly Changing Dimensions (SCD Type 2) 
via `dbt snapshot`, followed by updating the downstream data marts via `dbt run`.
It utilizes the modern Airflow TaskFlow API (@dag, @task.bash) for cleaner syntax 
and better maintainability.
"""

from datetime import datetime, timedelta
from airflow.decorators import dag, task

# -----------------------------
# Configuration Constants
# -----------------------------
# Centralizing paths makes the DAG easier to maintain across different environments
DBT_PROJECT_DIR = "/opt/airflow/banking_dbt"
DBT_PROFILES_DIR = "/home/airflow/.dbt"

# Default arguments applied to all tasks within this DAG
DEFAULT_ARGS = {
    "owner": "data_engineer",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}


@dag(
    dag_id="SCD2_snapshots",
    default_args=DEFAULT_ARGS,
    description="Run dbt snapshots for SCD2 and update downstream marts",
    schedule_interval="@daily",
    start_date=datetime(2026, 3, 12),
    catchup=False,
    tags=["banking", "dbt", "snapshots", "scd2"],
)
def dbt_snapshot_pipeline():
    """
    Main DAG function defining the workflow for dbt snapshot and mart runs.
    """

    @task.bash
    def execute_dbt_snapshot() -> str:
        """
        Executes the `dbt snapshot` command.

        This task captures the current state of mutable raw tables to maintain 
        a historical record of changes (SCD Type 2) before running downstream models.

        Returns:
            str: The bash command string to be executed by the Airflow worker.
        """
        return f"cd {DBT_PROJECT_DIR} && dbt snapshot --profiles-dir {DBT_PROFILES_DIR}"

    @task.bash
    def execute_dbt_run_marts() -> str:
        """
        Executes the `dbt run` command specifically for the mart layer.

        This task updates the business-level data models (marts) ensuring they 
        reflect the latest data captured by the preceding snapshot task.

        Returns:
            str: The bash command string to be executed by the Airflow worker.
        """
        return f"cd {DBT_PROJECT_DIR} && dbt run --select mart --profiles-dir {DBT_PROFILES_DIR}"

    # -----------------------------
    # Task Dependencies (The Pipeline)
    # -----------------------------
    # Ensure snapshots are taken BEFORE the downstream marts are updated.
    # (Fixed the missing dependency from the original script)
    execute_dbt_snapshot() >> execute_dbt_run_marts()


# Instantiate the DAG
dbt_pipeline_dag = dbt_snapshot_pipeline()