from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
import os

# PROJECT PATHS
# Get the absolute path to the project root
PROJECT_ROOT = "/mnt/c/Users/Kannan/OneDrive/Documents/enterprise-batch-etl-pipeline/enterprise-transaction-etl"

# DAG DEFAULT SETTINGS
default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# DAG DEFINITION
with DAG(
    dag_id="enterprise_transaction_etl",
    default_args=default_args,
    description="End-to-end batch ETL pipeline (Bronze → Silver → Gold)",
    schedule_interval="@daily",   # run once per day
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["etl", "batch", "spark"],
) as dag:

    #  INGESTION (BRONZE)
    ingest_raw_data = BashOperator(
        task_id="ingest_raw_transactions",
        bash_command=(
            f"cd {PROJECT_ROOT} && "
            f"python ingestion/ingest_raw.py "
            f"data/sample/online_retail.xlsx "
            f"{{{{ ds }}}}"  # Airflow template variable for execution date (YYYY-MM-DD)
        ),
    )

    #  TRANSFORMATION (SILVER)
    spark_transform = BashOperator(
        task_id="spark_transform_transactions",
        bash_command=f"cd {PROJECT_ROOT} && spark-submit transformations/spark_transform.py",
    )

    #  LOAD DIMENSIONS (GOLD)
    load_dimensions = BashOperator(
        task_id="load_dimensions",
        bash_command=f"cd {PROJECT_ROOT} && python warehouse/load_dimension.py",
    )

    #  LOAD FACT TABLE (GOLD)
    load_fact = BashOperator(
        task_id="load_fact_sales",
        bash_command=f"cd {PROJECT_ROOT} && python warehouse/load_fact.py",
    )

    #  DATA QUALITY CHECKS
    run_quality_checks = BashOperator(
        task_id="run_data_quality_checks",
        bash_command=f"cd {PROJECT_ROOT} && python quality/quality_checks.py",
    )

    # TASK DEPENDENCIES (ORDER)
    ingest_raw_data \
        >> spark_transform \
        >> load_dimensions \
        >> load_fact \
        >> run_quality_checks
