from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'varshitha',
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'email_on_failure': False,
}

def run_bronze():
    print("Running Bronze ingestion...")
    print(" Bronze complete.")

def run_silver():
    print("Running Silver transformation...")
    print(" Silver complete.")

def run_gold():
    print("Running Gold aggregation...")
    print(" Gold complete.")

def run_data_quality():
    print("Running data quality checks...")
    print(" Data quality passed.")

with DAG(
    dag_id="retail_medallion_pipeline",
    default_args=default_args,
    description="Retail data pipeline: Bronze → Silver → Gold",
    schedule_interval="0 6 * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["retail", "medallion", "data-engineering"],
) as dag:

    bronze_task = PythonOperator(
        task_id="bronze_ingestion",
        python_callable=run_bronze,
    )

    silver_task = PythonOperator(
        task_id="silver_transformation",
        python_callable=run_silver,
    )

    gold_task = PythonOperator(
        task_id="gold_aggregation",
        python_callable=run_gold,
    )

    dq_task = PythonOperator(
        task_id="data_quality_check",
        python_callable=run_data_quality,
    )

    bronze_task >> silver_task >> gold_task >> dq_task
