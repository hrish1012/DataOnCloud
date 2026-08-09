from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    "owner": "dataoncloud",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="dataoncloud_ingestion_pipeline",
    default_args=default_args,
    description="Runs the DataOnCloud ingestion framework across all configured sources",
    schedule_interval="@hourly",
    start_date=datetime(2026, 8, 1),
    catchup=False,
    tags=["dataoncloud", "ingestion"],
) as dag:

    run_ingestion = BashOperator(
        task_id="run_ingestion_framework",
        bash_command="cd /opt/airflow/project && python run_test.py",
    )