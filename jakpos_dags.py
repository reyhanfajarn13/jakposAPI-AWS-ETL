from datetime import timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from datetime import datetime
from api_etl import run_jakpos_etl

defaults_args = {
    'owner':'reyhan',
    'depends_on_past': False,
    'start_date': datetime(2020, 11, 8),
    'email':['reyhanfajarn13@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}

dag = DAG(
    'jakpos_dag',
    default_args=defaults_args,
    description='First ETL code'
)

run_etl = PythonOperator(
    task_id = 'complete_jakpos_etl',
    python_callable=run_jakpos_etl,
    dag=dag,
)

run_etl