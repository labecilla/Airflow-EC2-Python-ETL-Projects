from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from spotify_etl import run_spotify_etl

default_args={
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2020, 11, 8),
    'email': ['la_abecilla@yahoo.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}

dag = DAG(
    'spotify_dag',
    default_args=default_args,
    description='Inay recently played songs.',
    schedule_interval=timedelta(days=1)
)

def test_dag():
    print("DAG script is working!")

run_etl = PythonOperator(
    task_id='spotify_etl',
    python_callable=run_spotify_etl,
    dag=dag
)

run_etl

