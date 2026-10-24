from datetime import datetime
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator


def hello_from_fresh_setup():
    print("Czysty projekt działa! Połączenie PC -> HomeServer idealne.")

with DAG(
    dag_id='01_fresh_start',
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=['clean_setup'],
) as dag:

    test_task = PythonOperator(
        task_id='check_connection',
        python_callable=hello_from_fresh_setup,
    )
