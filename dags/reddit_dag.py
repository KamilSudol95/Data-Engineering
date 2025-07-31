import os
import sys
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator


sys.path.insert(0, '/root/airflow/projects/reddit_etl_project')

from pipelines.reddit_pipeline import reddit_pipeline




default_args = {
    'owner': 'Kamil Sudol',
    'start_date': datetime(2025, 7, 27),
}

file_postfix = datetime.now().strftime("%Y%m%d")

dag = DAG(
    dag_id= 'etl_reddit_pipeline',
    default_args=default_args,
    schedule= '@daily',
    catchup= False,
    tags=['reddit', 'etl', 'pipeline'],
)

#Extract from Reddit
extract = PythonOperator(
    task_id='reddit_extract',
    python_callable=reddit_pipeline,
    op_kwargs={
        'file_name': f'reddit_{file_postfix}',
        'subreddit': 'dataengineering',
        'time_filter': 'day',
        'limit': 100,
    },
    dag=dag,
)