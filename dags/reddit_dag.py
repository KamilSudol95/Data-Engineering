import os
import sys
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator


sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from pipelines.reddit_pipeline import reddit_pipeline
from pipelines.aws_s3_pipeline import upload_to_s3_pipeline




default_args = {
    'owner': 'Kamil Sudol',
    'start_date': datetime(2025, 8, 4),
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
#Upload to AWS S3
upload_to_s3 = PythonOperator(
    task_id='upload_to_s3_pipeline',
    python_callable=upload_to_s3_pipeline,
    dag=dag,
)
extract >> upload_to_s3