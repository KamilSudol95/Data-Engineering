
from utils.constants import AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_REGION, AWS_BUCKET_NAME
from etls.aws_etl import connect_to_s3, create_bucket_if_not_exists, upload_to_s3

def upload_to_s3_pipeline (ti):
    file_path = ti.xcom_pull(task_ids='reddit_extract', key='return_value')

    s3 = connect_to_s3()
    create_bucket_if_not_exists(s3, AWS_BUCKET_NAME)
    upload_to_s3(s3, AWS_BUCKET_NAME, file_path, file_path.split('/')[-1])

