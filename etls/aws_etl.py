import sys
import s3fs

from utils.constants import AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_REGION

def connect_to_s3():


    try:
        s3 = s3fs.S3FileSystem(anon=False,
                               key=AWS_ACCESS_KEY_ID,
                               secret=AWS_SECRET_ACCESS_KEY)
        return s3
    
    except Exception as e:
        print(f"Failed to connect to S3: {e}")
        sys.exit(1)


def create_bucket_if_not_exists(s3: s3fs.S3FileSystem, bucket: str):

    try:
        if not s3.exists(bucket):
            s3.mkdir(bucket)
            print(f"Bucket {bucket} created successfully.")
        else:
            print(f"Bucket {bucket} already exists.")
    
    except Exception as e:
        print(f"Failed to create bucket {bucket}: {e}")
        sys.exit(1)


def upload_to_s3(s3: s3fs.S3FileSystem, bucket: str, file_path: str, s3_file_name: str):

    try:
        s3.put(file_path, bucket + '/raw/' + s3_file_name)
        print(f"File {s3_file_name} uploaded to bucket {bucket} successfully.")
    
    except Exception as e:
        print(f"Failed to upload file {s3_file_name} to bucket {bucket}: {e}")
        sys.exit(1)