import os
import configparser

parser = configparser.ConfigParser()
config_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '../config/config.conf'))
parser.read(config_path)

#REDDIT
SECRET = parser.get('api_keys', 'reddit_secret_key')
CLIENT_ID = parser.get('api_keys', 'reddit_client_id')
REDDIT_USERNAME = parser.get('api_keys', 'reddit_username')
REDDIT_PASSWORD = parser.get('api_keys', 'reddit_password')
REDDIT_USER_AGENT = parser.get('api_keys', 'reddit_user_agent')

#DATABASE
DATABASE_HOST = parser.get('database', 'database_host')
DATABASE_PORT = parser.get('database', 'database_port')
DATABASE_NAME = parser.get('database', 'database_name')
DATABASE_USER = parser.get('database', 'database_user')
DATABASE_PASSWORD = parser.get('database', 'database_password')

#AWS
AWS_ACCESS_KEY_ID = parser.get('aws', 'aws_access_key_id')
AWS_SECRET_ACCESS_KEY = parser.get('aws', 'aws_secret_access_key')
AWS_REGION = parser.get('aws', 'aws_region')
AWS_BUCKET_NAME = parser.get('aws', 'aws_bucket_name')



INPUT_PATH = parser.get('paths', 'input_path')
OUTPUT_PATH = parser.get('paths', 'output_path')


POST_FIELDS = (
    'id',
    'title',
    'selftext',
    'score',
    'num_comments',
    'author',
    'created_utc',
    'url',
    'upvote_ratio',
    'over_18',
    'edited',
    'spoiler',
    'stickied',
)