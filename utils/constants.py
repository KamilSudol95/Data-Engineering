import os
import configparser

parser = configparser.ConfigParser()
config_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '../config/config.conf'))
parser.read(config_path)


SECRET = parser.get('api_keys', 'reddit_secret_key')
CLIENT_ID = parser.get('api_keys', 'reddit_client_id')
REDDIT_USERNAME = parser.get('api_keys', 'reddit_username')
REDDIT_PASSWORD = parser.get('api_keys', 'reddit_password')
REDDIT_USER_AGENT = parser.get('api_keys', 'reddit_user_agent')

DATABASE_HOST = parser.get('database', 'database_host')
DATABASE_PORT = parser.get('database', 'database_port')
DATABASE_NAME = parser.get('database', 'database_name')
DATABASE_USER = parser.get('database', 'database_user')
DATABASE_PASSWORD = parser.get('database', 'database_password')

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