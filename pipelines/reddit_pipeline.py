import pandas as pd
import numpy as np

from etls.reddit_etl import connect_reddit, extract_posts
from utils.constants import CLIENT_ID, SECRET, REDDIT_USER_AGENT, REDDIT_USERNAME, REDDIT_PASSWORD, OUTPUT_PATH


def reddit_pipeline(file_name: str, subreddit: str, time_filter='day', limit= None):
    
    #connect to reddit
    instance = connect_reddit(CLIENT_ID, SECRET, REDDIT_USER_AGENT, REDDIT_USERNAME, REDDIT_PASSWORD)
    #extraction
    posts = extract_posts(instance, subreddit, time_filter, limit)
    post_df = pd.DataFrame(posts)
    post_df = transform_data(post_df)
    #transformation
    transform_data(post_df)
    #loading to csv
    load_to_csv(post_df, OUTPUT_PATH)


def transform_data(post_df: pd.DataFrame):
    post_df['created_utc'] = pd.to_datetime(post_df['created_utc'], unit='s')
    post_df['over_18'] = np.where((post_df['over_18'] == True), 'Yes', 'No')
    post_df['author'] = post_df['author'].astype(str)

    return post_df

def load_to_csv(data: pd.DataFrame, path:str):
    data.to_csv(path, index=False)