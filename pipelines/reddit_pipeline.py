import pandas as pd
import numpy as np

from etls.reddit_etl import connect_reddit, extract_posts, transform_data, load_to_csv
from utils.constants import CLIENT_ID, SECRET, REDDIT_USER_AGENT, OUTPUT_PATH



def reddit_pipeline(file_name: str, subreddit: str, time_filter='day', limit= None):
    
    #connect to reddit
    instance = connect_reddit(CLIENT_ID, SECRET, REDDIT_USER_AGENT)
    #extraction
    posts = extract_posts(instance, subreddit, time_filter, limit)
    post_df = pd.DataFrame(posts)
    post_df = transform_data(post_df)
    #transformation
    transform_data(post_df)
    #loading to csv
    file_path = f"{OUTPUT_PATH}/{file_name}.csv"
    load_to_csv(post_df, file_path)

    return file_path

