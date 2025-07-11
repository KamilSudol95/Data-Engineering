from etls.reddit_etl import connect_reddit, extract_posts
from utils.constants import CLIENT_ID, SECRET


def reddit_pipeline(file_name: str, subreddit: str, time_filter: str, limit: int):
    
    #connect to reddit

    instance = connect_reddit(CLIENT_ID, SECRET, 'Airscholar Agent')
    #extraction
    posts = extract_posts(instance, subreddit, time_filter, limit)
    #transformation
    #loading to csv
