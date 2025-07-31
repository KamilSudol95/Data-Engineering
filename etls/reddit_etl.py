import praw
import sys

def connect_reddit(client_id, client_secret, user_agent):
    """
    Connects to Reddit using PRAW (Python Reddit API Wrapper).
    
    Args:
        client_id (str): The client ID for Reddit API.
        client_secret (str): The client secret for Reddit API.
        user_agent (str): A unique user agent string for the application.
    """

    try:
        reddit = praw.Reddit(client_id=client_id,
                             client_secret=client_secret,
                             user_agent=user_agent)
        print("Connected to Reddit successfully.")
        return reddit
    
    except Exception as e:
        print(f"Failed to connect to Reddit: {e}")
        sys.exit(1)

def extract_posts(reddit_instance, subreddit: str, time_filter: str, limit=None):

    subreddit = reddit_instance.subreddit(subreddit)
    posts = subreddit.top(time_filter=time_filter, limit=limit)


    post_list = []

    print(posts)
