import os, time
from collections import deque
from dotenv import load_dotenv
import praw

load_dotenv()

_reddit = praw.Reddit(   # keep a single Reddit session alive
    client_id=os.getenv("REDDIT_CLIENT_ID"),
    client_secret=os.getenv("REDDIT_CLIENT_SECRET"),
    user_agent="team-crawler (requester)",
    username=os.getenv("REDDIT_USERNAME"),
    password=os.getenv("REDDIT_PASSWORD"),
)

################### 60 Requests Per MIN timer ###################
_history = deque()       # timestamps of the last 60 seconds

def _respect_rate(max_rpm: int = 60) -> None:
    """Sleep just enough to stay ≤ max_rpm requests per minute."""
    now = time.time()
    _history.append(now)
    while _history and now - _history[0] > 60:
        _history.popleft()
    if len(_history) > max_rpm:
        wait = 60 - (now - _history[0])
        time.sleep(wait)
#################################################################

#  public
def fetch(url: str, max_rpm: int = 60):
    """
    Return the raw PRAW Submission for `url`.
    Call this repeatedly; it rate-limits itself.
    """
    _respect_rate(max_rpm)
    return _reddit.submission(url=url)
