import os
from dotenv import load_dotenv
import time
import praw
from typing import List, Optional
from prawcore.exceptions import PrawcoreException

load_dotenv()
# Initialize empty globals
_sessions = []
_current = None
_last_request_time = 0

def init_reddit_sessions():
    #Initialize Reddit API sessions from environment variables.
    global _sessions, _current
   
    _sessions = []
   
    # Check if .env file exists and notify user
    if not os.path.exists('.env'):
        print("Warning: No .env file found. Please create one with your Reddit API credentials.")
        print("Example .env format:")
        print("REDDIT_CLIENT_ID1=your_client_id")
        print("REDDIT_CLIENT_SECRET1=your_client_secret")
        print("REDDIT_USERNAME1=your_username")
        print("REDDIT_PASSWORD1=your_password")
   
    found_env_vars = False
   
    # try default account
    client_id = os.getenv("REDDIT_CLIENT_ID")
    client_secret = os.getenv("REDDIT_CLIENT_SECRET")
    username = os.getenv("REDDIT_USERNAME")
    password = os.getenv("REDDIT_PASSWORD")
    
    if all([client_id, client_secret, username, password]):
        found_env_vars = True
        try:
            session = praw.Reddit(
                client_id=client_id,
                client_secret=client_secret,
                username=username,
                password=password,
                user_agent=f"cs172:reddit_crawler:v1.0 (by /u/{username})"
            )
            _sessions.append(session)
            print(f"Initialized Reddit session for user {username}")
        except Exception as e:
            print(f"Failed to initialize Reddit session: {e}")

    i = 1
    while True:
        client_id = os.getenv(f"REDDIT_CLIENT_ID{i}")
        client_secret = os.getenv(f"REDDIT_CLIENT_SECRET{i}")
        username = os.getenv(f"REDDIT_USERNAME{i}")
        password = os.getenv(f"REDDIT_PASSWORD{i}")
       
        if not any([client_id, client_secret, username, password]):
            break
           
        found_env_vars = True
       
        if not all([client_id, client_secret, username, password]):
            print(f"Warning: Incomplete credentials for account {i}. Please check your .env file.")
            i += 1
            continue
           
        try:
            session = praw.Reddit(
                client_id=client_id,
                client_secret=client_secret,
                username=username,
                password=password,
                user_agent=f"cs172:reddit_crawler:v1.0 (by /u/{username})"
            )
            _sessions.append(session)
            print(f"Initialized Reddit session {i} for user {username}")
        except Exception as e:
            print(f"Failed to initialize Reddit session {i}: {e}")
       
        i += 1
   
    if not found_env_vars:
        print("Error: No Reddit API credentials found in environment variables.")
        print("Please create a .env file with your Reddit API credentials in the project root.")
        print("Required format:")
        print("REDDIT_CLIENT_ID1=your_client_id")
        print("REDDIT_CLIENT_SECRET1=your_client_secret")
        print("REDDIT_USERNAME1=your_username")
        print("REDDIT_PASSWORD1=your_password")
   
    if not _sessions:
        print(f"Failed to initialize even fallback session: {e}")
            raise RuntimeError("No valid Reddit sessions could be initialized. Check your environment variables.")
   
    # Set the first session as current
    _current = _sessions[0]
    return _sessions

def switch_account():
    # Switch to the next available Reddit account.

    global _current, _sessions
    
    if not _sessions:
        init_reddit_sessions()
    
    # Find the current session's index
    try:
        current_index = _sessions.index(_current)
    except ValueError:
        current_index = -1
    
    # Switch to next acc (wrap around if necessary)
    next_index = (current_index + 1) % len(_sessions)
    _current = _sessions[next_index]
    
    print(f"Switched to Reddit account {next_index + 1} (user: {_current.config.username})")
    return _current

def respect_rate(max_rpm: int = 180):
    # Ensure the request rate doesn't exceed max_rpm (requests per minute).
    global _last_request_time
    
    # Calculate minimum time between requests (in seconds)
    min_interval = 60.0 / max_rpm
    
    # Check if we need to wait
    current_time = time.time()
    elapsed = current_time - _last_request_time
    
    if elapsed < min_interval and _last_request_time > 0:
        sleep_time = min_interval - elapsed
        # Sleep if more that 10ms to avoid little delays
        if sleep_time > 0.01:
            time.sleep(sleep_time)
    
    # Update the last request time
    _last_request_time = time.time()

def fetch(url: str, max_rpm: int = 60) -> Optional[praw.models.Submission]:
    # Return a PRAW Submission for `url`.
    global _current
    
    if not _sessions:
        init_reddit_sessions()
    
    for attempt in range(len(_sessions)):  # try for as many active sessions we have available
        respect_rate(max_rpm)
        try:
            return _current.submission(url=url)
        except PrawcoreException as e:
            status = getattr(getattr(e, "response", None), "status_code", None)
            # Switch account if too many requests
            if status == 429:
                _current = switch_account()
                continue 
            print(f"Error fetching {url}: {e}")
            raise
    
    # All accounts hit the limit; last resort – wait a minute then raise
    time.sleep(60)
    raise RuntimeError("All accounts rate-limited (429); try again later")

def get_diverse_submissions(subreddit_name: str, limit: int = 1000) -> List[praw.models.Submission]:
    # Get a diverse set of submissions using different sort methods and time filters.
    global _current
    
    if not _sessions:
        init_reddit_sessions()
    
    all_submissions = []
    
    # Use different sorting methods to maximize seed count
    sort_methods = ["top", "hot", "new", "controversial"]
    time_filters = ["all", "year", "month", "week", "day"]
    
    subreddit = _current.subreddit(subreddit_name)
    # Split total limit with 20 possible filters (sortmethod * time filter)
    submissions_per_combo = max(10, limit // (len(sort_methods) * len(time_filters)))
    
    print(f"Collecting submissions from r/{subreddit_name}...")
    
    for sort_method in sort_methods:
        if sort_method in ["top", "controversial"]:
            # These methods use time filters
            for time_filter in time_filters:
                try:
                    respect_rate(180)
                    print(f"Getting {submissions_per_combo} {sort_method} posts from time filter: {time_filter}")
                    
                    method = getattr(subreddit, sort_method)
                    for submission in method(time_filter=time_filter, limit=submissions_per_combo):
                        all_submissions.append(submission)
                        
                except Exception as e:
                    print(f"Error getting {sort_method} submissions with {time_filter}: {e}")
        else:
            # don't use time filter on hot (time based)
            try:
                respect_rate(180)
                print(f"Getting {submissions_per_combo} {sort_method} posts")
                
                method = getattr(subreddit, sort_method)
                for submission in method(limit=submissions_per_combo):
                    all_submissions.append(submission)
                    
            except Exception as e:
                print(f"Error getting {sort_method} submissions: {e}")
    
    # Remove duplicates by ID
    seen_ids = set()
    unique_submissions = []
    
    for submission in all_submissions:
        if submission.id not in seen_ids:
            seen_ids.add(submission.id)
            unique_submissions.append(submission)
    
    print(f"Retrieved {len(unique_submissions)} unique submissions from r/{subreddit_name}")
    return unique_submissions

# # Initialize sessions when the module is imported
# init_reddit_sessions()