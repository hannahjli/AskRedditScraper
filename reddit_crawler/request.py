import os
import time
import praw
from typing import List, Optional
from prawcore.exceptions import PrawcoreException

# Initialize empty globals
_sessions = []
_current = None
_last_request_time = 0

def init_reddit_sessions():
    """
    Initialize Reddit API sessions from environment variables.
    Looks for variables in the pattern REDDIT_CLIENT_IDx, REDDIT_CLIENT_SECRETx, etc.
    """
    global _sessions, _current
    
    # Clear existing sessions
    _sessions = []
    
    # Check if .env file exists and notify user
    if not os.path.exists('.env'):
        print("Warning: No .env file found. Please create one with your Reddit API credentials.")
        print("Example .env format:")
        print("REDDIT_CLIENT_ID1=your_client_id")
        print("REDDIT_CLIENT_SECRET1=your_client_secret")
        print("REDDIT_USERNAME1=your_username")
        print("REDDIT_PASSWORD1=your_password")
    
    # Try to create a session with hardcoded demo credentials if no environment variables
    # This is just for testing and should be removed in production
    found_env_vars = False
    
    # Find all Reddit accounts in environment variables
    i = 1
    while True:
        client_id = os.getenv(f"REDDIT_CLIENT_ID{i}")
        client_secret = os.getenv(f"REDDIT_CLIENT_SECRET{i}")
        username = os.getenv(f"REDDIT_USERNAME{i}")
        password = os.getenv(f"REDDIT_PASSWORD{i}")
        
        if not any([client_id, client_secret, username, password]):
            # No more credentials found
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
                user_agent=f"python:reddit_crawler:v1.0 (by /u/{username})"
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
        print("\nFallback to read-only mode (limiting functionality, not recommended)")
        try:
            # Create a read-only session as fallback (limited functionality)
            session = praw.Reddit(
                client_id="_iPr-_4v_ecdTowwfIs4Sw",  # Sample ID from example
                client_secret="EyZ0QNOXrN5T4unMn44VLyV5pmZwnA",  # Sample secret from example
                user_agent="python:reddit_crawler:v1.0 (FALLBACK_READ_ONLY)"
            )
            _sessions.append(session)
            print("Initialized fallback read-only Reddit session")
            print("Warning: Limited functionality in read-only mode")
            _current = _sessions[0]
            return _sessions
        except Exception as e:
            print(f"Failed to initialize even fallback session: {e}")
            raise RuntimeError("No valid Reddit sessions could be initialized. Check your environment variables.")
    
    # Set the first session as current
    _current = _sessions[0]
    return _sessions

def switch_account():
    """
    Switch to the next available Reddit account.
    Returns the new current session.
    """
    global _current, _sessions
    
    if not _sessions:
        init_reddit_sessions()
    
    # Find the current session's index
    try:
        current_index = _sessions.index(_current)
    except ValueError:
        current_index = -1
    
    # Switch to the next account (with wraparound)
    next_index = (current_index + 1) % len(_sessions)
    _current = _sessions[next_index]
    
    print(f"Switched to Reddit account {next_index + 1} (user: {_current.config.username})")
    return _current

def respect_rate(max_rpm: int = 180):
    """
    Ensure the request rate doesn't exceed max_rpm (requests per minute).
    Sleeps if necessary to maintain the rate limit.
    More efficient implementation.
    """
    global _last_request_time
    
    # Calculate minimum time between requests (in seconds)
    min_interval = 60.0 / max_rpm
    
    # Check if we need to wait
    current_time = time.time()
    elapsed = current_time - _last_request_time
    
    if elapsed < min_interval and _last_request_time > 0:
        sleep_time = min_interval - elapsed
        # Only sleep if it's more than 10ms to avoid unnecessary tiny delays
        if sleep_time > 0.01:
            time.sleep(sleep_time)
    
    # Update the last request time
    _last_request_time = time.time()

def top_submissions(subreddit_name: str, limit: int = 1000, time_filter: str = "all") -> List[praw.models.Submission]:
    """
    Get top submissions from a subreddit with error handling and rate limiting.
    Optimized for better performance.
    
    Args:
        subreddit_name: Name of the subreddit
        limit: Maximum number of submissions to retrieve
        time_filter: One of: all, day, hour, month, week, year
        
    Returns:
        List of submission objects
    """
    global _current
    
    if not _sessions:
        init_reddit_sessions()
    
    submissions = []
    collected = 0
    retry_count = 0
    max_retries = len(_sessions) * 2  # Allow twice as many retries as we have accounts
    
    print(f"Fetching up to {limit} submissions from r/{subreddit_name} (time filter: {time_filter})")
    
    try:
        # Use a larger batch size to reduce API calls
        # PRAW handles pagination internally, so we can request a large number
        batch_size = min(1000, limit)  # Reddit API cap is 1000 per request
        
        # Only respect rate once per batch request, not per item
        respect_rate(180)  # Increase to 180 RPM (3 per second) - still safe
        
        subreddit = _current.subreddit(subreddit_name)
        
        # Process each submission from the generator without extra delays
        for submission in subreddit.top(time_filter=time_filter, limit=batch_size):
            submissions.append(submission)
            collected += 1
            
            # Print progress every 100 submissions
            if collected % 100 == 0:
                print(f"Retrieved {collected} submissions so far...")
                
            if collected >= limit:
                break
    
    except PrawcoreException as e:
        status = getattr(getattr(e, "response", None), "status_code", None)
        
        if status == 429:  # Too Many Requests
            print(f"Rate limited. Switching accounts...")
            _current = switch_account()
            # Continue with reduced batch size on rate limit
            remaining = limit - collected
            if remaining > 0:
                print(f"Continuing to fetch remaining {remaining} submissions")
                additional = top_submissions(subreddit_name, limit=remaining, time_filter=time_filter)
                submissions.extend(additional)
        else:
            print(f"Error fetching submissions from r/{subreddit_name}: {e}")
    
    except Exception as e:
        print(f"Unexpected error: {e}")
    
    print(f"Retrieved {len(submissions)} submissions from r/{subreddit_name}")
    return submissions

def fetch(url: str, max_rpm: int = 60) -> Optional[praw.models.Submission]:
    """
    Return a PRAW Submission for `url`.
    If Reddit responds 429 (Too Many Requests) the function
    transparently switches to the next configured account and retries once.
    
    Args:
        url: Reddit submission URL
        max_rpm: Maximum requests per minute
        
    Returns:
        PRAW Submission object
    """
    global _current
    
    if not _sessions:
        init_reddit_sessions()
    
    for attempt in range(len(_sessions)):  # at most once per account
        respect_rate(max_rpm)
        try:
            return _current.submission(url=url)
        except PrawcoreException as e:
            # Too many requests? -> status code 429
            status = getattr(getattr(e, "response", None), "status_code", None)
            if status == 429:
                _current = switch_account()
                continue  # retry with the new account
            print(f"Error fetching {url}: {e}")
            raise  # some other error – let the caller handle
    
    # All accounts hit the limit; last resort – wait a minute then raise
    time.sleep(60)
    raise RuntimeError("All accounts rate-limited (429); try again later")

# Initialize sessions when the module is imported
init_reddit_sessions()