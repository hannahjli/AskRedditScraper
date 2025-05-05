# REQUEST.PY

import os
from dotenv import load_dotenv
import time
import praw
from typing import List, Optional
from prawcore.exceptions import PrawcoreException, ResponseException, OAuthException

load_dotenv()
# Initialize empty globals
_sessions = []
_account_request_times = {}
_account_status = {}  # Track which accounts are working

def init_reddit_sessions():
    """Initialize Reddit API sessions from environment variables."""
    global _sessions, _account_status
   
    _sessions = []
    _account_status = {}
   
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
            
            # Test the session with a simple API call
            try:
                # Check if credentials are valid with a simple API call
                _ = session.user.me()
                _sessions.append(session)
                _account_status[username] = "active"
                print(f"✅ Initialized Reddit session for user {username}")
            except Exception as e:
                print(f"❌ Failed to validate session for user {username}: {e}")
                _account_status[username] = "invalid"
        except Exception as e:
            print(f"❌ Failed to initialize Reddit session: {e}")
            if username:
                _account_status[username] = "invalid"

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
            if username:
                _account_status[username] = "incomplete"
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
            
            # Test the session with a simple API call
            try:
                # Check if credentials are valid with a simple API call
                _ = session.user.me()
                _sessions.append(session)
                _account_status[username] = "active"
                print(f"✅ Initialized Reddit session {i} for user {username}")
            except Exception as e:
                print(f"❌ Failed to validate session {i} for user {username}: {e}")
                _account_status[username] = "invalid"
        except Exception as e:
            print(f"❌ Failed to initialize Reddit session {i}: {e}")
            if username:
                _account_status[username] = "invalid"
       
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
        print("⚠️ Failed to initialize any Reddit sessions")
        print("Please check your .env file and make sure:")
        print("1. Your Reddit app is registered as a 'script' type app")
        print("2. Your credentials (username/password) are correct")
        print("3. Your account has verified email and sufficient karma")
        raise RuntimeError("No valid Reddit sessions could be initialized. Check your environment variables.")
   
    print(f"Successfully initialized {len(_sessions)} Reddit sessions")
    return _sessions

def respect_rate(session, max_rpm: int = 180):
    """Respect rate limits for a specific session"""
    global _account_request_times
    
    account_name = session.config.username
    last_request_time = _account_request_times.get(account_name, 0)
    
    # Calculate minimum time between requests (in seconds)
    min_interval = 60.0 / (max_rpm * 0.9)  # 90% of allowed capacity
    
    # Check if we need to wait
    current_time = time.time()
    elapsed = current_time - last_request_time
    
    if elapsed < min_interval and last_request_time > 0:
        sleep_time = min_interval - elapsed
        if sleep_time > 0.005:  # Only sleep for meaningful delays
            time.sleep(sleep_time)
    
    # Update the last request time for this account
    _account_request_times[account_name] = time.time()

def validate_reddit_url(url):
    """Check if a URL is a valid Reddit submission URL"""
    # Filter out URLs that are clearly not submission URLs
    if "/wiki/" in url or "/u/" in url or "/user/" in url or "/message/compose" in url:
        return False
    
    # Check for valid patterns - should contain /comments/ for submission URLs
    if "/comments/" not in url and not url.endswith(".rss"):
        return False
    
    return True

def fetch_with_retries(session, url: str, max_rpm: int = 60, max_retries: int = 5) -> Optional[praw.models.Submission]:
    """Fetch a submission with retries for rate limiting"""
    # First, validate the URL before trying to fetch
    if not validate_reddit_url(url):
        raise ValueError(f"Invalid URL (subreddit, not submission): {url}")
    
    for retry in range(max_retries):
        respect_rate(session, max_rpm)
        try:
            return session.submission(url=url)
        except ResponseException as e:
            status = getattr(e.response, "status_code", None)
            
            # If rate limited, wait and retry
            if status == 429:
                wait_time = 5 + retry  # Increase wait time with each retry
                print(f"⚠️ Rate limit (429) hit for {session.config.username}, retry {retry+1}/{max_retries}, waiting {wait_time}s")
                time.sleep(wait_time)
                continue  # Try again
            elif status == 404:
                print(f"❌ Resource not found (404): {url}")
                raise
            else:
                # Other error, raise it
                print(f"❌ Error fetching {url}: {e}")
                raise
        except OAuthException as e:
            # Authentication errors
            if "invalid_grant" in str(e) or "unauthorized_client" in str(e):
                print(f"❌ Authentication error for {session.config.username}: {e}")
                # No point in retrying with same credentials
                raise
            else:
                print(f"❌ OAuth error fetching {url}: {e}")
                raise
        except ValueError as e:
            # URL parsing errors
            print(f"❌ Invalid URL: {url} - {e}")
            raise
        except Exception as e:
            print(f"❌ Unexpected error fetching {url}: {e}")
            if retry < max_retries - 1:
                wait_time = 2 + retry
                print(f"⚠️ Retrying in {wait_time} seconds...")
                time.sleep(wait_time)
                continue
            raise
    
    # If we've exhausted all retries
    print(f"❌ Failed to fetch {url} after {max_retries} retries due to rate limiting")
    return None

def get_diverse_submissions(subreddit_name: str, limit: int = 1000, session=None) -> List[praw.models.Submission]:
    """Get diverse submissions using a specific session if provided"""
    global _sessions
    
    if not _sessions:
        init_reddit_sessions()
    
    # Use provided session or the first one
    if not session:
        session = _sessions[0]
    
    all_submissions = []
    
    # Use different sorting methods to maximize seed count
    sort_methods = ["top", "hot", "new", "controversial"]
    time_filters = ["all", "year", "month", "week", "day"]
    
    subreddit = session.subreddit(subreddit_name)
    # Split total limit with 20 possible filters (sortmethod * time filter)
    submissions_per_combo = max(10, limit // (len(sort_methods) * len(time_filters)))
    
    print(f"📚 Collecting submissions from r/{subreddit_name} using account {session.config.username}...")
    
    for sort_method in sort_methods:
        if sort_method in ["top", "controversial"]:
            # These methods use time filters
            for time_filter in time_filters:
                try:
                    respect_rate(session, 180)
                    print(f"🔍 Getting {submissions_per_combo} {sort_method} posts from time filter: {time_filter}")
                    
                    method = getattr(subreddit, sort_method)
                    for submission in method(time_filter=time_filter, limit=submissions_per_combo):
                        all_submissions.append(submission)
                        
                except Exception as e:
                    print(f"❌ Error getting {sort_method} submissions with {time_filter}: {e}")
        else:
            # don't use time filter on hot (time based)
            try:
                respect_rate(session, 180)
                print(f"🔍 Getting {submissions_per_combo} {sort_method} posts")
                
                method = getattr(subreddit, sort_method)
                for submission in method(limit=submissions_per_combo):
                    all_submissions.append(submission)
                    
            except Exception as e:
                print(f"❌ Error getting {sort_method} submissions: {e}")
    
    # Remove duplicates by ID
    seen_ids = set()
    unique_submissions = []
    
    for submission in all_submissions:
        if submission.id not in seen_ids:
            seen_ids.add(submission.id)
            unique_submissions.append(submission)
    
    print(f"✅ Retrieved {len(unique_submissions)} unique submissions from r/{subreddit_name}")
    return unique_submissions