#CRAWLER.PY

import sys, json, os
import re
from reddit_crawler1.request import fetch_with_retries, init_reddit_sessions, get_diverse_submissions, validate_reddit_url
import praw
import time
from prawcore.exceptions import PrawcoreException, ResponseException, OAuthException

from concurrent.futures import ThreadPoolExecutor, as_completed
import threading

# Add a lock for thread-safe file operations
file_lock = threading.Lock()

count = 0
name = "data"
output_dir = "data_files"
firstfile = True
processed_ids = set()

if not os.path.exists(output_dir):
    os.makedirs(output_dir)


def data_clean(response):
    global processed_ids

    # Use a lock when checking/updating processed_ids
    with threading.Lock():
        # Check if the post ID is already processed
        if response.id in processed_ids:
            return 
        processed_ids.add(response.id)

    try:
        # First, replace MoreComments objects with actual comments
        response.comments.replace_more(limit=None)  # Replace all MoreComments objects
        
        comments = []
        # Now iterate through the flattened comment tree
        for comment in response.comments.list():  # .list() gets all comments, including replies
            if hasattr(comment, "body"):
                author_name = "[deleted]"
                if hasattr(comment, "author") and comment.author is not None:
                    author_name = comment.author.name
                comments.append(f"{author_name}: {comment.body}")

        # Rest of your function remains the same
        if response.author is not None:
            name = response.author.name
        else:
            name = "n/a"

        dictionary = {
            "Post Title": response.title,
            "Subreddit": response.subreddit.display_name,
            "Selftext": response.selftext,
            "Post ID": response.id,
            "Post Date": response.created_utc,
            "Post Score": response.score,
            "URL": "https://www.reddit.com/" + response.permalink,
            "Links": response.url,
            "Username": name,
            "Upvotes": response.ups,
            "Comments": comments,
        }
        
        # Use a lock when writing to file
        with file_lock:
            write_json(dictionary, False)
        
        return True
    except Exception as e:
        print(f"❌ Error processing data for {response.id}: {e}")
        return False

#dictionary is a dictionary object and lastfile is a bool 
#endfile is used to check for the last inputted value to the json file
# to add closing ']' to the json file
def write_json(dictionary, endfile):
    global count, name, firstfile
    file_path = os.path.join(output_dir, f"{name}{count}.json")

    if endfile:
        with open(file_path, "r+", encoding = 'utf-8') as outfile:
            outfile.seek(0, 2)
            size = outfile.tell()
            outfile.seek(size - 1)
            if outfile.read(1) == ',':
                outfile.seek(size - 1)
                outfile.truncate()
            outfile.write(']')
        return
    
    if firstfile:
        with open(file_path, "w", encoding = 'utf-8') as outfile:
            outfile.write('[')
            json.dump(dictionary, outfile, ensure_ascii=False)
            outfile.write(",")
        firstfile = False

    else:
        with open(file_path, "a", encoding = 'utf-8') as outfile:   
            outfile.write("\n")
            json.dump(dictionary, outfile, ensure_ascii=False)
            outfile.flush()

            filepath = os.path.join(output_dir, f"{name}{count}.json")
            filesize = os.path.getsize(filepath)

            # Exactly 10MB (10,485,760 bytes)
            if filesize >= 10485760:
                outfile.write("]")
                outfile.close()
                count += 1
                firstfile = True
            else:
                outfile.write(",")				

def process_url(url, session, max_rpm=180):
    # First validate the URL format before even trying to fetch
    try:
        if not validate_reddit_url(url):
            print(f"⚠️ Skipping invalid URL format: {url}")
            return set(), False
            
        # Fetch the submission with retries
        text = fetch_with_retries(session, url, max_rpm=max_rpm, max_retries=5)
        
        # If fetch returned None (after all retries failed), skip this URL
        if text is None:
            print(f"⚠️ Skipping {url} after failed retries")
            return set(), False
        
        # Process and save the data
        success = data_clean(text)
        
        # Get more URLs to process
        new_urls = parse(text)
        
        return new_urls, success
    except ValueError as e:
        # URL validation errors
        print(f"⚠️ Invalid URL: {url} - {e}")
        return set(), False
    except ResponseException as e:
        # HTTP errors
        status = getattr(e.response, "status_code", None)
        print(f"⚠️ HTTP error {status} processing {url}: {e}")
        return set(), False
    except OAuthException as e:
        # Auth errors (invalid_grant, etc.)
        print(f"⚠️ Authentication error processing {url}: {e}")
        return set(), False
    except PrawcoreException as e:
        # Other PRAW errors
        print(f"⚠️ PRAW error processing {url}: {e}")
        return set(), False
    except Exception as e:
        # Catch-all for other errors
        print(f"⚠️ Unexpected error processing {url}: {e}")
        return set(), False

def parse(text):
    urls = set()  # Initialize a set to store URLs
    
    try:
        # Find URLs in the selftext
        all_urls = re.findall(r'(https?://\S+)', text.selftext)
        
        # Add URLs from comments
        try:
            # Expand comment trees to get more URLs
            text.comments.replace_more(limit=5)
            
            # Check comments for URLs
            for comment in text.comments.list():  # Get all comments including replies
                if hasattr(comment, "body"):
                    comment_urls = re.findall(r'(https?://\S+)', comment.body)
                    all_urls.extend(comment_urls)
        except Exception as e:
            print(f"⚠️ Error parsing comments for URLs: {e}")
        
        # Keep all Reddit URLs that look like valid submissions
        for url in all_urls:
            if 'reddit.com' in url:
                # Clean URL by removing any trailing characters and URL parameters
                clean_url = re.sub(r'[)\]}]$', '', url)
                # Remove tracking parameters
                clean_url = re.sub(r'\?utm_.*', '', clean_url)
                
                # Only add URLs that look like valid submission URLs
                if "/comments/" in clean_url and validate_reddit_url(clean_url):
                    urls.add(clean_url)
    except Exception as e:
        print(f"⚠️ Error parsing URLs: {e}")
    
    return urls

# Worker function for each thread
def worker_thread(thread_id, session, urls, frontier_lock, result_queue, max_rpm=180):
    """Worker function that processes URLs for a single thread with its own session"""
    print(f"🧵 Thread {thread_id} started with account {session.config.username}")
    urls_processed = 0
    urls_failed = 0
    
    while True:
        # Get a URL to process
        with frontier_lock:
            if not urls:
                break  # No more URLs to process
            url = urls.pop(0)
        
        # Process the URL
        try:
            new_urls, success = process_url(url, session, max_rpm)
            
            if success:
                urls_processed += 1
                print(f"✅ Thread {thread_id} successfully processed: {url}")
            else:
                urls_failed += 1
                print(f"❌ Thread {thread_id} failed to process: {url}")
            
            # Add new URLs to frontier
            if new_urls:
                with frontier_lock:
                    for new_url in new_urls:
                        if new_url not in urls:
                            urls.append(new_url)
                print(f"🔍 Thread {thread_id} found {len(new_urls)} new URLs from {url}")
                
        except Exception as e:
            urls_failed += 1
            print(f"⚠️ Thread {thread_id} exception processing {url}: {e}")
    
    # Report back
    result_queue.append((thread_id, urls_processed, urls_failed))
    print(f"🏁 Thread {thread_id} finished, processed {urls_processed} URLs, failed {urls_failed} URLs")

def crawl_thread(frontier, max_rpm=180, timeout=30, max_workers=5):
    global count, firstfile
    target_file_count = 10  # We want exactly 10 files
    initial_count = count   # Remember where we started
    
    # Initialize Reddit sessions - one per thread
    reddit_sessions = init_reddit_sessions()
    
    if not reddit_sessions:
        print("❌ No valid Reddit sessions available. Please check your credentials.")
        return
        
    # Limit max_workers to the number of available sessions
    max_workers = min(max_workers, len(reddit_sessions))
    print(f"🚀 Starting crawl with {len(frontier)} seed URLs using {max_workers} threads")
    
    # Frontier lock for thread-safe frontier access
    frontier_lock = threading.Lock()
    
    # Results queue
    results = []
    
    # Pre-filter frontier to remove obviously invalid URLs
    with frontier_lock:
        valid_frontier = [url for url in frontier if validate_reddit_url(url)]
        frontier.clear()
        frontier.extend(valid_frontier)
        print(f"🧹 Filtered frontier to {len(frontier)} valid submission URLs")
    
    # Create and start worker threads
    threads = []
    for i in range(max_workers):
        # Each thread gets its own session
        session = reddit_sessions[i % len(reddit_sessions)]
        
        # Create thread
        thread = threading.Thread(
            target=worker_thread, 
            args=(i, session, frontier, frontier_lock, results, max_rpm)
        )
        threads.append(thread)
        thread.start()
    
    # Wait for all threads to finish
    for thread in threads:
        thread.join()
    
    # If we're running low on frontier URLs and still need more files, get more
    if count < initial_count + target_file_count:
        print("📊 Need more content. Getting additional submissions...")
        try:
            # Use the first session to get more submissions
            more_submissions = get_diverse_submissions(subreddit_home, limit=500, session=reddit_sessions[0])
            for submission in more_submissions:
                seed_url = "https://www.reddit.com" + submission.permalink
                with frontier_lock:
                    if seed_url not in frontier and validate_reddit_url(seed_url):
                        frontier.append(seed_url)
            print(f"➕ Added {len(more_submissions)} more URLs to frontier")
            
            # Process more URLs if needed
            if frontier and count < initial_count + target_file_count:
                print("🔄 Starting additional processing round...")
                crawl_thread(frontier, max_rpm, timeout, max_workers)
        except Exception as e:
            print(f"❌ Error getting more submissions: {e}")
    
    print(f"🏁 Crawling complete. Generated files from {initial_count} to {count-1}")
    # Close the JSON file properly
    with file_lock:
        write_json({}, True)

    # Print summary of results
    print("\n📊 CRAWLING SUMMARY 📊")
    total_processed = sum(r[1] for r in results)
    total_failed = sum(r[2] for r in results)
    print(f"Total URLs processed successfully: {total_processed}")
    print(f"Total URLs failed: {total_failed}")
    print(f"Success rate: {total_processed/(total_processed+total_failed)*100:.2f}%")
    print(f"Total JSON files created: {count - initial_count}")

# Modify the main block to specify the number of worker threads
if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python -m reddit_crawler.crawler <subreddit> [limit=1000] [threads=5]")
        sys.exit(1)
    try:
        global subreddit_home
        subreddit_home = sys.argv[1]
        seeds = [] # Initialize the seeds list

        try:
            limit = int(sys.argv[2]) if len(sys.argv) > 2 else 1000
            # Get number of threads from command line args
            num_threads = int(sys.argv[3]) if len(sys.argv) > 3 else 5
        except ValueError:
            print("❌ Arguments must be integers")
            sys.exit(1)

        # Set the starting file count based on subreddit
        if (subreddit_home == "AskReddit"):
            count = 0
        elif (subreddit_home == "AITAH"):
            count = 10
        elif (subreddit_home == "askscience"):
            count = 20
        elif (subreddit_home == "explainlikeimfive"):
            count = 30   
        elif (subreddit_home == "scarystories"):
            count = 40
        else:
            # Default to 0 for any other subreddit
            count = 0

        print("🔧 Setup Crawler: Getting Subreddits")
        
        # Initialize sessions - we need them for getting initial submissions
        reddit_sessions = init_reddit_sessions()
        if not reddit_sessions:
            print("❌ Error: No Reddit sessions available. Check your environment variables.")
            sys.exit(1)
        
        # Get diverse submissions using the first session
        submissions = get_diverse_submissions(subreddit_home, limit=limit, session=reddit_sessions[0])
        
        for submission in submissions:
            seed_url = "https://www.reddit.com" + submission.permalink
            if seed_url not in seeds and validate_reddit_url(seed_url):  # Avoid duplicates and invalid URLs
                seeds.append(seed_url)

        print("🚀 Crawling threads")
        print(f"📋 Collected {len(seeds)} seeds, using up to {num_threads} threads")
        
        # Start crawling with threads
        crawl_thread(seeds, max_rpm=180, max_workers=num_threads)
        
    except KeyboardInterrupt:
        print("\n⏹  Stopped by user")