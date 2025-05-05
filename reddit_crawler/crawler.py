import sys, json, os
import re
from reddit_crawler.request import fetch, init_reddit_sessions, get_diverse_submissions
import praw
import time

count = 0
name = "data"
output_dir = "data_files"
firstfile = True
processed_ids = set()

if not os.path.exists(output_dir):
    os.makedirs(output_dir)

def data_clean(response):
    global processed_ids

    # Check if the post ID is already processed
    if response.id in processed_ids:
        return 

    processed_ids.add(response.id)

    # Try to expand all comment trees to get more comments
    try:
        response.comments.replace_more(limit=10)  # Expand comment trees
    except Exception as e:
        print(f"Error expanding comments: {e}")

    comments = []
    # Process more comments - up to 100 per post instead of just top level
    comment_count = 0
    for comment in response.comments.list():  # Use list() to get all comments
        if hasattr(comment, "body") and comment_count < 100:
            if hasattr(comment, "author") and comment.author is not None:
                if comment.author.name != "[deleted]":
                    comments.append(comment.author.name + ": " + comment.body)
                    comment_count += 1
            else:
                comments.append("n/a: " + comment.body)
                comment_count += 1

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
    write_json(dictionary, False)


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
				
def parse(text):
    urls = set()  # Initialize a set to store URLs
    
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
        print(f"Error parsing comments for URLs: {e}")
    
    # Keep all Reddit URLs 
    for url in all_urls:
        if 'reddit.com' in url:
            # Clean URL by removing any trailing characters 
            clean_url = re.sub(r'[)\]}]$', '', url)
            urls.add(clean_url)
    
    return urls
    
    return urls

def crawl_thread(frontier, max_rpm=180, timeout=30):
    global count, firstfile
    target_file_count = 10  # We want exactly 10 files
    initial_count = count   # Remember where we started
    processed_count = 0
    print(f"Starting crawl with {len(frontier)} seed URLs")
    
    # Continue until we have 10 files or run out of URLs
    while frontier and (count < initial_count + target_file_count):
        # Keep track of where we are
        current_file = count
        remaining_files = (initial_count + target_file_count) - count
        
        # Get the URL to process (prioritize shorter URLs as they're more likely to be valid)
        url_index = 0
        shortest_len = float('inf')
        for i, u in enumerate(frontier[:50]):  # Look at first 50 URLs
            if len(u) < shortest_len and "/comments/" in u:
                url_index = i
                shortest_len = len(u)
        
        url = frontier.pop(url_index)  # Get the selected URL
        
        print(f"Processing URL {current_file}/{initial_count + target_file_count - 1} ({remaining_files} files to go): {url}")
        
        try:
            # Add timeout to prevent getting stuck
            text = fetch(url, max_rpm=max_rpm)
            data_clean(text)  # Clean and save the data
            processed_count += 1
            
            # Get more URLs to process
            new_urls = parse(text)
            print(f"Found {len(new_urls)} new URLs")
            for found_url in new_urls:
                if found_url not in frontier:
                    frontier.append(found_url)
            
        except Exception as e:
            print(f"⚠️ Error processing {url}: {e}")
        
        # Add a short sleep to avoid overwhelming the API
        time.sleep(0.5)
        
        # If we're running low on frontier URLs and still need more files, try to get more
        if len(frontier) < 100 and count < initial_count + target_file_count - 1:
            print("Frontier running low. Getting more submissions...")
            try:
                more_submissions = get_diverse_submissions(subreddit_home, limit=500)
                for submission in more_submissions:
                    seed_url = "https://www.reddit.com" + submission.permalink
                    if seed_url not in frontier:
                        frontier.append(seed_url)
                print(f"Added {len(more_submissions)} more URLs to frontier")
            except Exception as e:
                print(f"Error getting more submissions: {e}")
    
    print(f"Crawling complete. Generated files from {initial_count} to {count-1}")
    # Close the JSON file properly
    write_json({}, True)

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("usage: python -m reddit_crawler.crawler <subreddit> [limit=1000]")
        sys.exit(1)
    try:
        # Initialize Reddit sessions
        reddit_sessions = init_reddit_sessions()
        if not reddit_sessions:
            print("Error: No Reddit sessions available. Check your environment variables.")
            sys.exit(1)
        
        # Use the first session for initial operations
        reddit = reddit_sessions[0]

        global subreddit_home
        subreddit_home = sys.argv[1]
        seeds = [] # Initialize the seeds list

        try:
            limit = int(sys.argv[2]) if len(sys.argv) > 2 else 1000
        except ValueError:
            print("Argument must be an integer")
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

        print("Setup Crawler: Getting Subreddits")

        # Get diverse submissions using our enhanced function
        submissions = get_diverse_submissions(subreddit_home, limit=limit)
        
        for submission in submissions:
            seed_url = "https://www.reddit.com" + submission.permalink
            if seed_url not in seeds:  # Avoid duplicates
                seeds.append(seed_url)

        print("Crawling threads")
        print(f"Collected {len(seeds)} seeds")
        crawl_thread(seeds, max_rpm=180) # Start crawling threads
        
    except KeyboardInterrupt:
        print("\n⏹  stopped by user")