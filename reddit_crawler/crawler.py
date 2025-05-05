# #CRAWLER.PY

# import sys, json, os
# import re
# from reddit_crawler.request import fetch, init_reddit_sessions, get_diverse_submissions
# import praw
# import time

# from concurrent.futures import ThreadPoolExecutor, as_completed
# import threading

# # Add a lock for thread-safe file operations
# file_lock = threading.Lock()

# count = 0
# name = "data"
# output_dir = "data_files"
# firstfile = True
# processed_ids = set()

# if not os.path.exists(output_dir):
#     os.makedirs(output_dir)


# def data_clean(response):
#     global processed_ids

#     # Use a lock when checking/updating processed_ids
#     with threading.Lock():
#         # Check if the post ID is already processed
#         if response.id in processed_ids:
#             return 
#         processed_ids.add(response.id)

#     # First, replace MoreComments objects with actual comments
#     response.comments.replace_more(limit=None)  # Limit to 10 for speed
    
#     comments = []
#     # Now iterate through the flattened comment tree
#     for comment in response.comments.list():  # .list() gets all comments, including replies
#         if hasattr(comment, "body"):
#             author_name = "[deleted]"
#             if hasattr(comment, "author") and comment.author is not None:
#                 author_name = comment.author.name
#             comments.append(f"{author_name}: {comment.body}")

#     # Rest of your function remains the same
#     if response.author is not None:
#         name = response.author.name
#     else:
#         name = "n/a"

#     dictionary = {
#         "Post Title": response.title,
#         "Subreddit": response.subreddit.display_name,
#         "Selftext": response.selftext,
#         "Post ID": response.id,
#         "Post Date": response.created_utc,
#         "Post Score": response.score,
#         "URL": "https://www.reddit.com/" + response.permalink,
#         "Links": response.url,
#         "Username": name,
#         "Upvotes": response.ups,
#         "Comments": comments,
#     }
    
#     # Use a lock when writing to file
#     with file_lock:
#         write_json(dictionary, False)
    
#     return True

# #dictionary is a dictionary object and lastfile is a bool 
# #endfile is used to check for the last inputted value to the json file
# # to add closing ']' to the json file
# def write_json(dictionary, endfile):
#     global count, name, firstfile
#     file_path = os.path.join(output_dir, f"{name}{count}.json")

#     if endfile:
#         with open(file_path, "r+", encoding = 'utf-8') as outfile:
#             outfile.seek(0, 2)
#             size = outfile.tell()
#             outfile.seek(size - 1)
#             if outfile.read(1) == ',':
#                 outfile.seek(size - 1)
#                 outfile.truncate()
#             outfile.write(']')
#         return
    
#     if firstfile:
#         with open(file_path, "w", encoding = 'utf-8') as outfile:
#             outfile.write('[')
#             json.dump(dictionary, outfile, ensure_ascii=False)
#             outfile.write(",")
#         firstfile = False

#     else:
#         with open(file_path, "a", encoding = 'utf-8') as outfile:   
#             outfile.write("\n")
#             json.dump(dictionary, outfile, ensure_ascii=False)
#             outfile.flush()

#             filepath = os.path.join(output_dir, f"{name}{count}.json")
#             filesize = os.path.getsize(filepath)

#             # Exactly 10MB (10,485,760 bytes)
#             if filesize >= 10485760:
#                 outfile.write("]")
#                 outfile.close()
#                 count += 1
#                 firstfile = True
#             else:
#                 outfile.write(",")				

# def process_url(url, max_rpm=180):
#     try:
#         # Fetch the submission
#         text = fetch(url, max_rpm=max_rpm)
        
#         # Process and save the data
#         success = data_clean(text)
        
#         # Get more URLs to process
#         new_urls = parse(text)
#         # new_urls = []
        
#         return new_urls, success
#     except Exception as e:
#         print(f"⚠️ Error processing {url}: {e}")
#         return set(), False

# def parse(text):
#     urls = set()  # Initialize a set to store URLs
    
#     # Find URLs in the selftext
#     all_urls = re.findall(r'(https?://\S+)', text.selftext)
    
#     # Add URLs from comments
#     try:
#         # Expand comment trees to get more URLs
#         text.comments.replace_more(limit=5)
        
#         # Check comments for URLs
#         for comment in text.comments.list():  # Get all comments including replies
#             if hasattr(comment, "body"):
#                 comment_urls = re.findall(r'(https?://\S+)', comment.body)
#                 all_urls.extend(comment_urls)
#     except Exception as e:
#         print(f"Error parsing comments for URLs: {e}")
    
#     # Keep all Reddit URLs 
#     for url in all_urls:
#         if 'reddit.com' in url:
#             # Clean URL by removing any trailing characters 
#             clean_url = re.sub(r'[)\]}]$', '', url)
#             urls.add(clean_url)
    
#     return urls


# def crawl_thread(frontier, max_rpm=180, timeout=30, max_workers=5):
#     global count, firstfile
#     target_file_count = 10  # We want exactly 10 files
#     initial_count = count   # Remember where we started
#     print(f"Starting crawl with {len(frontier)} seed URLs using {max_workers} threads")
    
#     # Continue until we have 10 files or run out of URLs
#     while frontier and (count < initial_count + target_file_count):
#         # Create a batch of URLs to process
#         batch_urls = []
#         for _ in range(min(max_workers, len(frontier))):
#             # Find the shortest URL with "/comments/" in it
#             url_index = 0
#             shortest_len = float('inf')
#             for i, u in enumerate(frontier[:50]):
#                 if len(u) < shortest_len and "/comments/" in u:
#                     url_index = i
#                     shortest_len = len(u)
            
#             if shortest_len == float('inf'):
#                 break  # No valid URLs found
                
#             # Remove and add to batch
#             batch_urls.append(frontier.pop(url_index))
        
#         if not batch_urls:
#             print("No valid URLs in frontier")
#             break
            
#         print(f"Processing batch of {len(batch_urls)} URLs, {count-initial_count} of {target_file_count} files processed")
        
#         # Process URLs in parallel
#         with ThreadPoolExecutor(max_workers=max_workers) as executor:
#             # Submit all URLs for processing
#             future_to_url = {executor.submit(process_url, url, max_rpm): url for url in batch_urls}
            
#             # Collect results as they complete
#             new_urls_set = set()
#             for future in as_completed(future_to_url):
#                 url = future_to_url[future]
#                 try:
#                     new_urls, success = future.result()
#                     if success:
#                         print(f"Successfully processed: {url}")
#                     else:
#                         print(f"Failed to process: {url}")
                    
#                     if new_urls:
#                         print(f"Found {len(new_urls)} new URLs from {url}")
#                         new_urls_set.update(new_urls)
#                 except Exception as e:
#                     print(f"Exception processing {url}: {e}")
        
#         # Add new URLs to frontier
#         for url in new_urls_set:
#             if url not in frontier:
#                 frontier.append(url)
        
#         # If we're running low on frontier URLs and still need more files, try to get more
#         if len(frontier) < 100 and count < initial_count + target_file_count - 1:
#             print("Frontier running low. Getting more submissions...")
#             try:
#                 more_submissions = get_diverse_submissions(subreddit_home, limit=500)
#                 for submission in more_submissions:
#                     seed_url = "https://www.reddit.com" + submission.permalink
#                     if seed_url not in frontier:
#                         frontier.append(seed_url)
#                 print(f"Added {len(more_submissions)} more URLs to frontier")
#             except Exception as e:
#                 print(f"Error getting more submissions: {e}")
    
#     print(f"Crawling complete. Generated files from {initial_count} to {count-1}")
#     # Close the JSON file properly
#     with file_lock:
#         write_json({}, True)

# # Modify the main block to specify the number of worker threads
# if __name__ == "__main__":
#     if len(sys.argv) < 2:
#         print("usage: python -m reddit_crawler.crawler <subreddit> [limit=1000] [threads=5]")
#         sys.exit(1)
#     try:
#         # Initialize Reddit sessions
#         reddit_sessions = init_reddit_sessions()
#         if not reddit_sessions:
#             print("Error: No Reddit sessions available. Check your environment variables.")
#             sys.exit(1)
        
#         # Use the first session for initial operations
#         reddit = reddit_sessions[0]

#         global subreddit_home
#         subreddit_home = sys.argv[1]
#         seeds = [] # Initialize the seeds list

#         try:
#             limit = int(sys.argv[2]) if len(sys.argv) > 2 else 1000
#             # Get number of threads from command line args (default to number of accounts or 5)
#             num_threads = int(sys.argv[3]) if len(sys.argv) > 3 else min(len(reddit_sessions), 5)
#         except ValueError:
#             print("Arguments must be integers")
#             sys.exit(1)

#         # Set the starting file count based on subreddit
#         if (subreddit_home == "AskReddit"):
#             count = 0
#         elif (subreddit_home == "AITAH"):
#             count = 10
#         elif (subreddit_home == "askscience"):
#             count = 20
#         elif (subreddit_home == "explainlikeimfive"):
#             count = 30   
#         elif (subreddit_home == "scarystories"):
#             count = 40
#         else:
#             # Default to 0 for any other subreddit
#             count = 0

#         print("Setup Crawler: Getting Subreddits")

#         # Get diverse submissions using our enhanced function
#         submissions = get_diverse_submissions(subreddit_home, limit=limit)
        
#         for submission in submissions:
#             seed_url = "https://www.reddit.com" + submission.permalink
#             if seed_url not in seeds:  # Avoid duplicates
#                 seeds.append(seed_url)

#         print("Crawling threads")
#         print(f"Collected {len(seeds)} seeds, using {num_threads} threads")
#         crawl_thread(seeds, max_rpm=180, max_workers=num_threads) # Start crawling with threads
        
#     except KeyboardInterrupt:
#         print("\n⏹  stopped by user")