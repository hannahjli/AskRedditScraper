import sys, json, os
import re
from reddit_crawler.request import top_submissions, fetch, init_reddit_sessions
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

    comments = []
    for comment in response.comments:
        if hasattr(comment, "body"):
            if hasattr(comment, "author") and comment.author is not None:
                # Check if the comment author is a valid Reddit user
                if comment.author.name != "[deleted]":
                    comments.append(comment.author.name + ": " + comment.body)
            else:
                # If the author is None, append the comment body only
                comments.append("n/a: " + comment.body)

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
    write_json(dictionary,False)


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

			#CHANGE 1000 TO 1e7 FOR 10MB
			if filesize >= 1e7:
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
    
    # Filter URLs to include only valid subreddit post links
    for url in all_urls:
        if re.match(r'https?://(www\.)?reddit\.com/r/\w+/comments/\w+', url):
            urls.add(url)
    
    return urls

def crawl_thread(frontier, max_rpm=180, timeout=30):
    processed_count = 0
    print(f"Starting crawl with {len(frontier)} seed URLs")
    
    while frontier:
        if count > max_count:
            return # Stop crawling after reaching max file count
        # Rest of the code
        url = frontier[0]  # Get the first URL from the frontier
        print(f"Processing URL {count}/{max_count}: {url}")
        
        try:
            # Add timeout to prevent getting stuck on a single request
            text = fetch(url, max_rpm=max_rpm)
            data_clean(text)  # Clean the data and write to JSON
            processed_count += 1
            
            # Only add URLs to frontier if we haven't reached max_count
            if processed_count < max_count:
                new_urls = parse(text)
                print(f"Found {len(new_urls)} new URLs")
                for found_url in new_urls:
                    if found_url not in frontier:
                        frontier.append(found_url)
            
        except Exception as e:
            print(f"⚠️ Error processing {url}: {e}")
        
        # Always remove the processed URL from the frontier
        frontier.pop(0)
        
        # Add a short sleep to avoid overwhelming the API
        time.sleep(0.5)
    
    print(f"Crawling complete. Processed {processed_count} URLs.")
    # Close the JSON file properly
    write_json({}, True)

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("usage: python -m reddit_crawler.crawler <filename> [limit=1000]")
        sys.exit(1)
    try:
        # Initialize Reddit sessions
        reddit_sessions = init_reddit_sessions()
        if not reddit_sessions:
            print("Error: No Reddit sessions available. Check your environment variables.")
            sys.exit(1)
        
        # Use the first session for initial operations
        reddit = reddit_sessions[0]

        subreddit_home = sys.argv[1]
        seeds = [] # Initialize the seeds list

        try:
            limit = sys.argv[2] if len(sys.argv) > 2 else 1000
        except ValueError:
            print("Argument must be an integer")
            sys.exit(1)

        global max_count
        
        if (subreddit_home == "AskReddit"):
            count = 0
            max_count = 9
        elif (subreddit_home == "AITAH"):
            count = 10
            max_count = 19
        elif (subreddit_home == "askscience"):
            count = 20
            max_count = 29
        elif (subreddit_home == "explainlikeimfive"):
            count = 30   
            max_count = 39
        elif (subreddit_home == "scarystories"):
            count = 40
            max_count = 49

        print("Setup Crawler: Getting Subreddits")

        submissions = top_submissions(subreddit_home, limit=1000)
        for submission in submissions:
            seed_url = "https://www.reddit.com" + submission.permalink
            if seed_url not in seeds:  # Avoid duplicates
                seeds.append(seed_url)

        print("Crawling threads")
        print("Crawled Seeds")
        crawl_thread(seeds, max_rpm=60) # Start crawling threads

    except KeyboardInterrupt:
        print("\n⏹  stopped by user") 
