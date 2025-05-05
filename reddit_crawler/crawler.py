import sys, json, os
import re
from reddit_crawler.request import fetch   # Include "fetch" function from requests.py
import praw

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
        "URL": "https://www.reddit.com" + response.permalink,
        "Links": response.url,
        "Username": name,
        "Upvotes": response.ups,
        "Comments": response.num_comments,
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

def crawl_thread(frontier, max_rpm=60):
    while frontier:
        if count > 9:
            return # Stop crawling after 10 files
        url = frontier[0] # Get the first URL from the frontier
        text = fetch(url, max_rpm=60) # Get the selftext of the URL
        data_clean(text) # Clean the data and write to JSON
    
        for found_urls in parse(text):
            if found_urls not in frontier:
                # Check if the URL is already in the frontier to avoid duplicates
                frontier.append(found_urls) # Add found URLs to the frontier

        frontier.pop(0)
        # Remove the crawled URL from the frontier

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("usage: python -m reddit_crawler.crawler <filename> [limit=1000]")
        sys.exit(1)

    try:
        subreddit_home = sys.argv[1]
        seeds = [] # Initialize the seeds list

        try:
            limit = sys.argv[2] if len(sys.argv) > 2 else 1000
        except ValueError:
            print("Argument must be an integer")
            sys.exit(1)

        reddit = praw.Reddit(
            client_id=os.getenv("REDDIT_CLIENT_ID"),
            client_secret=os.getenv("REDDIT_CLIENT_SECRET"),
            user_agent="team-crawler (requester)",
            username=os.getenv("REDDIT_USERNAME"),
            password=os.getenv("REDDIT_PASSWORD"),
        )
        
        for submission in reddit.subreddit(subreddit_home).top(time_filter="all",limit=15000):
            seeds.append("https://www.reddit.com" + submission.permalink) # Add the submission URL to the seeds list
            
        crawl_thread(seeds, max_rpm=60) # Start crawling threads

    except KeyboardInterrupt:
        print("\n⏹  stopped by user") 
