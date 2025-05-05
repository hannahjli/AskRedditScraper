import sys, json
import re
from reddit_crawler.request import fetch   # Include "fetch" function from requests.py

def store_document(url, submission):
    data = {
        "url": url,
        "title": submission.title,
        "selftext": submission.selftext,
        "subreddit": submission.subreddit.display_name,
        "author": submission.author.name if submission.author else None,
        "created_utc": submission.created_utc,
        "num_comments": submission.num_comments,
    }

    return data
    # Save `data` somewhere: database, JSON file, etc.

def parse(text):
    urls = []
    
    # Find URLs in the selftext
    urls.update(re.findall(r'(https?://\S+)', text.selftext))
    
    # You could also add code here to crawl links inside comments if you want
    
    return urls

def crawl_thread(frontier, max_rpm=60):
    while not frontier:
        try:
            text = fetch(url, max_rpm=max_rpm)
            doc = store_document(url, text)
            write_json(doc, False) # Save the document to a JSON file
        
            for found_urls in parse(text):
                if found_urls not in frontier:
                    # Check if the URL is already in the frontier to avoid duplicates
                    frontier.append(found_urls) # Add found URLs to the frontier

        except Exception as e:
            print(f"Error crawling {url}: {e}")

        finally:
            frontier.pop(0)
            # Remove the crawled URL from the frontier

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("usage: python -m reddit_crawler.crawler <filename> [limit=1000]")
        sys.exit(1)

    try:
        filename = sys.argv[1]
        seeds = [] # Initialize the seeds list

        try:
            limit = sys.argv[2] if len(sys.argv) > 2 else 1000
        except ValueError:
            print("Argument must be an integer")
            sys.exit(1)

        for url in open(filename, "r"):
            response = fetch(url) # Send request
            # just show something prove-it-works:
            print(json.dumps(
                {"id": response.id, "title": response.title}, ensure_ascii=False
            ))
            seeds.append(url) # Add the URL to the seeds list

        crawl_thread(seeds, max_rpm=60) # Start crawling threads

    except KeyboardInterrupt:
        print("\n⏹  stopped by user")
