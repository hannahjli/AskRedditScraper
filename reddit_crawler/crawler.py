import sys, json, os
import re
from reddit_crawler.request import fetch   # Include "fetch" function from requests.py

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
            data_clean(text)
        
            for found_urls in parse(text):
                if found_urls not in frontier:
                    # Check if the URL is already in the frontier to avoid duplicates
                    frontier.append(found_urls) # Add found URLs to the frontier

        except Exception as e:
            print(f"Error crawling {url}: {e}")

        finally:
            frontier.pop(0)
            # Remove the crawled URL from the frontier

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
	
	dictionary = {
		
		"Post Title": response.title,
		"Selftext": response.selftext,
		"URL": "https://www.reddit.com" + response.permalink,
		"Links": response.url,
		"Username": response.author.name,
		"Upvotes": response.ups,
		"Comments": response.num_comments
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
			if filesize >= 10000:
				outfile.write("]")
				outfile.close()
				count += 1
				firstfile = True
			
			else:
				outfile.write(",")

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
        write_json({}, True)
    except KeyboardInterrupt:
        print("\n⏹  stopped by user") 
