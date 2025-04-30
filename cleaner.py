import os  #used for file and directory manipulation
import hashlib #creattes hashes for detecting duplicates
import requests  # allows us to make HTTP requests
import json #used for parsing JSON data
import praw 



reddit = praw.Reddit(client_id = 'YOUR_CLIENT_ID',client_secret = 'YOUR_CLIENT_SECRET',user_agent = 'YOUR_USER_AGENT') #initialize the Reddit API

input_dir = 'raw_data' #folder with raw json files
output_dir = 'cleaned_data' #folder to save cleaned data
max_file_size_mb = 10
target_total_mb = 500

os.makedirs(output_dir, exist_ok=True) #create output directory if it doesn't exist

def read_json_lines(filepath):
    with open(filepath, 'r', encoding = 'utf-8') as file:
        for line in file:
            try:
                yield json.loads(line)
            except json.JSONDecodeError:
                continue #for malformed lines

def get_reddit_post_title(url):# fethces title from a URL
    try:
        submission = reddit.submission (url = url)
        return submission.title
    except Exception:
        return None
    
def clean_post(post, seen_hashes):  #cleans a single post
    author = post.get('author')
    if not author or author.lower() in ['deleted', 'automoderator']:
        return None 
    
    title = post.get('title', '').strip() #gets the title of the post
    body = post.get('selftext', '').strip()
    if not title and not body:
        return None 
    
    hash_key = hashlib.md5((title + body).encode()).hexdigest() #creates a hash of the title and body
    if hash_key in seen_hashes:
        return None
    seen_hashes.add(hash_key) #adds the hash to the set of seen hashes  

    post['title'] = title.lower()
    post['selftext'] = body.lower()

    if 'url' in post and 'reddit.com' in post['url']: #only get reddit titles
        reddit_title = get_reddit_post_title(post['url'])
        if reddit_title:
            post['url_title'] = reddit_title

    return post

def clean_all_files():
    seen_hashes = set() #set to keep track of seen hashes
    current_file_index = 0 #index for the current file
    current_file_size = 0 #size of the current file in bytes
    outfile = open(os.path.join(output_dir, f'cleaned_data_{current_file_index}.jsonl'), 'w', encoding='utf-8') #output file

    total_written = 0 #total number of bytes written to the output file

    input_files = [f for f in os.listdir(input_dir) if os.path.isfile(os.path.join(input_dir, f))]
    for filename in input_files:
        filepath = os.path.join(input_dir, filename) #path to the input file
        for post in read_json_lines(filepath):
            cleaned = clean_post(post, seen_hashes)
            if cleaned:
                line = json.dumps(cleaned) + '\n' #convert the cleaned post to a JSON string
                line_size = len(line.encode('utf-8')) #size of the line in bytes

                if current_file_size + line_size > max_file_size_mb * 1024 * 1024: #if the current file size exceeds the limit
                    outfile.close()
                    current_file_index += 1
                    current_file_size = 0
                    outfile = open(os.path.join(output_dir, f'cleaned_data_{current_file_index}.jsonl'), 'w', encoding='utf-8') #create a new output file

                outfile.write(line) #write the cleaned post to the output file
                current_file_size += line_size
                total_written += line_size

                if total_written >= target_total_mb * 1024 * 1024: #if the total size exceeds the target size
                    print(f"Total size of cleaned data: {total_written / (1024 * 1024):.2f} MB")
                    outfile.close()
                    return
                
    outfile.close() #close the output file
    print(f"Total size of cleaned data: {total_written / (1024 * 1024):.2f} MB") #print the total size of the cleaned data

if __name__ == '__main__':
    clean_all_files() #call the function to clean all files
    print("Cleaning completed.")
    
    
