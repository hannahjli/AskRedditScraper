import sys, json, os
from reddit_crawler.request import fetch   # Include "fetch" function from requests.py

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

	#ADDS COMMENTS TO THE JSON FILE
	# comments = []
	# for comment in response.comments:
	# 	if hasattr(comment, "body"):
	# 		comments.append(comment.body)

	#pattern = r'https?://\S+|www\.\S+'
	#links = re.findall(pattern, response.selftext)
	
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

        try:
            limit = sys.argv[2] if len(sys.argv) > 2 else 1000
        except ValueError:
            print("Argument must be an integer")
            sys.exit(1)

        for url in open(filename, "r"):
            response = fetch(url) # Send request
            data_clean(response)
 
        write_json({}, True)
    except KeyboardInterrupt:
        print("\n⏹  stopped by user") 
