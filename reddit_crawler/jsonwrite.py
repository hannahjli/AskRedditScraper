import json
import os

count = 0
name = "data"
firstfile = True

#dictionary is a dictionary object and lastfile is a bool 
#lastfile is used to check for the last inputted value to
# add closing ']' to the json file

def write_json(dictionary, lastfile):
	global count, name, firstfile
	#json_object = json.dumps(dictionary, indent=4)

	if firstfile:
		with open(name + str(count) + ".json", "w") as outfile:
			outfile.write('[')
			#outfile.write(json_object)
			json.dump(dictionary, outfile)
			outfile.write(",")
		firstfile = False

	else:
		with open(name + str(count) + ".json", "a") as outfile:
			
			outfile.write("\n")
			#outfile.write(json_object)
			json.dump(dictionary, outfile)
			outfile.flush()

			filepath = os.path.join(os.getcwd(), name + str(count) + ".json")
			filesize = os.path.getsize(filepath)

			#CHANGE 1000 TO 1e7 FOR 10MB
			if filesize >= 1000 or lastfile:
				outfile.write("]")
				outfile.close()
				count += 1
				firstfile = True
			
			else:
				outfile.write(",")

for i in range(50):
	#dictionary needed for json format
	dictionary = {
	"Post Title": "Funny",
	"ID": i,
	"Content": "text",
	"Username": "Miguel",
	"Upvotes": i + 1,
	"Downvotes": i + 2,
	"Comments" : "comment"
	}

	write_json(dictionary, False)

write_json(dictionary, True)