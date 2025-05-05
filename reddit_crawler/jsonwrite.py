import json
import os

count = 0
name = "data"
firstfile = True

#dictionary is a dictionary object and lastfile is a bool 
#endfile is used to check for the last inputted value to the json file
# to add closing ']' to the json file
def write_json(dictionary, endfile):
	global count, name, firstfile

	if endfile:
		with open(name + str(count) + ".json", "r+") as outfile:
			outfile.seek(0, 2)
			size = outfile.tell()
			outfile.seek(size - 1)
			if outfile.read(1) == ',':
				outfile.seek(size - 1)
				outfile.truncate()
			outfile.write(']')
		return
	
	if firstfile:
		with open(name + str(count) + ".json", "w") as outfile:
			outfile.write('[')
			json.dump(dictionary, outfile)
			outfile.write(",")
		firstfile = False

	else:
		with open(name + str(count) + ".json", "a") as outfile:	

			outfile.write("\n")
			json.dump(dictionary, outfile)
			outfile.flush()

			filepath = os.path.join(os.getcwd(), name + str(count) + ".json")
			filesize = os.path.getsize(filepath)

			#CHANGE 1000 TO 1e7 FOR 10MB
			if filesize >= 10000:
				outfile.write("]")
				outfile.close()
				count += 1
				firstfile = True
			
			else:
				outfile.write(",")

for i in range(500):
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
