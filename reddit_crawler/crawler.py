import sys, json
from reddit_crawler.request import fetch   # Include "fetch" function from requests.py

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
            # just show something prove-it-works:
            print(json.dumps(
                {"id": response.id, "title": response.title}, ensure_ascii=False
            ))
    except KeyboardInterrupt:
        print("\n⏹  stopped by user")
