import sys, json
from reddit_crawler.request import fetch   # Include "fetch" function from

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("usage: python -m reddit_crawler.crawler <url1> <url2> ...")
        sys.exit(1)

    try:
        for url in sys.argv[1:]:
            response = fetch(url) # Send request
            # just show something prove-it-works:
            print(json.dumps(
                {"id": response.id, "title": response.title}, ensure_ascii=False
            ))
    except KeyboardInterrupt:
        print("\n⏹  stopped by user")
