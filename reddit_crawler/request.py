import os, time
from collections import deque
from itertools import cycle

from dotenv import load_dotenv
import praw
from prawcore.exceptions import PrawcoreException   # base for ResponseException, etc.

load_dotenv()  # read .env once

##############################################################################
# 1.  Build all sessions found in env vars
##############################################################################
def _make_reddit(idx: str | None = None) -> praw.Reddit:
    suff = '' if idx is None else idx
    return praw.Reddit(
        client_id=os.getenv(f"REDDIT_CLIENT_ID{suff}"),
        client_secret=os.getenv(f"REDDIT_CLIENT_SECRET{suff}"),
        username=os.getenv(f"REDDIT_USERNAME{suff}"),
        password=os.getenv(f"REDDIT_PASSWORD{suff}"),
        user_agent=f"team-crawler (requester, acct {suff or 0})",
    )

_sessions = [
    _make_reddit(),
    *[_make_reddit(str(i))
      for i in range(1, 3)
      if os.getenv(f"REDDIT_CLIENT_ID{i}")]
]
if not _sessions:
    raise RuntimeError("No Reddit credentials found in environment variables")

_session_cycle = cycle(_sessions)
_current = next(_session_cycle)

##############################################################################
# 2.  Simple rate‑limiter (per account)
##############################################################################
_history = {sess: deque() for sess in _sessions}

def _respect_rate(sess, max_rpm: int = 60):
    now = time.time()
    h = _history[sess]
    h.append(now)
    while h and now - h[0] > 60:
        h.popleft()
    if len(h) > max_rpm:
        time.sleep(60 - (now - h[0]))

##############################################################################
# 3.  Public helper
##############################################################################
def _switch_account() -> praw.Reddit:
    """Move to the next Reddit instance in the cycle and return it."""
    global _current
    _current = next(_session_cycle)
    return _current

def fetch(url: str, max_rpm: int = 60):
    """
    Return a PRAW Submission for `url`.
    If Reddit responds 429 (Too Many Requests) the function
    transparently switches to the next configured account and retries once.
    """
    global _current

    for attempt in range(len(_sessions)):       # at most once per account
        _respect_rate(_current, max_rpm)
        try:
            return _current.submission(url=url)
        except PrawcoreException as e:
            # Too many requests? -> status code 429
            status = getattr(getattr(e, "response", None), "status_code", None)
            if status == 429:
                _current = _switch_account()
                continue        # retry with the new account
            raise               # some other error – let the caller handle
    # All accounts hit the limit; last resort – wait a minute then raise
    time.sleep(60)
    raise RuntimeError("All accounts rate‑limited (429); try again later")

def top_submissions(
    sub_name: str,
    *,
    time_filter: str = "all",
    limit: int | None = None,
    max_rpm: int = 60,
):
    """
    Yield at most `limit` submissions from r/{sub_name}.top().
    Handles 429 by hopping to the next configured Reddit account
    and continues from the last seen post.
    """
    fetched = 0
    after = None                     # fullname of the last item
    global _current

    while limit is None or fetched < limit:
        # throttle BEFORE every network call
        _respect_rate(_current, max_rpm)

        try:
            page_limit = (
                None if limit is None else min(100, limit - fetched)
            )                       # Reddit caps at 100 per call
            gen = _current.subreddit(sub_name).top(
                time_filter=time_filter,
                limit=page_limit,
                params={"after": after} if after else None,
            )

            for sub in gen:
                yield sub
                fetched += 1
                after = sub.fullname
                if limit is not None and fetched >= limit:
                    break

        except PrawcoreException as e:
            if getattr(getattr(e, "response", None), "status_code", None) == 429:
                _current = _switch_account()
                continue
            raise