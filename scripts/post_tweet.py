from pathlib import Path
import os
import sys

SUMMARY_PATH = Path("data/daily_summary.txt")

API_KEY = os.getenv("X_API_KEY")
API_SECRET = os.getenv("X_API_SECRET")
ACCESS_TOKEN = os.getenv("X_ACCESS_TOKEN")
ACCESS_SECRET = os.getenv("X_ACCESS_SECRET")
DRY_RUN = os.getenv("DRY_RUN", "").lower() in {"1", "true", "yes"}

HASHTAGS = "#DeFi #Avalanche #DEX #BlackholeDex"
MAX_LEN = 280

def load_summary() -> str:
    if not SUMMARY_PATH.exists():
        return ""
    core = SUMMARY_PATH.read_text(encoding="utf-8").strip()
    if not core:
        return ""
    # append hashtags and hard-cap at 280
    candidate = f"{core}\n{HASHTAGS}".strip()
    return candidate[:MAX_LEN]

def post_v2(text: str):
    import tweepy
    client = tweepy.Client(
        consumer_key=API_KEY,
        consumer_secret=API_SECRET,
        access_token=ACCESS_TOKEN,
        access_token_secret=ACCESS_SECRET,
        wait_on_rate_limit=True,
    )
    resp = client.create_tweet(text=text)  # v2 /2/tweets
    return {"platform": "v2", "response": getattr(resp, "data", resp)}

def post_v11(text: str):
    import tweepy
    auth = tweepy.OAuth1UserHandler(API_KEY, API_SECRET, ACCESS_TOKEN, ACCESS_SECRET)
    api = tweepy.API(auth, wait_on_rate_limit=True)
    status = api.update_status(status=text)  # v1.1
    return {"platform": "v1.1", "response": {"id": status.id_str}}

def main():
    tweet = load_summary()
    if not tweet:
        print("No summary to post; skipping.")
        return

    if DRY_RUN:
        print("DRY-RUN. Preview:\n---\n" + tweet + "\n---")
        return

    if not all([API_KEY, API_SECRET, ACCESS_TOKEN, ACCESS_SECRET]):
        print("Missing secrets.")
        sys.exit(1)

    try:
        result = post_v2(tweet)
        print(f"Tweet posted via {result['platform']}: {result['response']}")
    except Exception as e:
        print("v2 failed, falling back to v1.1…", e)
        try:
            result = post_v11(tweet)
            print(f"Tweet posted via {result['platform']}: {result['response']}")
        except Exception as e2:
            print("v1.1 fallback failed:", e2)
            sys.exit(1)

if __name__ == "__main__":
    main()
