# scripts/post_tweet.py
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
    candidate = f"{core}\n{HASHTAGS}".strip()
    if len(candidate) <= MAX_LEN:
        return candidate
    # If it won't fit with hashtags, trim the core and keep hashtags
    # Keep at least 2 chars for "\n" plus hashtags
    keep = MAX_LEN - (len(HASHTAGS) + 1)
    if keep < 0:
        # Fallback: no hashtags if something goes very wrong
        return core[:MAX_LEN]
    return (core[:keep].rstrip() + "\n" + HASHTAGS).strip()

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

def main():
    tweet = load_summary()
    if not tweet:
        print("No summary to post; skipping.")
        return

    if DRY_RUN:
        print("DRY-RUN. Preview:\n---\n" + tweet + "\n---")
        return

    missing = [k for k,v in {
        "X_API_KEY": API_KEY,
        "X_API_SECRET": API_SECRET,
        "X_ACCESS_TOKEN": ACCESS_TOKEN,
        "X_ACCESS_SECRET": ACCESS_SECRET,
    }.items() if not v]
    if missing:
        print(f"Missing secrets: {', '.join(missing)}")
        sys.exit(1)

    try:
        result = post_v2(tweet)
        print(f"Tweet posted via {result['platform']}: {result['response']}")
    except Exception as e:
        print("Tweet failed.")
        print("Error:", repr(e))
        print("Check: App permission is 'Read and write'; tokens regenerated after enabling write; tier limits.")
        sys.exit(1)

if __name__ == "__main__":
    main()
