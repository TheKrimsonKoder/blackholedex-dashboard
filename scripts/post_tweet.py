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
    txt = SUMMARY_PATH.read_text(encoding="utf-8").strip()
    lines = [l.strip() for l in txt.splitlines() if l.strip()]

    # Build up to ~240 chars from natural lines, then append hashtags if they fit.
    keep = []
    for L in lines:
        candidate = ("\n".join(keep + [L])).strip()
        if len(candidate) > 240:
            break
        keep.append(L)

    core = "\n".join(keep) if keep else txt[:240].rstrip()

    candidate = (core + "\n" + HASHTAGS).strip()
    if len(candidate) <= MAX_LEN:
        return candidate
    return core[:MAX_LEN].rstrip()

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
        print(f"Missing secrets: {', '.join(missing)}. Set them in GitHub → Settings → Secrets and variables → Actions.")
        sys.exit(1)

    try:
        result = post_v2(tweet)
        print(f"Tweet posted via {result['platform']}: {result['response']}")
    except Exception as e:
        # Print a concise diagnostic so you know whether it's a permissions issue
        print("Tweet failed.")
        print("Error:", repr(e))
        print("Common causes:\n"
              "- App is not set to 'Read and write' (or higher) in X Developer Portal\n"
              "- Access Token & Secret were not regenerated after enabling write\n"
              "- Plan limits (Free/Basic) exhausted for the month/day")
        sys.exit(1)

if __name__ == "__main__":
    main()
