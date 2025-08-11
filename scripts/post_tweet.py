from pathlib import Path
import os, textwrap

SUMMARY_PATH = Path("data/daily_summary.txt")

# Env vars from repo secrets
API_KEY = os.getenv("X_API_KEY")
API_SECRET = os.getenv("X_API_SECRET")
ACCESS_TOKEN = os.getenv("X_ACCESS_TOKEN")
ACCESS_SECRET = os.getenv("X_ACCESS_SECRET")

def load_summary() -> str:
    if not SUMMARY_PATH.exists():
        return ""
    txt = SUMMARY_PATH.read_text(encoding="utf-8").strip()
    # Make a concise tweet: keep headline + key lines, trim extras
    lines = [l.strip() for l in txt.splitlines() if l.strip()]
    keep = []
    for L in lines:
        keep.append(L)
        if len("\n".join(keep)) > 240:  # leave room for hashtags
            break
    core = "\n".join(keep)
    hashtags = "\n#DeFi #Avalanche #DEX #BlackholeDex"
    tweet = core
    if len(core) + len(hashtags) <= 280:
        tweet = core + hashtags
    return tweet[:280]  # hard cap

def main():
    tweet = load_summary()
    if not tweet:
        print("No summary to post; skipping.")
        return

    # Dry-run if secrets missing
    if not all([API_KEY, API_SECRET, ACCESS_TOKEN, ACCESS_SECRET]):
        print("X API secrets missing; DRY-RUN. Preview:\n---\n" + tweet + "\n---")
        return

    try:
        import tweepy
        auth = tweepy.OAuth1UserHandler(API_KEY, API_SECRET, ACCESS_TOKEN, ACCESS_SECRET)
        api = tweepy.API(auth)
        api.verify_credentials()
        api.update_status(status=tweet)
        print("Tweet posted.")
    except Exception as e:
        print(f"Tweet failed: {e}")

if __name__ == "__main__":
    main()
