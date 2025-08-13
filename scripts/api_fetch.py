# scripts/post_tweet.py  (v2 with v1.1 fallback)
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
        if len("\n".join(keep + [L])) > 240:
            break
        keep.append(L)

    core = "\n".join(keep) if keep else txt[:240].rstrip()

    candidate = f"{core}\n{HASHTAGS}".strip()
    if len(candidate) <= MAX_LEN:
        return candidate
    # If hashtags don't fit, return core trimmed to 280
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
    resp = client.create_tweet(text=text)  # /2/tweets
    return {"platform": "v2", "response": getattr(resp, "data", resp)}

def post_v1(text: str):
    import tweepy
    auth = tweepy.OAuth1UserHandler(API_KEY, API_SECRET, ACCESS_TOKEN, ACCESS_SECRET)
    api = tweepy.API(auth, wait_on_rate_limit=True)
    status = api.update_status(status=text)  # v1.1 statuses/update
    return {"platform": "v1.1", "response": {"id": status.id_str}}

def is_permission_error(e) -> bool:
    """Return True for likely 453/permission issues on v2."""
    msg = str(e)
    code = getattr(getattr(e, "response", None), "status_code", None)
    if code in (453, 403):  # 403 is common for 'forbidden' / permission scopes
        return True
    # Tweepy often carries API error payloads with codes; keep it simple:
    lowered = msg.lower()
    return any(k in lowered for k in [
        "453", "forbidden", "you currently have access to a subset", "not authorized to access or delete"
    ])

def main():
    tweet = load_summary()
    if not tweet:
        print("No summary to post; skipping.")
        return

    if DRY_RUN or not all([API_KEY, API_SECRET, ACCESS_TOKEN, ACCESS_SECRET]):
        print("DRY‑RUN. Preview:\n---\n" + tweet + "\n---")
        return

    # Try v2 first, then fall back to v1.1 on permission errors
    try:
        result = post_v2(tweet)
        print(f"Tweet posted via {result['platform']}: {result['response']}")
    except Exception as e:
        if is_permission_error(e):
            print(f"v2 failed due to permissions/453. Falling back to v1.1…\nDetail: {e}")
            try:
                result = post_v1(tweet)
                print(f"Tweet posted via {result['platform']}: {result['response']}")
            except Exception as e2:
                print(f"v1.1 fallback failed: {e2}")
                sys.exit(1)
        else:
            print(f"Tweet failed (non-permission error): {e}")
            sys.exit(1)

if __name__ == "__main__":
    main()
