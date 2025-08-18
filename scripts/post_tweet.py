# scripts/post_tweet.py  (v2 with v1.1 fallback + freshness + dedupe)
from pathlib import Path
import os
import sys
from datetime import datetime, timezone

SUMMARY_PATH = Path("data/daily_summary.txt")
CSV_PATH = Path("data/black_data.csv")

API_KEY = os.getenv("X_API_KEY")
API_SECRET = os.getenv("X_API_SECRET")
ACCESS_TOKEN = os.getenv("X_ACCESS_TOKEN")
ACCESS_SECRET = os.getenv("X_ACCESS_SECRET")
DRY_RUN = os.getenv("DRY_RUN", "").lower() in {"1", "true", "yes"}
ALLOW_STALE = os.getenv("ALLOW_STALE", "").lower() in {"1", "true", "yes"}

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

def check_freshness_or_die():
    """Abort posting if CSV's newest date >1 day old (unless ALLOW_STALE=1)."""
    if ALLOW_STALE:
        print("⚠️  ALLOW_STALE=1 set — skipping freshness check.")
        return
    if not CSV_PATH.exists():
        print("❌ data/black_data.csv not found; refusing to post stale/unknown data.")
        sys.exit(1)

    import pandas as pd
    df = pd.read_csv(CSV_PATH)
    if df.empty:
        print("❌ data/black_data.csv is empty; refusing to post.")
        sys.exit(1)

    # find a date-like column
    date_col = None
    for c in ["date", "day", "timestamp", "ts"]:
        if c in df.columns:
            date_col = c
            break
    if not date_col:
        print("❌ No date column (date/day/timestamp/ts) in CSV; refusing to post.")
        sys.exit(1)

    df[date_col] = pd.to_datetime(df[date_col], errors="coerce")
    df = df.sort_values(date_col)
    last = df.iloc[-1][date_col]
    if pd.isna(last):
        print("❌ Last row date is NaT; refusing to post.")
        sys.exit(1)

    now = datetime.now(timezone.utc)
    if last.tzinfo is None:
        last = last.tz_localize(timezone.utc)
    age = now - last
    print(f"ℹ️ Latest CSV date: {last.isoformat()} (age ≈ {age.days} days)")
    if age.days > 1:
        print("❌ Data is stale (>1 day). Set ALLOW_STALE=1 to override. Aborting.")
        sys.exit(1)

def utc_hm():
    return datetime.now(timezone.utc).strftime("%H:%M")

def append_update_tag(text: str) -> str:
    tag = f" (update {utc_hm()} UTC)"
    if len(text) + len(tag) <= MAX_LEN:
        return text + tag
    # trim to make room
    trim = len(text) + len(tag) - MAX_LEN
    return text[:-trim].rstrip() + tag

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
    lowered = msg.lower()
    return any(k in lowered for k in [
        "453", "forbidden", "you currently have access to a subset", "not authorized to access or delete"
    ])

def is_duplicate_error(e) -> bool:
    msg = str(e).lower()
    code = getattr(getattr(e, "response", None), "status_code", None)
    return code == 403 and ("duplicate" in msg or "already posted" in msg)

def main():
    tweet = load_summary()
    if not tweet:
        print("No summary to post; skipping.")
        return

    # Freshness gate (will exit if stale)
    check_freshness_or_die()

    if DRY_RUN or not all([API_KEY, API_SECRET, ACCESS_TOKEN, ACCESS_SECRET]):
        print("DRY-RUN. Preview:\n---\n" + tweet + "\n---")
        return

    # Try v2 first
    try:
        result = post_v2(tweet)
        print(f"Tweet posted via {result['platform']}: {result['response']}")
        return
    except Exception as e:
        if is_duplicate_error(e):
            print("Detected duplicate tweet 403 — appending update tag and retrying v2…")
            try:
                tweaked = append_update_tag(tweet)
                result = post_v2(tweaked)
                print(f"Tweet posted via v2 (deduped): {result['response']}")
                return
            except Exception as e2:
                print(f"Retry after dedupe failed: {e2}")

        if is_permission_error(e):
            print(f"v2 failed due to permissions/453. Falling back to v1.1…\nDetail: {e}")
            try:
                result = post_v1(tweet)
                print(f"Tweet posted via {result['platform']}: {result['response']}")
                return
            except Exception as e2:
                print(f"v1.1 fallback failed: {e2}")
                sys.exit(1)

        print(f"Tweet failed (non-permission error): {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
