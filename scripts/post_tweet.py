# scripts/post_tweet.py  (safe date parse + v2→v1.1 fallback + dedupe)
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
    return core[:MAX_LEN].rstrip()

def safe_last_date_str(csv_path: Path) -> str | None:
    """Return last date in CSV as YYYY-MM-DD, or None if not parsable."""
    try:
        import pandas as pd
    except Exception:
        print("⚠️ pandas not installed; skipping freshness check.")
        return None
    if not csv_path.exists():
        print("⚠️ CSV missing; skipping freshness check.")
        return None
    try:
        df = pd.read_csv(csv_path)
        # Find a plausible date column
        date_col = None
        for c in ["date","day","timestamp","ts"]:
            if c in df.columns:
                date_col = c
                break
        if not date_col or df.empty:
            print("⚠️ No date column or empty CSV; skipping freshness check.")
            return None
        # Strip/parse robustly
        df[date_col] = df[date_col].astype(str).str.strip()
        df[date_col] = pd.to_datetime(df[date_col], errors="coerce")
        df = df.dropna(subset=[date_col])
        if df.empty:
            print("⚠️ All dates NaT after parse; skipping freshness check.")
            return None
        last = df[date_col].max()
        return last.strftime("%Y-%m-%d")
    except Exception as e:
        print(f"⚠️ Could not parse CSV dates: {e}")
        return None

def warn_if_stale(last_date_str: str | None) -> None:
    """Warn (don’t abort) if data looks old, unless ALLOW_STALE=0 and it's very old."""
    if last_date_str is None:
        print("ℹ️ Freshness unknown (no parsable date). Proceeding.")
        return
    try:
        last_dt = datetime.strptime(last_date_str, "%Y-%m-%d").replace(tzinfo=timezone.utc)
        age_days = (datetime.now(timezone.utc) - last_dt).days
        print(f"ℹ️ Latest CSV date: {last_date_str} (≈{age_days} days old)")
        if age_days > 1 and not ALLOW_STALE:
            # Soft safety: for >1 day old we still proceed, but call it out loudly.
            print("⚠️ Data appears >1 day old. Proceeding anyway (no hard stop). Set ALLOW_STALE=1 to silence this.")
    except Exception:
        print("ℹ️ Could not compute age from last_date_str; proceeding.")

def utc_hm():
    return datetime.now(timezone.utc).strftime("%H:%M")

def append_update_tag(text: str) -> str:
    tag = f" (update {utc_hm()} UTC)"
    if len(text) + len(tag) <= MAX_LEN:
        return text + tag
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
    msg = str(e)
    code = getattr(getattr(e, "response", None), "status_code", None)
    if code in (453, 403):
        return True
    lowered = msg.lower()
    return any(k in lowered for k in [
        "453", "forbidden", "you currently have access to a subset", "not authorized"
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

    # Freshness: warn only (never hard-fail for NaT anymore)
    last_date = safe_last_date_str(CSV_PATH)
    warn_if_stale(last_date)

    if DRY_RUN or not all([API_KEY, API_SECRET, ACCESS_TOKEN, ACCESS_SECRET]):
        print("DRY-RUN. Preview:\n---\n" + tweet + "\n---")
        return

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

        print(f"Tweet failed (other error): {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
