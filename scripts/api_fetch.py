# === scripts/api_fetch.py (DexScreener only; daily append) ===
from pathlib import Path
from datetime import datetime, timezone
import json
import requests
import pandas as pd

DATA_DIR = Path("data"); DATA_DIR.mkdir(parents=True, exist_ok=True)
CSV_PATH = DATA_DIR / "black_data.csv"
SUMMARY_PATH = DATA_DIR / "daily_summary.txt"
RAW_PATH = DATA_DIR / "dexscreener_raw.json"
DEBUG_PATH = DATA_DIR / "debug_counts.txt"

# DexScreener search endpoint (no key). We search for "blackhole avalanche".
DEX_URL = "https://api.dexscreener.com/latest/dex/search?q=blackhole%20avalanche"

def today_utc():
    return datetime.now(timezone.utc).date().isoformat()

def fetch_json(url, timeout=45):
    r = requests.get(url, timeout=timeout)
    r.raise_for_status()
    return r.json()

def extract_total_24h_volume(payload):
    """
    Sum 24h volume across all Avalanche pairs that belong to Blackhole.
    DexScreener can report volume in:
      - pair['volume']['h24']
      - or pair['volume24h']
    We accept either. We match Blackhole by dexId or URL containing 'blackhole'.
    """
    pairs = (payload or {}).get("pairs") or []
    total = 0.0
    matched = 0

    for p in pairs:
        chain = (p.get("chainId") or "").lower()
        dexid = (p.get("dexId") or "").lower()
        url   = (p.get("url") or "").lower()

        if chain != "avalanche":
            continue
        is_blackhole = ("blackhole" in dexid) or ("blackhole" in url)
        if not is_blackhole:
            continue

        vol = None
        if isinstance(p.get("volume"), dict) and p["volume"].get("h24") is not None:
            vol = p["volume"]["h24"]
        elif p.get("volume24h") is not None:
            vol = p["volume24h"]

        try:
            v = float(vol)
        except Exception:
            continue

        total += v
        matched += 1

    return total, matched, len(pairs)

def upsert_today(csv_path: Path, date_str: str, volume_usd: float):
    """
    Read existing CSV (if any), replace or add the row for `date_str`.
    Columns: date,dex,chain,volume_usd
    """
    if csv_path.exists():
        df = pd.read_csv(csv_path)
        # ensure columns exist
        for col in ["date", "dex", "chain", "volume_usd"]:
            if col not in df.columns:
                df[col] = None
    else:
        df = pd.DataFrame(columns=["date", "dex", "chain", "volume_usd"])

    # drop any existing row for today, then append
    df = df[df["date"] != date_str]
    new_row = pd.DataFrame([{
        "date": date_str,
        "dex": "Blackhole",
        "chain": "Avalanche",
        "volume_usd": float(volume_usd)
    }])
    df = pd.concat([df, new_row], ignore_index=True).sort_values("date")
    df.to_csv(csv_path, index=False)
    return df

def main():
    date_str = today_utc()
    try:
        payload = fetch_json(DEX_URL)
    except Exception as e:
        # Hard fail: keep action green, but record error
        RAW_PATH.write_text(json.dumps({"error": str(e)}, indent=2), encoding="utf-8")
        pd.DataFrame(columns=["date","dex","chain","volume_usd"]).to_csv(CSV_PATH, index=False)
        SUMMARY_PATH.write_text(f"⚠️ DexScreener fetch failed: {e}", encoding="utf-8")
        DEBUG_PATH.write_text("fetch_error=1", encoding="utf-8")
        return

    # Save raw (pretty) for visibility
    RAW_PATH.write_text(json.dumps(payload, indent=2)[:300000], encoding="utf-8")

    total24, matched, total_pairs = extract_total_24h_volume(payload)

    # Update CSV (append/replace today's value)
    df = upsert_today(CSV_PATH, date_str, total24)

    # Write summary
    SUMMARY_PATH.write_text(
        f"📊 BlackholeDex Daily Stats ({date_str})\n\n"
        f"🔸 24h DEX Volume: ${total24:,.0f}\n"
        f"🔹 Pairs matched (Avalanche/Blackhole): {matched} of {total_pairs} returned\n\n"
        f"Source: DexScreener (search)\n"
        f"#Crypto #DEX #BlackholeDex",
        encoding="utf-8"
    )

    # Debug counts
    DEBUG_PATH.write_text(
        f"date={date_str}, matched_pairs={matched}, pairs_returned={total_pairs}, "
        f"total24={total24:.2f}, rows_after_upsert={len(df)}",
        encoding="utf-8"
    )

if __name__ == "__main__":
    main()
