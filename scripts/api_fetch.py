# === scripts/api_fetch.py ===
import os
import sys
import json
import math
import time
from pathlib import Path
from datetime import datetime, timezone

import requests
import pandas as pd

# -----------------------------
# Settings
# -----------------------------
CHAIN = "avalanche"
TARGET_DEX = "blackholedex"
ROOT = Path(__file__).resolve().parents[1]  # repo root
DATA_DIR = ROOT / "data"
CSV_PATH = DATA_DIR / "black_data.csv"
SUMMARY_PATH = DATA_DIR / "daily_summary.txt"
API_URL = f"https://api.dexscreener.com/latest/dex/pairs/{CHAIN}"

# -----------------------------
# Helpers
# -----------------------------
def to_float(x):
    try:
        if x is None or x == "":
            return None
        return float(x)
    except Exception:
        return None

def utc_now():
    return datetime.now(timezone.utc)

def utc_midnight(dt=None):
    dt = dt or utc_now()
    return dt.replace(hour=0, minute=0, second=0, microsecond=0)

def safe_get(dct, *keys, default=None):
    cur = dct
    for k in keys:
        if not isinstance(cur, dict) or k not in cur:
            return default
        cur = cur[k]
    return cur

def ensure_dirs():
    DATA_DIR.mkdir(parents=True, exist_ok=True)

# -----------------------------
# Fetch
# -----------------------------
def fetch_pairs():
    print(f"📡 Fetching pairs from {API_URL}")
    r = requests.get(API_URL, timeout=30)
    if r.status_code != 200:
        raise RuntimeError(f"DexScreener HTTP {r.status_code}: {r.text[:200]}")
    payload = r.json()
    pairs = payload.get("pairs", []) or []
    print(f"Fetched {len(pairs)} total Avalanche pairs.")
    return pairs

# -----------------------------
# Filter + Normalize
# -----------------------------
def normalize_blackhole_pairs(pairs):
    target = []
    for p in pairs:
        dex_id = (p.get("dexId") or "").lower()
        if dex_id != TARGET_DEX:
            continue

        base = p.get("baseToken") or {}
        quote = p.get("quoteToken") or {}
        volume = p.get("volume") or {}
        liq = p.get("liquidity") or {}
        txns = p.get("txns") or {}

        row = {
            "token_name": base.get("name"),
            "symbol": base.get("symbol"),
            "quote_symbol": quote.get("symbol"),
            "price_usd": to_float(p.get("priceUsd")),
            "volume_24h_usd": to_float(volume.get("h24")),
            "liquidity_usd": to_float(liq.get("usd")),
            "fdv_usd": to_float(p.get("fdv")),
            "pair_address": p.get("pairAddress"),
            "dex": p.get("dexId"),
            "chain": CHAIN,
            "txns_24h_buys": safe_get(txns, "h24", "buys", default=None),
            "txns_24h_sells": safe_get(txns, "h24", "sells", default=None),
            "price_change_24h_pct": safe_get(p, "priceChange", "h24", default=None),
            "pair_created_at": None,
        }

        # pairCreatedAt is ms epoch; store as ISO UTC
        created_ms = p.get("pairCreatedAt")
        if isinstance(created_ms, (int, float)):
            try:
                row["pair_created_at"] = datetime.fromtimestamp(created_ms / 1000, tz=timezone.utc).isoformat()
            except Exception:
                row["pair_created_at"] = None

        target.append(row)

    print(f"Filtered {len(target)} BlackholeDex pairs.")
    return pd.DataFrame(target)

# -----------------------------
# Summary / Tweet text
# -----------------------------
def write_summary(df: pd.DataFrame):
    today_utc_mid = utc_midnight()
    # Count new listings since UTC midnight
    new_listings = 0
    if not df.empty and "pair_created_at" in df.columns:
        def _is_new(iso):
            if not iso:
                return False
            try:
                dt = datetime.fromisoformat(iso)
                return dt >= today_utc_mid
            except Exception:
                return False
        new_listings = int(df["pair_created_at"].apply(_is_new).sum())

    total_pairs = int(len(df))
    total_vol = 0.0 if df.empty else float(df["volume_24h_usd"].fillna(0).sum())

    top_token_line = "N/A"
    if not df.empty:
        df_sorted = df.sort_values(["volume_24h_usd"], ascending=[False], na_position="last")
        top = df_sorted.iloc[0]
        sym = top.get("symbol") or "N/A"
        liq = top.get("liquidity_usd")
        liq_str = f"${liq:,.0f}" if isinstance(liq, (int, float)) and not math.isnan(liq) else "N/A"
        top_token_line = f"${sym} (liquidity {liq_str})"

    date_str = utc_now().strftime("%B %d")
    summary = (
        f"📊 BlackholeDex Daily Stats ({date_str})\n\n"
        f"🔸 Total Pairs: {total_pairs}\n"
        f"🔸 24h Volume: ${total_vol:,.0f}\n"
        f"🔸 Top Token: {top_token_line}\n"
        f"🔸 New Listings: {new_listings}\n\n"
        f"Track it live → https://github.com/TheKrimsonKoder/blackholedex-dashboard\n\n"
        f"#Crypto #DEX #BlackholeDex"
    )

    SUMMARY_PATH.write_text(summary, encoding="utf-8")
    print(f"📝 Wrote daily summary → {SUMMARY_PATH}")

# -----------------------------
# Main
# -----------------------------
def main():
    ensure_dirs()

    try:
        pairs = fetch_pairs()
    except Exception as e:
        print(f"❌ Fetch failed: {e}", file=sys.stderr)
        # Still write empty CSV/summary so the workflow can proceed
        pd.DataFrame().to_csv(CSV_PATH, index=False)
        SUMMARY_PATH.write_text("⚠️ No BlackholeDex data available today.", encoding="utf-8")
        sys.exit(0)

    df = normalize_blackhole_pairs(pairs)

    # Save CSV
    df.to_csv(CSV_PATH, index=False)
    print(f"✅ Saved {len(df)} rows → {CSV_PATH}")

    # Write tweet summary
    write_summary(df)

if __name__ == "__main__":
    main()
