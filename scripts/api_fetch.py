# === scripts/api_fetch.py — Blackhole + Aerodrome + Uniswap (public-page scrape; no API deps) ===
from __future__ import annotations

from pathlib import Path
from datetime import datetime, timezone
from typing import Any, Dict, Optional, Tuple
import os, re, json
import requests
import pandas as pd
from bs4 import BeautifulSoup

# ---------------- Paths ----------------
DATA_DIR = Path("data"); DATA_DIR.mkdir(parents=True, exist_ok=True)

CSV_BLACK = DATA_DIR / "black_metrics.csv"          # Blackhole timeseries
CSV_AERO  = DATA_DIR / "aerodrome_metrics.csv"      # Aerodrome timeseries
CSV_UNI   = DATA_DIR / "uniswap_metrics.csv"        # Uniswap timeseries (for context)

SUMMARY_PATH = DATA_DIR / "daily_summary.txt"       # tweet source
DEBUG_PATH   = DATA_DIR / "debug_counts.txt"        # debug snapshot

# ---------------- Public page scrape (DeFiLlama) ----------------
LLAMA_PROTOCOL_URL = "https://defillama.com/protocol/{slug}"
_HTTP_HEADERS = {
    "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/120.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
}

def _get_llama_next_data(slug: str) -> Optional[dict]:
    url = LLAMA_PROTOCOL_URL.format(slug=slug)
    try:
        r = requests.get(url, headers=_HTTP_HEADERS, timeout=45)
        r.raise_for_status()
    except Exception as e:
        print(f"[SCRAPE] GET failed for {slug}: {e}")
        return None

    soup = BeautifulSoup(r.text, "html.parser")
    tag = soup.find("script", id="__NEXT_DATA__")
    raw = tag.string if tag and tag.string else None
    if not raw:
        m = re.search(r'<script[^>]*id="__NEXT_DATA__"[^>]*>(.*?)</script>', r.text, re.S)
        raw = m.group(1) if m else None
    if not raw:
        print(f"[SCRAPE] __NEXT_DATA__ not found for {slug}")
        return None

    try:
        return json.loads(raw)
    except Exception as e:
        print(f"[SCRAPE] JSON parse error for {slug}: {e}")
        return None

def _deep_find_numbers(d: Any, keys: tuple[str, ...]) -> Dict[str, Optional[float]]:
    want = {k.lower(): None for k in keys}
    def walk(x):
        if isinstance(x, dict):
            for k, v in x.items():
                lk = k.lower()
                if lk in want and want[lk] is None and isinstance(v, (int, float)):
                    want[lk] = float(v)
                walk(v)
        elif isinstance(x, list):
            for it in x: walk(it)
    walk(d)
    return want

def scrape_llama_protocol(slug: str) -> Dict[str, float]:
    """
    Scrape core metrics from DeFiLlama protocol page JSON.
    Returns dict with: tvl_usd, volume_24h_usd, fees_24h_usd, fees_7d_usd,
                       revenue_24h_usd, revenue_7d_usd, bribes_24h_usd, bribes_7d_usd
    (only keys that are present will be returned).
    """
    data = _get_llama_next_data(slug)
    if not data: return {}
    node = data.get("props") or {}
    for k in ("pageProps", "dehydratedState", "initialState", "fallback"):
        node = node.get(k, node)

    wanted = _deep_find_numbers(node, keys=(
        # TVL / Volume
        "tvl","tvlUsd","totalLiquidityUSD","volume24h","dailyVolumeUsd",
        # Fees / revenue (24h, 7d)
        "fees24h","total24h","revenue24h","fees7d","total7d","revenue7d",
        # Incentives / bribes (optional)
        "bribes24h","bribes24hUsd","incentives24h","incentives24hUsd",
        "bribes7d","bribes7dUsd","incentives7d","incentives7dUsd",
    ))

    result = {
        "tvl_usd": wanted.get("tvl") or wanted.get("tvlusd") or wanted.get("totalliquidityusd"),
        "volume_24h_usd": wanted.get("volume24h") or wanted.get("dailyvolumeusd"),
        "fees_24h_usd": wanted.get("fees24h") or wanted.get("total24h"),
        "fees_7d_usd": wanted.get("fees7d") or wanted.get("total7d"),
        "revenue_24h_usd": wanted.get("revenue24h"),
        "revenue_7d_usd": wanted.get("revenue7d"),
        "bribes_24h_usd": wanted.get("bribes24h") or wanted.get("bribes24husd") or wanted.get("incentives24h") or wanted.get("incentives24husd"),
        "bribes_7d_usd": wanted.get("bribes7d") or wanted.get("bribes7dusd") or wanted.get("incentives7d") or wanted.get("incentives7dusd"),
    }
    return {k: v for k, v in result.items() if v is not None}

# ---------------- CSV helpers ----------------
def upsert_today(csv_path: Path, date_str: str, row: Dict[str, Any], volume_key: str) -> pd.DataFrame:
    cols = [
        "date","volume_24h_usd","tvl_usd",
        "fees_24h_usd","fees_7d_usd",
        "revenue_24h_usd","revenue_7d_usd",
        "bribes_24h_usd","bribes_7d_usd",
        "avg7d_volume_usd"
    ]
    if csv_path.exists(): df = pd.read_csv(csv_path)
    else: df = pd.DataFrame(columns=cols)
    for c in cols:
        if c not in df.columns: df[c] = None

    # Remove today's existing row, then append
    df = df[df["date"] != date_str]
    df = pd.concat([df, pd.DataFrame([row])], ignore_index=True)

    # 7d rolling avg on volume
    df = df.sort_values("date").reset_index(drop=True)
    s = pd.to_numeric(df[volume_key], errors="coerce").rolling(window=7, min_periods=1).mean()
    df["avg7d_volume_usd"] = s.values

    df.to_csv(csv_path, index=False)
    return df

# ---------------- Format helpers ----------------
def today_utc_date() -> str:
    return datetime.now(timezone.utc).date().isoformat()

def utc_hm() -> str:
    return datetime.now(timezone.utc).strftime("%H:%M")

def money(v: Optional[float]) -> Optional[str]:
    return f"${v:,.0f}" if isinstance(v, (int, float)) else None

# ---------------- Main ----------------
def main():
    date_str = today_utc_date()

    # Scrape Blackhole + Aerodrome + Uniswap
    bh = scrape_llama_protocol("blackhole")
    ae = scrape_llama_protocol("aerodrome")
    uni = scrape_llama_protocol("uniswap")

    # Build rows
    row_bh = {
        "date": date_str,
        "volume_24h_usd": bh.get("volume_24h_usd"),
        "tvl_usd": bh.get("tvl_usd"),
        "fees_24h_usd": bh.get("fees_24h_usd"),
        "fees_7d_usd": bh.get("fees_7d_usd"),
        "revenue_24h_usd": bh.get("revenue_24h_usd"),
        "revenue_7d_usd": bh.get("revenue_7d_usd"),
        "bribes_24h_usd": bh.get("bribes_24h_usd"),
        "bribes_7d_usd": bh.get("bribes_7d_usd"),
        "avg7d_volume_usd": None
    }
    row_ae = {
        "date": date_str,
        "volume_24h_usd": ae.get("volume_24h_usd"),
        "tvl_usd": ae.get("tvl_usd"),
        "fees_24h_usd": ae.get("fees_24h_usd"),
        "fees_7d_usd": ae.get("fees_7d_usd"),
        "revenue_24h_usd": ae.get("revenue_24h_usd"),
        "revenue_7d_usd": ae.get("revenue_7d_usd"),
        "bribes_24h_usd": ae.get("bribes_24h_usd"),
        "bribes_7d_usd": ae.get("bribes_7d_usd"),
        "avg7d_volume_usd": None
    }
    row_uni = {
        "date": date_str,
        "volume_24h_usd": uni.get("volume_24h_usd"),
        "tvl_usd": uni.get("tvl_usd"),
        "fees_24h_usd": uni.get("fees_24h_usd"),
        "fees_7d_usd": uni.get("fees_7d_usd"),
        "revenue_24h_usd": uni.get("revenue_24h_usd"),
        "revenue_7d_usd": uni.get("revenue_7d_usd"),
        "bribes_24h_usd": uni.get("bribes_24h_usd"),
        "bribes_7d_usd": uni.get("bribes_7d_usd"),
        "avg7d_volume_usd": None
    }

    # Upsert to CSVs
    df_bh = upsert_today(CSV_BLACK, date_str, row_bh, "volume_24h_usd")
    df_ae = upsert_today(CSV_AERO,  date_str, row_ae, "volume_24h_usd")
    df_uni = upsert_today(CSV_UNI,  date_str, row_uni, "volume_24h_usd")

    # Compose concise 3-line summary for tweeting
    bh_vol, bh_tvl, bh_fee = row_bh["volume_24h_usd"], row_bh["tvl_usd"], row_bh["fees_24h_usd"]
    ae_vol, ae_tvl, ae_fee = row_ae["volume_24h_usd"], row_ae["tvl_usd"], row_ae["fees_24h_usd"]
    uni_vol = row_uni["volume_24h_usd"]

    asof = f"{utc_hm()} UTC"
    lines = [
        f"📊 Daily Snapshot ({date_str})",
        "",
        f"Blackhole — Vol {money(bh_vol) or 'N/A'}, TVL {money(bh_tvl) or 'N/A'}, Fees {money(bh_fee) or 'N/A'}",
        f"Aerodrome — Vol {money(ae_vol) or 'N/A'}, TVL {money(ae_tvl) or 'N/A'}, Fees {money(ae_fee) or 'N/A'}",
        f"Uniswap — Vol {money(uni_vol) or 'N/A'}",
        "",
        f"⏱️ As of {asof}",
        # post_tweet.py appends tags
    ]
    SUMMARY_PATH.write_text("\n".join(lines).strip(), encoding="utf-8")

    # Debug snapshot
    DEBUG_PATH.write_text(json.dumps({
        "date": date_str,
        "blackhole": row_bh,
        "aerodrome": row_ae,
        "uniswap": row_uni,
        "csv_black_rows": len(df_bh),
        "csv_aero_rows": len(df_ae),
        "csv_uni_rows": len(df_uni),
        "cwd": os.getcwd(),
        "data_dir": str(DATA_DIR.resolve()),
    }, indent=2), encoding="utf-8")

    # Log and assert presence
    print(f"✅ Wrote CSV: {CSV_BLACK.resolve()}")
    print(f"✅ Wrote CSV: {CSV_AERO.resolve()}")
    print(f"✅ Wrote CSV: {CSV_UNI.resolve()}")
    print(f"✅ Wrote summary: {SUMMARY_PATH.resolve()}")
    for p in (CSV_BLACK, CSV_AERO, CSV_UNI, SUMMARY_PATH):
        if not p.exists(): raise RuntimeError(f"Missing: {p}")

if __name__ == "__main__":
    main()
