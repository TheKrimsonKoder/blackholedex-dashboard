# === scripts/api_fetch.py — Blackhole-only: Volume, TVL, Fees, Bribes (API + public-page scrape fallback) ===
from __future__ import annotations

from pathlib import Path
from datetime import datetime, timezone
from typing import Any, Dict, Optional, Tuple, List
import os
import re
import json
import time
import requests
import pandas as pd

# ---------------- Paths ----------------
DATA_DIR = Path("data"); DATA_DIR.mkdir(parents=True, exist_ok=True)
CSV_PATH = DATA_DIR / "black_metrics.csv"      # <-- main timeseries
SUMMARY_PATH = DATA_DIR / "daily_summary.txt"  # <-- tweet source
RAW_DS_PATH = DATA_DIR / "dexscreener_raw.json"
RAW_TVL_PATH = DATA_DIR / "tvl_raw.json"
RAW_FEES_PATH = DATA_DIR / "fees_raw.json"
RAW_INCENTIVES_PATH = DATA_DIR / "incentives_raw.json"
DEBUG_PATH = DATA_DIR / "debug_counts.txt"

# ---------------- Endpoints (API — will be bypassed if flaky) ----------------
# DexScreener (pairs + search) for volume
DS_SEARCH = "https://api.dexscreener.com/latest/dex/search?q="
# DeFiLlama (keep for best-effort; scraper will override if needed)
TVL_SIMPLE = "https://api.llama.fi/tvl/{slug}"
TVL_PROTO  = "https://api.llama.fi/protocol/{slug}"
LL_FEES_SUMMARY = "https://api.llama.fi/summary/fees/{slug}"
LL_INCENTIVES = "https://api.llama.fi/incentives/{slug}"

# ---------------- Public page scrape (reliable fallback) ----------------
from bs4 import BeautifulSoup
LLAMA_PROTOCOL_URL = "https://defillama.com/protocol/{slug}"
_HTTP_HEADERS = {
    "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/120.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
}

# ---------------- Slugs ----------------
SLUG_TVL_COMBINED = "blackhole"
SLUG_FEES_AMM = "blackhole-amm"
SLUG_FEES_CLMM = "blackhole-clmm"

# ---------------- Helpers ----------------
def today_utc() -> str:
    return datetime.now(timezone.utc).date().isoformat()

def safe_get_json(url: str, timeout: int = 45) -> Any:
    try:
        r = requests.get(url, timeout=timeout)
        r.raise_for_status()
        return r.json()
    except Exception as e:
        return {"_error": str(e), "_url": url}

def to_float(x: Any) -> Optional[float]:
    try:
        if x is None: return None
        return float(x)
    except:
        return None

# ---------------- DexScreener: Blackhole Volume (24h) ----------------
def get_blackhole_volume_24h() -> Tuple[Optional[float], Dict[str, Any]]:
    query = "blackhole avalanche"
    keywords = ["blackhole"]
    url = DS_SEARCH + requests.utils.quote(query)
    payload: Any = safe_get_json(url)

    total = 0.0
    matched = 0
    pairs = (payload or {}).get("pairs") or []
    for p in pairs:
        if (p.get("chainId") or "").lower() != "avalanche":
            continue
        dexid = (p.get("dexId") or "").lower()
        urlp  = (p.get("url") or "").lower()
        if not (any(k in dexid for k in keywords) or any(k in urlp for k in keywords)):
            continue
        vol = p.get("volume", {})
        v = vol.get("h24", p.get("volume24h"))
        try:
            total += float(v); matched += 1
        except:
            pass

    return (total if matched > 0 else None), {
        "search_query": query,
        "matched_pairs": matched,
        "total_pairs_seen": len(pairs),
        "raw": payload
    }

# ---------------- DeFiLlama (best-effort API) ----------------
def get_tvl_combined(slug: str = SLUG_TVL_COMBINED) -> Tuple[Optional[float], Dict[str, Any]]:
    j1 = safe_get_json(TVL_SIMPLE.format(slug=slug))
    if isinstance(j1, (int, float)):
        return float(j1), {"source": "tvl_simple_number", "value": j1}
    if isinstance(j1, dict) and "tvl" in j1 and isinstance(j1["tvl"], (int, float)):
        return float(j1["tvl"]), {"source": "tvl_simple_obj", "value": j1}

    j2 = safe_get_json(TVL_PROTO.format(slug=slug))
    arr = (j2 or {}).get("tvl") or []
    if isinstance(arr, list) and arr:
        last = arr[-1]
        if isinstance(last, (list, tuple)) and len(last) >= 2 and last[1] is not None:
            v = to_float(last[1])
            if v is not None: return v, {"source": "protocol_timeseries", "last": last}
        if isinstance(last, dict) and "totalLiquidityUSD" in last:
            v = to_float(last.get("totalLiquidityUSD"))
            if v is not None: return v, {"source": "protocol_timeseries_dict", "last": last}
    return None, {"source": "none", "note": "no tvl value found", "raw": j2}

def get_fees_summary(slug: str) -> Tuple[Optional[float], Optional[float], Dict[str, Any]]:
    j = safe_get_json(LL_FEES_SUMMARY.format(slug=slug))
    total24 = None; total7 = None
    if isinstance(j, dict):
        total24 = to_float(j.get("total24h"))
        total7 = to_float(j.get("total7d"))
        if total24 is None and isinstance(j.get("totalDataChart"), list) and j["totalDataChart"]:
            try: total24 = to_float(j["totalDataChart"][-1][1])
            except: pass
        if total7 is None and isinstance(j.get("totalDataChart7d"), list) and j["totalDataChart7d"]:
            try: total7 = to_float(j["totalDataChart7d"][-1][1])
            except: pass
    return total24, total7, j

def get_blackhole_fees_and_revenue() -> Tuple[Dict[str, Optional[float]], Dict[str, Any]]:
    f24_amm, f7_amm, raw_amm = get_fees_summary(SLUG_FEES_AMM)
    f24_clmm, f7_clmm, raw_clmm = get_fees_summary(SLUG_FEES_CLMM)
    fees_24h = (f24_amm or 0.0) + (f24_clmm or 0.0) if (f24_amm is not None or f24_clmm is not None) else None
    fees_7d  = (f7_amm or 0.0) + (f7_clmm or 0.0) if (f7_amm is not None or f7_clmm is not None) else None
    result = {"fees_24h_usd": fees_24h, "fees_7d_usd": fees_7d, "revenue_24h_usd": None, "revenue_7d_usd": None}
    raw = {"amm": raw_amm, "clmm": raw_clmm}
    return result, raw

def extract_bribes_from_incentives_json(j: Any) -> Tuple[Optional[float], Optional[float]]:
    if isinstance(j, dict):
        for k in ["bribes24hUsd","bribes_24h_usd","bribes24h","totalBribes24hUsd"]:
            v = to_float(j.get(k))
            if v is not None:
                b24 = v
                b7 = to_float(j.get("bribes7dUsd")) or to_float(j.get("totalBribes7dUsd"))
                return b24, b7
    if isinstance(j, list):
        b24=b7=None
        for it in j:
            if isinstance(it, dict):
                if "bribe" in (it.get("type","")).lower():
                    val = to_float(it.get("valueUsd") or it.get("usd") or it.get("value"))
                    w = (it.get("window") or it.get("period") or "").lower()
                    if "24" in w: b24 = (b24 or 0) + (val or 0)
                    if "7" in w:  b7 = (b7 or 0) + (val or 0)
        return b24,b7
    return None,None

def get_blackhole_bribes(slug: str = SLUG_TVL_COMBINED) -> Tuple[Dict[str, Optional[float]], Dict[str, Any]]:
    j = safe_get_json(LL_INCENTIVES.format(slug=slug))
    b24,b7 = extract_bribes_from_incentives_json(j)
    return {"bribes_24h_usd": b24, "bribes_7d_usd": b7}, j

# ---------------- Public-page scraper (fallback/primary) ----------------
def _get_llama_next_data(slug: str) -> Optional[dict]:
    url = LLAMA_PROTOCOL_URL.format(slug=slug)
    try:
        resp = requests.get(url, headers=_HTTP_HEADERS, timeout=45)
        resp.raise_for_status()
    except Exception as e:
        print(f"LL scrape GET failed: {e}")
        return None

    soup = BeautifulSoup(resp.text, "html.parser")
    tag = soup.find("script", id="__NEXT_DATA__")
    raw = tag.string if tag and tag.string else None
    if not raw:
        m = re.search(r'<script[^>]*id="__NEXT_DATA__"[^>]*>(.*?)</script>', resp.text, re.S)
        raw = m.group(1) if m else None
    if not raw:
        print("LL scrape: __NEXT_DATA__ not found")
        return None
    try:
        return json.loads(raw)
    except Exception as e:
        print(f"LL scrape JSON parse error: {e}")
        return None

def _deep_find_numbers(d: Any, keys: Tuple[str, ...]) -> Dict[str, Optional[float]]:
    want = {k.lower(): None for k in keys}
    def walk(x):
        if isinstance(x, dict):
            for k,v in x.items():
                lk = k.lower()
                if lk in want and want[lk] is None and isinstance(v, (int, float)):
                    want[lk] = float(v)
                walk(v)
        elif isinstance(x, list):
            for it in x: walk(it)
    walk(d)
    return want

def scrape_llama_blackhole_metrics(slug: str = "blackhole") -> Dict[str, float]:
    data = _get_llama_next_data(slug)
    if not data: return {}
    node = data.get("props") or {}
    for k in ("pageProps", "dehydratedState", "initialState", "fallback"):
        node = node.get(k, node)
    wanted = _deep_find_numbers(node, keys=(
        "tvl","tvlUsd","totalLiquidityUSD","volume24h","dailyVolumeUsd",
        "fees24h","total24h","revenue24h","fees7d","total7d","revenue7d",
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
    return {k:v for k,v in result.items() if v is not None}

# ---------------- CSV Upsert & Rolling Avg ----------------
def upsert_today(csv_path: Path, date_str: str, row: Dict[str, Any]) -> pd.DataFrame:
    cols = ["date","volume_24h_usd","tvl_usd","fees_24h_usd","fees_7d_usd",
            "revenue_24h_usd","revenue_7d_usd","bribes_24h_usd","bribes_7d_usd","avg7d_volume_usd"]
    if csv_path.exists(): df = pd.read_csv(csv_path)
    else: df = pd.DataFrame(columns=cols)
    for c in cols:
        if c not in df.columns: df[c] = None
    df = df[df["date"] != date_str]
    df = pd.concat([df, pd.DataFrame([row])], ignore_index=True)
    df = df.sort_values("date").reset_index(drop=True)
    s = pd.to_numeric(df["volume_24h_usd"], errors="coerce").rolling(window=7, min_periods=1).mean()
    df["avg7d_volume_usd"] = s.values
    df.to_csv(csv_path, index=False)
    # TEMP diagnostic copies to help locate files if cwd is odd:
    try:
        df.to_csv(DATA_DIR / "black_metrics_copy.csv", index=False)
        df.to_csv(Path.cwd() / "black_metrics_at_cwd.csv", index=False)
    except Exception as _e:
        print(f"(non-fatal) extra copy failed: {_e}")
    return df

# ---------------- Main ----------------
def main():
    date_str = today_utc()

    # Volume (DexScreener)
    bh_vol_24h, raw_ds = get_blackhole_volume_24h()
    RAW_DS_PATH.write_text(json.dumps(raw_ds, indent=2)[:300000], encoding="utf-8")

    # TVL (API, best-effort)
    tvl_val, tvl_raw = get_tvl_combined(SLUG_TVL_COMBINED)
    RAW_TVL_PATH.write_text(json.dumps(tvl_raw, indent=2), encoding="utf-8")

    # Fees/Revenue (API, best-effort)
    fees_map, fees_raw = get_blackhole_fees_and_revenue()
    RAW_FEES_PATH.write_text(json.dumps(fees_raw, indent=2)[:300000], encoding="utf-8")

    # Bribes (API, best-effort)
    bribe_map, inc_raw = get_blackhole_bribes(SLUG_TVL_COMBINED)
    RAW_INCENTIVES_PATH.write_text(json.dumps(inc_raw, indent=2)[:300000], encoding="utf-8")

    # ---- Public page scrape (fallback to fill any Nones) ----
    scraped = scrape_llama_blackhole_metrics("blackhole")
    if scraped:
        if tvl_val is None and scraped.get("tvl_usd") is not None:
            tvl_val = scraped["tvl_usd"]
        if (bh_vol_24h is None or bh_vol_24h == 0) and scraped.get("volume_24h_usd") is not None:
            bh_vol_24h = scraped["volume_24h_usd"]

        # Make sure maps exist even if APIs were totally empty
        if "fees_map" not in locals(): fees_map = {"fees_24h_usd": None, "fees_7d_usd": None, "revenue_24h_usd": None, "revenue_7d_usd": None}
        if "bribe_map" not in locals(): bribe_map = {"bribes_24h_usd": None, "bribes_7d_usd": None}

        for k in ("fees_24h_usd","fees_7d_usd","revenue_24h_usd","revenue_7d_usd"):
            if fees_map.get(k) is None and scraped.get(k) is not None:
                fees_map[k] = scraped[k]
        for k in ("bribes_24h_usd","bribes_7d_usd"):
            if bribe_map.get(k) is None and scraped.get(k) is not None:
                bribe_map[k] = scraped[k]

    # Build today row
    row = {
        "date": date_str,
        "volume_24h_usd": float(bh_vol_24h) if isinstance(bh_vol_24h, (int, float)) else None,
        "tvl_usd": float(tvl_val) if isinstance(tvl_val, (int, float)) else None,
        "fees_24h_usd": fees_map.get("fees_24h_usd"),
        "fees_7d_usd": fees_map.get("fees_7d_usd"),
        "revenue_24h_usd": fees_map.get("revenue_24h_usd"),
        "revenue_7d_usd": fees_map.get("revenue_7d_usd"),
        "bribes_24h_usd": bribe_map.get("bribes_24h_usd"),
        "bribes_7d_usd": bribe_map.get("bribes_7d_usd"),
        "avg7d_volume_usd": None
    }
    df = upsert_today(CSV_PATH, date_str, row)

    # Compose summary (omit N/A; always add timestamp)
    def money(v: Optional[float]) -> Optional[str]:
        return f"${v:,.0f}" if isinstance(v, (int, float)) else None

    latest = df[df["date"] == date_str].tail(1)
    vals = {
        "vol": float(latest["volume_24h_usd"].values[0]) if not latest.empty and pd.notna(latest["volume_24h_usd"].values[0]) else None,
        "tvl": float(latest["tvl_usd"].values[0]) if not latest.empty and pd.notna(latest["tvl_usd"].values[0]) else None,
        "fees24": float(latest["fees_24h_usd"].values[0]) if not latest.empty and pd.notna(latest["fees_24h_usd"].values[0]) else None,
        "bribes24": float(latest["bribes_24h_usd"].values[0]) if not latest.empty and pd.notna(latest["bribes_24h_usd"].values[0]) else None,
        "avg7": float(latest["avg7d_volume_usd"].values[0]) if not latest.empty and pd.notna(latest["avg7d_volume_usd"].values[0]) else None,
    }

    asof = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    lines = [
        f"📊 Blackhole Daily Snapshot ({date_str})",
        "",
        *( (f"🔸 24h Volume: {money(vals['vol'])}",) if money(vals["vol"]) else () ),
        *( (f"🔹 TVL: {money(vals['tvl'])}",) if money(vals["tvl"]) else () ),
        *( (f"💸 24h Fees (AMM+CLMM): {money(vals['fees24'])}",) if money(vals["fees24"]) else () ),
        *( (f"🎁 24h Bribes: {money(vals['bribes24'])}",) if money(vals["bribes24"]) else () ),
        *( (f"📈 7-Day Avg Volume: {money(vals['avg7'])}",) if money(vals["avg7"]) else () ),
        "",
        f"⏱️ As of {asof}",
        "Sources: DexScreener (volume), DeFiLlama (public page scrape for TVL/Fees/Bribes)",
        "#DeFi #Avalanche #DEX #BlackholeDex"
    ]
    SUMMARY_PATH.write_text("\n".join([ln for ln in lines if ln]), encoding="utf-8")

    # Debug + assertions so we know where files landed
    DEBUG_PATH.write_text(json.dumps({
        "date": date_str,
        "values": vals,
        "row": row,
        "scrape_used": bool(scraped),
    }, indent=2), encoding="utf-8")

    print(f"PWD: {os.getcwd()}")
    print(f"DATA_DIR: {DATA_DIR.resolve()}")
    print(f"✅ Wrote CSV to: {CSV_PATH.resolve()}")
    print(f"✅ Wrote summary to: {SUMMARY_PATH.resolve()}")
    if not CSV_PATH.exists():
        raise RuntimeError(f"CSV not found after write attempt: {CSV_PATH.resolve()}")
    if not SUMMARY_PATH.exists():
        raise RuntimeError(f"Summary not found after write attempt: {SUMMARY_PATH.resolve()}")

if __name__ == "__main__":
    main()
