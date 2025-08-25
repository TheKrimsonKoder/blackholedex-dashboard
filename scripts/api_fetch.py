# === scripts/api_fetch.py — Blackhole-only: Volume, TVL, Fees, Bribes ===
from __future__ import annotations

from pathlib import Path
from datetime import datetime, timezone
from typing import List, Tuple, Dict, Optional, Any
import json, time, requests, pandas as pd

# ---------------- Paths ----------------
DATA_DIR = Path("data"); DATA_DIR.mkdir(parents=True, exist_ok=True)
CSV_PATH = DATA_DIR / "black_metrics.csv"
SUMMARY_PATH = DATA_DIR / "daily_summary.txt"
RAW_DS_PATH = DATA_DIR / "dexscreener_raw.json"
RAW_TVL_PATH = DATA_DIR / "tvl_raw.json"
RAW_FEES_PATH = DATA_DIR / "fees_raw.json"
RAW_INCENTIVES_PATH = DATA_DIR / "incentives_raw.json"
DEBUG_PATH = DATA_DIR / "debug_counts.txt"

# ---------------- Config / Endpoints ----------------
# DexScreener (pairs + search)
DS_SEARCH = "https://api.dexscreener.com/latest/dex/search?q="

# DeFiLlama — TVL
TVL_SIMPLE = "https://api.llama.fi/tvl/{slug}"               # returns number or object
TVL_PROTO  = "https://api.llama.fi/protocol/{slug}"          # returns protocol object w/ tvl series

# DeFiLlama — Fees/Revenue summary (we sum AMM + CLMM)
LL_FEES_SUMMARY = "https://api.llama.fi/summary/fees/{slug}" # expects keys like total24h/total7d/total30d

# DeFiLlama — Incentives/Bribes (best-effort; schema may vary by project/day)
LL_INCENTIVES = "https://api.llama.fi/incentives/{slug}"

# Blackhole slugs on Llama
SLUG_TVL_COMBINED = "blackhole"         # combined tvl/volume page
SLUG_FEES_AMM = "blackhole-amm"         # AMM fees breakdown
SLUG_FEES_CLMM = "blackhole-clmm"       # CLMM fees breakdown

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
    """
    Best-effort: query DexScreener search for 'blackhole avalanche',
    sum h24 volumes for pairs where dexId/url mentions 'blackhole' on Avalanche.
    """
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
            total += float(v)
            matched += 1
        except:
            pass

    return (total if matched > 0 else None), {"search_query": query, "keywords": keywords, "matched_pairs": matched, "total_pairs_seen": len(pairs), "raw": payload}

# ---------------- DeFiLlama: TVL (combined) ----------------
def get_tvl_combined(slug: str = SLUG_TVL_COMBINED) -> Tuple[Optional[float], Dict[str, Any]]:
    # Try simple first
    j1 = safe_get_json(TVL_SIMPLE.format(slug=slug))
    if isinstance(j1, (int, float)):
        return float(j1), {"source": "tvl_simple_number", "value": j1}
    if isinstance(j1, dict) and "tvl" in j1 and isinstance(j1["tvl"], (int, float)):
        return float(j1["tvl"]), {"source": "tvl_simple_obj", "value": j1}

    # Fallback to protocol timeseries
    j2 = safe_get_json(TVL_PROTO.format(slug=slug))
    arr = (j2 or {}).get("tvl") or []
    if isinstance(arr, list) and arr:
        last = arr[-1]
        # can be [timestamp, value] or dicts; handle both
        if isinstance(last, (list, tuple)) and len(last) >= 2 and last[1] is not None:
            v = to_float(last[1])
            if v is not None:
                return v, {"source": "protocol_timeseries", "last": last}
        if isinstance(last, dict) and "totalLiquidityUSD" in last:
            v = to_float(last.get("totalLiquidityUSD"))
            if v is not None:
                return v, {"source": "protocol_timeseries_dict", "last": last}
    return None, {"source": "none", "note": "no tvl value found", "raw": j2}

# ---------------- DeFiLlama: Fees/Revenue (AMM + CLMM) ----------------
def get_fees_summary(slug: str) -> Tuple[Optional[float], Optional[float], Dict[str, Any]]:
    """
    Returns (fees_24h, fees_7d, raw). 'summary/fees' usually exposes total24h/total7d.
    """
    j = safe_get_json(LL_FEES_SUMMARY.format(slug=slug))
    total24 = None
    total7 = None
    if isinstance(j, dict):
        total24 = to_float(j.get("total24h"))
        total7 = to_float(j.get("total7d"))
        # some variants nest under "totalDataChart" or similar—best-effort grab latest if needed
        if total24 is None and isinstance(j.get("totalDataChart"), list) and j["totalDataChart"]:
            try:
                total24 = to_float(j["totalDataChart"][-1][1])
            except:
                pass
        if total7 is None and isinstance(j.get("totalDataChart7d"), list) and j["totalDataChart7d"]:
            try:
                total7 = to_float(j["totalDataChart7d"][-1][1])
            except:
                pass
    return total24, total7, j

def get_blackhole_fees_and_revenue() -> Tuple[Dict[str, Optional[float]], Dict[str, Any]]:
    """
    Sum AMM + CLMM 24h/7d fees. For 'revenue', many Llama summaries equate to fees for DEXs;
    if revenue is explicitly provided later, adapt here (kept as same for now).
    """
    f24_amm, f7_amm, raw_amm = get_fees_summary(SLUG_FEES_AMM)
    f24_clmm, f7_clmm, raw_clmm = get_fees_summary(SLUG_FEES_CLMM)

    fees_24h = (f24_amm or 0.0) + (f24_clmm or 0.0) if (f24_amm or f24_clmm) is not None else None
    fees_7d  = (f7_amm or 0.0) + (f7_clmm or 0.0) if (f7_amm or f7_clmm) is not None else None

    # If you later want *revenue* specifically and Llama exposes it in summary, add a dedicated accessor.
    result = {"fees_24h_usd": fees_24h, "fees_7d_usd": fees_7d, "revenue_24h_usd": None, "revenue_7d_usd": None}
    raw = {"amm": raw_amm, "clmm": raw_clmm}
    return result, raw

# ---------------- DeFiLlama: Incentives / Bribes (best-effort) ----------------
def extract_bribes_from_incentives_json(j: Any) -> Tuple[Optional[float], Optional[float]]:
    """
    Incentives schemas vary. We try common shapes:
    - list of emissions/bribes entries with {type, valueUsd, window}
    - totals keyed by 'bribes24hUsd'/'bribes7dUsd'
    - projects/pools arrays with daily USD amounts
    """
    # Direct totals
    if isinstance(j, dict):
        for k in ["bribes24hUsd", "bribes_24h_usd", "bribes24h", "totalBribes24hUsd"]:
            v = to_float(j.get(k))
            if v is not None:
                b24 = v
                b7 = to_float(j.get("bribes7dUsd")) or to_float(j.get("totalBribes7dUsd"))
                return b24, b7

    # Walk arrays and sum
    def sum_if_present(items, window_keys=("24h", "7d")):
        b24 = 0.0; b7 = 0.0; seen24 = False; seen7 = False
        for it in items:
            if not isinstance(it, dict): continue
            t = (it.get("type") or "").lower()
            val = to_float(it.get("valueUsd") or it.get("usd") or it.get("value"))
            w = (it.get("window") or it.get("period") or "").lower()
            if t.startswith("bribe") or "bribe" in t:
                if "24" in w:
                    if val is not None: b24 += val; seen24 = True
                elif "7" in w:
                    if val is not None: b7 += val; seen7 = True
        return (b24 if seen24 else None, b7 if seen7 else None)

    if isinstance(j, list):
        return sum_if_present(j)

    # Nested
    for key in ["data", "list", "entries", "items", "protocols", "pools"]:
        if isinstance(j, dict) and isinstance(j.get(key), list):
            return sum_if_present(j[key])

    return None, None

def get_blackhole_bribes(slug: str = SLUG_TVL_COMBINED) -> Tuple[Dict[str, Optional[float]], Dict[str, Any]]:
    j = safe_get_json(LL_INCENTIVES.format(slug=slug))
    b24, b7 = extract_bribes_from_incentives_json(j)
    return {"bribes_24h_usd": b24, "bribes_7d_usd": b7}, j

# ---------------- CSV Upsert & Rolling Avg ----------------
def upsert_today(csv_path: Path, date_str: str, row: Dict[str, Any]) -> pd.DataFrame:
    cols = [
        "date", "volume_24h_usd", "tvl_usd",
        "fees_24h_usd", "fees_7d_usd",
        "revenue_24h_usd", "revenue_7d_usd",
        "bribes_24h_usd", "bribes_7d_usd",
        "avg7d_volume_usd"
    ]
    if csv_path.exists():
        df = pd.read_csv(csv_path)
    else:
        df = pd.DataFrame(columns=cols)
    for c in cols:
        if c not in df.columns:
            df[c] = None

    # remove any existing for today, then append
    df = df[df["date"] != date_str]
    add = pd.DataFrame([row])
    df = pd.concat([df, add], ignore_index=True)

    # rolling 7d avg on volume
    df = df.sort_values("date").reset_index(drop=True)
    s = pd.to_numeric(df["volume_24h_usd"], errors="coerce").rolling(window=7, min_periods=1).mean()
    df
