from __future__ import annotations
from pathlib import Path
from datetime import datetime
from zoneinfo import ZoneInfo
import os, math, json, sys
import pandas as pd
import requests

DATA_CSV = Path("data/black_data.csv")
DATA_CSV.parent.mkdir(parents=True, exist_ok=True)

# --- Config knobs (override via repo/environment if needed) ---
# Chain we care about for DEX volumes
CHAIN = os.getenv("CHAIN", "avalanche")
# Candidate protocol names for Blackhole in DeFiLlama responses
BLACKHOLE_NAMES = json.loads(os.getenv("BLACKHOLE_NAMES", '["BlackHole","Blackhole","BlackHoleDex","BlackHole DEX","BlackHoleSwap","BlackHole (Avalanche)"]'))
TRADERJOE_NAMES = json.loads(os.getenv("TRADERJOE_NAMES", '["Trader Joe","TraderJoe"]'))
PANGOLIN_NAMES  = json.loads(os.getenv("PANGOLIN_NAMES",  '["Pangolin"]'))
# Candidate protocol slugs for TVL (DeFiLlama /protocol/<slug>)
TVL_SLUGS = json.loads(os.getenv("TVL_SLUGS", '["blackhole","blackhole-dex","blackholeswap"]'))

# Timeouts / retry
HTTP_TIMEOUT = float(os.getenv("HTTP_TIMEOUT", "20"))

def now_et_date() -> str:
    # Stamp rows with America/New_York (as your tweet date uses ET)
    try:
        return datetime.now(ZoneInfo("America/New_York")).strftime("%Y-%m-%d")
    except Exception:
        return datetime.now().strftime("%Y-%m-%d")

def http_json(url: str):
    r = requests.get(url, timeout=HTTP_TIMEOUT, headers={"User-Agent": "blackholedex/metrics"})
    r.raise_for_status()
    return r.json()

# ---------- Upstream fetchers ----------
def fetch_llama_dex_summary(chain: str) -> list[dict]:
    """
    DeFiLlama summary of DEX volumes by chain.
    Example endpoint (public):
      https://api.llama.fi/summary/dexs/<chain>?excludeTotalDataChart=true&excludeTotalDataChartBreakdown=true
    We expect a dict with "protocols": [{name, total24h, ...}, ...]
    """
    url = f"https://api.llama.fi/summary/dexs/{chain}?excludeTotalDataChart=true&excludeTotalDataChartBreakdown=true"
    try:
        data = http_json(url)
        # Some variants return data under 'protocols', older variants may differ — be defensive
        protos = data.get("protocols") or data.get("data") or []
        if isinstance(protos, dict):  # some versions nest under key
            protos = protos.get("protocols", [])
        return protos if isinstance(protos, list) else []
    except Exception as e:
        print(f"⚠️  Llama DEX summary fetch failed: {e}")
        return []

def pick_protocol_value(protocols: list[dict], candidate_names: list[str], field="total24h") -> float | None:
    if not protocols:
        return None
    # First pass: exact or case-insensitive matches
    for want in candidate_names:
        for p in protocols:
            name = str(p.get("name", "")).strip()
            if name.lower() == want.lower():
                v = p.get(field)
                try:
                    v = float(v)
                    return v if v >= 0 else None
                except Exception:
                    pass
    # Second pass: contains match
    for want in candidate_names:
        w = want.lower()
        for p in protocols:
            name = str(p.get("name", "")).strip().lower()
            if w in name:
                v = p.get(field)
                try:
                    v = float(v)
                    return v if v >= 0 else None
                except Exception:
                    pass
    return None

def fetch_llama_tvl(slugs: list[str]) -> float | None:
    """
    Try a few protocol slugs; TVL endpoint returns daily points – take last totalLiquidityUSD.
    Example: https://api.llama.fi/protocol/blackhole
    """
    for slug in slugs:
        url = f"https://api.llama.fi/protocol/{slug}"
        try:
            data = http_json(url)
            tvl_arr = data.get("tvl", [])
            if isinstance(tvl_arr, list) and tvl_arr:
                last = tvl_arr[-1]
                v = last.get("totalLiquidityUSD")
                if v is None:
                    # some objects use 'totalLiquidity' or 'tvl' keys – be tolerant
                    v = last.get("totalLiquidity") or last.get("tvl")
                if v is not None:
                    v = float(v)
                    if v >= 0:
                        return v
        except Exception as e:
            print(f"⚠️  Llama TVL fetch failed for slug '{slug}': {e}")
    return None

# ---------- CSV helpers ----------
def load_existing() -> pd.DataFrame:
    if DATA_CSV.exists():
        try:
            df = pd.read_csv(DATA_CSV)
            return df
        except Exception as e:
            print(f"⚠️  Could not read existing CSV: {e}")
    return pd.DataFrame()

def safe_float(x) -> float | None:
    try:
        f = float(x)
        if math.isfinite(f):
            return f
        return None
    except Exception:
        return None

def compute_7d_avg(series: pd.Series) -> float | None:
    s = pd.to_numeric(series, errors="coerce").dropna()
    if len(s) == 0:
        return None
    return float(s.tail(7).mean())

# ---------- main ----------
def main():
    today = now_et_date()
    print(f"🕒 Building row for {today} (ET)")

    # 1) Load existing CSV (for fallback + 7d avg)
    df_old = load_existing()
    if not df_old.empty and "date" in df_old.columns:
        try:
            df_old["date"] = pd.to_datetime(df_old["date"], errors="coerce")
        except Exception:
            pass
        df_old = df_old.sort_values("date", na_position="first")

    # 2) Try live pulls
    protos = fetch_llama_dex_summary(CHAIN)
    bh_vol = pick_protocol_value(protos, BLACKHOLE_NAMES, field="total24h")
    tj_vol = pick_protocol_value(protos, TRADERJOE_NAMES, field="total24h")
    pg_vol = pick_protocol_value(protos, PANGOLIN_NAMES, field="total24h")
    tvl    = fetch_llama_tvl(TVL_SLUGS)

    # 3) Fallbacks to last known values (so we still write today's row)
    def last_known(col: str) -> float | None:
        if df_old.empty or col not in df_old.columns:
            return None
        s = pd.to_numeric(df_old[col], errors="coerce").dropna()
        return float(s.iloc[-1]) if len(s) else None

    if bh_vol is None: bh_vol = last_known("blackhole_volume_24h_usd") or last_known("volume_24h_usd")
    if tj_vol is None: tj_vol = last_known("traderjoe_volume_24h_usd")
    if pg_vol is None: pg_vol = last_known("pangolin_volume_24h_usd")
    if tvl    is None: tvl    = last_known("tvl_usd")

    # 4) Construct new row
    new_row = {
        "date": today,
        "blackhole_volume_24h_usd": bh_vol,
        "traderjoe_volume_24h_usd": tj_vol,
        "pangolin_volume_24h_usd": pg_vol,
        "tvl_usd": tvl,
    }

    # 5) Merge into dataframe (append or create)
    if df_old.empty:
        df = pd.DataFrame([new_row])
    else:
        # If a row for 'today' exists, replace it; else append
        if str(df_old.iloc[-1].get("date", ""))[:10] == today:
            df_old.iloc[-1, :] = pd.Series(new_row)
            df = df_old
        else:
            df = pd.concat([df_old, pd.DataFrame([new_row])], ignore_index=True)

    # 6) Compute/refresh 7d averages
    if "blackhole_volume_24h_usd" in df.columns:
        vol7 = compute_7d_avg(df["blackhole_volume_24h_usd"])
    else:
        vol7 = None
    df["volume_7d_avg_usd"] = vol7

    # You can also compute fees_24h_usd if you later add that source
    # df["fees_24h_usd"] = ...

    # 7) Save
    df.to_csv(DATA_CSV, index=False)
    print("✅ Wrote:", DATA_CSV)
    print(df.tail(3).to_string(index=False))

if __name__ == "__main__":
    main()
