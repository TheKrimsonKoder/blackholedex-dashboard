from __future__ import annotations
from pathlib import Path
from datetime import datetime
from zoneinfo import ZoneInfo
import os, math, json
import pandas as pd
import requests
import re

DATA_CSV = Path("data/black_data.csv")
DATA_CSV.parent.mkdir(parents=True, exist_ok=True)

CHAIN = os.getenv("CHAIN", "avalanche").strip()

# Broad alias lists; you can extend via env with JSON
BLACKHOLE_NAMES = json.loads(os.getenv("BLACKHOLE_NAMES", '["BlackHole","Blackhole","Blackhole DEX","BlackHole DEX","BlackholeSwap","BlackHoleSwap","BlackHole (Avalanche)","Blackhole (Avalanche)","BlackholeDex"]'))
TRADERJOE_NAMES = json.loads(os.getenv("TRADERJOE_NAMES", '["Trader Joe","Trader Joe v2","Trader Joe v2.1","Trader Joe v3","TraderJoe","TraderJoe v2","TraderJoe v3","TJ"]'))
PANGOLIN_NAMES  = json.loads(os.getenv("PANGOLIN_NAMES",  '["Pangolin","Pangolin Exchange"]'))
TVL_SLUGS       = json.loads(os.getenv("TVL_SLUGS", '["blackhole","blackhole-dex","blackholeswap"]'))

HTTP_TIMEOUT = float(os.getenv("HTTP_TIMEOUT", "25"))

def now_et_date() -> str:
    try:
        return datetime.now(ZoneInfo("America/New_York")).strftime("%Y-%m-%d")
    except Exception:
        return datetime.now().strftime("%Y-%m-%d")

def http_json(url: str):
    r = requests.get(url, timeout=HTTP_TIMEOUT, headers={"User-Agent": "blackholedex/metrics"})
    r.raise_for_status()
    return r.json()

def normalize_name(s: str) -> str:
    """lowercase letters/digits only, collapse spaces, remove punctuation; e.g. 'Trader Joe v2.1' -> 'traderjoev21'"""
    s = s.lower()
    s = re.sub(r"[\s\-_]+", "", s)
    s = re.sub(r"[^a-z0-9]", "", s)
    return s

def name_matches(name: str, candidates: list[str]) -> bool:
    """Token/substring fuzzy match against many variants."""
    n = normalize_name(name)
    for want in candidates:
        w = normalize_name(want)
        if not w:
            continue
        if w in n or n in w:
            return True
        # token presence: require both 'trader' and 'joe' for Trader Joe
        if ("trader" in n and "joe" in n and "trader" in w and "joe" in w):
            return True
        if ("blackhole" in n and "blackhole" in w):
            return True
        if ("pangolin" in n and "pangolin" in w):
            return True
    return False

def fetch_llama_dex_summary(chain: str) -> list[dict]:
    url = f"https://api.llama.fi/summary/dexs/{chain}?excludeTotalDataChart=true&excludeTotalDataChartBreakdown=true"
    try:
        data = http_json(url)
        protos = data.get("protocols") or data.get("data") or []
        if isinstance(protos, dict):
            protos = protos.get("protocols", [])
        return protos if isinstance(protos, list) else []
    except Exception as e:
        print(f"⚠️  Llama DEX summary fetch failed: {e}")
        return []

def fetch_llama_tvl(slugs: list[str]) -> float | None:
    for slug in slugs:
        url = f"https://api.llama.fi/protocol/{slug}"
        try:
            data = http_json(url)
            tvl_arr = data.get("tvl", [])
            if isinstance(tvl_arr, list) and tvl_arr:
                last = tvl_arr[-1]
                v = last.get("totalLiquidityUSD") or last.get("totalLiquidity") or last.get("tvl")
                if v is not None:
                    v = float(v)
                    if math.isfinite(v):
                        return v
        except Exception as e:
            print(f"⚠️  Llama TVL fetch failed for slug '{slug}': {e}")
    return None

def pick_value(protocols: list[dict], aliases: list[str], field="total24h") -> tuple[float | None, str | None]:
    """Return (value, matched_name) for the first protocol whose name matches aliases using fuzzy logic."""
    if not protocols:
        return (None, None)

    # 1) exact/ci match
    for p in protocols:
        name = str(p.get("name", "")).strip()
        for want in aliases:
            if name.lower() == str(want).strip().lower():
                v = p.get(field)
                try:
                    f = float(v)
                    return (f if math.isfinite(f) and f >= 0 else None, name)
                except Exception:
                    pass

    # 2) fuzzy match
    for p in protocols:
        name = str(p.get("name", "")).strip()
        if name_matches(name, aliases):
            v = p.get(field)
            try:
                f = float(v)
                return (f if math.isfinite(f) and f >= 0 else None, name)
            except Exception:
                continue

    # 3) nothing matched
    return (None, None)

def load_existing() -> pd.DataFrame:
    if DATA_CSV.exists():
        try:
            return pd.read_csv(DATA_CSV)
        except Exception as e:
            print(f"⚠️  Could not read existing CSV: {e}")
    return pd.DataFrame()

def compute_7d_avg(series: pd.Series) -> float | None:
    s = pd.to_numeric(series, errors="coerce").dropna()
    if len(s) == 0:
        return None
    return float(s.tail(7).mean())

def main():
    today = now_et_date()
    print(f"🕒 Building row for {today} (ET) | chain={CHAIN}")

    df_old = load_existing()
    if not df_old.empty and "date" in df_old.columns:
        df_old["date"] = pd.to_datetime(df_old["date"], errors="coerce")
        df_old = df_old.sort_values("date", na_position="first")

    # ---- Live fetch ----
    protocols = fetch_llama_dex_summary(CHAIN)

    # Debug: print first 15 protocol names with total24h so you can see what’s there
    if protocols:
        preview = [(p.get("name"), p.get("total24h")) for p in protocols[:15]]
        print("🔎 Llama protocols preview (name → total24h):")
        for name, v in preview:
            print(f"   - {name}: {v}")

    bh_vol, bh_match = pick_value(protocols, BLACKHOLE_NAMES, "total24h")
    tj_vol, tj_match = pick_value(protocols, TRADERJOE_NAMES, "total24h")
    pg_vol, pg_match = pick_value(protocols, PANGOLIN_NAMES, "total24h")
    tvl = fetch_llama_tvl(TVL_SLUGS)

    print(f"✅ Matched Blackhole as: {bh_match} → {bh_vol}")
    print(f"✅ Matched Trader Joe as: {tj_match} → {tj_vol}")
    print(f"✅ Matched Pangolin  as: {pg_match} → {pg_vol}")
    print(f"✅ TVL: {tvl}")

    # ---- Fallbacks to last known if live missing ----
    def last_known(col: str) -> float | None:
        if df_old.empty or col not in df_old.columns:
            return None
        s = pd.to_numeric(df_old[col], errors="coerce").dropna()
        return float(s.iloc[-1]) if len(s) else None

    if bh_vol is None: bh_vol = last_known("blackhole_volume_24h_usd")
    if tj_vol is None: tj_vol = last_known("traderjoe_volume_24h_usd")
    if pg_vol is None: pg_vol = last_known("pangolin_volume_24h_usd")
    if tvl is None:    tvl    = last_known("tvl_usd")

    # ---- Build new row ----
    new_row = {
        "date": today,
        "blackhole_volume_24h_usd": bh_vol,
        "traderjoe_volume_24h_usd": tj_vol,
        "pangolin_volume_24h_usd": pg_vol,
        "tvl_usd": tvl,
    }

    # ---- Merge/append ----
    if df_old.empty:
        df = pd.DataFrame([new_row])
    else:
        last_date_str = str(df_old.iloc[-1].get("date", ""))[:10]
        if last_date_str == today:
            # replace today's row
            df_old.iloc[-1, :] = pd.Series(new_row)
            df = df_old
        else:
            df = pd.concat([df_old, pd.DataFrame([new_row])], ignore_index=True)

    # ---- Compute/update 7d avg ----
    if "blackhole_volume_24h_usd" in df.columns:
        df["volume_7d_avg_usd"] = compute_7d_avg(df["blackhole_volume_24h_usd"])
    else:
        df["volume_7d_avg_usd"] = None

    # ---- Save & show tail ----
    df.to_csv(DATA_CSV, index=False)
    print("💾 Wrote:", DATA_CSV)
    print(df.tail(5).to_string(index=False))

if __name__ == "__main__":
    main()
