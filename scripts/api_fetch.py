# === scripts/api_fetch.py ===
from __future__ import annotations

from pathlib import Path
from datetime import datetime, timezone
from typing import List, Tuple, Dict, Optional, Any

import json
import requests
import pandas as pd

# ------------------------------------------------------------------------------
# Paths
# ------------------------------------------------------------------------------
DATA_DIR = Path("data")
DATA_DIR.mkdir(parents=True, exist_ok=True)

CSV_PATH = DATA_DIR / "black_data.csv"
SUMMARY_PATH = DATA_DIR / "daily_summary.txt"
RAW_DS_PATH = DATA_DIR / "dexscreener_raw.json"
RAW_TVL_PATH = DATA_DIR / "tvl_raw.json"
DEBUG_PATH = DATA_DIR / "debug_counts.txt"

# ------------------------------------------------------------------------------
# Config
# ------------------------------------------------------------------------------
DEXES = [
    # name, DexScreener search query, id/url keywords (lowercase), DeFiLlama TVL slug
    {"name": "Blackhole",   "query": "blackhole avalanche",   "kw": ["blackhole"],                "tvl_slug": "blackhole"},
    {"name": "Trader Joe",  "query": "trader joe avalanche",  "kw": ["traderjoe", "trader-joe"], "tvl_slug": "trader-joe"},
    {"name": "Pangolin",    "query": "pangolin avalanche",    "kw": ["pangolin"],                 "tvl_slug": "pangolin"},
]

DS_SEARCH = "https://api.dexscreener.com/latest/dex/search?q="
TVL_SIMPLE = "https://api.llama.fi/tvl/{slug}"
TVL_PROTO  = "https://api.llama.fi/protocol/{slug}"

# ------------------------------------------------------------------------------
# Helpers
# ------------------------------------------------------------------------------
def today_utc() -> str:
    return datetime.now(timezone.utc).date().isoformat()

def fetch_json(url: str, timeout: int = 45) -> Any:
    """HTTP GET -> JSON; return {} on error (keeps run green)."""
    try:
        r = requests.get(url, timeout=timeout)
        r.raise_for_status()
        return r.json()
    except Exception as e:
        return {"_error": str(e), "_url": url}

def get_dexscreener_volume(query: str, keywords: List[str]) -> Tuple[float, int, int, Any]:
    """
    Search DexScreener and sum 24h volume for Avalanche pools whose dexId/url
    contains any of the provided keywords.
    Returns: (total24h, matched_pairs, total_pairs_returned, raw_payload)
    """
    url = DS_SEARCH + requests.utils.quote(query)
    payload: Any = fetch_json(url)
    pairs = (payload or {}).get("pairs") or []
    total = 0.0
    matched = 0

    for p in pairs:
        chain = (p.get("chainId") or "").lower()
        dexid = (p.get("dexId") or "").lower()
        urlp  = (p.get("url") or "").lower()

        if chain != "avalanche":
            continue

        hit = any(k in dexid for k in keywords) or any(k in urlp for k in keywords)
        if not hit:
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

    return total, matched, len(pairs), payload

def get_tvl(slug: str) -> Tuple[Optional[float], Dict[str, Any]]:
    """
    Return (current_tvl_usd or None, raw_debug).
    Try /tvl/{slug} (may return number or {"tvl":num}), then /protocol/{slug} last point in 'tvl'.
    """
    j = fetch_json(TVL_SIMPLE.format(slug=slug))
    if isinstance(j, (int, float)):
        return float(j), {"source": "tvl_simple", "value": j}
    if isinstance(j, dict) and "tvl" in j and isinstance(j["tvl"], (int, float)):
        return float(j["tvl"]), {"source": "tvl_simple_obj", "value": j}

    j = fetch_json(TVL_PROTO.format(slug=slug))
    arr = (j or {}).get("tvl") or []
    if isinstance(arr, list) and arr:
        last = arr[-1]
        if isinstance(last, (list, tuple)) and len(last) >= 2 and last[1] is not None:
            try:
                return float(last[1]), {"source": "protocol", "last": last}
            except Exception:
                pass
    return None, {"source": "none", "note": "no tvl value found"}

def upsert_today_all(csv_path: Path, date_str: str, rows: List[Dict[str, Any]]) -> pd.DataFrame:
    """
    rows: list of dicts with keys: date,dex,chain,volume_usd,tvl_usd
    Keeps history; replaces today's rows; computes 7d avg for Blackhole only.
    """
    if csv_path.exists():
        df = pd.read_csv(csv_path)
    else:
        df = pd.DataFrame(columns=["date", "dex", "chain", "volume_usd", "tvl_usd", "avg7d_volume"])

    # ensure columns
    for col in ["date", "dex", "chain", "volume_usd", "tvl_usd", "avg7d_volume"]:
        if col not in df.columns:
            df[col] = None

    # drop today's rows and append new
    df = df[df["date"] != date_str]
    add = pd.DataFrame(rows)
    df = pd.concat([df, add], ignore_index=True)

    # 7d avg for Blackhole
    mask = df["dex"] == "Blackhole"
    df_bh = df[mask].copy().sort_values("date")
    if not df_bh.empty:
        s = pd.to_numeric(df_bh["volume_usd"], errors="coerce").rolling(window=7, min_periods=1).mean()
        df.loc[df_bh.index, "avg7d_volume"] = s.values

    df = df.sort_values(["date", "dex"]).reset_index(drop=True)
    df.to_csv(csv_path, index=False)
    return df

# ------------------------------------------------------------------------------
# Main
# ------------------------------------------------------------------------------
def main():
    date_str = today_utc()

    # DexScreener volumes for all three
    per_dex: Dict[str, float] = {}
    matched_map: Dict[str, int] = {}
    total_pairs_map: Dict[str, int] = {}
    raw_out: Dict[str, Any] = {}

    for d in DEXES:
        vol, matched, total_pairs, raw = get_dexscreener_volume(d["query"], d["kw"])
        per_dex[d["name"]] = vol
        matched_map[d["name"]] = matched
        total_pairs_map[d["name"]] = total_pairs
        raw_out[d["name"]] = raw

    RAW_DS_PATH.write_text(json.dumps(raw_out, indent=2)[:300000], encoding="utf-8")

    # TVL for all three (best-effort)
    tvl_map: Dict[str, Optional[float]] = {}
    tvl_raw: Dict[str, Any] = {}
    for d in DEXES:
        tvl, rawt = get_tvl(d["tvl_slug"])
        tvl_map[d["name"]] = tvl
        tvl_raw[d["name"]] = rawt
    RAW_TVL_PATH.write_text(json.dumps(tvl_raw, indent=2), encoding="utf-8")

    # Build today's rows
    today_rows: List[Dict[str, Any]] = []
    for d in DEXES:
        tvl_val = tvl_map.get(d["name"])
        today_rows.append({
            "date": date_str,
            "dex": d["name"],
            "chain": "Avalanche",
            "volume_usd": float(per_dex.get(d["name"], 0.0)),
            "tvl_usd": float(tvl_val) if isinstance(tvl_val, (int, float)) else None,
            "avg7d_volume": None  # computed for Blackhole in upsert
        })

    # Upsert into CSV and compute Blackhole 7d avg
    df = upsert_today_all(CSV_PATH, date_str, today_rows)

    # Prepare comparison (sorted by 24h volume desc)
    today_df = df[df["date"] == date_str].copy()
    today_df["volume_usd"] = pd.to_numeric(today_df["volume_usd"], errors="coerce").fillna(0.0)
    comp = today_df.sort_values("volume_usd", ascending=False)[["dex", "volume_usd"]].values.tolist()

    # Blackhole highlights
    bh_today = today_df[today_df["dex"] == "Blackhole"].head(1)
    bh_vol = float(bh_today["volume_usd"].values[0]) if not bh_today.empty else 0.0
    bh_tvl = tvl_map.get("Blackhole")
    bh_hist = df[df["dex"] == "Blackhole"].sort_values("date")
    bh_avg7 = float(bh_hist["avg7d_volume"].tail(1).values[0]) if not bh_hist.empty and pd.notna(bh_hist["avg7d_volume"].tail(1).values[0]) else None

    # Daily summary
    tvl_line = f"🔹 TVL: ${bh_tvl:,.0f}\n" if isinstance(bh_tvl, (int, float)) else "🔹 TVL: N/A\n"
    avg_line = f"📈 7‑Day Avg (Blackhole): ${bh_avg7:,.0f}\n" if isinstance(bh_avg7, (int, float)) else ""
    comp_lines = "\n".join([f"• {name}: ${vol:,.0f}" for name, vol in comp])

    SUMMARY_PATH.write_text(
        f"📊 BlackholeDex Daily Stats ({date_str})\n\n"
        f"🔸 24h Volume: ${bh_vol:,.0f}\n"
        f"{tvl_line}"
        f"{avg_line}\n"
        f"💹 Comparison (24h Volume):\n{comp_lines}\n\n"
        f"Sources: DexScreener (volume), DeFiLlama (TVL)\n"
        f"#DeFi #Avalanche #DEX #BlackholeDex",
        encoding="utf-8"
    )

    # Debug snapshot
    DEBUG_PATH.write_text(
        json.dumps({
            "date": date_str,
            "matched_pairs": matched_map,
            "pairs_returned": total_pairs_map,
            "volumes_24h": per_dex,
            "tvls": tvl_map,
            "rows_after_upsert": len(df)
        }, indent=2),
        encoding="utf-8"
    )

if __name__ == "__main__":
    main()
