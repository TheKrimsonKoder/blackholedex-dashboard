# === scripts/api_fetch.py (DexScreener volumes + DeFiLlama TVL; 3 DEX compare; 7d avg for Blackhole) ===
from pathlib import Path
from datetime import datetime, timezone
import json
import requests
import pandas as pd
from typing import Tuple, Dict, Any, List

# ------------------------------------------------------------------------------
# Paths
# ------------------------------------------------------------------------------
DATA_DIR = Path("data"); DATA_DIR.mkdir(parents=True, exist_ok=True)
CSV_PATH = DATA_DIR / "black_data.csv"
SUMMARY_PATH = DATA_DIR / "daily_summary.txt"

RAW_DS_PATH = DATA_DIR / "dexscreener_raw.json"
RAW_TVL_PATH = DATA_DIR / "tvl_raw.json"
DEBUG_PATH = DATA_DIR / "debug_counts.txt"

# ------------------------------------------------------------------------------
# Config
# ------------------------------------------------------------------------------
DEXES = [
    # name, DexScreener search query, id/url keywords, DeFiLlama TVL slug
    {"name": "Blackhole",   "query": "blackhole avalanche",   "kw": ["blackhole"],                   "tvl_slug": "blackhole"},
    {"name": "Trader Joe",  "query": "trader joe avalanche",  "kw": ["traderjoe", "trader-joe"],    "tvl_slug": "trader-joe"},
    {"name": "Pangolin",    "query": "pangolin avalanche",    "kw": ["pangolin"],                    "tvl_slug": "pangolin"},
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
    r = requests.get(url, timeout=timeout)
    r.raise_for_status()
    return r.json()

def get_dexscreener_volume(query: string, keywords: List[str]) -> Tuple[float, int, int, Any]:
    """
    Search DexScreener and sum 24h volume for Avalanche pools whose dexId/url
    contains any of the provided keywords.
    Returns: (total24h, matched_pairs, total_pairs_returned, raw_payload)
    """
    url = DS_SEARCH + requests.utils.quote(query)
    payload = fetch_json(url)
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

def get_tvl(slug: str) -> Tuple[float | None, Dict[str, Any]]:
    """
    Return (current_tvl_usd or None, raw_debug).
    Try /tvl/{slug} (may return number or {"tvl":num}), then /protocol/{slug} last point in 'tvl'.
    """
    # first try simple
    try:
        j = fetch_json(TVL_SIMPLE.format(slug=slug))
        if isinstance(j, (int, float)):     # number
            return float(j), {"source": "tvl_simple", "value": j}
        if isinstance(j, dict) and "tvl" in j:
            return float(j["tvl"]), {"source": "tvl_simple_obj", "value": j}
    except Exception as e:
        pass

    # fallback protocol
    try:
        j = fetch_json(TVL_PROTO.format(slug=slug))
        arr = (j or {}).get("tvl") or []
        if isinstance(arr, list) and arr:
            last = arr[-1]
            if isinstance(last, (list, tuple)) and len(last) >= 2 and last[1] is not None:
                return float(last[1]), {"source": "protocol", "last": last}
        return None, {"source": "protocol", "note": "no tvl array or last missing"}
    except Exception as e:
        return None, {"source": "error", "error": str(e)}

def upsert_today_all(csv_path: Path, date_str: str, rows: List[Dict[str, Any]]) -> pd.DataFrame:
    """
    rows: list of dicts with keys: date,dex,chain,volume_usd,tvl_usd
    Keeps history; replaces today's rows.
    """
    if csv_path.exists():
        df = pd.read_csv(csv_path)
    else:
        df = pd.DataFrame(columns=["date", "dex", "chain", "volume_usd", "tvl_usd", "avg7d_volume"])

    # ensure columns exist
    for col in ["date", "dex", "chain", "volume_usd", "tvl_usd", "avg7d_volume"]:
        if col not in df.columns:
            df[col] = None

    # remove today's rows (any dex)
    df = df[df["date"] != date_str]

    # append new
    add = pd.DataFrame(rows)
    df = pd.concat([df, add], ignore_index=True)

    # compute 7d avg for Blackhole only
    df["avg7d_volume"] = df["avg7d_volume"].astype(float, errors="ignore")
    mask = df["dex"] == "Blackhole"
    df_bh = df[mask].copy()
    # sort by date for rolling
    df_bh = df_bh.sort_values("date")
    if not df_bh.empty:
        # rolling 7 incl. today (needs numeric)
        s = pd.to_numeric(df_bh["volume_usd"], errors="coerce").rolling(window=7, min_periods=1).mean()
        df.loc[df_bh.index, "avg7d_volume"] = s.values

    # final order
    df = df.sort_values(["date", "dex"]).reset_index(drop=True)
    df.to_csv(csv_path, index=False)
    return df

# ------------------------------------------------------------------------------
# Main
# ------------------------------------------------------------------------------
def main():
    date_str = today_utc()

    # Pull DexScreener volumes for all three
    per_dex = {}
    raw_out = {}
    total_pairs_map = {}
    matched_map = {}

    for d in DEXES:
        try:
            vol, matched, total_pairs, raw = get_dexscreener_volume(d["query"], d["kw"])
        except Exception as e:
            vol, matched, total_pairs, raw = 0.0, 0, 0, {"error": str(e)}
        per_dex[d["name"]] = vol
        matched_map[d["name"]] = matched
        total_pairs_map[d["name"]] = total_pairs
        raw_out[d["name"]] = raw

    # Save DS raw for transparency
    RAW_DS_PATH.write_text(json.dumps(raw_out, indent=2)[:300000], encoding="utf-8")

    # Pull TVL for all three
    tvl_map = {}
    tvl_raw = {}
    for d in DEXES:
        tvl, rawt = get_tvl(d["tvl_slug"])
        tvl_map[d["name"]] = tvl
        tvl_raw[d["name"]] = rawt
    RAW_TVL_PATH.write_text(json.dumps(tvl_raw, indent=2), encoding="utf-8")

    # Build today's rows (all 3)
    today_rows = []
    for d in DEXES:
        today_rows.append({
            "date": date_str,
            "dex": d["name"],
            "chain": "Avalanche",
            "volume_usd": float(per_dex.get(d["name"], 0.0)),
            "tvl_usd": float(tvl_map.get(d["name"])) if isinstance(tvl_map.get(d["name"]), (int, float)) else None,
            "avg7d_volume": None  # filled for Blackhole inside upsert
        })

    # Upsert into CSV (+ compute 7d avg for Blackhole)
    df = upsert_today_all(CSV_PATH, date_str, today_rows)

    # Build comparison (sorted by 24h volume desc)
    today_df = df[df["date"] == date_str].copy()
    today_df["volume_usd"] = pd.to_numeric(today_df["volume_usd"], errors="coerce").fillna(0.0)
    comp = today_df.sort_values("volume_usd", ascending=False)[["dex", "volume_usd"]].values.tolist()

    # Blackhole highlights (today)
    bh_row = today_df[today_df["dex"] == "Blackhole"].head(1)
    bh_vol = float(bh_row["volume_usd"].values[0]) if not bh_row.empty else 0.0
    bh_tvl = tvl_map.get("Blackhole")
    # 7d avg from CSV (latest Blackhole row)
    bh_hist = df[df["dex"] == "Blackhole"].sort_values("date")
    bh_avg7 = float(bh_hist["avg7d_volume"].tail(1).values[0]) if not bh_hist.empty else None

    # Write daily summary
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
        f"#Crypto #DEX #BlackholeDex",
        encoding="utf-8"
    )

    # Debug counts
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
