# === scripts/api_fetch.py (DexScreener chain total for AVAX share) ===
from __future__ import annotations

from pathlib import Path
from datetime import datetime, timezone
from typing import List, Tuple, Dict, Optional, Any

import json, time, requests, pandas as pd

# Paths
DATA_DIR = Path("data"); DATA_DIR.mkdir(parents=True, exist_ok=True)
CSV_PATH = DATA_DIR / "black_data.csv"
SUMMARY_PATH = DATA_DIR / "daily_summary.txt"
RAW_DS_PATH = DATA_DIR / "dexscreener_raw.json"        # per-DEX queries
RAW_TVL_PATH = DATA_DIR / "tvl_raw.json"
RAW_AVAX_DS_PATH = DATA_DIR / "avax_chain_total_raw.json"  # new: DS chain-wide raw
DEBUG_PATH = DATA_DIR / "debug_counts.txt"

# Config (DEX volumes via DexScreener, TVL via DeFiLlama)
DEXES = [
    {"name": "Blackhole",   "query": "blackhole avalanche",   "kw": ["blackhole"],                "tvl_slug": "blackhole"},
    {"name": "Trader Joe",  "query": "trader joe avalanche",  "kw": ["traderjoe", "trader-joe"], "tvl_slug": "trader-joe"},
    {"name": "Pangolin",    "query": "pangolin avalanche",    "kw": ["pangolin"],                 "tvl_slug": "pangolin"},
]
DS_SEARCH = "https://api.dexscreener.com/latest/dex/search?q="
DS_PAIRS_CHAIN = "https://api.dexscreener.com/latest/dex/pairs/avalanche"  # chain-wide pairs
TVL_SIMPLE = "https://api.llama.fi/tvl/{slug}"
TVL_PROTO  = "https://api.llama.fi/protocol/{slug}"

def today_utc() -> str:
    return datetime.now(timezone.utc).date().isoformat()

def safe_get_json(url: str, timeout: int = 45) -> Any:
    try:
        r = requests.get(url, timeout=timeout)
        r.raise_for_status()
        return r.json()
    except Exception as e:
        return {"_error": str(e), "_url": url}

# ---------- DexScreener per‑DEX volume ----------
def get_dexscreener_volume(query: str, keywords: List[str]) -> Tuple[float, int, int, Any]:
    url = DS_SEARCH + requests.utils.quote(query)
    payload: Any = safe_get_json(url)
    pairs = (payload or {}).get("pairs") or []
    total = 0.0; matched = 0
    for p in pairs:
        if (p.get("chainId") or "").lower() != "avalanche":
            continue
        dexid = (p.get("dexId") or "").lower()
        urlp  = (p.get("url") or "").lower()
        if not (any(k in dexid for k in keywords) or any(k in urlp for k in keywords)):
            continue
        vol = p.get("volume", {})
        v = vol.get("h24", p.get("volume24h"))
        try: total += float(v); matched += 1
        except: pass
    return total, matched, len(pairs), payload

# ---------- DexScreener chain‑wide AVAX total ----------
def get_avalanche_total_24h_ds(max_pages: int = 5, delay: float = 0.6) -> Tuple[Optional[float], Dict[str, Any]]:
    """
    Sum h24 volume across all Avalanche pairs from DexScreener.
    The chain endpoint may support paging via '?page=N'. We'll try a few pages.
    """
    grand_total = 0.0
    raw_pages: Dict[str, Any] = {}
    any_page = False

    for page in range(1, max_pages + 1):
        url = DS_PAIRS_CHAIN + (f"?page={page}" if page > 1 else "")
        j = safe_get_json(url)
        raw_pages[str(page)] = j
        pairs = (j or {}).get("pairs") or []
        if not pairs:
            # stop if the page is empty or errored
            break
        any_page = True
        for p in pairs:
            vol = p.get("volume", {})
            v = vol.get("h24", p.get("volume24h"))
            try: grand_total += float(v)
            except: pass
        time.sleep(delay)  # be gentle

    RAW_AVAX_DS_PATH.write_text(json.dumps(raw_pages, indent=2)[:300000], encoding="utf-8")
    if not any_page:
        return None, raw_pages
    return grand_total, raw_pages

# ---------- DeFiLlama TVL (best‑effort) ----------
def get_tvl(slug: str) -> Tuple[Optional[float], Dict[str, Any]]:
    j = safe_get_json(TVL_SIMPLE.format(slug=slug))
    if isinstance(j, (int, float)):    return float(j), {"source": "tvl_simple", "value": j}
    if isinstance(j, dict) and "tvl" in j and isinstance(j["tvl"], (int, float)):
        return float(j["tvl"]), {"source": "tvl_simple_obj", "value": j}
    j = safe_get_json(TVL_PROTO.format(slug=slug))
    arr = (j or {}).get("tvl") or []
    if isinstance(arr, list) and arr:
        last = arr[-1]
        if isinstance(last, (list, tuple)) and len(last) >= 2 and last[1] is not None:
            try: return float(last[1]), {"source": "protocol", "last": last}
            except: pass
    return None, {"source": "none", "note": "no tvl value found"}

# ---------- CSV upsert & 7‑day avg ----------
def upsert_today_all(csv_path: Path, date_str: str, rows: List[Dict[str, Any]], chain_total_24h: Optional[float]) -> pd.DataFrame:
    if csv_path.exists(): df = pd.read_csv(csv_path)
    else: df = pd.DataFrame(columns=["date","dex","chain","volume_usd","tvl_usd","avg7d_volume","chain_total_24h","blackhole_share_pct"])
    for col in ["date","dex","chain","volume_usd","tvl_usd","avg7d_volume","chain_total_24h","blackhole_share_pct"]:
        if col not in df.columns: df[col] = None

    df = df[df["date"] != date_str]
    add = pd.DataFrame(rows)
    df = pd.concat([df, add], ignore_index=True)

    # 7-day avg for Blackhole
    m = df["dex"] == "Blackhole"
    bh = df[m].copy().sort_values("date")
    if not bh.empty:
        roll = pd.to_numeric(bh["volume_usd"], errors="coerce").rolling(window=7, min_periods=1).mean()
        df.loc[bh.index, "avg7d_volume"] = roll.values

    # share for Blackhole (today only)
    if isinstance(chain_total_24h, (int, float)) and chain_total_24h > 0:
        idx = df[(df["date"] == date_str) & (df["dex"] == "Blackhole")].index
        if len(idx):
            df.loc[idx, "chain_total_24h"] = float(chain_total_24h)
            try:
                vol_today = float(df.loc[idx, "volume_usd"].values[0])
                df.loc[idx, "blackhole_share_pct"] = 100.0 * vol_today / float(chain_total_24h)
            except: pass

    df = df.sort_values(["date","dex"]).reset_index(drop=True)
    df.to_csv(csv_path, index=False)
    return df

# ---------- Main ----------
def main():
    date_str = today_utc()

    # per DEX volumes (DexScreener)
    per_dex: Dict[str, float] = {}; matched_map: Dict[str, int] = {}; total_pairs_map: Dict[str, int] = {}; raw_out: Dict[str, Any] = {}
    for d in DEXES:
        vol, matched, total_pairs, raw = get_dexscreener_volume(d["query"], d["kw"])
        per_dex[d["name"]] = vol; matched_map[d["name"]] = matched; total_pairs_map[d["name"]] = total_pairs; raw_out[d["name"]] = raw
    RAW_DS_PATH.write_text(json.dumps(raw_out, indent=2)[:300000], encoding="utf-8")

    # TVL (DeFiLlama)
    tvl_map: Dict[str, Optional[float]] = {}; tvl_raw: Dict[str, Any] = {}
    for d in DEXES:
        tvl, rawt = get_tvl(d["tvl_slug"])
        tvl_map[d["name"]] = tvl; tvl_raw[d["name"]] = rawt
    RAW_TVL_PATH.write_text(json.dumps(tvl_raw, indent=2), encoding="utf-8")

    # AVAX chain total 24h (DexScreener chain-wide)
    avax_total_24h, avax_raw = get_avalanche_total_24h_ds()

    # Build today's rows for CSV
    today_rows = []
    for d in DEXES:
        tvl_val = tvl_map.get(d["name"])
        today_rows.append({
            "date": date_str,
            "dex": d["name"],
            "chain": "Avalanche",
            "volume_usd": float(per_dex.get(d["name"], 0.0)),
            "tvl_usd": float(tvl_val) if isinstance(tvl_val, (int, float)) else None,
            "avg7d_volume": None,
            "chain_total_24h": None,
            "blackhole_share_pct": None
        })

    df = upsert_today_all(CSV_PATH, date_str, today_rows, avax_total_24h)

    # Compose summary
    today_df = df[df["date"] == date_str].copy()
    today_df["volume_usd"] = pd.to_numeric(today_df["volume_usd"], errors="coerce").fillna(0.0)
    comp = today_df.sort_values("volume_usd", ascending=False)[["dex","volume_usd"]].values.tolist()

    bh_today = today_df[today_df["dex"] == "Blackhole"].head(1)
    bh_vol = float(bh_today["volume_usd"].values[0]) if not bh_today.empty else 0.0
    bh_tvl = tvl_map.get("Blackhole")
    bh_hist = df[df["dex"] == "Blackhole"].sort_values("date")
    bh_avg7 = float(bh_hist["avg7d_volume"].tail(1).values[0]) if not bh_hist.empty and pd.notna(bh_hist["avg7d_volume"].tail(1).values[0]) else None
    share_val = bh_today["blackhole_share_pct"].values[0] if not bh_today.empty else None

    tvl_line = f"🔹 TVL: ${bh_tvl:,.0f}\n" if isinstance(bh_tvl, (int, float)) else "🔹 TVL: N/A\n"
    avg_line = f"📈 7‑Day Avg (Blackhole): ${bh_avg7:,.0f}\n" if isinstance(bh_avg7, (int, float)) else ""
    share_line = f"🧮 AVAX DEX share (24h): {share_val:.2f}%\n" if isinstance(share_val, (int, float)) else "🧮 AVAX DEX share (24h): N/A\n"
    comp_lines = "\n".join([f"• {name}: ${vol:,.0f}" for name, vol in comp])

    SUMMARY_PATH.write_text(
        f"📊 BlackholeDex Daily Stats ({date_str})\n\n"
        f"🔸 24h Volume: ${bh_vol:,.0f}\n"
        f"{tvl_line}"
        f"{avg_line}"
        f"{share_line}\n"
        f"💹 Comparison (24h Volume):\n{comp_lines}\n\n"
        f"Sources: DexScreener (volume & AVAX total), DeFiLlama (TVL)\n"
        f"#DeFi #Avalanche #DEX #BlackholeDex",
        encoding="utf-8"
    )

    # Debug snapshot
    DEBUG_PATH.write_text(json.dumps({
        "date": date_str,
        "volumes_24h": per_dex,
        "matched_pairs": matched_map,
        "pairs_returned": total_pairs_map,
        "avax_total_24h": avax_total_24h,
        "tvls": tvl_map,
        "rows_after_upsert": len(df)
    }, indent=2), encoding="utf-8")

if __name__ == "__main__":
    main()
