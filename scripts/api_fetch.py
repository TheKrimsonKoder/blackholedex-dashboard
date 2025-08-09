# === scripts/api_fetch.py (DefiLlama slug: blackhole, robust timestamps) ===
from pathlib import Path
from datetime import datetime, timezone
import math, json, requests, pandas as pd

ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = ROOT / "data"
CSV_PATH = DATA_DIR / "black_data.csv"
SUMMARY_PATH = DATA_DIR / "daily_summary.txt"

DEX_SUMMARY = "https://api.llama.fi/summary/dexs/blackhole"  # AMM+CLMM combined
AVL_OVERVIEW = "https://api.llama.fi/overview/dexs/chain/avalanche?dataType=volumes"

def ensure_dirs(): DATA_DIR.mkdir(parents=True, exist_ok=True)

def get_json(url: str, timeout=45):
    r = requests.get(url, timeout=timeout)
    r.raise_for_status()
    return r.json()

def to_date_utc(ts):
    """Accept seconds OR milliseconds; return YYYY-MM-DD (UTC)."""
    try:
        t = float(ts)
        # if it's clearly ms, convert
        if t > 1_000_000_000_000:  # > ~2001 in ms
            t = t / 1000.0
        dt = datetime.fromtimestamp(int(t), tz=timezone.utc).date().isoformat()
        return dt
    except Exception:
        return None

def fetch_blackhole_series():
    payload = get_json(DEX_SUMMARY)
    (DATA_DIR / "blackhole_raw.json").write_text(json.dumps(payload)[:200000], encoding="utf-8")

    chart = payload.get("total24hChart") or []  # list of [ts, value]
    rows = []
    for item in chart:
        if not isinstance(item, (list, tuple)) or len(item) < 2:
            continue
        ts, vol = item[0], item[1]
        dt = to_date_utc(ts)
        try:
            v = float(vol) if vol is not None else None
        except Exception:
            v = None
        if dt:
            rows.append({"date": dt, "volume_usd": v})

    # Debug counts (helps future troubleshooting)
    (DATA_DIR / "debug_counts.txt").write_text(
        f"chart_len={len(chart)}, parsed_rows={len(rows)}", encoding="utf-8"
    )

    df = pd.DataFrame(rows).sort_values("date")
    df["dex"] = "Blackhole"
    df["chain"] = "Avalanche"
    return df[["date", "dex", "chain", "volume_usd"]]

def fetch_avalanche_totals():
    payload = get_json(AVL_OVERVIEW)
    protos = payload.get("protocols", []) or []
    totals = {}
    for p in protos:
        for day, vol in (p.get("dailyVolume") or {}).items():
            if vol is None: continue
            try: totals[day] = totals.get(day, 0.0) + float(vol)
            except: pass
    out = pd.DataFrame([{"date": d, "chain_volume_usd": v} for d, v in totals.items()])
    return out.sort_values("date")

def merge_and_share(black_df, chain_df):
    if black_df.empty:
        return pd.DataFrame(columns=["date","dex","chain","volume_usd","chain_volume_usd","share_pct"])
    df = pd.merge(black_df, chain_df, on="date", how="left")
    def calc_share(row):
        v, t = row.get("volume_usd"), row.get("chain_volume_usd")
        if v is None or t in (None, 0) or (isinstance(t, float) and math.isnan(t)): return None
        try: return 100.0 * float(v) / float(t)
        except: return None
    SUMMARY_PATH.write_text(
        f"📊 BlackholeDex Daily Stats ({date_label})\n\n"
        f"🔸 24h DEX Volume: ${vol_24h:,.0f}\n"
        f"🔸 Avalanche Share: {share_str}{trend_line}\n\n"
        f"Track it live → https://defillama.com/dexs/chain/avalanche\n\n"
        f"#Crypto #DEX #BlackholeDex",
        encoding="utf-8")
