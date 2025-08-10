# === scripts/api_fetch.py (final) ===
from pathlib import Path
from datetime import datetime, timezone
import json, math, requests, pandas as pd

DATA_DIR = Path("data"); DATA_DIR.mkdir(parents=True, exist_ok=True)
CSV_PATH = DATA_DIR / "black_data.csv"
SUMMARY_PATH = DATA_DIR / "daily_summary.txt"
RAW_PATH = DATA_DIR / "blackhole_raw.json"
DEBUG_PATH = DATA_DIR / "debug_counts.txt"

BLACKHOLE_URL = "https://api.llama.fi/summary/dexs/blackhole"
AVAX_OVERVIEW_URL = "https://api.llama.fi/overview/dexs/chain/avalanche?dataType=volumes"

def fetch_json(url, timeout=45):
    r = requests.get(url, timeout=timeout)
    r.raise_for_status()
    return r.json()

def ts_to_date(ts):
    t = float(ts)
    if t > 1_000_000_000_000: t /= 1000.0  # ms -> s
    return datetime.utcfromtimestamp(int(t)).strftime("%Y-%m-%d")

def blackhole_df():
    p = fetch_json(BLACKHOLE_URL)
    RAW_PATH.write_text(json.dumps(p)[:200000], encoding="utf-8")
    rows = []
    for item in p.get("total24hChart", []) or []:
        if not isinstance(item, (list, tuple)) or len(item) < 2: continue
        dt = ts_to_date(item[0]); vol = item[1]
        v = float(vol) if vol is not None else None
        rows.append({"date": dt, "dex":"Blackhole","chain":"Avalanche","volume_usd": v})
    return pd.DataFrame(rows).sort_values("date")

def avax_totals_df():
    p = fetch_json(AVAX_OVERVIEW_URL)
    totals = {}
    for proto in p.get("protocols", []) or []:
        for day, vol in (proto.get("dailyVolume") or {}).items():
            if vol is None: continue
            totals[day] = totals.get(day, 0.0) + float(vol)
    return pd.DataFrame([{"date": d, "chain_volume_usd": v} for d,v in totals.items()]).sort_values("date")

def main():
    try:
        bh = blackhole_df()
    except Exception as e:
        pd.DataFrame().to_csv(CSV_PATH, index=False)
        SUMMARY_PATH.write_text(f"⚠️ Fetch Blackhole failed: {e}", encoding="utf-8")
        return

    try:
        avax = avax_totals_df()
        out = pd.merge(bh, avax, on="date", how="left")
        def share(row):
            v, t = row.get("volume_usd"), row.get("chain_volume_usd")
            if v is None or not t or (isinstance(t,float) and math.isnan(t)): return None
            return 100.0 * v / t
        out["share_pct"] = out.apply(share, axis=1)
    except Exception:
        out = bh.copy()
        out["chain_volume_usd"] = None
        out["share_pct"] = None

    out.to_csv(CSV_PATH, index=False)
    DEBUG_PATH.write_text(f"rows={len(out)}", encoding="utf-8")

    if out.empty:
        SUMMARY_PATH.write_text("⚠️ No rows parsed from DefiLlama.", encoding="utf-8"); return
    last = out.dropna(subset=["volume_usd"]).tail(1).iloc[0]
    share_str = f"{last['share_pct']:.2f}%" if last['share_pct'] is not None else "N/A"
    SUMMARY_PATH.write_text(
        f"📊 BlackholeDex Daily Stats ({last['date']})\n\n"
        f"🔸 24h DEX Volume: ${last['volume_usd']:,.0f}\n"
        f"🔸 Avalanche Share: {share_str}\n\n"
        f"Track it live → https://defillama.com/dexs/chain/avalanche\n\n"
        f"#Crypto #DEX #BlackholeDex",
        encoding="utf-8"
    )

if __name__ == "__main__":
    main()
