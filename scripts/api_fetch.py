# === scripts/api_fetch.py (robust parser + pretty JSON) ===
from pathlib import Path
from datetime import datetime, timezone
import json, math, requests, pandas as pd

# Paths
DATA_DIR = Path("data"); DATA_DIR.mkdir(parents=True, exist_ok=True)
CSV_PATH = DATA_DIR / "black_data.csv"
SUMMARY_PATH = DATA_DIR / "daily_summary.txt"
RAW_PATH = DATA_DIR / "blackhole_raw.json"
DEBUG_PATH = DATA_DIR / "debug_counts.txt"

# Endpoints
BLACKHOLE_URL = "https://api.llama.fi/summary/dexs/blackhole"  # Blackhole (AMM+CLMM combined)
AVAX_OVERVIEW_URL = "https://api.llama.fi/overview/dexs/chain/avalanche?dataType=volumes"  # Avalanche totals

def fetch_json(url, timeout=45):
    r = requests.get(url, timeout=timeout)
    r.raise_for_status()
    return r.json()

def ts_to_date(ts):
    try:
        t = float(ts)
        if t > 1_000_000_000_000:  # ms -> s
            t /= 1000.0
        return datetime.utcfromtimestamp(int(t)).strftime("%Y-%m-%d")
    except Exception:
        return None

def parse_chart(chart):
    """Accepts list[[ts,val],...] OR dict{ts:val,...}. Returns dict date->float."""
    out = {}
    if isinstance(chart, dict):
        items = chart.items()
    elif isinstance(chart, list):
        items = chart
    else:
        return out
    for item in items:
        if isinstance(item, (list, tuple)) and len(item) >= 2:
            ts, val = item[0], item[1]
        elif isinstance(chart, dict):
            ts, val = item[0], item[1]
        else:
            continue
        d = ts_to_date(ts)
        if not d:
            continue
        try:
            v = float(val) if val is not None else None
        except Exception:
            v = None
        if v is None:
            continue
        out[d] = out.get(d, 0.0) + v
    return out

def blackhole_series():
    """Build Blackhole daily volumes; if parent has no chart, sum childProtocols (AMM+CLMM)."""
    p = fetch_json(BLACKHOLE_URL)

    # Pretty-print raw for easier inspection
    RAW_PATH.write_text(json.dumps(p, indent=2), encoding="utf-8")

    totals = {}

    # 1) Try parent total24hChart first
    parent_chart = p.get("total24hChart")
    if parent_chart:
        totals.update(parse_chart(parent_chart))

    # 2) If empty or partial, merge childProtocols charts
    for child in p.get("childProtocols", []) or []:
        ch_chart = child.get("total24hChart")
        if not ch_chart:
            continue
        child_map = parse_chart(ch_chart)
        for d, v in child_map.items():
            totals[d] = totals.get(d, 0.0) + v

    # Convert to DataFrame
    rows = [{"date": d, "dex": "Blackhole", "chain": "Avalanche", "volume_usd": v}
            for d, v in sorted(totals.items())]
    return pd.DataFrame(rows)

def avalanche_totals():
    """Sum daily volume across all Avalanche DEXs."""
    p = fetch_json(AVAX_OVERVIEW_URL)
    totals = {}
    for proto in p.get("protocols", []) or []:
        for day, vol in (proto.get("dailyVolume") or {}).items():
            if vol is None: 
                continue
            try:
                totals[day] = totals.get(day, 0.0) + float(vol)
            except Exception:
                pass
    rows = [{"date": d, "chain_volume_usd": v} for d, v in sorted(totals.items())]
    return pd.DataFrame(rows)

def main():
    try:
        bh = blackhole_series()
    except Exception as e:
        pd.DataFrame().to_csv(CSV_PATH, index=False)
        SUMMARY_PATH.write_text(f"⚠️ Fetch Blackhole failed: {e}", encoding="utf-8")
        return

    # Always write Blackhole rows, even if Avalanche totals fail
    try:
        avax = avalanche_totals()
        out = pd.merge(bh, avax, on="date", how="left")
        def share(row):
            v, t = row.get("volume_usd"), row.get("chain_volume_usd")
            if v is None or t in (None, 0) or (isinstance(t, float) and math.isnan(t)):
                return None
            return 100.0 * float(v) / float(t)
        out["share_pct"] = out.apply(share, axis=1)
    except Exception:
        out = bh.copy()
        out["chain_volume_usd"] = None
        out["share_pct"] = None

    out.to_csv(CSV_PATH, index=False)

    # Debug summary
    DEBUG_PATH.write_text(
        f"bh_rows={len(bh)}, csv_rows={len(out)}, "
        f"min_date={out['date'].min() if not out.empty else 'NA'}, "
        f"max_date={out['date'].max() if not out.empty else 'NA'}",
        encoding="utf-8"
    )

    # Write human summary
    if out.empty:
        SUMMARY_PATH.write_text("⚠️ No rows parsed from DefiLlama.", encoding="utf-8")
        return
    last = out.dropna(subset=["volume_usd"]).tail(1).iloc[0]
    share_str = f"{last['share_pct']:.2f}%" if pd.notna(last.get('share_pct')) else "N/A"
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
