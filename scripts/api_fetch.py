# === scripts/api_fetch.py (fail-safe) ===
from pathlib import Path
from datetime import datetime
import json, math, requests, pandas as pd

# Paths
DATA_DIR = Path("data"); DATA_DIR.mkdir(parents=True, exist_ok=True)
CSV_PATH = DATA_DIR / "black_data.csv"
SUMMARY_PATH = DATA_DIR / "daily_summary.txt"
RAW_PATH = DATA_DIR / "blackhole_raw.json"
DEBUG_PATH = DATA_DIR / "debug_counts.txt"
MATCHED_PATH = DATA_DIR / "matched_protocols.txt"

# Endpoints
BLACKHOLE_URL = "https://api.llama.fi/summary/dexs/blackhole"  # AMM+CLMM combined object
AVAX_OVERVIEW_URL = "https://api.llama.fi/overview/dexs/avalanche?excludeTotalDataChart=false&dataType=volumes"

# ---------------- utils ----------------
def fetch_json(url, timeout=45):
    try:
        r = requests.get(url, timeout=timeout)
        r.raise_for_status()
        return r.json()
    except requests.HTTPError as e:
        print(f"⚠️ HTTP error fetching {url}: {e}")
        return {}
    except Exception as e:
        print(f"⚠️ General error fetching {url}: {e}")
        return {}

def ts_to_date(ts):
    try:
        t = float(ts)
        if t > 1_000_000_000_000: t /= 1000.0  # ms -> s
        return datetime.utcfromtimestamp(int(t)).strftime("%Y-%m-%d")
    except Exception:
        return None

def parse_chart_to_map(chart):
    """Accept list[[ts,val],...] or dict{ts:val} -> {YYYY-MM-DD: float}"""
    out = {}
    if isinstance(chart, dict):
        items = chart.items()
    elif isinstance(chart, list):
        items = chart
    else:
        return out
    for itm in items:
        if isinstance(itm, (list, tuple)) and len(itm) >= 2:
            ts, val = itm[0], itm[1]
        elif isinstance(chart, dict):
            ts, val = itm[0], itm[1]
        else:
            continue
        d = ts_to_date(ts)
        if not d or val is None:
            continue
        try:
            out[d] = out.get(d, 0.0) + float(val)
        except:
            pass
    return out

# ---------------- sources ----------------
def from_slug_summary():
    """Try the Blackhole slug endpoint; sum parent + child charts if present."""
    p = fetch_json(BLACKHOLE_URL)
    # Save whatever we got for inspection (pretty)
    try:
        RAW_PATH.write_text(json.dumps(p, indent=2), encoding="utf-8")
    except:
        pass

    totals = {}

    parent = (p or {}).get("total24hChart")
    if parent:
        totals.update(parse_chart_to_map(parent))

    for child in (p or {}).get("childProtocols", []) or []:
        ch = child.get("total24hChart")
        if not ch:
            continue
        m = parse_chart_to_map(ch)
        for d, v in m.items():
            totals[d] = totals.get(d, 0.0) + v

    if not totals:
        return pd.DataFrame()

    rows = [{"date": d, "dex": "Blackhole", "chain": "Avalanche", "volume_usd": v}
            for d, v in sorted(totals.items())]
    return pd.DataFrame(rows)

def from_overview_fallback():
    """Fallback: list all protocol names/slugs from Avalanche overview."""
    p = fetch_json(AVAX_OVERVIEW_URL)
    protos = p.get("protocols", []) or []

    lines = []
    for proto in protos:
        lines.append(f"{proto.get('name')} | {proto.get('slug')} | {proto.get('id')}")
    
    # Save the list of all available protocols in Avalanche overview
    MATCHED_PATH.write_text("\n".join(lines), encoding="utf-8")

    # Return empty DataFrame so downstream steps won't fail
    return pd.DataFrame()

def avalanche_totals_df():
    """Chain-wide Avalanche DEX totals via overview (best-effort)."""
    p = fetch_json(AVAX_OVERVIEW_URL)
    protos = p.get("protocols", []) or []
    totals = {}
    for proto in protos:
        for day, vol in (proto.get("dailyVolume") or {}).items():
            if vol is None: 
                continue
            try:
                totals[day] = totals.get(day, 0.0) + float(vol)
            except:
                pass
    rows = [{"date": d, "chain_volume_usd": v} for d, v in sorted(totals.items())]
    return pd.DataFrame(rows)

# ---------------- main ----------------
def main():
    # 1) Try slug
    bh = from_slug_summary()

    # 2) Fallback if slug yields no rows
    source = "slug"
    if bh.empty:
        bh = from_overview_fallback()
        source = "overview"

    # 3) Merge chain totals (but never fail the run)
    avax = avalanche_totals_df()
    if not bh.empty and not avax.empty:
        out = pd.merge(bh, avax, on="date", how="left")
        def share(row):
            v, t = row.get("volume_usd"), row.get("chain_volume_usd")
            if v is None or t in (None, 0) or (isinstance(t, float) and math.isnan(t)):
                return None
            return 100.0 * float(v) / float(t)
        out["share_pct"] = out.apply(share, axis=1)
    else:
        out = bh.copy()
        out["chain_volume_usd"] = None
        out["share_pct"] = None

    out.to_csv(CSV_PATH, index=False)

    # Debug summary
    DEBUG_PATH.write_text(
        f"source={source}, bh_rows={len(bh)}, csv_rows={len(out)}, "
        f"min_date={out['date'].min() if not out.empty else 'NA'}, "
        f"max_date={out['date'].max() if not out.empty else 'NA'}",
        encoding="utf-8"
    )

    # Human summary
    if out.empty:
        SUMMARY_PATH.write_text("⚠️ No Blackhole rows parsed from DefiLlama.", encoding="utf-8")
        return
    last = out.dropna(subset=["volume_usd"]).tail(1).iloc[0]
    share_str = f"{last['share_pct']:.2f}%" if last.get('share_pct') is not None else "N/A"
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
