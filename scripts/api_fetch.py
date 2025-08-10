# === scripts/api_fetch.py (slug first, overview fallback, pretty JSON) ===
from pathlib import Path
from datetime import datetime, timezone
import json, math, requests, pandas as pd

DATA_DIR = Path("data"); DATA_DIR.mkdir(parents=True, exist_ok=True)
CSV_PATH = DATA_DIR / "black_data.csv"
SUMMARY_PATH = DATA_DIR / "daily_summary.txt"
RAW_PATH = DATA_DIR / "blackhole_raw.json"
DEBUG_PATH = DATA_DIR / "debug_counts.txt"

BLACKHOLE_URL = "https://api.llama.fi/summary/dexs/blackhole"  # may or may not include chart
AVAX_OVERVIEW_URL = "https://api.llama.fi/overview/dexs/chain/avalanche?dataType=volumes"

def fetch_json(url, timeout=45):
    r = requests.get(url, timeout=timeout)
    r.raise_for_status()
    return r.json()

def ts_to_date(ts):
    try:
        t = float(ts)
        if t > 1_000_000_000_000: t /= 1000.0  # ms -> s
        return datetime.utcfromtimestamp(int(t)).strftime("%Y-%m-%d")
    except Exception:
        return None

def parse_chart_to_map(chart):
    """Accept list[[ts,val],...] or dict{ts:val} -> {date: float}"""
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
        if not d or val is None: continue
        try:
            out[d] = out.get(d, 0.0) + float(val)
        except:  # noqa: E722
            pass
    return out

def from_slug_summary():
    """Try the slug endpoint (may include parent + child charts). Return DataFrame or empty."""
    try:
        p = fetch_json(BLACKHOLE_URL)
        RAW_PATH.write_text(json.dumps(p, indent=2), encoding="utf-8")
    except Exception as e:
        RAW_PATH.write_text(json.dumps({"error": str(e)}), encoding="utf-8")
        return pd.DataFrame()

    totals = {}

    # parent chart
    parent = p.get("total24hChart")
    if parent:
        totals.update(parse_chart_to_map(parent))

    # child charts
    for child in p.get("childProtocols", []) or []:
        ch = child.get("total24hChart")
        if not ch: continue
        m = parse_chart_to_map(ch)
        for d, v in m.items():
            totals[d] = totals.get(d, 0.0) + v

    if not totals:
        return pd.DataFrame()

    rows = [{"date": d, "dex": "Blackhole", "chain": "Avalanche", "volume_usd": v}
            for d, v in sorted(totals.items())]
    return pd.DataFrame(rows)

def from_overview_fallback():
    """Fallback: use Avalanche overview and sum all protocols matching 'blackhole'."""
    p = fetch_json(AVAX_OVERVIEW_URL)
    protos = p.get("protocols", []) or []
    totals = {}
    matched = []
    for proto in protos:
        name = (proto.get("name") or "").lower()
        slug = (proto.get("slug") or proto.get("id") or "").lower()
        if "blackhole" in name or "blackhole" in slug:
            matched.append(proto.get("name") or slug or "unknown")
            for day, vol in (proto.get("dailyVolume") or {}).items():
                if vol is None: continue
                try:
                    totals[day] = totals.get(day, 0.0) + float(vol)
                except:  # noqa: E722
                    pass

    # store matched names for transparency
    (DATA_DIR / "matched_protocols.txt").write_text("\n".join(matched) or "NO MATCHES", encoding="utf-8")

    if not totals:
        return pd.DataFrame()

    rows = [{"date": d, "dex": "Blackhole", "chain": "Avalanche", "volume_usd": v}
            for d, v in sorted(totals.items())]
    return pd.DataFrame(rows)

def avalanche_totals_df():
    p = fetch_json(AVAX_OVERVIEW_URL)
    totals = {}
    for proto in p.get("protocols", []) or []:
        for day, vol in (proto.get("dailyVolume") or {}).items():
            if vol is None: continue
            try:
                totals[day] = totals.get(day, 0.0) + float(vol)
            except:  # noqa: E722
                pass
    rows = [{"date": d, "chain_volume_usd": v} for d, v in sorted(totals.items())]
    return pd.DataFrame(rows)

def main():
    # 1) Try slug summary
    bh = from_slug_summary()

    # 2) If empty, fallback to overview-based match
    used = "slug"
    if bh.empty:
        bh = from_overview_fallback()
        used = "overview"

    # 3) Merge chain totals (best-effort)
    try:
        avax = avalanche_totals_df()
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

    # Debug note
    DEBUG_PATH.write_text(
        f"source={used}, bh_rows={len(bh)}, csv_rows={len(out)}, "
        f"min_date={out['date'].min() if not out.empty else 'NA'}, "
        f"max_date={out['date'].max() if not out.empty else 'NA'}",
        encoding="utf-8"
    )

    if out.empty:
        SUMMARY_PATH.write_text("⚠️ No Blackhole rows parsed from DefiLlama.", encoding="utf-8")
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
