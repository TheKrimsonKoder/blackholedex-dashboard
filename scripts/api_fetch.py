# === scripts/api_fetch.py (DefiLlama slug: blackhole) ===
from pathlib import Path
from datetime import datetime, timezone
import math
import requests
import pandas as pd
import json

ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = ROOT / "data"
CSV_PATH = DATA_DIR / "black_data.csv"
SUMMARY_PATH = DATA_DIR / "daily_summary.txt"

# Endpoints
DEX_SUMMARY = "https://api.llama.fi/summary/dexs/blackhole"  # Blackhole (AMM+CLMM combined)
AVL_OVERVIEW = "https://api.llama.fi/overview/dexs/chain/avalanche?dataType=volumes"  # for chain totals

def ensure_dirs():
    DATA_DIR.mkdir(parents=True, exist_ok=True)

def get_json(url: str, timeout=45):
    r = requests.get(url, timeout=timeout)
    r.raise_for_status()
    return r.json()

def to_date_utc(ts):
    # ts in seconds (DefiLlama), convert to YYYY-MM-DD (UTC)
    try:
        return datetime.fromtimestamp(int(ts), tz=timezone.utc).date().isoformat()
    except Exception:
        return None

def fetch_blackhole_series():
    """Return DataFrame with columns: date, volume_usd (daily), dex, chain."""
    payload = get_json(DEX_SUMMARY)
    # Save raw for debugging if ever needed
    (DATA_DIR / "blackhole_raw.json").write_text(json.dumps(payload)[:200000], encoding="utf-8")

    chart = payload.get("total24hChart") or []  # list of [ts, value]
    rows = []
    for item in chart:
        if not isinstance(item, (list, tuple)) or len(item) < 2:
            continue
        ts, vol = item[0], item[1]
        dt = to_date_utc(ts)
        if dt is None:
            continue
        try:
            v = float(vol) if vol is not None else None
        except Exception:
            v = None
        rows.append({"date": dt, "volume_usd": v})

    df = pd.DataFrame(rows).sort_values("date")
    df["dex"] = "Blackhole"
    df["chain"] = "Avalanche"
    return df[["date", "dex", "chain", "volume_usd"]]

def fetch_avalanche_totals():
    """Sum all Avalanche DEX daily volumes to get chain totals."""
    payload = get_json(AVL_OVERVIEW)
    protos = payload.get("protocols", []) or []
    totals = {}
    for p in protos:
        dv = p.get("dailyVolume") or {}
        for day, vol in dv.items():
            if vol is None:
                continue
            try:
                totals[day] = totals.get(day, 0.0) + float(vol)
            except Exception:
                pass
    out = pd.DataFrame([{"date": d, "chain_volume_usd": v} for d, v in totals.items()])
    return out.sort_values("date")

def merge_and_share(black_df, chain_df):
    if black_df.empty:
        return pd.DataFrame(columns=["date","dex","chain","volume_usd","chain_volume_usd","share_pct"])
    df = pd.merge(black_df, chain_df, on="date", how="left")
    def calc_share(row):
        v, t = row.get("volume_usd"), row.get("chain_volume_usd")
        if v is None or t in (None, 0) or (isinstance(t, float) and math.isnan(t)):
            return None
        try:
            return 100.0 * float(v) / float(t)
        except Exception:
            return None
    df["share_pct"] = df.apply(calc_share, axis=1)
    return df.sort_values("date")[["date","dex","chain","volume_usd","chain_volume_usd","share_pct"]]

def write_summary(df: pd.DataFrame):
    if df.empty:
        SUMMARY_PATH.write_text("⚠️ No Blackhole data available from DefiLlama.", encoding="utf-8")
        return
    latest = df.dropna(subset=["volume_usd"]).tail(1)
    if latest.empty:
        SUMMARY_PATH.write_text("⚠️ Latest Blackhole volume is null.", encoding="utf-8")
        return

    row = latest.iloc[0]
    date_label = datetime.strptime(row["date"], "%Y-%m-%d").strftime("%B %d")
    vol_24h = float(row["volume_usd"])
    share = row.get("share_pct")
    share_str = f"{share:.2f}%" if pd.notna(share) else "N/A"

    # 7d share trend
    trend_line = ""
    df_share = df.dropna(subset=["share_pct"])
    if len(df_share) >= 14:
        last7 = df_share["share_pct"].tail(7).mean()
        prev7 = df_share["share_pct"].tail(14).head(7).mean()
        if pd.notna(last7) and pd.notna(prev7):
            delta = last7 - prev7
            arrow = "🔺" if delta >= 0 else "🔻"
            trend_line = f"\n🔹 7d Share Avg: {last7:.2f}% ({arrow}{abs(delta):.2f} pts vs prior 7d)"
    elif len(df_share) >= 7:
        last7 = df_share["share_pct"].tail(7).mean()
        trend_line = f"\n🔹 7d Share Avg: {last7:.2f}%"

    summary = (
        f"📊 BlackholeDex Daily Stats ({date_label})\n\n"
        f"🔸 24h DEX Volume: ${vol_24h:,.0f}\n"
        f"🔸 Avalanche Share: {share_str}{trend_line}\n\n"
        f"Track it live → https://defillama.com/dexs/chain/avalanche\n\n"
        f"#Crypto #DEX #BlackholeDex"
    )
    SUMMARY_PATH.write_text(summary, encoding="utf-8")

def main():
    ensure_dirs()
    try:
        bh = fetch_blackhole_series()
    except Exception as e:
        # Hard failure: write diagnostics, but keep workflow green
        pd.DataFrame().to_csv(CSV_PATH, index=False)
        SUMMARY_PATH.write_text(f"⚠️ Fetch Blackhole failed: {e}", encoding="utf-8")
        return

    # Chain totals (best effort)
    try:
        avl = fetch_avalanche_totals()
    except Exception:
        avl = pd.DataFrame(columns=["date","chain_volume_usd"])

    final = merge_and_share(bh, avl)
    final.to_csv(CSV_PATH, index=False)
    write_summary(final)

if __name__ == "__main__":
    main()
