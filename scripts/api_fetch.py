# === scripts/api_fetch.py (DefiLlama + share %) ===
import math
from pathlib import Path
from datetime import datetime, timezone

import requests
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = ROOT / "data"
CSV_PATH = DATA_DIR / "black_data.csv"
SUMMARY_PATH = DATA_DIR / "daily_summary.txt"

# DefiLlama open API: DEX overview for Avalanche (volumes)
API = "https://api.llama.fi/overview/dexs/chain/avalanche?dataType=volumes"

def ensure_dirs():
    DATA_DIR.mkdir(parents=True, exist_ok=True)

def utc_now():
    return datetime.now(timezone.utc)

def fetch_llama():
    r = requests.get(API, timeout=30)
    r.raise_for_status()
    return r.json()

def build_blackhole_df(payload):
    """Return dataframe of Blackhole daily volumes."""
    protos = payload.get("protocols", []) or []
    rows = []
    for p in protos:
        if (p.get("name") or "").lower() != "blackhole":
            continue
        dv = p.get("dailyVolume") or {}
        for day, vol in dv.items():
            rows.append({"date": day, "volume_usd": float(vol) if vol is not None else None})
    df = pd.DataFrame(rows).sort_values("date")
    return df

def build_chain_totals_df(payload):
    """Sum daily volume of all protocols on Avalanche to get chain totals."""
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
    rows = [{"date": d, "chain_volume_usd": v} for d, v in totals.items()]
    df = pd.DataFrame(rows).sort_values("date")
    return df

def finalize_dataframe(black_df, chain_df):
    """Merge and compute share %."""
    if black_df.empty and chain_df.empty:
        return pd.DataFrame(columns=["date","dex","chain","volume_usd","chain_volume_usd","share_pct"])
    df = pd.merge(black_df, chain_df, on="date", how="outer", validate="one_to_one")
    df["dex"] = "Blackhole"
    df["chain"] = "Avalanche"
    # Compute share
    def calc_share(row):
        v = row.get("volume_usd")
        t = row.get("chain_volume_usd")
        if v is None or t is None or t == 0 or (isinstance(t, float) and math.isnan(t)):
            return None
        try:
            return 100.0 * float(v) / float(t)
        except Exception:
            return None
    df["share_pct"] = df.apply(calc_share, axis=1)
    # Sort by date and tidy types
    df = df.sort_values("date").reset_index(drop=True)
    return df[["date","dex","chain","volume_usd","chain_volume_usd","share_pct"]]

def write_summary(df: pd.DataFrame):
    if df.empty:
        SUMMARY_PATH.write_text("⚠️ No Blackhole volume data available today.", encoding="utf-8")
        return

    # Latest available day with volume
    latest = df.dropna(subset=["volume_usd"]).tail(1)
    if latest.empty:
        SUMMARY_PATH.write_text("⚠️ No Blackhole volume data available today.", encoding="utf-8")
        return

    latest_row = latest.iloc[0]
    date_label = datetime.strptime(latest_row["date"], "%Y-%m-%d").strftime("%B %d")
    vol_24h = float(latest_row["volume_usd"])
    share = latest_row["share_pct"]
    share_str = f"{share:.2f}%" if pd.notna(share) else "N/A"

    # 7-day share trend (average share of last 7 vs previous 7)
    df_share = df.dropna(subset=["share_pct"]).copy()
    trend_line = ""
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
    print(f"📝 Wrote daily summary → {SUMMARY_PATH}")

def main():
    ensure_dirs()
    try:
        payload = fetch_llama()
    except Exception as e:
        print(f"❌ Fetch failed: {e}")
        pd.DataFrame().to_csv(CSV_PATH, index=False)
        SUMMARY_PATH.write_text("⚠️ No Blackhole volume data available today.", encoding="utf-8")
        return

    black_df = build_blackhole_df(payload)
    chain_df = build_chain_totals_df(payload)
    final_df = finalize_dataframe(black_df, chain_df)

    final_df.to_csv(CSV_PATH, index=False)
    print(f"✅ Saved {len(final_df)} rows → {CSV_PATH}")

    write_summary(final_df)

if __name__ == "__main__":
    main()
