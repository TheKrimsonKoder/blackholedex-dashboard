# === scripts/api_fetch.py (DefiLlama DEBUG) ===
import json
import math
from pathlib import Path
from datetime import datetime, timezone

import requests
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = ROOT / "data"
CSV_PATH = DATA_DIR / "black_data.csv"
SUMMARY_PATH = DATA_DIR / "daily_summary.txt"
RAW_JSON_PATH = DATA_DIR / "last_api_response.json"
NAMES_TXT_PATH = DATA_DIR / "protocol_names.txt"

API = "https://api.llama.fi/overview/dexs/chain/avalanche?dataType=volumes"

def ensure_dirs():
    DATA_DIR.mkdir(parents=True, exist_ok=True)

def utc_now():
    return datetime.now(timezone.utc)

def fetch_llama():
    print(f"📡 GET {API}")
    r = requests.get(API, timeout=45)
    print(f"HTTP {r.status_code}")
    r.raise_for_status()
    payload = r.json()
    # Save raw for inspection
    RAW_JSON_PATH.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    print(f"💾 Wrote raw JSON → {RAW_JSON_PATH}")
    return payload

def list_protocol_names(payload):
    protos = payload.get("protocols", []) or []
    names = [str(p.get("name") or "") for p in protos]
    NAMES_TXT_PATH.write_text("\n".join(names), encoding="utf-8")
    print(f"🔎 protocols count: {len(names)}")
    print("📝 First 50 protocol names:")
    for nm in names[:50]:
        print(f" • {nm}")
    return protos, names

def find_blackhole_candidates(protos):
    # Flexible matching just in case DefiLlama uses a slightly different name
    terms = ["blackhole", "black hole", "blackholedex", "blackhole dex", "black hole dex"]
    cands = []
    for p in protos:
        name = (p.get("name") or "")
        lname = name.lower()
        if any(t in lname for t in terms):
            cands.append(p)
    print(f"🎯 Candidate protocols that look like 'Blackhole': {[p.get('name') for p in cands] or 'NONE'}")
    return cands

def build_df_from_candidates(cands):
    rows = []
    for p in cands:
        name = p.get("name") or "Unknown"
        dv = p.get("dailyVolume") or {}
        print(f"📈 '{name}' dailyVolume points: {len(dv)}")
        for day, vol in dv.items():
            rows.append({
                "date": day,
                "dex": name,
                "chain": "Avalanche",
                "volume_usd": float(vol) if vol is not None else None,
            })
    if not rows:
        print("⚠️ No rows built from candidates.")
        return pd.DataFrame(columns=["date","dex","chain","volume_usd"]).sort_values("date")
    return pd.DataFrame(rows).sort_values("date")

def build_chain_totals_df(payload):
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
    print(f"🧮 Chain totals days: {len(df)}")
    return df

def add_share(df_black, df_chain):
    if df_black.empty:
        return pd.DataFrame(columns=["date","dex","chain","volume_usd","chain_volume_usd","share_pct"])
    out = pd.merge(df_black, df_chain, on="date", how="left")
    def share(row):
        v = row.get("volume_usd")
        t = row.get("chain_volume_usd")
        if v is None or t in (None, 0) or (isinstance(t, float) and math.isnan(t)):
            return None
        try:
            return 100.0 * float(v) / float(t)
        except Exception:
            return None
    out["share_pct"] = out.apply(share, axis=1)
    return out[["date","dex","chain","volume_usd","chain_volume_usd","share_pct"]].sort_values("date")

def write_summary(df: pd.DataFrame):
    if df.empty:
        msg = (
            "⚠️ No Blackhole volume rows were produced.\n\n"
            "Tips:\n"
            "• Open data/last_api_response.json and search for 'Blackhole' (case-insensitive).\n"
            "• Check data/protocol_names.txt to see exact protocol names returned by DefiLlama.\n"
            "• If the name differs, we’ll adjust the match terms in the script.\n"
        )
        SUMMARY_PATH.write_text(msg, encoding="utf-8")
        print("📝 Wrote diagnostic summary (empty data).")
        return

    latest = df.dropna(subset=["volume_usd"]).tail(1)
    if latest.empty:
        SUMMARY_PATH.write_text("⚠️ No non-null daily volume found yet.", encoding="utf-8")
        print("📝 Wrote summary: no non-null volume.")
        return

    lr = latest.iloc[0]
    date_label = datetime.strptime(lr["date"], "%Y-%m-%d").strftime("%B %d")
    vol_24h = float(lr["volume_usd"])
    share = lr.get("share_pct")
    share_str = f"{share:.2f}%" if pd.notna(share) else "N/A"

    # 7d share trend if present
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
    print(f"📝 Wrote daily summary → {SUMMARY_PATH}")

def main():
    ensure_dirs()
    try:
        payload = fetch_llama()
    except Exception as e:
        print(f"❌ Fetch failed: {e}")
        pd.DataFrame().to_csv(CSV_PATH, index=False)
        SUMMARY_PATH.write_text("⚠️ API fetch failed. See workflow logs.", encoding="utf-8")
        return

    protos, names = list_protocol_names(payload)
    cands = find_blackhole_candidates(protos)
    df_black = build_df_from_candidates(cands)
    df_chain = build_chain_totals_df(payload)
    final_df = add_share(df_black, df_chain)

    final_df.to_csv(CSV_PATH, index=False)
    print(f"✅ Saved {len(final_df)} rows → {CSV_PATH}")

    write_summary(final_df)

if __name__ == "__main__":
    main()
