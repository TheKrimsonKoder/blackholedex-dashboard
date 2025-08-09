import requests
import pandas as pd
from datetime import datetime, timezone
from pathlib import Path

# Paths
DATA_DIR = Path("data")
DATA_DIR.mkdir(exist_ok=True)
CSV_PATH = DATA_DIR / "black_data.csv"
SUMMARY_PATH = DATA_DIR / "daily_summary.txt"
RAW_PATH = DATA_DIR / "blackhole_raw.json"
DEBUG_PATH = DATA_DIR / "debug_counts.txt"

# API endpoints
DEX_URL = "https://api.llama.fi/summary/dexs/blackhole"
CHAIN_URL = "https://api.llama.fi/summary/dexs/avalanche"

def fetch_json(url):
    r = requests.get(url, timeout=30)
    r.raise_for_status()
    return r.json()

def main():
    # Fetch Blackhole summary
    bh_data = fetch_json(DEX_URL)
    pd.Series(bh_data).to_json(RAW_PATH, indent=2)  # save raw for debugging

    chart = bh_data.get("total24hChart", [])
    chart_len = len(chart)

    # Fetch Avalanche totals
    avax_data = fetch_json(CHAIN_URL)
    avax_chart = avax_data.get("total24hChart", [])

    # Build dataframe
    rows = []
    for ts, vol in chart:
        date_str = datetime.utcfromtimestamp(ts).strftime("%Y-%m-%d")
        # Find matching Avalanche volume for same timestamp
        avax_match = next((v for t, v in avax_chart if t == ts), None)
        share_pct = (vol / avax_match * 100) if avax_match else None
        rows.append({
            "date": date_str,
            "dex": "Blackhole",
            "chain": "Avalanche",
            "volume_usd": vol,
            "chain_volume_usd": avax_match,
            "share_pct": share_pct
        })

    df = pd.DataFrame(rows)
    df.to_csv(CSV_PATH, index=False)

    # Save debug counts
    parsed_rows = len(df)
    DEBUG_PATH.write_text(f"chart_len={chart_len}, parsed_rows={parsed_rows}")

    # Write summary for latest date
    if not df.empty:
        latest = df.iloc[-1]
        date_label = latest['date']
        vol_24h = latest['volume_usd']
        share_str = f"{latest['share_pct']:.2f}%" if latest['share_pct'] else "N/A"
        SUMMARY_PATH.write_text(
            f"📊 BlackholeDex Daily Stats ({date_label})\n\n"
            f"🔸 24h DEX Volume: ${vol_24h:,.0f}\n"
            f"🔸 Avalanche Share: {share_str}\n\n"
            f"Track live → https://defillama.com/dexs/chain/avalanche\n\n"
            f"#Crypto #DEX #BlackholeDex",
            encoding="utf-8"
        )

if __name__ == "__main__":
    main()
