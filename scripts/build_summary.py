from pathlib import Path
import pandas as pd
from datetime import datetime

DATA_CSV = Path("data/black_data.csv")
OUT_TXT  = Path("data/daily_summary.txt")

ALIASES = {
    "date":     ["date","day","timestamp","ts"],
    "volume":   ["volume_24h_usd","volume_usd_24h","volume_usd","volume","vol_24h_usd","vol_24h","blackhole_volume_24h"],
    "fees":     ["fees_24h_usd","fees_usd_24h","fees_usd","fees","fee_24h_usd","fee_usd_24h"],
    "tvl":      ["tvl_usd","tvl","tvl_24h_usd"],
    "traders":  ["traders_24h","unique_traders_24h","unique_traders","traders"],
    "vol7":     ["volume_7d_avg_usd","vol_7d_avg_usd","vol_7d_avg","volume_7d_avg"],
    "fees7":    ["fees_7d_avg_usd","fees_7d_avg","fee_7d_avg_usd"],
}

def find_col(df, keys):
    for k in keys:
        if k in df.columns: return k
    lc = {c.lower(): c for c in df.columns}
    for k in keys:
        if k.lower() in lc: return lc[k.lower()]
    return None

def usd_compact(x):
    if pd.isna(x): return "—"
    x=float(x)
    if abs(x)>=1_000_000_000: return f"${x/1_000_000_000:.1f}B".replace(".0B","B")
    if abs(x)>=1_000_000:     return f"${x/1_000_000:.1f}M".replace(".0M","M")
    if abs(x)>=1_000:         return f"${x/1_000:.0f}K"
    return f"${x:,.0f}"

def dd_arrow(cur, prev):
    if pd.isna(cur) or pd.isna(prev) or prev==0: return ""
    chg=(float(cur)-float(prev))/float(prev)*100
    return f" ({'▲' if chg>=0 else '▼'}{abs(chg):.1f}%)"

def main():
    if not DATA_CSV.exists():
        OUT_TXT.write_text("📊 BlackholeDex Daily Pulse\nData not found.\nDashboard & sources: link in bio\nSupport: wallet in bio 🙌", encoding="utf-8")
        return

    df = pd.read_csv(DATA_CSV)
    if len(df)==0:
        OUT_TXT.write_text("📊 BlackholeDex Daily Pulse\nNo data.\nDashboard & sources: link in bio\nSupport: wallet in bio 🙌", encoding="utf-8")
        return

    date_col = find_col(df, ALIASES["date"])
    if date_col:
        df[date_col]=pd.to_datetime(df[date_col], errors="coerce")
        df=df.sort_values(date_col)

    t=df.iloc[-1]
    y=df.iloc[-2] if len(df)>1 else None

    vol_c  = find_col(df, ALIASES["volume"])
    fees_c = find_col(df, ALIASES["fees"])
    tvl_c  = find_col(df, ALIASES["tvl"])
    trd_c  = find_col(df, ALIASES["traders"])
    vol7_c = find_col(df, ALIASES["vol7"])
    fees7_c= find_col(df, ALIASES["fees7"])

    vol  = pd.to_numeric(t.get(vol_c), errors="coerce")   if vol_c  else pd.NA
    fees = pd.to_numeric(t.get(fees_c), errors="coerce")  if fees_c else pd.NA
    tvl  = pd.to_numeric(t.get(tvl_c), errors="coerce")   if tvl_c  else pd.NA
    trd  = pd.to_numeric(t.get(trd_c), errors="coerce")   if trd_c  else pd.NA
    vol7 = pd.to_numeric(t.get(vol7_c), errors="coerce")  if vol7_c else pd.NA
    fees7= pd.to_numeric(t.get(fees7_c), errors="coerce") if fees7_c else pd.NA

    dlabel = t.get(date_col)
    try:
        dlabel = pd.to_datetime(dlabel).strftime("%b %-d") if date_col else datetime.utcnow().strftime("%b %-d")
    except Exception:
        dlabel = datetime.utcnow().strftime("%b %-d")

    vol_line  = f"• 24h Volume: {usd_compact(vol)}"
    fees_line = f"• 24h Fees: {usd_compact(fees)}"
    tvl_line  = f"• TVL: {usd_compact(tvl)}"
    trd_line  = f"• Traders: {int(trd):,}" if pd.notna(trd) else ""

    if y is not None:
        if vol_c:  vol_line  += dd_arrow(vol,  pd.to_numeric(y.get(vol_c),  errors="coerce"))
        if fees_c: fees_line += dd_arrow(fees, pd.to_numeric(y.get(fees_c), errors="coerce"))
        if tvl_c:  tvl_line  += dd_arrow(tvl,  pd.to_numeric(y.get(tvl_c),  errors="coerce"))
        if trd_c and pd.notna(trd):
            prev_trd = pd.to_numeric(y.get(trd_c), errors="coerce")
            if pd.notna(prev_trd) and prev_trd!=0:
                trd_line += dd_arrow(float(trd), float(prev_trd))

    if pd.notna(vol7):  vol_line  += f" | 7d avg: {usd_compact(vol7)}"
    if pd.notna(fees7): fees_line += f" | 7d avg: {usd_compact(fees7)}"

    lines = [f"📊 BlackholeDex Daily Pulse ({dlabel})", vol_line, fees_line, tvl_line]
    if trd_line: lines.append(trd_line)
    lines += ["", "Dashboard & sources: link in bio", "Support: wallet in bio 🙌"]

    OUT_TXT.parent.mkdir(parents=True, exist_ok=True)
    OUT_TXT.write_text("\n".join(lines).strip(), encoding="utf-8")

if __name__ == "__main__":
    main()
