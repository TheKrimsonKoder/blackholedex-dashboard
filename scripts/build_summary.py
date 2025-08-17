# scripts/build_summary.py
from pathlib import Path
import pandas as pd
import re
from datetime import datetime

DATA_CSV = Path("data/black_data.csv")
OUT_TXT  = Path("data/daily_summary.txt")

def usd_compact(x: float) -> str:
    if pd.isna(x):
        return "—"
    x = float(x)
    if abs(x) >= 1_000_000_000:
        return f"${x/1_000_000_000:.1f}B".replace(".0B","B")
    if abs(x) >= 1_000_000:
        return f"${x/1_000_000:.1f}M".replace(".0M","M")
    if abs(x) >= 1_000:
        return f"${x/1_000:.0f}K"
    return f"${x:,.0f}"

def arrow_dd(cur, prev) -> str:
    if prev in (None, 0) or pd.isna(prev) or pd.isna(cur):
        return ""
    chg = (float(cur) - float(prev)) / float(prev) * 100
    arrow = "▲" if chg >= 0 else "▼"
    return f" ({arrow}{abs(chg):.1f}%)"

def pretty_date(s) -> str:
    try:
        dt = pd.to_datetime(s)
    except Exception:
        dt = pd.Timestamp.utcnow().normalize()
    return dt.strftime("%b %-d")

def clean_name(col: str) -> str:
    base = re.sub(r"volume.*", "", col, flags=re.I)
    base = re.sub(r"[_\-]+", " ", base).strip()
    base = base.replace("traderjoe", "Trader Joe").replace("pangolin", "Pangolin")
    return " ".join(w.capitalize() for w in base.split()) if base else col

def pick_competitor(s: pd.Series, main_key: str = "volume_24h_usd"):
    candidates = {}
    for col, val in s.items():
        if col == main_key: continue
        if "volume" in col.lower() and pd.notna(val) and float(val) > 0:
            candidates[clean_name(col)] = float(val)
    if not candidates: return None
    comp_name = max(candidates, key=candidates.get)
    return comp_name, candidates[comp_name]

def main():
    if not DATA_CSV.exists():
        OUT_TXT.write_text("BlackholeDex daily pulse: data not found.", encoding="utf-8")
        return

    df = pd.read_csv(DATA_CSV)
    if "date" in df.columns:
        df["date"] = pd.to_datetime(df["date"], errors="coerce")
        df = df.sort_values("date")

    if len(df) == 0:
        OUT_TXT.write_text("BlackholeDex daily pulse: no data.", encoding="utf-8")
        return

    today = df.iloc[-1]
    yday  = df.iloc[-2] if len(df) > 1 else None

    vol  = float(today.get("volume_24h_usd", float("nan")))
    fees = float(today.get("fees_24h_usd", float("nan")))
    tvl  = float(today.get("tvl_usd", float("nan")))
    trd  = today.get("traders_24h", float("nan"))

    vol7 = float(today.get("volume_7d_avg_usd", float("nan")))
    fees7 = float(today.get("fees_7d_avg_usd", float("nan")))

    dlabel = pretty_date(today.get("date")) if "date" in df.columns else datetime.utcnow().strftime("%b %-d")

    vol_line  = f"• 24h Volume: {usd_compact(vol)}"
    fees_line = f"• 24h Fees: {usd_compact(fees)}"
    tvl_line  = f"• TVL: {usd_compact(tvl)}"
    trd_line  = f"• Traders: {int(trd):,}" if pd.notna(trd) else ""

    if yday is not None:
        vol_line  += arrow_dd(vol, yday.get("volume_24h_usd", float("nan")))
        fees_line += arrow_dd(fees, yday.get("fees_24h_usd", float("nan")))
        tvl_line  += arrow_dd(tvl, yday.get("tvl_usd", float("nan")))
        if pd.notna(trd) and pd.notna(yday.get("traders_24h")):
            trd_line += arrow_dd(float(trd), float(yday["traders_24h"]))

    if pd.notna(vol7):
        vol_line += f" | 7d avg: {usd_compact(vol7)}"
    if pd.notna(fees7):
        fees_line += f" | 7d avg: {usd_compact(fees7)}"

    comp_line = ""
    comp = pick_competitor(today)
    if comp:
        comp_name, comp_vol = comp
        comp_line = f"• Next competitor ({comp_name}): {usd_compact(comp_vol)}"
        if pd.notna(vol) and comp_vol > 0:
            comp_line += f" ({vol/comp_vol:.1f}×)"

    lines = [
        f"📊 BlackholeDex Daily Pulse ({dlabel})",
        vol_line,
        fees_line,
        tvl_line,
    ]
    if trd_line: lines.append(trd_line)
    if comp_line: lines.append(comp_line)

    lines += ["", "Dashboard & sources: link in bio", "Support: wallet in bio 🙌"]

    OUT_TXT.parent.mkdir(parents=True, exist_ok=True)
    OUT_TXT.write_text("\n".join(lines).strip(), encoding="utf-8")

if __name__ == "__main__":
    main()
