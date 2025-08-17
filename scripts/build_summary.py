from pathlib import Path
import pandas as pd
import re
from datetime import datetime

DATA_CSV = Path("data/black_data.csv")
OUT_TXT  = Path("data/daily_summary.txt")

# --- formatting helpers ---
def usd_compact(x):
    if pd.isna(x): return "—"
    x = float(x)
    if abs(x) >= 1_000_000_000: return f"${x/1_000_000_000:.1f}B".replace(".0B","B")
    if abs(x) >= 1_000_000:     return f"${x/1_000_000:.1f}M".replace(".0M","M")
    if abs(x) >= 1_000:         return f"${x/1_000:.0f}K"
    return f"${x:,.0f}"

def arrow_dd(cur, prev):
    if prev in (None, 0) or pd.isna(prev) or pd.isna(cur): return ""
    chg = (float(cur) - float(prev)) / float(prev) * 100
    return f" ({'▲' if chg>=0 else '▼'}{abs(chg):.1f}%)"

def pretty_date(s):
    try:
        dt = pd.to_datetime(s)
    except Exception:
        dt = pd.Timestamp.utcnow().normalize()
    # e.g., "Aug 14"
    return dt.strftime("%b %-d") if hasattr(dt, "strftime") else str(s)

def clean_name(col: str) -> str:
    # "traderjoe_volume_24h" -> "Trader Joe"
    base = re.sub(r"volume.*", "", col, flags=re.I)
    base = re.sub(r"[_\-]+", " ", base).strip()
    base = base.replace("traderjoe","Trader Joe").replace("pangolin","Pangolin").replace("dexalot","Dexalot")
    return " ".join(w.capitalize() for w in base.split()) if base else col

def pick_competitor(series: pd.Series, main_key="volume_24h_usd"):
    cands = {}
    for col, val in series.items():
        if col == main_key: continue
        if "volume" in col.lower() and pd.notna(val) and float(val) > 0:
            cands[clean_name(col)] = float(val)
    if not cands: return None
    name = max(cands, key=cands.get)
    return name, cands[name]

# --- build summary safely and compactly ---
def main():
    if not DATA_CSV.exists():
        OUT_TXT.write_text("📊 BlackholeDex Daily Pulse\nData not found.\nDashboard & sources: link in bio\nSupport: wallet in bio 🙌", encoding="utf-8")
        return

    df = pd.read_csv(DATA_CSV)
    if "date" in df.columns:
        df["date"] = pd.to_datetime(df["date"], errors="coerce")
        df = df.sort_values("date")

    if len(df) == 0:
        OUT_TXT.write_text("📊 BlackholeDex Daily Pulse\nNo data.\nDashboard & sources: link in bio\nSupport: wallet in bio 🙌", encoding="utf-8")
        return

    t = df.iloc[-1]
    y = df.iloc[-2] if len(df) > 1 else None

    dlabel = pretty_date(t.get("date")) if "date" in df.columns else datetime.utcnow().strftime("%b %-d")

    vol   = float(t.get("volume_24h_usd", float("nan")))
    fees  = float(t.get("fees_24h_usd", float("nan")))
    tvl   = float(t.get("tvl_usd", float("nan")))
    trd   = t.get("traders_24h", float("nan"))

    vol7  = float(t.get("volume_7d_avg_usd", float("nan")))
    fees7 = float(t.get("fees_7d_avg_usd", float("nan")))

    # Lines (no AVAX share anywhere)
    vol_line  = f"• 24h Volume: {usd_compact(vol)}"
    fees_line = f"• 24h Fees: {usd_compact(fees)}"
    tvl_line  = f"• TVL: {usd_compact(tvl)}"
    trd_line  = f"• Traders: {int(trd):,}" if pd.notna(trd) else ""

    if y is not None:
        vol_line  += arrow_dd(vol,  y.get("volume_24h_usd", float("nan")))
        fees_line += arrow_dd(fees, y.get("fees_24h_usd", float("nan")))
        tvl_line  += arrow_dd(tvl,  y.get("tvl_usd", float("nan")))
        if pd.notna(trd) and pd.notna(y.get("traders_24h")) and y["traders_24h"] not in (0, None):
            trd_line += arrow_dd(float(trd), float(y["traders_24h"]))

    if pd.notna(vol7):  vol_line  += f" | 7d avg: {usd_compact(vol7)}"
    if pd.notna(fees7): fees_line += f" | 7d avg: {usd_compact(fees7)}"

    comp_line = ""
    comp = pick_competitor(t)
    if comp:
        name, cvol = comp
        comp_line = f"• Next competitor ({name}): {usd_compact(cvol)}"
        if pd.notna(vol) and cvol > 0:
            comp_line += f" ({vol/cvol:.1f}×)"

    lines = [
        f"📊 BlackholeDex Daily Pulse ({dlabel})",
        vol_line,
        fees_line,
        tvl_line,
    ]
    if trd_line:  lines.append(trd_line)
    if comp_line: lines.append(comp_line)

    # CTA footer
    footer = ["", "Dashboard & sources: link in bio", "Support: wallet in bio 🙌"]
    text = "\n".join([l for l in lines if l.strip()]) + "\n" + "\n".join(footer)

    # Hard cap with intelligent pruning to ensure <= 280 incl. hashtags (~40 chars)
    HASHTAGS = "#DeFi #Avalanche #DEX #BlackholeDex"
    MAX_TWEET = 280
    reserve = len(HASHTAGS) + 1  # newline + hashtags
    limit = MAX_TWEET - reserve

    def fit(txt: str) -> str:
        if len(txt) <= limit: return txt
        # Try dropping lowest-priority lines in order: Traders -> Competitor -> shorten 7d labels
        def drop_line(txt, startswith):
            rows = txt.splitlines()
            keep = [r for r in rows if not r.strip().startswith(startswith)]
            return "\n".join(keep)
        t1 = drop_line(txt, "• Traders")
        if len(t1) <= limit: return t1
        t2 = drop_line(t1, "• Next competitor")
        if len(t2) <= limit: return t2
        # Compress 7d labels
        t3 = t2.replace(" | 7d avg:", " | 7d:")
        if len(t3) <= limit: return t3
        # Final brute trim from the end if still too long
        return t3[:limit].rstrip()

    text = fit(text)

    OUT_TXT.parent.mkdir(parents=True, exist_ok=True)
    OUT_TXT.write_text(text.strip(), encoding="utf-8")

if __name__ == "__main__":
    main()
