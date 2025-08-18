from pathlib import Path
import pandas as pd
import re
from datetime import datetime

DATA_CSV = Path("data/black_data.csv")
OUT_TXT  = Path("data/daily_summary.txt")

# ---- Column alias maps (add more if your schema differs) ----
ALIASES = {
    "date": ["date", "day", "timestamp", "ts"],
    "volume": [
        "volume_24h_usd", "volume_usd_24h", "volume_usd", "volume",
        "blackhole_volume_24h_usd", "blackhole_volume_24h", "black_volume_24h",
        "vol_24h_usd", "vol_24h"
    ],
    "fees": [
        "fees_24h_usd", "fees_usd_24h", "fees_usd", "fees", "fee_24h_usd", "fee_usd_24h"
    ],
    "tvl": [
        "tvl_usd", "tvl", "tvl_24h_usd"
    ],
    "traders": [
        "traders_24h", "unique_traders_24h", "unique_traders", "traders"
    ],
    "vol7": [
        "volume_7d_avg_usd", "vol_7d_avg_usd", "vol_7d_avg", "volume_7d_avg"
    ],
    "fees7": [
        "fees_7d_avg_usd", "fee_7d_avg_usd", "fees_7d_avg", "fees_7d"
    ],
}

# Words that likely indicate the Blackhole/primary DEX in column names
BLACKHOLE_HINTS = ["blackhole", "bh", "black"]

def find_col(df, candidates):
    for c in candidates:
        if c in df.columns: return c
    # Try case-insensitive fallback
    lower = {c.lower(): c for c in df.columns}
    for c in candidates:
        if c.lower() in lower: return lower[c.lower()]
    return None

def find_first(df, key):
    return find_col(df, ALIASES[key])

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
        dt = pd.to_datetime(s, errors="coerce")
    except Exception:
        dt = None
    if pd.isna(dt):
        dt = pd.Timestamp.utcnow().normalize()
    return dt.strftime("%b %-d")

def clean_name(col: str) -> str:
    # "traderjoe_volume_24h" -> "Trader Joe"
    base = re.sub(r"(24h|usd|volume|vol|_+)$", "", col, flags=re.I)
    base = re.sub(r"(volume.*|vol.*)", "", base, flags=re.I)
    base = re.sub(r"[_\-]+", " ", base).strip()
    # Friendly replacements
    repl = {
        "traderjoe": "Trader Joe",
        "pangolin": "Pangolin",
        "dexalot": "Dexalot",
        "yak": "Yak",
        "lydia": "Lydia",
    }
    b = base.lower()
    for k,v in repl.items():
        if k in b: return v
    if not base: base = col
    return " ".join(w.capitalize() for w in base.split())

def mark_numeric(s):
    return pd.to_numeric(s, errors="coerce")

def pick_main_volume_col(df):
    # Prefer an alias that explicitly says blackhole/bh/black
    vol_candidates = [c for c in df.columns if "volume" in c.lower() or re.search(r"\bvol\b", c.lower())]
    bh_cols = [c for c in vol_candidates if any(h in c.lower() for h in BLACKHOLE_HINTS)]
    if bh_cols: return bh_cols[0]
    # Otherwise, use the alias list
    alias = find_first(df, "volume")
    if alias: return alias
    # Fallback: if only one "volume" column exists, use it
    if len(vol_candidates) == 1: return vol_candidates[0]
    return None

def pick_competitor(series, main_key):
    cands = {}
    for col, val in series.items():
        cl = col.lower()
        if col == main_key: 
            continue
        if "volume" in cl or re.search(r"\bvol\b", cl):
            if any(h in cl for h in BLACKHOLE_HINTS):
                continue  # skip obvious blackhole columns
            if pd.notna(val) and float(val) > 0:
                cands[clean_name(col)] = float(val)
    if not cands: 
        return None
    # next-closest competitor by volume
    comp_name = max(cands, key=cands.get)
    return comp_name, cands[comp_name]

def main():
    if not DATA_CSV.exists():
        OUT_TXT.write_text(
            "📊 BlackholeDex Daily Pulse\nData not found.\nDashboard & sources: link in bio\nSupport: wallet in bio 🙌",
            encoding="utf-8"
        )
        return

    df = pd.read_csv(DATA_CSV)

    # Date handling
    date_col = find_first(df, "date")
    if date_col:
        df[date_col] = pd.to_datetime(df[date_col], errors="coerce")
        df = df.sort_values(date_col)

    # Resolve primary keys (with aliases & hints)
    vol_key   = pick_main_volume_col(df)
    fees_key  = find_first(df, "fees")
    tvl_key   = find_first(df, "tvl")
    trd_key   = find_first(df, "traders")
    vol7_key  = find_first(df, "vol7")
    fees7_key = find_first(df, "fees7")

    if len(df) == 0:
        OUT_TXT.write_text(
            "📊 BlackholeDex Daily Pulse\nNo data.\nDashboard & sources: link in bio\nSupport: wallet in bio 🙌",
            encoding="utf-8"
        )
        return

    t = df.iloc[-1]
    y = df.iloc[-2] if len(df) > 1 else None

    # Extract values (numeric)
    vol   = mark_numeric(t.get(vol_key))   if vol_key   else pd.NA
    fees  = mark_numeric(t.get(fees_key))  if fees_key  else pd.NA
    tvl   = mark_numeric(t.get(tvl_key))   if tvl_key   else pd.NA
    trd   = mark_numeric(t.get(trd_key))   if trd_key   else pd.NA
    vol7  = mark_numeric(t.get(vol7_key))  if vol7_key  else pd.NA
    fees7 = mark_numeric(t.get(fees7_key)) if fees7_key else pd.NA

    # Lines (no AVAX share, no NaN%)
    dlabel = pretty_date(t.get(date_col)) if date_col else datetime.utcnow().strftime("%b %-d")

    vol_line  = f"• 24h Volume: {usd_compact(vol)}"
    fees_line = f"• 24h Fees: {usd_compact(fees)}"
    tvl_line  = f"• TVL: {usd_compact(tvl)}"
    trd_line  = f"• Traders: {int(trd):,}" if pd.notna(trd) else ""

    if y is not None:
        vol_line  += arrow_dd(vol,  mark_numeric(y.get(vol_key))   if vol_key   else pd.NA)
        fees_line += arrow_dd(fees, mark_numeric(y.get(fees_key))  if fees_key  else pd.NA)
        tvl_line  += arrow_dd(tvl,  mark_numeric(y.get(tvl_key))   if tvl_key   else pd.NA)
        if pd.notna(trd) and trd_key and pd.notna(y.get(trd_key)):
            trd_line += arrow_dd(float(trd), float(mark_numeric(y.get(trd_key))))

    if pd.notna(vol7):  vol_line  += f" | 7d avg: {usd_compact(vol7)}"
    if pd.notna(fees7): fees_line += f" | 7d avg: {usd_compact(fees7)}"

    # Competitor (auto-detect)
    comp = pick_competitor(t, vol_key or "")
    comp_line = ""
    if comp:
        name, cvol = comp
        comp_line = f"• Next competitor ({name}): {usd_compact(cvol)}"
        if pd.notna(vol) and pd.notna(cvol) and float(cvol) > 0:
            comp_line += f" ({float(vol)/float(cvol):.1f}×)"

    # Assemble
    lines = [f"📊 BlackholeDex Daily Pulse ({dlabel})", vol_line, fees_line, tvl_line]
    if trd_line:  lines.append(trd_line)
    if comp_line: lines.append(comp_line)

    footer = ["", "Dashboard & sources: link in bio", "Support: wallet in bio 🙌"]
    text = "\n".join([l for l in lines if l.strip()]) + "\n" + "\n".join(footer)

    # Fit under 280 with hashtags (handled in post script), prune if needed
    HASHTAGS = "#DeFi #Avalanche #DEX #BlackholeDex"
    MAX_TWEET = 280
    reserve = len(HASHTAGS) + 1
    limit = MAX_TWEET - reserve

    def fit(txt: str) -> str:
        if len(txt) <= limit: return txt
        def drop_line(txt, prefix):
            rows = txt.splitlines()
            keep = [r for r in rows if not r.strip().startswith(prefix)]
            return "\n".join(keep)
        t1 = drop_line(txt, "• Traders")
        if len(t1) <= limit: return t1
        t2 = drop_line(t1, "• Next competitor")
        if len(t2) <= limit: return t2
        t3 = t2.replace(" | 7d avg:", " | 7d:")
        if len(t3) <= limit: return t3
        return t3[:limit].rstrip()

    text = fit(text)

    OUT_TXT.parent.mkdir(parents=True, exist_ok=True)
    OUT_TXT.write_text(text.strip(), encoding="utf-8")

if __name__ == "__main__":
    main()
