# scripts/build_summary.py
from pathlib import Path
import pandas as pd
from datetime import datetime
from zoneinfo import ZoneInfo

DATA_CSV = Path("data/black_data.csv")
OUT_TXT  = Path("data/daily_summary.txt")

# --- Flexible aliases so header name variations still work ---
ALIASES = {
    "date":     ["date", "day", "timestamp", "ts"],
    "bh_vol":   ["blackhole_volume_24h_usd","volume_24h_usd","volume_usd_24h","volume_usd",
                 "volume","vol_24h_usd","vol_24h","black_volume_24h"],
    "tvl":      ["tvl_usd","tvl","tvl_24h_usd"],
    "bh_vol_7d":["volume_7d_avg_usd","vol_7d_avg_usd","vol_7d_avg","volume_7d_avg"],
    # Kept for future use; not printed in this exact format
    "fees":     ["fees_24h_usd","fees_usd_24h","fees_usd","fees","fee_24h_usd","fee_usd_24h"],
}

# Competitors to show in the comparison block (add more aliases if you track more)
COMP_KEYS = {
    "Trader Joe": ["traderjoe_volume_24h_usd","trader_joe_volume_24h_usd",
                   "traderjoe_volume_usd","trader_joe_volume_usd","traderjoe_volume","trader_joe_volume"],
    "Pangolin":   ["pangolin_volume_24h_usd","pangolin_volume_usd","pangolin_volume"],
}

def find_col(df: pd.DataFrame, candidates: list[str]) -> str | None:
    # exact match
    for c in candidates:
        if c in df.columns:
            return c
    # case-insensitive fallback
    lower = {c.lower(): c for c in df.columns}
    for c in candidates:
        if c.lower() in lower:
            return lower[c.lower()]
    return None

def usd_commas(x) -> str | None:
    """$6,082,796 formatting; returns None if value missing."""
    if pd.isna(x):
        return None
    try:
        return f"${int(round(float(x))):,}"
    except Exception:
        return None

def compact_if_needed(lines: list[str],
                      max_len: int = 280,
                      hashtags_len: int = 1 + len("#DeFi #Avalanche #DEX")) -> str:
    """
    The post script appends hashtags; keep room for them.
    Strategy:
      1) Compact big numbers: $6,082,796 -> $6.1M, $335,342 -> $335K
      2) If still long, drop the last comparison bullet (usually Pangolin)
      3) If STILL long, drop the entire comparison block header + bullets except Blackhole
      4) Final hard trim (rare)
    """
    text = "\n".join(lines)
    if len(text) + hashtags_len <= max_len:
        return text

    # 1) compact numbers
    import re
    def compact_num(m):
        n = int(m.group(1).replace(",", ""))
        if n >= 1_000_000:
            return f"${n/1_000_000:.1f}M".replace(".0M","M")
        if n >= 1_000:
            return f"${n/1_000:.0f}K"
        return f"${n}"
    text2 = re.sub(r"\$(\d{1,3}(?:,\d{3})+)", compact_num, text)
    if len(text2) + hashtags_len <= max_len:
        return text2

    rows = text2.splitlines()

    # 2) drop last comparison bullet (keep Blackhole + first competitor)
    if any(r.startswith("💹 Comparison") for r in rows):
        # find indices of bullet lines after the comparison header
        comp_start = next((i for i,r in enumerate(rows) if r.startswith("💹 Comparison")), None)
        if comp_start is not None:
            bullets = [i for i in range(comp_start+1, len(rows)) if rows[i].startswith("• ")]
            if len(bullets) >= 3:
                rows.pop(bullets[-1])  # drop the 3rd bullet (e.g., Pangolin)
                text3 = "\n".join(rows)
                if len(text3) + hashtags_len <= max_len:
                    return text3

    # 3) keep only the Blackhole bullet in comparison (drop header and others if needed)
    if any(r.startswith("💹 Comparison") for r in rows):
        new_rows = []
        in_comp = False
        kept_blackhole = False
        for r in rows:
            if r.startswith("💹 Comparison"):
                in_comp = True
                continue
            if in_comp:
                if r.startswith("• Blackhole:") and not kept_blackhole:
                    new_rows.append(r)  # keep just this bullet
                    kept_blackhole = True
                elif r.startswith("• "):
                    continue  # drop other bullets
                else:
                    in_comp = False
                    if r.strip():
                        new_rows.append(r)
            else:
                new_rows.append(r)
        text4 = "\n".join(new_rows)
        if len(text4) + hashtags_len <= max_len:
            return text4

    # 4) Final brute trim (very rare)
    return (text2[: (max_len - hashtags_len)]).rstrip()

def main():
    # Force today's date (America/New_York)
    try:
        date_label = datetime.now(ZoneInfo("America/New_York")).strftime("%Y-%m-%d")
    except Exception:
        # Fallback to naive local if zoneinfo not available
        date_label = datetime.now().strftime("%Y-%m-%d")

    if not DATA_CSV.exists():
        OUT_TXT.write_text(
            f"📊 BlackholeDex Daily Stats ({date_label})\nData not found.\n\nSources: DexScreener, DeFiLlama",
            encoding="utf-8"
        )
        return

    df = pd.read_csv(DATA_CSV)
    if df.empty:
        OUT_TXT.write_text(
            f"📊 BlackholeDex Daily Stats ({date_label})\nNo data.\n\nSources: DexScreener, DeFiLlama",
            encoding="utf-8"
        )
        return

    # Sort by CSV date if present (so we read the newest row), but we don't display it
    date_col = find_col(df, ALIASES["date"])
    if date_col:
        df[date_col] = pd.to_datetime(df[date_col], errors="coerce")
        df = df.sort_values(date_col)

    last = df.iloc[-1]

    # Resolve main columns
    bh_vol_c = find_col(df, ALIASES["bh_vol"])
    tvl_c    = find_col(df, ALIASES["tvl"])
    vol7_c   = find_col(df, ALIASES["bh_vol_7d"])

    bh_vol = pd.to_numeric(last.get(bh_vol_c), errors="coerce") if bh_vol_c else pd.NA
    tvl    = pd.to_numeric(last.get(tvl_c), errors="coerce")     if tvl_c    else pd.NA

    # 7-day average: use dedicated column if available; else compute from last 7 rows of bh_vol
    if vol7_c:
        vol7 = pd.to_numeric(last.get(vol7_c), errors="coerce")
    else:
        if bh_vol_c and bh_vol_c in df.columns:
            series = pd.to_numeric(df[bh_vol_c], errors="coerce")
            vol7 = series.tail(7).mean() if not series.empty else pd.NA
        else:
            vol7 = pd.NA

    # Competitors (only those present + numeric)
    comps: list[tuple[str, float]] = []
    for name, aliases in COMP_KEYS.items():
        c = find_col(df, aliases)
        if c:
            v = pd.to_numeric(last.get(c), errors="coerce")
            if pd.notna(v):
                comps.append((name, float(v)))

    # Build the tweet lines
    lines: list[str] = [f"📊 BlackholeDex Daily Stats ({date_label})"]

    vol_txt = usd_commas(bh_vol)
    if vol_txt: lines.append(f"🔸 24h Volume: {vol_txt}")

    tvl_txt = usd_commas(tvl)
    if tvl_txt: lines.append(f"🔹 TVL: {tvl_txt}")

    vol7_txt = usd_commas(vol7)
    if vol7_txt: lines.append(f"📈 7-Day Avg (Blackhole): {vol7_txt}")

    # Comparison block
    have_any_comparison = bool(vol_txt or comps)
    if have_any_comparison:
        lines.append("")  # blank line
        lines.append("💹 Comparison (24h Volume):")
        if vol_txt:
            lines.append(f"• Blackhole: {vol_txt}")
        for name, v in comps:
            vtxt = usd_commas(v)
            if vtxt:
                lines.append(f"• {name}: {vtxt}")

    # Footer
    lines.append("")
    lines.append("Sources: DexScreener, DeFiLlama")

    # Fit under 280 after hashtags are added by the poster
    tweet_text = compact_if_needed(lines)

    OUT_TXT.parent.mkdir(parents=True, exist_ok=True)
    OUT_TXT.write_text(tweet_text, encoding="utf-8")

if __name__ == "__main__":
    main()
