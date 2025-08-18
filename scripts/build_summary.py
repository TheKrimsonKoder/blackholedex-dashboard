from pathlib import Path
import pandas as pd
from datetime import datetime, timezone

DATA_CSV = Path("data/black_data.csv")
OUT_TXT  = Path("data/daily_summary.txt")

# --- Alias sets so we work with your CSV no matter the header names ---
ALIASES = {
    "date":     ["date","day","timestamp","ts"],
    "bh_vol":   ["blackhole_volume_24h_usd","volume_24h_usd","volume_usd_24h","volume_usd","volume","vol_24h_usd","vol_24h","black_volume_24h"],
    "tvl":      ["tvl_usd","tvl","tvl_24h_usd"],
    "bh_vol_7d":["volume_7d_avg_usd","vol_7d_avg_usd","vol_7d_avg","volume_7d_avg"],
    "fees":     ["fees_24h_usd","fees_usd_24h","fees_usd","fees","fee_24h_usd","fee_usd_24h"],  # not shown in tweet, but kept if you want later
}

# Competitors we’ll look for (add more if you track more)
COMP_KEYS = {
    "Trader Joe": ["traderjoe_volume_24h_usd","trader_joe_volume_24h_usd","traderjoe_volume_usd","trader_joe_volume_usd","traderjoe_volume","trader_joe_volume"],
    "Pangolin":   ["pangolin_volume_24h_usd","pangolin_volume_usd","pangolin_volume"],
}

def find_col(df, candidates):
    # exact then case-insensitive
    for c in candidates:
        if c in df.columns: return c
    lower = {c.lower(): c for c in df.columns}
    for c in candidates:
        if c.lower() in lower: return lower[c.lower()]
    return None

def usd_commas(x):
    if pd.isna(x): return None
    try:
        return f"${int(round(float(x))):,}"
    except Exception:
        return None

def compact_if_needed(lines, max_len=280, hashtags_len=1+len("#DeFi #Avalanche #DEX")):
    """If tweet is too long, progressively compact amounts to $6.1M/$335K and/or drop the last comparison line."""
    text = "\n".join(lines)
    if len(text) + hashtags_len <= max_len:
        return text

    # 1) compact amounts like $6,082,796 -> $6.1M, $335,342 -> $335K
    import re
    def compact_num(m):
        n = int(m.group(1).replace(",", ""))
        if n >= 1_000_000:
            return f"${n/1_000_000:.1f}M".replace(".0M","M")
        if n >= 1_000:
            return f"${n/1_000:.0f}K"
        return f"${n}"
    text2 = re.sub(r"\$(\d{1,3}(?:,\d{3})+)", lambda m: compact_num(m), text)
    if len(text2) + hashtags_len <= max_len:
        return text2

    # 2) drop the 3rd comparison line (usually Pangolin) if present
    rows = text2.splitlines()
    if "💹 Comparison" in text2:
        comp_start = next((i for i,r in enumerate(rows) if r.startswith("💹 Comparison")), None)
        if comp_start is not None:
            # try dropping last bullet if there are at least 3 bullets
            bullets = [i for i in range(comp_start+1, len(rows)) if rows[i].startswith("• ")]
            if len(bullets) >= 2:
                rows.pop(bullets[-1])
                text3 = "\n".join(rows)
                if len(text3) + hashtags_len <= max_len:
                    return text3

    # 3) final brute trim (should rarely trigger)
    return (text2[: (max_len - hashtags_len)]).rstrip()

def main():
    if not DATA_CSV.exists():
        OUT_TXT.write_text("📊 BlackholeDex Daily Stats\nData not found.\nSources: DexScreener, DeFiLlama", encoding="utf-8")
        return

    df = pd.read_csv(DATA_CSV)
    if df.empty:
        OUT_TXT.write_text("📊 BlackholeDex Daily Stats\nNo data.\nSources: DexScreener, DeFiLlama", encoding="utf-8")
        return

    # Sort by date if available
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

    # 7d avg: use column if present; else compute last 7 rows avg
    if vol7_c:
        vol7 = pd.to_numeric(last.get(vol7_c), errors="coerce")
    else:
        series = pd.to_numeric(df[bh_vol_c], errors="coerce") if bh_vol_c else pd.Series(dtype=float)
        vol7 = series.tail(7).mean() if not series.empty else pd.NA

    # Competitors (read if present; skip missing)
    comps = []
    for name, aliases in COMP_KEYS.items():
        c = find_col(df, aliases)
        if c:
            v = pd.to_numeric(last.get(c), errors="coerce")
            if pd.notna(v):
                comps.append((name, v))

    # Build lines
    date_label = None
    if date_col and pd.notna(last.get(date_col)):
        try:
            date_label = pd.to_datetime(last.get(date_col)).strftime("%Y-%m-%d")
        except Exception:
            pass
    if not date_label:
        date_label = datetime.now(timezone.utc).strftime("%Y-%m-%d")

    lines = [f"📊 BlackholeDex Daily Stats ({date_label})"]

    vol_txt = usd_commas(bh_vol)
    if vol_txt: lines.append(f"🔸 24h Volume: {vol_txt}")

    tvl_txt = usd_commas(tvl)
    if tvl_txt: lines.append(f"🔹 TVL: {tvl_txt}")

    vol7_txt = usd_commas(vol7)
    if vol7_txt: lines.append(f"📈 7-Day Avg (Blackhole): {vol7_txt}")

    # Comparison block (Blackhole first, then competitors we have)
    if vol_txt or comps:
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

    # Fit to 280 chars incl hashtags that will be appended by poster
    tweet_text = compact_if_needed(lines)

    OUT_TXT.parent.mkdir(parents=True, exist_ok=True)
    OUT_TXT.write_text(tweet_text, encoding="utf-8")

if __name__ == "__main__":
    main()
