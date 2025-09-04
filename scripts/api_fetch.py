# === scripts/api_fetch.py — Visible-page scrape (HTML) for Blackhole + Aerodrome + Uniswap ===
from __future__ import annotations

from pathlib import Path
from datetime import datetime, timezone
from typing import Any, Dict, Optional, Tuple
import os, re, json
import requests
import pandas as pd
from bs4 import BeautifulSoup

# ---------------- Paths ----------------
DATA_DIR = Path("data"); DATA_DIR.mkdir(parents=True, exist_ok=True)

CSV_BLACK = DATA_DIR / "black_metrics.csv"
CSV_AERO  = DATA_DIR / "aerodrome_metrics.csv"
CSV_UNI   = DATA_DIR / "uniswap_metrics.csv"

SUMMARY_PATH = DATA_DIR / "daily_summary.txt"
DEBUG_PATH   = DATA_DIR / "debug_counts.txt"

# ---------------- DeFiLlama public protocol pages ----------------
LLAMA_PROTOCOL_URL = "https://defillama.com/protocol/{slug}"
_HTTP_HEADERS = {
    "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/120.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
}

def today_utc_date() -> str:
    return datetime.now(timezone.utc).date().isoformat()

def utc_hm() -> str:
    return datetime.now(timezone.utc).strftime("%H:%M")

# ---------------- Parsing helpers ----------------
_ABBR = {"K": 1_000, "M": 1_000_000, "B": 1_000_000_000, "T": 1_000_000_000_000}

def parse_money(s: str) -> Optional[float]:
    """
    Convert strings like '$1,234', '1.2M', '$3.4 B' into float USD.
    Returns None if no number is found.
    """
    if not s: return None
    s = s.strip()
    # pick the first number-like token with optional K/M/B/T
    m = re.search(r'(?i)\$?\s*([0-9][0-9,\.]*)\s*([KMBT]?)', s)
    if not m: return None
    num = m.group(1).replace(",", "")
    mult = _ABBR.get(m.group(2).upper(), 1)
    try:
        return float(num) * mult
    except Exception:
        return None

def get_visible_text(slug: str) -> Optional[str]:
    url = LLAMA_PROTOCOL_URL.format(slug=slug)
    try:
        r = requests.get(url, headers=_HTTP_HEADERS, timeout=45)
        r.raise_for_status()
    except Exception as e:
        print(f"[HTML] GET failed for {slug}: {e}")
        return None
    soup = BeautifulSoup(r.text, "html.parser")
    # Get page text; keep newlines so label/value proximity is searchable
    return soup.get_text("\n", strip=True)

def extract_metrics_from_text(txt: str) -> Dict[str, Optional[float]]:
    """
    Heuristic extraction from visible page text. We look for common
    label patterns and grab the nearest money-like number.
    """
    out: Dict[str, Optional[float]] = {"tvl_usd": None, "volume_24h_usd": None, "fees_24h_usd": None}

    # Pre-narrow the text to reduce false positives: split to lines
    lines = [l for l in txt.split("\n") if l.strip()]
    joined = "\n".join(lines)

    # Patterns (robust to spacing/casing)
    # TVL
    m = re.search(r'(?i)\bTVL\b.{0,40}?(\$?[0-9][0-9,\.]*\s*[KMBT]?)', joined)
    if m:
        out["tvl_usd"] = parse_money(m.group(1))

    # Volume 24h / 24-hour volume
    m = re.search(r'(?i)\b(24h\s*volume|volume\s*24h|24-hour\s*volume)\b.{0,40}?(\$?[0-9][0-9,\.]*\s*[KMBT]?)', joined)
    if m:
        out["volume_24h_usd"] = parse_money(m.group(2))
    else:
        # fallback: generic "Volume" near a number (may overmatch, but better than N/A)
        m = re.search(r'(?i)\bvolume\b.{0,40}?(\$?[0-9][0-9,\.]*\s*[KMBT]?)', joined)
        if m:
            out["volume_24h_usd"] = parse_money(m.group(1))

    # Fees 24h (or just "Fees" if 24h not labeled)
    m = re.search(r'(?i)\b(24h\s*fees|fees\s*24h|24-hour\s*fees)\b.{0,40}?(\$?[0-9][0-9,\.]*\s*[KMBT]?)', joined)
    if m:
        out["fees_24h_usd"] = parse_money(m.group(2))
    else:
        m = re.search(r'(?i)\bfees\b.{0,40}?(\$?[0-9][0-9,\.]*\s*[KMBT]?)', joined)
        if m:
            out["fees_24h_usd"] = parse_money(m.group(1))

    return out

def scrape_visible_protocol(slug: str) -> Dict[str, Optional[float]]:
    txt = get_visible_text(slug)
    if not txt:
        return {}
    return extract_metrics_from_text(txt)

# ---------------- CSV helpers ----------------
def upsert_today(csv_path: Path, date_str: str, row: Dict[str, Any]) -> pd.DataFrame:
    cols = [
        "date","volume_24h_usd","tvl_usd","fees_24h_usd",
        "fees_7d_usd","revenue_24h_usd","revenue_7d_usd",
        "bribes_24h_usd","bribes_7d_usd","avg7d_volume_usd"
    ]
    if csv_path.exists(): df = pd.read_csv(csv_path)
    else: df = pd.DataFrame(columns=cols)
    for c in cols:
        if c not in df.columns: df[c] = None

    df = df[df["date"] != date_str]
    df = pd.concat([df, pd.DataFrame([row])], ignore_index=True)

    # 7-day rolling avg on volume (only if volume present)
    df = df.sort_values("date").reset_index(drop=True)
    s = pd.to_numeric(df["volume_24h_usd"], errors="coerce").rolling(window=7, min_periods=1).mean()
    df["avg7d_volume_usd"] = s.values

    df.to_csv(csv_path, index=False)
    return df

def money(v: Optional[float]) -> Optional[str]:
    return f"${v:,.0f}" if isinstance(v, (int, float)) else None

# ---------------- Main ----------------
def main():
    date_str = today_utc_date()

    # Scrape visible metrics for each protocol
    bh = scrape_visible_protocol("blackhole")
    ae = scrape_visible_protocol("aerodrome")
    uni = scrape_visible_protocol("uniswap")

    # Rows (we keep columns consistent; leave unknowns as None)
    row_bh = {
        "date": date_str,
        "volume_24h_usd": bh.get("volume_24h_usd"),
        "tvl_usd": bh.get("tvl_usd"),
        "fees_24h_usd": bh.get("fees_24h_usd"),
        "fees_7d_usd": None, "revenue_24h_usd": None, "revenue_7d_usd": None,
        "bribes_24h_usd": None, "bribes_7d_usd": None, "avg7d_volume_usd": None
    }
    row_ae = {
        "date": date_str,
        "volume_24h_usd": ae.get("volume_24h_usd"),
        "tvl_usd": ae.get("tvl_usd"),
        "fees_24h_usd": ae.get("fees_24h_usd"),
        "fees_7d_usd": None, "revenue_24h_usd": None, "revenue_7d_usd": None,
        "bribes_24h_usd": None, "bribes_7d_usd": None, "avg7d_volume_usd": None
    }
    row_uni = {
        "date": date_str,
        "volume_24h_usd": uni.get("volume_24h_usd"),
        "tvl_usd": uni.get("tvl_usd"),
        "fees_24h_usd": uni.get("fees_24h_usd"),
        "fees_7d_usd": None, "revenue_24h_usd": None, "revenue_7d_usd": None,
        "bribes_24h_usd": None, "bribes_7d_usd": None, "avg7d_volume_usd": None
    }

    # Upsert CSVs
    df_bh = upsert_today(CSV_BLACK, date_str, row_bh)
    df_ae = upsert_today(CSV_AERO,  date_str, row_ae)
    df_uni = upsert_today(CSV_UNI,  date_str, row_uni)

    # Compose concise 3-line tweet (no N/A if we can help it)
    def line_proto(name: str, vol: Optional[float], tvl: Optional[float], fee: Optional[float], show_tvl_fee=True) -> str:
        v = money(vol) or "N/A"
        if show_tvl_fee:
            t = money(tvl) or "N/A"
            f = money(fee) or "N/A"
            return f"{name} — Vol {v}, TVL {t}, Fees {f}"
        return f"{name} — Vol {v}"

    asof = f"{utc_hm()} UTC"
    lines = [
        f"📊 Daily Snapshot ({date_str})",
        "",
        line_proto("Blackhole", row_bh["volume_24h_usd"], row_bh["tvl_usd"], row_bh["fees_24h_usd"], True),
        line_proto("Aerodrome", row_ae["volume_24h_usd"], row_ae["tvl_usd"], row_ae["fees_24h_usd"], True),
        line_proto("Uniswap",   row_uni["volume_24h_usd"], None, None, False),
        "",
        f"⏱️ As of {asof}",
    ]
    SUMMARY_PATH.write_text("\n".join(lines).strip(), encoding="utf-8")

    # Debug snapshot
    DEBUG_PATH.write_text(json.dumps({
        "date": date_str,
        "blackhole": row_bh,
        "aerodrome": row_ae,
        "uniswap": row_uni,
        "csv_black_rows": len(df_bh),
        "csv_aero_rows": len(df_ae),
        "csv_uni_rows": len(df_uni),
        "cwd": os.getcwd(),
        "data_dir": str(DATA_DIR.resolve()),
    }, indent=2), encoding="utf-8")

    # Log + asserts so we never silently fail
    print(f"✅ Wrote CSV: {CSV_BLACK.resolve()}")
    print(f"✅ Wrote CSV: {CSV_AERO.resolve()}")
    print(f"✅ Wrote CSV: {CSV_UNI.resolve()}")
    print(f"✅ Wrote summary: {SUMMARY_PATH.resolve()}")
    for p in (CSV_BLACK, CSV_AERO, CSV_UNI, SUMMARY_PATH):
        if not p.exists(): raise RuntimeError(f"Missing: {p}")

if __name__ == "__main__":
    main()
