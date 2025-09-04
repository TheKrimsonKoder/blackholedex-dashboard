# === scripts/api_fetch.py — Simplified visible scrape for Blackhole + Aerodrome + Uniswap ===
from __future__ import annotations

from pathlib import Path
from datetime import datetime, timezone
from typing import Any, Dict, Optional
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

# ---------------- DeFiLlama protocol pages ----------------
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
    if not s: return None
    s = s.strip()
    m = re.search(r'(?i)\$?\s*([0-9][0-9,\.]*)\s*([KMBT]?)', s)
    if not m: return None
    num = m.group(1).replace(",", "")
    mult = _ABBR.get(m.group(2).upper(), 1)
    try:
        return float(num) * mult
    except Exception:
        return None

# ---------------- Visible scrape ----------------
def scrape_visible_protocol(slug: str) -> Dict[str, Optional[float]]:
    url = LLAMA_PROTOCOL_URL.format(slug=slug)
    try:
        r = requests.get(url, headers=_HTTP_HEADERS, timeout=45)
        r.raise_for_status()
    except Exception as e:
        print(f"[HTML] GET failed for {slug}: {e}")
        return {}

    soup = BeautifulSoup(r.text, "html.parser")

    # Grab the main stats (usually TVL, Volume, Fees) at the top
    stats = soup.select("div[class*=stats] div, div[class*=stats] span")
    numbers = [parse_money(el.get_text()) for el in stats if parse_money(el.get_text())]

    out = {"tvl_usd": None, "volume_24h_usd": None, "fees_24h_usd": None}
    if len(numbers) > 0: out["tvl_usd"] = numbers[0]
    if len(numbers) > 1: out["volume_24h_usd"] = numbers[1]
    if len(numbers) > 2: out["fees_24h_usd"] = numbers[2]
    return out

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

    # 7-day rolling avg on volume
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

    bh = scrape_visible_protocol("blackhole")
    ae = scrape_visible_protocol("aerodrome")
    uni = scrape_visible_protocol("uniswap")

    row_bh = {"date": date_str, "volume_24h_usd": bh.get("volume_24h_usd"),
              "tvl_usd": bh.get("tvl_usd"), "fees_24h_usd": bh.get("fees_24h_usd"),
              "fees_7d_usd": None, "revenue_24h_usd": None, "revenue_7d_usd": None,
              "bribes_24h_usd": None, "bribes_7d_usd": None, "avg7d_volume_usd": None}
    row_ae = {"date": date_str, "volume_24h_usd": ae.get("volume_24h_usd"),
              "tvl_usd": ae.get("tvl_usd"), "fees_24h_usd": ae.get("fees_24h_usd"),
              "fees_7d_usd": None, "revenue_24h_usd": None, "revenue_7d_usd": None,
              "bribes_24h_usd": None, "bribes_7d_usd": None, "avg7d_volume_usd": None}
    row_uni = {"date": date_str, "volume_24h_usd": uni.get("volume_24h_usd"),
               "tvl_usd": uni.get("tvl_usd"), "fees_24h_usd": uni.get("fees_24h_usd"),
               "fees_7d_usd": None, "revenue_24h_usd": None, "revenue_7d_usd": None,
               "bribes_24h_usd": None, "bribes_7d_usd": None, "avg7d_volume_usd": None}

    df_bh = upsert_today(CSV_BLACK, date_str, row_bh)
    df_ae = upsert_today(CSV_AERO,  date_str, row_ae)
    df_uni = upsert_today(CSV_UNI,  date_str, row_uni)

    # Tweet summary (compact 3 lines)
    def line_proto(name, vol, tvl, fee, show_tvl_fee=True):
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
        line_proto("Blackhole", row_bh["volume_24h_usd"], row_bh["tvl_usd"], row_bh["fees_24h_usd"]),
        line_proto("Aerodrome", row_ae["volume_24h_usd"], row_ae["tvl_usd"], row_ae["fees_24h_usd"]),
        line_proto("Uniswap", row_uni["volume_24h_usd"], None, None, False),
        "",
        f"⏱️ As of {asof}",
    ]
    SUMMARY_PATH.write_text("\n".join(lines).strip(), encoding="utf-8")

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

    print(f"✅ Wrote CSV: {CSV_BLACK.resolve()}")
    print(f"✅ Wrote CSV: {CSV_AERO.resolve()}")
    print(f"✅ Wrote CSV: {CSV_UNI.resolve()}")
    print(f"✅ Wrote summary: {SUMMARY_PATH.resolve()}")

if __name__ == "__main__":
    main()
