# BlackholeDex Daily 📊
**Your one-stop Avalanche C-Chain dashboard for daily volume, top movers, and new token listings — fully automated.**  

![Daily Dex Update](https://github.com/TheKrimsonKoder/blackholedex-dashboard/actions/workflows/daily_fetch.yml/badge.svg)
![Last Updated](https://img.shields.io/github/last-commit/TheKrimsonKoder/blackholedex-dashboard?label=Last%20Update&color=green)

---

## 📌 Overview
BlackholeDex Daily is an **automated crypto data pipeline** that fetches the latest stats for all trading pairs on **BlackholeDex (Avalanche C-Chain)** and updates daily at **04:00 UTC**.  

You’ll get:
- **Total daily volume** 💰
- **Top performing token** 🚀
- **New listings in the last 24h** 🆕
- **Full pair data** including price, liquidity, and FDV

---

## 📊 Live Data
- **CSV**: [black_data.csv](data/black_data.csv)  
- **Daily Tweet**: [@metherferee](https://x.com/metherferee)  

Example of a daily summary tweet:  

---

## ⚙️ How It Works
```mermaid
graph LR
A[DexScreener API] --> B[api_fetch.py]
B --> C[black_data.csv]
B --> D[daily_summary.txt]
C --> E[GitHub Actions Daily Update]
D --> F[Twitter Bot / Zapier Post]

git clone https://github.com/TheKrimsonKoder/blackholedex-dashboard.git
cd blackholedex-dashboard

pip install pandas requests

python scripts/api_fetch.py
---

