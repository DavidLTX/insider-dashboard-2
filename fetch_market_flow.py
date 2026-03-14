#!/usr/bin/env python3
"""
Fetch aggregate US market insider buy/sell flow from SEC EDGAR
quarterly insider transaction datasets.
Writes market_flow.json for the last 90 days.
"""
import requests, json, zipfile, io, csv
from datetime import datetime, timedelta

HEADERS = {"User-Agent": "InsiderTradesTracker contact@example.com"}
LOOKBACK = 90

def quarter_urls():
    """Return download URLs for current and previous quarter's dataset."""
    today = datetime.utcnow()
    urls = []
    # generate last 3 quarters to ensure 90-day coverage
    for offset in range(3):
        d = today - timedelta(days=offset * 91)
        q = (d.month - 1) // 3 + 1
        y = d.year
        urls.append(
            f"https://www.sec.gov/files/structureddata/data/"
            f"insider-transactions-data-sets/{y}q{q}_form345.zip"
        )
    return list(dict.fromkeys(urls))  # deduplicate keeping order

def fetch_and_parse(url):
    """Download zip and parse NONDERIV_TRANS.tsv. Returns list of (date, buy_val, sell_val)."""
    print(f"  Trying: {url}")
    r = requests.get(url, headers=HEADERS, timeout=60)
    if r.status_code != 200:
        print(f"  ⚠ HTTP {r.status_code} — skipping")
        return []
    rows = []
    with zipfile.ZipFile(io.BytesIO(r.content)) as z:
        # find the non-derivative transactions file
        names = z.namelist()
        tsv_name = next((n for n in names if "NONDERIV_TRANS" in n.upper() or "nonderiv_trans" in n.lower()), None)
        if not tsv_name:
            print(f"  ⚠ NONDERIV_TRANS not found in {names}")
            return []
        print(f"  Found: {tsv_name}")
        with z.open(tsv_name) as fh:
            reader = csv.DictReader(io.TextIOWrapper(fh, encoding="utf-8", errors="replace"), delimiter="\t")
            for row in reader:
                try:
                    date  = row.get("TRANS_DATE","").strip()[:10]
                    code  = row.get("TRANS_CODE","").strip().upper()
                    acq   = row.get("TRANS_ACQUIRED_DISP_CD","").strip().upper()
                    shares= float(row.get("TRANS_SHARES","0") or 0)
                    price = float(row.get("TRANS_PRICE_PER_SHARE","0") or 0)
                    value = shares * price
                    if not date or len(date) != 10: continue
                    # open-market buys: code P or acquiredDisp = A with code not in awards/tax
                    if code == "P" or (acq == "A" and code not in ("A","M","X","V","I")):
                        rows.append((date, value, 0.0))
                    elif code == "S" or (acq == "D" and code not in ("F","D","G","W")):
                        rows.append((date, 0.0, value))
                except Exception:
                    continue
    print(f"  Parsed {len(rows):,} transaction rows")
    return rows

def main():
    cutoff = (datetime.utcnow() - timedelta(days=LOOKBACK)).strftime("%Y-%m-%d")
    print(f"Building US market insider flow (last {LOOKBACK} days, cutoff {cutoff})")

    all_rows = []
    for url in quarter_urls():
        rows = fetch_and_parse(url)
        all_rows.extend(rows)
        if all_rows:
            dates_covered = set(r[0] for r in all_rows if r[0] >= cutoff)
            if len(dates_covered) >= 60:
                break  # enough data

    # aggregate by date
    by_date = {}
    for date, buy, sell in all_rows:
        if date < cutoff: continue
        if date not in by_date:
            by_date[date] = {"buy_value": 0.0, "sell_value": 0.0, "buy_count": 0, "sell_count": 0}
        by_date[date]["buy_value"]  += buy
        by_date[date]["sell_value"] += sell
        if buy  > 0: by_date[date]["buy_count"]  += 1
        if sell > 0: by_date[date]["sell_count"] += 1

    series = [{"date": d, **v} for d, v in sorted(by_date.items())]
    with open("market_flow.json", "w") as fh:
        json.dump(series, fh, indent=2)
    total_buy  = sum(x["buy_value"]  for x in series)
    total_sell = sum(x["sell_value"] for x in series)
    print(f"\nDone — {len(series)} days written to market_flow.json")
    print(f"  Total buy:  ${total_buy/1e9:.2f}B")
    print(f"  Total sell: ${total_sell/1e9:.2f}B")

if __name__ == "__main__":
    main()
