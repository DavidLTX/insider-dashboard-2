#!/usr/bin/env python3
"""
Fetch US market insider buy/sell flow from SEC quarterly Form 3/4/5 datasets.
SEC publishes data with a ~1 quarter lag — we use the last 2 completed quarters.
Confirmed URL: https://www.sec.gov/files/structureddata/data/
               insider-transactions-data-sets/{year}q{quarter}_form345.zip
Writes market_flow.json with daily buy/sell for last 90 days.
"""
import requests, json, zipfile, io, csv
from datetime import datetime, timedelta
from collections import defaultdict

HEADERS  = {"User-Agent": "InsiderTradesTracker contact@example.com"}
BASE_URL = "https://www.sec.gov/files/structureddata/data/insider-transactions-data-sets"
LOOKBACK = 90

def completed_quarters(n=2):
    """Return the last n COMPLETED quarters (never the current in-progress quarter)."""
    today = datetime.utcnow()
    current_q = (today.month - 1) // 3 + 1
    current_y = today.year
    # step back one quarter to get the most recent completed one
    q, y = current_q - 1, current_y
    if q == 0:
        q, y = 4, y - 1
    pairs = []
    for _ in range(n):
        pairs.append((y, q))
        q -= 1
        if q == 0:
            q, y = 4, y - 1
    return pairs  # most recent first

def fetch_zip(year, qtr):
    url = f"{BASE_URL}/{year}q{qtr}_form345.zip"
    print(f"  Downloading {year} Q{qtr}: {url}", flush=True)
    try:
        r = requests.get(url, headers=HEADERS, timeout=180, stream=True)
        if r.status_code != 200:
            print(f"  HTTP {r.status_code} — skipping"); return None
        data = b"".join(r.iter_content(chunk_size=65536))
        print(f"  Downloaded {len(data)/1e6:.1f} MB")
        return data
    except Exception as e:
        print(f"  Error: {e}"); return None

def parse_nonderiv(zip_bytes):
    rows = []
    with zipfile.ZipFile(io.BytesIO(zip_bytes)) as z:
        names = z.namelist()
        print(f"  ZIP contents: {names}")
        tsv = next((n for n in names if "nonderiv_trans" in n.lower()), None)
        if not tsv:
            print("  NONDERIV_TRANS not found in archive"); return rows
        print(f"  Parsing: {tsv}")
        with z.open(tsv) as fh:
            reader = csv.DictReader(
                io.TextIOWrapper(fh, encoding="utf-8", errors="replace"),
                delimiter="\t"
            )
            for row in reader:
                try:
                    _raw = (row.get("TRANS_DATE","") or row.get("transactionDate","")).strip()
                    # Normalise to YYYY-MM-DD regardless of source format
                    if len(_raw) >= 10:
                        _s = _raw[:10]
                        if _s[2] == "/" and _s[5] == "/":  # MM/DD/YYYY
                            _p = _s.split("/")
                            _s = f"{_p[2]}-{_p[0]}-{_p[1]}"
                        date = _s
                    else:
                        date = ""
                    code = (row.get("TRANS_CODE","") or row.get("transactionCode","")).strip().upper()
                    acq  = (row.get("TRANS_ACQUIRED_DISP_CD","") or row.get("acquiredDisposedCode","")).strip().upper()
                    sh   = float((row.get("TRANS_SHARES","") or row.get("transactionShares","") or "0").replace(",","") or 0)
                    px   = float((row.get("TRANS_PRICE_PER_SHARE","") or row.get("transactionPricePerShare","") or "0").replace(",","") or 0)
                    val  = sh * px
                    if not date or len(date) != 10: continue
                    if code == "P" or (acq == "A" and code not in ("A","M","X","V","I","F","D","G","W")):
                        rows.append({"date": date, "buy": val, "sell": 0})
                    elif code == "S" or (acq == "D" and code not in ("F","D","G","W","A","M","X")):
                        rows.append({"date": date, "buy": 0, "sell": val})
                except: continue
    print(f"  {len(rows):,} rows parsed")
    return rows

def main():
    cutoff = (datetime.utcnow() - timedelta(days=LOOKBACK)).strftime("%Y-%m-%d")
    quarters = completed_quarters(n=2)
    print(f"Using last 2 completed quarters: {quarters}")
    print(f"Cutoff date: {cutoff}")

    all_rows = []
    for year, qtr in quarters:
        raw = fetch_zip(year, qtr)
        if raw:
            all_rows.extend(parse_nonderiv(raw))

    by_date = defaultdict(lambda: {"buy_value":0.0,"sell_value":0.0,"buy_count":0,"sell_count":0})
    for row in all_rows:
        if row["date"] < cutoff: continue
        d = by_date[row["date"]]
        d["buy_value"]  += row["buy"]
        d["sell_value"] += row["sell"]
        if row["buy"]  > 0: d["buy_count"]  += 1
        if row["sell"] > 0: d["sell_count"] += 1

    series = [{"date": dt, **v} for dt, v in sorted(by_date.items())]
    with open("market_flow.json","w") as fh:
        json.dump(series, fh, indent=2)

    total_buy  = sum(x["buy_value"]  for x in series)
    total_sell = sum(x["sell_value"] for x in series)
    print(f"\nDone — {len(series)} days written to market_flow.json")
    print(f"  Total buy:  ${total_buy/1e9:.2f}B")
    print(f"  Total sell: ${total_sell/1e9:.2f}B")

if __name__ == "__main__":
    main()
