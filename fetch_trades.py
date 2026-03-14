#!/usr/bin/env python3
"""
Fetch insider trades (Form 4) from SEC EDGAR for a defined portfolio.
Outputs trades.json consumed by index.html.
"""

import requests, json, time, re
from datetime import datetime, timedelta
import xml.etree.ElementTree as ET

TICKERS = [
    "PLTR","NVDA","AAPL","MSFT","META","AMZN","GOOG","AVGO",
    "NOW","UNH","V","MA","MELI","PANW","FTNT","MNST",
    "BLDR","LLY","CELH","CPRT","ASML","AXON","MPWR","APH","TQQQ"
]

HEADERS  = {"User-Agent": "InsiderTradesTracker contact@example.com"}
LOOKBACK = 90   # days of history to pull
SLEEP    = 0.15 # seconds between EDGAR requests (rate-limit courtesy)

# ── helpers ──────────────────────────────────────────────────────────────────

def get_cik_map():
    r = requests.get("https://www.sec.gov/files/company_tickers.json", headers=HEADERS)
    r.raise_for_status()
    return {v["ticker"].upper(): str(v["cik_str"]).zfill(10) for v in r.json().values()}

def get_recent_form4(cik):
    url = f"https://data.sec.gov/submissions/CIK{cik}.json"
    r = requests.get(url, headers=HEADERS); r.raise_for_status()
    data   = r.json()
    recent = data.get("filings", {}).get("recent", {})
    cutoff = (datetime.utcnow() - timedelta(days=LOOKBACK)).strftime("%Y-%m-%d")
    results = []
    forms    = recent.get("form", [])
    accnos   = recent.get("accessionNumber", [])
    dates    = recent.get("filingDate", [])
    primdocs = recent.get("primaryDocument", [])
    for i, f in enumerate(forms):
        if f == "4" and dates[i] >= cutoff:
            results.append({
                "accession":   accnos[i],
                "filing_date": dates[i],
                "primary_doc": primdocs[i] if i < len(primdocs) else None
            })
    return results

def fetch_xml(cik_int, accession, primary_doc):
    acc_nodash = accession.replace("-", "")
    base = f"https://www.sec.gov/Archives/edgar/data/{cik_int}/{acc_nodash}"
    # Try primary doc first, then common fallback names
    candidates = []
    if primary_doc and primary_doc.endswith(".xml"):
        candidates.append(f"{base}/{primary_doc}")
    candidates += [
        f"{base}/{accession}.xml",
        f"{base}/form4.xml",
    ]
    for url in candidates:
        try:
            r = requests.get(url, headers=HEADERS, timeout=10)
            if r.status_code == 200 and r.text.strip().startswith("<?xml"):
                return r.text
        except Exception:
            pass
    # Fall back: scrape index page for any .xml link
    try:
        idx = requests.get(f"{base}/", headers=HEADERS, timeout=10)
        matches = re.findall(r'href="([^"]+\.xml)"', idx.text, re.I)
        for m in matches:
            xml_url = f"https://www.sec.gov{m}" if m.startswith("/") else f"{base}/{m}"
            r = requests.get(xml_url, headers=HEADERS, timeout=10)
            if r.status_code == 200:
                return r.text
    except Exception:
        pass
    return None

def parse_form4(xml_text, ticker, filing_date):
    """Parse Form 4 XML and return list of transaction dicts."""
    try:
        root = ET.fromstring(xml_text)
    except ET.ParseError:
        return []

    def txt(node, path):
        el = node.find(path)
        return el.text.strip() if el is not None and el.text else ""

    issuer_name   = txt(root, "issuer/issuerName")
    issuer_ticker = txt(root, "issuer/issuerTradingSymbol") or ticker

    owner_name  = txt(root, "reportingOwner/reportingOwnerId/rptOwnerName")
    is_dir      = txt(root, "reportingOwner/reportingOwnerRelationship/isDirector") == "1"
    is_off      = txt(root, "reportingOwner/reportingOwnerRelationship/isOfficer")  == "1"
    is_10pct    = txt(root, "reportingOwner/reportingOwnerRelationship/isTenPercentOwner") == "1"
    officer_title = txt(root, "reportingOwner/reportingOwnerRelationship/officerTitle")

    if is_dir and not officer_title:
        role = "Director"
    elif is_off:
        role = officer_title or "Officer"
    elif is_10pct:
        role = "10% Owner"
    else:
        role = "Insider"

    trades = []
    for txn in root.findall(".//nonDerivativeTransaction"):
        security = txt(txn, "securityTitle/value")
        txn_date = txt(txn, "transactionDate/value") or filing_date
        code     = txt(txn, "transactionCoding/transactionCode")
        shares   = txt(txn, "transactionAmounts/transactionShares/value")
        price    = txt(txn, "transactionAmounts/transactionPricePerShare/value")
        acq_disp = txt(txn, "transactionAmounts/transactionAcquiredDisposedCode/value")
        post_shrs= txt(txn, "postTransactionAmounts/sharesOwnedFollowingTransaction/value")

        # Only keep open-market buys (P) and sells (S)
        if code not in ("P", "S"):
            continue

        try: shares_f = float(shares.replace(",",""))
        except: shares_f = 0
        try: price_f  = float(price.replace(",",""))
        except: price_f  = 0
        total = round(shares_f * price_f, 2)

        trades.append({
            "ticker":       issuer_ticker.upper(),
            "company":      issuer_name,
            "insider":      owner_name,
            "role":         role,
            "type":         "BUY" if (code == "P" or acq_disp == "A") else "SELL",
            "shares":       shares_f,
            "price":        price_f,
            "total_value":  total,
            "security":     security,
            "txn_date":     txn_date,
            "filing_date":  filing_date,
            "shares_after": post_shrs,
        })
    return trades

# ── main ─────────────────────────────────────────────────────────────────────

def main():
    print("Fetching CIK map...")
    cik_map = get_cik_map()

    all_trades = []
    for ticker in TICKERS:
        cik = cik_map.get(ticker.upper())
        if not cik:
            print(f"  [SKIP] {ticker}: CIK not found"); continue
        cik_int = int(cik)
        print(f"  [{ticker}] CIK={cik_int}", end="")
        try:
            filings = get_recent_form4(cik)
            print(f" — {len(filings)} Form 4(s)")
            for f in filings:
                time.sleep(SLEEP)
                xml = fetch_xml(cik_int, f["accession"], f["primary_doc"])
                if not xml:
                    continue
                trades = parse_form4(xml, ticker, f["filing_date"])
                all_trades.extend(trades)
        except Exception as e:
            print(f" ERROR: {e}")

    all_trades.sort(key=lambda x: x["filing_date"], reverse=True)
    output = {
        "updated_at": datetime.utcnow().strftime("%Y-%m-%d %H:%M UTC"),
        "trades": all_trades
    }
    with open("trades.json", "w") as fh:
        json.dump(output, fh, indent=2)
    print(f"\nDone. {len(all_trades)} trades written to trades.json")

if __name__ == "__main__":
    main()
