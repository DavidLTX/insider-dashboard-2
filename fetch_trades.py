#!/usr/bin/env python3
"""
Fetch insider trades (Forms 3/4/5) from SEC EDGAR for a defined portfolio.
Writes trades.json consumed by index.html.
"""

import requests, json, re, time
from datetime import datetime, timedelta
import xml.etree.ElementTree as ET

TICKERS = [
    "PLTR","NVDA","AAPL","MSFT","META","AMZN","GOOG","AVGO",
    "NOW","UNH","V","MA","MELI","PANW","FTNT","MNST",
    "LLY","ASML","AXON","MPWR","APH"
]

HEADERS      = {"User-Agent": "InsiderTradesTracker contact@example.com"}
LOOKBACK     = 90
SLEEP        = 0.2
FETCH_FORMS  = {"3", "4", "5"}

TXN_META = {
    "P": ("BUY",  "Open Market Buy",       "open_market"),
    "S": ("SELL", "Open Market Sell",       "open_market"),
    "M": ("BUY",  "Option Exercise",        "derivative"),
    "X": ("BUY",  "Option Exercise (ITM)",  "derivative"),
    "A": ("BUY",  "Award / Grant",          "award"),
    "F": ("SELL", "Tax Withholding",        "tax"),
    "D": ("SELL", "Disposed to Issuer",     "other"),
    "G": ("SELL", "Gift",                   "other"),
    "J": (None,   "Other",                  "other"),
    "W": ("SELL", "Inheritance / Will",     "other"),
    "V": (None,   "10b5-1 Plan",            "other"),
    "I": ("BUY",  "Discretionary Txn",     "other"),
}

# ── helpers ───────────────────────────────────────────────────────────────────

def get_cik_map():
    r = requests.get("https://www.sec.gov/files/company_tickers.json", headers=HEADERS)
    r.raise_for_status()
    return {v["ticker"].upper(): str(v["cik_str"]).zfill(10) for v in r.json().values()}

def get_recent_filings(cik):
    r = requests.get(f"https://data.sec.gov/submissions/CIK{cik}.json", headers=HEADERS)
    r.raise_for_status()
    data   = r.json()
    recent = data.get("filings", {}).get("recent", {})
    cutoff = (datetime.utcnow() - timedelta(days=LOOKBACK)).strftime("%Y-%m-%d")
    results = []
    forms    = recent.get("form", [])
    accnos   = recent.get("accessionNumber", [])
    dates    = recent.get("filingDate", [])
    primdocs = recent.get("primaryDocument", [])
    for i, form in enumerate(forms):
        if form in FETCH_FORMS and dates[i] >= cutoff:
            results.append({
                "form":        form,
                "accession":   accnos[i],
                "filing_date": dates[i],
                "primary_doc": primdocs[i] if i < len(primdocs) else "",
            })
    return results

def fetch_xml(cik_int, accession, primary_doc):
    """Return ownershipDocument XML text or None."""
    acc_nodash = accession.replace("-", "")
    base = f"https://www.sec.gov/Archives/edgar/data/{cik_int}/{acc_nodash}"

    candidates = []

    # 1. Primary doc is .xml → try first
    if primary_doc and primary_doc.lower().endswith(".xml"):
        candidates.append(f"{base}/{primary_doc}")

    # 2. Scrape filing directory for .xml files (skipping stylesheets)
    try:
        idx = requests.get(f"{base}/", headers=HEADERS, timeout=10)
        if idx.status_code == 200:
            for path in re.findall(r'href="(/Archives/edgar/data/[^"]+\.xml)"', idx.text, re.I):
                fname = path.split("/")[-1].lower()
                if not any(x in fname for x in ("xsl", ".xsd", "viewer")):
                    candidates.append(f"https://www.sec.gov{path}")
    except Exception:
        pass

    # 3. Common fallback names
    candidates += [
        f"{base}/{accession}.xml",
        f"{base}/form4.xml",
        f"{base}/form3.xml",
        f"{base}/form5.xml",
    ]

    for url in candidates:
        try:
            r = requests.get(url, headers=HEADERS, timeout=12)
            if r.status_code == 200 and "<ownershipDocument" in r.text:
                return r.text
        except Exception:
            pass
    return None

# ── parser ────────────────────────────────────────────────────────────────────

def _txt(node, path):
    el = node.find(path)
    return el.text.strip() if el is not None and el.text else ""

def _float(s):
    try:    return float(str(s).replace(",", ""))
    except: return 0.0

def _parse_owner(root):
    name  = _txt(root, "reportingOwner/reportingOwnerId/rptOwnerName")
    is_dir = _txt(root, "reportingOwner/reportingOwnerRelationship/isDirector") == "1"
    is_off = _txt(root, "reportingOwner/reportingOwnerRelationship/isOfficer")  == "1"
    is_10  = _txt(root, "reportingOwner/reportingOwnerRelationship/isTenPercentOwner") == "1"
    title  = _txt(root, "reportingOwner/reportingOwnerRelationship/officerTitle")
    if is_off and title:  role = title
    elif is_dir:          role = "Director"
    elif is_off:          role = "Officer"
    elif is_10:           role = "10% Owner"
    else:                 role = "Insider"
    return name, role

def _make_trade(ticker, company, owner, role, filing_date,
                security, txn_date, code, acq_disp,
                shares, price, post_shares, is_deriv):
    meta      = TXN_META.get(code, (None, code, "other"))
    direction = meta[0] or ("BUY" if acq_disp == "A" else "SELL")
    shares_f  = _float(shares)
    price_f   = _float(price)
    return {
        "ticker":        ticker,
        "company":       company,
        "insider":       owner,
        "role":          role,
        "type":          direction,
        "txn_code":      code,
        "txn_label":     meta[1],
        "txn_category":  meta[2],
        "shares":        shares_f,
        "price":         price_f,
        "total_value":   round(shares_f * price_f, 2),
        "security":      security,
        "is_derivative": is_deriv,
        "txn_date":      txn_date or filing_date,
        "filing_date":   filing_date,
        "shares_after":  post_shares,
    }

def parse_ownership_doc(xml_text, ticker, filing_date):
    try:
        root = ET.fromstring(xml_text)
    except ET.ParseError:
        return []

    company = _txt(root, "issuer/issuerName")
    sym     = (_txt(root, "issuer/issuerTradingSymbol") or ticker).upper()
    owner, role = _parse_owner(root)
    trades = []

    for txn in root.findall(".//nonDerivativeTransaction"):
        code = _txt(txn, "transactionCoding/transactionCode")
        if not code: continue
        trades.append(_make_trade(
            sym, company, owner, role, filing_date,
            security    = _txt(txn, "securityTitle/value"),
            txn_date    = _txt(txn, "transactionDate/value"),
            code        = code,
            acq_disp    = _txt(txn, "transactionAmounts/transactionAcquiredDisposedCode/value"),
            shares      = _txt(txn, "transactionAmounts/transactionShares/value"),
            price       = _txt(txn, "transactionAmounts/transactionPricePerShare/value"),
            post_shares = _txt(txn, "postTransactionAmounts/sharesOwnedFollowingTransaction/value"),
            is_deriv    = False,
        ))

    for txn in root.findall(".//derivativeTransaction"):
        code = _txt(txn, "transactionCoding/transactionCode")
        if not code: continue
        shares = (_txt(txn, "transactionAmounts/transactionShares/value")
               or _txt(txn, "underlyingSecurity/underlyingSecurityShares/value"))
        price  = (_txt(txn, "transactionAmounts/transactionPricePerShare/value")
               or _txt(txn, "conversionOrExercisePrice/value"))
        trades.append(_make_trade(
            sym, company, owner, role, filing_date,
            security    = _txt(txn, "securityTitle/value"),
            txn_date    = _txt(txn, "transactionDate/value"),
            code        = code,
            acq_disp    = _txt(txn, "transactionAmounts/transactionAcquiredDisposedCode/value"),
            shares      = shares,
            price       = price,
            post_shares = _txt(txn, "postTransactionAmounts/sharesOwnedFollowingTransaction/value"),
            is_deriv    = True,
        ))
    return trades

# ── main ──────────────────────────────────────────────────────────────────────

def main():
    print("Fetching CIK map...")
    cik_map = get_cik_map()
    all_trades = []

    for ticker in TICKERS:
        cik = cik_map.get(ticker.upper())
        if not cik:
            print(f"  [SKIP] {ticker}: CIK not found"); continue
        cik_int = int(cik)
        print(f"  [{ticker}] CIK={cik_int}", end="", flush=True)
        try:
            filings = get_recent_filings(cik)
            print(f" — {len(filings)} filing(s)")
            for f in filings:
                time.sleep(SLEEP)
                xml = fetch_xml(cik_int, f["accession"], f["primary_doc"])
                if not xml:
                    print(f"    ⚠ no XML: {f['accession']}")
                    continue
                trades = parse_ownership_doc(xml, ticker, f["filing_date"])
                all_trades.extend(trades)
        except Exception as e:
            print(f" ERROR: {e}")

    all_trades.sort(key=lambda x: x["txn_date"], reverse=True)
    out = {
        "updated_at": datetime.utcnow().strftime("%Y-%m-%d %H:%M UTC"),
        "trades": all_trades,
    }
    with open("trades.json", "w") as fh:
        json.dump(out, fh, indent=2)
    print(f"\nDone — {len(all_trades)} trades written to trades.json")

if __name__ == "__main__":
    main()
