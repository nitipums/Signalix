#!/usr/bin/env python3
"""
tools/check_data.py — Signalix data inspection & export CLI.

Usage (run from repo root):
  python tools/check_data.py --check           # completeness report
  python tools/check_data.py --anomalies       # list suspect signals
  python tools/check_data.py --export csv      # export all Firestore data to CSV
  python tools/check_data.py --export json     # export all Firestore data to JSON
  python tools/check_data.py --export-history  # full OHLCV history (from IPO) per stock

Requires: GCP credentials in env (GOOGLE_APPLICATION_CREDENTIALS or gcloud auth)
"""

import argparse
import json
import os
import sys
from datetime import datetime

# Allow running from repo root
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pandas as pd
from google.cloud import firestore

from data import SECTOR_MAP, fetch_ohlcv_max, get_stock_list

TODAY = datetime.now().strftime("%Y%m%d")
EXPORT_DIR = f"exports/{TODAY}"


def _get_db() -> firestore.Client:
    return firestore.Client()


# ─── Firestore export helpers ─────────────────────────────────────────────────

def _stream_to_list(collection_ref) -> list[dict]:
    return [{"_id": doc.id, **doc.to_dict()} for doc in collection_ref.stream()]


def export_all(fmt: str) -> None:
    """Export all Firestore collections to CSV or JSON."""
    os.makedirs(EXPORT_DIR, exist_ok=True)
    db = _get_db()

    collections = {
        "users": db.collection("users"),
        "ath_cache": db.collection("ath_cache"),
        "market_breadth": db.collection("market_breadth"),
        "fundamentals_cache": db.collection("fundamentals_cache"),
    }

    for name, ref in collections.items():
        print(f"Exporting {name}...", end=" ", flush=True)
        try:
            rows = _stream_to_list(ref)
            if not rows:
                print(f"empty (0 docs)")
                continue

            if fmt == "csv":
                path = f"{EXPORT_DIR}/{name}_{TODAY}.csv"
                pd.DataFrame(rows).to_csv(path, index=False, encoding="utf-8-sig")
            else:
                path = f"{EXPORT_DIR}/{name}_{TODAY}.json"
                with open(path, "w", encoding="utf-8") as f:
                    json.dump(rows, f, ensure_ascii=False, indent=2, default=str)

            print(f"{len(rows)} docs → {path}")
        except Exception as exc:
            print(f"ERROR: {exc}")

    # Export current in-memory scan signals (latest ath_cache as signals proxy)
    print(f"\nExport complete → {EXPORT_DIR}/")


def export_history() -> None:
    """Export full OHLCV history (from IPO date) for all SET stocks."""
    symbols = get_stock_list()
    ohlcv_dir = f"{EXPORT_DIR}/ohlcv"
    os.makedirs(ohlcv_dir, exist_ok=True)

    summary_rows = []
    total = len(symbols)

    print(f"Fetching full history for {total} stocks (this may take a while)...")
    for i, symbol in enumerate(symbols, 1):
        print(f"  [{i}/{total}] {symbol}", end=" ", flush=True)
        try:
            df = fetch_ohlcv_max(symbol)
            if df is None or df.empty:
                print("no data")
                summary_rows.append({"symbol": symbol, "first_date": None, "last_date": None, "total_rows": 0, "ath": None, "ath_date": None})
                continue

            first_date = str(df.index[0].date())
            last_date = str(df.index[-1].date())
            total_rows = len(df)
            ath_idx = df["High"].idxmax()
            ath = round(float(df["High"].max()), 2)
            ath_date = str(ath_idx.date())

            path = f"{ohlcv_dir}/ohlcv_{symbol}_full.csv"
            df.to_csv(path, encoding="utf-8-sig")
            summary_rows.append({"symbol": symbol, "first_date": first_date, "last_date": last_date, "total_rows": total_rows, "ath": ath, "ath_date": ath_date})
            print(f"ok ({total_rows} rows, from {first_date})")
        except Exception as exc:
            print(f"ERROR: {exc}")
            summary_rows.append({"symbol": symbol, "first_date": None, "last_date": None, "total_rows": 0, "ath": None, "ath_date": None})

    summary_path = f"{EXPORT_DIR}/history_summary_{TODAY}.csv"
    pd.DataFrame(summary_rows).to_csv(summary_path, index=False, encoding="utf-8-sig")
    print(f"\nSummary → {summary_path}")


# ─── Completeness check ───────────────────────────────────────────────────────

def check_completeness() -> None:
    """Print data completeness report."""
    db = _get_db()
    symbols = get_stock_list()
    total = len(symbols)

    print(f"\n=== Signalix Data Completeness Report ({TODAY}) ===\n")

    # ATH cache
    ath_docs = {doc.id: doc.to_dict() for doc in db.collection("ath_cache").stream()}
    covered = [s for s in symbols if s in ath_docs]
    missing = [s for s in symbols if s not in ath_docs]
    print(f"ATH Cache: {len(covered)}/{total} ({len(covered)/total*100:.1f}%)")
    if missing:
        print(f"  Missing ({len(missing)}): {', '.join(missing[:30])}{'...' if len(missing) > 30 else ''}")

    # Users
    users = list(db.collection("users").stream())
    subscribed = sum(1 for u in users if u.to_dict().get("subscribed"))
    print(f"\nUsers: {len(users)} total, {subscribed} subscribed")

    # Market breadth snapshots
    breadth = list(db.collection("market_breadth").limit(1000).stream())
    print(f"Market Breadth Snapshots: {len(breadth)}")

    # Fundamentals cache
    fund = list(db.collection("fundamentals_cache").stream())
    print(f"Fundamentals Cache: {len(fund)} entries")

    print(f"\nSector coverage: {len(set(SECTOR_MAP.values()))} sectors, {len(SECTOR_MAP)} stocks mapped")
    print(f"\nTotal symbols in stock list: {total}")


# ─── Anomaly detection ────────────────────────────────────────────────────────

def check_anomalies() -> None:
    """Check ATH cache and scan data for anomalies."""
    db = _get_db()
    symbols = get_stock_list()

    print(f"\n=== Anomaly Check ({TODAY}) ===\n")

    ath_docs = {doc.id: doc.to_dict().get("ath", 0) for doc in db.collection("ath_cache").stream()}

    bad_ath = [(sym, ath_docs[sym]) for sym in ath_docs if not isinstance(ath_docs[sym], (int, float)) or ath_docs[sym] <= 0]
    if bad_ath:
        print(f"Bad ATH entries ({len(bad_ath)}):")
        for sym, val in bad_ath[:20]:
            print(f"  {sym}: {val}")
    else:
        print("ATH cache: no anomalies found")

    missing = [s for s in symbols if s not in ath_docs]
    print(f"\nSymbols missing from ATH cache: {len(missing)}")
    if missing:
        print(f"  {', '.join(missing[:30])}{'...' if len(missing) > 30 else ''}")


# ─── CLI ──────────────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(description="Signalix data check & export tool")
    parser.add_argument("--check", action="store_true", help="Print completeness report")
    parser.add_argument("--anomalies", action="store_true", help="List anomalies in data")
    parser.add_argument("--export", choices=["csv", "json"], help="Export all Firestore data")
    parser.add_argument("--export-history", action="store_true", dest="export_history", help="Export full OHLCV history from IPO")
    args = parser.parse_args()

    if not any([args.check, args.anomalies, args.export, args.export_history]):
        parser.print_help()
        sys.exit(0)

    if args.check:
        check_completeness()
    if args.anomalies:
        check_anomalies()
    if args.export:
        export_all(args.export)
    if args.export_history:
        export_history()


if __name__ == "__main__":
    main()
