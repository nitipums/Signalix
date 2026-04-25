#!/usr/bin/env python3
"""Scanner validation — run stage + pattern detection on 7 sample assets
and print a rule-level trace for manual comparison against TradingView.

Pure-local, credential-free (yfinance only). Covers:
  4 SET stocks  : STECON / KKP / DELTA / SCC    (scan_stock path)
  2 indexes     : SET Composite / KOSPI         (analyze_index path)
  1 crypto      : Bitcoin (BTC-USD)             (scan_stock path)

Usage:
    python3 scripts/scanner_validate.py
"""
from __future__ import annotations

import os
import sys

# Make the project root importable when run from anywhere.
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Stub env so config.py imports don't blow up — we only use analyzer + yfinance.
os.environ.setdefault("LINE_CHANNEL_ACCESS_TOKEN", "x")
os.environ.setdefault("LINE_CHANNEL_SECRET", "x")
os.environ.setdefault("SCAN_SECRET", "x")
os.environ.setdefault("SETTRADE_APP_ID", "x")
os.environ.setdefault("SETTRADE_APP_SECRET", "x")
os.environ.setdefault("SETTRADE_BROKER_ID", "x")
os.environ.setdefault("SETTRADE_APP_CODE", "x")
os.environ.setdefault("GCP_PROJECT_ID", "x")

import numpy as np
import yfinance as yf

from analyzer import analyze_index, classify_stage, detect_pattern, scan_stock


# symbol, display name, yfinance ticker, path ("stock" runs scan_stock,
# "index" runs analyze_index)
SAMPLES = [
    ("STECON",  "Sino-Thai Engineering & Construction",  "STECON.BK", "stock"),
    ("KKP",     "Kiatnakin Phatra Bank",                 "KKP.BK",    "stock"),
    ("DELTA",   "Delta Electronics (Thailand)",          "DELTA.BK",  "stock"),
    ("SCC",     "Siam Cement Group",                     "SCC.BK",    "stock"),
    ("SET",     "SET Composite Index",                   "^SET.BK",   "index"),
    ("KOSPI",   "KOSPI (Korea)",                         "^KS11",     "index"),
    ("BTC",     "Bitcoin (USD)",                         "BTC-USD",   "stock"),
]


def fetch(yf_ticker: str):
    """1y daily OHLCV. Returns DataFrame or None."""
    try:
        df = yf.Ticker(yf_ticker).history(period="1y", auto_adjust=False)
        if df is None or df.empty:
            return None
        df = df.dropna(subset=["Close"])
        return df if len(df) >= 60 else None
    except Exception as exc:
        print(f"  FETCH FAILED: {exc}")
        return None


def stage2_rule_trace(df):
    """Reproduce the 6-clause boolean from analyzer.classify_stage so we can
    see WHY the classifier picked the stage it did. Returns a list of
    (clause_label, passed, detail_string) tuples."""
    if len(df) < 200:
        return [("(insufficient history — stage 1 fallback)", None, f"{len(df)} rows")]

    close = df["Close"]
    high = df["High"]
    s50 = close.rolling(50).mean()
    s150 = close.rolling(150).mean()
    s200 = close.rolling(200).mean()

    c = float(close.iloc[-1])
    m50 = float(s50.iloc[-1])
    m150 = float(s150.iloc[-1])
    m200 = float(s200.iloc[-1])
    m200_20 = float(s200.iloc[-21]) if len(s200.dropna()) > 21 else float("nan")

    lookback = min(252, len(df))
    hi52 = float(high.iloc[-lookback:].max())
    lo52 = float(close.iloc[-lookback:].min())

    clauses = [
        ("close > sma150",           c > m150,                        f"{c:,.2f} vs {m150:,.2f}"),
        ("close > sma200",           c > m200,                        f"{c:,.2f} vs {m200:,.2f}"),
        ("sma150 > sma200",          m150 > m200,                     f"{m150:,.2f} vs {m200:,.2f}"),
        ("sma200 rising (20d)",      (not np.isnan(m200_20)) and m200 > m200_20,
                                     f"{m200:,.2f} now vs {m200_20:,.2f} 20d ago"),
        ("close >= 1.25 × low_52w",  c >= lo52 * 1.25,                f"{c:,.2f} vs {lo52 * 1.25:,.2f}  (low={lo52:,.2f})"),
        ("close >= 0.75 × high_52w", c >= hi52 * 0.75,                f"{c:,.2f} vs {hi52 * 0.75:,.2f}  (high={hi52:,.2f})"),
    ]
    return clauses


def dump_stock(code: str, name: str, yf_tk: str, df) -> dict:
    """Run scan_stock + rule trace. Return a dict for the summary table."""
    sig = scan_stock(code, df)
    print(f"\n{'=' * 68}")
    print(f"{code}  ({name})     yf={yf_tk}")
    print(f"{'=' * 68}")

    if sig is None:
        print("  scan_stock returned None (insufficient data or too stale)")
        return {"code": code, "stage": None, "pattern": None, "data_date": None}

    print(f"  data_date      : {sig.data_date}")
    print(f"  close          : {sig.close:,.2f}   change: {sig.change_pct:+.2f}%")
    print(f"  SMA50/150/200  : {sig.sma50:,.2f} / {sig.sma150:,.2f} / {sig.sma200:,.2f}")
    print(f"  52W range      : {sig.low_52w:,.2f}  →  {sig.high_52w:,.2f}"
          f"    (from_high: {sig.pct_from_52w_high:+.2f}%)")
    print(f"  volume_ratio   : {sig.volume_ratio:.2f}×   trade_value: {sig.trade_value_m:.1f}M")
    print(f"  atr(14)        : {sig.atr:.2f}   stop: {sig.stop_loss:.2f}   target: {sig.target_price:.2f}")
    print(f"  strength_score : {sig.strength_score}/100")
    print(f"  breakouts(1y)  : {sig.breakout_count_1y}")
    print()
    print(f"  ── Stage-2 rule trace ──")
    for label, passed, detail in stage2_rule_trace(df):
        if passed is True:
            mark = "[✓]"
        elif passed is False:
            mark = "[✗]"
        else:
            mark = "[?]"
        print(f"    {mark} {label:<30s}  {detail}")
    print()
    weak = " ⚠ WEAKENING (close < SMA50)" if getattr(sig, "stage_weakening", False) else ""
    print(f"  >>> SCANNER DECISION: stage={sig.stage}{weak}  pattern={sig.pattern}")
    if sig.breakout_details:
        print(f"      breakout_details: {sig.breakout_details}")
    return {"code": code, "stage": sig.stage, "pattern": sig.pattern,
            "data_date": sig.data_date, "close": sig.close,
            "weakening": getattr(sig, "stage_weakening", False)}


def dump_index(code: str, name: str, yf_tk: str, df) -> dict:
    """Run analyze_index. As of the index-pattern fix, indexes ALSO get
    pattern detection (breakout / breakout_attempt / vcp) but with the
    1.4× volume gate dropped — index volume is aggregate, not directional."""
    res = analyze_index(df, name)
    print(f"\n{'=' * 68}")
    print(f"{code}  ({name})     yf={yf_tk}    [index path]")
    print(f"{'=' * 68}")
    print(f"  close          : {res.get('close', 0):,.2f}   change: {res.get('change_pct', 0):+.2f}%")
    print(f"  MA50/150/200   : {res.get('ma50', 0):,.2f} / {res.get('ma150', 0):,.2f} / {res.get('ma200', 0):,.2f}")
    print(f"  RSI(14)        : {res.get('rsi', 0):.1f}")
    print(f"  MACD hist      : {res.get('macd_hist', 0):+.3f}")
    print(f"  52W from high  : {res.get('pct_from_52w_high', 0):+.1f}%")
    print(f"  above MA200    : {res.get('above_ma200')}   MA200 rising: {res.get('ma200_rising')}")
    print()
    print(f"  ── Stage-2 rule trace ──")
    for label, passed, detail in stage2_rule_trace(df):
        mark = "[✓]" if passed is True else ("[✗]" if passed is False else "[?]")
        print(f"    {mark} {label:<30s}  {detail}")
    print()
    pattern = res.get("pattern")
    print(f"  >>> SCANNER DECISION: stage={res.get('stage')}  pattern={pattern}")
    print(f"      implication: {res.get('implication', '')}")
    bd = res.get("breakout_details") or {}
    if bd:
        print(f"      breakout_details: {bd}")
    return {"code": code, "stage": res.get("stage"),
            "pattern": pattern or "(none)", "data_date": "",
            "close": res.get("close"), "weakening": False}


def main():
    print("Signalix scanner validation — 7 sample assets")
    print("=" * 68)
    results = []
    for code, name, yf_tk, path in SAMPLES:
        df = fetch(yf_tk)
        if df is None:
            print(f"\n{code}: no data")
            results.append({"code": code, "stage": None, "pattern": None, "data_date": None})
            continue
        if path == "stock":
            results.append(dump_stock(code, name, yf_tk, df))
        else:
            results.append(dump_index(code, name, yf_tk, df))

    # Summary table
    print()
    print("=" * 68)
    print("SUMMARY — compare the Stage/Pattern columns with your TradingView read")
    print("=" * 68)
    print(f"  {'SYMBOL':<8s} {'STAGE':<6s} {'PATTERN':<16s} {'CLOSE':>12s}   DATA DATE")
    print(f"  {'-' * 8} {'-' * 6} {'-' * 16} {'-' * 12}   {'-' * 10}")
    for r in results:
        stg_raw = str(r["stage"]) if r["stage"] is not None else "—"
        stg = f"{stg_raw}⚠" if r.get("weakening") else stg_raw
        pat = r["pattern"] or "—"
        close = f"{r['close']:,.2f}" if r.get("close") else "—"
        date = r.get("data_date") or ""
        print(f"  {r['code']:<8s} {stg:<6s} {pat:<16s} {close:>12s}   {date}")


if __name__ == "__main__":
    main()
