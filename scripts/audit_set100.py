#!/usr/bin/env python3
"""SET100 classification audit harness — runs the FSM classifier against
every SET100 constituent (yfinance live data) and applies a battery of
rule-of-thumb sanity checks. Surfaces every classification that disagrees
with basic price-action expectations.

The audit is the gating bar: don't deploy classifier changes until the
audit's flagged-count is at or below the agreed threshold and every
remaining flag is a known acceptable edge case.

Run:
    python3 scripts/audit_set100.py [--limit N]

Reports:
    SUMMARY  — counts by stage / sub-stage + flagged count
    FLAGS    — table of every classification that tripped a sanity check
                (sorted by severity), with the specific rule that fired
"""
from __future__ import annotations

import argparse
import sys
import warnings
from collections import Counter, defaultdict
from dataclasses import dataclass

warnings.filterwarnings("ignore")
sys.path.insert(0, "/home/user/Signalix")

import numpy as np  # noqa: E402
import pandas as pd  # noqa: E402
import yfinance as yf  # noqa: E402

from analyzer import (  # noqa: E402
    classify_stage, classify_sub_stage, _sma, _sma_roc, _cross_within,
    _atr,
)


# ─── Auxiliary metrics ─────────────────────────────────────────────────────────
def _bars_since_high(df: pd.DataFrame, lookback: int = 252) -> int:
    n = len(df)
    win = df["High"].iloc[-min(lookback, n):]
    return (len(win) - 1) - int(win.values.argmax())


def _return_pct(df: pd.DataFrame, n: int) -> float:
    if len(df) < n + 1:
        return 0.0
    a = float(df["Close"].iloc[-(n + 1)])
    b = float(df["Close"].iloc[-1])
    return (b - a) / a * 100.0 if a > 0 else 0.0


# ─── Sanity checks ─────────────────────────────────────────────────────────────
@dataclass
class Flag:
    sym: str
    stage: int
    sub: str
    rule: str
    severity: str         # "block" / "warn"
    detail: str


def audit_one(sym: str, df: pd.DataFrame) -> tuple[int, str, list[Flag]]:
    """Classify the stock, then run the rule battery. Return
    (parent_stage, sub_stage, flags)."""
    flags: list[Flag] = []

    p = classify_stage(df)
    s = classify_sub_stage(df, p)

    close = df["Close"]
    c = float(close.iloc[-1])
    s10 = float(_sma(close, 10).iloc[-1])
    s20 = float(_sma(close, 20).iloc[-1])
    s50 = float(_sma(close, 50).iloc[-1])
    s200 = float(_sma(close, 200).iloc[-1])
    roc200 = _sma_roc(close, 200, 20)
    high_52w = float(df["High"].iloc[-min(252, len(df)):].max())
    pct_from_high = (c / high_52w - 1) * 100 if high_52w > 0 else 0.0
    bars_since_peak = _bars_since_high(df)
    ret_60 = _return_pct(df, 60)
    ret_20 = _return_pct(df, 20)
    ret_5  = _return_pct(df, 5)

    def F(rule, sev, detail):
        flags.append(Flag(sym, p, s, rule, sev, detail))

    # ── Parent-stage sanity ──────────────────────────────────────────────────

    # Stage 2 should be a real uptrend. If 60-day return is negative AND
    # close is below SMA50, that's not Stage 2 — that's a stock weakening
    # into Stage 3 / Stage 1. Catches Path 4 entrenched mis-firing on
    # decaying stocks.
    if p == 2 and ret_60 < -5 and c < s50:
        F("stage2_but_60d_negative_and_below_sma50", "warn",
          f"60d_ret={ret_60:+.1f}% c={c:.2f} sma50={s50:.2f}")

    # Stage 3 = topping. Required: peak is recent (≤60 bars), price still
    # close to peak (≥75% of high), AND price is rolling over (close < SMA50).
    if p == 3:
        if bars_since_peak > 60:
            F("stage3_but_peak_old", "block",
              f"bars_since_peak={bars_since_peak} > 60 → basing not topping")
        if pct_from_high < -25:
            F("stage3_but_far_from_peak", "block",
              f"pct_from_high={pct_from_high:+.1f}% — too far off peak to be topping")
        if ret_20 > 5:
            F("stage3_but_recent_rally", "warn",
              f"20d_ret={ret_20:+.1f}% — rallying not topping")

    # Stage 4 = downtrend. Required: rolling over fully, ROC negative,
    # price below SMA200.
    if p == 4:
        if ret_60 > 10:
            F("stage4_but_recent_rally", "warn",
              f"60d_ret={ret_60:+.1f}% — recovering not downtrending")
        if c > s200:
            F("stage4_but_above_sma200", "warn",
              f"c={c:.2f} > sma200={s200:.2f}")

    # Stage 1 = basing / accumulation. Generic check: nothing severe.
    # Allow Stage 1 to be the catch-all for "not actively trading".

    # ── Sub-stage sanity ─────────────────────────────────────────────────────

    # PIVOT_READY definition: 5-bar tightness < 7% + close < SMA20
    if s == "STAGE_2_PIVOT_READY":
        h5 = float(df["High"].iloc[-5:].max())
        l5 = float(df["Low"].iloc[-5:].min())
        tight = (h5 - l5) / l5 if l5 > 0 else 1.0
        if tight >= 0.07:
            F("pivot_ready_but_not_tight", "warn",
              f"5bar_tightness={tight:.3f} >= 0.07")

    # IGNITION definition: recent kick (cross within 20 bars OR new
    # 52W high OR price leads all 3 MAs). Sanity: should have positive
    # recent momentum.
    if s == "STAGE_2_IGNITION":
        if ret_20 < -5:
            F("ignition_but_negative_20d", "warn",
              f"20d_ret={ret_20:+.1f}% — ignition needs positive momentum")
        # Should have a recent move — either today's bar or last 5 days
        if ret_5 < -5:
            F("ignition_but_falling_recent", "warn",
              f"5d_ret={ret_5:+.1f}%")

    # MARKUP definition: close > SMA10, close > SMA20. Sanity check
    # that this actually holds.
    if s == "STAGE_2_MARKUP":
        if not (c > s10 and c > s20):
            F("markup_but_below_short_mas", "warn",
              f"c={c:.2f} sma10={s10:.2f} sma20={s20:.2f}")

    # OVEREXTENDED: should genuinely be far from MAs. Already gated
    # but let's verify the actual spread.
    if s == "STAGE_2_OVEREXTENDED":
        if c <= s50 * 1.20:
            F("overextended_but_close_to_sma50", "warn",
              f"c/sma50={c/s50:.3f} — barely above SMA50")

    # CONTRACTION: close < SMA10 OR < SMA20 AND ~at-or-above SMA50.
    # Tolerance mirrors classifier's c >= s50*0.97 — stocks dipping
    # briefly below SMA50 still count as 'pullback within Stage 2'.
    if s == "STAGE_2_CONTRACTION":
        if not ((c < s10 or c < s20) and c >= s50 * 0.97):
            F("contraction_violates_ma_structure", "warn",
              f"c={c:.2f} sma10={s10:.2f} sma20={s20:.2f} sma50={s50:.2f}")

    # PREP: should be loading toward Stage 2 — close > SMA200, SMA50
    # near SMA200 (within ~5%), some upward bias
    if s == "STAGE_1_PREP":
        if c <= s200:
            F("prep_but_below_sma200", "warn",
              f"c={c:.2f} sma200={s200:.2f}")

    # BREAKDOWN: fresh death cross + heavy volume + below SMA200
    if s == "STAGE_4_BREAKDOWN":
        if c >= s50:
            F("breakdown_but_above_sma50", "warn",
              f"c={c:.2f} sma50={s50:.2f}")

    # DOWNTREND: only flag SEVERE alignment breaks. Close oscillating near
    # SMA20 is normal Stage 4 noise — what matters is c < s50 < s200
    # (mid- and long-term still down). Don't flag stocks where close
    # is just barely above s20.
    if s == "STAGE_4_DOWNTREND":
        if c > s50 or s50 > s200:
            F("downtrend_severe_alignment_break", "warn",
              f"c={c:.2f} s20={s20:.2f} s50={s50:.2f} s200={s200:.2f}")

    return p, s, flags


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--limit", type=int, default=999)
    ap.add_argument("--symbol", help="Audit a single symbol only (debug)")
    args = ap.parse_args()

    from data import get_index_members
    members = sorted(get_index_members("SET100"))
    if args.symbol:
        members = [args.symbol.upper()]
    members = members[:args.limit]

    print(f"\nAuditing {len(members)} SET100 members (yfinance live)…\n")

    classifications: list[tuple[str, int, str]] = []
    all_flags: list[Flag] = []
    skipped = 0
    for i, sym in enumerate(members, 1):
        try:
            df = yf.Ticker(f"{sym}.BK").history(period="1y", auto_adjust=False)
        except Exception as e:
            print(f"  {sym}: yfinance fetch failed: {e}")
            skipped += 1
            continue
        if df.empty or len(df) < 60:
            skipped += 1
            continue
        p, s, flags = audit_one(sym, df)
        classifications.append((sym, p, s))
        all_flags.extend(flags)
        if i % 25 == 0:
            print(f"  …{i}/{len(members)} processed")

    # Summary
    print("\n" + "=" * 78)
    print("SUMMARY")
    print("=" * 78)
    by_stage = Counter(p for _, p, _ in classifications)
    by_sub   = Counter(s for _, _, s in classifications)
    print(f"  Total scanned: {len(classifications)}  (skipped: {skipped})")
    print(f"  Parent stages: " + "  ".join(
        f"{k}:{v}" for k, v in sorted(by_stage.items())))
    print(f"  Sub-stages:")
    for sub, cnt in sorted(by_sub.items(), key=lambda kv: (-kv[1], kv[0])):
        print(f"    {sub:<24s} {cnt:>4d}")

    # Flags table
    print(f"\n  Total flags: {len(all_flags)} "
          f"({sum(1 for f in all_flags if f.severity == 'block')} block, "
          f"{sum(1 for f in all_flags if f.severity == 'warn')} warn)")

    if all_flags:
        # Group by rule
        by_rule = defaultdict(list)
        for f in all_flags:
            by_rule[f.rule].append(f)
        print("\n" + "=" * 78)
        print("FLAGS BY RULE")
        print("=" * 78)
        for rule, fs in sorted(by_rule.items(), key=lambda kv: (-len(kv[1]), kv[0])):
            sev = fs[0].severity
            print(f"\n  ⚠ {rule}  [{sev}]  ×{len(fs)}")
            for f in fs[:10]:  # show up to 10 examples per rule
                print(f"      {f.sym:<10s} stage={f.stage} sub={f.sub:<22s} {f.detail}")
            if len(fs) > 10:
                print(f"      … and {len(fs) - 10} more")

    # Exit code = # of BLOCK-severity flags. Local CI gates can fail the
    # build when block flags are non-zero.
    blocks = sum(1 for f in all_flags if f.severity == "block")
    print(f"\n  Block-severity flags: {blocks}")
    return 1 if blocks > 0 else 0


if __name__ == "__main__":
    sys.exit(main())
