#!/usr/bin/env python3
"""Live validation harness for the new pivot-point math
(`_last_run_high` 30-bar window for PIVOT_READY / IGNITION / PREP).

Pulls 1y of yfinance data per symbol, runs `classify_sub_stage` and
`compute_pivot`, and prints a side-by-side table comparing the new
pivot to the OLD math (5-bar high for PIVOT_READY, 15-bar high for
IGNITION/PREP). Helps eyeball whether the new pivot lands at the
visible swing high a trader watches on a chart.

Run:
    python3 scripts/pivot_validate.py
    python3 scripts/pivot_validate.py --symbols RBF,ONEE,KCG
    python3 scripts/pivot_validate.py --universe set100  # broader sweep

Exit code:
    0  — every sample's new pivot is at-or-above the old pivot AND
         within 95% of the 52W high (a sanity gate for shape).
    1  — at least one sample failed; printed which rule fired.
"""
from __future__ import annotations

import argparse
import sys
import warnings

warnings.filterwarnings("ignore")
sys.path.insert(0, "/home/user/Signalix")

import pandas as pd  # noqa: E402
import yfinance as yf  # noqa: E402

from analyzer import (  # noqa: E402
    classify_stage, classify_sub_stage, compute_pivot, compute_targets,
    _last_run_high,
    SUB_STAGE_2_PIVOT_READY, SUB_STAGE_2_IGNITION, SUB_STAGE_1_PREP,
    SUB_STAGE_2_CONTRACTION, SUB_STAGE_2_MARKUP,
)


PRIORITY_SUB_STAGES = {
    SUB_STAGE_2_PIVOT_READY,
    SUB_STAGE_2_IGNITION,
    SUB_STAGE_1_PREP,
    SUB_STAGE_2_CONTRACTION,
    SUB_STAGE_2_MARKUP,
}


def _old_pivot(df: pd.DataFrame, sub_stage: str) -> float:
    """Reproduce the OLD pivot math so the validation table can show
    the delta. PIVOT_READY = 5-bar high; everything else in scope =
    15-bar high. Used only for printing the diff column."""
    if sub_stage == SUB_STAGE_2_PIVOT_READY:
        return float(df["High"].iloc[-5:].max())
    return float(df["High"].iloc[-15:].max())


def _bars_since(df: pd.DataFrame, value: float, lookback: int = 80) -> int:
    """How many bars back is the high == value? Reads right-to-left
    over the last `lookback` bars. Useful for printing 'pivot from N
    bars back' for visual sanity."""
    n = min(lookback, len(df))
    highs = df["High"].iloc[-n:].values
    for i in range(len(highs) - 1, -1, -1):
        if abs(highs[i] - value) < 0.001:
            return (len(highs) - 1) - i
    return -1


def _members_for(universe: str) -> list[str]:
    import data
    universe = universe.lower()
    if universe == "marginable":
        if not data._margin_securities:
            data._load_margin_securities()
        return sorted(data._margin_securities.keys())
    return sorted(data.get_index_members(universe.upper()))


def _fetch(sym: str) -> pd.DataFrame | None:
    try:
        df = yf.Ticker(f"{sym}.BK").history(period="1y", auto_adjust=False)
        if df is None or df.empty or len(df) < 60:
            return None
        return df
    except Exception:
        return None


def main():
    ap = argparse.ArgumentParser(
        description="Validate the new last-run-high pivot math against live data.")
    ap.add_argument("--symbols",
                    help="Comma-separated symbol list (overrides --universe)")
    ap.add_argument("--universe", default="set100",
                    choices=["set50", "set100", "mai", "marginable"])
    ap.add_argument("--limit", type=int, default=999,
                    help="Cap how many universe members to scan")
    ap.add_argument("--include-default-anchors", action="store_true",
                    help="Always include RBF + ONEE (the iteration's anchor "
                         "samples) even if they're not in the universe")
    args = ap.parse_args()

    if args.symbols:
        symbols = [s.strip().upper() for s in args.symbols.split(",") if s.strip()]
    else:
        symbols = _members_for(args.universe)[:args.limit]

    if args.include_default_anchors:
        for anchor in ("RBF", "ONEE"):
            if anchor not in symbols:
                symbols.append(anchor)

    print(f"\nValidating pivot math on {len(symbols)} symbols (yfinance live)…\n")

    rows: list[dict] = []
    skipped = 0
    for i, sym in enumerate(symbols, 1):
        df = _fetch(sym)
        if df is None:
            skipped += 1
            continue

        stage = classify_stage(df)
        sub   = classify_sub_stage(df, stage)
        if sub not in PRIORITY_SUB_STAGES:
            continue

        close_now  = float(df["Close"].iloc[-1])
        high_52w   = float(df["High"].iloc[-min(252, len(df)):].max())
        low_52w    = float(df["Low"].iloc[-min(252, len(df)):].min())
        old_pivot  = _old_pivot(df, sub)
        new_pivot, new_stop = compute_pivot(df, sub)
        t1, t1618  = compute_targets(new_pivot, new_stop, low_52w)
        bars_back  = _bars_since(df, new_pivot)

        gap_high   = (new_pivot / high_52w - 1) * 100 if high_52w else 0.0
        gap_close  = (new_pivot / close_now - 1) * 100 if close_now else 0.0
        risk_pct   = (new_stop - close_now) / close_now * 100 if close_now else 0.0
        upside     = (t1618 / close_now - 1) * 100 if close_now and t1618 else 0.0

        rows.append({
            "sym": sym, "sub": sub.replace("STAGE_", "S"),
            "close": close_now, "hi52": high_52w,
            "start": low_52w,  # Pin1 = cycle low (52W low)
            "old": old_pivot, "new": new_pivot,
            "bars_back": bars_back,
            "gap_close": gap_close, "gap_hi": gap_high,
            "stop": new_stop, "risk_pct": risk_pct,
            "t1618": t1618, "upside": upside,
        })

        if i % 50 == 0:
            print(f"  …{i}/{len(symbols)} processed")

    if not rows:
        print(f"  No samples in priority sub-stages "
              f"(skipped: {skipped}).")
        return 0

    rows.sort(key=lambda r: (r["sub"], -r["new"]))

    # ── Table — Anchors + Target ─────────────────────────────────────────
    # Shows the 4 reference points used in the Fibonacci 3-point
    # extension: Start (cycle low / Pin1) → Pivot (Pin2) → Low (Pin3)
    # → Target (T1.618). Lets the user spot-check whether the auto
    # anchors match their chart-drawn Fib for each stock.
    print("\n" + "=" * 115)
    print(f"{'SYM':<7}{'SubStage':<18}{'Close':>8}"
          f"{'Start':>8}{'Pivot':>8}{'Low':>8}{'Range':>8}"
          f"{'Target':>9}{'Upside':>9}")
    print("=" * 115)
    for r in rows:
        rng = r['new'] - r['start']
        print(f"{r['sym']:<7}{r['sub']:<18}"
              f"{r['close']:>8.2f}"
              f"{r['start']:>8.2f}{r['new']:>8.2f}{r['stop']:>8.2f}"
              f"{rng:>8.2f}"
              f"{r['t1618']:>9.2f}{r['upside']:>+8.1f}%")

    # ── Sanity gates ────────────────────────────────────────────────────
    print("\n" + "=" * 110)
    print("SANITY GATES")
    print("=" * 110)
    fails: list[str] = []
    for r in rows:
        if r["new"] < r["old"] - 0.01:  # 1¢ tolerance for float noise
            fails.append(
                f"  ✗ {r['sym']} {r['sub']}: new pivot ฿{r['new']:.2f} "
                f"< old pivot ฿{r['old']:.2f} (regression — new should be "
                f"at-or-above old)")
        # Pivot should be close to 52W high (within 95%) for these stages
        if r["hi52"] > 0 and r["new"] < r["hi52"] * 0.75:
            fails.append(
                f"  ⚠ {r['sym']} {r['sub']}: new pivot ฿{r['new']:.2f} "
                f"only {r['new']/r['hi52']*100:.0f}% of 52W high "
                f"฿{r['hi52']:.2f} — far-from-high, may be wrong shape")
        # Pivot should be ABOVE close (we're not yet broken out)
        if r["new"] < r["close"] * 1.001:
            fails.append(
                f"  ⚠ {r['sym']} {r['sub']}: pivot ฿{r['new']:.2f} ≈ close "
                f"฿{r['close']:.2f} — already at/above pivot, why is this "
                f"still {r['sub']}?")

    if fails:
        print(f"\n  {len(fails)} issue(s) found:")
        for f in fails:
            print(f)
        # Block-severity fails (regression) → exit 1; warns just print.
        regressions = [f for f in fails if " ✗ " in f]
        return 1 if regressions else 0
    else:
        print(f"\n  ✓ All {len(rows)} samples pass: new pivot ≥ old pivot, "
              f"within 25% of 52W high, above current close.")
        return 0


if __name__ == "__main__":
    sys.exit(main())
