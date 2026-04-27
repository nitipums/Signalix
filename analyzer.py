"""
analyzer.py — Minervini Stage Analysis + Pattern Detection for SET stocks.

Stages (Mark Minervini):
  Stage 1 — Basing / Neglect (consolidation after downtrend, MA flattening)
  Stage 2 — Uptrend (price above rising MAs, within 25% of 52-week high)
  Stage 3 — Distribution / Topping (price breaking below support after Stage 2)
  Stage 4 — Downtrend (price below declining MAs)

Patterns detected:
  breakout         — Stage 2 stock closing above recent resistance on high volume
  ath_breakout     — Breakout at or above all-time high
  breakout_attempt — Day's HIGH broke 52-bar pivot on ≥1.4× volume, but CLOSE
                     retreated below pivot. Captures in-progress breakouts
                     before the close confirms (reversal at resistance / handle).
  vcp              — Volatility Contraction Pattern (3+ tightening contractions)
  vcp_low_cheat    — Entry near final VCP low, volume drying up
  consolidating    — Stage 1 basing with contracting volatility
  going_down       — Stage 4 downtrend
"""

import logging
from dataclasses import dataclass, field
from typing import Optional

import numpy as np
import pandas as pd

from data import SECTOR_MAP, fetch_all_stocks, get_sector, get_stock_list, tradingview_url

logger = logging.getLogger(__name__)

MIN_ROWS = 200  # Minimum trading days of data required for analysis
MAX_CANDLE_STALENESS_DAYS = 10  # Skip scans whose latest candle is older than this
                                # — filters out suspended / untradeable tickers
                                # (e.g. SET "Suspend" status) so they don't emit
                                # misleading stage/pattern signals on pre-halt bars.


# ─── Data classes ─────────────────────────────────────────────────────────────

@dataclass
class StockSignal:
    # All fields have defaults for Firestore backward compatibility —
    # null values in old docs are filtered out before construction,
    # so missing fields fall back to these safe defaults.
    symbol: str = ""
    name: str = ""
    stage: int = 1                  # 1, 2, 3, or 4
    pattern: str = "consolidating"  # see module docstring
    close: float = 0.0
    change_pct: float = 0.0
    volume: int = 0
    volume_ratio: float = 0.0       # today's vol / 20-day avg vol
    sma50: float = 0.0
    sma100: float = 0.0
    sma150: float = 0.0
    sma200: float = 0.0
    high_52w: float = 0.0
    low_52w: float = 0.0
    strength_score: float = 0.0     # 0–100 composite ranking
    tradingview_url: str = ""
    scanned_at: str = ""            # ISO timestamp
    breakout_details: dict = field(default_factory=dict)
    # Risk/reward fields
    atr: float = 0.0               # 14-day Average True Range
    trade_value_m: float = 0.0     # Trade value in THB millions (volume * close / 1M)
    pct_from_52w_high: float = 0.0 # (close / high_52w - 1) * 100, negative when below ATH
    stop_loss: float = 0.0         # ATR-based: close - 1.5 * ATR
    target_price: float = 0.0      # 2:1 risk/reward: close + 2 * (close - stop_loss)
    breakout_count_1y: int = 0     # Number of distinct breakout events in past year
    data_date: str = ""            # Date of the last candle used (YYYY-MM-DD) — separate from scanned_at
    # Sub-stage finite state machine (one of 9 SUB_STAGE_* constants, or "" for
    # old Firestore docs / unclassified). Source of truth for "what state is
    # this stock in"; the legacy `pattern` field is auto-derived from this.
    sub_stage: str = ""
    sma10: float = 0.0             # Short-term MA needed by STAGE_2_RUNNING
    sma20: float = 0.0             # Short-term MA used across multiple sub-stages
    sma200_roc20: float = 0.0      # SMA200 % change vs 20 bars ago — slope signal
    # Pivot-point fields, computed only for STAGE_1_PREP / STAGE_2_EARLY /
    # STAGE_2_RUNNING / STAGE_2_PULLBACK (the actionable buy-side states).
    # 0.0 for stocks outside this scope. See compute_pivot() in analyzer.
    pivot_price: float = 0.0       # Local resistance / buy trigger (15-bar high)
    pivot_stop: float = 0.0        # Setup invalidation floor (10-bar low)
    # Fibonacci 3-point extension targets, computed from
    # (Pin1=52W_low, Pin2=pivot, Pin3=stop). T1.0 = first take-profit
    # zone; T1.618 = extended target. Both stay 0.0 when pivot is 0
    # (out-of-scope sub-stages).
    target_1: float = 0.0
    target_1618: float = 0.0
    fib_start: float = 0.0         # ZigZag-detected cycle low (Pin1 anchor)
    fib_pivot: float = 0.0         # 1st-leg peak (Pin2) — current pivot OR an
                                   # earlier qualifying H if it's >= 80% of pivot
    # Margin tier from Krungsri Securities Marginable Securities List:
    # 50/60/70/80 = Initial Margin %, lower = more leverage.
    # 0 = NOT marginable (broker rejects margin orders, must trade 100% cash).
    # Refresh source: scripts/refresh_margin_list.py + commit + deploy.
    margin_im_pct: int = 0
    # Stage-2 weakening modifier: True when stage == 2 AND close < SMA50.
    # Minervini's stage-2 template (MA150/200 alignment) can stay true while
    # near-term momentum rolls over below SMA50 — a classic precursor to a
    # stage-3 transition. The flag lets the UI distinguish "fresh uptrend"
    # from "uptrend faltering" without changing the stage integer itself.
    stage_weakening: bool = False


@dataclass
class MarketBreadth:
    scanned_at: str
    total_stocks: int
    stage1_count: int
    stage2_count: int
    stage3_count: int
    stage4_count: int
    advancing: int
    declining: int
    unchanged: int
    new_highs_52w: int
    new_lows_52w: int
    breakout_count: int
    vcp_count: int
    stage2_pct: float
    above_ma200: int = 0
    below_ma200: int = 0
    above_ma200_pct: float = 0.0
    set_index_close: float = 0.0
    set_index_change_pct: float = 0.0


@dataclass
class SectorSummary:
    sector: str
    total: int
    stage2_count: int
    stage2_pct: float
    breakout_count: int
    avg_strength: float
    advancing: int
    declining: int


# ─── Indicator helpers ─────────────────────────────────────────────────────────

def _sma(series: pd.Series, window: int) -> pd.Series:
    return series.rolling(window=window, min_periods=window).mean()


def _atr(df: pd.DataFrame, window: int = 14) -> pd.Series:
    high = df["High"]
    low = df["Low"]
    prev_close = df["Close"].shift(1)
    tr = pd.concat([
        high - low,
        (high - prev_close).abs(),
        (low - prev_close).abs(),
    ], axis=1).max(axis=1)
    return tr.rolling(window=window, min_periods=window).mean()


def _vol_avg(df: pd.DataFrame, window: int = 20) -> pd.Series:
    return df["Volume"].rolling(window=window, min_periods=window).mean()


# ─── Sub-stage helpers ─────────────────────────────────────────────────────────
# All return scalar floats / bools using the LAST bar of the input series.
# Designed to be cheap (single tail operation each) so classify_sub_stage
# can run them per stock without measurable overhead.

def _sma_roc(series: pd.Series, window: int = 200, lookback: int = 20) -> float:
    """Rate of change of SMA{window} now vs `lookback` bars ago, as percent.
    Returns 0.0 when there isn't enough data to compute either point."""
    sma = _sma(series, window)
    sma_clean = sma.dropna()
    if len(sma_clean) <= lookback:
        return 0.0
    now = float(sma_clean.iloc[-1])
    then = float(sma_clean.iloc[-(lookback + 1)])
    if then == 0:
        return 0.0
    return (now - then) / then * 100.0


def _cross_within(short: pd.Series, long: pd.Series, n: int = 20,
                  direction: str = "up") -> bool:
    """True if `short` crossed the `long` series anywhere in the last n bars
    in the given direction. direction='up' = golden cross; 'down' = death."""
    if len(short) < n + 1 or len(long) < n + 1:
        return False
    s_tail = short.iloc[-(n + 1):]
    l_tail = long.iloc[-(n + 1):]
    if direction == "up":
        # cross up: previous bar short<=long, current bar short>long
        diff = (s_tail.values - l_tail.values)
        return any((diff[i - 1] <= 0 < diff[i]) for i in range(1, len(diff)))
    diff = (s_tail.values - l_tail.values)
    return any((diff[i - 1] >= 0 > diff[i]) for i in range(1, len(diff)))


def _volume_dry_up(df: pd.DataFrame, n: int = 10, threshold: float = 0.7) -> bool:
    """True when last-n-bar avg volume < threshold × 20-bar avg volume.
    Captures the 'volume drying up' fingerprint for STAGE_2_PULLBACK."""
    if len(df) < 20:
        return False
    last_n_avg = float(df["Volume"].iloc[-n:].mean())
    avg_20 = float(df["Volume"].iloc[-20:].mean())
    if avg_20 <= 0:
        return False
    return last_n_avg < avg_20 * threshold


def _range_contraction(df: pd.DataFrame, n: int = 10) -> bool:
    """True when last-n-bar (high-low) range is narrower than the prior n-bar
    range. Used by STAGE_2_PULLBACK to confirm 'narrowing range'."""
    if len(df) < 2 * n:
        return False
    recent = float((df["High"].iloc[-n:] - df["Low"].iloc[-n:]).mean())
    prior = float((df["High"].iloc[-(2 * n):-n] - df["Low"].iloc[-(2 * n):-n]).mean())
    if prior <= 0:
        return False
    return recent < prior


def _price_tightness_5bar(df: pd.DataFrame) -> float:
    """Tightness ratio over last 5 bars: (max High − min Low) / min Low.
    Drives STAGE_2_PIVOT_READY's < 0.07 (7%) gate. Returns 1.0 (= 100%
    range, i.e. NOT tight) when there isn't enough data."""
    if len(df) < 5:
        return 1.0
    h = float(df["High"].iloc[-5:].max())
    l = float(df["Low"].iloc[-5:].min())
    if l <= 0:
        return 1.0
    return (h - l) / l


def _vol_dry_up_strict(df: pd.DataFrame, window: int = 50, threshold: float = 0.5) -> bool:
    """Strict volume dry-up: today's volume < SMA_volume_{window} × threshold.
    Drives STAGE_2_PIVOT_READY's < 50% of 50-day avg gate. Distinct from
    the looser _volume_dry_up which compares last N bars to 20-bar avg."""
    if len(df) < window:
        return False
    today_vol = float(df["Volume"].iloc[-1])
    avg_vol = float(df["Volume"].iloc[-window:].mean())
    if avg_vol <= 0:
        return False
    return today_vol < avg_vol * threshold


def _zigzag_cycle_low(df: pd.DataFrame, threshold: float = 0.20,
                      lookback: int = 252) -> float:
    """Detect the start of the current uptrend cycle via ZigZag.

    Walks the last `lookback` bars finding alternating swing highs and
    swing lows where each swing >= `threshold`. Returns the swing-LOW
    that started the most recent uptrend (the bar just BEFORE the
    current Pin2 swing-high, OR — if the latest pivot is itself an L
    — the L before THAT).

    Used as the Pin1 anchor for the Fibonacci 3-point extension. Falls
    back to the absolute lookback-window low when fewer than 2 swings
    are detected (stock too quiet for ZigZag at this threshold).

    Validated against user's chart-drawn Fib anchors:
      - SPRC ฿4.28 ✓ matches user's pin
      - WHA  ฿3.08 ✓ matches user's pin
      - SCC  ฿163.0 ✓ matches user's pin
      - STECON ฿10.70 ✓ matches user's pin
      - HANA ฿15.10 vs user ~฿15.50 (close, +3%)
      - KKP  ฿43.50 vs user ฿62.75 (user picks LATER cycle restart)
      - GULF ฿51.07 area
    """
    n = len(df)
    if n < 2:
        return 0.0
    win = min(lookback, n)
    high = df["High"].iloc[-win:].values
    low  = df["Low"].iloc[-win:].values
    base = n - win
    pivots: list[tuple[int, float, str]] = []
    cur_h = high[0]; cur_h_bar = 0
    cur_l = low[0];  cur_l_bar = 0
    direction = 0  # 0=undetermined, 1=up, -1=down
    for i in range(1, win):
        if direction >= 0:
            if high[i] > cur_h: cur_h = high[i]; cur_h_bar = i
            if low[i] < cur_l and direction == 0: cur_l = low[i]; cur_l_bar = i
        if direction <= 0:
            if low[i] < cur_l: cur_l = low[i]; cur_l_bar = i
            if high[i] > cur_h and direction == 0: cur_h = high[i]; cur_h_bar = i
        if direction == 0:
            if cur_l > 0 and (cur_h - cur_l) / cur_l >= threshold:
                if cur_h_bar < cur_l_bar:
                    pivots.append((base + cur_h_bar, float(cur_h), "H"))
                    direction = -1; cur_l = low[i]; cur_l_bar = i
                else:
                    pivots.append((base + cur_l_bar, float(cur_l), "L"))
                    direction = 1; cur_h = high[i]; cur_h_bar = i
        elif direction == 1:
            if cur_h > 0 and (cur_h - low[i]) / cur_h >= threshold:
                pivots.append((base + cur_h_bar, float(cur_h), "H"))
                direction = -1; cur_l = low[i]; cur_l_bar = i
        else:
            if cur_l > 0 and (high[i] - cur_l) / cur_l >= threshold:
                pivots.append((base + cur_l_bar, float(cur_l), "L"))
                direction = 1; cur_h = high[i]; cur_h_bar = i
    # Append the most recent running pivot
    if direction == 1:
        pivots.append((base + cur_h_bar, float(cur_h), "H"))
    elif direction == -1:
        pivots.append((base + cur_l_bar, float(cur_l), "L"))
    if not pivots:
        return float(low.min())
    # Find Pin1: the swing-LOW that started the current uptrend cycle.
    # The "current cycle's peak" = the HIGHEST H in the detected pivots
    # (not necessarily the most recent H — small post-peak rebounds
    # would otherwise mislead the picker). Pin1 = the L immediately
    # before that highest H.
    #
    # STECON example (validated against user's chart):
    #   ZigZag: L฿5.25 → H฿13.60 → L฿10.70 → H฿13.40
    #   Most-recent-H = ฿13.40 → wrong Pin1 = ฿10.70
    #   Highest-H    = ฿13.60 → correct Pin1 = ฿5.25 ✓
    highs = [(b, v) for b, v, t in pivots if t == "H"]
    if not highs:
        return float(low.min())
    peak_bar, _peak_val = max(highs, key=lambda bv: bv[1])
    ls_before_peak = [v for b, v, t in pivots if t == "L" and b < peak_bar]
    return ls_before_peak[-1] if ls_before_peak else float(low.min())


def _zigzag_first_leg_peak(df: pd.DataFrame, current_pivot: float,
                           similarity: float = 0.80,
                           threshold: float = 0.20,
                           lookback: int = 252) -> float:
    """Detect the 1st-uptrend-leg peak (Pin2) for Fibonacci targets.

    When the user draws Fib by hand, Pin2 isn't always the current peak:
    sometimes it's a meaningful EARLIER peak (the 1st leg's top) that
    sits below the current high. Heuristic: find the previous H in
    ZigZag pivots; if it's >= `similarity` × current_pivot, use it as
    Pin2; otherwise fall back to current_pivot.

    Validated against user's chart-drawn Fib anchors:
      HANA:  prev H ฿26.75 (85% of ฿31.50) → Pin2 ฿26.75 ✓ (~฿27)
      KOSPI: prev H ฿6,347 (97% of ฿6,558) → Pin2 ฿6,347 ✓
      STECON: prev H ฿9.50 (70% of ฿13.60) → falls back to ฿13.60 ✓
      SPRC:  no prior H qualifies → ฿8.20 (current) ✓
    """
    n = len(df)
    if n < 2 or current_pivot <= 0:
        return current_pivot
    win = min(lookback, n)
    high = df["High"].iloc[-win:].values
    low  = df["Low"].iloc[-win:].values
    base = n - win
    pivots: list[tuple[int, float, str]] = []
    cur_h = high[0]; cur_h_bar = 0
    cur_l = low[0];  cur_l_bar = 0
    direction = 0
    for i in range(1, win):
        if direction >= 0:
            if high[i] > cur_h: cur_h = high[i]; cur_h_bar = i
            if low[i] < cur_l and direction == 0: cur_l = low[i]; cur_l_bar = i
        if direction <= 0:
            if low[i] < cur_l: cur_l = low[i]; cur_l_bar = i
            if high[i] > cur_h and direction == 0: cur_h = high[i]; cur_h_bar = i
        if direction == 0:
            if cur_l > 0 and (cur_h - cur_l) / cur_l >= threshold:
                if cur_h_bar < cur_l_bar:
                    pivots.append((base + cur_h_bar, float(cur_h), "H"))
                    direction = -1; cur_l = low[i]; cur_l_bar = i
                else:
                    pivots.append((base + cur_l_bar, float(cur_l), "L"))
                    direction = 1; cur_h = high[i]; cur_h_bar = i
        elif direction == 1:
            if cur_h > 0 and (cur_h - low[i]) / cur_h >= threshold:
                pivots.append((base + cur_h_bar, float(cur_h), "H"))
                direction = -1; cur_l = low[i]; cur_l_bar = i
        else:
            if cur_l > 0 and (high[i] - cur_l) / cur_l >= threshold:
                pivots.append((base + cur_l_bar, float(cur_l), "L"))
                direction = 1; cur_h = high[i]; cur_h_bar = i
    if direction == 1:
        pivots.append((base + cur_h_bar, float(cur_h), "H"))
    elif direction == -1:
        pivots.append((base + cur_l_bar, float(cur_l), "L"))
    highs = [(b, v) for b, v, t in pivots if t == "H"]
    if len(highs) < 2:
        return current_pivot
    peak_bar, peak_val = max(highs, key=lambda bv: bv[1])
    prior_hs = [(b, v) for b, v in highs if b < peak_bar]
    if not prior_hs:
        return current_pivot
    _, prev_val = prior_hs[-1]  # most recent prior H
    if prev_val >= similarity * peak_val:
        return prev_val
    return current_pivot


def _last_run_high(df: pd.DataFrame, lookback: int = 60) -> float:
    """Highest high over the last `lookback` bars.

    For stocks in PIVOT_READY / IGNITION / PREP sub-stages this is by
    construction the swing peak of the most recent advance: those
    stages imply a recent leg up followed by either a base or a
    pullback, so the max-high in a wide-enough lookback window
    catches the pre-pullback peak — the level a trader actually
    watches as the breakout trigger.

    A 5-bar window (the previous PIVOT_READY pivot) sat at the top of
    the latest tight zone, missing swing highs 25-40 bars back. The
    initial fix (30-bar) caught RBF (peak 14 bars back) but missed
    ONEE whose peak ฿3.06 landed ~35 bars back. 60 bars (~3 months)
    covers the typical multi-month post-breakout consolidation.
    """
    n = min(lookback, len(df))
    if n <= 0:
        return 0.0
    return float(df["High"].iloc[-n:].max())


def _oscillates_around(close: pd.Series, sma: pd.Series, n: int = 20) -> bool:
    """True if Close has crossed `sma` BOTH directions in the last n bars
    (i.e. is sloshing around the line, not trending). Drives STAGE_1_BASE."""
    if len(close) < n + 1 or len(sma) < n + 1:
        return False
    diff = (close.iloc[-(n + 1):].values - sma.iloc[-(n + 1):].values)
    has_up = any(diff[i - 1] <= 0 < diff[i] for i in range(1, len(diff)))
    has_dn = any(diff[i - 1] >= 0 > diff[i] for i in range(1, len(diff)))
    return has_up and has_dn


# ─── Sub-stage taxonomy ────────────────────────────────────────────────────────
# 9-state finite state machine spanning stocks' full life-cycle. Source of truth
# for the new classification; the legacy `pattern` field on StockSignal is
# auto-derived from this via _derive_pattern() during scan.

SUB_STAGE_1_BASE      = "STAGE_1_BASE"
SUB_STAGE_1_PREP      = "STAGE_1_PREP"
# Legacy Stage 2 sub-stages — kept as module constants so old Firestore
# docs that store these strings still load via the StockSignal
# dataclass. The classifier no longer EMITS these names; new scans
# emit IGNITION / OVEREXTENDED / CONTRACTION / PIVOT_READY / MARKUP.
# Filter commands `early` / `running` / `pullback` map through to the
# new constants (see SUB_STAGE_FILTERS in main.py).
SUB_STAGE_2_EARLY     = "STAGE_2_EARLY"          # legacy → STAGE_2_IGNITION
SUB_STAGE_2_RUNNING   = "STAGE_2_RUNNING"        # legacy → STAGE_2_MARKUP
SUB_STAGE_2_PULLBACK  = "STAGE_2_PULLBACK"       # legacy → CONTRACTION ∪ PIVOT_READY
# New Stage 2 sub-stages from the 2-layer classifier refactor.
# Priority: OVEREXTENDED > PIVOT_READY > IGNITION > CONTRACTION > MARKUP.
SUB_STAGE_2_OVEREXTENDED = "STAGE_2_OVEREXTENDED"
SUB_STAGE_2_PIVOT_READY  = "STAGE_2_PIVOT_READY"
SUB_STAGE_2_IGNITION     = "STAGE_2_IGNITION"
SUB_STAGE_2_CONTRACTION  = "STAGE_2_CONTRACTION"
SUB_STAGE_2_MARKUP       = "STAGE_2_MARKUP"
SUB_STAGE_3_VOLATILE  = "STAGE_3_VOLATILE"
SUB_STAGE_3_DIST_DIST = "STAGE_3_DIST_DIST"
SUB_STAGE_4_BREAKDOWN = "STAGE_4_BREAKDOWN"
SUB_STAGE_4_DOWNTREND = "STAGE_4_DOWNTREND"

ALL_SUB_STAGES: frozenset = frozenset({
    SUB_STAGE_1_BASE, SUB_STAGE_1_PREP,
    SUB_STAGE_2_EARLY, SUB_STAGE_2_RUNNING, SUB_STAGE_2_PULLBACK,
    SUB_STAGE_2_OVEREXTENDED, SUB_STAGE_2_PIVOT_READY,
    SUB_STAGE_2_IGNITION, SUB_STAGE_2_CONTRACTION, SUB_STAGE_2_MARKUP,
    SUB_STAGE_3_VOLATILE, SUB_STAGE_3_DIST_DIST,
    SUB_STAGE_4_BREAKDOWN, SUB_STAGE_4_DOWNTREND,
})


def classify_sub_stage(df: pd.DataFrame, parent_stage: int) -> str:
    """Return one of the 9 SUB_STAGE constants, or '' if no condition fires.

    Parent stage drives which condition tree runs. Within each parent, sub-
    stages are evaluated in priority order — the more 'actionable' state
    wins on tie (e.g. STAGE_2_EARLY beats STAGE_2_PULLBACK beats RUNNING).

    All conditions use SMA (no EMA) per the locked-in design decision.
    """
    if df is None or len(df) < 60:
        return ""

    close = df["Close"]
    s10 = _sma(close, 10)
    s20 = _sma(close, 20)
    s50 = _sma(close, 50)
    s200 = _sma(close, 200)

    if any(s.dropna().empty for s in (s10, s20, s50, s200)):
        return ""

    c = float(close.iloc[-1])
    m10 = float(s10.iloc[-1])
    m20 = float(s20.iloc[-1])
    m50 = float(s50.iloc[-1])
    m200 = float(s200.iloc[-1])

    if any(np.isnan(x) for x in (c, m10, m20, m50, m200)):
        return ""

    roc200 = _sma_roc(close, window=200, lookback=20)
    vol_now = float(df["Volume"].iloc[-1])
    vol_avg_20 = float(df["Volume"].iloc[-20:].mean())
    vol_avg_50 = float(df["Volume"].iloc[-50:].mean()) if len(df) >= 50 else 0.0

    # ── Parent stage 2 — Layer 2 sub-stage classifier ──────────────
    # Five sub-stages, evaluated in this STRICT priority order so
    # multi-condition stocks resolve deterministically:
    #   1. PIVOT_READY  — actionable VCP setup (CONTRACTION + tightness
    #      + volume dry-up triggers all firing). Highest priority —
    #      a tight contraction is actionable regardless of how
    #      stretched the larger structure is.
    #   2. IGNITION     — fresh momentum kick. Wins over OVEREXTENDED
    #      (per locked-in tie-break): a fresh golden-cross breakout
    #      that's already 25% extended is still a fresh breakout, not
    #      a climax warning.
    #   3. OVEREXTENDED — entrenched climax warning. 3-path OR:
    #      Path A: +25% above SMA50 AND golden cross > 40 bars old
    #      Path B: +40% above SMA50 (very stretched flat threshold)
    #      Path C: +25% above SMA50 AND ATR(5) > 2× ATR(20) (parabolic)
    #   4. CONTRACTION  — base building (PIVOT_READY MA structure
    #      WITHOUT the tightness/dry-up triggers).
    #   5. MARKUP       — riding short-term MAs; default fallback for
    #      any Stage 2 stock that doesn't match the others.
    if parent_stage == 2:
        # PIVOT_READY + CONTRACTION share the same MA structure
        # (price below short-term MA but at-or-above mid-term).
        # Tolerance c >= m50 × 0.97 mirrors the parent Stage 2
        # entrenched path — stocks dipping briefly below SMA50 (within
        # 3%) still classify as 'in pullback within Stage 2' rather
        # than the MARKUP fallback which implies running uptrend.
        in_contraction_zone = (c < m10 or c < m20) and c >= m50 * 0.97

        # Priority 1 — PIVOT_READY: contraction + tightness + dry-up.
        if (in_contraction_zone
                and _price_tightness_5bar(df) < 0.07
                and _vol_dry_up_strict(df, window=50, threshold=0.5)):
            return SUB_STAGE_2_PIVOT_READY

        # Priority 2 — IGNITION: fresh momentum kick. Mirrors Layer 1
        # Path 3 (a)+(b) so any stock promoted to Stage 2 via the
        # ignition override paths lands in IGNITION at the sub-stage
        # level. Three trigger forms:
        #   (a) Post-cross: golden cross within 20 bars (tighter than
        #       L1's 30-bar window — only the freshest crosses).
        #   (b) Pre-cross blast: price leads all 3 MAs (close > SMA10
        #       AND close > SMA20 AND close > SMA50) on a decisive
        #       breakout day. Catches GLORY-class stocks where SMA50
        #       hasn't crossed SMA200 yet.
        #   (c) New 52W high on heavy volume (pre-existing path).
        c_prev_l2 = float(close.iloc[-2]) if len(close) > 1 else c
        today_chg_l2 = (c - c_prev_l2) / c_prev_l2 * 100.0 if c_prev_l2 else 0.0
        high_52w_now = float(df["High"].iloc[-min(252, len(df)):].max())
        new_52w_high = high_52w_now > 0 and c >= high_52w_now * 0.999
        strong_today = today_chg_l2 > 5.0
        decisive_kick_l2 = new_52w_high or strong_today
        ignition_volume = vol_avg_50 > 0 and vol_now > vol_avg_50 * 1.5
        golden_cross_recent = _cross_within(s50, s200, n=20, direction="up")
        price_leads_all_mas = c > m10 and c > m20 and c > m50
        if (golden_cross_recent
                or (price_leads_all_mas and decisive_kick_l2)
                or (new_52w_high and ignition_volume)):
            return SUB_STAGE_2_IGNITION

        # Priority 3 — OVEREXTENDED: entrenched climax warning.
        # 3-path OR — only fires when one of the climax tells is
        # genuinely present, not just any +25% gap. Note IGNITION has
        # already returned above for fresh-cross cases, so reaching
        # here means cross is NOT recent (>20 bars old).
        if m50 > 0:
            extended_25  = c > m50 * 1.25
            extended_40  = c > m50 * 1.40
            cross_old_40 = not _cross_within(s50, s200, n=40, direction="up")
            atr_5  = float(_atr(df, 5).iloc[-1])  if len(df) >= 5  else 0.0
            atr_20 = float(_atr(df, 20).iloc[-1]) if len(df) >= 20 else 0.0
            parabolic = atr_20 > 0 and atr_5 > 2.0 * atr_20
            if ((extended_25 and cross_old_40)
                    or extended_40
                    or (extended_25 and parabolic)):
                return SUB_STAGE_2_OVEREXTENDED

        # Priority 4 — CONTRACTION: same MA structure as PIVOT_READY
        # but the tightness/dry-up gates haven't fired yet.
        if in_contraction_zone:
            return SUB_STAGE_2_CONTRACTION

        # Priority 5 (default) — MARKUP: riding short-term MAs.
        return SUB_STAGE_2_MARKUP

    # ── Parent stage 3: DIST_DIST (deeper distribution) → VOLATILE ──
    if parent_stage == 3:
        # Dead cross of SMA20 below SMA50 within last 10 bars + close near
        # SMA200 (within ±5%) = late-distribution defending support.
        near_sma200 = m200 > 0 and abs(c - m200) / m200 < 0.05
        if (_cross_within(s20, s50, n=10, direction="down")
                and c < m50
                and near_sma200):
            return SUB_STAGE_3_DIST_DIST
        # Volatile = wide swings + close repeatedly under SMA20.
        atr_now = float(_atr(df).iloc[-1]) if len(df) >= 14 else 0.0
        atr_20d_ago = float(_atr(df).iloc[-21]) if len(df) > 21 else atr_now
        wide_swings = atr_20d_ago > 0 and atr_now > atr_20d_ago * 1.5
        below_sma20_count = sum(
            1 for k in range(1, 6)
            if k <= len(close) and float(close.iloc[-k]) < float(s20.iloc[-k])
        )
        if wide_swings or below_sma20_count >= 3:
            return SUB_STAGE_3_VOLATILE
        return SUB_STAGE_3_VOLATILE  # default for parent 3

    # ── Parent stage 4: BREAKDOWN (fresh) → DOWNTREND (entrenched) ──
    if parent_stage == 4:
        # Breakdown = today's vol > 1.5× avg AND SMA50 death-crossed SMA200
        # in the last 10 bars. The 'fresh and ugly' state.
        breakdown_vol = vol_avg_20 > 0 and vol_now > vol_avg_20 * 1.5
        death_cross_recent = _cross_within(s50, s200, n=10, direction="down")
        if c < m200 and breakdown_vol and death_cross_recent:
            return SUB_STAGE_4_BREAKDOWN
        if c < m20 < m50 < m200 and roc200 < 0:
            return SUB_STAGE_4_DOWNTREND
        return SUB_STAGE_4_DOWNTREND  # default for parent 4

    # ── Parent stage 1: PREP (loading) → BASE (frozen) ──
    if parent_stage == 1:
        # Prep: close above SMA200 sustained, SMA50 within 3% of SMA200
        # (squeezing), ROC(200,20) ≥ 0.
        sma50_near_sma200 = m200 > 0 and abs(m50 - m200) / m200 < 0.03
        close_above_sma200_5bar = (
            len(close) >= 5
            and all(float(close.iloc[-k]) > float(s200.iloc[-k])
                    for k in range(1, 6))
        )
        if close_above_sma200_5bar and sma50_near_sma200 and roc200 >= 0:
            return SUB_STAGE_1_PREP
        # Base: oscillating around SMA200 with SMA50 below SMA200 and
        # ROC flat-or-negative.
        if _oscillates_around(close, s200, n=20) and m50 < m200 and roc200 <= 0:
            return SUB_STAGE_1_BASE
        return SUB_STAGE_1_BASE  # default for parent 1

    return ""


def _derive_pattern(sub_stage: str, vcp_result: str, is_ath: bool,
                    breakout_confirmed: bool) -> str:
    """Map sub-stages back to the legacy `pattern` vocabulary so existing
    cards / Firestore docs / e2e assertions keep working without code
    changes elsewhere. Source of truth is sub_stage; this is a view.

    Includes mappings for both the new Stage 2 sub-stages (IGNITION /
    OVEREXTENDED / CONTRACTION / PIVOT_READY / MARKUP) and the legacy
    ones (EARLY / RUNNING / PULLBACK) for backward compat with old
    Firestore docs that haven't been re-scanned yet.
    """
    # NEW Stage 2 sub-stages
    if sub_stage == SUB_STAGE_2_IGNITION:
        if breakout_confirmed and is_ath:
            return "ath_breakout"
        if breakout_confirmed:
            return "breakout"
        return "breakout_attempt"
    if sub_stage == SUB_STAGE_2_PIVOT_READY:
        if vcp_result == "vcp_low_cheat":
            return "vcp_low_cheat"
        if vcp_result == "vcp":
            return "vcp"
        return "consolidating"
    if sub_stage in (SUB_STAGE_2_CONTRACTION, SUB_STAGE_2_MARKUP,
                      SUB_STAGE_2_OVEREXTENDED):
        return "consolidating"
    # LEGACY Stage 2 sub-stages (kept for backward compat)
    if sub_stage == SUB_STAGE_2_EARLY:
        if breakout_confirmed and is_ath:
            return "ath_breakout"
        if breakout_confirmed:
            return "breakout"
        return "breakout_attempt"
    if sub_stage == SUB_STAGE_2_PULLBACK:
        if vcp_result == "vcp_low_cheat":
            return "vcp_low_cheat"
        if vcp_result == "vcp":
            return "vcp"
        return "consolidating"
    if sub_stage in (SUB_STAGE_4_BREAKDOWN, SUB_STAGE_4_DOWNTREND):
        return "going_down"
    return "consolidating"


# Sub-stages that get pivot-point computation. Per the locked-in 5-state
# scope: PREP + the four actionable Stage 2 states (IGNITION /
# CONTRACTION / PIVOT_READY / MARKUP). OVEREXTENDED is EXCLUDED — it's
# a warning state, not a buy zone, even though parent is Stage 2.
_PIVOT_SUB_STAGES: frozenset = frozenset({
    SUB_STAGE_1_PREP,
    SUB_STAGE_2_IGNITION,
    SUB_STAGE_2_CONTRACTION,
    SUB_STAGE_2_PIVOT_READY,
    SUB_STAGE_2_MARKUP,
    # Legacy aliases retained so old Firestore docs (loaded mid-migration)
    # still get pivot computed when re-scanned. Once every doc has been
    # re-classified with new constants, these can be removed.
    SUB_STAGE_2_EARLY,
    SUB_STAGE_2_RUNNING,
    SUB_STAGE_2_PULLBACK,
})


def compute_pivot(df: pd.DataFrame, sub_stage: str) -> tuple[float, float]:
    """Return (pivot_price, pivot_stop) for actionable sub-stages.

    PIVOT_READY / IGNITION / PREP (and their legacy aliases) anchor
    the pivot on the last-run swing high: highest high over the last
    30 bars. Catches the pre-pullback peak that a trader actually
    watches as the breakout trigger — the level above which the next
    leg of the advance is confirmed. Stop is the 10-bar pullback
    floor (lowest low in last 10 bars); gives the consolidation room
    to wiggle without invalidating the setup.

    Earlier iteration used a 5-bar window for PIVOT_READY (top of the
    latest tight zone). It was too tight: stocks like RBF / ONEE had
    swing highs 25-40 bars back, so the 5-bar pivot sat ~฿0.30 below
    the actual breakout level. The 30-bar window aligns with the user's
    stated intent ("highest from the last run, 30 days should cover").

    CONTRACTION / MARKUP / legacy RUNNING keep the prior 15-bar / 10-bar
    formula for now — they'll fold into the same math once the priority
    three states are validated visually.

    Returns (0.0, 0.0) for sub-stages outside the actionable set so
    cards / filter commands know to skip those stocks.
    """
    if sub_stage not in _PIVOT_SUB_STAGES or len(df) < 15:
        return 0.0, 0.0
    # PREP override: pivot = 52W high directly. PREP stocks haven't
    # crested into Stage 2 yet — the relevant pivot is the resistance
    # level they need to clear (the 52W high), NOT a recent intra-base
    # shelf. Once they break above the 52W high they classify as
    # IGNITION/MARKUP automatically and the swing-high logic below
    # kicks in. Fixes RAM-class stocks where the 52W high sits OUTSIDE
    # the 60-bar lookback window (RAM 60-bar=฿19.00 vs 52W=฿22.90).
    if sub_stage == SUB_STAGE_1_PREP:
        pivot = float(df["High"].iloc[-min(252, len(df)):].max())
        stop  = float(df["Low"].iloc[-10:].min())
        return pivot, stop
    if sub_stage in (
        SUB_STAGE_2_PIVOT_READY, SUB_STAGE_2_PULLBACK,  # new + legacy
        SUB_STAGE_2_IGNITION,    SUB_STAGE_2_EARLY,     # new + legacy
    ):
        pivot = _last_run_high(df, lookback=60)
    else:
        # CONTRACTION / MARKUP / legacy RUNNING.
        pivot = float(df["High"].iloc[-15:].max())
    stop = float(df["Low"].iloc[-10:].min())
    # Snap-to-52W-high: when the computed pivot is within 1% of the
    # 52W high, the older swing high IS the relevant breakout level —
    # the tiny gap is a single tick from earlier in the year, not a
    # different swing structure. Fixes WHA (15-bar 4.56 → snap to 52W
    # 4.60). When the computed pivot is meaningfully below the 52W
    # high (RBF: 4.20 vs 4.52, 7% gap), the recent local peak IS the
    # relevant pivot — don't snap.
    high_52w = float(df["High"].iloc[-min(252, len(df)):].max())
    if high_52w > pivot and pivot >= high_52w * 0.99:
        pivot = high_52w
    return pivot, stop


def compute_targets(df: pd.DataFrame, pivot: float, stop: float,
                    low_52w: float) -> tuple[float, float, float, float]:
    """3-point Fibonacci extension targets.

    Anchors:
      Pin1 = ZigZag-detected cycle low (start of current uptrend leg)
      Pin2 = 1st-leg peak (most recent prior H if >= 80% of current
             pivot, else current pivot itself)
      Pin3 = stop  (= 10-bar low, the recent pullback floor)

    Math:
      Range = Pin2 − Pin1
      Target 1.0   = Pin3 + Range            (not surfaced on card)
      Target 1.618 = Pin3 + 1.618 × Range

    Validated against user's chart-drawn Fib anchors:
      SPRC: Pin1 ฿4.28, Pin2 ฿8.20 (current); T1.618 ฿12.69 vs chart ฿12.70 ✓
      STECON: Pin1 ฿5.25, Pin2 ฿13.60 (current); T1.618 ฿24.39 vs chart ฿24.20 (within 1%) ✓
      HANA: Pin1 ฿15.10, Pin2 ~฿26.75 (1st-leg, 85% of current peak); closer match
      KOSPI: Pin1 ~52W low, Pin2 ฿6347 (1st-leg, 97% of current); matches chart
      KKP:  Pin1 ฿43.50 vs user's ฿62.75 (user picks LATER cycle restart)
      GULF: user picks Pin2 PROJECTED above current high (manual override)

    Returns (target_1, target_1618, fib_start, fib_pivot).
    """
    if pivot <= 0:
        return 0.0, 0.0, 0.0, 0.0
    fib_start = _zigzag_cycle_low(df, threshold=0.20)
    if fib_start <= 0 or fib_start >= pivot:
        # ZigZag found nothing useful; fall back to 52W low.
        fib_start = low_52w
    if fib_start <= 0 or fib_start >= pivot:
        return 0.0, 0.0, 0.0, 0.0
    fib_pivot = _zigzag_first_leg_peak(df, current_pivot=pivot, similarity=0.80)
    if fib_pivot <= fib_start:
        fib_pivot = pivot
    range_ = fib_pivot - fib_start
    return stop + range_, stop + 1.618 * range_, fib_start, fib_pivot


# ─── Stage classification ──────────────────────────────────────────────────────

def classify_stage(df: pd.DataFrame) -> int:
    """Layer 1 — general regime classification. Returns 1, 2, 3, or 4.

    6-path priority pipeline:
      1. Stage 2 strict      — canonical Minervini template
      2. Stage 4              — explicit downtrend
      3. Stage 2 ignition    — fresh-breakout-from-flat-base override
                                (catches GLORY / GPI when SMA200 ROC
                                hasn't caught up yet)
      4. Stage 2 entrenched  — structurally-intact mid-pullback Stage 2
                                (catches KKP / KCG when SMA200 ROC has
                                temporarily flattened)
      5. Stage 3 strict      — explicit topping (SMA20 dead-cross +
                                close < SMA50). Drops the ROC-flat
                                gate that previously caught fresh
                                breakouts.
      6. Stage 1              — default fallback (everything else)

    Each Stage 2 path enforces the canonical 52W proximity gates so
    dead cats (down 50%+ from peak) don't slip into the buy zone.
    """
    if len(df) < MIN_ROWS:
        return 1

    close = df["Close"]
    high = df["High"]

    sma20 = _sma(close, 20)
    sma50 = _sma(close, 50)
    sma200 = _sma(close, 200)

    c = float(close.iloc[-1])
    c_prev = float(close.iloc[-2]) if len(close) > 1 else c
    today_chg = (c - c_prev) / c_prev * 100.0 if c_prev else 0.0
    s20 = float(sma20.iloc[-1]) if not sma20.dropna().empty else float("nan")
    s50 = float(sma50.iloc[-1])
    s200 = float(sma200.iloc[-1])

    if any(np.isnan(x) for x in [c, s50, s200]):
        return 1

    roc200 = _sma_roc(close, window=200, lookback=20)
    lookback = min(252, len(df))
    high_52w = float(high.iloc[-lookback:].max())
    low_52w = float(close.iloc[-lookback:].min())

    # Shared 52W proximity gates — used by every Stage 2 path so dead
    # cats (down 50%+ from peak) don't slip into Stage 2.
    #
    # The low-gate has a 1e-6 tolerance for float-precision edge cases
    # (yfinance returns prices as float32 round-tripped to float64,
    # which can land 1e-7 above an exact threshold and falsely fail
    # `>=`). It also has a tight-range exemption: if the 52W range is
    # narrow (high - low < 30% of low), the stock isn't a dead cat by
    # definition and the 25%-above-low rule shouldn't exclude it.
    # Catches narrow-range Stage 2 stocks (e.g. KCG / GPI) that pass
    # every structural condition but barely miss the 1.25× gate.
    range_52w = (high_52w - low_52w) / low_52w if low_52w > 0 else 0.0
    above_52w_low_x125 = (
        c + 1e-6 >= low_52w * 1.25       # float tolerance
        or range_52w < 0.30              # tight 52W range exemption
    )
    within_52w_high_x75 = c >= high_52w * 0.75

    # ── Path 1: Stage 2 strict — canonical Minervini template ──
    # Minervini's Template of Excellence requires price ABOVE SMA50 in
    # addition to SMA50 > SMA200. Without `c > s50`, weakening Stage 2
    # stocks (close drifted below SMA50) get held in Stage 2 with the
    # MARKUP sub-stage default — but they're really pullbacks or
    # entering Stage 3.
    if (s50 > s200 and c > s50 and c > s200 and roc200 > 0
            and above_52w_low_x125 and within_52w_high_x75):
        return 2

    # ── Path 2: Stage 4 — confirmed downtrend ──
    # Five-gate alignment for genuine downtrend:
    #   1. c < s50    — price below mid-term MA
    #   2. c < s200   — price below long-term MA
    #   3. s50 < s200 — alignment broken (death-cross zone)
    #   4. ROC < 0    — long-term slope down
    # Without `c < s50`, stocks recovering past the mid-term MA (JMT,
    # TIDLOR — both with +20d return > +18%) get held in Stage 4
    # even though their structure is mid-recovery. With it, those
    # stocks correctly fall through to Stage 1 (basing).
    if c < s50 and c < s200 and s50 < s200 and roc200 < 0:
        return 4

    # ── Path 3: Stage 2 ignition override — fresh breakout from flat
    # base. Two forms:
    #   (a) Post-cross: golden cross has occurred within last 30 bars
    #       + price > SMA200 + close > SMA20 + decisive breakout
    #       (new 52W high OR +5% kick day) + 52W gates.
    #   (b) Pre-cross blast: SMA50 hasn't crossed SMA200 yet because
    #       the long flat base drags SMA200 down faster than SMA50
    #       can catch up — but price is LEADING all 3 MAs (close >
    #       SMA20 AND close > SMA50 AND close > SMA200) on a
    #       decisive breakout day. Catches "GLORY-class" stocks
    #       where the rally is so explosive (+170% from base in
    #       weeks) that SMA50 hasn't yet overtaken SMA200.
    recent_cross   = _cross_within(sma50, sma200, n=30, direction="up")
    above_sma20    = (not np.isnan(s20)) and c > s20
    new_52w_high   = high_52w > 0 and c >= high_52w * 0.999
    strong_today   = today_chg > 5.0
    decisive_kick  = new_52w_high or strong_today
    # Form (a) — post-cross
    if (recent_cross and c > s200 and above_sma20
            and above_52w_low_x125 and decisive_kick):
        return 2
    # Form (b) — pre-cross blast (SMA50 < SMA200 still, price-led)
    if (c > s200 and c > s50 and above_sma20
            and above_52w_low_x125 and within_52w_high_x75
            and decisive_kick):
        return 2

    # ── Path 4: Stage 2 entrenched — structurally-intact mid-pullback.
    # SMA stack still bullish, 52W gates still pass — only ROC has
    # slipped. Catches KCG-class stocks where a shallow pullback briefly
    # flattens SMA200's slope but the stock is clearly still in Stage 2
    # territory.
    #
    # Includes a 3% tolerance below SMA50 (c >= s50 × 0.97) so stocks
    # in mild Stage-2 weakening (close briefly dipped below SMA50 but
    # still very close to it) stay in Stage 2 — the `stage_weakening`
    # flag captures this nuance. STECON-class stocks (c=11.50,
    # s50=11.66 = 0.986 ratio) keep their Stage 2 + weakening label
    # rather than falling to Stage 1 BASE which would lose the
    # uptrend-context.
    if (s50 > s200 and c >= s50 * 0.97 and c > s200
            and above_52w_low_x125 and within_52w_high_x75):
        return 2

    # ── Path 5: Stage 3 strict — explicit topping ──
    # Five conditions, all required (each one closes a misfire surface
    # that surfaced in production via real-stock spot-checks):
    #   1. SMA20 dead-crossed SMA50 within last 20 bars (rolling over).
    #   2. Close MEANINGFULLY below SMA50 — at least 2% below. Float-
    #      precision guard so stocks AT the SMA50 don't trip on
    #      arithmetic noise (SINGER: close=5.30, SMA50=5.3041).
    #   3. Close still within 25% of 52W high (peak is RECENT). Mirrors
    #      Stage 2's c >= high*0.75 gate inverted.
    #   4. 52W high was set within the last 60 bars. Topping means
    #      'just peaked and now rolling over' — if the high is from
    #      months ago and the SMA20/50 chop is just normal in-base
    #      noise, this is BASING, not TOPPING. Without this gate,
    #      KTC (high 239 bars ago), SINGER (150), COM7 (129) get
    #      mis-flagged as Stage 3 because of late-cycle chop.
    #   5. Recent 20-day return < +5%. Topping = currently rolling
    #      over. Stocks rallying back >5% in 20 days are recovering,
    #      not topping (BH +8.3%, PTG +8.6% bug).
    high_idx_recent = False
    try:
        # bars_since_high = position from end of the 52W-high bar
        lookback_n = min(252, len(df))
        high_series = high.iloc[-lookback_n:]
        # idxmax() returns the timestamp; convert to integer position
        peak_pos = high_series.values.argmax()
        bars_since_peak = (lookback_n - 1) - peak_pos
        high_idx_recent = bars_since_peak <= 60
    except Exception:
        high_idx_recent = False

    # 20-day return — guards against bouncing-back stocks getting
    # tagged Stage 3 when they're actually recovering.
    ret_20 = 0.0
    if len(close) > 21:
        c20 = float(close.iloc[-21])
        ret_20 = (c - c20) / c20 * 100.0 if c20 > 0 else 0.0

    if (_cross_within(sma20, sma50, n=20, direction="down")
            and c < s50 * 0.98
            and c >= high_52w * 0.75
            and high_idx_recent
            and ret_20 < 5.0):
        return 3

    # ── Path 6: Stage 1 default fallback ──
    return 1


# ─── Pattern detection ─────────────────────────────────────────────────────────

def detect_pattern(df: pd.DataFrame, stage: int, ath_override: Optional[float] = None,
                   is_index: bool = False) -> tuple[str, dict]:
    """
    Detect the most significant pattern for a stock or index.

    Returns (pattern_name, details_dict).
    ath_override: true all-time high from Firestore cache; falls back to window max if None.
    is_index: when True, drops the 1.4× volume gate on breakout / breakout_attempt.
              Index volume is aggregate (sum of constituents) and doesn't carry the
              same directional confirmation signal as individual-stock volume —
              relying on it gates legitimate price-only breakouts that are
              perfectly visible on a chart. Stocks keep the volume gate.
    """
    if len(df) < 60:
        return ("going_down" if stage == 4 else "consolidating"), {}

    close = df["Close"]
    high = df["High"]
    low = df["Low"]
    volume = df["Volume"]

    c = float(close.iloc[-1])
    vol_avg_20 = float(_vol_avg(df).iloc[-1])
    volume_ratio = float(volume.iloc[-1]) / vol_avg_20 if vol_avg_20 > 0 else 1.0

    # All-time high: take the max of the cached historical ATH (from Firestore via
    # ath_override) AND the current 1Y window high. This lets the analyzer recognise
    # new unadjusted highs set inside the scan window even if the Firestore cache
    # hasn't been refreshed by /sync_ath, and keeps old peaks that are older than
    # the window (e.g. pre-2024 for a 1Y scan). ATH is monotonically non-decreasing,
    # so max() is the correct combinator.
    window_high = float(high.max())
    if ath_override is not None and ath_override > 0:
        ath = max(float(ath_override), window_high)
    else:
        ath = window_high

    if stage == 4:
        return "going_down", {}

    if stage == 1:
        # Check if consolidating with contracting volatility
        atr_now = float(_atr(df).iloc[-1])
        atr_20d_ago = float(_atr(df).iloc[-21]) if len(df) > 21 else atr_now
        contracting = atr_now < atr_20d_ago * 0.9
        return ("consolidating", {"atr_contracting": contracting})

    if stage in (2, 3):
        # ── VCP Detection ──
        vcp_result, vcp_details = _detect_vcp(df)
        if vcp_result == "vcp_low_cheat":
            return "vcp_low_cheat", vcp_details
        if vcp_result == "vcp":
            return "vcp", vcp_details

        # ── Breakout Detection ──
        # vol_threshold: 0.0 for indexes (price-only), 1.4× for stocks.
        vol_threshold = 0.0 if is_index else 1.4
        pivot_high = float(high.iloc[-52:-1].max()) if len(df) > 52 else float(high.iloc[:-1].max())
        is_breakout = c > pivot_high and volume_ratio >= vol_threshold

        if is_breakout:
            is_ath = c >= ath * 0.99  # within 1% of all-time high counts
            details = {
                "pivot_high": round(pivot_high, 2),
                "volume_ratio": round(volume_ratio, 2),
                "is_ath": is_ath,
            }
            return ("ath_breakout" if is_ath else "breakout"), details

        # ── Breakout Attempt ──
        # Any of the last 3 bars had HIGH > 52-bar pivot on qualifying volume
        # (≥1.4× for stocks, any vol for indexes), AND current close is still
        # within 3% of that attempt's high (price hasn't fully reversed away
        # from resistance). Captures the in-progress "broke out intraday,
        # now pausing near resistance" state — a signal that the strict
        # close-based rule flashes for a single bar and then drops, which is
        # too brittle for scheduled-scan alerting.
        vol_avg = _vol_avg(df)
        attempt = None  # {"bar": int-from-end, "high": float, "vol_ratio": float}
        for k in range(1, 4):  # check today (k=1), yesterday (k=2), 2-days-ago (k=3)
            if k > len(df):
                break
            bar_high = float(high.iloc[-k])
            bar_vol = float(volume.iloc[-k])
            bar_vol_avg = float(vol_avg.iloc[-k]) if not np.isnan(vol_avg.iloc[-k]) else 0.0
            bar_vr = bar_vol / bar_vol_avg if bar_vol_avg > 0 else 0.0
            # Pivot as of bar k's open: max High in the 52 bars ending the day before
            pivot_slice_end = len(df) - k  # exclusive
            pivot_slice_start = max(0, pivot_slice_end - 52)
            if pivot_slice_end <= pivot_slice_start:
                continue
            bar_pivot = float(high.iloc[pivot_slice_start:pivot_slice_end].max())
            if bar_high > bar_pivot and bar_vr >= vol_threshold:
                attempt = {"bars_ago": k - 1, "high": bar_high,
                           "pivot": bar_pivot, "vol_ratio": bar_vr}
                break  # take the most recent qualifying attempt

        if attempt and c >= attempt["high"] * 0.97:  # close still within 3% of attempt peak
            details = {
                "pivot_high": round(attempt["pivot"], 2),
                "attempt_high": round(attempt["high"], 2),
                "bars_ago": attempt["bars_ago"],
                "volume_ratio": round(attempt["vol_ratio"], 2),
                "close_from_attempt_pct": round((c - attempt["high"]) / attempt["high"] * 100, 2),
                "is_ath_touch": attempt["high"] >= ath * 0.99,
            }
            return "breakout_attempt", details

        return "consolidating", {}

    return "consolidating", {}


def _detect_vcp(df: pd.DataFrame) -> tuple[str, dict]:
    """
    Detect Volatility Contraction Pattern.

    Looks for 3+ successive price contractions over the last 20 weeks
    where each contraction is shallower than the previous, and
    volume dries up progressively.

    Returns ("vcp", details), ("vcp_low_cheat", details), or ("none", {}).
    """
    if len(df) < 100:
        return "none", {}

    # Work on the last 100 days
    window = df.iloc[-100:]
    close = window["Close"].values
    high = window["High"].values
    low = window["Low"].values
    volume = window["Volume"].values

    # Find local swing highs and lows using a simple 5-bar pivot
    swing_highs = []
    swing_lows = []
    for i in range(5, len(close) - 5):
        if high[i] == max(high[i - 5:i + 6]):
            swing_highs.append((i, float(high[i])))
        if low[i] == min(low[i - 5:i + 6]):
            swing_lows.append((i, float(low[i])))

    if len(swing_highs) < 2 or len(swing_lows) < 2:
        return "none", {}

    # Build contraction pairs (high → subsequent low)
    contractions = []
    for i, (hi_idx, hi_val) in enumerate(swing_highs[:-1]):
        # Find the swing low that follows this high
        subsequent_lows = [(li, lv) for li, lv in swing_lows if li > hi_idx]
        if not subsequent_lows:
            continue
        lo_idx, lo_val = subsequent_lows[0]
        depth_pct = (hi_val - lo_val) / hi_val * 100
        avg_vol = float(np.mean(volume[hi_idx:lo_idx + 1])) if lo_idx > hi_idx else 0.0
        contractions.append({
            "hi_idx": hi_idx,
            "lo_idx": lo_idx,
            "hi": hi_val,
            "lo": lo_val,
            "depth_pct": depth_pct,
            "avg_vol": avg_vol,
        })

    # Open-ended (unclosed) final contraction. The 5-bar pivot detector
    # can't mark a swing in the last 5 bars, so when price has rallied
    # past the most recent confirmed swing high and is now pulling back,
    # the current consolidation is invisible to the closed-contraction
    # list. Synthesise a final "unclosed" contraction from (post-swing
    # max high) → current close to capture in-progress tight pullbacks.
    # Only fires when there's been a genuine higher high after the last
    # confirmed swing AND the close is pulling back from it.
    if swing_highs:
        last_sh_idx = swing_highs[-1][0]
        last_sh_val = swing_highs[-1][1]
        post_slice = high[last_sh_idx + 1:]
        if len(post_slice) >= 2:
            post_peak_offset = int(np.argmax(post_slice))
            post_peak = float(post_slice[post_peak_offset])
            post_peak_idx = last_sh_idx + 1 + post_peak_offset
            last_close = float(close[-1])
            if post_peak > last_sh_val and post_peak > last_close:
                current_bar = len(close) - 1
                depth_pct = (post_peak - last_close) / post_peak * 100
                avg_vol = (float(np.mean(volume[post_peak_idx:current_bar + 1]))
                           if current_bar > post_peak_idx else float(volume[post_peak_idx]))
                contractions.append({
                    "hi_idx": post_peak_idx,
                    "lo_idx": current_bar,
                    "hi": post_peak,
                    "lo": last_close,
                    "depth_pct": depth_pct,
                    "avg_vol": avg_vol,
                    "unclosed": True,
                })

    if len(contractions) < 3:
        return "none", {}

    # Check last 3 contractions for decreasing depth and volume
    last3 = contractions[-3:]
    depths_decreasing = all(
        last3[i]["depth_pct"] > last3[i + 1]["depth_pct"]
        for i in range(len(last3) - 1)
    )
    vols_decreasing = all(
        last3[i]["avg_vol"] > last3[i + 1]["avg_vol"] * 0.9
        for i in range(len(last3) - 1)
    )

    if not (depths_decreasing and vols_decreasing):
        return "none", {}

    details = {
        "contraction_count": len(contractions),
        "final_depth_pct": round(last3[-1]["depth_pct"], 1),
        "depths": [round(c["depth_pct"], 1) for c in last3],
    }

    # VCP Low Cheat: price currently at or near the latest contraction low
    current_close = float(close[-1])
    final_lo = last3[-1]["lo"]
    near_low = current_close <= final_lo * 1.03  # within 3% of the final pivot low
    current_vol = float(volume[-1])
    avg_vol_20 = float(np.mean(volume[-20:]))
    vol_dry = current_vol < avg_vol_20 * 0.7  # volume drying up

    if near_low and vol_dry:
        details["near_pivot_low"] = True
        return "vcp_low_cheat", details

    return "vcp", details


# ─── Strength score ────────────────────────────────────────────────────────────

def _strength_score(df: pd.DataFrame, stage: int, pattern: str, volume_ratio: float,
                    sub_stage: str = "") -> float:
    """Composite score 0–100, sub-stage aware.

    When sub_stage is set (current scans), the sub-stage score map drives
    the primary classification component. When sub_stage is empty (old
    Firestore docs loaded mid-migration), falls back to the legacy
    stage+pattern lookup so historical scores stay sensible.

    Rewards (sub-stage scoring):
      • STAGE_2_EARLY    +65   (fresh entry, biggest alpha)
      • STAGE_2_PULLBACK +55   (setup forming)
      • STAGE_2_RUNNING  +50   (already running, less alpha)
      • STAGE_1_PREP     +30   (watchlist)
      • STAGE_3_VOLATILE +20   (defensive — trim)
      • STAGE_1_BASE     +10
      • STAGE_3_DIST_DIST +5   (no buy)
      • STAGE_4_*         +0
      Plus volume bonus (max +15) and 52W proximity (max +20).
    """
    score = 0.0

    sub_stage_scores = {
        # New Stage 2 sub-stages — PIVOT_READY ranks highest (actionable
        # entry with a computed pivot trigger), followed by IGNITION
        # (fresh momentum), then MARKUP (already running), CONTRACTION
        # (watching). OVEREXTENDED scores low to deprioritise warnings
        # in ranked lists like 'top breakout' / 'best score'.
        SUB_STAGE_2_PIVOT_READY:    65,
        SUB_STAGE_2_IGNITION:       60,
        SUB_STAGE_2_MARKUP:         50,
        SUB_STAGE_2_CONTRACTION:    45,
        SUB_STAGE_2_OVEREXTENDED:   10,
        # Legacy Stage 2 (kept so old Firestore docs score sensibly
        # mid-migration; deletable once every doc is re-scanned).
        SUB_STAGE_2_EARLY:          60,   # ≈ IGNITION
        SUB_STAGE_2_PULLBACK:       55,   # ≈ CONTRACTION ∪ PIVOT_READY
        SUB_STAGE_2_RUNNING:        50,   # ≈ MARKUP
        # Stage 1, 3, 4 sub-stages — unchanged
        SUB_STAGE_1_PREP:      30,
        SUB_STAGE_3_VOLATILE:  20,
        SUB_STAGE_1_BASE:      10,
        SUB_STAGE_3_DIST_DIST:  5,
        SUB_STAGE_4_BREAKDOWN:  0,
        SUB_STAGE_4_DOWNTREND:  0,
    }
    if sub_stage and sub_stage in sub_stage_scores:
        score += sub_stage_scores[sub_stage]
    else:
        # Legacy fallback for Firestore docs without sub_stage.
        stage_scores = {1: 10, 2: 40, 3: 15, 4: 0}
        score += stage_scores.get(stage, 0)
        pattern_scores = {
            "ath_breakout":     25,
            "breakout":         20,
            "vcp_low_cheat":    18,
            "vcp":              15,
            "breakout_attempt": 12,
            "consolidating":     5,
            "going_down":        0,
        }
        score += pattern_scores.get(pattern, 0)

    # Volume bonus (capped at 15) — applies in both branches.
    vol_bonus = min(15, (volume_ratio - 1.0) * 10) if volume_ratio > 1.0 else 0
    score += vol_bonus

    # RS: proximity to 52-week high — applies in both branches.
    if len(df) >= 20:
        high_52w = float(df["High"].iloc[-252:].max()) if len(df) >= 252 else float(df["High"].max())
        c = float(df["Close"].iloc[-1])
        rs_score = (c / high_52w) * 20
        score += rs_score

    return round(min(100.0, score), 1)


# ─── Full scan ─────────────────────────────────────────────────────────────────

def scan_stock(symbol: str, df: pd.DataFrame, ath_override: Optional[float] = None,
               is_index: bool = False) -> Optional[StockSignal]:
    """Analyse a single stock or index and return a StockSignal, or None if
    data is insufficient. is_index relaxes the breakout volume gate — see
    detect_pattern for the rationale."""
    if df is None or len(df) < 60:
        return None

    # Freshness gate: skip stocks whose latest candle is too old (suspended /
    # untradeable). Compared against current Bangkok calendar date so we don't
    # accidentally filter market-closed weekends — the gate is days, not hours.
    try:
        last_candle = pd.Timestamp(df.index[-1]).normalize()
        today = pd.Timestamp.now().normalize()
        if (today - last_candle).days > MAX_CANDLE_STALENESS_DAYS:
            return None
    except Exception:
        pass  # if index isn't timestamp-like, let downstream error surface normally

    close = df["Close"]
    high = df["High"]
    volume = df["Volume"]

    c = float(close.iloc[-1])
    c_prev = float(close.iloc[-2]) if len(df) > 1 else c
    change_pct = (c - c_prev) / c_prev * 100 if c_prev else 0.0

    vol_now = int(volume.iloc[-1])
    vol_avg_20 = float(_vol_avg(df).iloc[-1])
    volume_ratio = vol_now / vol_avg_20 if vol_avg_20 > 0 else 1.0

    stage = classify_stage(df)
    sub_stage = classify_sub_stage(df, stage)
    pattern, bp_details = detect_pattern(df, stage, ath_override=ath_override, is_index=is_index)

    lookback = min(252, len(df))
    high_52w = float(high.iloc[-lookback:].max())
    low_52w = float(close.iloc[-lookback:].min())

    sma10 = float(_sma(close, 10).iloc[-1]) if len(df) >= 10 else float("nan")
    sma20 = float(_sma(close, 20).iloc[-1]) if len(df) >= 20 else float("nan")
    sma50 = float(_sma(close, 50).iloc[-1]) if len(df) >= 50 else float("nan")
    sma100 = float(_sma(close, 100).iloc[-1]) if len(df) >= 100 else float("nan")
    sma150 = float(_sma(close, 150).iloc[-1]) if len(df) >= 150 else float("nan")
    sma200 = float(_sma(close, 200).iloc[-1]) if len(df) >= 200 else float("nan")
    sma200_roc20 = _sma_roc(close, window=200, lookback=20)

    # Stage-2 weakening: Minervini template still passes but short-term
    # momentum has rolled over below SMA50. Retained alongside sub_stage
    # for backward compat with existing cards/e2e.
    stage_weakening = (stage == 2 and not np.isnan(sma50) and c < sma50)

    score = _strength_score(df, stage, pattern, volume_ratio, sub_stage=sub_stage)

    # Pivot-point: buy trigger + invalidation stop, computed only for the
    # actionable buy-side sub-stages (PREP / EARLY / RUNNING / PULLBACK).
    pivot_price, pivot_stop = compute_pivot(df, sub_stage)
    # Fibonacci 3-point extension targets: T1.0 + T1.618 from
    # (Pin1=cycle_low → Pin2=1st-leg peak, projected from Pin3=stop).
    # Pin2 may be earlier than current pivot when an earlier H qualifies.
    target_1, target_1618, fib_start, fib_pivot = compute_targets(
        df, pivot_price, pivot_stop, low_52w)

    # Margin tier from Krungsri's marginable list (loaded once at startup
    # from data_static/margin_securities.json). 0 means non-marginable.
    # Indexes (is_index=True) and non-SET tickers don't have margin data;
    # the lookup returns 0 for unknown symbols so the field stays 0.
    try:
        from data import get_margin_im_pct as _mp
        margin_im_pct = _mp(symbol)
    except Exception:
        margin_im_pct = 0

    # Risk/reward calculations
    atr_series = _atr(df)
    atr_val = float(atr_series.iloc[-1]) if len(df) >= 14 and not np.isnan(atr_series.iloc[-1]) else 0.0
    trade_value_m = round(float(c * vol_now) / 1_000_000, 2)
    pct_from_high = round((c / high_52w - 1) * 100, 2) if high_52w > 0 else 0.0
    stop_loss_price = round(c - 1.5 * atr_val, 2) if atr_val > 0 else 0.0
    risk_per_share = c - stop_loss_price if stop_loss_price > 0 else 0.0
    target = round(c + 2 * risk_per_share, 2) if risk_per_share > 0 else 0.0
    bo_count = count_breakouts_1y(df)

    from datetime import datetime
    import pytz
    bkk = pytz.timezone("Asia/Bangkok")
    now_str = datetime.now(bkk).isoformat()

    try:
        data_date = pd.Timestamp(df.index[-1]).strftime("%Y-%m-%d")
    except Exception:
        data_date = ""

    return StockSignal(
        symbol=symbol,
        name=symbol,  # Thai name could be enriched later via SET Trade API metadata
        stage=stage,
        pattern=pattern,
        close=round(c, 2),
        change_pct=round(change_pct, 2),
        volume=vol_now,
        volume_ratio=round(volume_ratio, 2),
        sma10=round(sma10, 2) if not np.isnan(sma10) else 0.0,
        sma20=round(sma20, 2) if not np.isnan(sma20) else 0.0,
        sma50=round(sma50, 2) if not np.isnan(sma50) else 0.0,
        sma100=round(sma100, 2) if not np.isnan(sma100) else 0.0,
        sma150=round(sma150, 2) if not np.isnan(sma150) else 0.0,
        sma200=round(sma200, 2) if not np.isnan(sma200) else 0.0,
        sma200_roc20=round(sma200_roc20, 4),
        high_52w=round(high_52w, 2),
        low_52w=round(low_52w, 2),
        strength_score=score,
        tradingview_url=tradingview_url(symbol),
        scanned_at=now_str,
        breakout_details=bp_details,
        atr=round(atr_val, 4),
        trade_value_m=trade_value_m,
        pct_from_52w_high=pct_from_high,
        stop_loss=stop_loss_price,
        target_price=target,
        breakout_count_1y=bo_count,
        data_date=data_date,
        sub_stage=sub_stage,
        pivot_price=round(pivot_price, 2),
        pivot_stop=round(pivot_stop, 2),
        target_1=round(target_1, 2),
        target_1618=round(target_1618, 2),
        fib_start=round(fib_start, 2),
        fib_pivot=round(fib_pivot, 2),
        margin_im_pct=margin_im_pct,
        stage_weakening=bool(stage_weakening),
    )


def count_breakouts_1y(df: pd.DataFrame) -> int:
    """Count distinct 52-week breakout events in the past year of price data."""
    if len(df) < 60:
        return 0
    window = df.iloc[-252:]
    close = window["Close"]
    vol = window["Volume"]
    vol_avg = vol.rolling(20, min_periods=20).mean()
    prev_resistance = close.shift(1).rolling(52, min_periods=52).max()
    breakout_days = (close > prev_resistance) & (vol > vol_avg * 1.4)
    # Count transitions from False→True (distinct events, not consecutive days)
    transitions = breakout_days.astype(int).diff().clip(lower=0)
    return int(transitions.sum())


def analyze_index(df: pd.DataFrame, name: str) -> dict:
    """
    Stock-like analysis for an index: MACD, RSI, MA50/150/200, stage, 52W range.
    Returns an enriched dict compatible with the existing {close, change_pct} schema.
    """
    close = df["Close"].astype(float).dropna()
    if len(close) < 30:
        return {"name": name, "close": 0.0, "change_pct": 0.0}

    # Moving averages
    ma50 = close.rolling(50, min_periods=20).mean()
    ma150 = close.rolling(150, min_periods=50).mean()
    ma200 = close.rolling(200, min_periods=50).mean()

    # MACD (12/26/9)
    ema12 = close.ewm(span=12, adjust=False).mean()
    ema26 = close.ewm(span=26, adjust=False).mean()
    macd_line = ema12 - ema26
    macd_signal_line = macd_line.ewm(span=9, adjust=False).mean()
    macd_hist = macd_line - macd_signal_line

    # RSI (14, Wilder)
    delta = close.diff()
    gain = delta.clip(lower=0).ewm(alpha=1 / 14, adjust=False).mean()
    loss = (-delta.clip(upper=0)).ewm(alpha=1 / 14, adjust=False).mean()
    rsi_series = 100 - (100 / (1 + gain / loss.replace(0, np.nan)))

    # 52-week high / low (use up to 252 bars)
    window_52w = min(252, len(close))
    high_52w = float(close.iloc[-window_52w:].max())
    low_52w = float(close.iloc[-window_52w:].min())

    # Latest values
    cur_close = float(close.iloc[-1])
    prev_close = float(close.iloc[-2]) if len(close) > 1 else cur_close
    change_pct = round((cur_close - prev_close) / prev_close * 100, 2) if prev_close else 0.0

    def _last(series):
        v = series.iloc[-1]
        return float(v) if not np.isnan(v) else 0.0

    cur_ma50 = _last(ma50)
    cur_ma150 = _last(ma150)
    cur_ma200 = _last(ma200)
    cur_macd = _last(macd_line)
    cur_signal = _last(macd_signal_line)
    cur_hist = _last(macd_hist)
    prev_hist = float(macd_hist.iloc[-2]) if len(macd_hist) > 1 else 0.0
    cur_rsi = _last(rsi_series) or 50.0

    above_ma50 = cur_close > cur_ma50 if cur_ma50 else None
    above_ma150 = cur_close > cur_ma150 if cur_ma150 else None
    above_ma200 = cur_close > cur_ma200 if cur_ma200 else None

    # MA200 rising (compare last 20 bars)
    ma200_rising = bool(cur_ma200 > float(ma200.iloc[-20])) if len(ma200) >= 20 and cur_ma200 else False

    # Minervini stage for the index
    stage = classify_stage(df) if len(close) >= 200 else None

    # Pattern detection for indexes — uses the same detect_pattern logic
    # the stocks use, but with is_index=True so the volume gate is dropped
    # (index volume is aggregate, not directional). Falls back to None for
    # short histories where stage couldn't be classified.
    pattern: Optional[str] = None
    pattern_details: dict = {}
    if stage is not None:
        try:
            pattern, pattern_details = detect_pattern(df, stage, is_index=True)
        except Exception as exc:
            logger.warning("analyze_index(%s): pattern detection failed: %s", name, exc)

    pct_from_52w_high = round((cur_close / high_52w - 1) * 100, 1) if high_52w else 0.0

    macd_bullish_cross = cur_hist > 0 and prev_hist <= 0
    macd_bearish_cross = cur_hist < 0 and prev_hist >= 0

    # Thai implication text
    parts = []
    if stage is not None:
        stage_thai = {1: "Stage 1 (Basing)", 2: "Stage 2 (Uptrend)", 3: "Stage 3 (Topping)", 4: "Stage 4 (Downtrend)"}
        parts.append(stage_thai.get(stage, f"Stage {stage}"))
    if above_ma200 is True:
        parts.append("เหนือ MA200 ✓")
    elif above_ma200 is False:
        parts.append("ต่ำกว่า MA200 ✗")
    if macd_bullish_cross:
        parts.append("🟢 MACD Cross Up")
    elif macd_bearish_cross:
        parts.append("🔴 MACD Cross Down")
    elif cur_macd > cur_signal:
        parts.append("MACD เป็นบวก")
    else:
        parts.append("MACD เป็นลบ")
    if cur_rsi > 70:
        parts.append(f"⚠️ RSI Overbought ({cur_rsi:.0f})")
    elif cur_rsi < 30:
        parts.append(f"🟢 RSI Oversold ({cur_rsi:.0f})")
    # Append pattern badge after the momentum read so the implication
    # string ends with the user-facing call ('Breakout', 'Breakout
    # Attempt'). 'consolidating' / 'going_down' are noise here — only
    # surface the actionable patterns.
    if pattern in ("breakout", "ath_breakout", "breakout_attempt", "vcp", "vcp_low_cheat"):
        pattern_thai = {
            "breakout": "🚀 Breakout",
            "ath_breakout": "🏆 ATH Breakout",
            "breakout_attempt": "⚡ Breakout Attempt",
            "vcp": "🔄 VCP",
            "vcp_low_cheat": "🔄 VCP Low Cheat",
        }
        parts.append(pattern_thai.get(pattern, pattern))

    return {
        "name": name,
        "close": round(cur_close, 2),
        "change_pct": change_pct,
        "prev_close": round(prev_close, 2),
        # MAs
        "ma50": round(cur_ma50, 2),
        "ma150": round(cur_ma150, 2),
        "ma200": round(cur_ma200, 2),
        "above_ma50": above_ma50,
        "above_ma150": above_ma150,
        "above_ma200": above_ma200,
        "ma200_rising": ma200_rising,
        # Stage + pattern (pattern is index-aware: no volume gate)
        "stage": stage,
        "pattern": pattern,
        "breakout_details": pattern_details,
        # 52W range
        "high_52w": round(high_52w, 2),
        "low_52w": round(low_52w, 2),
        "pct_from_52w_high": pct_from_52w_high,
        # MACD
        "macd_line": round(cur_macd, 4),
        "macd_signal": round(cur_signal, 4),
        "macd_hist": round(cur_hist, 4),
        "macd_bullish_cross": macd_bullish_cross,
        "macd_bearish_cross": macd_bearish_cross,
        # RSI
        "rsi": round(cur_rsi, 1),
        "trend": "uptrend" if above_ma200 else ("downtrend" if above_ma200 is False else "unknown"),
        "implication": " | ".join(parts),
    }


def run_full_scan(
    period: str = "1y",
    ath_cache: Optional[dict[str, float]] = None,
) -> tuple[list[StockSignal], dict[str, "pd.DataFrame"]]:
    """
    Fetch all stocks and run Minervini analysis on each.

    Returns (signals sorted by strength_score desc, all_data dict for index extraction).
    """
    logger.info("Starting full scan...")
    all_data = fetch_all_stocks(period=period)
    signals: list[StockSignal] = []

    for symbol, df in all_data.items():
        ath_override = ath_cache.get(symbol) if ath_cache else None
        sig = scan_stock(symbol, df, ath_override=ath_override)
        if sig:
            signals.append(sig)
        else:
            logger.debug("Skipping %s — insufficient data", symbol)

    signals.sort(key=lambda s: s.strength_score, reverse=True)
    logger.info("Scan complete: %d stocks analysed", len(signals))
    return signals, all_data


def compute_market_breadth(
    signals: list[StockSignal],
    index_df: Optional["pd.DataFrame"] = None,
) -> MarketBreadth:
    """Aggregate market-wide breadth metrics from a list of StockSignals."""
    from datetime import datetime
    import pytz
    bkk = pytz.timezone("Asia/Bangkok")
    now_str = datetime.now(bkk).isoformat()

    total = len(signals)
    stage_counts = {1: 0, 2: 0, 3: 0, 4: 0}
    advancing = declining = unchanged = 0
    new_highs = new_lows = 0
    breakout_count = vcp_count = 0
    above_ma200 = below_ma200 = 0

    for s in signals:
        stage_counts[int(s.stage)] = stage_counts.get(int(s.stage), 0) + 1

        if s.change_pct > 0.1:
            advancing += 1
        elif s.change_pct < -0.1:
            declining += 1
        else:
            unchanged += 1

        if s.close >= s.high_52w * 0.99:
            new_highs += 1
        if s.close <= s.low_52w * 1.01:
            new_lows += 1

        if s.pattern in ("breakout", "ath_breakout"):
            breakout_count += 1
        if s.pattern in ("vcp", "vcp_low_cheat"):
            vcp_count += 1

        if s.sma200 > 0:
            if s.close > s.sma200:
                above_ma200 += 1
            else:
                below_ma200 += 1

    stage2_pct = round(stage_counts[2] / total * 100, 1) if total else 0.0
    above_ma200_pct = round(above_ma200 / total * 100, 1) if total else 0.0

    # Extract SET index level and daily change from provided DataFrame
    set_close = 0.0
    set_change_pct = 0.0
    if index_df is not None and len(index_df) >= 2:
        try:
            set_close = round(float(index_df["Close"].iloc[-1]), 2)
            prev = float(index_df["Close"].iloc[-2])
            set_change_pct = round((set_close - prev) / prev * 100, 2) if prev else 0.0
        except Exception:
            pass

    return MarketBreadth(
        scanned_at=now_str,
        total_stocks=total,
        stage1_count=stage_counts[1],
        stage2_count=stage_counts[2],
        stage3_count=stage_counts[3],
        stage4_count=stage_counts[4],
        advancing=advancing,
        declining=declining,
        unchanged=unchanged,
        new_highs_52w=new_highs,
        new_lows_52w=new_lows,
        breakout_count=breakout_count,
        vcp_count=vcp_count,
        stage2_pct=stage2_pct,
        above_ma200=above_ma200,
        below_ma200=below_ma200,
        above_ma200_pct=above_ma200_pct,
        set_index_close=set_close,
        set_index_change_pct=set_change_pct,
    )


def filter_signals(signals: list[StockSignal], pattern: Optional[str] = None, stage: Optional[int] = None) -> list[StockSignal]:
    """Filter signals by pattern name or stage number."""
    result = signals
    if stage is not None:
        result = [s for s in result if s.stage is not None and int(s.stage) == stage]
    if pattern is not None:
        result = [s for s in result if s.pattern == pattern]
    return result


def compute_index_breadth(
    signals: list[StockSignal],
    members: set[str],
    index_close: float = 0.0,
    index_change_pct: float = 0.0,
) -> MarketBreadth:
    """Same shape as compute_market_breadth but filtered to a sub-index's
    member set (SET50 / SET100 / MAI).

    The result reuses the MarketBreadth dataclass so all the existing card
    builders / breadth-context formatters work unchanged. set_index_close /
    set_index_change_pct fields here represent the SUB-INDEX's price (not
    SET composite), which the bulk index card / breadth card will surface.

    members: set of ticker codes (no .BK suffix). Use data.get_index_members.
    """
    filtered = [s for s in signals if s.symbol in members]
    breadth = compute_market_breadth(filtered, index_df=None)
    breadth.set_index_close = index_close
    breadth.set_index_change_pct = index_change_pct
    return breadth


def compute_sector_trends(signals: list[StockSignal]) -> list["SectorSummary"]:
    """Group signals by SET sector and compute breadth stats per sector.
    Uses get_sector() which checks the dynamic subsector map first, then falls
    back to the static SECTOR_MAP, so coverage improves after refresh_sector_map.
    """
    sector_groups: dict[str, list[StockSignal]] = {}
    for s in signals:
        sec = get_sector(s.symbol)
        sector_groups.setdefault(sec, []).append(s)

    summaries = []
    for sector, sigs in sector_groups.items():
        n = len(sigs)
        s2 = sum(1 for s in sigs if s.stage == 2)
        bk = sum(1 for s in sigs if s.pattern in ("breakout", "ath_breakout"))
        avg_score = round(sum(s.strength_score for s in sigs) / n, 1) if n else 0.0
        adv = sum(1 for s in sigs if s.change_pct > 0.1)
        dec = sum(1 for s in sigs if s.change_pct < -0.1)
        summaries.append(SectorSummary(
            sector=sector,
            total=n,
            stage2_count=s2,
            stage2_pct=round(s2 / n * 100, 1) if n else 0.0,
            breakout_count=bk,
            avg_strength=avg_score,
            advancing=adv,
            declining=dec,
        ))

    summaries.sort(key=lambda x: x.stage2_pct, reverse=True)
    return summaries
