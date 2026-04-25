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

from config import get_settings
from data import SECTOR_MAP, fetch_all_stocks, get_stock_list, tradingview_url

logger = logging.getLogger(__name__)

MIN_ROWS = 200  # Minimum trading days of data required for analysis


# ─── Data classes ─────────────────────────────────────────────────────────────

@dataclass
class StockSignal:
    symbol: str
    name: str
    stage: int                     # 1, 2, 3, or 4
    pattern: str                   # see module docstring
    close: float
    change_pct: float
    volume: int
    volume_ratio: float            # today's vol / 20-day avg vol
    sma50: float
    sma150: float
    sma200: float
    high_52w: float
    low_52w: float
    strength_score: float          # 0–100 composite ranking
    tradingview_url: str
    scanned_at: str                # ISO timestamp
    breakout_details: dict = field(default_factory=dict)
    # Risk/reward fields (all default 0 for backward compat with old Firestore docs)
    atr: float = 0.0               # 14-day Average True Range
    trade_value_m: float = 0.0     # Trade value in THB millions (volume * close / 1M)
    pct_from_52w_high: float = 0.0 # (close / high_52w - 1) * 100, negative when below ATH
    stop_loss: float = 0.0         # ATR-based: close - 1.5 * ATR
    target_price: float = 0.0      # close + REWARD_R_MULTIPLE * (close - stop_loss)
    breakout_count_1y: int = 0     # Number of distinct breakout events in past year


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


# ─── Stage classification ──────────────────────────────────────────────────────

def classify_stage(df: pd.DataFrame) -> int:
    """
    Classify current Minervini stage based on OHLCV DataFrame.

    Returns 1, 2, 3, or 4.
    Falls back to stage 1 if data is insufficient.
    """
    if len(df) < MIN_ROWS:
        return 1

    close = df["Close"]
    high = df["High"]

    sma50 = _sma(close, 50)
    sma150 = _sma(close, 150)
    sma200 = _sma(close, 200)

    c = float(close.iloc[-1])
    s50 = float(sma50.iloc[-1])
    s150 = float(sma150.iloc[-1])
    s200 = float(sma200.iloc[-1])

    if any(np.isnan(x) for x in [c, s50, s150, s200]):
        return 1

    # Is the 200-day MA rising? Compare to value 20 trading days ago
    sma200_now = float(sma200.iloc[-1])
    sma200_20d_ago = float(sma200.iloc[-21]) if len(sma200.dropna()) > 21 else np.nan
    sma200_rising = (not np.isnan(sma200_20d_ago)) and (sma200_now > sma200_20d_ago)

    # 52-week high and low
    lookback = min(252, len(df))
    high_52w = float(high.iloc[-lookback:].max())
    low_52w = float(close.iloc[-lookback:].min())

    # ── Stage 2 (Uptrend) — Minervini Template of Excellence ──
    # 1. Price above 150-day and 200-day MA
    # 2. 150-day MA above 200-day MA
    # 3. 200-day MA is trending upward
    # 4. Price is at least 25% above its 52-week low
    # 5. Price is within 25% of its 52-week high
    stage2 = (
        c > s150
        and c > s200
        and s150 > s200
        and sma200_rising
        and c >= low_52w * 1.25
        and c >= high_52w * 0.75
    )
    if stage2:
        return 2

    # ── Stage 4 (Downtrend) ──
    # Price below both 150 and 200 MA, and 200 MA is declining
    stage4 = c < s150 and c < s200 and not sma200_rising
    if stage4:
        return 4

    # ── Stage 3 (Distribution) ──
    # Was in Stage 2 territory but close broke below 150-day MA;
    # use the sign of recent price action relative to 200-day MA
    if c < s150 and c > s200:
        return 3

    # ── Stage 1 (Basing / Neglect) ──
    return 1


# ─── Pattern detection ─────────────────────────────────────────────────────────

def detect_pattern(df: pd.DataFrame, stage: int, ath_override: Optional[float] = None) -> tuple[str, dict]:
    """
    Detect the most significant pattern for a stock.

    Returns (pattern_name, details_dict).
    ath_override: true all-time high from Firestore cache; falls back to window max if None.
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

    # All-time high — use pre-synced cache if available, else fall back to window max
    ath = ath_override if ath_override is not None else float(high.max())

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
        pivot_high = float(high.iloc[-52:-1].max()) if len(df) > 52 else float(high.iloc[:-1].max())
        is_breakout = c > pivot_high and volume_ratio >= 1.4

        if is_breakout:
            is_ath = c >= ath * 0.99  # within 1% of all-time high counts
            details = {
                "pivot_high": round(pivot_high, 2),
                "volume_ratio": round(volume_ratio, 2),
                "is_ath": is_ath,
            }
            return ("ath_breakout" if is_ath else "breakout"), details

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

def _strength_score(df: pd.DataFrame, stage: int, pattern: str, volume_ratio: float) -> float:
    """
    Composite score 0–100.

    Rewards:
    - Being in Stage 2 (+40)
    - Breakout pattern (+20), VCP (+15), ATH breakout (+25)
    - High volume ratio on breakout (+up to 15)
    - RS: how close price is to 52-week high (+up to 20)
    """
    score = 0.0

    stage_scores = {1: 10, 2: 40, 3: 15, 4: 0}
    score += stage_scores.get(stage, 0)

    pattern_scores = {
        "ath_breakout": 25,
        "breakout": 20,
        "vcp": 15,
        "vcp_low_cheat": 18,
        "consolidating": 5,
        "going_down": 0,
    }
    score += pattern_scores.get(pattern, 0)

    # Volume bonus (capped at 15)
    vol_bonus = min(15, (volume_ratio - 1.0) * 10) if volume_ratio > 1.0 else 0
    score += vol_bonus

    # RS: proximity to 52-week high
    if len(df) >= 20:
        high_52w = float(df["High"].iloc[-252:].max()) if len(df) >= 252 else float(df["High"].max())
        c = float(df["Close"].iloc[-1])
        rs_score = (c / high_52w) * 20
        score += rs_score

    return round(min(100.0, score), 1)


# ─── Full scan ─────────────────────────────────────────────────────────────────

def scan_stock(symbol: str, df: pd.DataFrame, ath_override: Optional[float] = None) -> Optional[StockSignal]:
    """Analyse a single stock and return a StockSignal, or None if data is insufficient."""
    if df is None or len(df) < 60:
        return None

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
    pattern, bp_details = detect_pattern(df, stage, ath_override=ath_override)

    lookback = min(252, len(df))
    high_52w = float(high.iloc[-lookback:].max())
    low_52w = float(close.iloc[-lookback:].min())

    sma50 = float(_sma(close, 50).iloc[-1]) if len(df) >= 50 else float("nan")
    sma150 = float(_sma(close, 150).iloc[-1]) if len(df) >= 150 else float("nan")
    sma200 = float(_sma(close, 200).iloc[-1]) if len(df) >= 200 else float("nan")

    score = _strength_score(df, stage, pattern, volume_ratio)

    # Risk/reward calculations
    atr_series = _atr(df)
    atr_val = float(atr_series.iloc[-1]) if len(df) >= 14 and not np.isnan(atr_series.iloc[-1]) else 0.0
    trade_value_m = round(float(c * vol_now) / 1_000_000, 2)
    pct_from_high = round((c / high_52w - 1) * 100, 2) if high_52w > 0 else 0.0
    stop_loss_price = round(c - 1.5 * atr_val, 2) if atr_val > 0 else 0.0
    risk_per_share = c - stop_loss_price if stop_loss_price > 0 else 0.0
    reward_r = get_settings().reward_r_multiple
    target = round(c + reward_r * risk_per_share, 2) if risk_per_share > 0 else 0.0
    bo_count = count_breakouts_1y(df)

    from datetime import datetime
    import pytz
    bkk = pytz.timezone("Asia/Bangkok")
    now_str = datetime.now(bkk).isoformat()

    return StockSignal(
        symbol=symbol,
        name=symbol,  # Thai name could be enriched later via SET Trade API metadata
        stage=stage,
        pattern=pattern,
        close=round(c, 2),
        change_pct=round(change_pct, 2),
        volume=vol_now,
        volume_ratio=round(volume_ratio, 2),
        sma50=round(sma50, 2) if not np.isnan(sma50) else 0.0,
        sma150=round(sma150, 2) if not np.isnan(sma150) else 0.0,
        sma200=round(sma200, 2) if not np.isnan(sma200) else 0.0,
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
    Compute MACD (12/26/9 EMA), RSI (14), trend vs MA200, and Thai implication text.
    Returns an enriched dict compatible with the existing {close, change_pct} schema.
    """
    close = df["Close"].dropna()
    if len(close) < 30:
        return {"name": name, "close": 0.0, "change_pct": 0.0}

    # MACD
    ema12 = close.ewm(span=12, adjust=False).mean()
    ema26 = close.ewm(span=26, adjust=False).mean()
    macd_line = ema12 - ema26
    macd_signal_line = macd_line.ewm(span=9, adjust=False).mean()
    macd_hist = macd_line - macd_signal_line

    # RSI (Wilder smoothing via ewm)
    delta = close.diff()
    gain = delta.clip(lower=0).ewm(alpha=1/14, adjust=False).mean()
    loss = (-delta.clip(upper=0)).ewm(alpha=1/14, adjust=False).mean()
    rs = gain / loss.replace(0, np.nan)
    rsi_series = 100 - (100 / (1 + rs))

    # Trend vs MA200
    ma200 = close.rolling(200, min_periods=50).mean()

    # Latest values
    cur_close = float(close.iloc[-1])
    prev_close = float(close.iloc[-2]) if len(close) > 1 else cur_close
    change_pct = round((cur_close - prev_close) / prev_close * 100, 2) if prev_close else 0.0

    cur_macd = float(macd_line.iloc[-1])
    cur_signal = float(macd_signal_line.iloc[-1])
    cur_hist = float(macd_hist.iloc[-1])
    prev_hist = float(macd_hist.iloc[-2]) if len(macd_hist) > 1 else 0.0
    cur_rsi = float(rsi_series.iloc[-1]) if not np.isnan(rsi_series.iloc[-1]) else 50.0
    cur_ma200 = float(ma200.iloc[-1]) if not np.isnan(ma200.iloc[-1]) else 0.0
    above_ma200 = cur_close > cur_ma200 if cur_ma200 > 0 else None

    # Crossover signals
    macd_bullish_cross = cur_hist > 0 and prev_hist <= 0
    macd_bearish_cross = cur_hist < 0 and prev_hist >= 0

    # Build Thai implication text
    parts = []
    if above_ma200 is True:
        parts.append("เหนือ MA200 (uptrend)")
    elif above_ma200 is False:
        parts.append("ต่ำกว่า MA200 (downtrend)")
    if macd_bullish_cross:
        parts.append("🟢 MACD cross up")
    elif macd_bearish_cross:
        parts.append("🔴 MACD cross down")
    elif cur_macd > cur_signal:
        parts.append("MACD เป็นบวก")
    else:
        parts.append("MACD เป็นลบ")
    if cur_rsi > 70:
        parts.append(f"⚠️ RSI overbought ({cur_rsi:.0f})")
    elif cur_rsi < 30:
        parts.append(f"🟢 RSI oversold ({cur_rsi:.0f})")
    else:
        parts.append(f"RSI {cur_rsi:.0f} (ปกติ)")

    return {
        "name": name,
        "close": round(cur_close, 2),
        "change_pct": change_pct,
        "prev_close": round(prev_close, 2),
        "macd_line": round(cur_macd, 4),
        "macd_signal": round(cur_signal, 4),
        "macd_hist": round(cur_hist, 4),
        "rsi": round(cur_rsi, 1),
        "above_ma200": above_ma200,
        "ma200": round(cur_ma200, 2),
        "trend": "uptrend" if above_ma200 else ("downtrend" if above_ma200 is False else "unknown"),
        "macd_bullish_cross": macd_bullish_cross,
        "macd_bearish_cross": macd_bearish_cross,
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
        stage_counts[s.stage] = stage_counts.get(s.stage, 0) + 1

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
        result = [s for s in result if s.stage == stage]
    if pattern is not None:
        result = [s for s in result if s.pattern == pattern]
    return result


def compute_sector_trends(signals: list[StockSignal]) -> list["SectorSummary"]:
    """Group signals by SET sector and compute breadth stats per sector."""
    sector_groups: dict[str, list[StockSignal]] = {}
    for s in signals:
        sec = SECTOR_MAP.get(s.symbol, "OTHER")
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
