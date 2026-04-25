"""
risk_manager.py — pre-trade gating, position sizing, exit-trigger logic.

All functions are pure (no I/O, no SDK calls) so they can be exercised in
isolation. Inputs are plain dicts / dataclasses; outputs are tuples or ints.
"""

from __future__ import annotations

import logging
import math
from typing import Optional

from analyzer import StockSignal
from config import get_settings

logger = logging.getLogger(__name__)


def _allowed_pattern_set() -> set[str]:
    return {p.strip() for p in get_settings().allowed_patterns.split(",") if p.strip()}


def compute_position_size(signal: StockSignal, account_info: dict) -> int:
    """
    volume = floor( (equity * RISK_PER_TRADE_PCT) / (entry - stop_loss) )
    clamped by MAX_POSITION_SIZE_THB / entry, rounded down to BOARD_LOT.
    Returns 0 when sizing isn't viable.
    """
    s = get_settings()
    entry = signal.close
    stop = signal.stop_loss
    if entry <= 0 or stop <= 0 or stop >= entry:
        return 0

    risk_per_share = entry - stop
    equity = float(account_info.get("equity") or account_info.get("line_available") or 0)
    if equity <= 0:
        return 0

    risk_budget = equity * s.risk_per_trade_pct
    by_risk = risk_budget / risk_per_share
    by_cap = s.max_position_size_thb / entry
    raw = min(by_risk, by_cap)

    lots = math.floor(raw / s.board_lot)
    return max(0, lots * s.board_lot)


def check_can_open_new(
    signal: StockSignal,
    account_info: dict,
    open_positions: list[dict],
    today_realized_pnl: float,
    market_open: bool,
) -> tuple[bool, str]:
    """Return (allowed, reason). `reason` is non-empty whether allowed or not."""
    s = get_settings()

    if not s.trading_enabled:
        return False, "trading_disabled"
    if not market_open:
        return False, "market_closed"
    if signal.pattern not in _allowed_pattern_set():
        return False, f"pattern_not_allowed:{signal.pattern}"
    if signal.strength_score < s.min_strength_score:
        return False, f"score_below_threshold:{signal.strength_score}<{s.min_strength_score}"
    if signal.stop_loss <= 0 or signal.stop_loss >= signal.close:
        return False, "invalid_stop_loss"
    if any(p.get("symbol") == signal.symbol for p in open_positions):
        return False, "already_holding"
    if len(open_positions) >= s.max_open_positions:
        return False, f"max_open_positions_reached:{len(open_positions)}>={s.max_open_positions}"
    if today_realized_pnl <= -abs(s.max_daily_loss_thb):
        return False, f"daily_loss_limit_hit:{today_realized_pnl}"

    volume = compute_position_size(signal, account_info)
    if volume <= 0:
        return False, "position_size_zero"
    notional = volume * signal.close
    line_available = float(account_info.get("line_available") or account_info.get("cash_balance") or 0)
    if notional > line_available:
        return False, f"insufficient_buying_power:{notional}>{line_available}"

    return True, "ok"


def should_exit(position: dict, latest_quote: dict, trade_at_entry: dict) -> tuple[str, str]:
    """
    Decide whether to exit an open position.
    Returns (action, reason) where action ∈ {"hold", "sell"}.

    `trade_at_entry` carries the planned `stop_loss` and `target_price` from
    the signal at the time of entry (persisted in Firestore trades/{id}).
    """
    last = float(latest_quote.get("last") or 0)
    if last <= 0:
        return "hold", "no_quote"

    stop = float(trade_at_entry.get("stop_loss") or 0)
    target = float(trade_at_entry.get("target_price") or 0)

    if stop > 0 and last <= stop:
        return "sell", f"stop_hit:{last}<={stop}"
    if target > 0 and last >= target:
        return "sell", f"target_hit:{last}>={target}"
    return "hold", "within_band"


def qualifying_signals(signals: list[StockSignal]) -> list[StockSignal]:
    """Pre-filter the scan output by pattern + score. Cheap, no broker calls."""
    s = get_settings()
    allowed = _allowed_pattern_set()
    return [
        sig for sig in signals
        if sig.pattern in allowed
        and sig.strength_score >= s.min_strength_score
        and sig.stop_loss > 0
        and sig.stop_loss < sig.close
    ]
