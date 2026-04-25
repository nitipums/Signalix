"""
trader.py — bridge from analyzer.StockSignal to live (or paper) Settrade orders.

Two entry points:
  process_signals(signals, db)        — called from POST /scan
  manage_open_positions(db)           — called from POST /trading/manage

State lives in Firestore:
  trades/{trade_id}                    — open + closed real trades
  paper_trades/{trade_id}              — paper-mode shadow ledger
  trading_state/kill_switch            — Firestore-persisted manual override
  trading_state/daily_pnl_{YYYY-MM-DD} — running realized P&L for the day
"""

from __future__ import annotations

import logging
from dataclasses import asdict
from datetime import datetime, timezone
from typing import Optional

import pytz

import settrade_client
from analyzer import StockSignal
from config import get_settings
from risk_manager import (
    check_can_open_new,
    compute_position_size,
    qualifying_signals,
    should_exit,
)

logger = logging.getLogger(__name__)

BANGKOK_TZ = pytz.timezone("Asia/Bangkok")


# --------------------------------------------------------------------------- #
# Firestore helpers — all guarded against db=None                             #
# --------------------------------------------------------------------------- #

def _now_bkk() -> datetime:
    return datetime.now(BANGKOK_TZ)


def _today_key() -> str:
    return _now_bkk().strftime("%Y-%m-%d")


def _trade_id(symbol: str) -> str:
    return f"{symbol}-{_now_bkk().strftime('%Y%m%d-%H%M%S')}"


def kill_switch_active(db) -> bool:
    if db is None:
        return False
    try:
        doc = db.collection("trading_state").document("kill_switch").get()
        return bool(doc.exists and doc.to_dict().get("disabled"))
    except Exception as exc:
        logger.error("kill_switch_active read failed: %s", exc)
        return False


def set_kill_switch(db, disabled: bool, reason: str = "") -> None:
    if db is None:
        return
    db.collection("trading_state").document("kill_switch").set({
        "disabled": bool(disabled),
        "reason": reason,
        "updated_at": _now_bkk().isoformat(),
    })


def today_realized_pnl(db) -> float:
    if db is None:
        return 0.0
    try:
        doc = db.collection("trading_state").document(f"daily_pnl_{_today_key()}").get()
        if not doc.exists:
            return 0.0
        return float(doc.to_dict().get("realized_pnl") or 0.0)
    except Exception as exc:
        logger.error("today_realized_pnl read failed: %s", exc)
        return 0.0


def _bump_daily_pnl(db, delta: float) -> None:
    if db is None:
        return
    ref = db.collection("trading_state").document(f"daily_pnl_{_today_key()}")
    try:
        snap = ref.get()
        cur = float(snap.to_dict().get("realized_pnl") or 0.0) if snap.exists else 0.0
        ref.set({"realized_pnl": cur + delta, "updated_at": _now_bkk().isoformat()})
    except Exception as exc:
        logger.error("_bump_daily_pnl failed: %s", exc)


def _trades_collection(db, mode: str):
    return db.collection("paper_trades" if mode == "paper" else "trades")


def _open_trades(db, mode: str) -> list[dict]:
    if db is None:
        return []
    try:
        q = _trades_collection(db, mode).where("status", "==", "open").stream()
        return [{"id": d.id, **d.to_dict()} for d in q]
    except Exception as exc:
        logger.error("_open_trades failed: %s", exc)
        return []


# --------------------------------------------------------------------------- #
# Entry — process_signals                                                     #
# --------------------------------------------------------------------------- #

def process_signals(signals: list[StockSignal], db=None) -> dict:
    """
    For each qualifying signal: gate via risk_manager, size, place order
    (or write paper-trade), persist trades/{id}, return a summary.
    """
    s = get_settings()
    summary = {"considered": 0, "skipped": [], "placed": [], "errors": []}

    if not s.trading_enabled:
        return {**summary, "reason": "trading_disabled_in_config"}
    if kill_switch_active(db):
        return {**summary, "reason": "kill_switch_active"}

    candidates = qualifying_signals(signals)
    summary["considered"] = len(candidates)
    if not candidates:
        return summary

    mode = s.trading_mode.lower()
    market_open = settrade_client.market_is_open()

    if mode == "live":
        account_info = settrade_client.get_account_info() or {}
        open_positions = settrade_client.get_portfolio()
    else:
        # Paper mode: synthesise an account so sizing math still works
        account_info = {
            "cash_balance": 1_000_000,
            "line_available": 1_000_000,
            "equity": 1_000_000,
        }
        # In paper mode the "portfolio" is the open trades in our shadow ledger
        open_positions = [
            {"symbol": t["symbol"], "volume": t.get("volume", 0)}
            for t in _open_trades(db, "paper")
        ]

    today_pnl = today_realized_pnl(db)

    for sig in candidates:
        allowed, reason = check_can_open_new(
            sig, account_info, open_positions, today_pnl, market_open
        )
        if not allowed:
            summary["skipped"].append({"symbol": sig.symbol, "reason": reason})
            continue

        volume = compute_position_size(sig, account_info)
        if volume <= 0:
            summary["skipped"].append({"symbol": sig.symbol, "reason": "size_zero"})
            continue

        trade = {
            "symbol": sig.symbol,
            "side": "Buy",
            "volume": volume,
            "entry_price": sig.close,
            "stop_loss": sig.stop_loss,
            "target_price": sig.target_price,
            "strength_score": sig.strength_score,
            "pattern": sig.pattern,
            "status": "open",
            "mode": mode,
            "entered_at": _now_bkk().isoformat(),
            "signal_snapshot": _signal_to_dict(sig),
        }

        if mode == "live":
            resp = settrade_client.place_order(
                symbol=sig.symbol,
                side="Buy",
                volume=volume,
                price=sig.close,
                price_type=s.entry_price_type,
            )
            if not resp:
                summary["errors"].append({"symbol": sig.symbol, "reason": "broker_rejected"})
                continue
            trade["order_no"] = resp.get("order_no")

        tid = _trade_id(sig.symbol)
        if db is not None:
            try:
                _trades_collection(db, mode).document(tid).set(trade)
            except Exception as exc:
                logger.error("trades doc write failed for %s: %s", tid, exc)
                summary["errors"].append({"symbol": sig.symbol, "reason": f"firestore:{exc}"})
                continue

        # Locally track that we just opened this so subsequent loop iterations
        # see it in `open_positions` and don't double-enter.
        open_positions.append({"symbol": sig.symbol, "volume": volume})

        summary["placed"].append({
            "trade_id": tid,
            "symbol": sig.symbol,
            "volume": volume,
            "entry_price": sig.close,
            "stop_loss": sig.stop_loss,
            "target_price": sig.target_price,
            "mode": mode,
            "order_no": trade.get("order_no"),
        })

        _notify_entry_safe(trade)

    return summary


# --------------------------------------------------------------------------- #
# Position management — called by scheduler every ~10 min                     #
# --------------------------------------------------------------------------- #

def manage_open_positions(db=None) -> dict:
    s = get_settings()
    summary = {"checked": 0, "held": [], "exited": [], "errors": []}

    if not s.trading_enabled:
        return {**summary, "reason": "trading_disabled_in_config"}

    mode = s.trading_mode.lower()
    open_trades = _open_trades(db, mode)
    summary["checked"] = len(open_trades)
    if not open_trades:
        return summary

    if mode == "live":
        # Cross-reference broker portfolio so we don't act on trades the broker no longer holds
        broker_positions = {p["symbol"]: p for p in settrade_client.get_portfolio()}
    else:
        broker_positions = {t["symbol"]: t for t in open_trades}

    for trade in open_trades:
        symbol = trade["symbol"]
        if symbol not in broker_positions and mode == "live":
            # Position closed externally — reconcile and skip
            _close_trade(db, trade, exit_price=None, reason="external_close", mode=mode)
            summary["exited"].append({"symbol": symbol, "reason": "external_close"})
            continue

        quote = settrade_client.get_quote(symbol) or {}
        action, reason = should_exit(broker_positions.get(symbol, {}), quote, trade)

        if action == "hold":
            summary["held"].append({"symbol": symbol, "reason": reason})
            continue

        # action == "sell"
        last_price = float(quote.get("last") or trade.get("entry_price") or 0)
        if mode == "live":
            resp = settrade_client.place_order(
                symbol=symbol,
                side="Sell",
                volume=int(trade.get("volume") or 0),
                price=last_price,
                price_type="Market" if reason.startswith("stop_hit") else s.entry_price_type,
                position="Close",
            )
            if not resp:
                summary["errors"].append({"symbol": symbol, "reason": f"sell_failed:{reason}"})
                continue
            exit_order_no = resp.get("order_no")
        else:
            exit_order_no = None

        _close_trade(db, trade, exit_price=last_price, reason=reason, mode=mode,
                     exit_order_no=exit_order_no)
        summary["exited"].append({
            "symbol": symbol, "exit_price": last_price, "reason": reason,
        })

    return summary


# --------------------------------------------------------------------------- #
# Internals                                                                   #
# --------------------------------------------------------------------------- #

def _close_trade(
    db,
    trade: dict,
    exit_price: Optional[float],
    reason: str,
    mode: str,
    exit_order_no: Optional[str] = None,
) -> None:
    if db is None:
        return
    entry = float(trade.get("entry_price") or 0)
    volume = int(trade.get("volume") or 0)
    realized = (float(exit_price or 0) - entry) * volume if exit_price else 0.0
    update = {
        "status": "closed",
        "exit_price": exit_price,
        "exit_reason": reason,
        "exit_order_no": exit_order_no,
        "exited_at": _now_bkk().isoformat(),
        "realized_pnl": realized,
    }
    try:
        _trades_collection(db, mode).document(trade["id"]).update(update)
    except Exception as exc:
        logger.error("_close_trade update failed: %s", exc)
        return
    _bump_daily_pnl(db, realized)
    _notify_exit_safe({**trade, **update})


def _signal_to_dict(sig: StockSignal) -> dict:
    try:
        return asdict(sig)
    except Exception:
        # Best-effort fallback
        return {k: getattr(sig, k, None) for k in (
            "symbol", "stage", "pattern", "close", "strength_score",
            "stop_loss", "target_price", "atr", "volume_ratio", "scanned_at",
        )}


def _notify_entry_safe(trade: dict) -> None:
    try:
        from notifier import notify_trade_entry
        notify_trade_entry(trade)
    except Exception as exc:
        logger.error("notify_trade_entry failed: %s", exc)


def _notify_exit_safe(trade: dict) -> None:
    try:
        from notifier import notify_trade_exit
        notify_trade_exit(trade)
    except Exception as exc:
        logger.error("notify_trade_exit failed: %s", exc)
