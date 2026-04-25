"""
settrade_client.py — SET Trade Open API client using settrade_v2 SDK.

Package: settrade-v2
Docs: https://developer.settrade.com/open-api/api-reference
"""

import logging
from datetime import datetime
from functools import lru_cache
from typing import Optional

import pandas as pd

from config import get_settings

logger = logging.getLogger(__name__)


@lru_cache(maxsize=1)
def _get_investor():
    """Return a cached Investor instance."""
    settings = get_settings()
    if not all([
        settings.settrade_app_id,
        settings.settrade_app_secret,
        settings.settrade_broker_id,
        settings.settrade_app_code,
    ]):
        logger.warning("SET Trade API credentials not configured")
        return None
    try:
        from settrade_v2 import Investor
        investor = Investor(
            app_id=settings.settrade_app_id,
            app_secret=settings.settrade_app_secret,
            broker_id=settings.settrade_broker_id,
            app_code=settings.settrade_app_code,
            is_auto_queue=False,
        )
        logger.info("Settrade Investor client initialised")
        return investor
    except Exception as exc:
        logger.error("Failed to init Settrade client: %s", exc)
        return None


def is_api_available() -> bool:
    return _get_investor() is not None


def get_ohlcv(symbol: str, period: str = "1Y") -> Optional[pd.DataFrame]:
    """
    Fetch historical daily OHLCV via settrade_v2 SDK.

    Response is a plain dict with array values:
    {"time": [...], "open": [...], "high": [...], "low": [...],
     "close": [...], "volume": [...]}
    time values are Unix timestamps in seconds.
    """
    period_to_limit = {"1M": 30, "3M": 90, "6M": 180, "1Y": 365, "3Y": 1095, "5Y": 1825}
    limit = period_to_limit.get(period, 365)

    try:
        investor = _get_investor()
        if not investor:
            return None
        market = investor.MarketData()
        data = market.get_candlestick(
            symbol=symbol,
            interval="1d",
            limit=limit,
            normalized=True,
        )
        if not data or "close" not in data:
            return None

        df = pd.DataFrame({
            "Date":   pd.to_datetime(data["time"], unit="s"),
            "Open":   data["open"],
            "High":   data["high"],
            "Low":    data["low"],
            "Close":  data["close"],
            "Volume": data["volume"],
        })
        df = df.set_index("Date").sort_index()
        df.index = df.index.tz_localize(None)
        for col in ["Open", "High", "Low", "Close", "Volume"]:
            df[col] = pd.to_numeric(df[col], errors="coerce")
        return df.dropna(subset=["Close"])

    except Exception as exc:
        logger.error("get_ohlcv(%s) failed: %s", symbol, exc)
        return None


def get_stock_list_from_api() -> list[dict]:
    """Settrade API has no security list endpoint — returns empty list."""
    return []


def get_quote(symbol: str) -> Optional[dict]:
    """Fetch real-time quote for a symbol using get_quote_symbol."""
    try:
        investor = _get_investor()
        if not investor:
            return None
        market = investor.MarketData()
        q = market.get_quote_symbol(symbol)
        if not q:
            return None
        return {
            "symbol":      q.get("symbol", symbol),
            "last":        q.get("last") or 0,
            "change":      q.get("change") or 0,
            "change_pct":  q.get("percentChange") or 0,
            "volume":      q.get("totalVolume") or 0,
            "high":        q.get("high") or 0,
            "low":         q.get("low") or 0,
            "status":      q.get("marketStatus", ""),
        }
    except Exception as exc:
        logger.error("get_quote(%s) failed: %s", symbol, exc)
        return None


def get_all_symbols_from_api() -> list[str]:
    return [s["symbol"] for s in get_stock_list_from_api() if s.get("symbol")]


# --------------------------------------------------------------------------- #
# Trading API — Investor.Equity(account_no)                                   #
# --------------------------------------------------------------------------- #

@lru_cache(maxsize=1)
def _get_equity_account():
    """Return a cached Equity-account client bound to SETTRADE_ACCOUNT_NO."""
    settings = get_settings()
    acct = (settings.settrade_account_no or "").strip()
    pin = (settings.settrade_pin or "").strip()
    if not acct or not pin or "PLACEHOLDER" in acct.upper() or "PLACEHOLDER" in pin.upper():
        logger.warning("Settrade account/PIN not configured — trading disabled")
        return None
    investor = _get_investor()
    if not investor:
        return None
    try:
        return investor.Equity(account_no=settings.settrade_account_no)
    except Exception as exc:
        logger.error("Failed to bind Equity account: %s", exc)
        return None


def is_trading_available() -> bool:
    return _get_equity_account() is not None


def get_account_info() -> Optional[dict]:
    """Cash balance, line available, equity, etc."""
    eq = _get_equity_account()
    if not eq:
        return None
    try:
        info = eq.get_account_info()
        return {
            "cash_balance": float(info.get("cashBalance") or 0),
            "line_available": float(info.get("lineAvailable") or 0),
            "credit_limit": float(info.get("creditLimit") or 0),
            "equity": float(info.get("equity") or info.get("totalMarketValue") or 0),
            "raw": info,
        }
    except Exception as exc:
        logger.error("get_account_info failed: %s", exc)
        return None


def get_portfolio() -> list[dict]:
    """Open positions — symbol, volume, avg cost, market price, unrealized P&L."""
    eq = _get_equity_account()
    if not eq:
        return []
    try:
        portfolios = eq.get_portfolios()
        # portfolios may be {"portfolioList": [...]} or a plain list
        rows = portfolios.get("portfolioList", portfolios) if isinstance(portfolios, dict) else portfolios
        out: list[dict] = []
        for p in rows or []:
            volume = float(p.get("startVolume") or p.get("actualVolume") or p.get("availableVolume") or 0)
            if volume <= 0:
                continue
            out.append({
                "symbol": p.get("symbol"),
                "volume": int(volume),
                "avg_cost": float(p.get("averagePrice") or p.get("avgCost") or 0),
                "market_price": float(p.get("marketPrice") or p.get("lastPrice") or 0),
                "unrealized_pnl": float(p.get("unrealizedPL") or p.get("unrealized") or 0),
                "raw": p,
            })
        return out
    except Exception as exc:
        logger.error("get_portfolio failed: %s", exc)
        return []


def place_order(
    symbol: str,
    side: str,                       # "Buy" or "Sell"
    volume: int,
    price: float,
    price_type: str = "Limit",       # "Limit" or "Market"
    validity_type: str = "Day",
    position: str = "Open",          # "Open" or "Close" (cash accounts ignore this)
) -> Optional[dict]:
    """Place a buy/sell order. Returns dict with order_no on success, None on failure."""
    settings = get_settings()
    eq = _get_equity_account()
    if not eq:
        return None
    try:
        kwargs = dict(
            pin=settings.settrade_pin,
            side=side,
            symbol=symbol,
            volume=int(volume),
            price=float(price),
            qty_open=int(volume),
            trustee_id_type="Local",
            price_type=price_type,
            validity_type=validity_type,
            position=position,
        )
        # SDK signature drift: some versions don't accept `position`
        try:
            resp = eq.place_order(**kwargs)
        except TypeError:
            kwargs.pop("position", None)
            resp = eq.place_order(**kwargs)
        order_no = resp.get("orderNo") or resp.get("order_no") or resp.get("id")
        logger.info(
            "Settrade order placed: %s %s %d @ %s → order_no=%s",
            side, symbol, volume, price, order_no,
        )
        return {"order_no": order_no, "raw": resp}
    except Exception as exc:
        logger.error("place_order(%s %s %d @ %s) failed: %s", side, symbol, volume, price, exc)
        return None


def cancel_order(order_no: str) -> bool:
    eq = _get_equity_account()
    if not eq:
        return False
    try:
        eq.cancel_order(pin=get_settings().settrade_pin, order_no=order_no)
        logger.info("Cancelled order %s", order_no)
        return True
    except Exception as exc:
        logger.error("cancel_order(%s) failed: %s", order_no, exc)
        return False


def get_orders(status: Optional[str] = None) -> list[dict]:
    """Today's orders. Optionally filter by status (e.g. 'O', 'M', 'C')."""
    eq = _get_equity_account()
    if not eq:
        return []
    try:
        resp = eq.get_orders()
        rows = resp.get("orderList", resp) if isinstance(resp, dict) else resp
        if status:
            rows = [r for r in (rows or []) if r.get("status") == status]
        return rows or []
    except Exception as exc:
        logger.error("get_orders failed: %s", exc)
        return []


def get_trades() -> list[dict]:
    """Today's filled trades (executions)."""
    eq = _get_equity_account()
    if not eq:
        return []
    try:
        resp = eq.get_trades()
        rows = resp.get("tradeList", resp) if isinstance(resp, dict) else resp
        return rows or []
    except Exception as exc:
        logger.error("get_trades failed: %s", exc)
        return []


def market_is_open() -> bool:
    """Simple ICT-time check: SET equity session is 10:00-12:30 and 14:30-16:30 Mon-Fri."""
    from datetime import datetime
    import pytz
    now = datetime.now(pytz.timezone("Asia/Bangkok"))
    if now.weekday() >= 5:  # Sat=5, Sun=6
        return False
    t = now.time()
    morning = (t >= datetime.strptime("10:00", "%H:%M").time()
               and t <= datetime.strptime("12:30", "%H:%M").time())
    afternoon = (t >= datetime.strptime("14:30", "%H:%M").time()
                 and t <= datetime.strptime("16:30", "%H:%M").time())
    return morning or afternoon
