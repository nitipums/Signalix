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

    get_candlestick returns a list with one dict where each key holds a list:
    [{"close": [...], "high": [...], "low": [...], "open": [...],
      "time": [...],  "volume": [...]}]
    time values are Unix timestamps (seconds).
    """
    period_to_limit = {"1M": 30, "3M": 90, "6M": 180, "1Y": 365, "3Y": 1095, "5Y": 1825}
    limit = period_to_limit.get(period, 365)

    try:
        investor = _get_investor()
        if not investor:
            return None
        market = investor.MarketData()
        res = market.get_candlestick(
            symbol=symbol,
            interval="1d",
            limit=limit,
            normalized=True,
        )
        if not res:
            return None

        data = res[0]  # single dict with list values
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
        return df

    except Exception as exc:
        logger.error("get_ohlcv(%s) failed: %s", symbol, exc)
        return None


def get_stock_list_from_api() -> list[dict]:
    """Fetch all SET-listed securities."""
    try:
        investor = _get_investor()
        if not investor:
            return []
        market = investor.MarketData()
        securities = market.get_security_list(market="SET")
        result = []
        for s in securities:
            result.append({
                "symbol":   s.get("symbol", ""),
                "name_th":  s.get("nameTH") or s.get("securityNameTH", ""),
                "name_en":  s.get("nameEN") or s.get("securityNameEN", ""),
                "sector":   s.get("industryName") or s.get("sector", ""),
                "market":   s.get("market", "SET"),
            })
        logger.info("Fetched %d securities from Settrade API", len(result))
        return result
    except Exception as exc:
        logger.error("get_stock_list_from_api failed: %s", exc)
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
