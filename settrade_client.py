"""
settrade_client.py — SET Trade Open API client using official SDK.

Uses: settrade-open-api Python SDK
Docs: https://settrade-open-api.readthedocs.io

Credentials (stored in Secret Manager):
  SETTRADE_APP_ID, SETTRADE_APP_SECRET, SETTRADE_BROKER_ID, SETTRADE_APP_CODE
"""

import logging
from functools import lru_cache
from typing import Optional

import pandas as pd

from config import get_settings

logger = logging.getLogger(__name__)


@lru_cache(maxsize=1)
def _get_investor():
    """Return a cached Settrade Investor instance."""
    settings = get_settings()
    if not all([
        settings.settrade_app_id,
        settings.settrade_app_secret,
        settings.settrade_broker_id,
        settings.settrade_app_code,
    ]):
        logger.warning("SET Trade API credentials not fully configured")
        return None
    try:
        from settrade_open_api import Investor
        investor = Investor(
            app_id=settings.settrade_app_id,
            app_secret=settings.settrade_app_secret,
            broker_id=settings.settrade_broker_id,
            app_code=settings.settrade_app_code,
        )
        logger.info("SET Trade API client initialised")
        return investor
    except Exception as exc:
        logger.error("Failed to init SET Trade API client: %s", exc)
        return None


def is_api_available() -> bool:
    """Return True if Settrade API is configured and reachable."""
    try:
        investor = _get_investor()
        if investor is None:
            return False
        # Light ping — get market data client
        investor.MarketData()
        return True
    except Exception as exc:
        logger.warning("SET Trade API not available: %s", exc)
        return False


def get_stock_list_from_api() -> list[dict]:
    """
    Fetch all SET-listed securities via SDK.

    Returns list of dicts: {symbol, name_th, name_en, sector, market}
    """
    try:
        investor = _get_investor()
        if not investor:
            return []
        md = investor.MarketData()
        securities = md.get_security_list(market="SET")

        result = []
        for s in securities:
            result.append({
                "symbol": s.get("symbol", ""),
                "name_th": s.get("nameTH") or s.get("securityNameTH", ""),
                "name_en": s.get("nameEN") or s.get("securityNameEN", ""),
                "sector": s.get("industryName") or s.get("sector", ""),
                "market": s.get("market", "SET"),
            })
        logger.info("Fetched %d securities from SET Trade API", len(result))
        return result
    except Exception as exc:
        logger.error("get_stock_list_from_api failed: %s", exc)
        return []


def get_ohlcv(symbol: str, period: str = "1Y") -> Optional[pd.DataFrame]:
    """
    Fetch historical daily OHLCV via SDK.

    Args:
        symbol: SET ticker e.g. "PTT"
        period: "1M" | "3M" | "6M" | "1Y" | "3Y" | "5Y"
    """
    try:
        investor = _get_investor()
        if not investor:
            return None
        md = investor.MarketData()

        # Map period to limit (trading days)
        limit_map = {"1M": 30, "3M": 90, "6M": 180, "1Y": 365, "3Y": 1095, "5Y": 1825}
        limit = limit_map.get(period, 365)

        candles = md.get_candlestick(
            symbol=symbol,
            limit=limit,
            timeframe="1D",
        )
        if not candles:
            return None

        df = pd.DataFrame(candles)

        # Normalise column names
        rename = {}
        for col in df.columns:
            low = col.lower()
            if low in ("time", "date", "datetime", "t"):
                rename[col] = "Date"
            elif low in ("open", "o"):
                rename[col] = "Open"
            elif low in ("high", "h"):
                rename[col] = "High"
            elif low in ("low", "l"):
                rename[col] = "Low"
            elif low in ("close", "c"):
                rename[col] = "Close"
            elif "vol" in low:
                rename[col] = "Volume"
        df = df.rename(columns=rename)

        if "Date" not in df.columns:
            logger.warning("No date column in candlestick response for %s", symbol)
            return None

        df["Date"] = pd.to_datetime(df["Date"], unit="ms", errors="coerce").fillna(
            pd.to_datetime(df["Date"], errors="coerce")
        )
        df = df.set_index("Date").sort_index()

        for col in ["Open", "High", "Low", "Close", "Volume"]:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce")

        df = df.dropna(subset=["Close"])
        return df

    except Exception as exc:
        logger.error("get_ohlcv(%s) failed: %s", symbol, exc)
        return None


def get_quote(symbol: str) -> Optional[dict]:
    """Fetch real-time quote for a symbol."""
    try:
        investor = _get_investor()
        if not investor:
            return None
        md = investor.MarketData()
        q = md.get_quote_symbol(symbol=symbol)
        if not q:
            return None
        return {
            "symbol": symbol,
            "last": float(q.get("last") or q.get("close") or 0),
            "change": float(q.get("change") or 0),
            "change_pct": float(q.get("percentChange") or q.get("changePct") or 0),
            "volume": int(q.get("volume") or q.get("vol") or 0),
            "bid": float(q.get("bid") or 0),
            "ask": float(q.get("offer") or q.get("ask") or 0),
        }
    except Exception as exc:
        logger.error("get_quote(%s) failed: %s", symbol, exc)
        return None


def get_all_symbols_from_api() -> list[str]:
    """Return just the symbol strings for all SET-listed stocks."""
    stocks = get_stock_list_from_api()
    return [s["symbol"] for s in stocks if s.get("symbol")]
