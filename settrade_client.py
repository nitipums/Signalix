"""
settrade_client.py — SET Trade Open API client using settrade_v2 SDK.

Package: settrade-v2
Docs: https://developer.settrade.com/open-api/api-reference
"""

import logging
from concurrent.futures import ThreadPoolExecutor
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


def get_bulk_ohlcv(symbols: list[str], period: str = "1M", max_workers: int = 10) -> dict[str, pd.DataFrame]:
    """Fetch recent OHLCV for multiple symbols in parallel via Settrade API."""
    if not is_api_available():
        return {}

    def _fetch(sym):
        return sym, get_ohlcv(sym, period=period)

    results: dict[str, pd.DataFrame] = {}
    with ThreadPoolExecutor(max_workers=max_workers) as ex:
        for sym, df in ex.map(_fetch, symbols):
            if df is not None and not df.empty:
                results[sym] = df
    logger.info("get_bulk_ohlcv: %d/%d symbols fetched", len(results), len(symbols))
    return results


def get_bulk_quotes(symbols: list[str], max_workers: int = 30) -> dict[str, dict]:
    """Fetch real-time quotes for multiple symbols in parallel via Settrade API.

    Two-pass strategy:
      1. Aggressive parallel fetch at max_workers (default 30).
      2. Re-try any symbols the first pass missed with a smaller pool
         (max_workers=10) — catches transient concurrency-induced failures
         (diagnosed in prod: BBL, BDMS and other blue chips work in single
         quotes but fall out of large parallel batches, presumably hitting
         a Settrade rate-limit / queue bound).

    Symbols still missing after the retry are returned unresolved; callers
    should fall back to cached or yfinance data for those.
    """
    if not is_api_available():
        logger.warning("get_bulk_quotes: Settrade API unavailable (missing creds or init failed)")
        return {}

    def _fetch(sym):
        return sym, get_quote(sym)

    def _pass(targets: list[str], workers: int) -> dict[str, dict]:
        got: dict[str, dict] = {}
        with ThreadPoolExecutor(max_workers=workers) as ex:
            for sym, q in ex.map(_fetch, targets):
                if q and (q.get("last") or 0) > 0:
                    got[sym] = q
        return got

    results = _pass(symbols, max_workers)

    missing = [s for s in symbols if s not in results]
    if missing:
        retry_workers = min(10, max_workers)
        recovered = _pass(missing, retry_workers)
        results.update(recovered)
        logger.info("get_bulk_quotes: retry recovered %d/%d missed symbols (pool=%d)",
                    len(recovered), len(missing), retry_workers)

    coverage = (len(results) / len(symbols) * 100) if symbols else 0.0
    logger.info("get_bulk_quotes: %d/%d symbols final (%.1f%% coverage)",
                len(results), len(symbols), coverage)
    return results


def get_all_symbols_from_api() -> list[str]:
    return [s["symbol"] for s in get_stock_list_from_api() if s.get("symbol")]
