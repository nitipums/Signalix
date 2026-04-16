"""
data.py — Stock data fetching and storage.

Primary source: yfinance (Thai stocks via .BK suffix, SET index via ^SET.BK)
Future: swap in SET Trade Open API as primary, keep yfinance as fallback.
"""

import logging
from datetime import datetime
from typing import Optional

import pandas as pd
import yfinance as yf
import pytz

logger = logging.getLogger(__name__)

BANGKOK_TZ = pytz.timezone("Asia/Bangkok")

# ─── Symbol list ─────────────────────────────────────────────────────────────
# Major SET stocks covering key sectors + all SET indexes.
# Expand this list or replace with a live fetch from SET Trade API.
SET_STOCKS = [
    # Energy
    "PTT", "PTTEP", "TOP", "IRPC", "SPRC", "BCP",
    # Banking & Finance
    "BBL", "KBANK", "SCB", "KTB", "TMB", "BAY", "TISCO", "KKP", "TCAP",
    # Telecom & Tech
    "ADVANC", "DTAC", "TRUE", "INTUCH",
    # Property & Construction
    "LH", "QH", "SIRI", "AP", "PSH", "SC", "ORI", "SPALI", "CPN", "AMATA",
    # Commerce & Retail
    "CPALL", "BJC", "MAKRO", "ROBINS", "HMPRO",
    # Food & Beverage
    "CPF", "TU", "OSP", "CBG",
    # Healthcare
    "BDMS", "BH", "BCH", "CHG",
    # Industrial & Transport
    "DELTA", "HANA", "KCE", "SCC", "SCCC", "PYLON",
    # Tourism & Aviation
    "AOT", "THAI", "AAV", "MINT",
    # Utilities
    "GULF", "GPSC", "RATCH", "BGRIM", "EGCO",
    # Media & Entertainment
    "BEC", "WORK", "PLANB",
    # Agriculture
    "GFPT", "NMG",
]

SET_INDEXES = ["^SET.BK"]  # SET Index

# Map clean symbol → yfinance ticker
def _to_yf_ticker(symbol: str) -> str:
    if symbol.startswith("^"):
        return symbol  # already a yfinance index ticker
    return f"{symbol}.BK"


# ─── Fetch functions ──────────────────────────────────────────────────────────

def get_stock_list() -> list[str]:
    """Return the full list of tracked SET stock symbols (without .BK suffix)."""
    return SET_STOCKS.copy()


def get_all_symbols() -> list[str]:
    """Return stocks + index symbols."""
    return SET_STOCKS + ["SET"]  # "SET" maps to ^SET.BK


def fetch_ohlcv(symbol: str, period: str = "1y") -> Optional[pd.DataFrame]:
    """
    Fetch OHLCV data for a single symbol.

    Args:
        symbol: Clean SET symbol, e.g. "PTT" or "SET" (for index)
        period: yfinance period string, e.g. "1y", "2y", "5y"

    Returns:
        DataFrame with columns [Open, High, Low, Close, Volume, Adj Close]
        indexed by Date (timezone-naive), or None on failure.
    """
    ticker = "^SET.BK" if symbol == "SET" else _to_yf_ticker(symbol)
    try:
        df = yf.download(ticker, period=period, progress=False, auto_adjust=True)
        if df.empty:
            logger.warning("No data returned for %s", ticker)
            return None
        df.index = pd.to_datetime(df.index).tz_localize(None)
        df.index.name = "Date"
        # Flatten multi-level columns if present
        if isinstance(df.columns, pd.MultiIndex):
            df.columns = df.columns.get_level_values(0)
        df.columns = [c.replace(" ", "_") for c in df.columns]
        return df
    except Exception as exc:
        logger.error("Failed to fetch %s: %s", ticker, exc)
        return None


def fetch_all_stocks(period: str = "1y") -> dict[str, pd.DataFrame]:
    """
    Fetch OHLCV for all SET_STOCKS + SET index.

    Returns:
        Dict mapping clean symbol → DataFrame.
        Symbols that failed are omitted.
    """
    results: dict[str, pd.DataFrame] = {}
    all_symbols = GET_ALL_SYMBOLS_WITH_INDEX()

    tickers = [("^SET.BK" if s == "SET" else _to_yf_ticker(s)) for s in all_symbols]
    logger.info("Downloading %d tickers from yfinance...", len(tickers))

    try:
        raw = yf.download(
            tickers,
            period=period,
            group_by="ticker",
            progress=False,
            auto_adjust=True,
            threads=True,
        )
    except Exception as exc:
        logger.error("Batch download failed: %s", exc)
        return results

    for symbol, ticker in zip(all_symbols, tickers):
        try:
            if len(tickers) == 1:
                df = raw.copy()
            else:
                df = raw[ticker].copy() if ticker in raw.columns.get_level_values(0) else pd.DataFrame()

            if df.empty or df["Close"].dropna().empty:
                logger.warning("Empty data for %s", symbol)
                continue

            df = df.dropna(subset=["Close"])
            df.index = pd.to_datetime(df.index).tz_localize(None)
            df.index.name = "Date"
            df.columns = [c.replace(" ", "_") for c in df.columns]
            results[symbol] = df
        except Exception as exc:
            logger.warning("Could not process %s: %s", symbol, exc)

    logger.info("Fetched data for %d/%d symbols", len(results), len(all_symbols))
    return results


def GET_ALL_SYMBOLS_WITH_INDEX() -> list[str]:
    return SET_STOCKS + ["SET"]


def get_latest_price(symbol: str) -> Optional[dict]:
    """
    Get latest price info for a single symbol.

    Returns dict with: symbol, close, change_pct, volume, date
    """
    df = fetch_ohlcv(symbol, period="5d")
    if df is None or len(df) < 2:
        return None

    latest = df.iloc[-1]
    prev = df.iloc[-2]
    change_pct = ((latest["Close"] - prev["Close"]) / prev["Close"]) * 100

    return {
        "symbol": symbol,
        "close": round(float(latest["Close"]), 2),
        "change_pct": round(float(change_pct), 2),
        "volume": int(latest["Volume"]),
        "date": df.index[-1].strftime("%Y-%m-%d"),
    }


def tradingview_url(symbol: str) -> str:
    """Return TradingView chart URL for a SET symbol."""
    if symbol == "SET":
        return "https://www.tradingview.com/chart/?symbol=SET%3ASET"
    return f"https://www.tradingview.com/chart/?symbol=SET%3A{symbol}"
