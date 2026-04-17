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
    # Energy & Petrochemical
    "PTT", "PTTEP", "TOP", "IRPC", "SPRC", "BCP", "ESSO", "PTTGC",
    # Banking & Finance
    "BBL", "KBANK", "SCB", "KTB", "BAY", "TISCO", "KKP", "TCAP",
    "AEONTS", "MTC", "SAWAD", "TIDLOR",
    # Insurance
    "BLA", "THRE", "TQM",
    # Telecom & Tech
    "ADVANC", "TRUE", "INTUCH", "DTAC", "JMART", "JMT",
    # Property & Construction
    "LH", "QH", "SIRI", "AP", "PSH", "SC", "ORI", "SPALI",
    "CPN", "AMATA", "WHA", "LALIN", "NOBLE", "ANAN",
    # Commerce & Retail
    "CPALL", "BJC", "MAKRO", "ROBINS", "HMPRO", "COM7", "SINGER",
    # Food & Beverage
    "CPF", "TU", "OSP", "CBG", "OISHI", "ICHI", "SNP",
    # Healthcare & Pharma
    "BDMS", "BH", "BCH", "CHG", "PR9", "RJH", "VIH",
    # Industrial & Materials
    "DELTA", "HANA", "KCE", "SCC", "SCCC", "PYLON",
    "STEC", "ITD", "CK", "TPIPL",
    # Tourism, Hotel & Aviation
    "AOT", "THAI", "AAV", "MINT", "ERW", "CENTEL", "AWC",
    # Utilities & Power
    "GULF", "GPSC", "RATCH", "BGRIM", "EGCO", "EA", "BCPG",
    "SUPER", "SPCG",
    # Media & Entertainment
    "BEC", "WORK", "PLANB", "MAJOR", "VGI",
    # Agriculture
    "GFPT", "NMG", "TFG",
    # Logistics
    "WICE", "LEO", "SONIC",
    # Electronics & Auto
    "STANLY", "SAT", "AH",
]

# Alias map — common brand names → actual SET ticker
SYMBOL_ALIASES: dict[str, str] = {
    "SCG": "SCC",          # Siam Cement Group brand → SCC ticker
    "SIAM CEMENT": "SCC",
    "KASIKORN": "KBANK",
    "KASIKORNBANK": "KBANK",
    "KRUNGTHAI": "KTB",
    "BANGKOK BANK": "BBL",
    "SCB": "SCB",
    "KRUNGSRI": "BAY",
    "CENTRAL PATTANA": "CPN",
    "CENTRAL RETAIL": "CRC",
    "THAI UNION": "TU",
    "CHAROEN POKPHAND": "CPF",
    "TRUE CORP": "TRUE",
    "AIS": "ADVANC",
    "PTG": "PTG",
}

SET_INDEXES = ["^SET.BK"]  # SET Index

_STOCK_SET = set(SET_STOCKS)


def resolve_symbol(text: str) -> Optional[str]:
    """
    Resolve user input to a valid SET symbol.
    Handles aliases (SCG→SCC), case, and whitespace.
    Returns the symbol string or None if not found.
    """
    upper = text.upper().strip().replace("SET:", "")
    if upper in _STOCK_SET:
        return upper
    alias = SYMBOL_ALIASES.get(upper)
    if alias and alias in _STOCK_SET:
        return alias
    return None


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
    Fetch OHLCV for a single symbol.
    Tries SET Trade API first, falls back to yfinance.
    """
    # ── Primary: SET Trade Open API ──
    try:
        from settrade_client import get_ohlcv, is_api_available
        if is_api_available():
            # Map period: yfinance "1y" → settrade "1Y"
            period_map = {"1y": "1Y", "2y": "3Y", "5y": "5Y", "6mo": "6M", "3mo": "3M"}
            st_period = period_map.get(period, "1Y")
            df = get_ohlcv(symbol, period=st_period)
            if df is not None and not df.empty:
                logger.debug("Fetched %s from SET Trade API (%d rows)", symbol, len(df))
                return df
    except Exception as exc:
        logger.debug("SET Trade API failed for %s, falling back: %s", symbol, exc)

    # ── Fallback: yfinance ──
    ticker = "^SET.BK" if symbol == "SET" else _to_yf_ticker(symbol)
    try:
        df = yf.download(ticker, period=period, progress=False, auto_adjust=True)
        if df.empty:
            logger.warning("No data returned for %s", ticker)
            return None
        df.index = pd.to_datetime(df.index).tz_localize(None)
        df.index.name = "Date"
        if isinstance(df.columns, pd.MultiIndex):
            df.columns = df.columns.get_level_values(0)
        df.columns = [c.replace(" ", "_") for c in df.columns]
        return df
    except Exception as exc:
        logger.error("yfinance also failed for %s: %s", ticker, exc)
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
