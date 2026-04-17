"""
settrade_client.py — SET Trade Open API client.

Credentials required (store in Secret Manager / .env):
  SETTRADE_APP_ID, SETTRADE_APP_SECRET, SETTRADE_BROKER_ID, SETTRADE_APP_CODE

Provides:
  - get_stock_list()       → all SET/MAI listed symbols
  - get_ohlcv()            → historical OHLCV (daily)
  - get_quote()            → real-time quote
  - get_sector_list()      → sector classification
"""

import logging
import time
from typing import Optional

import httpx
import pandas as pd

from config import get_settings

logger = logging.getLogger(__name__)

BASE_URL = "https://openapi.settrade.com"

_token_cache: dict = {"access_token": None, "expires_at": 0}


# ─── Authentication ───────────────────────────────────────────────────────────

def _get_token() -> Optional[str]:
    """Get a valid access token, refreshing if expired."""
    now = time.time()
    if _token_cache["access_token"] and _token_cache["expires_at"] > now + 60:
        return _token_cache["access_token"]

    settings = get_settings()
    if not settings.settrade_app_id or not settings.settrade_app_secret:
        logger.warning("SET Trade API credentials not configured")
        return None

    try:
        resp = httpx.post(
            f"{BASE_URL}/api/RTS/1.0/Token/getToken",
            json={
                "appId": settings.settrade_app_id,
                "appSecret": settings.settrade_app_secret,
                "appCode": settings.settrade_app_code,
                "brokerId": settings.settrade_broker_id,
            },
            timeout=10,
        )
        resp.raise_for_status()
        data = resp.json()

        token = data.get("access_token") or data.get("accessToken")
        expires_in = int(data.get("expires_in", 1800))

        if token:
            _token_cache["access_token"] = token
            _token_cache["expires_at"] = now + expires_in
            logger.info("SET Trade API token refreshed (expires in %ds)", expires_in)
            return token
        else:
            logger.error("No token in response: %s", data)
            return None

    except Exception as exc:
        logger.error("Failed to get SET Trade API token: %s", exc)
        return None


def _headers() -> dict:
    token = _get_token()
    if not token:
        return {}
    return {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}


# ─── Stock list ───────────────────────────────────────────────────────────────

def get_stock_list_from_api() -> list[dict]:
    """
    Fetch all listed securities from SET Trade API.

    Returns list of dicts with keys: symbol, nameTH, nameEN, sector, market
    Falls back to empty list on failure.
    """
    try:
        resp = httpx.get(
            f"{BASE_URL}/api/RTS/1.0/Market/SecurityList",
            headers=_headers(),
            params={"securityType": "S", "market": "SET"},
            timeout=15,
        )
        resp.raise_for_status()
        data = resp.json()
        securities = data.get("securityList") or data.get("data") or data

        result = []
        for s in securities:
            result.append({
                "symbol": s.get("symbol") or s.get("securityId", ""),
                "name_th": s.get("nameTH") or s.get("securityNameTH", ""),
                "name_en": s.get("nameEN") or s.get("securityNameEN", ""),
                "sector": s.get("industryName") or s.get("sector", ""),
                "market": s.get("market", "SET"),
            })
        logger.info("Fetched %d securities from SET Trade API", len(result))
        return result

    except Exception as exc:
        logger.error("Failed to fetch stock list: %s", exc)
        return []


# ─── Historical OHLCV ─────────────────────────────────────────────────────────

def get_ohlcv(symbol: str, period: str = "1Y") -> Optional[pd.DataFrame]:
    """
    Fetch historical daily OHLCV from SET Trade API.

    Args:
        symbol: SET ticker, e.g. "PTT"
        period: "1M" | "3M" | "6M" | "1Y" | "3Y" | "5Y"

    Returns DataFrame with [Open, High, Low, Close, Volume] indexed by Date.
    """
    try:
        resp = httpx.get(
            f"{BASE_URL}/api/RTS/1.0/Market/SecurityDailyInfo",
            headers=_headers(),
            params={"symbol": symbol, "period": period},
            timeout=15,
        )
        resp.raise_for_status()
        data = resp.json()
        rows = data.get("dailyList") or data.get("data") or []

        if not rows:
            return None

        df = pd.DataFrame(rows)

        # Normalise column names — API may use different casing
        col_map = {}
        for col in df.columns:
            lower = col.lower()
            if "date" in lower:
                col_map[col] = "Date"
            elif lower in ("open", "o"):
                col_map[col] = "Open"
            elif lower in ("high", "h"):
                col_map[col] = "High"
            elif lower in ("low", "l"):
                col_map[col] = "Low"
            elif lower in ("close", "c", "last", "prior"):
                col_map[col] = "Close"
            elif "vol" in lower:
                col_map[col] = "Volume"
        df = df.rename(columns=col_map)

        if "Date" not in df.columns:
            return None

        df["Date"] = pd.to_datetime(df["Date"])
        df = df.set_index("Date").sort_index()

        for col in ["Open", "High", "Low", "Close", "Volume"]:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce")

        df = df.dropna(subset=["Close"])
        return df

    except Exception as exc:
        logger.error("Failed to fetch OHLCV for %s: %s", symbol, exc)
        return None


# ─── Real-time quote ──────────────────────────────────────────────────────────

def get_quote(symbol: str) -> Optional[dict]:
    """
    Fetch real-time quote for a single symbol.

    Returns dict with: symbol, last, change, change_pct, volume, bid, ask
    """
    try:
        resp = httpx.get(
            f"{BASE_URL}/api/RTS/1.0/Market/SecurityQuote",
            headers=_headers(),
            params={"symbol": symbol},
            timeout=10,
        )
        resp.raise_for_status()
        data = resp.json()
        q = data.get("quote") or data.get("data") or data

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
        logger.error("Failed to fetch quote for %s: %s", symbol, exc)
        return None


# ─── Sector list ─────────────────────────────────────────────────────────────

def get_sector_list() -> list[dict]:
    """Fetch all SET sectors/industries."""
    try:
        resp = httpx.get(
            f"{BASE_URL}/api/RTS/1.0/Market/SectorList",
            headers=_headers(),
            timeout=10,
        )
        resp.raise_for_status()
        data = resp.json()
        return data.get("sectorList") or data.get("data") or []
    except Exception as exc:
        logger.error("Failed to fetch sector list: %s", exc)
        return []


# ─── Health check ─────────────────────────────────────────────────────────────

def is_api_available() -> bool:
    """Check if SET Trade API is reachable and credentials are valid."""
    return _get_token() is not None
