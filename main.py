"""
main.py — Signalix FastAPI application.

Endpoints:
  GET  /health            — health check
  POST /webhook/line      — LINE Messaging API webhook
  POST /scan              — internal scan + notify (called by Cloud Scheduler)
"""

import hashlib
import hmac
import logging
import secrets
from datetime import datetime
from typing import Optional

import pytz
from fastapi import FastAPI, Header, HTTPException, Request, status
from fastapi.responses import JSONResponse
from pydantic import BaseModel

from analyzer import (
    MarketBreadth,
    StockSignal,
    compute_market_breadth,
    filter_signals,
    run_full_scan,
    scan_stock,
)
from config import get_settings
from data import fetch_ohlcv, get_stock_list, resolve_symbol, tradingview_url
from notifier import (
    broadcast_flex,
    build_help_card,
    build_market_breadth_card,
    build_single_stock_card,
    build_stock_list_carousel,
    build_welcome_card,
    get_webhook_handler,
    reply_flex,
    reply_text,
)

# ─── Firestore (optional — gracefully skipped if credentials missing) ──────────
try:
    from google.cloud import firestore as _fs
    _db = _fs.Client()
    FIRESTORE_AVAILABLE = True
except Exception:
    _db = None
    FIRESTORE_AVAILABLE = False

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("signalix")

BANGKOK_TZ = pytz.timezone("Asia/Bangkok")
app = FastAPI(title="Signalix", version="1.0.0")

# In-memory cache of last scan results (refreshed on each /scan call)
_last_signals: list[StockSignal] = []
_last_breadth: Optional[MarketBreadth] = None


@app.on_event("startup")
async def startup_scan():
    """Run a background scan on startup so cache is warm immediately."""
    import asyncio
    asyncio.create_task(_background_scan())


async def _background_scan():
    import asyncio
    await asyncio.sleep(5)  # let server finish booting first
    global _last_signals, _last_breadth
    try:
        logger.info("Running startup scan...")
        signals = run_full_scan()
        _last_signals = signals
        _last_breadth = compute_market_breadth(signals)
        logger.info("Startup scan complete: %d stocks", len(signals))
    except Exception as exc:
        logger.error("Startup scan failed: %s", exc)


# ─── Health ────────────────────────────────────────────────────────────────────

@app.get("/health")
async def health():
    return {
        "status": "ok",
        "time": datetime.now(BANGKOK_TZ).isoformat(),
        "firestore": FIRESTORE_AVAILABLE,
        "cached_stocks": len(_last_signals),
    }


@app.get("/test/settrade")
async def test_settrade():
    """Test SET Trade API connectivity and return sample data."""
    from settrade_client import is_api_available, get_stock_list_from_api, get_ohlcv, get_quote

    result: dict = {"api_available": False, "stock_count": 0, "sample_stocks": [], "sample_ohlcv": None, "sample_quote": None}

    result["api_available"] = is_api_available()
    if not result["api_available"]:
        result["error"] = "Cannot get access token — check SETTRADE_APP_ID / APP_SECRET credentials"
        return result

    # Stock list
    stocks = get_stock_list_from_api()
    result["stock_count"] = len(stocks)
    result["sample_stocks"] = stocks[:5]

    # OHLCV for PTT
    df = get_ohlcv("PTT", period="1M")
    if df is not None:
        result["sample_ohlcv"] = {
            "symbol": "PTT",
            "rows": len(df),
            "latest_date": str(df.index[-1].date()),
            "latest_close": round(float(df["Close"].iloc[-1]), 2),
        }

    # Real-time quote for PTT
    quote = get_quote("PTT")
    if quote:
        result["sample_quote"] = quote

    return result


# ─── Scan endpoint (Cloud Scheduler → POST /scan) ────────────────────────────

class ScanRequest(BaseModel):
    scan_type: str = "full"    # "full" | "breadth" | "breakout" | "vcp"
    broadcast: bool = True


@app.post("/scan")
async def scan(
    body: ScanRequest,
    x_scan_secret: Optional[str] = Header(default=None),
):
    settings = get_settings()
    if not secrets.compare_digest(
        x_scan_secret or "",
        settings.scan_secret,
    ):
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid scan secret")

    global _last_signals, _last_breadth

    logger.info("Running scan: type=%s broadcast=%s", body.scan_type, body.broadcast)
    signals = run_full_scan()
    breadth = compute_market_breadth(signals)

    _last_signals = signals
    _last_breadth = breadth

    # Persist to Firestore if available
    if FIRESTORE_AVAILABLE and _db:
        _save_breadth_to_firestore(breadth)

    if not body.broadcast:
        return {"scanned": len(signals), "breadth": breadth.__dict__}

    # Choose what to broadcast based on scan_type
    if body.scan_type == "breadth":
        _broadcast_breadth(breadth)

    elif body.scan_type == "breakout":
        breakouts = filter_signals(signals, pattern="breakout") + filter_signals(signals, pattern="ath_breakout")
        if breakouts:
            carousel = build_stock_list_carousel(breakouts[:10], "🚀 Breakout Stocks")
            broadcast_flex("Breakout Stocks Update", carousel)

    elif body.scan_type == "vcp":
        vcps = filter_signals(signals, pattern="vcp") + filter_signals(signals, pattern="vcp_low_cheat")
        if vcps:
            carousel = build_stock_list_carousel(vcps[:10], "🔍 VCP Setups")
            broadcast_flex("VCP Pattern Update", carousel)

    else:  # "full" — post-close full report
        _broadcast_full_report(breadth, signals)

    return {"scanned": len(signals), "broadcast": body.scan_type}


def _broadcast_breadth(breadth: MarketBreadth) -> None:
    card = build_market_breadth_card(breadth)
    broadcast_flex("ภาพรวมตลาด SET", card)


def _broadcast_full_report(breadth: MarketBreadth, signals: list[StockSignal]) -> None:
    # 1. Market breadth bubble
    _broadcast_breadth(breadth)

    # 2. Top Stage 2 stocks carousel
    stage2 = filter_signals(signals, stage=2)[:10]
    if stage2:
        carousel = build_stock_list_carousel(stage2, "🟢 Stage 2 Stocks")
        broadcast_flex("Stage 2 Stocks", carousel)

    # 3. Breakout + ATH breakout
    breakouts = (
        filter_signals(signals, pattern="breakout")
        + filter_signals(signals, pattern="ath_breakout")
    )[:10]
    if breakouts:
        carousel = build_stock_list_carousel(breakouts, "🚀 Breakout Stocks")
        broadcast_flex("Breakout Stocks", carousel)

    # 4. VCP setups
    vcps = (
        filter_signals(signals, pattern="vcp")
        + filter_signals(signals, pattern="vcp_low_cheat")
    )[:10]
    if vcps:
        carousel = build_stock_list_carousel(vcps, "🔍 VCP Setups")
        broadcast_flex("VCP Setups", carousel)


# ─── LINE Webhook ──────────────────────────────────────────────────────────────

@app.post("/webhook/line")
async def line_webhook(
    request: Request,
    x_line_signature: Optional[str] = Header(default=None),
):
    body = await request.body()
    body_str = body.decode("utf-8")

    # Validate LINE signature
    settings = get_settings()
    handler = get_webhook_handler()
    try:
        hash_val = hmac.new(
            settings.line_channel_secret.encode("utf-8"),
            body,
            hashlib.sha256,
        ).digest()
        import base64
        expected = base64.b64encode(hash_val).decode("utf-8")
        if not secrets.compare_digest(x_line_signature or "", expected):
            raise HTTPException(status_code=400, detail="Invalid signature")
    except Exception as exc:
        logger.warning("Signature validation error: %s", exc)
        raise HTTPException(status_code=400, detail="Signature error")

    import json
    try:
        payload = json.loads(body_str)
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid JSON")

    for event in payload.get("events", []):
        await _handle_line_event(event)

    return JSONResponse(content={"status": "ok"})


async def _handle_line_event(event: dict) -> None:
    event_type = event.get("type")
    reply_token = event.get("replyToken")
    source = event.get("source", {})
    user_id = source.get("userId")

    if event_type == "follow":
        await _handle_follow(user_id, reply_token)

    elif event_type == "unfollow":
        _unsubscribe_user(user_id)

    elif event_type == "message":
        msg = event.get("message", {})
        if msg.get("type") == "text":
            text = msg.get("text", "").strip()
            await _handle_text_query(text, reply_token, user_id)


async def _handle_follow(user_id: Optional[str], reply_token: Optional[str]) -> None:
    if not user_id:
        return
    display_name = _get_line_display_name(user_id) or "นักลงทุน"
    _subscribe_user(user_id, display_name)
    if reply_token:
        reply_flex(reply_token, "ยินดีต้อนรับสู่ Signalix!", build_welcome_card(display_name))


async def _handle_text_query(text: str, reply_token: Optional[str], user_id: Optional[str]) -> None:
    if not reply_token:
        return

    cmd = text.lower().strip()

    # ── Market Breadth ──
    if cmd in ("ตลาด", "market", "breadth", "ดัชนี"):
        breadth = _last_breadth
        if breadth is None:
            reply_text(reply_token, "กำลังโหลดข้อมูล กรุณารอสักครู่...")
            return
        reply_flex(reply_token, "ภาพรวมตลาด SET", build_market_breadth_card(breadth))

    # ── Stock lists by pattern/stage ──
    elif cmd in ("breakout", "break out", "บ้ระเอาท์"):
        signals = _get_signals_for(pattern="breakout") + _get_signals_for(pattern="ath_breakout")
        _reply_stock_list(reply_token, signals, "🚀 Breakout Stocks")

    elif cmd in ("ath", "all time high", "ath breakout"):
        signals = _get_signals_for(pattern="ath_breakout")
        _reply_stock_list(reply_token, signals, "🏆 ATH Breakout Stocks")

    elif cmd in ("vcp",):
        signals = _get_signals_for(pattern="vcp") + _get_signals_for(pattern="vcp_low_cheat")
        _reply_stock_list(reply_token, signals, "🔍 VCP Pattern Stocks")

    elif cmd in ("vcp low cheat", "vcp_low_cheat", "low cheat"):
        signals = _get_signals_for(pattern="vcp_low_cheat")
        _reply_stock_list(reply_token, signals, "🎯 VCP Low Cheat Stocks")

    elif cmd in ("stage2", "stage 2"):
        signals = _get_signals_for(stage=2)
        _reply_stock_list(reply_token, signals, "🟢 Stage 2 Stocks")

    elif cmd in ("stage1", "stage 1"):
        signals = _get_signals_for(stage=1)
        _reply_stock_list(reply_token, signals, "⚪ Stage 1 Stocks")

    elif cmd in ("stage3", "stage 3"):
        signals = _get_signals_for(stage=3)
        _reply_stock_list(reply_token, signals, "🟡 Stage 3 Stocks")

    elif cmd in ("stage4", "stage 4"):
        signals = _get_signals_for(stage=4)
        _reply_stock_list(reply_token, signals, "🔴 Stage 4 Stocks")

    elif cmd in ("consolidating", "consolidate", "coil"):
        signals = _get_signals_for(pattern="consolidating")
        _reply_stock_list(reply_token, signals, "⚙️ Consolidating Stocks")

    # ── Help ──
    elif cmd in ("help", "ช่วย", "คำสั่ง", "?"):
        reply_flex(reply_token, "คำสั่ง Signalix", build_help_card())

    # ── Single stock lookup ──
    else:
        symbol = resolve_symbol(text)
        if symbol:
            _reply_single_stock(reply_token, symbol)
        else:
            reply_text(
                reply_token,
                f'ไม่พบหุ้น "{text.upper()}"\n\nเช็คชื่อ ticker ที่ถูกต้อง เช่น:\n• SCC (ไม่ใช่ SCG)\n• ADVANC (ไม่ใช่ AIS)\n\nพิมพ์ help เพื่อดูคำสั่งทั้งหมดครับ',
            )


def _get_signals_for(pattern: Optional[str] = None, stage: Optional[int] = None) -> list[StockSignal]:
    return filter_signals(_last_signals, pattern=pattern, stage=stage)


def _reply_stock_list(reply_token: str, signals: list[StockSignal], title: str) -> None:
    if not signals:
        reply_text(reply_token, f"ไม่มีหุ้นใน {title} ขณะนี้")
        return
    carousel = build_stock_list_carousel(signals[:10], title)
    reply_flex(reply_token, title, carousel)


def _reply_single_stock(reply_token: str, symbol: str) -> None:
    # Check cache first
    cached = next((s for s in _last_signals if s.symbol == symbol), None)
    if cached:
        reply_flex(reply_token, f"วิเคราะห์ {symbol}", build_single_stock_card(cached))
        return

    # Live fetch
    reply_text(reply_token, f"กำลังวิเคราะห์ {symbol}...")
    df = fetch_ohlcv(symbol)
    if df is None:
        reply_text(reply_token, f"ไม่พบข้อมูล {symbol}")
        return

    from analyzer import scan_stock
    signal = scan_stock(symbol, df)
    if signal is None:
        reply_text(reply_token, f"ข้อมูลไม่เพียงพอสำหรับ {symbol}")
        return

    reply_flex(reply_token, f"วิเคราะห์ {symbol}", build_single_stock_card(signal))


# ─── Firestore helpers ─────────────────────────────────────────────────────────

def _subscribe_user(user_id: str, display_name: str) -> None:
    if not FIRESTORE_AVAILABLE or not _db:
        logger.info("Firestore unavailable — skipping subscribe for %s", user_id)
        return
    _db.collection("users").document(user_id).set(
        {
            "displayName": display_name,
            "subscribed": True,
            "followedAt": datetime.now(BANGKOK_TZ).isoformat(),
        },
        merge=True,
    )


def _unsubscribe_user(user_id: Optional[str]) -> None:
    if not user_id or not FIRESTORE_AVAILABLE or not _db:
        return
    _db.collection("users").document(user_id).set({"subscribed": False}, merge=True)


def _get_all_subscriber_ids() -> list[str]:
    if not FIRESTORE_AVAILABLE or not _db:
        return []
    docs = _db.collection("users").where("subscribed", "==", True).stream()
    return [doc.id for doc in docs]


def _save_breadth_to_firestore(breadth: MarketBreadth) -> None:
    if not FIRESTORE_AVAILABLE or not _db:
        return
    _db.collection("market_breadth").add(breadth.__dict__)


def _get_line_display_name(user_id: str) -> Optional[str]:
    """Fetch LINE display name via the profile API."""
    settings = get_settings()
    if not settings.line_channel_access_token:
        return None
    try:
        import httpx
        resp = httpx.get(
            f"https://api.line.me/v2/bot/profile/{user_id}",
            headers={"Authorization": f"Bearer {settings.line_channel_access_token}"},
            timeout=5,
        )
        if resp.status_code == 200:
            return resp.json().get("displayName")
    except Exception:
        pass
    return None
