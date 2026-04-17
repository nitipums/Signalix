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
    SectorSummary,
    StockSignal,
    compute_market_breadth,
    compute_sector_trends,
    filter_signals,
    run_full_scan,
    scan_stock,
)
from config import get_settings
from data import fetch_indexes, fetch_ohlcv, get_stock_list, load_ath_cache, resolve_symbol, sync_ath_to_firestore, tradingview_url
from notifier import (
    broadcast_flex,
    build_compact_stock_carousel,
    build_explain_card,
    build_help_card,
    build_index_carousel,
    build_market_breadth_card,
    build_remaining_symbols_text,
    build_sector_carousel,
    build_single_stock_card,
    build_stock_list_carousel,
    build_welcome_card,
    get_webhook_handler,
    reply_flex,
    reply_flex_and_text,
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
_last_indexes: dict[str, dict] = {}
_last_sector_trends: list[SectorSummary] = []
_ath_cache: dict[str, float] = {}

# Explanation texts for ⓘ buttons
_EXPLANATIONS: dict[str, str] = {
    "explain stage1": (
        "Stage 1 – Basing / Neglect\n\n"
        "หุ้นอยู่ในระยะสะสม ราคาเคลื่อนไหวแคบๆ หลังจากลงมา\n"
        "MA200 เริ่มแบนราบหรือหันขึ้น ยังไม่มีสัญญาณซื้อชัดเจน\n\n"
        "✅ รอดูการ breakout ออกจาก base"
    ),
    "explain stage2": (
        "Stage 2 – Advancing / Uptrend\n\n"
        "ระยะที่น่าลงทุนที่สุดตาม Minervini Template:\n"
        "• ราคา > MA150 > MA200\n"
        "• MA200 กำลังขึ้น (rising)\n"
        "• ราคาสูงกว่า 52w low อย่างน้อย 25%\n"
        "• ราคาอยู่ภายใน 25% จาก 52w high\n\n"
        "✅ โซนซื้อที่มีโอกาสสูงสุด"
    ),
    "explain stage3": (
        "Stage 3 – Distribution / Topping\n\n"
        "หุ้นเริ่มหยุด uptrend ราคาหลุด MA150 แต่ยังอยู่เหนือ MA200\n"
        "Smart money เริ่มขาย ควรระวังสัญญาณ top\n\n"
        "⚠️ ลดการถือหรือขายออกบางส่วน"
    ),
    "explain stage4": (
        "Stage 4 – Declining / Downtrend\n\n"
        "หุ้นอยู่ในขาลงชัดเจน ราคาต่ำกว่า MA150 และ MA200\n"
        "MA200 กำลังลง (declining)\n\n"
        "❌ หลีกเลี่ยงการซื้อ รอจนกว่าจะเห็น Stage 1 base"
    ),
    "explain stage": (
        "Stage Analysis (Minervini)\n\n"
        "แบ่งหุ้นออกเป็น 4 stage ตามตำแหน่งของ MA50/150/200:\n\n"
        "Stage 1 – Basing (สะสม)\n"
        "Stage 2 – Uptrend ✅ (น่าซื้อ)\n"
        "Stage 3 – Topping ⚠️ (ระวัง)\n"
        "Stage 4 – Downtrend ❌ (หลีกเลี่ยง)\n\n"
        "พิมพ์ 'explain stage2' เพื่อดูรายละเอียดแต่ละ stage"
    ),
    "explain breakout": (
        "Breakout\n\n"
        "ราคาปิดสูงกว่า resistance สูงสุด 52 สัปดาห์\n"
        "พร้อม Volume สูงกว่าค่าเฉลี่ย 20 วัน อย่างน้อย 1.4x\n\n"
        "✅ สัญญาณซื้อหลักในระบบ Minervini"
    ),
    "explain ath_breakout": (
        "ATH Breakout (All-Time High)\n\n"
        "หุ้น breakout และราคาอยู่ใกล้หรือสูงกว่า All-Time High\n"
        "สัญญาณแข็งแกร่งมาก — ไม่มี overhead supply\n\n"
        "🏆 หุ้นที่ทำ ATH breakout มักวิ่งต่อได้ดี"
    ),
    "explain vcp": (
        "VCP – Volatility Contraction Pattern\n\n"
        "รูปแบบที่ราคาหดตัวแคบลงเรื่อยๆ (3+ contractions)\n"
        "Volume ลดลงในแต่ละ contraction\n\n"
        "ตัวอย่าง: ลง 15% → 10% → 6% → 3%\n\n"
        "✅ Entry point เมื่อราคา breakout จาก contraction สุดท้าย"
    ),
    "explain vcp_low_cheat": (
        "VCP Low Cheat\n\n"
        "Entry แบบ aggressive ใน VCP pattern\n"
        "ราคาอยู่ใกล้ low ของ contraction สุดท้าย + volume แห้ง\n\n"
        "✅ Risk ต่ำ reward สูง แต่ต้องการ stop loss แคบ"
    ),
    "explain consolidating": (
        "Consolidating\n\n"
        "หุ้อยู่ใน base หรือพักตัว ATR กำลังหด\n"
        "ยังไม่ถึงจุด breakout แต่เป็น setup ที่ดีสำหรับอนาคต\n\n"
        "⚙️ จับตาดู รอสัญญาณ breakout"
    ),
    "explain going_down": (
        "Going Down\n\n"
        "หุ้ออยู่ใน Stage 4 downtrend ชัดเจน\n\n"
        "❌ ไม่แนะนำให้ซื้อ"
    ),
    "explain score": (
        "Strength Score (0–100)\n\n"
        "คะแนนรวมจาก:\n"
        "• Stage (Stage 2 = +40 คะแนน)\n"
        "• Pattern (ATH Breakout = +25, Breakout = +20, VCP = +15)\n"
        "• Volume Ratio bonus (max +15)\n"
        "• Proximity to 52W High (max +20)\n\n"
        "คะแนนสูง = หุ้นแข็งแกร่งและมี setup ที่ดี"
    ),
}


@app.on_event("startup")
async def startup_scan():
    """Run a background scan on startup so cache is warm immediately."""
    import asyncio
    asyncio.create_task(_background_scan())


async def _background_scan():
    import asyncio
    await asyncio.sleep(5)  # let server finish booting first
    global _last_signals, _last_breadth, _last_indexes, _last_sector_trends, _ath_cache
    try:
        logger.info("Running startup scan...")
        if FIRESTORE_AVAILABLE and _db:
            _ath_cache = load_ath_cache(_db)
            logger.info("ATH cache loaded: %d entries", len(_ath_cache))
        signals, all_data = run_full_scan(ath_cache=_ath_cache)
        _last_signals = signals
        _last_breadth = compute_market_breadth(signals, index_df=all_data.get("SET"))
        _last_sector_trends = compute_sector_trends(signals)
        _last_indexes = fetch_indexes()
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
    """Test SET Trade API connectivity and return diagnostic info."""
    from settrade_client import _get_investor

    result: dict = {"api_available": False}

    investor = _get_investor()
    if not investor:
        result["error"] = "Cannot init Investor — check credentials"
        return result

    result["api_available"] = True
    market = investor.MarketData()

    # ── Quote ──
    try:
        q = market.get_quote_symbol("PTT")
        result["quote_PTT"] = q
    except Exception as e:
        result["quote_error"] = str(e)

    # ── Candlestick (parsed) ──
    try:
        from settrade_client import get_ohlcv
        df = get_ohlcv("PTT", period="1M")
        if df is not None:
            result["ohlcv_PTT"] = {
                "rows": len(df),
                "latest_date": str(df.index[-1].date()),
                "latest_close": round(float(df["Close"].iloc[-1]), 2),
                "sample": df.tail(3).reset_index().to_dict(orient="records"),
            }
        else:
            result["ohlcv_PTT"] = None
    except Exception as e:
        result["ohlcv_error"] = str(e)

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

    global _last_signals, _last_breadth, _last_indexes, _last_sector_trends, _ath_cache

    logger.info("Running scan: type=%s broadcast=%s", body.scan_type, body.broadcast)
    if FIRESTORE_AVAILABLE and _db and not _ath_cache:
        _ath_cache = load_ath_cache(_db)
    signals, all_data = run_full_scan(ath_cache=_ath_cache)
    breadth = compute_market_breadth(signals, index_df=all_data.get("SET"))
    sector_trends = compute_sector_trends(signals)
    indexes = fetch_indexes()

    _last_signals = signals
    _last_breadth = breadth
    _last_sector_trends = sector_trends
    _last_indexes = indexes

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
            carousel = build_compact_stock_carousel(breakouts[:10], "🚀 Breakout Stocks")
            broadcast_flex("Breakout Stocks Update", carousel)

    elif body.scan_type == "vcp":
        vcps = filter_signals(signals, pattern="vcp") + filter_signals(signals, pattern="vcp_low_cheat")
        if vcps:
            carousel = build_compact_stock_carousel(vcps[:10], "🔍 VCP Setups")
            broadcast_flex("VCP Pattern Update", carousel)

    else:  # "full" — post-close full report
        _broadcast_full_report(breadth, signals)

    return {"scanned": len(signals), "broadcast": body.scan_type}


@app.post("/sync_ath")
async def sync_ath_endpoint(
    x_scan_secret: Optional[str] = Header(default=None),
    chunk: int = 0,
):
    """One-time ATH sync endpoint. Call with ?chunk=0, ?chunk=1, ... until synced=0."""
    settings = get_settings()
    if not secrets.compare_digest(x_scan_secret or "", settings.scan_secret):
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid scan secret")
    if not FIRESTORE_AVAILABLE or not _db:
        raise HTTPException(status_code=503, detail="Firestore not available")

    global _ath_cache
    symbols = get_stock_list()
    synced = sync_ath_to_firestore(_db, symbols, chunk=chunk)
    _ath_cache.update(synced)
    total_chunks = (len(symbols) + 99) // 100
    return {"synced": len(synced), "chunk": chunk, "total_chunks": total_chunks, "next_chunk": chunk + 1 if chunk + 1 < total_chunks else None}


def _broadcast_breadth(breadth: MarketBreadth) -> None:
    card = build_market_breadth_card(breadth)
    broadcast_flex("ภาพรวมตลาด SET", card)


def _broadcast_full_report(breadth: MarketBreadth, signals: list[StockSignal]) -> None:
    # 1. Market breadth bubble
    _broadcast_breadth(breadth)

    # 2. Top Stage 2 stocks — compact carousel for notifications
    stage2 = filter_signals(signals, stage=2)[:10]
    if stage2:
        carousel = build_compact_stock_carousel(stage2, "🟢 Stage 2 Stocks")
        broadcast_flex("Stage 2 Stocks", carousel)

    # 3. Breakout + ATH breakout
    breakouts = (
        filter_signals(signals, pattern="breakout")
        + filter_signals(signals, pattern="ath_breakout")
    )[:10]
    if breakouts:
        carousel = build_compact_stock_carousel(breakouts, "🚀 Breakout Stocks")
        broadcast_flex("Breakout Stocks", carousel)

    # 4. VCP setups
    vcps = (
        filter_signals(signals, pattern="vcp")
        + filter_signals(signals, pattern="vcp_low_cheat")
    )[:10]
    if vcps:
        carousel = build_compact_stock_carousel(vcps, "🔍 VCP Setups")
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

    # ── Explain metric ⓘ ──
    if cmd.startswith("explain "):
        explanation = _EXPLANATIONS.get(cmd)
        if explanation:
            metric_name = cmd.replace("explain ", "")
            reply_flex(reply_token, f"ℹ️ {metric_name}", build_explain_card(metric_name, explanation))
        else:
            reply_text(reply_token, f'ไม่พบคำอธิบายสำหรับ "{cmd.replace("explain ", "")}"')

    # ── Market Breadth ──
    elif cmd in ("ตลาด", "market", "breadth"):
        breadth = _last_breadth
        if breadth is None:
            reply_text(reply_token, "กำลังโหลดข้อมูล กรุณารอสักครู่...")
            return
        reply_flex(reply_token, "ภาพรวมตลาด SET", build_market_breadth_card(breadth))

    # ── Key Indexes ──
    elif cmd in ("index", "indexes", "ดัชนี", "ดัชนีหุ้น"):
        if not _last_indexes:
            reply_text(reply_token, "กำลังโหลดข้อมูลดัชนี...")
            return
        carousel = build_index_carousel(_last_indexes)
        reply_flex(reply_token, "ดัชนีหุ้นไทย", carousel)

    # ── Sector Trends ──
    elif cmd in ("sector", "sectors", "เซกเตอร์", "กลุ่มหุ้น"):
        if not _last_sector_trends:
            reply_text(reply_token, "กำลังโหลดข้อมูลกลุ่มหุ้น...")
            return
        carousel = build_sector_carousel(_last_sector_trends)
        reply_flex(reply_token, "แนวโน้มกลุ่มอุตสาหกรรม", carousel)

    # ── Watchlist: view ──
    elif cmd in ("watchlist", "รายการโปรด", "watch"):
        if not user_id:
            reply_text(reply_token, "ไม่สามารถระบุผู้ใช้ได้")
            return
        wl = _get_user_watchlist(user_id)
        if not wl:
            reply_text(reply_token, "Watchlist ว่างเปล่า\nพิมพ์ add {ชื่อหุ้น} เพื่อเพิ่มหุ้น\nเช่น: add PTT")
            return
        cached = {s.symbol: s for s in _last_signals}
        wl_signals = [cached[sym] for sym in wl if sym in cached]
        uncached = [sym for sym in wl if sym not in cached]
        for sym in uncached[:5]:
            df = fetch_ohlcv(sym)
            if df is not None:
                sig = scan_stock(sym, df, ath_override=_ath_cache.get(sym))
                if sig:
                    wl_signals.append(sig)
        if not wl_signals:
            reply_text(reply_token, "ไม่มีข้อมูลหุ้นใน Watchlist ขณะนี้")
            return
        _reply_stock_list(reply_token, wl_signals, f"📌 Watchlist ({len(wl_signals)} หุ้น)")

    # ── Watchlist: add ──
    elif cmd.startswith("add ") or cmd.startswith("เพิ่ม "):
        parts = text.split(" ", 1)
        raw = parts[1].strip() if len(parts) > 1 else ""
        symbol = resolve_symbol(raw)
        if not symbol:
            reply_text(reply_token, f'ไม่พบหุ้น "{raw.upper()}"')
            return
        if not user_id:
            reply_text(reply_token, "ไม่สามารถระบุผู้ใช้ได้")
            return
        ok, msg = _add_to_watchlist(user_id, symbol)
        reply_text(reply_token, msg)

    # ── Watchlist: remove ──
    elif cmd.startswith("remove ") or cmd.startswith("ลบ "):
        parts = text.split(" ", 1)
        raw = parts[1].strip() if len(parts) > 1 else ""
        symbol = resolve_symbol(raw)
        if not symbol:
            reply_text(reply_token, f'ไม่พบหุ้น "{raw.upper()}"')
            return
        if not user_id:
            reply_text(reply_token, "ไม่สามารถระบุผู้ใช้ได้")
            return
        ok, msg = _remove_from_watchlist(user_id, symbol)
        reply_text(reply_token, msg)

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
    if len(signals) > 10:
        extra = build_remaining_symbols_text(signals, title)
        reply_flex_and_text(reply_token, title, carousel, extra)
    else:
        reply_flex(reply_token, title, carousel)


def _reply_single_stock(reply_token: str, symbol: str) -> None:
    # Always fetch fresh data (Settrade API → yfinance fallback)
    df = fetch_ohlcv(symbol)
    if df is None:
        reply_text(reply_token, f"ไม่พบข้อมูล {symbol}")
        return
    signal = scan_stock(symbol, df, ath_override=_ath_cache.get(symbol))
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


# ─── Watchlist helpers ─────────────────────────────────────────────────────────

def _get_user_watchlist(user_id: str) -> list[str]:
    if not FIRESTORE_AVAILABLE or not _db:
        return []
    try:
        doc = _db.collection("users").document(user_id).get()
        if not doc.exists:
            return []
        return doc.to_dict().get("watchlist", [])
    except Exception:
        return []


def _add_to_watchlist(user_id: str, symbol: str) -> tuple[bool, str]:
    if not FIRESTORE_AVAILABLE or not _db:
        return False, "ไม่สามารถบันทึกได้ขณะนี้"
    wl = _get_user_watchlist(user_id)
    if symbol in wl:
        return False, f"{symbol} อยู่ใน Watchlist แล้ว ✅"
    if len(wl) >= 30:
        return False, "Watchlist เต็มแล้ว (สูงสุด 30 หุ้น)"
    wl.append(symbol)
    _db.collection("users").document(user_id).set({"watchlist": wl}, merge=True)
    return True, f"เพิ่ม {symbol} เข้า Watchlist แล้ว ✅\nWatchlist ของคุณมี {len(wl)} หุ้น"


def _remove_from_watchlist(user_id: str, symbol: str) -> tuple[bool, str]:
    if not FIRESTORE_AVAILABLE or not _db:
        return False, "ไม่สามารถแก้ไขได้ขณะนี้"
    wl = _get_user_watchlist(user_id)
    if symbol not in wl:
        return False, f"{symbol} ไม่อยู่ใน Watchlist"
    wl.remove(symbol)
    _db.collection("users").document(user_id).set({"watchlist": wl}, merge=True)
    return True, f"ลบ {symbol} ออกจาก Watchlist แล้ว"
