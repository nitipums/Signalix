"""
main.py — Signalix FastAPI application.

Endpoints:
  GET  /health            — health check
  POST /webhook/line      — LINE Messaging API webhook
  POST /scan              — internal scan + notify (called by Cloud Scheduler)
"""

import asyncio
import functools
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
    analyze_index,
    compute_market_breadth,
    compute_sector_trends,
    filter_signals,
    run_full_scan,
    scan_stock,
)
from config import get_settings
from data import (
    SECTOR_MAP, append_new_candles_to_bq, BQ_AVAILABLE,
    fetch_indexes_with_history, fetch_latest_candles, fetch_ohlcv,
    fetch_ohlcv_settrade, get_cached_fundamentals, get_fundamentals,
    get_stock_list, init_bq, load_ath_cache, load_ath_from_bq,
    increment_stage4_views, load_breakout_review,
    load_scan_state, load_signal_from_firestore, load_signals_from_firestore,
    log_breakout, resolve_symbol,
    save_scan_state, save_signals_to_firestore,
    sync_ath_to_firestore, tradingview_url, update_user_score,
)
from notifier import (
    broadcast_flex,
    broadcast_text,
    build_compact_stock_carousel,
    build_explain_card,
    build_guide_carousel,
    build_help_card,
    build_index_carousel,
    build_market_breadth_card,
    build_pattern_detail_card,
    build_ranked_stock_list_bubble,
    build_sector_carousel,
    build_sector_overview_card,
    build_single_stock_card,
    build_stage_cycle_card,
    build_stage_picker_card,
    build_performance_review_card,
    build_score_card,
    build_watchlist_carousel,
    build_watchlist_stock_card,
    build_welcome_card,
    get_webhook_handler,
    init_notifier,
    multicast_flex,
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

# ─── BigQuery (optional) ───────────────────────────────────────────────────────
_bq_settings = get_settings()
if _bq_settings.gcp_project_id:
    init_bq(_bq_settings.gcp_project_id, _bq_settings.bq_dataset)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("signalix")

BANGKOK_TZ = pytz.timezone("Asia/Bangkok")
app = FastAPI(title="Signalix", version="1.0.0")


def _analyze_index_dfs(index_dfs: dict) -> dict:
    """Run analyze_index on each fetched DataFrame; fall back to fetch_indexes for missing ones."""
    result = {}
    for name, df in index_dfs.items():
        if df is not None and len(df) >= 30:
            result[name] = analyze_index(df, name)
    return result

# In-memory cache of last scan results (refreshed on each /scan call)
_last_signals: list[StockSignal] = []
_last_breadth: Optional[MarketBreadth] = None
_last_breadth_card: Optional[dict] = None  # pre-built, invalidated when _last_breadth changes
_last_scan_time: Optional[datetime] = None   # when _last_signals was last populated
_last_indexes: dict[str, dict] = {}
_last_sector_trends: list[SectorSummary] = []
_ath_cache: dict[str, float] = {}

# Static card caches (built once, never change between scans)
_help_card_cache: Optional[dict] = None
_guide_carousel_cache: Optional[dict] = None

_CACHE_TTL_MINUTES = 15

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
    "explain volume_ratio": (
        "Volume Ratio\n\n"
        "= ปริมาณการซื้อขายวันนี้ ÷ ค่าเฉลี่ย 20 วัน\n\n"
        "1.0x = ปกติ\n"
        "1.4x+ = Volume สูงกว่าปกติ (สัญญาณ breakout)\n"
        "2.0x+ = Volume สูงมาก\n\n"
        "ใช้ยืนยัน breakout — ราคาขึ้นพร้อม volume สูง = สัญญาณแข็งแกร่ง"
    ),
}


@app.on_event("startup")
async def startup_event():
    """Initialize singletons then warm in-memory cache from Firestore (no scan on startup)."""
    loop = asyncio.get_running_loop()
    settings = get_settings()
    if settings.gcp_project_id:
        await loop.run_in_executor(None, init_bq, settings.gcp_project_id, settings.bq_dataset)
    init_notifier(settings.line_channel_access_token)
    asyncio.create_task(_warm_from_firestore())


async def _warm_from_firestore():
    """Load all cached state from Firestore. No scan is run — data comes from last scheduled scan."""
    await asyncio.sleep(2)  # let server finish booting first
    global _last_signals, _last_breadth, _last_breadth_card, _last_scan_time, _last_indexes, _last_sector_trends, _ath_cache
    loop = asyncio.get_running_loop()
    try:
        # Load ATH non-blocking: prefer BQ, fall back to Firestore
        if BQ_AVAILABLE:
            _ath_cache = await loop.run_in_executor(None, load_ath_from_bq)
            logger.info("ATH cache loaded from BQ: %d entries", len(_ath_cache))
        elif FIRESTORE_AVAILABLE and _db:
            _ath_cache = await loop.run_in_executor(None, load_ath_cache, _db)
            logger.info("ATH cache loaded from Firestore: %d entries", len(_ath_cache))

        if not FIRESTORE_AVAILABLE or not _db:
            logger.warning("Firestore unavailable — cache will be empty until first /scan")
            return

        # Load scan_state (breadth + indexes + sector_trends) and signals in parallel
        state_fut = loop.run_in_executor(None, load_scan_state, _db)
        sigs_fut = loop.run_in_executor(None, load_signals_from_firestore, _db)
        state, signals = await asyncio.gather(state_fut, sigs_fut)

        if state:
            _last_breadth = state["breadth"]
            _last_breadth_card = build_market_breadth_card(_last_breadth, state["sector_trends"])
            _last_indexes = state["indexes"]
            _last_sector_trends = state["sector_trends"]
            try:
                _last_scan_time = datetime.fromisoformat(state["scanned_at"]).replace(tzinfo=BANGKOK_TZ)
            except Exception:
                pass
        if signals:
            _last_signals = signals
            # Fallback: compute derived caches from signals when scan_state is missing/incomplete
            if not _last_breadth:
                _last_breadth = compute_market_breadth(signals)
                _last_breadth_card = build_market_breadth_card(_last_breadth, _last_sector_trends)
                logger.info("Computed breadth from %d signals (scan_state missing)", len(signals))
            if not _last_sector_trends:
                _last_sector_trends = compute_sector_trends(signals)
                _last_breadth_card = build_market_breadth_card(_last_breadth, _last_sector_trends)
                logger.info("Computed sector_trends from signals (scan_state missing)")

        logger.info("Warmed from Firestore: %d signals, breadth=%s, indexes=%d, sectors=%d",
                    len(_last_signals), "ok" if _last_breadth else "missing",
                    len(_last_indexes), len(_last_sector_trends))
    except Exception as exc:
        logger.error("_warm_from_firestore failed: %s", exc)


# ─── Health ────────────────────────────────────────────────────────────────────

@app.get("/health")
async def health():
    from data import BQ_AVAILABLE as _bq_avail
    return {
        "status": "ok",
        "time": datetime.now(BANGKOK_TZ).isoformat(),
        "firestore": FIRESTORE_AVAILABLE,
        "bigquery": _bq_avail,
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
    mode: str = "full"         # "full" (fetch all + BQ write) | "intraday" (BQ history + latest candle only)


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

    global _last_signals, _last_breadth, _last_breadth_card, _last_scan_time, _last_indexes, _last_sector_trends, _ath_cache

    logger.info("Running scan: type=%s mode=%s broadcast=%s", body.scan_type, body.mode, body.broadcast)
    loop = asyncio.get_running_loop()

    if not _ath_cache:
        if BQ_AVAILABLE:
            _ath_cache = await loop.run_in_executor(None, load_ath_from_bq)
        elif FIRESTORE_AVAILABLE and _db:
            _ath_cache = await loop.run_in_executor(None, load_ath_cache, _db)

    # Intraday mode: use BQ history + latest candle (fast). Full mode: standard fetch.
    if body.mode == "intraday":
        all_data = await loop.run_in_executor(None, fetch_latest_candles)
        signals = []
        for symbol, df in all_data.items():
            if symbol == "SET":
                continue
            sig = scan_stock(symbol, df, ath_override=_ath_cache.get(symbol))
            if sig:
                signals.append(sig)
        signals.sort(key=lambda s: s.strength_score, reverse=True)
    else:
        signals, all_data = await loop.run_in_executor(
            None, functools.partial(run_full_scan, ath_cache=_ath_cache)
        )

    breadth = compute_market_breadth(signals, index_df=all_data.get("SET"))
    sector_trends = compute_sector_trends(signals)

    # Always fetch full index history for MACD/RSI on all scan types
    index_dfs = await loop.run_in_executor(None, fetch_indexes_with_history)
    indexes = _analyze_index_dfs(index_dfs)
    del index_dfs

    _last_signals = signals
    _last_scan_time = datetime.now(BANGKOK_TZ)
    _last_breadth = breadth
    _last_breadth_card = build_market_breadth_card(breadth, sector_trends)
    _last_sector_trends = sector_trends
    _last_indexes = indexes

    # Persist to Firestore (always); BigQuery only on full mode
    if FIRESTORE_AVAILABLE and _db:
        loop.run_in_executor(None, save_scan_state, _db, breadth, indexes, sector_trends, body.scan_type, body.mode)
        loop.run_in_executor(None, save_signals_to_firestore, signals, _db)
        # Log new Stage-2 breakouts for performance review
        for _sig in signals:
            if _sig.stage == 2 and _sig.pattern in ("breakout", "ath_breakout"):
                loop.run_in_executor(None, log_breakout, _db, _sig)
    if BQ_AVAILABLE and body.mode == "full":
        loop.run_in_executor(None, append_new_candles_to_bq, all_data)
    del all_data  # release 900+ DataFrames; BQ executor holds its own ref

    if not body.broadcast:
        return {"scanned": len(signals), "mode": body.mode, "breadth": breadth.__dict__}

    # Choose what to broadcast based on scan_type
    if body.scan_type == "breadth":
        _broadcast_breadth(breadth)

    elif body.scan_type == "breakout":
        breakouts = filter_signals(signals, pattern="breakout") + filter_signals(signals, pattern="ath_breakout")
        if breakouts:
            carousel = build_compact_stock_carousel(breakouts[:10], "🚀 Breakout Stocks")
            broadcast_flex("Breakout Stocks Update", carousel)
        else:
            broadcast_text(f"🔍 สแกน Breakout เสร็จแล้ว ({len(signals)} หุ้น) — ไม่พบสัญญาณ Breakout วันนี้")

    elif body.scan_type == "vcp":
        vcps = filter_signals(signals, pattern="vcp") + filter_signals(signals, pattern="vcp_low_cheat")
        if vcps:
            carousel = build_compact_stock_carousel(vcps[:10], "🔍 VCP Setups")
            broadcast_flex("VCP Pattern Update", carousel)
        else:
            broadcast_text(f"🔍 สแกน VCP เสร็จแล้ว ({len(signals)} หุ้น) — ไม่พบ VCP Setup วันนี้")

    else:  # "full" — post-close full report
        _broadcast_full_report(breadth, signals)

    return {"scanned": len(signals), "mode": body.mode, "broadcast": body.scan_type}


@app.post("/sync_ath")
async def sync_ath_endpoint(
    x_scan_secret: Optional[str] = Header(default=None),
    chunk: int = 0,
    chunk_size: int = 20,
):
    """One-time ATH sync endpoint. Call with ?chunk=0&chunk_size=20, increment chunk until next_chunk=null."""
    settings = get_settings()
    if not secrets.compare_digest(x_scan_secret or "", settings.scan_secret):
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid scan secret")
    if not FIRESTORE_AVAILABLE or not _db:
        raise HTTPException(status_code=503, detail="Firestore not available")

    global _ath_cache
    symbols = get_stock_list()
    synced = sync_ath_to_firestore(_db, symbols, chunk=chunk, chunk_size=chunk_size)
    _ath_cache.update(synced)
    total_chunks = (len(symbols) + chunk_size - 1) // chunk_size
    return {
        "synced": len(synced),
        "chunk": chunk,
        "chunk_size": chunk_size,
        "total_chunks": total_chunks,
        "next_chunk": chunk + 1 if chunk + 1 < total_chunks else None,
    }


@app.get("/admin/check")
async def admin_check(x_scan_secret: Optional[str] = Header(default=None)):
    """Admin data completeness + anomaly report. Protected by X-Scan-Secret header."""
    settings = get_settings()
    if not secrets.compare_digest(x_scan_secret or "", settings.scan_secret):
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid scan secret")

    signals = _last_signals
    breadth = _last_breadth

    # ── Scan summary ──
    stage_counts = {1: 0, 2: 0, 3: 0, 4: 0}
    pattern_counts: dict[str, int] = {}
    for sig in signals:
        stage_counts[sig.stage] = stage_counts.get(sig.stage, 0) + 1
        pattern_counts[sig.pattern] = pattern_counts.get(sig.pattern, 0) + 1

    scan_summary = {
        "scanned_at": breadth.scanned_at if breadth else None,
        "total": len(signals),
        "stage_counts": stage_counts,
        "pattern_counts": pattern_counts,
    }

    # ── Data completeness from ATH cache ──
    ath_count = len(_ath_cache)
    stocks_with_ath = list(_ath_cache.keys())
    all_symbols = get_stock_list()
    missing_ath = [s for s in all_symbols if s not in _ath_cache]

    data_completeness = {
        "ath_cache_entries": ath_count,
        "total_symbols": len(all_symbols),
        "stocks_with_full_history": ath_count,
        "stocks_missing_ath": len(missing_ath),
        "missing_ath_symbols": missing_ath[:50],
    }

    # ── Pattern verification (top 20 stage2 stocks) ──
    stage2_signals = [s for s in signals if s.stage == 2][:20]
    pattern_verification = [
        {
            "symbol": s.symbol,
            "stage": s.stage,
            "pattern": s.pattern,
            "close": s.close,
            "change_pct": round(s.change_pct, 2),
            "volume_ratio": round(s.volume_ratio, 2),
            "strength_score": int(s.strength_score),
            "above_sma200": s.close > s.sma200 if s.sma200 else None,
            "within_25pct_ath": ((s.close / _ath_cache[s.symbol]) >= 0.75) if s.symbol in _ath_cache else None,
        }
        for s in stage2_signals
    ]

    # ── Anomaly detection ──
    anomalies = []
    for s in signals:
        issues = []
        if s.stage == 2 and s.pattern == "going_down":
            issues.append("stage2 but pattern=going_down")
        if s.stage == 4 and s.pattern in ("breakout", "ath_breakout", "vcp"):
            issues.append(f"stage4 but pattern={s.pattern}")
        if s.volume_ratio < 0:
            issues.append(f"negative volume_ratio={s.volume_ratio:.2f}")
        if s.strength_score > 100 or s.strength_score < 0:
            issues.append(f"out-of-range score={s.strength_score:.1f}")
        if issues:
            anomalies.append({"symbol": s.symbol, "stage": s.stage, "pattern": s.pattern, "issues": issues})

    # ── Firestore stats ──
    firestore_stats: dict = {}
    if FIRESTORE_AVAILABLE and _db:
        try:
            users_count = len(list(_db.collection("users").stream()))
            breadth_snaps = len(list(_db.collection("market_breadth").limit(1000).stream()))
            ath_cache_count = len(list(_db.collection("ath_cache").stream()))
            firestore_stats = {
                "ath_cache_count": ath_cache_count,
                "users_count": users_count,
                "breadth_snapshots": breadth_snaps,
            }
        except Exception as exc:
            firestore_stats = {"error": str(exc)}

    return {
        "scan_summary": scan_summary,
        "data_completeness": data_completeness,
        "pattern_verification": pattern_verification,
        "anomalies": anomalies,
        "firestore_stats": firestore_stats,
        "stage_criteria": {
            "stage2": "close>MA150>MA200, MA200 rising, price>=52w_low*1.25, price>=52w_high*0.75"
        },
    }


def _broadcast_breadth(breadth: MarketBreadth) -> None:
    card = _last_breadth_card or build_market_breadth_card(breadth)
    broadcast_flex("ภาพรวมตลาด SET", card)


def _broadcast_full_report(breadth: MarketBreadth, signals: list[StockSignal]) -> None:
    # 1. Market breadth bubble (tappable — all stage/sector links inside)
    _broadcast_breadth(breadth)

    # 2. Breakout + ATH breakout
    breakouts = sorted(
        [s for s in signals if s.stage == 2 and s.pattern in ("breakout", "ath_breakout")],
        key=lambda s: s.strength_score, reverse=True,
    )[:20]
    if breakouts:
        bubble = build_ranked_stock_list_bubble(breakouts, "🚀 Breaking Out")
        broadcast_flex("Breaking Out", bubble)

    # 3. Trend-change / fallen stocks (Stage 3-4 with negative day)
    fallen = sorted(
        [s for s in signals if s.stage in (3, 4) and s.change_pct < -1.5],
        key=lambda s: s.change_pct,
    )[:20]
    if fallen:
        bubble = build_ranked_stock_list_bubble(fallen, "⚠️ Trend Change Alert")
        broadcast_flex("Trend Change Alert", bubble)

    # 4. Per-user watchlist push (multicast — each user gets their own watchlist snapshot)
    if FIRESTORE_AVAILABLE and _db:
        loop = asyncio.get_event_loop()
        loop.run_in_executor(None, _push_watchlist_updates_sync, signals)


def _push_watchlist_updates_sync(signals: list[StockSignal]) -> None:
    """Sync helper: multicast each user's watchlist snapshot. Runs in executor."""
    if not FIRESTORE_AVAILABLE or not _db:
        return
    sigs_map = {s.symbol: s for s in signals}
    try:
        users = list(_db.collection("users").stream())
    except Exception as exc:
        logger.error("_push_watchlist_updates_sync: fetch users failed: %s", exc)
        return
    for u in users:
        data = u.to_dict() or {}
        wl = data.get("watchlist", [])
        if not wl:
            continue
        wl_sigs = [sigs_map[sym] for sym in wl if sym in sigs_map]
        if not wl_sigs:
            continue
        try:
            card = build_watchlist_carousel(wl_sigs)
            multicast_flex([u.id], "📌 Watchlist Update", card)
        except Exception as exc:
            logger.error("watchlist push failed for %s: %s", u.id, exc)


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
        # Normalize aliases
        _aliases = {"explain volume": "explain volume_ratio", "explain vol": "explain volume_ratio"}
        lookup = _aliases.get(cmd, cmd)
        metric_name = lookup.replace("explain ", "")

        # Route stage queries to the comprehensive stage cycle card
        if metric_name in ("stage", "stage1", "stage2", "stage3", "stage4"):
            reply_flex(reply_token, "📊 Stage Analysis Guide", build_stage_cycle_card())
        # Route pattern queries to rich pattern cards
        elif metric_name in ("breakout", "ath_breakout", "vcp", "vcp_low_cheat", "consolidating", "going_down"):
            reply_flex(reply_token, f"📈 {metric_name.replace('_', ' ').title()}", build_pattern_detail_card(metric_name))
        else:
            explanation = _EXPLANATIONS.get(lookup)
            if explanation:
                reply_flex(reply_token, f"ℹ️ {metric_name}", build_explain_card(metric_name, explanation))
            else:
                reply_text(reply_token, f'ไม่พบคำอธิบายสำหรับ "{metric_name}"')

    # ── Market Breadth ──
    elif cmd in ("ตลาด", "market", "breadth"):
        if _last_breadth_card is None:
            reply_text(reply_token, "ยังไม่มีข้อมูล กรุณารอการสแกนตามกำหนด (10:15 / 12:15 / 15:15 / 16:45 น.)")
            return
        reply_flex(reply_token, "ภาพรวมตลาด SET", _last_breadth_card)

    # ── Key Indexes ──
    elif cmd in ("index", "indexes", "ดัชนี", "ดัชนีหุ้น"):
        if not _last_indexes:
            reply_text(reply_token, "ยังไม่มีข้อมูล กรุณารอการสแกนตามกำหนด (10:15 / 12:15 / 15:15 / 16:45 น.)")
            return
        carousel = build_index_carousel(_last_indexes)
        reply_flex(reply_token, "ดัชนีหุ้นไทย", carousel)

    # ── Sector Trends: overview or drill-down ──
    elif cmd.startswith("sector ") and len(cmd) > 7:
        sector_name = cmd[7:].upper().strip()
        sector_sigs = [s for s in _last_signals if SECTOR_MAP.get(s.symbol) == sector_name and s.stage in (1, 2)]
        sector_sigs.sort(key=lambda s: s.strength_score, reverse=True)
        if sector_sigs:
            _reply_stock_list(reply_token, sector_sigs, f"🏭 {sector_name} — Leaders")
        else:
            reply_text(reply_token, f"ไม่พบหุ้นในกลุ่ม {sector_name}\nกลุ่มที่มี: AGRO, CONSUMP, FINCIAL, INDUS, PROPCON, RESOURC, SERVICE, TECH")

    elif cmd in ("sector", "sectors", "เซกเตอร์", "กลุ่มหุ้น"):
        if not _last_sector_trends:
            reply_text(reply_token, "ยังไม่มีข้อมูล กรุณารอการสแกนตามกำหนด (10:15 / 12:15 / 15:15 / 16:45 น.)")
            return
        card = build_sector_overview_card(_last_sector_trends)
        reply_flex(reply_token, "แนวโน้มกลุ่มอุตสาหกรรม", card)

    # ── Guide ──
    elif cmd in ("guide", "คู่มือ", "explain all", "all explain"):
        global _guide_carousel_cache
        if _guide_carousel_cache is None:
            _guide_carousel_cache = build_guide_carousel()
        reply_flex(reply_token, "คู่มือ Signalix", _guide_carousel_cache)

    # ── Stage picker ──
    elif cmd in ("stage", "stages", "สเตจ"):
        reply_flex(reply_token, "เลือก Stage", build_stage_picker_card(_last_breadth))

    # ── Subscribe stub ──
    elif cmd in ("subscribe", "สมัคร", "membership"):
        reply_text(reply_token, "🔔 ระบบ Membership กำลังจะมา!\nตอนนี้คุณรับแจ้งเตือนอัตโนมัติทุกวันอยู่แล้วครับ")

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
        for sym in uncached:
            # Try Firestore signals collection before live yfinance fetch
            if FIRESTORE_AVAILABLE and _db:
                sig = load_signal_from_firestore(_db, sym)
                if sig:
                    wl_signals.append(sig)
                    continue
            df = fetch_ohlcv(sym)
            if df is not None:
                sig = scan_stock(sym, df, ath_override=_ath_cache.get(sym))
                del df
                if sig:
                    wl_signals.append(sig)
        if not wl_signals:
            reply_text(reply_token, "ไม่มีข้อมูลหุ้นใน Watchlist ขณะนี้")
            return
        card = build_watchlist_carousel(wl_signals)
        reply_flex(reply_token, f"📌 Watchlist ({len(wl_signals)} หุ้น)", card)

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

    # ── Detail: deep insight with fundamentals ──
    elif cmd.startswith("detail "):
        raw = text[7:].strip()
        symbol = resolve_symbol(raw)
        if not symbol:
            reply_text(reply_token, f'ไม่พบหุ้น "{raw.upper()}"')
            return
        _reply_detailed_stock(reply_token, symbol)

    # ── Help ──
    elif cmd in ("help", "ช่วย", "คำสั่ง", "?"):
        global _help_card_cache
        if _help_card_cache is None:
            _help_card_cache = build_help_card()
        reply_flex(reply_token, "คำสั่ง Signalix", _help_card_cache)

    # ── Performance Review ──
    elif cmd in ("review", "performance", "ผลงาน"):
        if not FIRESTORE_AVAILABLE or not _db:
            reply_text(reply_token, "ไม่สามารถโหลดข้อมูลได้ขณะนี้")
            return
        rows = await loop.run_in_executor(None, load_breakout_review, _db, _last_signals)
        card = build_performance_review_card(rows)
        reply_flex(reply_token, "📊 Breakout Performance", card)

    # ── User Score ──
    elif cmd in ("score", "คะแนน", "myscore"):
        if not user_id or not FIRESTORE_AVAILABLE or not _db:
            reply_text(reply_token, "ไม่สามารถโหลดข้อมูลได้ขณะนี้")
            return
        user_data = await loop.run_in_executor(
            None, lambda: (_db.collection("users").document(user_id).get().to_dict() or {})
        )
        card = build_score_card(user_data)
        reply_flex(reply_token, "⭐ Captain's Score", card)

    # ── Single stock lookup ──
    else:
        symbol = resolve_symbol(text)
        if symbol:
            _reply_single_stock(reply_token, symbol, user_id)
        else:
            reply_text(
                reply_token,
                f'ไม่พบหุ้น "{text.upper()}"\n\nเช็คชื่อ ticker ที่ถูกต้อง เช่น:\n• SCC (ไม่ใช่ SCG)\n• ADVANC (ไม่ใช่ AIS)\n\nพิมพ์ help เพื่อดูคำสั่งทั้งหมดครับ',
            )


def _get_signals_for(pattern: Optional[str] = None, stage: Optional[int] = None) -> list[StockSignal]:
    return filter_signals(_last_signals, pattern=pattern, stage=stage)


def _reply_stock_list(reply_token: str, signals: list[StockSignal], title: str) -> None:
    if not signals:
        if not _last_signals:
            reply_text(reply_token, "ยังไม่มีข้อมูล กรุณารอการสแกนตามกำหนด (10:15 / 12:15 / 15:15 / 16:45 น.)")
        else:
            reply_text(reply_token, f"ไม่มีหุ้นใน {title} ขณะนี้")
        return
    bubble = build_ranked_stock_list_bubble(signals, title)
    reply_flex(reply_token, title, bubble)


def _cache_is_fresh() -> bool:
    """Return True if _last_signals was updated within CACHE_TTL_MINUTES."""
    if _last_scan_time is None:
        return False
    age = (datetime.now(BANGKOK_TZ) - _last_scan_time).total_seconds() / 60
    return age < _CACHE_TTL_MINUTES


def _reply_single_stock(reply_token: str, symbol: str, user_id: str = "") -> None:
    # Cache-first: serve from last scan if cache is fresh (< 15 min old)
    cached = next((s for s in _last_signals if s.symbol == symbol), None)
    signal_to_show = None
    if cached and _cache_is_fresh():
        reply_flex(reply_token, f"วิเคราะห์ {symbol}", build_single_stock_card(cached))
        signal_to_show = cached
    else:
        # Cache stale or miss: try Settrade API only (no yfinance for on-demand)
        df = fetch_ohlcv_settrade(symbol)
        if df is None:
            if cached:
                reply_flex(reply_token, f"วิเคราะห์ {symbol} (แคช)", build_single_stock_card(cached))
                signal_to_show = cached
            elif FIRESTORE_AVAILABLE and _db:
                try:
                    doc = _db.collection("signals").document(symbol).get()
                    if doc.exists:
                        fs_signal = StockSignal(**doc.to_dict())
                        reply_flex(reply_token, f"วิเคราะห์ {symbol} (แคช)", build_single_stock_card(fs_signal))
                        signal_to_show = fs_signal
                except Exception:
                    pass
            if signal_to_show is None:
                reply_text(reply_token, f"ไม่พบข้อมูล {symbol} ขณะนี้\nลองพิมพ์ชื่อใหม่หลังจาก scan ถัดไปครับ")
                return
        else:
            live_signal = scan_stock(symbol, df, ath_override=_ath_cache.get(symbol))
            if live_signal is None:
                reply_text(reply_token, f"ข้อมูลไม่เพียงพอสำหรับ {symbol}")
                return
            reply_flex(reply_token, f"วิเคราะห์ {symbol}", build_single_stock_card(live_signal))
            signal_to_show = live_signal

    # Gamification: update user score based on what they viewed
    if user_id and FIRESTORE_AVAILABLE and _db and signal_to_show:
        loop = asyncio.get_event_loop()
        s = signal_to_show
        if s.stage == 2 and s.pattern in ("breakout", "ath_breakout", "vcp"):
            loop.run_in_executor(None, update_user_score, _db, user_id, 1, "viewed_s2_breakout", symbol)
        elif s.stage == 4:
            user_data = _db.collection("users").document(user_id).get().to_dict() or {}
            week_views = user_data.get("stage4_views_this_week", 0)
            if week_views >= 2:
                loop.run_in_executor(None, update_user_score, _db, user_id, -1, "repeated_stage4", symbol)
            loop.run_in_executor(None, increment_stage4_views, _db, user_id)


def _reply_detailed_stock(reply_token: str, symbol: str) -> None:
    """Serve deep insight card (technical + fundamentals) for a single stock."""
    cached = next((s for s in _last_signals if s.symbol == symbol), None)
    if cached:
        signal = cached
    else:
        df = fetch_ohlcv_settrade(symbol) or fetch_ohlcv(symbol)
        if df is None:
            reply_text(reply_token, f"ไม่พบข้อมูล {symbol}")
            return
        signal = scan_stock(symbol, df, ath_override=_ath_cache.get(symbol))
        if signal is None:
            reply_text(reply_token, f"ข้อมูลไม่เพียงพอสำหรับ {symbol}")
            return
    fund = get_fundamentals(symbol, _db if FIRESTORE_AVAILABLE else None)
    reply_flex(reply_token, f"📌 {symbol} Detail", build_watchlist_stock_card(signal, fund))


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
    if len(wl) >= 10:
        return False, "Watchlist เต็มแล้ว (สูงสุด 10 หุ้น)"
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
