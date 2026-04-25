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
import time
from datetime import datetime
from typing import Optional

import pandas as pd
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
    SECTOR_MAP, SUBSECTOR_TO_SECTOR, SECTOR_INDEX_SYMBOLS,
    append_new_candles_to_bq, BQ_AVAILABLE,
    fetch_global_asset, fetch_global_snapshot, fetch_indexes_with_history, fetch_latest_candles, fetch_sector_index_prices,
    is_global_code,
    fetch_sector_map_from_yfinance, get_fundamentals,
    get_sector, get_stock_list, init_bq, load_ath_cache, load_ath_from_bq,
    increment_stage4_views, load_breakout_review,
    load_latest_signals_from_bq,
    load_scan_state, load_sector_map_from_firestore, load_signal_from_firestore,
    load_signals_from_firestore,
    log_breakout, resolve_symbol,
    save_scan_state, save_sector_map_to_firestore, save_signals_to_bq,
    save_signals_to_firestore,
    sync_ath_to_firestore, tradingview_url, update_user_score,
)
from notifier import (
    broadcast_flex,
    broadcast_text,
    build_compact_stock_carousel,
    build_explain_card,
    build_guide_carousel,
    build_global_single_card,
    build_global_snapshot_card,
    build_index_breadth_card,
    build_index_carousel,
    build_market_breadth_card,
    build_pattern_detail_card,
    build_pivot_explainer_card,
    build_pattern_overview_card,
    build_ranked_stock_list_bubble,
    build_simple_tappable_list,
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
_last_indexes: dict[str, dict] = {}          # SET/SET50/MAI/sSET/SETESG index analysis
_last_sector_indexes: dict[str, dict] = {}   # AGRO/CONSUMP/… index prices
_last_sector_trends: list[SectorSummary] = []
_ath_cache: dict[str, float] = {}
_ath_cache_loaded_date: Optional[str] = None  # YYYY-MM-DD in BANGKOK_TZ; drives daily reload


def _load_ath_merged() -> dict[str, float]:
    """Merge BQ MAX(high) and Firestore ath_cache, taking the max per symbol.

    BQ's MAX(high) reflects only the rows BQ has indexed — which may be short
    of the true historical ATH if append_new_candles_to_bq didn't accumulate
    enough history yet. Firestore ath_cache was explicitly populated by
    sync_ath_to_firestore using yfinance max-period data (authoritative).
    ATH is monotonically non-decreasing, so max(BQ, Firestore) is always
    correct and defensive against either source being truncated.
    """
    merged: dict[str, float] = {}
    try:
        if FIRESTORE_AVAILABLE and _db:
            for sym, v in (load_ath_cache(_db) or {}).items():
                if v and v > 0:
                    merged[sym] = float(v)
    except Exception as exc:
        logger.warning("ATH load from Firestore failed: %s", exc)
    try:
        if BQ_AVAILABLE:
            for sym, v in (load_ath_from_bq() or {}).items():
                if v and v > 0:
                    merged[sym] = max(merged.get(sym, 0.0), float(v))
    except Exception as exc:
        logger.warning("ATH load from BQ failed: %s", exc)
    return merged

# Static card caches (built once, never change between scans)
_guide_carousel_cache: Optional[dict] = None

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
    "explain breakout_attempt": (
        "Breakout Attempt ⚡\n\n"
        "High ของวันใดวันหนึ่งใน 3 วันล่าสุด ทะลุ pivot 52 สัปดาห์\n"
        "พร้อม volume ≥ 1.4x — แต่ close ยังไม่ confirm เหนือ pivot\n"
        "(ราคายังอยู่ภายใน 3% จาก attempt high)\n\n"
        "⚡ จับสัญญาณ breakout ระหว่างวันที่ยังไม่ปิดยืนยัน\n"
        "เป็น signal อ่อนกว่า Breakout จริง — มอนิเตอร์รอ confirm"
    ),
    "explain attempt": (
        "Breakout Attempt ⚡\n\n"
        "High ของวันใดวันหนึ่งใน 3 วันล่าสุด ทะลุ pivot 52 สัปดาห์\n"
        "พร้อม volume ≥ 1.4x — แต่ close ยังไม่ confirm เหนือ pivot\n"
        "(ราคายังอยู่ภายใน 3% จาก attempt high)\n\n"
        "⚡ จับสัญญาณ breakout ระหว่างวันที่ยังไม่ปิดยืนยัน\n"
        "เป็น signal อ่อนกว่า Breakout จริง — มอนิเตอร์รอ confirm"
    ),
    "explain weakening": (
        "Stage 2 ⚠ Weakening\n\n"
        "หุ้นยังผ่าน Minervini Stage 2 ทุกข้อ (MA150/200 alignment)\n"
        "แต่ราคาวันนี้หลุดต่ำกว่า SMA50\n\n"
        "⚠️ โมเมนตัมระยะสั้นเริ่มอ่อน — เป็นสัญญาณเตือนก่อน Stage 3\n"
        "ใช้สำหรับเตรียม trim position หรือ trailing stop"
    ),
    "explain stage_weakening": (
        "Stage 2 ⚠ Weakening\n\n"
        "หุ้นยังผ่าน Minervini Stage 2 ทุกข้อ (MA150/200 alignment)\n"
        "แต่ราคาวันนี้หลุดต่ำกว่า SMA50\n\n"
        "⚠️ โมเมนตัมระยะสั้นเริ่มอ่อน — เป็นสัญญาณเตือนก่อน Stage 3\n"
        "ใช้สำหรับเตรียม trim position หรือ trailing stop"
    ),
    "explain global": (
        "Global Assets 🌏\n\n"
        "ติดตามตลาดโลก ETFs หุ้นสหรัฐฯ และ crypto\n"
        "ในที่เดียวพร้อม SET\n\n"
        "📊 พิมพ์ 'global' — ดู snapshot 25 ตัวเรียงตาม % เปลี่ยน\n"
        "⚡ พิมพ์ ticker ตรงๆ (BTC, SPX, NVDA…) — เปิด detail card\n"
        "📌 'add BTC' — เพิ่มเข้า watchlist เดียวกับหุ้น SET\n\n"
        "Indexes: SPX/NDX/DJI/KOSPI/NI225/HSI/SSE\n"
        "ETFs: SMH/SPY/QQQ/ARKK/ARKW/IWM\n"
        "US Stocks: NVDA/AAPL/GOOG/TSLA/MSFT/META/AMD/NFLX/GEV\n"
        "Crypto: BTC/ETH/SOL"
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
    """Initialise singletons + synchronously warm the in-memory cache before
    Cloud Run routes traffic to this instance.

    Previously `_warm_from_firestore` was scheduled via asyncio.create_task, so
    uvicorn accepted requests before warmup finished — the first user tap on a
    new revision hit an instance with empty _last_signals / _last_breadth and
    ate the lazy-reload cost. Blocking here delays startup by a few seconds
    (BQ/Firestore reads) but guarantees every request sees a fully warmed
    process. Combined with --min-instances=1 in cloudbuild.yaml, no user ever
    experiences a cold start.
    """
    loop = asyncio.get_running_loop()
    settings = get_settings()
    if settings.gcp_project_id:
        await loop.run_in_executor(None, init_bq, settings.gcp_project_id, settings.bq_dataset)
    init_notifier(settings.line_channel_access_token)
    await _warm_from_firestore()


async def _warm_from_firestore():
    """Load cached state on startup. BQ is tried first (durable), Firestore as fallback."""
    global _last_signals, _last_breadth, _last_breadth_card, _last_scan_time, _last_indexes, _last_sector_trends, _ath_cache, _ath_cache_loaded_date, _last_sector_indexes
    loop = asyncio.get_running_loop()
    try:
        # Load ATH from BOTH sources and take the max per symbol. BQ gives
        # MAX(high) over whatever rows BQ happens to have (may be truncated if
        # append_new_candles_to_bq hasn't accumulated full history); Firestore
        # ath_cache was populated by sync_ath_to_firestore using yfinance
        # max-period history (authoritative). ATH is monotonically non-decreasing,
        # so max() is always correct.
        _ath_cache = await loop.run_in_executor(None, _load_ath_merged)
        _ath_cache_loaded_date = datetime.now(BANGKOK_TZ).strftime("%Y-%m-%d")
        logger.info("ATH cache loaded (merged BQ+Firestore): %d entries", len(_ath_cache))

        # ── Sector map: load from Firestore, wire into data module ───────────────
        if FIRESTORE_AVAILABLE and _db:
            from data import _dynamic_sector_map as _dsm
            loaded_map = await loop.run_in_executor(None, load_sector_map_from_firestore, _db)
            if loaded_map:
                _dsm.update(loaded_map)
                logger.info("Sector map loaded: %d symbols with subsector data", len(loaded_map))

        # ── Signals: Firestore first (full StockSignal dataclass via __dict__),
        # BQ fallback only when Firestore unavailable. BQ scan_results table
        # has a fixed schema that lags new dataclass fields by one
        # ALTER TABLE migration; Firestore stays in sync automatically.
        if FIRESTORE_AVAILABLE and _db:
            fs_signals = await loop.run_in_executor(None, load_signals_from_firestore, _db)
            if fs_signals:
                _last_signals = fs_signals
                logger.info("Signals loaded from Firestore: %d stocks", len(fs_signals))

        if not _last_signals and BQ_AVAILABLE:
            bq_signals = await loop.run_in_executor(None, load_latest_signals_from_bq)
            if bq_signals:
                _last_signals = bq_signals
                logger.info("Signals loaded from BQ (Firestore fallback): %d stocks", len(bq_signals))

        # ── Scan state (breadth/indexes/sectors): always from Firestore ──────────
        if FIRESTORE_AVAILABLE and _db:
            state = await loop.run_in_executor(None, load_scan_state, _db)
            if state:
                _last_breadth = state["breadth"]
                _last_indexes = state["indexes"]
                _last_sector_trends = state["sector_trends"]
                _last_sector_indexes = state.get("sector_indexes") or {}
                try:
                    _last_breadth_card = build_market_breadth_card(_last_breadth, _last_sector_trends, _last_indexes)
                except Exception as exc:
                    logger.error("build_market_breadth_card failed in warmup: %s", exc)
                try:
                    _last_scan_time = datetime.fromisoformat(state["scanned_at"]).replace(tzinfo=BANGKOK_TZ)
                except Exception:
                    pass

        # ── Derive breadth/sectors from signals if scan_state is missing ─────────
        if _last_signals and not _last_breadth:
            try:
                _last_breadth = compute_market_breadth(_last_signals)
                if not _last_sector_trends:
                    _last_sector_trends = compute_sector_trends(_last_signals)
                _last_breadth_card = build_market_breadth_card(_last_breadth, _last_sector_trends, _last_indexes)
                logger.info("Breadth computed from signals (scan_state missing)")
            except Exception as exc:
                logger.error("compute breadth fallback failed: %s", exc)
        elif _last_signals and not _last_sector_trends:
            try:
                _last_sector_trends = compute_sector_trends(_last_signals)
            except Exception as exc:
                logger.error("compute sector_trends fallback failed: %s", exc)

        # Load index member overrides from Firestore (if previously written
        # by /admin/refresh_index_members). Falls back to data.py hardcoded
        # constants when missing.
        if FIRESTORE_AVAILABLE and _db:
            try:
                from data import set_index_members
                for index_name in ("SET50", "SET100", "MAI"):
                    doc = _db.collection("index_members").document(index_name).get()
                    if doc.exists:
                        members_list = (doc.to_dict() or {}).get("members") or []
                        if members_list:
                            set_index_members(index_name, set(members_list))
                            logger.info("Loaded %s members from Firestore: %d tickers",
                                        index_name, len(members_list))
            except Exception as exc:
                logger.warning("Failed to load index_members from Firestore: %s", exc)

        logger.info("Cache warm complete: %d signals, breadth=%s, indexes=%d, sectors=%d",
                    len(_last_signals), "ok" if _last_breadth else "missing",
                    len(_last_indexes), len(_last_sector_trends))
    except Exception as exc:
        logger.error("_warm_from_firestore failed: %s", exc)


# ─── Health ────────────────────────────────────────────────────────────────────

@app.get("/health")
async def health():
    """Health probe. Reports in-memory cache state; lazy-reloads _last_signals
    from BQ/Firestore when this Cloud Run instance hasn't warmed yet so the
    reported count doesn't spuriously show 0 just because a fresh instance
    happens to serve the probe."""
    from data import BQ_AVAILABLE as _bq_avail
    global _last_signals
    if not _last_signals:
        loop = asyncio.get_running_loop()
        # Firestore first — has full StockSignal dataclass; BQ fallback.
        if FIRESTORE_AVAILABLE and _db:
            fs = await loop.run_in_executor(None, load_signals_from_firestore, _db)
            if fs:
                _last_signals = fs
        if not _last_signals and BQ_AVAILABLE:
            bq = await loop.run_in_executor(None, load_latest_signals_from_bq)
            if bq:
                _last_signals = bq
    return {
        "status": "ok",
        "time": datetime.now(BANGKOK_TZ).isoformat(),
        "firestore": FIRESTORE_AVAILABLE,
        "bigquery": _bq_avail,
        "cached_stocks": len(_last_signals),
        "last_scan_time": _last_scan_time.isoformat() if _last_scan_time else None,
    }


@app.get("/test/coverage")
async def test_coverage(x_scan_secret: Optional[str] = Header(default=None)):
    """List SET_STOCKS that the most recent scan didn't classify.

    Used after the BQ-merge removal to identify which 31 stocks fell out
    of the scan. Causes are usually: insufficient bars (<60 → scan_stock
    returns None), data freshness gate (last bar > 10 days old), or
    Settrade + yfinance both returning empty for that ticker.

    For each missing symbol, also probes both data sources to attribute
    the cause so we can decide whether to widen the universe filter or
    just accept the drop as genuine data scarcity.
    """
    settings = get_settings()
    if not secrets.compare_digest(x_scan_secret or "", settings.scan_secret):
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid scan secret")

    from data import SET_STOCKS
    scanned = {s.symbol for s in _last_signals}
    missing = sorted(set(SET_STOCKS) - scanned)

    # For each missing symbol, classify why it's missing.
    loop = asyncio.get_running_loop()
    detail: list[dict] = []

    def _probe(sym: str) -> dict:
        info = {"symbol": sym}
        # Settrade
        try:
            from settrade_client import get_ohlcv as st_get_ohlcv, is_api_available
            if is_api_available():
                df = st_get_ohlcv(sym, period="1Y")
                info["settrade_bars"] = len(df) if df is not None else 0
            else:
                info["settrade_bars"] = "api_unavailable"
        except Exception as exc:
            info["settrade_bars"] = f"err: {exc}"
        # yfinance
        try:
            import yfinance as yf
            df = yf.Ticker(f"{sym}.BK").history(period="1y", auto_adjust=False).dropna(subset=["Close"])
            info["yfinance_bars"] = len(df)
            if len(df) > 0:
                last = df.index[-1]
                from datetime import datetime
                import pytz
                today = datetime.now(pytz.timezone("Asia/Bangkok")).date()
                last_date = last.date() if hasattr(last, "date") else last
                info["yfinance_last_date"] = str(last_date)
                info["yfinance_age_days"] = (today - last_date).days
        except Exception as exc:
            info["yfinance_bars"] = f"err: {exc}"
        # Categorise
        st_b = info.get("settrade_bars") if isinstance(info.get("settrade_bars"), int) else 0
        yf_b = info.get("yfinance_bars") if isinstance(info.get("yfinance_bars"), int) else 0
        if st_b == 0 and yf_b == 0:
            info["cause"] = "no_data_anywhere"
        elif max(st_b, yf_b) < 60:
            info["cause"] = "insufficient_bars"
        elif info.get("yfinance_age_days", 0) > 10:
            info["cause"] = "stale_data"
        elif max(st_b, yf_b) < 200:
            info["cause"] = "lt200_bars_no_stage_classification"
        else:
            info["cause"] = "unknown_check_logs"
        return info

    if missing:
        from concurrent.futures import ThreadPoolExecutor
        def _probe_all():
            with ThreadPoolExecutor(max_workers=8) as ex:
                return list(ex.map(_probe, missing))
        detail = await loop.run_in_executor(None, _probe_all)

    # Aggregate
    from collections import Counter
    cause_breakdown = dict(Counter(d.get("cause", "?") for d in detail))

    return {
        "universe_total": len(SET_STOCKS),
        "scanned_count": len(scanned),
        "missing_count": len(missing),
        "missing_symbols": missing,
        "cause_breakdown": cause_breakdown,
        "detail": detail,
    }


@app.get("/test/compare/{symbol}")
async def test_compare(symbol: str, x_scan_secret: Optional[str] = Header(default=None)):
    """Compare Settrade vs yfinance OHLCV side-by-side for one SET ticker.

    Diagnostic for the SYMC/JMT stage-2 false-positive: SET dividend-paying
    stocks show very different historical 52W highs between Settrade
    (`normalized=False`) and yfinance (`auto_adjust=False`). Hypothesis:
    Settrade returns broker-display prices (retroactively dividend-adjusted)
    despite the flag, while yfinance returns raw traded prices. This
    endpoint dumps both so we can confirm on a third stock (e.g. KBANK
    with known large dividends) before changing the primary data source.

    Returns: dict with settrade and yfinance subsections, each showing
    latest 3 bars + computed SMA50/150/200 + 52W high + bar count, plus
    a ratio = settrade.high_52w / yfinance.high_52w. Ratio < 1.0 means
    Settrade's history is dividend-adjusted relative to yfinance.
    """
    settings = get_settings()
    if not secrets.compare_digest(x_scan_secret or "", settings.scan_secret):
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid scan secret")

    symbol = symbol.upper().strip()
    out: dict = {"symbol": symbol}
    loop = asyncio.get_running_loop()

    def _summarise(df, label):
        if df is None or len(df) == 0:
            return {"source": label, "bars": 0, "error": "empty"}
        try:
            close = df["Close"]
            sma50 = float(close.rolling(50).mean().iloc[-1]) if len(df) >= 50 else None
            sma150 = float(close.rolling(150).mean().iloc[-1]) if len(df) >= 150 else None
            sma200 = float(close.rolling(200).mean().iloc[-1]) if len(df) >= 200 else None
            high_52w = float(df["High"].iloc[-min(252, len(df)):].max())
            last3 = df.tail(3)
            tail = [{
                "date": str(idx.date()) if hasattr(idx, "date") else str(idx),
                "open": round(float(row["Open"]), 4),
                "high": round(float(row["High"]), 4),
                "low": round(float(row["Low"]), 4),
                "close": round(float(row["Close"]), 4),
            } for idx, row in last3.iterrows()]
            return {
                "source": label,
                "bars": len(df),
                "first_date": str(df.index[0].date()) if hasattr(df.index[0], "date") else str(df.index[0]),
                "last_date": str(df.index[-1].date()) if hasattr(df.index[-1], "date") else str(df.index[-1]),
                "last_close": round(float(close.iloc[-1]), 4),
                "high_52w": round(high_52w, 4),
                "sma50": round(sma50, 4) if sma50 is not None else None,
                "sma150": round(sma150, 4) if sma150 is not None else None,
                "sma200": round(sma200, 4) if sma200 is not None else None,
                "tail3": tail,
            }
        except Exception as exc:
            return {"source": label, "error": str(exc)}

    # Settrade
    try:
        from settrade_client import get_ohlcv as st_get_ohlcv, is_api_available
        if is_api_available():
            df_st = await loop.run_in_executor(None, st_get_ohlcv, symbol, "1Y")
            out["settrade"] = _summarise(df_st, "settrade(normalized=False)")
        else:
            out["settrade"] = {"source": "settrade", "error": "api_unavailable"}
    except Exception as exc:
        out["settrade"] = {"source": "settrade", "error": str(exc)}

    # yfinance unadjusted
    try:
        import yfinance as yf
        df_yf = await loop.run_in_executor(
            None, lambda: yf.Ticker(f"{symbol}.BK").history(period="1y", auto_adjust=False).dropna(subset=["Close"]),
        )
        out["yfinance"] = _summarise(df_yf, "yfinance(auto_adjust=False)")
    except Exception as exc:
        out["yfinance"] = {"source": "yfinance", "error": str(exc)}

    # BigQuery — what's stored in our own historical cache?
    try:
        from data import load_all_ohlcv_from_bq, BQ_AVAILABLE
        if BQ_AVAILABLE:
            bq_data = await loop.run_in_executor(
                None, lambda: load_all_ohlcv_from_bq(lookback_days=400),
            )
            df_bq = bq_data.get(symbol)
            out["bigquery"] = _summarise(df_bq, "bigquery(merged_400d)")
        else:
            out["bigquery"] = {"source": "bigquery", "error": "bq_unavailable"}
    except Exception as exc:
        out["bigquery"] = {"source": "bigquery", "error": str(exc)}

    # Merged dataframe — exactly what the scan pipeline produces. Replicates
    # the Settrade + BQ merge from fetch_all_stocks lines 1371-1373.
    try:
        st_df_for_merge = None
        bq_df_for_merge = None
        try:
            from settrade_client import get_ohlcv as st_get_ohlcv2
            st_df_for_merge = await loop.run_in_executor(None, st_get_ohlcv2, symbol, "1Y")
        except Exception:
            pass
        try:
            from data import load_all_ohlcv_from_bq, BQ_AVAILABLE as _bq_ok
            if _bq_ok:
                bq_data2 = await loop.run_in_executor(
                    None, lambda: load_all_ohlcv_from_bq(lookback_days=400),
                )
                bq_df_for_merge = bq_data2.get(symbol)
        except Exception:
            pass

        if st_df_for_merge is not None and bq_df_for_merge is not None:
            import pandas as pd
            combined = pd.concat([bq_df_for_merge, st_df_for_merge])
            combined = combined[~combined.index.duplicated(keep="last")]
            combined = combined.sort_index()
            out["merged"] = _summarise(combined, "merged(bq+settrade)")
        elif st_df_for_merge is not None:
            out["merged"] = _summarise(st_df_for_merge, "merged(settrade-only,no-bq)")
        elif bq_df_for_merge is not None:
            out["merged"] = _summarise(bq_df_for_merge, "merged(bq-only,no-settrade)")
        else:
            out["merged"] = {"source": "merged", "error": "neither_source_available"}
    except Exception as exc:
        out["merged"] = {"source": "merged", "error": str(exc)}

    # Ratios for at-a-glance comparison.
    st = out.get("settrade", {})
    yf_ = out.get("yfinance", {})
    bq = out.get("bigquery", {})
    mg = out.get("merged", {})
    diagnoses = []
    if st.get("high_52w") and yf_.get("high_52w"):
        out["ratio_settrade_over_yf"] = round(st["high_52w"] / yf_["high_52w"], 4)
    if mg.get("high_52w") and st.get("high_52w"):
        out["ratio_merged_over_settrade"] = round(mg["high_52w"] / st["high_52w"], 4)
        if mg["high_52w"] != st["high_52w"]:
            diagnoses.append(
                f"merged({mg['high_52w']}) ≠ settrade({st['high_52w']}) — "
                f"BQ contributing different historical highs"
            )
    if bq.get("high_52w") and st.get("high_52w"):
        out["ratio_bq_over_settrade"] = round(bq["high_52w"] / st["high_52w"], 4)
        if abs(bq["high_52w"] - st["high_52w"]) > 0.01 * st["high_52w"]:
            diagnoses.append(
                f"BQ({bq['high_52w']}) differs from Settrade({st['high_52w']}) by "
                f"{(bq['high_52w']/st['high_52w']-1)*100:+.1f}% — BQ history is "
                f"{'higher' if bq['high_52w'] > st['high_52w'] else 'lower'}"
            )
    out["diagnoses"] = diagnoses or ["all sources agree"]
    return out


@app.get("/test/signal/{symbol}")
async def test_signal(symbol: str):
    """Return the cached StockSignal for a symbol — both in-memory and Firestore.

    Useful to debug why a card shows stale data when Settrade says otherwise:
    tells you when scan_stock last wrote this symbol and what close it recorded.
    """
    symbol = symbol.upper().strip()
    result: dict = {"symbol": symbol}

    in_mem = next((s for s in _last_signals if s.symbol == symbol), None)
    result["in_memory"] = _signal_snapshot(in_mem) if in_mem else None

    if FIRESTORE_AVAILABLE and _db:
        loop = asyncio.get_running_loop()
        fs_sig = await loop.run_in_executor(None, load_signal_from_firestore, _db, symbol)
        result["firestore"] = _signal_snapshot(fs_sig) if fs_sig else None
    else:
        result["firestore"] = None

    # ATH sources — useful for diagnosing wrong ath_breakout classifications.
    ath_info: dict = {"in_memory_cache": _ath_cache.get(symbol)}
    if FIRESTORE_AVAILABLE and _db:
        try:
            doc = _db.collection("ath_cache").document(symbol).get()
            ath_info["firestore"] = doc.to_dict() if doc.exists else None
        except Exception as exc:
            ath_info["firestore_error"] = str(exc)
    result["ath"] = ath_info

    return result


def _signal_snapshot(signal) -> dict:
    """Minimal StockSignal view for diagnostic endpoints. Includes the
    sub_stage finite-state-machine label as the primary classification;
    legacy stage/pattern fields retained for backward compat."""
    return {
        "symbol": signal.symbol,
        "close": signal.close,
        "change_pct": signal.change_pct,
        "high_52w": signal.high_52w,
        "pct_from_52w_high": signal.pct_from_52w_high,
        "stage": signal.stage,
        "sub_stage": getattr(signal, "sub_stage", ""),
        "pattern": signal.pattern,
        "strength_score": signal.strength_score,
        "scanned_at": signal.scanned_at,
        "data_date": getattr(signal, "data_date", ""),
        "sma10": getattr(signal, "sma10", 0.0),
        "sma20": getattr(signal, "sma20", 0.0),
        "sma50": getattr(signal, "sma50", 0.0),
        "sma200_roc20": getattr(signal, "sma200_roc20", 0.0),
        "pivot_price": getattr(signal, "pivot_price", 0.0),
        "pivot_stop": getattr(signal, "pivot_stop", 0.0),
        "stage_weakening": getattr(signal, "stage_weakening", False),
    }


@app.get("/test/query")
async def test_query(cmd: str, x_scan_secret: Optional[str] = Header(default=None)):
    """E2E probe: runs the same filter logic as _handle_text_query for list-type
    commands and returns a JSON summary instead of pushing to LINE.

    Gated by x-scan-secret. Designed for scripts/e2e_check.py to exercise every
    user-facing card path after deploys without touching real LINE users.
    """
    settings = get_settings()
    if not secrets.compare_digest(x_scan_secret or "", settings.scan_secret):
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid scan secret")

    global _last_signals
    loop = asyncio.get_running_loop()
    if not _last_signals:
        # Prefer Firestore — it serializes the full StockSignal dataclass via
        # signal.__dict__, so new fields (sub_stage, pivot_price, pivot_stop,
        # sma10/20, sma200_roc20) are present on lazy-load. The BQ scan_results
        # table has a fixed schema that lags new fields by one ALTER TABLE
        # migration, so loading from BQ leaves new fields at default 0.0/"".
        # BQ stays as a fallback for the rare case Firestore is unavailable.
        if FIRESTORE_AVAILABLE and _db:
            fs_sigs = await loop.run_in_executor(None, load_signals_from_firestore, _db)
            if fs_sigs:
                _last_signals = fs_sigs
        if not _last_signals and BQ_AVAILABLE:
            bq_sigs = await loop.run_in_executor(None, load_latest_signals_from_bq)
            if bq_sigs:
                _last_signals = bq_sigs

    c = cmd.lower().strip()

    def summary(sigs, title):
        return {
            "kind": "list",
            "title": title,
            "count": len(sigs),
            "first_5": [
                {"symbol": s.symbol, "close": s.close, "change_pct": s.change_pct,
                 "stage": s.stage,
                 "sub_stage": getattr(s, "sub_stage", ""),
                 "pattern": s.pattern, "strength": s.strength_score,
                 "pivot_price": getattr(s, "pivot_price", 0.0),
                 "pivot_stop": getattr(s, "pivot_stop", 0.0)}
                for s in sigs[:5]
            ],
        }

    # Advance / decline / flat
    if c.startswith("advancing") or c.startswith("up ") or c == "up":
        sigs = sorted([s for s in _last_signals if (s.change_pct or 0) > 0],
                      key=lambda s: s.change_pct, reverse=True)
        return summary(sigs, "Advancing")
    if c.startswith("declining") or c.startswith("down ") or c == "down":
        sigs = sorted([s for s in _last_signals if (s.change_pct or 0) < 0],
                      key=lambda s: s.change_pct)
        return summary(sigs, "Declining")
    if c.startswith("flat"):
        sigs = sorted([s for s in _last_signals if (s.change_pct or 0) == 0],
                      key=lambda s: s.strength_score, reverse=True)
        return summary(sigs, "Flat")

    # Stage lists
    for n in (1, 2, 3, 4):
        if c.startswith(f"stage{n}") or c.startswith(f"stage {n}"):
            sigs = [s for s in _last_signals if s.stage == n]
            sigs.sort(key=lambda s: s.strength_score, reverse=True)
            return summary(sigs, f"Stage {n}")

    # Pattern lists
    if c in ("breakout", "break out"):
        sigs = [s for s in _last_signals if s.pattern in ("breakout", "ath_breakout")]
        sigs.sort(key=lambda s: s.strength_score, reverse=True)
        return summary(sigs, "Breakout")
    if c in ("attempt", "attempts", "breakout attempt", "breakout_attempt", "breakout attempts"):
        sigs = [s for s in _last_signals if s.pattern == "breakout_attempt"]
        sigs.sort(key=lambda s: s.strength_score, reverse=True)
        return summary(sigs, "Breakout Attempt")
    if c in ("ath", "all time high", "ath breakout"):
        sigs = [s for s in _last_signals if s.pattern == "ath_breakout"]
        sigs.sort(key=lambda s: s.strength_score, reverse=True)
        return summary(sigs, "ATH Breakout")
    if c == "vcp":
        sigs = [s for s in _last_signals if s.pattern in ("vcp", "vcp_low_cheat")]
        sigs.sort(key=lambda s: s.strength_score, reverse=True)
        return summary(sigs, "VCP")
    if c in ("consolidating", "consolidate", "coil"):
        sigs = [s for s in _last_signals if s.pattern == "consolidating"]
        sigs.sort(key=lambda s: s.strength_score, reverse=True)
        return summary(sigs, "Consolidating")
    if c in ("weakening", "weak", "stage2 weak", "stage2 weakening"):
        sigs = [s for s in _last_signals
                if s.stage == 2 and getattr(s, "stage_weakening", False)]
        sigs.sort(key=lambda s: s.strength_score, reverse=True)
        return summary(sigs, "Stage 2 Weakening")

    # ── Sub-stage filters (9-state finite state machine) ──────────────
    # Same vocabulary as classify_sub_stage(): one of 9 SUB_STAGE_*.
    # Aliases support both "early" and "stage2 early" forms; the long
    # form is what tappable rows from the breadth card emit.
    SUB_STAGE_FILTERS = {
        ("base", "stage1 base"):                 ("STAGE_1_BASE",      "Stage 1 · Base"),
        ("prep", "stage1 prep"):                 ("STAGE_1_PREP",      "Stage 1 · Prep"),
        ("early", "stage2 early"):               ("STAGE_2_EARLY",     "Stage 2 · Early"),
        ("running", "stage2 running"):           ("STAGE_2_RUNNING",   "Stage 2 · Running"),
        ("pullback", "stage2 pullback"):         ("STAGE_2_PULLBACK",  "Stage 2 · Pullback"),
        ("volatile", "stage3 volatile"):         ("STAGE_3_VOLATILE",  "Stage 3 · Volatile"),
        ("dist", "distribution", "stage3 dist",
         "stage3 distribution"):                 ("STAGE_3_DIST_DIST", "Stage 3 · Distribution"),
        ("breakdown", "stage4 breakdown"):       ("STAGE_4_BREAKDOWN", "Stage 4 · Breakdown"),
        ("downtrend", "stage4 downtrend"):       ("STAGE_4_DOWNTREND", "Stage 4 · Downtrend"),
    }
    for aliases, (sub_stage_const, label) in SUB_STAGE_FILTERS.items():
        if c in aliases:
            sigs = [s for s in _last_signals
                    if getattr(s, "sub_stage", "") == sub_stage_const]
            sigs.sort(key=lambda s: s.strength_score, reverse=True)
            return summary(sigs, label)

    # ── Pivot-point candidates ─────────────────────────────────────────
    # All stocks with a computed pivot (= one of the 4 actionable
    # sub-stages: PREP / EARLY / RUNNING / PULLBACK). Sorted by closeness
    # to pivot — stocks already AT or ABOVE pivot rank highest, then those
    # approaching it from below by % distance.
    if c in ("pivot", "pivots", "pivot point", "pivot points"):
        sigs = [s for s in _last_signals
                if getattr(s, "pivot_price", 0.0) > 0]

        def _pivot_distance(s):
            # Closer to pivot = smaller positive value; above pivot = negative.
            if not s.pivot_price:
                return 999.0
            return (s.pivot_price - s.close) / s.pivot_price

        sigs.sort(key=_pivot_distance)
        return summary(sigs, "Pivot Candidates")

    # Sector list (e.g. "sector FINCIAL")
    if c.startswith("sector ") and len(c) > 7:
        sector_name = c[7:].strip().upper()
        from data import get_sector, get_subsector
        sigs = [s for s in _last_signals
                if get_sector(s.symbol) == sector_name or get_subsector(s.symbol) == sector_name]
        sigs.sort(key=lambda s: s.strength_score, reverse=True)
        return summary(sigs, f"Sector {sector_name}")

    # Global watchlist (US + Asia indexes + ETFs + US stocks + crypto)
    if c in ("global", "world", "g", "ตลาดโลก", "กลอบอล"):
        from data import GLOBAL_SYMBOLS
        snap = await loop.run_in_executor(None, fetch_global_snapshot)
        ordered = sorted(snap.items(), key=lambda kv: -(kv[1].get("change_pct") or 0))
        return {
            "kind": "global",
            "count": len(snap),
            "configured": len(GLOBAL_SYMBOLS),
            "top_5_up": [
                {"code": code, "class": d["class"], "close": d["close"],
                 "change_pct": d["change_pct"]}
                for code, d in ordered[:5]
            ],
            "top_5_down": [
                {"code": code, "class": d["class"], "close": d["close"],
                 "change_pct": d["change_pct"]}
                for code, d in ordered[-5:][::-1]
            ],
        }

    # Per-sub-index drill-down: 'set50 members' / 'set50 stage2' / etc.
    # Returns kind=list (same shape as /test/query?cmd=stage2) so e2e can
    # use the existing list-summary helper.
    # Sub-stage tokens that the per-index drill-down also recognises.
    # Same vocab as the global SUB_STAGE_FILTERS above; allows e.g.
    # 'set50 pullback' to filter SET50 constituents to STAGE_2_PULLBACK.
    SUB_STAGE_TOKEN_MAP = {
        "base": "STAGE_1_BASE", "prep": "STAGE_1_PREP",
        "early": "STAGE_2_EARLY", "running": "STAGE_2_RUNNING",
        "pullback": "STAGE_2_PULLBACK",
        "volatile": "STAGE_3_VOLATILE",
        "dist": "STAGE_3_DIST_DIST", "distribution": "STAGE_3_DIST_DIST",
        "breakdown": "STAGE_4_BREAKDOWN", "downtrend": "STAGE_4_DOWNTREND",
    }
    for prefix in ("set50 ", "set100 ", "mai "):
        if c.startswith(prefix):
            from data import get_index_members
            index_name = "SET50" if prefix.startswith("set50") else (
                         "SET100" if prefix.startswith("set100") else "MAI")
            members = get_index_members(index_name)
            rest = c[len(prefix):].strip().replace(" ", "")
            constituents = [s for s in _last_signals if s.symbol in members]
            stage_filter = None
            for n in (1, 2, 3, 4):
                if rest in (f"stage{n}", f"s{n}"):
                    stage_filter = n
                    break
            sub_stage_const = SUB_STAGE_TOKEN_MAP.get(rest)
            if stage_filter is not None:
                sigs = [s for s in constituents if s.stage == stage_filter]
                label = f"{index_name} Stage {stage_filter}"
            elif sub_stage_const is not None:
                sigs = [s for s in constituents
                        if getattr(s, "sub_stage", "") == sub_stage_const]
                label = f"{index_name} {rest}"
            elif rest in ("members", "list", "all"):
                sigs = list(constituents)
                label = f"{index_name} members"
            else:
                sigs = []
                label = f"{index_name} {rest}"
            sigs.sort(key=lambda s: s.strength_score, reverse=True)
            return summary(sigs, label)

    # Per-sub-index breadth (SET50 / SET100 / MAI)
    if c in ("set50", "set 50", "set100", "set 100", "mai"):
        from data import get_index_members
        from analyzer import compute_index_breadth
        index_name = "SET50" if c.replace(" ", "") == "set50" else (
                     "SET100" if c.replace(" ", "") == "set100" else "MAI")
        members = get_index_members(index_name)
        idx_data = (_last_indexes or {}).get(index_name, {})
        idx_close = float(idx_data.get("close", 0) or 0)
        idx_chg = float(idx_data.get("change_pct", 0) or 0)
        breadth = compute_index_breadth(
            _last_signals, members, index_close=idx_close, index_change_pct=idx_chg,
        )
        constituents = [s for s in _last_signals if s.symbol in members]
        return {
            "kind": "index_breadth",
            "index": index_name,
            "members_configured": len(members),
            "members_scanned": breadth.total_stocks,
            "advancing": breadth.advancing,
            "declining": breadth.declining,
            "unchanged": breadth.unchanged,
            "stage_counts": {1: breadth.stage1_count, 2: breadth.stage2_count,
                             3: breadth.stage3_count, 4: breadth.stage4_count},
            "above_ma200_pct": getattr(breadth, "above_ma200_pct", 0.0),
            "index_close": idx_close,
            "index_change_pct": idx_chg,
            "first_5_constituents": [s.symbol for s in constituents[:5]],
        }

    # Market breadth summary
    if c in ("market", "breadth", "ตลาด"):
        if _last_breadth is None:
            return {"kind": "breadth", "breadth": None}
        b = _last_breadth
        return {
            "kind": "breadth",
            "breadth": {
                "scanned_at": getattr(b, "scanned_at", None),
                "total_stocks": getattr(b, "total_stocks", 0),
                "advancing": getattr(b, "advancing", 0),
                "declining": getattr(b, "declining", 0),
                "unchanged": getattr(b, "unchanged", 0),
                "new_highs_52w": getattr(b, "new_highs_52w", 0),
                "new_lows_52w": getattr(b, "new_lows_52w", 0),
                "breakout_count": getattr(b, "breakout_count", 0),
                "vcp_count": getattr(b, "vcp_count", 0),
                "stage1_count": getattr(b, "stage1_count", 0),
                "stage2_count": getattr(b, "stage2_count", 0),
                "stage3_count": getattr(b, "stage3_count", 0),
                "stage4_count": getattr(b, "stage4_count", 0),
                "set_index_close": getattr(b, "set_index_close", 0),
                "set_index_change_pct": getattr(b, "set_index_change_pct", 0),
            },
        }

    # Indexes carousel
    if c in ("index", "indexes", "ดัชนี", "ดัชนีหุ้น"):
        idx = _last_indexes or {}
        return {
            "kind": "indexes",
            "count": len(idx),
            "symbols": sorted(idx.keys()),
            "set": idx.get("SET") or None,
        }

    # Sector overview (no name)
    if c in ("sector", "sectors", "เซกเตอร์", "กลุ่มหุ้น"):
        trends = _last_sector_trends or []
        return {
            "kind": "sectors",
            "count": len(trends),
            "first_5": [
                {
                    "sector": getattr(t, "sector", None),
                    "stage2_pct": getattr(t, "stage2_pct", None),
                    "advancing": getattr(t, "advancing", None),
                    "declining": getattr(t, "declining", None),
                }
                for t in trends[:5]
            ],
        }

    # 52W high / low lists
    if c in ("52wh", "52w high", "52whigh", "new highs", "new_highs"):
        sigs = sorted(
            [s for s in _last_signals if s.high_52w > 0 and s.close >= s.high_52w * 0.99],
            key=lambda s: -(s.close / s.high_52w - 1),
        )
        return summary(sigs, "52W High")
    if c in ("52wl", "52w low", "52wlow", "new lows", "new_lows"):
        sigs = sorted(
            [s for s in _last_signals if s.low_52w > 0 and s.close <= s.low_52w * 1.01],
            key=lambda s: (s.close / s.low_52w - 1),
        )
        return summary(sigs, "52W Low")

    # Static cards — must not error even with empty state
    if c in ("guide", "คู่มือ", "explain all", "all explain"):
        return {"kind": "static", "handler": "guide"}
    if c in ("stage", "stages", "สเตจ"):
        return {"kind": "static", "handler": "stage_picker"}
    if c in ("patterns", "pattern", "รูปแบบ"):
        return {"kind": "static", "handler": "pattern_overview"}
    if c in ("subsector", "subsectors", "หมวดย่อย"):
        from collections import Counter
        from data import SUBSECTOR_TO_SECTOR, get_subsector
        counts = Counter((get_subsector(s.symbol) or "—") for s in _last_signals)
        return {
            "kind": "subsector",
            "configured_codes": sorted(SUBSECTOR_TO_SECTOR.keys()),
            "counts": dict(sorted(counts.items())),
            "unmapped": counts.get("—", 0),
        }
    if c in ("help", "ช่วย", "คำสั่ง", "?"):
        return {"kind": "static", "handler": "help"}
    if c.startswith("explain "):
        metric = c[len("explain "):].strip()
        return {"kind": "static", "handler": "explain", "metric": metric}

    # detail SYM — uses same signal lookup path, reports fundamentals availability
    if c.startswith("detail "):
        from data import resolve_symbol
        raw = c[len("detail "):].strip()
        sym = resolve_symbol(raw)
        if not sym:
            return {"kind": "detail", "signal": None, "error": f"unresolved_symbol:{raw}"}
        signal = next((s for s in _last_signals if s.symbol == sym), None)
        if signal is None and FIRESTORE_AVAILABLE and _db:
            signal = await loop.run_in_executor(None, load_signal_from_firestore, _db, sym)
        return {"kind": "detail", "signal": _signal_snapshot(signal) if signal else None}

    # Watchlist dispatch probe — mirrors the LINE-side add/remove
    # resolution without touching Firestore. Lets e2e verify that
    # "add BTC" / "remove SPX" route to the global path while "add ADVANC"
    # still resolves to the SET path, all without needing a user_id.
    if c.startswith("add ") or c.startswith("remove "):
        op, _, raw = c.partition(" ")
        raw = raw.strip()
        if is_global_code(raw):
            return {"kind": f"watchlist_{op}", "raw": raw,
                    "resolved": raw.upper(), "source": "global"}
        from data import resolve_symbol as _rs
        sym = _rs(raw)
        if sym:
            return {"kind": f"watchlist_{op}", "raw": raw,
                    "resolved": sym, "source": "set"}
        return {"kind": f"watchlist_{op}", "raw": raw,
                "resolved": None, "source": None}

    # Single asset lookup — global takes precedence so typing "BTC" / "SPX" /
    # "GOOG" hits the global detail card, not a SET ticker (there IS a SET
    # retail ticker named GLOBAL that would otherwise hijack it).
    if is_global_code(c):
        code = c.strip().upper()
        asset = await loop.run_in_executor(None, fetch_global_asset, code)
        if asset is None:
            return {"kind": "global_single", "code": code, "asset": None,
                    "error": "fetch_failed"}
        return {
            "kind": "global_single",
            "code": code,
            "asset": {
                "code": asset["code"],
                "name": asset["name"],
                "class": asset["class"],
                "close": asset["close"],
                "change_pct": asset["change_pct"],
                "week52_high": asset["week52_high"],
                "week52_low": asset["week52_low"],
                "volume": asset["volume"],
                # Stage + pattern fields added with the global stage/pattern
                # rollout — let e2e verify they round-trip correctly.
                "stage": asset.get("stage"),
                "pattern": asset.get("pattern"),
                "stage_weakening": asset.get("stage_weakening", False),
                "sma50": asset.get("sma50", 0.0),
            },
        }

    # Single stock lookup (any recognised SET ticker)
    from data import resolve_symbol
    sym = resolve_symbol(c)
    if sym:
        signal = next((s for s in _last_signals if s.symbol == sym), None)
        if signal is None and FIRESTORE_AVAILABLE and _db:
            signal = await loop.run_in_executor(None, load_signal_from_firestore, _db, sym)
        return {"kind": "single_stock", "signal": _signal_snapshot(signal) if signal else None}

    return {"kind": "unknown", "cmd": c}


@app.get("/test/indexes")
async def test_indexes(x_scan_secret: Optional[str] = Header(default=None)):
    """Debug why fetch_indexes_with_history may return < all INDEX_SYMBOLS.

    For each configured index, reports rows fetched and a sample of the tail.
    """
    settings = get_settings()
    if not secrets.compare_digest(x_scan_secret or "", settings.scan_secret):
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid scan secret")

    from data import INDEX_SYMBOLS, fetch_indexes_with_history
    loop = asyncio.get_running_loop()
    dfs = await loop.run_in_executor(None, fetch_indexes_with_history)
    out: dict = {
        "configured": INDEX_SYMBOLS,
        "last_indexes_keys": sorted((_last_indexes or {}).keys()),
        "fetch_result": {},
    }
    for name in INDEX_SYMBOLS:
        df = dfs.get(name)
        if df is None or df.empty:
            out["fetch_result"][name] = None
        else:
            out["fetch_result"][name] = {
                "rows": len(df),
                "latest": str(df.index[-1].date()),
                "latest_close": round(float(df["Close"].iloc[-1]), 2),
            }
    return out


@app.get("/test/settrade_sectors")
async def test_settrade_sectors(symbol: str = "PTT", x_scan_secret: Optional[str] = Header(default=None)):
    """Introspect the settrade_v2 SDK to discover sector / industry methods.

    We've been classifying stocks via yfinance .info which has gaps for Thai
    REITs / funds. If Settrade's SDK exposes sector data natively, we should
    use that instead. This endpoint enumerates the MarketData class methods,
    shows the raw get_quote_symbol response (including all fields, not just
    the ones our adapter exposes), and probes likely method names.

    Query param `symbol` (default PTT) is used for the per-symbol probes.
    """
    settings = get_settings()
    if not secrets.compare_digest(x_scan_secret or "", settings.scan_secret):
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid scan secret")

    from settrade_client import _get_investor
    investor = _get_investor()
    if not investor:
        return {"error": "Settrade SDK not initialised"}

    result: dict = {"symbol_tested": symbol}
    try:
        market = investor.MarketData()
        # List all public methods on MarketData
        methods = sorted(m for m in dir(market) if not m.startswith("_"))
        result["MarketData_methods"] = methods

        # Raw get_quote_symbol — show ALL fields, not our filtered adapter
        try:
            q = market.get_quote_symbol(symbol)
            result["get_quote_symbol_raw_fields"] = sorted(q.keys()) if q else None
            # Look specifically for sector/industry fields
            if q:
                sector_like = {k: q[k] for k in q if any(
                    kw in k.lower() for kw in ("sector", "industry", "group", "segment", "category", "class")
                )}
                result["get_quote_symbol_sector_like"] = sector_like or "(none found)"
        except Exception as e:
            result["get_quote_symbol_error"] = f"{type(e).__name__}: {e}"

        # Probe likely sector/industry method names
        probes = [
            "get_instrument_info", "get_instrument_by_symbol", "get_instrument",
            "get_sector_info", "get_industry_info", "get_industry",
            "get_symbol_info", "get_stock_info",
            "get_market_info", "get_market_stat",
            "get_sector_list", "get_industry_list",
            "get_index_symbols", "get_sector_symbols",
        ]
        probe_results: dict = {}
        for m in probes:
            if hasattr(market, m):
                try:
                    fn = getattr(market, m)
                    # Try no-arg first, then with symbol
                    try:
                        out = fn()
                    except TypeError:
                        try:
                            out = fn(symbol)
                        except Exception as e2:
                            out = f"signature_error: {e2}"
                    # Summarise large outputs
                    if isinstance(out, dict):
                        probe_results[m] = {"type": "dict", "keys": sorted(out.keys())[:20], "size": len(out)}
                    elif isinstance(out, list):
                        probe_results[m] = {"type": "list", "size": len(out),
                                            "sample": out[:3] if out and len(str(out[:3])) < 800 else str(out[:1])[:400]}
                    else:
                        probe_results[m] = {"type": type(out).__name__, "repr": repr(out)[:400]}
                except Exception as e:
                    probe_results[m] = {"error": f"{type(e).__name__}: {e}"}
            else:
                probe_results[m] = "not_present"
        result["probes"] = probe_results
    except Exception as exc:
        result["error"] = f"{type(exc).__name__}: {exc}"
    return result


@app.get("/test/sector_coverage")
async def test_sector_coverage(x_scan_secret: Optional[str] = Header(default=None)):
    """Sector classification coverage report.

    Tells us:
      - How many SET stocks have a yfinance-derived subsector mapping in
        _dynamic_sector_map vs how many fall through to the static SECTOR_MAP.
      - How many end up as "OTHER" (no classification).
      - Per-sector count of stocks (8 main + OTHER).
      - Per-subsector count (25 codes used).
      - Sector index prices currently in _last_sector_indexes.

    Used by scripts/e2e_check.py to assert coverage doesn't regress.
    """
    settings = get_settings()
    if not secrets.compare_digest(x_scan_secret or "", settings.scan_secret):
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid scan secret")

    from data import (
        _dynamic_sector_map, _MANUAL_SUBSECTOR_OVERRIDES,
        SECTOR_MAP, SUBSECTOR_TO_SECTOR,
        SECTOR_INDEX_SYMBOLS, get_sector, get_subsector, get_stock_list,
    )

    # Lazy-load sector_indexes from Firestore if this instance hasn't scanned yet
    global _last_sector_indexes
    if not _last_sector_indexes and FIRESTORE_AVAILABLE and _db:
        loop = asyncio.get_running_loop()
        state = await loop.run_in_executor(None, load_scan_state, _db)
        if state:
            _last_sector_indexes = state.get("sector_indexes") or {}

    symbols = get_stock_list()
    sector_counts: dict[str, int] = {}
    subsector_counts: dict[str, int] = {}
    via_manual = 0
    via_dynamic = 0
    via_static = 0
    unmapped: list[str] = []

    for sym in symbols:
        if sym in _MANUAL_SUBSECTOR_OVERRIDES:
            via_manual += 1
            sub = _MANUAL_SUBSECTOR_OVERRIDES[sym]
            subsector_counts[sub] = subsector_counts.get(sub, 0) + 1
        elif sym in _dynamic_sector_map:
            via_dynamic += 1
            sub = _dynamic_sector_map[sym]
            subsector_counts[sub] = subsector_counts.get(sub, 0) + 1
        elif sym in SECTOR_MAP:
            via_static += 1
        else:
            unmapped.append(sym)

        sector = get_sector(sym)
        sector_counts[sector] = sector_counts.get(sector, 0) + 1

    total = len(symbols)
    subsector_mapped = via_manual + via_dynamic
    return {
        "total_symbols": total,
        "via_manual_override": via_manual,
        "via_dynamic_subsector": via_dynamic,
        "via_static_sector_only": via_static,
        "unmapped_other": len(unmapped),
        "coverage_pct": round((subsector_mapped + via_static) / total * 100, 1) if total else 0.0,
        "subsector_coverage_pct": round(subsector_mapped / total * 100, 1) if total else 0.0,
        "sector_counts": dict(sorted(sector_counts.items())),
        "subsector_counts": dict(sorted(subsector_counts.items())),
        "unmapped_sample": unmapped[:20],
        "configured_sector_indexes": SECTOR_INDEX_SYMBOLS,
        "live_sector_indexes": _last_sector_indexes,
    }


@app.get("/test/invariants")
async def test_invariants(x_scan_secret: Optional[str] = Header(default=None)):
    """Validate invariants that should always hold on _last_signals.

    Every returned boolean is an assertion the e2e suite can rely on. `details`
    carries the numbers behind each assertion so regressions are diagnosable
    without extra requests.
    """
    settings = get_settings()
    if not secrets.compare_digest(x_scan_secret or "", settings.scan_secret):
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid scan secret")

    global _last_signals
    loop = asyncio.get_running_loop()
    if not _last_signals:
        # Prefer Firestore — it serializes the full StockSignal dataclass via
        # signal.__dict__, so new fields (sub_stage, pivot_price, pivot_stop,
        # sma10/20, sma200_roc20) are present on lazy-load. The BQ scan_results
        # table has a fixed schema that lags new fields by one ALTER TABLE
        # migration, so loading from BQ leaves new fields at default 0.0/"".
        # BQ stays as a fallback for the rare case Firestore is unavailable.
        if FIRESTORE_AVAILABLE and _db:
            fs_sigs = await loop.run_in_executor(None, load_signals_from_firestore, _db)
            if fs_sigs:
                _last_signals = fs_sigs
        if not _last_signals and BQ_AVAILABLE:
            bq_sigs = await loop.run_in_executor(None, load_latest_signals_from_bq)
            if bq_sigs:
                _last_signals = bq_sigs

    sigs = list(_last_signals)
    total = len(sigs)

    # 1. change_pct partition is disjoint + exhaustive
    adv = sum(1 for s in sigs if (s.change_pct or 0) > 0)
    dec = sum(1 for s in sigs if (s.change_pct or 0) < 0)
    flat = sum(1 for s in sigs if (s.change_pct or 0) == 0)
    partition_ok = (adv + dec + flat == total) and all(c >= 0 for c in (adv, dec, flat))

    # 2. stage counts sum to total
    stage_counts = {n: sum(1 for s in sigs if s.stage == n) for n in (1, 2, 3, 4)}
    stage_ok = sum(stage_counts.values()) == total

    # 3. pattern counts
    pattern_counts: dict[str, int] = {}
    for s in sigs:
        pattern_counts[s.pattern] = pattern_counts.get(s.pattern, 0) + 1
    pattern_ok = sum(pattern_counts.values()) == total

    # 4. freshness: every signal's data_date within 10 days of today (matches MAX_CANDLE_STALENESS_DAYS)
    today = datetime.now(BANGKOK_TZ).date()
    stale = []
    for s in sigs:
        try:
            d = datetime.strptime(getattr(s, "data_date", "") or "", "%Y-%m-%d").date()
            if (today - d).days > 10:
                stale.append({"symbol": s.symbol, "data_date": s.data_date})
        except ValueError:
            stale.append({"symbol": s.symbol, "data_date": s.data_date, "error": "unparseable"})
    freshness_ok = len(stale) == 0

    # 5. ATH cache internal consistency: no signal has close > its cached ATH + a small ε
    ath_violations = []
    for s in sigs:
        cached = _ath_cache.get(s.symbol, 0.0)
        if cached > 0 and s.close > cached * 1.05:
            # >5% above cached ATH is implausibly large — either stale ATH or bad signal
            ath_violations.append({"symbol": s.symbol, "close": s.close, "cached_ath": cached})
    ath_ok = len(ath_violations) == 0

    # 6. all ath_breakout / breakout signals are Stage 2 or 3
    bad_breakout_stage = [
        {"symbol": s.symbol, "stage": s.stage, "pattern": s.pattern}
        for s in sigs
        if s.pattern in ("breakout", "ath_breakout") and s.stage not in (2, 3)
    ]
    breakout_stage_ok = len(bad_breakout_stage) == 0

    # 7. going_down signals should be Stage 4
    bad_going_down = [
        {"symbol": s.symbol, "stage": s.stage}
        for s in sigs
        if s.pattern == "going_down" and s.stage != 4
    ]
    going_down_stage_ok = len(bad_going_down) == 0

    # 8. strength_score in [0, 100]
    bad_score = [{"symbol": s.symbol, "score": s.strength_score} for s in sigs
                 if s.strength_score < 0 or s.strength_score > 100]
    score_range_ok = len(bad_score) == 0

    return {
        "total_signals": total,
        "partition_ok": partition_ok,
        "stage_ok": stage_ok,
        "pattern_ok": pattern_ok,
        "freshness_ok": freshness_ok,
        "ath_ok": ath_ok,
        "breakout_stage_ok": breakout_stage_ok,
        "going_down_stage_ok": going_down_stage_ok,
        "score_range_ok": score_range_ok,
        "details": {
            "adv_dec_flat": [adv, dec, flat],
            "stage_counts": stage_counts,
            "pattern_counts": pattern_counts,
            "stale_count": len(stale),
            "stale_sample": stale[:5],
            "ath_violations_count": len(ath_violations),
            "ath_violations_sample": ath_violations[:5],
            "bad_breakout_stage_sample": bad_breakout_stage[:5],
            "bad_going_down_sample": bad_going_down[:5],
            "bad_score_sample": bad_score[:5],
        },
    }


@app.get("/test/ath/{symbol}")
async def test_ath(symbol: str):
    """Compare ATH sources for a symbol: Settrade 5Y vs yfinance max vs cached.

    Helps diagnose false ath_breakout classifications caused by a data
    provider that doesn't have enough history (yfinance has documented gaps
    for Thai stocks; Settrade goes back 5Y directly from SET).
    """
    symbol = symbol.upper().strip()
    result: dict = {"symbol": symbol}

    # --- Settrade: try multiple periods to diagnose SDK limit behaviour ---
    from settrade_client import get_ohlcv
    result["settrade"] = {}
    for period in ("5Y", "3Y", "1Y"):
        try:
            st_df = get_ohlcv(symbol, period=period)
            if st_df is not None and not st_df.empty:
                st_high = st_df["High"].max()
                st_high_date = st_df["High"].idxmax()
                result["settrade"][period] = {
                    "rows": len(st_df),
                    "earliest": str(st_df.index.min().date()),
                    "latest": str(st_df.index.max().date()),
                    "max_high": round(float(st_high), 4),
                    "max_high_date": str(pd.Timestamp(st_high_date).date()),
                }
            else:
                result["settrade"][period] = None
        except Exception as e:
            result["settrade"][period] = {"error": f"{type(e).__name__}: {e}"}

    # --- yfinance adjusted AND unadjusted max (for split-aware comparison) ---
    from data import _to_yf_ticker
    import yfinance as _yf
    ticker = "^SET.BK" if symbol == "SET" else _to_yf_ticker(symbol)

    for label, adjust in (("yfinance_adjusted", True), ("yfinance_unadjusted", False)):
        try:
            yf_df = _yf.download(ticker, period="max", progress=False, auto_adjust=adjust)
            if yf_df is not None and not yf_df.empty:
                if isinstance(yf_df.columns, pd.MultiIndex):
                    yf_df.columns = yf_df.columns.get_level_values(0)
                yf_df = yf_df.dropna(subset=["Close"])
                yf_high = yf_df["High"].max()
                yf_high_date = yf_df["High"].idxmax()
                result[label] = {
                    "rows": len(yf_df),
                    "earliest": str(pd.Timestamp(yf_df.index.min()).date()),
                    "latest": str(pd.Timestamp(yf_df.index.max()).date()),
                    "max_high": round(float(yf_high), 4),
                    "max_high_date": str(pd.Timestamp(yf_high_date).date()),
                }
            else:
                result[label] = None
        except Exception as e:
            result[label] = {"error": f"{type(e).__name__}: {e}"}

    # --- Current cached ATH ---
    result["cached"] = {
        "in_memory": _ath_cache.get(symbol),
    }
    if FIRESTORE_AVAILABLE and _db:
        try:
            doc = _db.collection("ath_cache").document(symbol).get()
            result["cached"]["firestore"] = doc.to_dict() if doc.exists else None
        except Exception as e:
            result["cached"]["firestore_error"] = str(e)

    return result


@app.get("/test/settrade")
async def test_settrade(sample_size: int = 20, symbols: Optional[str] = None):
    """Test SET Trade API connectivity, measure bulk-quote latency + coverage.

    Query params:
      sample_size: int (default 20) — bulk-quote sample size for latency / coverage.
      symbols:     str — comma-separated list (e.g. "BBL,BDMS"). When provided,
                   returns raw SDK output per symbol so we can diagnose why
                   specific symbols fail in get_bulk_quotes.
    """
    from settrade_client import _get_investor, get_bulk_quotes, get_ohlcv

    result: dict = {"api_available": False}

    investor = _get_investor()
    if not investor:
        result["error"] = "Cannot init Investor — check credentials"
        return result

    result["api_available"] = True
    market = investor.MarketData()

    # ── Single quote ──
    try:
        q = market.get_quote_symbol("PTT")
        result["quote_PTT"] = q
    except Exception as e:
        result["quote_error"] = str(e)

    # ── Candlestick (parsed) ──
    try:
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

    # ── Bulk-quote coverage + latency ──
    try:
        universe = get_stock_list()[:max(1, min(sample_size, 900))]
        t0 = time.monotonic()
        quotes = get_bulk_quotes(universe)
        elapsed = time.monotonic() - t0
        result["bulk_quote_sample"] = {
            "requested": len(universe),
            "returned": len(quotes),
            "coverage_pct": round(len(quotes) / len(universe) * 100, 1) if universe else 0.0,
            "elapsed_sec": round(elapsed, 2),
            "projected_900_sec": round(elapsed * 900 / max(1, len(universe)), 1),
            "missing_symbols": [s for s in universe if s not in quotes][:10],
        }
    except Exception as e:
        result["bulk_quote_error"] = str(e)

    # ── Per-symbol raw SDK diagnostics (for BBL/BDMS etc.) ──
    if symbols:
        per_symbol: dict = {}
        for sym in [s.strip().upper() for s in symbols.split(",") if s.strip()]:
            entry: dict = {"quote_raw": None, "quote_error": None,
                           "ohlcv_rows": None, "ohlcv_latest": None, "ohlcv_error": None}
            try:
                entry["quote_raw"] = market.get_quote_symbol(sym)
            except Exception as e:
                entry["quote_error"] = f"{type(e).__name__}: {e}"
            try:
                df = get_ohlcv(sym, period="1M")
                if df is not None and not df.empty:
                    entry["ohlcv_rows"] = len(df)
                    entry["ohlcv_latest"] = {
                        "date": str(df.index[-1].date()),
                        "close": round(float(df["Close"].iloc[-1]), 2),
                    }
            except Exception as e:
                entry["ohlcv_error"] = f"{type(e).__name__}: {e}"
            per_symbol[sym] = entry
        result["per_symbol"] = per_symbol

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

    global _last_signals, _last_breadth, _last_breadth_card, _last_scan_time, _last_indexes, _last_sector_trends, _ath_cache, _ath_cache_loaded_date, _last_sector_indexes

    logger.info("Running scan: type=%s mode=%s broadcast=%s", body.scan_type, body.mode, body.broadcast)
    loop = asyncio.get_running_loop()

    # Daily ATH reload — refresh _ath_cache once per calendar day (BKK) so /sync_ath
    # writes propagate to running Cloud Run instances without restart, but we don't
    # hit Firestore on every scan. Also covers first-scan-after-boot.
    today_bkk = datetime.now(BANGKOK_TZ).strftime("%Y-%m-%d")
    if not _ath_cache or _ath_cache_loaded_date != today_bkk:
        _ath_cache = await loop.run_in_executor(None, _load_ath_merged)
        _ath_cache_loaded_date = today_bkk
        logger.info("ATH cache reloaded for %s: %d entries", today_bkk, len(_ath_cache))

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

    # Always fetch full index history for MACD/RSI on all scan types. Fetched BEFORE
    # compute_market_breadth so it can serve as a fallback when all_data["SET"] is
    # missing (e.g., Settrade doesn't carry SET and yfinance merge silently drops it).
    index_dfs = await loop.run_in_executor(None, fetch_indexes_with_history)
    indexes = _analyze_index_dfs(index_dfs)

    set_df = all_data.get("SET")
    if set_df is None or len(set_df) < 2:
        set_df = index_dfs.get("SET")
    breadth = compute_market_breadth(signals, index_df=set_df)
    sector_trends = compute_sector_trends(signals)
    del index_dfs

    # Fetch SET sector index prices (AGRO, FINCIAL, TECH, etc.) — fire and forget
    sector_indexes = await loop.run_in_executor(None, fetch_sector_index_prices)

    _last_signals = signals
    _last_scan_time = datetime.now(BANGKOK_TZ)
    _last_breadth = breadth
    _last_sector_trends = sector_trends
    _last_sector_indexes = sector_indexes
    _last_breadth_card = build_market_breadth_card(breadth, sector_trends, indexes)
    _last_indexes = indexes

    # Persist scan results: BQ on every scan (primary); Firestore always (cache)
    if BQ_AVAILABLE:
        loop.run_in_executor(None, save_signals_to_bq, signals)
    if FIRESTORE_AVAILABLE and _db:
        loop.run_in_executor(
            None, functools.partial(
                save_scan_state, _db, breadth, indexes, sector_trends,
                body.scan_type, body.mode, sector_indexes=sector_indexes,
            ),
        )
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

    # Always broadcast the full report (market + breakouts + fallen + per-user watchlist)
    # regardless of scan_type — every scheduled scan sends the same consistent output
    _broadcast_full_report(breadth, signals)

    return {"scanned": len(signals), "mode": body.mode, "broadcast": body.scan_type}


@app.post("/sync_ath")
async def sync_ath_endpoint(
    x_scan_secret: Optional[str] = Header(default=None),
    chunk: int = 0,
    chunk_size: int = 20,
    symbol: Optional[str] = None,
):
    """Sync all-time high cache from yfinance max-period history into Firestore.

    Modes:
      - ?symbol=WHAIR — sync just one symbol (seconds). Useful for targeted
        refresh when you know one ATH is stale.
      - ?chunk=N&chunk_size=M — sync symbols[N*M:(N+1)*M]. Increment chunk
        until next_chunk=null. Default chunk_size=20 keeps each request
        well under Cloud Run's 5-min timeout.
    """
    settings = get_settings()
    if not secrets.compare_digest(x_scan_secret or "", settings.scan_secret):
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid scan secret")
    if not FIRESTORE_AVAILABLE or not _db:
        raise HTTPException(status_code=503, detail="Firestore not available")

    global _ath_cache
    if symbol:
        sym = symbol.upper().strip()
        synced = sync_ath_to_firestore(_db, [sym], chunk=0, chunk_size=1)
        _ath_cache.update(synced)
        return {"synced": synced, "mode": "single_symbol"}

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


@app.post("/admin/refresh_index_members")
async def refresh_index_members_endpoint(
    request: Request,
    x_scan_secret: Optional[str] = Header(default=None),
):
    """Update SET50 / SET100 / MAI member lists.

    Two modes:
      1. Body provided: {"SET50": ["ADVANC", "AOT", ...], "SET100": [...]}
         → use the provided lists verbatim. For when you've fetched fresh
         lists from SET's site / official feed and want to push them
         without redeploying.
      2. No body / empty body: just verify current in-memory lists exist
         and return their counts. Useful for the monthly Cloud Scheduler
         health-check job — logs membership state without mutating it.

    Persists to Firestore index_members/{INDEX_NAME} so subsequent
    instance restarts pick up the new lists. In-memory _index_members
    in data.py is also updated immediately for the current process.
    """
    settings = get_settings()
    if not secrets.compare_digest(x_scan_secret or "", settings.scan_secret):
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid scan secret")

    from data import set_index_members, index_member_counts, get_index_members

    body: dict = {}
    try:
        body = await request.json() if (await request.body()) else {}
    except Exception:
        body = {}

    updated: dict[str, int] = {}
    if body:
        # Validate + apply
        for index_name in ("SET50", "SET100", "MAI"):
            members_in = body.get(index_name)
            if not members_in:
                continue
            if not isinstance(members_in, list):
                raise HTTPException(
                    status_code=400,
                    detail=f"{index_name} must be a list of ticker codes",
                )
            members_set = {str(s).strip().upper() for s in members_in if s}
            set_index_members(index_name, members_set)
            updated[index_name] = len(members_set)
            # Persist to Firestore
            if FIRESTORE_AVAILABLE and _db:
                try:
                    _db.collection("index_members").document(index_name).set({
                        "members": sorted(members_set),
                        "count": len(members_set),
                        "updated_at": pd.Timestamp.utcnow().isoformat(),
                    }, merge=False)
                except Exception as exc:
                    logger.error("refresh_index_members: Firestore write %s failed: %s",
                                 index_name, exc)

    return {
        "updated": updated,
        "current_counts": index_member_counts(),
        "members_sample": {
            k: sorted(get_index_members(k))[:5]
            for k in ("SET50", "SET100", "MAI")
        },
    }


@app.post("/admin/refresh_sector_map")
async def refresh_sector_map_endpoint(
    x_scan_secret: Optional[str] = Header(default=None),
):
    """One-time endpoint: fetch subsector classification for all SET stocks via yfinance.info,
    cache in Firestore sector_map/latest, wire into live sector routing.
    Takes ~2 min for 900 stocks. Call once; data is reused across all restarts."""
    settings = get_settings()
    if not secrets.compare_digest(x_scan_secret or "", settings.scan_secret):
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid scan secret")

    global _last_sector_trends
    loop = asyncio.get_running_loop()
    symbols = get_stock_list()
    sector_map = await loop.run_in_executor(None, fetch_sector_map_from_yfinance, symbols)

    # Wire into live routing
    from data import _dynamic_sector_map as _dsm
    _dsm.clear()
    _dsm.update(sector_map)

    # Persist to Firestore
    if FIRESTORE_AVAILABLE and _db:
        await loop.run_in_executor(None, save_sector_map_to_firestore, sector_map, _db)

    # Recompute sector trends with new mapping
    if _last_signals:
        _last_sector_trends = compute_sector_trends(_last_signals)

    return {
        "mapped": len(sector_map),
        "total_symbols": len(symbols),
        "coverage_pct": round(len(sector_map) / len(symbols) * 100, 1),
        "sample": dict(list(sector_map.items())[:10]),
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
    card = _last_breadth_card or build_market_breadth_card(breadth, _last_sector_trends, _last_indexes)
    broadcast_flex("ภาพรวมตลาด SET", card)


def _broadcast_full_report(breadth: MarketBreadth, signals: list[StockSignal]) -> None:
    # ─────────────────────────────────────────────────────────────────
    # Pre-launch posture: broadcast ONLY the market breadth card per
    # scheduled scan — LINE free-tier push quota conservation. The
    # breakout/fallen list broadcasts and the per-user watchlist
    # multicast are intentionally omitted; users still get those views
    # on demand via text commands (`breakout`, `vcp`, `watchlist`),
    # which use reply_flex and don't count against broadcast quota.
    #
    # On launch, re-enable the fuller body (git log for 'cost: limit
    # broadcast to breadth-only'). See CLAUDE.md "Deploy pipeline".
    # `signals` kept in the signature so the call site doesn't need
    # to change when we expand this back out.
    # ─────────────────────────────────────────────────────────────────
    _broadcast_breadth(breadth)


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
    global _last_breadth, _last_breadth_card, _last_indexes, _last_sector_trends, _last_signals
    if not reply_token:
        return

    loop = asyncio.get_running_loop()
    cmd = text.lower().strip()

    # Lazy reload of _last_signals on webhook instances that haven't populated it
    # yet (cold Cloud Run instance after scale-up, or warmup raced with an early
    # request). List handlers (advancing/declining/flat/stage/pattern) filter from
    # _last_signals directly — an empty list here produces "no stock" cards even
    # though Firestore/BQ have fresh signals.
    if not _last_signals:
        # Firestore first — has full StockSignal dataclass; BQ fallback.
        if FIRESTORE_AVAILABLE and _db:
            fs_sigs = await loop.run_in_executor(None, load_signals_from_firestore, _db)
            if fs_sigs:
                _last_signals = fs_sigs
                logger.info("_handle_text_query: lazy-reloaded %d signals from Firestore", len(fs_sigs))
        if not _last_signals and BQ_AVAILABLE:
            bq_sigs = await loop.run_in_executor(None, load_latest_signals_from_bq)
            if bq_sigs:
                _last_signals = bq_sigs
                logger.info("_handle_text_query: lazy-reloaded %d signals from BQ", len(bq_sigs))

    # ── Explain metric ⓘ ──
    if cmd.startswith("explain "):
        # Normalize aliases
        _aliases = {"explain volume": "explain volume_ratio", "explain vol": "explain volume_ratio"}
        lookup = _aliases.get(cmd, cmd)
        metric_name = lookup.replace("explain ", "")

        # Route stage queries to the comprehensive stage cycle card
        if metric_name in ("stage", "stage1", "stage2", "stage3", "stage4"):
            reply_flex(reply_token, "📊 Stage Analysis Guide", build_stage_cycle_card())
        # Route pivot query to dedicated explainer card
        elif metric_name in ("pivot", "pivot_point", "pivot point"):
            reply_flex(reply_token, "🎯 Pivot Point", build_pivot_explainer_card())
        # Route pattern queries to rich pattern cards
        elif metric_name in ("breakout", "ath_breakout", "vcp", "vcp_low_cheat", "consolidating", "going_down"):
            reply_flex(reply_token, f"📈 {metric_name.replace('_', ' ').title()}", build_pattern_detail_card(metric_name))
        else:
            explanation = _EXPLANATIONS.get(lookup)
            if explanation:
                reply_flex(reply_token, f"ℹ️ {metric_name}", build_explain_card(metric_name, explanation))
            else:
                reply_text(reply_token, f'ไม่พบคำอธิบายสำหรับ "{metric_name}"')

    # ── Per-index breadth: SET50 / SET100 / MAI ──
    # Catch the more-specific filter forms first so they don't fall into
    # the generic 'set50' breadth command.
    elif cmd.startswith("set50 ") or cmd.startswith("set100 ") or cmd in ("mai members", "mai list", "mai all"):
        # Support both 'set50 stage2' (filter by stage) and 'set50 list' /
        # 'set50 members' / 'set50 all' (full member list).
        parts = cmd.split(maxsplit=1)
        index_name = parts[0].upper()
        rest = parts[1].strip() if len(parts) > 1 else ""
        if cmd.startswith("mai "):
            index_name = "MAI"
            rest = cmd[len("mai "):].strip()
        await _reply_index_filter(reply_token, index_name, rest)

    elif cmd in ("set50", "set 50"):
        await _reply_index_breadth(reply_token, "SET50")

    elif cmd in ("set100", "set 100"):
        await _reply_index_breadth(reply_token, "SET100")

    elif cmd in ("mai", "ไหม"):
        await _reply_index_breadth(reply_token, "MAI")

    elif cmd in ("ตลาด", "market", "breadth"):
        card = _last_breadth_card
        if card is None and FIRESTORE_AVAILABLE and _db:
            state = await loop.run_in_executor(None, load_scan_state, _db)
            if state:
                _last_breadth = state["breadth"]
                _last_sector_trends = state.get("sector_trends", [])
                _last_indexes = state.get("indexes", {})
                try:
                    _last_breadth_card = build_market_breadth_card(_last_breadth, _last_sector_trends, _last_indexes)
                    card = _last_breadth_card
                except Exception as exc:
                    logger.error("build_market_breadth_card on-demand failed: %s", exc)
        if card is None:
            reply_text(reply_token, "ยังไม่มีข้อมูลตลาด กรุณารอการสแกนครั้งถัดไป")
            return
        reply_flex(reply_token, "ภาพรวมตลาด SET", card)

    # ── Key Indexes ──
    elif cmd in ("index", "indexes", "ดัชนี", "ดัชนีหุ้น"):
        indexes = _last_indexes
        if not indexes and FIRESTORE_AVAILABLE and _db:
            state = await loop.run_in_executor(None, load_scan_state, _db)
            if state:
                indexes = state.get("indexes", {})
                _last_indexes = indexes
        if not indexes:
            reply_text(reply_token, "ยังไม่มีข้อมูลดัชนี กรุณารอการสแกนครั้งถัดไป")
            return
        carousel = build_index_carousel(indexes)
        reply_flex(reply_token, "ดัชนีหุ้นไทย", carousel)

    # ── Global Snapshot (US + Asia indexes + ETFs + US stocks + crypto) ──
    # On-demand only — each call pulls ~28 yfinance quotes in parallel (~3-5s).
    # No scheduled broadcast; this is a "check the world" watchlist view.
    elif cmd in ("global", "world", "g", "ตลาดโลก", "กลอบอล"):
        snapshot = await loop.run_in_executor(None, fetch_global_snapshot)
        if not snapshot:
            reply_text(reply_token, "ดึงข้อมูลตลาดโลกไม่สำเร็จ ลองใหม่อีกครั้ง")
            return
        card = build_global_snapshot_card(snapshot)
        reply_flex(reply_token, "🌏 Global Snapshot", card)

    # ── Sector Trends: overview or drill-down ──
    elif cmd.startswith("sector ") and len(cmd) > 7:
        # Parse optional page: "sector INDUS p2"
        rest = cmd[7:].upper().strip()
        sec_page = 1
        if " P" in rest:
            parts = rest.rsplit(" P", 1)
            rest = parts[0].strip()
            try:
                sec_page = int(parts[1])
            except ValueError:
                pass
        sector_name = rest
        # get_sector() uses dynamic subsector map + SECTOR_MAP fallback
        # Also match by subsector code (e.g. "sector BANK" returns all BANK stocks)
        from data import get_subsector
        sector_sigs = [
            s for s in _last_signals
            if get_sector(s.symbol) == sector_name or get_subsector(s.symbol) == sector_name
        ]
        sector_sigs.sort(key=lambda s: s.strength_score, reverse=True)
        if sector_sigs:
            # Subsector breakdown for the header — most-common first, top 5
            from collections import Counter
            subs = Counter((get_subsector(s.symbol) or "—") for s in sector_sigs)
            breakdown = " · ".join(f"{code}:{n}" for code, n in subs.most_common(5))
            subtitle = f"Subsector: {breakdown}" if subs else "Sorted by Strength Score"
            _reply_stock_list(reply_token, sector_sigs, f"🏭 {sector_name} — Leaders",
                              page=sec_page, base_cmd=f"sector {sector_name}",
                              subtitle=subtitle)
        else:
            reply_text(reply_token,
                       f"ไม่พบหุ้นในกลุ่ม {sector_name}\n"
                       f"กลุ่มหลัก: AGRO, CONSUMP, FINCIAL, INDUS, PROPCON, RESOURC, SERVICE, TECH\n"
                       f"กลุ่มย่อย: ลองพิมพ์ 'subsector' เพื่อดูรหัสทั้งหมด")

    elif cmd in ("sector", "sectors", "เซกเตอร์", "กลุ่มหุ้น"):
        global _last_sector_indexes
        sector_trends = _last_sector_trends
        sector_indexes = _last_sector_indexes
        # Lazy-load both from Firestore when this Cloud Run instance hasn't
        # seen a /scan yet — same multi-instance pattern as _last_signals.
        if (not sector_trends or not sector_indexes) and FIRESTORE_AVAILABLE and _db:
            state = await loop.run_in_executor(None, load_scan_state, _db)
            if state:
                if not sector_trends:
                    sector_trends = state.get("sector_trends", [])
                    _last_sector_trends = sector_trends
                if not sector_indexes:
                    sector_indexes = state.get("sector_indexes") or {}
                    _last_sector_indexes = sector_indexes
        if not sector_trends and _last_signals:
            try:
                sector_trends = compute_sector_trends(_last_signals)
                _last_sector_trends = sector_trends
            except Exception as exc:
                logger.error("compute_sector_trends on-demand failed: %s", exc)
        if not sector_trends:
            reply_text(reply_token, "ยังไม่มีข้อมูลกลุ่มหุ้น กรุณารอการสแกนครั้งถัดไป")
            return
        card = build_sector_overview_card(sector_trends, sector_indexes=sector_indexes)
        reply_flex(reply_token, "แนวโน้มกลุ่มอุตสาหกรรม", card)

    # ── Subsector breakdown ──
    elif cmd in ("subsector", "subsectors", "หมวดย่อย"):
        from collections import Counter
        from data import SUBSECTOR_TO_SECTOR, get_subsector
        SUB_THAI = {
            "AGRI": "เกษตร", "FOOD": "อาหาร/เครื่องดื่ม",
            "FASHION": "แฟชั่น", "HOME": "ของใช้ในบ้าน", "PERSON": "ของใช้ส่วนตัว/ยา",
            "BANK": "ธนาคาร", "FIN": "การเงิน/หลักทรัพย์", "INSUR": "ประกันภัย",
            "AUTO": "ยานยนต์", "IMM": "วัสดุ/เครื่องจักร", "PAPER": "กระดาษ",
            "PETRO": "ปิโตรเคมี", "PKG": "บรรจุภัณฑ์", "STEEL": "เหล็ก",
            "CONMAT": "วัสดุก่อสร้าง", "CONS": "รับเหมาก่อสร้าง",
            "PF": "กองทุน/REIT", "PROP": "อสังหาฯ",
            "ENERG": "พลังงาน", "MINE": "เหมืองแร่",
            "COMM": "ค้าปลีก/ส่ง", "HELTH": "การแพทย์", "MEDIA": "สื่อ",
            "PROF": "บริการเฉพาะ", "TOURISM": "ท่องเที่ยว", "TRANS": "ขนส่ง",
            "ETRON": "ชิ้นส่วนอิเล็กทรอนิกส์", "ICT": "ICT/โทรคมนาคม",
        }
        counts = Counter((get_subsector(s.symbol) or "—") for s in _last_signals)
        # Group by main sector for readable layout
        rows = ["📂 Subsector breakdown (รหัส · ชื่อ · จำนวนหุ้น)\n"]
        sector_groups: dict[str, list[tuple[str, int]]] = {}
        for sub, sector in SUBSECTOR_TO_SECTOR.items():
            sector_groups.setdefault(sector, []).append((sub, counts.get(sub, 0)))
        for sector in sorted(sector_groups.keys()):
            rows.append(f"\n▸ {sector}")
            for sub, n in sector_groups[sector]:
                thai = SUB_THAI.get(sub, "")
                rows.append(f"  {sub:8s} {thai:18s} {n:>3d}")
        unmapped = counts.get("—", 0)
        if unmapped:
            rows.append(f"\n— ไม่ระบุ: {unmapped}")
        rows.append("\nพิมพ์ 'sector BANK' เพื่อดูรายชื่อหุ้นกลุ่มย่อย")
        reply_text(reply_token, "\n".join(rows))

    # ── Guide ──
    elif cmd in ("guide", "คู่มือ", "explain all", "all explain"):
        global _guide_carousel_cache
        if _guide_carousel_cache is None:
            _guide_carousel_cache = build_guide_carousel()
        reply_flex(reply_token, "คู่มือ Signalix", _guide_carousel_cache)

    # ── Stage picker ──
    elif cmd in ("stage", "stages", "สเตจ"):
        breadth = _last_breadth
        # Lazy-load from Firestore when this Cloud Run instance hasn't warmed yet —
        # same pattern as _last_signals. Otherwise the stage picker shows 0 per stage.
        if breadth is None and FIRESTORE_AVAILABLE and _db:
            state = await loop.run_in_executor(None, load_scan_state, _db)
            if state:
                _last_breadth = state["breadth"]
                breadth = _last_breadth
        # Still no breadth? Recompute from _last_signals in-memory.
        if breadth is None and _last_signals:
            try:
                breadth = compute_market_breadth(_last_signals)
                _last_breadth = breadth
            except Exception as exc:
                logger.error("compute_market_breadth on-demand failed: %s", exc)
        reply_flex(reply_token, "เลือก Stage", build_stage_picker_card(breadth))

    # ── 52W high / low lists (tappable from market card) ──
    elif cmd in ("52wh", "52w high", "52whigh", "new highs", "new_highs"):
        sigs = sorted(
            [s for s in _last_signals if s.high_52w > 0 and s.close >= s.high_52w * 0.99],
            key=lambda s: -(s.close / s.high_52w - 1),  # biggest break-above first
        )
        _reply_stock_list(reply_token, sigs, f"📈 ใกล้ 52W High ({len(sigs)} หุ้น)",
                          base_cmd="52wh",
                          subtitle="ราคาอยู่ภายใน 1% จาก 52W high")

    elif cmd in ("52wl", "52w low", "52wlow", "new lows", "new_lows"):
        sigs = sorted(
            [s for s in _last_signals if s.low_52w > 0 and s.close <= s.low_52w * 1.01],
            key=lambda s: (s.close / s.low_52w - 1),
        )
        _reply_stock_list(reply_token, sigs, f"📉 ใกล้ 52W Low ({len(sigs)} หุ้น)",
                          base_cmd="52wl",
                          subtitle="ราคาอยู่ภายใน 1% จาก 52W low")

    # ── Pattern overview ──
    elif cmd in ("patterns", "pattern", "รูปแบบ"):
        reply_flex(reply_token, "รูปแบบราคา", build_pattern_overview_card(_last_signals, _last_breadth))

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

        # Split the saved list into SET tickers vs global codes. SET signals
        # come from the in-memory cache first (fast), Firestore second
        # (re-hydrates after a cold start). Globals fetch live yfinance in
        # parallel — the snapshot endpoint can't serve them because we may
        # not have scanned globals recently, and cached snapshot data can
        # be minutes stale during heavy market hours.
        set_codes = [c for c in wl if not is_global_code(c)]
        global_codes = [c for c in wl if is_global_code(c)]

        cached = {s.symbol: s for s in _last_signals}
        wl_signals = [cached[sym] for sym in set_codes if sym in cached]
        uncached = [sym for sym in set_codes if sym not in cached]
        if uncached and FIRESTORE_AVAILABLE and _db:
            fs_sigs = await loop.run_in_executor(
                None, lambda: [load_signal_from_firestore(_db, s) for s in uncached]
            )
            wl_signals.extend(s for s in fs_sigs if s)

        global_assets = []
        if global_codes:
            from concurrent.futures import ThreadPoolExecutor

            def _fetch_all():
                with ThreadPoolExecutor(max_workers=4) as ex:
                    return [a for a in ex.map(fetch_global_asset, global_codes) if a]

            global_assets = await loop.run_in_executor(None, _fetch_all)

        if not wl_signals and not global_assets:
            reply_text(reply_token, "ไม่มีข้อมูลหุ้นใน Watchlist ขณะนี้")
            return
        card = build_watchlist_carousel(wl_signals, global_assets=global_assets)
        total = len(wl_signals) + len(global_assets)
        reply_flex(reply_token, f"📌 Watchlist ({total} หุ้น)", card)

    # ── Watchlist: add ──
    elif cmd.startswith("add ") or cmd.startswith("เพิ่ม "):
        parts = text.split(" ", 1)
        raw = parts[1].strip() if len(parts) > 1 else ""
        # Global code takes precedence for the same reason as single-stock
        # dispatch — type 'add BTC' and it should land in the watchlist as
        # "BTC" (global), not bounce because BTC isn't a SET ticker.
        if is_global_code(raw):
            symbol = raw.strip().upper()
        else:
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
        if is_global_code(raw):
            symbol = raw.strip().upper()
        else:
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

    elif cmd in ("attempt", "attempts", "breakout attempt", "breakout_attempt", "breakout attempts"):
        # Stocks that touched the 52-bar pivot intraday on ≥1.4× volume but
        # haven't confirmed on the close yet. Weaker signal than 'breakout'
        # but catches in-progress moves that the strict close-rule misses.
        signals = _get_signals_for(pattern="breakout_attempt")
        _reply_stock_list(reply_token, signals, "⚡ Breakout Attempts")

    elif cmd in ("ath", "all time high", "ath breakout"):
        signals = _get_signals_for(pattern="ath_breakout")
        _reply_stock_list(reply_token, signals, "🏆 ATH Breakout Stocks")

    elif cmd in ("vcp",):
        signals = _get_signals_for(pattern="vcp") + _get_signals_for(pattern="vcp_low_cheat")
        _reply_stock_list(reply_token, signals, "🔍 VCP Pattern Stocks")

    elif cmd in ("weakening", "weak", "stage2 weak", "stage2 weakening"):
        # Stage 2 stocks with close < SMA50 — uptrend structure intact but
        # near-term momentum has rolled over. Useful watch-list for "trim"
        # candidates before stage 3 transitions.
        signals = [s for s in _get_signals_for(stage=2)
                   if getattr(s, "stage_weakening", False)]
        _reply_stock_list(reply_token, signals, "⚠️ Stage 2 — Weakening")

    # ── Sub-stage filters (9-state finite state machine) ──
    # Same vocabulary as classify_sub_stage(); both short ('pullback')
    # and long ('stage2 pullback') forms accepted. Long form is what
    # tappable rows from the breadth card emit.
    elif cmd in ("base", "stage1 base", "prep", "stage1 prep",
                  "early", "stage2 early", "running", "stage2 running",
                  "pullback", "stage2 pullback",
                  "volatile", "stage3 volatile",
                  "dist", "distribution", "stage3 dist", "stage3 distribution",
                  "breakdown", "stage4 breakdown",
                  "downtrend", "stage4 downtrend"):
        SUB_STAGE_LABELS = {
            "STAGE_1_BASE":      "⚪ Stage 1 · Base",
            "STAGE_1_PREP":      "🟢 Stage 1 · Prep",
            "STAGE_2_EARLY":     "🟢 Stage 2 · Early",
            "STAGE_2_RUNNING":   "🟢 Stage 2 · Running",
            "STAGE_2_PULLBACK":  "🔵 Stage 2 · Pullback",
            "STAGE_3_VOLATILE":  "🟡 Stage 3 · Volatile",
            "STAGE_3_DIST_DIST": "🟠 Stage 3 · Distribution",
            "STAGE_4_BREAKDOWN": "🔴 Stage 4 · Breakdown",
            "STAGE_4_DOWNTREND": "🔴 Stage 4 · Downtrend",
        }
        TOKEN_TO_SUB = {
            "base": "STAGE_1_BASE", "stage1 base": "STAGE_1_BASE",
            "prep": "STAGE_1_PREP", "stage1 prep": "STAGE_1_PREP",
            "early": "STAGE_2_EARLY", "stage2 early": "STAGE_2_EARLY",
            "running": "STAGE_2_RUNNING", "stage2 running": "STAGE_2_RUNNING",
            "pullback": "STAGE_2_PULLBACK", "stage2 pullback": "STAGE_2_PULLBACK",
            "volatile": "STAGE_3_VOLATILE", "stage3 volatile": "STAGE_3_VOLATILE",
            "dist": "STAGE_3_DIST_DIST", "distribution": "STAGE_3_DIST_DIST",
            "stage3 dist": "STAGE_3_DIST_DIST", "stage3 distribution": "STAGE_3_DIST_DIST",
            "breakdown": "STAGE_4_BREAKDOWN", "stage4 breakdown": "STAGE_4_BREAKDOWN",
            "downtrend": "STAGE_4_DOWNTREND", "stage4 downtrend": "STAGE_4_DOWNTREND",
        }
        sub_stage_const = TOKEN_TO_SUB[cmd]
        signals = [s for s in _last_signals
                   if getattr(s, "sub_stage", "") == sub_stage_const]
        signals.sort(key=lambda s: s.strength_score, reverse=True)
        _reply_stock_list(reply_token, signals, SUB_STAGE_LABELS[sub_stage_const])

    elif cmd in ("pivot", "pivots", "pivot point", "pivot points"):
        # Stocks with a computed pivot price (PREP / EARLY / RUNNING /
        # PULLBACK sub-stages). Sorted by closeness to pivot — stocks
        # already at/above pivot rank first, then approaching from below.
        signals = [s for s in _last_signals
                   if getattr(s, "pivot_price", 0.0) > 0]

        def _pivot_distance(s):
            return (s.pivot_price - s.close) / s.pivot_price if s.pivot_price else 999.0

        signals.sort(key=_pivot_distance)
        _reply_stock_list(reply_token, signals, "🎯 Pivot Candidates")

    elif cmd in ("vcp low cheat", "vcp_low_cheat", "low cheat"):
        signals = _get_signals_for(pattern="vcp_low_cheat")
        _reply_stock_list(reply_token, signals, "🎯 VCP Low Cheat Stocks")

    elif cmd.startswith("stage2") or cmd.startswith("stage 2"):
        page = _parse_stage_page(cmd)
        all_sigs = _get_signals_for(stage=2)
        _reply_stock_list(reply_token, all_sigs, f"🟢 Stage 2 ({len(all_sigs)} stocks)", page=page, base_cmd="stage2")

    elif cmd.startswith("stage1") or cmd.startswith("stage 1"):
        page = _parse_stage_page(cmd)
        all_sigs = _get_signals_for(stage=1)
        _reply_stock_list(reply_token, all_sigs, f"⚪ Stage 1 ({len(all_sigs)} stocks)", page=page, base_cmd="stage1")

    elif cmd.startswith("stage3") or cmd.startswith("stage 3"):
        page = _parse_stage_page(cmd)
        all_sigs = _get_signals_for(stage=3)
        _reply_stock_list(reply_token, all_sigs, f"🟡 Stage 3 ({len(all_sigs)} stocks)", page=page, base_cmd="stage3")

    elif cmd.startswith("stage4") or cmd.startswith("stage 4"):
        page = _parse_stage_page(cmd)
        all_sigs = _get_signals_for(stage=4)
        _reply_stock_list(reply_token, all_sigs, f"🔴 Stage 4 ({len(all_sigs)} stocks)", page=page, base_cmd="stage4")

    elif cmd in ("consolidating", "consolidate", "coil"):
        signals = _get_signals_for(pattern="consolidating")
        _reply_stock_list(reply_token, signals, "⚙️ Consolidating Stocks")

    # ── Advancing / Declining / Flat (tappable from market card) ──
    elif cmd.startswith("advancing") or cmd.startswith("up ") or cmd == "up":
        page = _parse_stage_page(cmd)
        sigs = sorted([s for s in _last_signals if (s.change_pct or 0) > 0],
                      key=lambda s: s.change_pct, reverse=True)
        _reply_stock_list(reply_token, sigs, f"📈 Advancing ({len(sigs)} stocks)",
                          page=page, base_cmd="advancing", subtitle="Sorted by % Gain")

    elif cmd.startswith("declining") or cmd.startswith("down ") or cmd == "down":
        page = _parse_stage_page(cmd)
        sigs = sorted([s for s in _last_signals if (s.change_pct or 0) < 0],
                      key=lambda s: s.change_pct)
        _reply_stock_list(reply_token, sigs, f"📉 Declining ({len(sigs)} stocks)",
                          page=page, base_cmd="declining", subtitle="Sorted by % Drop")

    elif cmd.startswith("flat"):
        page = _parse_stage_page(cmd)
        sigs = sorted([s for s in _last_signals if (s.change_pct or 0) == 0],
                      key=lambda s: s.strength_score, reverse=True)
        _reply_stock_list(reply_token, sigs, f"➡️ Flat ({len(sigs)} stocks)",
                          page=page, base_cmd="flat")

    # ── Detail: deep insight with fundamentals ──
    elif cmd.startswith("detail "):
        raw = text[7:].strip()
        symbol = resolve_symbol(raw)
        if not symbol:
            reply_text(reply_token, f'ไม่พบหุ้น "{raw.upper()}"')
            return
        await _reply_detailed_stock(reply_token, symbol)

    # ── Help → Guide ──
    elif cmd in ("help", "ช่วย", "คำสั่ง", "?"):
        if _guide_carousel_cache is None:
            _guide_carousel_cache = build_guide_carousel()
        reply_flex(reply_token, "📖 คู่มือ Signalix", _guide_carousel_cache)

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

    # ── Single asset lookup ──
    else:
        # Global code takes precedence — "GLOBAL" is also a SET retail ticker,
        # but a user typing BTC/SPX/GOOG almost certainly wants the global
        # detail card, not a SET ticker search.
        if is_global_code(text):
            await _reply_global_single(reply_token, text.strip().upper())
            return
        symbol = resolve_symbol(text)
        if symbol:
            await _reply_single_stock(reply_token, symbol, user_id)
        else:
            reply_text(
                reply_token,
                f'ไม่พบหุ้น "{text.upper()}"\n\nเช็คชื่อ ticker ที่ถูกต้อง เช่น:\n• SCC (ไม่ใช่ SCG)\n• ADVANC (ไม่ใช่ AIS)\n\nพิมพ์ help เพื่อดูคำสั่งทั้งหมดครับ',
            )


def _parse_stage_page(cmd: str) -> int:
    """Extract page number from commands like 'stage4 p2' or 'stage 4 p3'. Returns 1 if absent."""
    if " p" in cmd:
        try:
            return int(cmd.split(" p")[-1])
        except ValueError:
            pass
    return 1


def _get_signals_for(pattern: Optional[str] = None, stage: Optional[int] = None) -> list[StockSignal]:
    return filter_signals(_last_signals, pattern=pattern, stage=stage)


# LINE carousel hard limit: 50KB (confirmed). 5 bubbles × 10 rows ≈ 42KB per page.
# Pagination: each page shows 50 stocks; "ดูเพิ่มเติม ▼" button sends "stage4 p2" etc.
_STAGE_PAGE_SIZE = 50


def _reply_stock_list(reply_token: str, signals: list[StockSignal], title: str,
                      text_only: bool = False, page: int = 1, base_cmd: str = "",
                      subtitle: str = "Sorted by Strength Score") -> None:
    if not signals:
        reply_text(reply_token, f"ไม่มีหุ้นใน {title} ขณะนี้")
        return
    if text_only:
        bubble = build_simple_tappable_list(signals, title)
        reply_flex(reply_token, title, bubble)
        return
    total = len(signals)
    start = (page - 1) * _STAGE_PAGE_SIZE
    chunk = signals[start:start + _STAGE_PAGE_SIZE]
    if not chunk:
        reply_text(reply_token, "ไม่มีข้อมูลเพิ่มเติมแล้วครับ")
        return
    has_more = total > start + _STAGE_PAGE_SIZE
    next_cmd = f"{base_cmd} p{page + 1}" if (has_more and base_cmd) else ""
    bubble = build_ranked_stock_list_bubble(chunk, title, next_cmd=next_cmd,
                                            rank_offset=start, subtitle=subtitle)
    reply_flex(reply_token, title, bubble)


async def _reply_index_filter(reply_token: str, index_name: str, rest: str) -> None:
    """Drill-down list inside an index. Two modes:
      • rest in {'members', 'list', 'all'} → all members sorted by score
      • rest in {'stage1', 'stage2', 'stage3', 'stage4'} or 'stage 2' etc.
        → members filtered to that stage
    Tap a row → single-stock detail card. Empty universe → friendly text.
    """
    from data import get_index_members
    members = get_index_members(index_name)
    if not members:
        reply_text(reply_token,
                   f"{index_name}: ยังไม่มีรายชื่อสมาชิก (กำลังเตรียมข้อมูล)")
        return
    if not _last_signals:
        reply_text(reply_token, "ยังไม่มีข้อมูลการสแกน กรุณารอการสแกนครั้งถัดไป")
        return

    # Constituent universe (signals filtered to index members).
    constituents = [s for s in _last_signals if s.symbol in members]
    if not constituents:
        reply_text(reply_token, f"{index_name}: ไม่มีข้อมูล constituent ขณะนี้")
        return

    # Normalise rest: 'stage 2' / 'stage2' / 's2' all map to stage=2.
    rest_norm = rest.lower().replace(" ", "")
    stage_filter: Optional[int] = None
    for n in (1, 2, 3, 4):
        if rest_norm in (f"stage{n}", f"s{n}"):
            stage_filter = n
            break
    members_only = rest_norm in ("members", "list", "all", "")

    # Sub-stage tokens ('pullback', 'early', 'breakdown', etc.) → constituent
    # filter. Same vocabulary as the global sub-stage filter commands.
    SUB_STAGE_TOKEN_MAP = {
        "base": "STAGE_1_BASE", "prep": "STAGE_1_PREP",
        "early": "STAGE_2_EARLY", "running": "STAGE_2_RUNNING",
        "pullback": "STAGE_2_PULLBACK",
        "volatile": "STAGE_3_VOLATILE",
        "dist": "STAGE_3_DIST_DIST", "distribution": "STAGE_3_DIST_DIST",
        "breakdown": "STAGE_4_BREAKDOWN", "downtrend": "STAGE_4_DOWNTREND",
    }
    sub_stage_const = SUB_STAGE_TOKEN_MAP.get(rest_norm)

    if stage_filter is not None:
        signals = [s for s in constituents if s.stage == stage_filter]
        title = f"📊 {index_name} · Stage {stage_filter} ({len(signals)})"
    elif sub_stage_const is not None:
        signals = [s for s in constituents
                   if getattr(s, "sub_stage", "") == sub_stage_const]
        # Friendly label: convert STAGE_2_PULLBACK → "Stage 2 · Pullback"
        parts = sub_stage_const.split("_")
        nice = f"Stage {parts[1]} · {parts[2].title() if len(parts) > 2 else ''}"
        if len(parts) > 3:  # e.g. STAGE_3_DIST_DIST
            nice = f"Stage {parts[1]} · Distribution"
        title = f"📊 {index_name} · {nice} ({len(signals)})"
    elif members_only:
        signals = constituents
        title = f"📊 {index_name} · Members ({len(signals)})"
    else:
        reply_text(reply_token,
                   f"คำสั่งไม่ถูกต้อง: '{index_name.lower()} {rest}'\n"
                   f"ลอง: '{index_name.lower()} members' / "
                   f"'{index_name.lower()} stage2' / "
                   f"'{index_name.lower()} pullback'")
        return

    if not signals:
        reply_text(reply_token, f"{title}\nไม่มีหุ้นในกลุ่มนี้")
        return

    _reply_stock_list(reply_token, signals, title,
                      base_cmd=f"{index_name.lower()} {rest_norm}")


async def _reply_index_breadth(reply_token: str, index_name: str) -> None:
    """Per-sub-index breadth card. Filters _last_signals by member set and
    runs compute_index_breadth, then renders with build_index_breadth_card.

    Index price + change% are read from _last_indexes (yfinance returns 1
    bar per scan for sub-indexes — enough for today's price). Members come
    from data._index_members (Firestore-backed, with hardcoded fallback).
    """
    from data import get_index_members, INDEX_SYMBOLS
    from analyzer import compute_index_breadth

    members = get_index_members(index_name)
    if not members:
        reply_text(reply_token,
                   f"{index_name}: ยังไม่มีรายชื่อสมาชิก (กำลังเตรียมข้อมูล)\nลอง 'set50' หรือ 'set100' ก่อน")
        return

    if not _last_signals:
        reply_text(reply_token, "ยังไม่มีข้อมูลการสแกน กรุณารอการสแกนครั้งถัดไป")
        return

    # Index price/change: yfinance gives us today's bar for ^SET50.BK etc.
    idx_data = (_last_indexes or {}).get(index_name, {})
    idx_close = float(idx_data.get("close", 0) or 0)
    idx_chg = float(idx_data.get("change_pct", 0) or 0)

    breadth = compute_index_breadth(_last_signals, members,
                                     index_close=idx_close,
                                     index_change_pct=idx_chg)

    # Top 3 movers up + 3 down within the index members
    constituents = [s for s in _last_signals if s.symbol in members]
    movers_up = sorted([s for s in constituents if s.change_pct > 0],
                      key=lambda s: -s.change_pct)[:3]
    movers_down = sorted([s for s in constituents if s.change_pct < 0],
                        key=lambda s: s.change_pct)[:3]

    card = build_index_breadth_card(
        index_name=index_name,
        breadth=breadth,
        movers_up=movers_up,
        movers_down=movers_down,
        member_count=len(members),
    )
    reply_flex(reply_token, f"📊 {index_name} Breadth", card)


async def _reply_global_single(reply_token: str, code: str) -> None:
    """Detail card for a non-SET asset (index / ETF / US stock / crypto).

    Fires fetch_global_asset in a thread (yfinance blocks), then renders
    via build_global_single_card. On fetch failure we fall back to a text
    message rather than an empty card so the user knows it was an upstream
    issue, not a silent drop.
    """
    loop = asyncio.get_running_loop()
    asset = await loop.run_in_executor(None, fetch_global_asset, code)
    if asset is None:
        reply_text(reply_token,
                   f'ดึงข้อมูล "{code}" ไม่สำเร็จ ลองใหม่อีกครั้ง\n(หรือพิมพ์ "global" เพื่อดูภาพรวม)')
        return
    reply_flex(reply_token, f"{code} Detail", build_global_single_card(asset))


async def _reply_single_stock(reply_token: str, symbol: str, user_id: str = "") -> None:
    loop = asyncio.get_running_loop()
    signal = next((s for s in _last_signals if s.symbol == symbol), None)
    if signal is None and FIRESTORE_AVAILABLE and _db:
        signal = await loop.run_in_executor(None, load_signal_from_firestore, _db, symbol)
    if signal is None:
        reply_text(reply_token, f'ไม่พบหุ้น "{symbol}" ในระบบ\nตรวจสอบ ticker ให้ถูกต้อง เช่น ADVANC, PTT, KBANK')
        return

    reply_flex(reply_token, f"วิเคราะห์ {symbol}", build_single_stock_card(signal))
    # Gamification — fire-and-forget, never block reply
    if user_id and FIRESTORE_AVAILABLE and _db:
        s = signal
        if s.stage == 2 and s.pattern in ("breakout", "ath_breakout", "vcp"):
            loop.run_in_executor(None, update_user_score, _db, user_id, 1, "viewed_s2_breakout", symbol)
        elif s.stage == 4:
            # Read stage4 view count non-blocking in background; penalise if needed
            def _stage4_gamification():
                user_data = (_db.collection("users").document(user_id).get().to_dict() or {})
                if user_data.get("stage4_views_this_week", 0) >= 2:
                    update_user_score(_db, user_id, -1, "repeated_stage4", symbol)
                increment_stage4_views(_db, user_id)
            loop.run_in_executor(None, _stage4_gamification)


async def _reply_detailed_stock(reply_token: str, symbol: str) -> None:
    """Serve deep insight card (technical + fundamentals) for a single stock."""
    loop = asyncio.get_running_loop()
    signal = next((s for s in _last_signals if s.symbol == symbol), None)
    if signal is None and FIRESTORE_AVAILABLE and _db:
        signal = await loop.run_in_executor(None, load_signal_from_firestore, _db, symbol)
    if signal is None:
        reply_text(reply_token, f'ไม่พบหุ้น "{symbol}" ในระบบ')
        return
    fund = await loop.run_in_executor(
        None, get_fundamentals, symbol, _db if FIRESTORE_AVAILABLE else None
    )
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
