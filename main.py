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
    build_index_carousel,
    build_market_breadth_card,
    build_pattern_detail_card,
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

        # ── Signals: BQ first (guaranteed durable), Firestore fallback ──────────
        if BQ_AVAILABLE:
            bq_signals = await loop.run_in_executor(None, load_latest_signals_from_bq)
            if bq_signals:
                _last_signals = bq_signals
                logger.info("Signals loaded from BQ: %d stocks", len(bq_signals))

        if not _last_signals and FIRESTORE_AVAILABLE and _db:
            fs_signals = await loop.run_in_executor(None, load_signals_from_firestore, _db)
            if fs_signals:
                _last_signals = fs_signals
                logger.info("Signals loaded from Firestore (BQ fallback): %d stocks", len(fs_signals))

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
        if BQ_AVAILABLE:
            bq = await loop.run_in_executor(None, load_latest_signals_from_bq)
            if bq:
                _last_signals = bq
        if not _last_signals and FIRESTORE_AVAILABLE and _db:
            fs = await loop.run_in_executor(None, load_signals_from_firestore, _db)
            if fs:
                _last_signals = fs
    return {
        "status": "ok",
        "time": datetime.now(BANGKOK_TZ).isoformat(),
        "firestore": FIRESTORE_AVAILABLE,
        "bigquery": _bq_avail,
        "cached_stocks": len(_last_signals),
        "last_scan_time": _last_scan_time.isoformat() if _last_scan_time else None,
    }


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
    """Minimal StockSignal view for diagnostic endpoints."""
    return {
        "symbol": signal.symbol,
        "close": signal.close,
        "change_pct": signal.change_pct,
        "high_52w": signal.high_52w,
        "pct_from_52w_high": signal.pct_from_52w_high,
        "stage": signal.stage,
        "pattern": signal.pattern,
        "strength_score": signal.strength_score,
        "scanned_at": signal.scanned_at,
        "data_date": getattr(signal, "data_date", ""),
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
        if BQ_AVAILABLE:
            bq_sigs = await loop.run_in_executor(None, load_latest_signals_from_bq)
            if bq_sigs:
                _last_signals = bq_sigs
        if not _last_signals and FIRESTORE_AVAILABLE and _db:
            fs_sigs = await loop.run_in_executor(None, load_signals_from_firestore, _db)
            if fs_sigs:
                _last_signals = fs_sigs

    c = cmd.lower().strip()

    def summary(sigs, title):
        return {
            "kind": "list",
            "title": title,
            "count": len(sigs),
            "first_5": [
                {"symbol": s.symbol, "close": s.close, "change_pct": s.change_pct,
                 "stage": s.stage, "pattern": s.pattern, "strength": s.strength_score}
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
        if BQ_AVAILABLE:
            bq_sigs = await loop.run_in_executor(None, load_latest_signals_from_bq)
            if bq_sigs:
                _last_signals = bq_sigs
        if not _last_signals and FIRESTORE_AVAILABLE and _db:
            fs_sigs = await loop.run_in_executor(None, load_signals_from_firestore, _db)
            if fs_sigs:
                _last_signals = fs_sigs

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
        if BQ_AVAILABLE:
            bq_sigs = await loop.run_in_executor(None, load_latest_signals_from_bq)
            if bq_sigs:
                _last_signals = bq_sigs
                logger.info("_handle_text_query: lazy-reloaded %d signals from BQ", len(bq_sigs))
        if not _last_signals and FIRESTORE_AVAILABLE and _db:
            fs_sigs = await loop.run_in_executor(None, load_signals_from_firestore, _db)
            if fs_sigs:
                _last_signals = fs_sigs
                logger.info("_handle_text_query: lazy-reloaded %d signals from Firestore", len(fs_sigs))

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
        cached = {s.symbol: s for s in _last_signals}
        wl_signals = [cached[sym] for sym in wl if sym in cached]
        uncached = [sym for sym in wl if sym not in cached]
        if uncached and FIRESTORE_AVAILABLE and _db:
            fs_sigs = await loop.run_in_executor(
                None, lambda: [load_signal_from_firestore(_db, s) for s in uncached]
            )
            wl_signals.extend(s for s in fs_sigs if s)
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
