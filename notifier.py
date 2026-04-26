"""
notifier.py — LINE Messaging API integration + Flex Message card builders.

Card types:
  - Market Breadth summary bubble
  - Stock list carousel (up to 10 stocks per message)
  - Single stock detail bubble
  - Welcome card
  - Help menu card
"""

import logging
from datetime import datetime
from typing import Optional
from urllib.parse import quote
import pytz

_BANGKOK_TZ = pytz.timezone("Asia/Bangkok")

from linebot.v3 import WebhookHandler
from linebot.v3.messaging import (
    ApiClient,
    Configuration,
    MessagingApi,
    BroadcastRequest,
    MulticastRequest,
    ReplyMessageRequest,
    TextMessage,
    FlexMessage,
    FlexContainer,
)
from linebot.v3.messaging.models import FlexBubble  # noqa: F401 (used via dict below)

from analyzer import MarketBreadth, SectorSummary, StockSignal
from config import get_settings
from data import INDEX_TV_URLS

logger = logging.getLogger(__name__)

# ─── LINE API singleton ───────────────────────────────────────────────────────

_messaging_api: Optional[MessagingApi] = None
_api_client: Optional[ApiClient] = None
_webhook_handler: Optional[WebhookHandler] = None


def init_notifier(token: str) -> None:
    """Initialize the LINE API singleton. Call once at startup."""
    global _messaging_api, _api_client
    if not token:
        return
    config = Configuration(access_token=token)
    _api_client = ApiClient(configuration=config)
    _messaging_api = MessagingApi(_api_client)


# ─── Pattern display config ───────────────────────────────────────────────────

PATTERN_LABEL = {
    "breakout": "Breakout",
    "ath_breakout": "ATH Breakout",
    "breakout_attempt": "Breakout Attempt",
    "vcp": "VCP",
    "vcp_low_cheat": "VCP Low Cheat",
    "consolidating": "Consolidating",
    "going_down": "Going Down",
}

PATTERN_COLOR = {
    "breakout": "#27AE60",
    "ath_breakout": "#F39C12",
    "breakout_attempt": "#16A085",  # teal — weaker than breakout green, still positive
    "vcp": "#2980B9",
    "vcp_low_cheat": "#1ABC9C",
    "consolidating": "#95A5A6",
    "going_down": "#E74C3C",
}

STAGE_COLOR = {1: "#95A5A6", 2: "#27AE60", 3: "#E67E22", 4: "#E74C3C"}
STAGE_LABEL = {1: "Stage 1 – Basing", 2: "Stage 2 – Uptrend", 3: "Stage 3 – Topping", 4: "Stage 4 – Downtrend"}

# ─── Sub-stage taxonomy (9-state finite state machine) ─────────────────────────
# Source-of-truth labels/colors/recommendations for the new sub_stage field on
# StockSignal. Cards display SUB_STAGE_LABEL[sub_stage] as the primary
# classification (replaces the legacy STAGE_LABEL + weakening suffix combo)
# when the signal has a sub_stage; falls back to STAGE_LABEL otherwise.
SUB_STAGE_LABEL = {
    "STAGE_1_BASE":         "Stage 1 · Base (frozen)",
    "STAGE_1_PREP":         "Stage 1 · Prep (loading)",
    # New Stage 2 sub-stages (2-layer refactor)
    "STAGE_2_IGNITION":     "Stage 2 · Ignition (kickoff)",
    "STAGE_2_OVEREXTENDED": "Stage 2 · Overextended (warning)",
    "STAGE_2_CONTRACTION":  "Stage 2 · Contraction (base)",
    "STAGE_2_PIVOT_READY":  "Stage 2 · Pivot Ready ✨",
    "STAGE_2_MARKUP":       "Stage 2 · Markup (running)",
    # Legacy Stage 2 (kept for backward compat with old Firestore docs)
    "STAGE_2_EARLY":        "Stage 2 · Early (fresh breakout)",
    "STAGE_2_RUNNING":      "Stage 2 · Running (trending)",
    "STAGE_2_PULLBACK":     "Stage 2 · Pullback (entry setup)",
    "STAGE_3_VOLATILE":     "Stage 3 · Volatile (distribution)",
    "STAGE_3_DIST_DIST":    "Stage 3 · Distribution (defend)",
    "STAGE_4_BREAKDOWN":    "Stage 4 · Breakdown (cut loss)",
    "STAGE_4_DOWNTREND":    "Stage 4 · Downtrend (avoid)",
}
SUB_STAGE_COLOR = {
    "STAGE_1_BASE":         "#7F8C8D",
    "STAGE_1_PREP":         "#1ABC9C",
    "STAGE_2_IGNITION":     "#27AE60",  # bright green — fresh momentum
    "STAGE_2_OVEREXTENDED": "#D35400",  # orange-red — warning
    "STAGE_2_CONTRACTION":  "#2980B9",  # blue — base building
    "STAGE_2_PIVOT_READY":  "#F1C40F",  # gold — actionable
    "STAGE_2_MARKUP":       "#16A085",  # teal-green — riding
    # Legacy
    "STAGE_2_EARLY":        "#27AE60",
    "STAGE_2_RUNNING":      "#16A085",
    "STAGE_2_PULLBACK":     "#2980B9",
    "STAGE_3_VOLATILE":     "#F39C12",
    "STAGE_3_DIST_DIST":    "#E67E22",
    "STAGE_4_BREAKDOWN":    "#C0392B",
    "STAGE_4_DOWNTREND":    "#922B21",
}
SUB_STAGE_ACTION = {
    "STAGE_1_BASE":         "Ignore — no setup yet",
    "STAGE_1_PREP":         "Watchlist — pre-Stage-2 watch",
    # New Stage 2 — prescriptive recommendations per spec
    "STAGE_2_IGNITION":     "🚀 TRADABLE — fresh momentum breakout",
    "STAGE_2_OVEREXTENDED": "⚠ WARNING — no new buys; tighten stops",
    "STAGE_2_CONTRACTION":  "👀 WATCH — base building, wait for tightness",
    "STAGE_2_PIVOT_READY":  "🎯 ACTIONABLE — pivot trigger active",
    "STAGE_2_MARKUP":       "✅ HOLD — let profits run; trail stop",
    # Legacy
    "STAGE_2_EARLY":        "Alert / Trade — focus on breakout entry",
    "STAGE_2_RUNNING":      "Hold / Trail Stop — don't add",
    "STAGE_2_PULLBACK":     "Setup Entry — pivot point coming",
    "STAGE_3_VOLATILE":     "Take Profit / Tighten Stop",
    "STAGE_3_DIST_DIST":    "Defend — no new buys",
    "STAGE_4_BREAKDOWN":    "Cut Loss — exit",
    "STAGE_4_DOWNTREND":    "Delete — remove from watch",
}


def _resolve_stage_label(signal) -> tuple[str, str]:
    """Return (label, color) for displaying the signal's primary classification.
    Prefers the new sub_stage when set; falls back to STAGE_LABEL +
    weakening suffix for old Firestore docs that pre-date sub_stage."""
    sub = getattr(signal, "sub_stage", "") or ""
    if sub and sub in SUB_STAGE_LABEL:
        return SUB_STAGE_LABEL[sub], SUB_STAGE_COLOR.get(sub, "#7F8C8D")
    label = STAGE_LABEL.get(signal.stage, f"Stage {signal.stage}")
    if getattr(signal, "stage_weakening", False):
        label = f"{label} ⚠ Weakening"
    return label, STAGE_COLOR.get(signal.stage, "#7F8C8D")

# Direct HTTPS PNG/JPEG URLs used as hero images in guide cards.
# LINE fetches these directly — must be publicly accessible.
# Set to empty string to skip the hero for that pattern.
PATTERN_IMAGES: dict[str, str] = {
    "stage_cycle": "https://upload.wikimedia.org/wikipedia/commons/thumb/6/69/Minervini_Stage_Analysis.png/640px-Minervini_Stage_Analysis.png",
    "breakout": "https://upload.wikimedia.org/wikipedia/commons/thumb/a/a7/Breakout_technical_analysis.png/640px-Breakout_technical_analysis.png",
    "ath_breakout": "",
    "vcp": "https://upload.wikimedia.org/wikipedia/commons/thumb/5/5b/Volatility_contraction_pattern.png/640px-Volatility_contraction_pattern.png",
    "vcp_low_cheat": "",
    "consolidating": "",
}


# ─── LINE client helpers ───────────────────────────────────────────────────────

def _get_api() -> MessagingApi:
    if _messaging_api is not None:
        return _messaging_api
    # Fallback: ad-hoc client for dev / before init_notifier is called
    settings = get_settings()
    config = Configuration(access_token=settings.line_channel_access_token)
    return MessagingApi(ApiClient(configuration=config))


def get_webhook_handler() -> WebhookHandler:
    global _webhook_handler
    if _webhook_handler is None:
        _webhook_handler = WebhookHandler(get_settings().line_channel_secret)
    return _webhook_handler


# ─── Flex Message builders ────────────────────────────────────────────────────

def _fmt_scan_time(scanned_at: str) -> str:
    """Format ISO scanned_at string to Thai-friendly display."""
    if not scanned_at:
        return ""
    try:
        dt = datetime.fromisoformat(scanned_at)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=_BANGKOK_TZ)
        else:
            dt = dt.astimezone(_BANGKOK_TZ)
        return dt.strftime("สแกนล่าสุด %H:%M น. %d/%m/%y")
    except Exception:
        return ""


def _pct_color(pct: float) -> str:
    if pct > 0:
        return "#27AE60"
    if pct < 0:
        return "#E74C3C"
    return "#7F8C8D"


# ─── Captain Signal recommendation helpers ───────────────────────────────────

def _captain_stock_advice(signal: StockSignal) -> str:
    """Return Captain Signal's Thai-language advice for a given stock signal."""
    s, p = signal.stage, signal.pattern
    sl = f"฿{signal.stop_loss:,.2f}" if getattr(signal, "stop_loss", 0) > 0 else ""
    sl_text = f" เซ็ต Stop Loss ไว้ที่ {sl}" if sl else ""
    if s == 2 and p in ("breakout", "ath_breakout"):
        return f"กัปตันชอบตัวนี้ครับ! Breakout ใน Stage 2 — วางแผนกระสุนให้ดี{sl_text} แล้วลุยตามวินัยได้เลย!"
    if s == 2 and p == "vcp":
        return "VCP ใน Stage 2 กัปตันจับตามองตัวนี้ครับ รอ Breakout จุดเข้าที่ดีกำลังจะมา"
    if s == 2:
        return "Stage 2 อยู่ครับ กัปตันให้ watch ไว้ก่อน รอสัญญาณ Breakout ที่ชัดขึ้น"
    if s == 1:
        return "ยัง Stage 1 สะสมตัวอยู่เลยครับ ไม่ต้องรีบซื้อก็ได้นะครับ รอให้ขึ้น Stage 2 ก่อน"
    if s == 3:
        return "Stage 3 Topping แล้วครับ กัปตันระวังไว้ ถ้ามีอยู่ระวังขายทำกำไรฝั่ง"
    if s == 4:
        return "ตัวนี้กัปตันขอสั่งห้ามครับ! Stage 4 กราฟแบบนี้คือเขตอันตราย อย่าเอาวินัยไปเสี่ยงกับการรับมีดเลยครับ"
    return ""


def _captain_market_advice(breadth: MarketBreadth) -> str:
    """Return Captain Signal's Thai-language market condition advice."""
    s2 = breadth.stage2_pct
    adv, dec = breadth.advancing, breadth.declining
    if s2 >= 35:
        return "กัปตันตรวจพบสัญญาณบวกในตลาดครับ Stage 2 กำลังเริ่มหนาตาขึ้น วางแผนกระสุนให้ดี แล้วลุยตามวินัยได้เลย!"
    if s2 >= 25:
        return "ตลาดยังพอไปได้ครับ มีโอกาสเลือกสรรตัว Stage 2 ที่แข็งแกร่งได้"
    if dec > adv * 1.5:
        return "วันนี้คลื่นลมแรงครับ กัปตันแนะนำให้เก็บกระสุนไว้ก่อน การไม่เทรดก็คือวินัยอย่างหนึ่งเหมือนกัน พักผ่อนให้เต็มที่ครับ"
    return "ตลาดยังต้องระวังครับ กัปตันแนะนำเลือกตัวที่ดีจริงๆ เท่านั้น"


def _captain_advice_box(text: str) -> dict:
    """Shared Captain advice box component for cards."""
    return {
        "type": "box", "layout": "horizontal",
        "backgroundColor": "#0D1A0D", "cornerRadius": "8px",
        "paddingAll": "10px", "margin": "sm",
        "contents": [
            {"type": "text", "text": "⚓", "size": "lg", "flex": 0},
            {"type": "text", "text": text, "size": "xs", "color": "#CCFFCC",
             "wrap": True, "flex": 1, "margin": "sm"},
        ],
    }


def _cmd_row(cmd: str, desc: str) -> dict:
    """Compact tappable command row for guide carousel (replaces button widget)."""
    return {
        "type": "box", "layout": "horizontal",
        "action": {"type": "message", "label": cmd, "text": cmd},
        "paddingTop": "5px", "paddingBottom": "5px",
        "contents": [
            {"type": "text", "text": cmd, "size": "sm", "weight": "bold", "color": "#2980B9", "flex": 2},
            {"type": "text", "text": desc, "size": "sm", "color": "#CCCCCC", "flex": 3},
        ],
    }


def build_index_breadth_card(
    index_name: str,
    breadth: MarketBreadth,
    movers_up: list[StockSignal] | None = None,
    movers_down: list[StockSignal] | None = None,
    member_count: int = 0,
    constituents: list[StockSignal] | None = None,
) -> dict:
    """Per-sub-index breadth card (SET50 / SET100 / MAI).

    Smaller surface than the full SET breadth card — focuses on the single
    question 'how is this index doing today?'. Header carries index price
    + change% (when available, only SET has yfinance history but sub-index
    last close + change% is fetched separately into breadth fields). Body
    shows: member count, adv/dec/flat tally, stage distribution bar,
    above/below MA200 pct, top-3 up + top-3 down movers within the index.

    Distinct from build_market_breadth_card (which has SET-specific RSI/
    MACD/sector strips); this one is intentionally trimmed because sub-
    indexes don't have those analyses available from yfinance's 1-bar
    response.
    """
    INDEX_COLORS = {"SET50": "#0D47A1", "SET100": "#1565C0", "MAI": "#4A148C", "sSET": "#006064", "SETESG": "#2E7D32"}
    index_color = INDEX_COLORS.get(index_name, "#1A237E")

    idx_close = getattr(breadth, "set_index_close", 0.0)
    idx_chg = getattr(breadth, "set_index_change_pct", 0.0)
    chg_color = _pct_color(idx_chg)
    chg_sign = "+" if idx_chg > 0 else ""
    if idx_chg > 0:
        header_bg = "#1B5E20"
    elif idx_chg < 0:
        header_bg = "#B71C1C"
    else:
        header_bg = index_color

    total = breadth.total_stocks
    adv = breadth.advancing
    dec = breadth.declining
    flat = breadth.unchanged
    s1, s2, s3, s4 = (breadth.stage1_count, breadth.stage2_count,
                       breadth.stage3_count, breadth.stage4_count)
    above_pct = getattr(breadth, "above_ma200_pct", 0.0)

    movers_up = (movers_up or [])[:3]
    movers_down = (movers_down or [])[:3]

    def _row(label: str, value: str, value_color: str = "#FFFFFF",
             tap_cmd: Optional[str] = None) -> dict:
        box: dict = {
            "type": "box", "layout": "horizontal",
            "paddingTop": "4px", "paddingBottom": "4px",
            "contents": [
                {"type": "text", "text": label, "size": "xs", "color": "#9E9E9E", "flex": 4},
                {"type": "text", "text": value, "size": "sm", "color": value_color,
                 "weight": "bold", "flex": 5, "align": "end"},
            ],
        }
        if tap_cmd:
            box["action"] = {"type": "message", "label": label[:20], "text": tap_cmd}
        return box

    def _mover_row(s: StockSignal, color: str) -> dict:
        sign = "+" if s.change_pct > 0 else ""
        return {
            "type": "box", "layout": "horizontal",
            "action": {"type": "message", "label": s.symbol[:20], "text": s.symbol},
            "paddingTop": "3px", "paddingBottom": "3px",
            "contents": [
                {"type": "text", "text": s.symbol, "size": "xs", "weight": "bold",
                 "color": "#FFFFFF", "flex": 3},
                {"type": "text", "text": f"{s.close:,.2f}", "size": "xs",
                 "color": "#9E9E9E", "flex": 3, "align": "end"},
                {"type": "text", "text": f"{sign}{s.change_pct:.2f}%",
                 "size": "xs", "color": color, "flex": 2, "align": "end", "weight": "bold"},
            ],
        }

    body_contents: list = [
        # Hero price row (when index has live price)
        *([
            {"type": "box", "layout": "baseline", "paddingBottom": "8px", "contents": [
                {"type": "text", "text": f"{idx_close:,.2f}", "weight": "bold",
                 "size": "xxl", "color": "#FFFFFF", "flex": 5},
                {"type": "text", "text": f"{chg_sign}{idx_chg:.2f}%", "weight": "bold",
                 "size": "lg", "color": chg_color, "align": "end", "flex": 3},
            ]},
            {"type": "separator", "color": "#333333"},
        ] if idx_close > 0 else []),

        {"type": "text", "text": "Constituent breadth", "size": "xs",
         "weight": "bold", "color": "#FFD54F", "margin": "sm"},
        _row("Members scanned", f"{total} / {member_count}" if member_count else f"{total}"),
        _row("Advancing", f"{adv}", value_color="#27AE60"),
        _row("Declining", f"{dec}", value_color="#E74C3C"),
        _row("Unchanged", f"{flat}"),
        _row("Above MA200", f"{above_pct:.1f}%",
             value_color="#27AE60" if above_pct >= 50 else "#E74C3C"),

        {"type": "separator", "color": "#333333", "margin": "md"},
        {"type": "text", "text": "Stage distribution · tap to filter",
         "size": "xs", "weight": "bold", "color": "#FFD54F", "margin": "sm"},
        # Each stage row taps to e.g. 'set50 stage2' → constituent list filtered
        # to that stage. Mirrors the existing 'top mover → detail card' gesture.
        _row("Stage 1 (Basing)", f"{s1}", value_color="#95A5A6",
             tap_cmd=f"{index_name.lower()} stage1"),
        _row("Stage 2 (Uptrend)", f"{s2}", value_color="#27AE60",
             tap_cmd=f"{index_name.lower()} stage2"),
        _row("Stage 3 (Topping)", f"{s3}", value_color="#E67E22",
             tap_cmd=f"{index_name.lower()} stage3"),
        _row("Stage 4 (Downtrend)", f"{s4}", value_color="#E74C3C",
             tap_cmd=f"{index_name.lower()} stage4"),
        # Members-list shortcut at the bottom of the stage block
        _row("All members", "→",
             value_color="#1ABC9C",
             tap_cmd=f"{index_name.lower()} members"),
    ]

    # ── NEW: Top-5 actionable Stage 2 sub-stage rows ──────────────────
    # Surfaces the user's "what to act on within this index today"
    # cohorts: PIVOT_READY (most actionable) → IGNITION → MARKUP →
    # CONTRACTION → OVEREXTENDED. Each row tappable to drill into the
    # scoped sub-stage list (e.g. 'set100 ready'). Hidden when
    # constituents are not provided (caller didn't compute them — old
    # callers untouched).
    if constituents is not None:
        from collections import Counter as _Ctr
        sub_counts = _Ctr(getattr(s, "sub_stage", "") or "" for s in constituents)
        prefix = f"{index_name.lower()} "
        body_contents.extend([
            {"type": "separator", "color": "#333333", "margin": "md"},
            {"type": "text", "text": "Sub-stage · Stage 2 actionable",
             "size": "xs", "weight": "bold", "color": "#FFD54F", "margin": "sm"},
            _row("🎯 Pivot Ready ✨",
                 f"{sub_counts.get('STAGE_2_PIVOT_READY', 0)}",
                 value_color=SUB_STAGE_COLOR.get("STAGE_2_PIVOT_READY", "#F1C40F"),
                 tap_cmd=f"{prefix}ready"),
            _row("🚀 Ignition",
                 f"{sub_counts.get('STAGE_2_IGNITION', 0)}",
                 value_color=SUB_STAGE_COLOR.get("STAGE_2_IGNITION", "#27AE60"),
                 tap_cmd=f"{prefix}ignition"),
            _row("✅ Markup",
                 f"{sub_counts.get('STAGE_2_MARKUP', 0)}",
                 value_color=SUB_STAGE_COLOR.get("STAGE_2_MARKUP", "#16A085"),
                 tap_cmd=f"{prefix}markup"),
            _row("👀 Contraction",
                 f"{sub_counts.get('STAGE_2_CONTRACTION', 0)}",
                 value_color=SUB_STAGE_COLOR.get("STAGE_2_CONTRACTION", "#2980B9"),
                 tap_cmd=f"{prefix}contraction"),
            _row("⚠ Overextended",
                 f"{sub_counts.get('STAGE_2_OVEREXTENDED', 0)}",
                 value_color=SUB_STAGE_COLOR.get("STAGE_2_OVEREXTENDED", "#D35400"),
                 tap_cmd=f"{prefix}overextended"),
            # Quick-access drill-downs to the dashboard / picker / pivot
            _row("📊 Stages dashboard (11 rows)", "→",
                 value_color="#1ABC9C",
                 tap_cmd=f"{prefix}stages"),
            _row("🎯 Pivot candidates", "→",
                 value_color="#F1C40F",
                 tap_cmd=f"{prefix}pivot"),
        ])

    if movers_up:
        body_contents.append({"type": "separator", "color": "#333333", "margin": "md"})
        body_contents.append({"type": "text", "text": "Top movers up",
                              "size": "xs", "weight": "bold", "color": "#27AE60", "margin": "sm"})
        for s in movers_up:
            body_contents.append(_mover_row(s, "#27AE60"))
    if movers_down:
        body_contents.append({"type": "separator", "color": "#333333", "margin": "md"})
        body_contents.append({"type": "text", "text": "Top movers down",
                              "size": "xs", "weight": "bold", "color": "#E74C3C", "margin": "sm"})
        for s in movers_down:
            body_contents.append(_mover_row(s, "#E74C3C"))

    return {
        "type": "bubble", "size": "mega",
        "header": {
            "type": "box", "layout": "vertical",
            "backgroundColor": header_bg, "paddingAll": "14px",
            "contents": [
                {"type": "text", "text": f"📊 {index_name}", "weight": "bold",
                 "size": "xl", "color": "#FFFFFF"},
                {"type": "text", "text": "Sub-index breadth · tap mover for detail",
                 "size": "xxs", "color": "#E3F2FD"},
            ],
        },
        "body": {
            "type": "box", "layout": "vertical",
            "backgroundColor": "#1A1A1A",
            "paddingAll": "14px",
            "spacing": "none",
            "contents": body_contents,
        },
    }


def build_market_breadth_card(breadth: MarketBreadth, sector_trends: list | None = None, indexes: dict | None = None) -> dict:
    """Build a Flex Bubble card for market breadth summary with SET index as hero header."""
    set_close = getattr(breadth, "set_index_close", 0.0)
    set_chg = getattr(breadth, "set_index_change_pct", 0.0)
    # Fallback: if breadth didn't capture SET, use the value from the index analysis
    # pass so the hero number isn't blank just because all_data["SET"] was missing.
    if (not set_close) and indexes:
        _idx_set = indexes.get("SET") or {}
        set_close = float(_idx_set.get("close") or 0.0)
        if not set_chg:
            set_chg = float(_idx_set.get("change_pct") or 0.0)
    chg_color = _pct_color(set_chg)
    chg_sign = "+" if set_chg > 0 else ""
    above_pct = getattr(breadth, "above_ma200_pct", 0.0)
    below_pct = round(100 - above_pct, 1)
    above_cnt = getattr(breadth, "above_ma200", 0)
    below_cnt = getattr(breadth, "below_ma200", 0)

    if set_chg > 0:
        header_bg = "#1B5E20"
    elif set_chg < 0:
        header_bg = "#B71C1C"
    else:
        header_bg = "#1A237E"

    # Extract RSI/MACD before building header so badges are available
    set_idx = (indexes or {}).get("SET", {})
    _rsi = (set_idx.get("rsi", 0) or 0) if set_idx else 0
    _macd_h = (set_idx.get("macd_hist", set_idx.get("macd_histogram", 0)) or 0) if set_idx else 0
    _rsi_color = "#E74C3C" if _rsi > 70 else "#27AE60" if _rsi < 40 else "#F39C12"
    _macd_color = "#27AE60" if _macd_h > 0 else "#E74C3C"
    _macd_label = "▲ MACD Bull" if _macd_h > 0 else "▼ MACD Bear"

    # Header styled like build_single_stock_card: label → name xl → price+chg% → badges → timestamp
    header_contents: list = [
        {"type": "text", "text": "📊 ภาพรวมตลาด", "size": "xs", "color": "#DDDDDD"},
        {"type": "text", "text": "SET INDEX", "weight": "bold", "size": "xl", "color": "#FFFFFF"},
    ]
    if set_close > 0:
        header_contents.append({
            "type": "box", "layout": "horizontal",
            "contents": [
                {"type": "text", "text": f"{set_close:,.2f}", "weight": "bold", "size": "xxl", "color": "#FFFFFF", "flex": 1},
                {"type": "text", "text": f"{chg_sign}{set_chg:.2f}%", "size": "lg", "color": chg_color, "weight": "bold", "align": "end"},
            ],
        })
    if set_idx and _rsi:
        header_contents.append({
            "type": "box", "layout": "horizontal",
            "contents": [
                {"type": "text", "text": f"RSI {_rsi:.0f}", "size": "xs", "color": _rsi_color, "weight": "bold"},
                {"type": "text", "text": _macd_label, "size": "xs", "color": _macd_color, "weight": "bold", "margin": "md"},
            ],
        })
    header_contents.append(
        {"type": "text", "text": breadth.scanned_at[:16].replace("T", " "), "size": "xxs", "color": "#DDDDDD"}
    )

    above_flex = max(1, int(above_pct))
    below_flex = max(1, 100 - above_flex)

    stage_row = {
        "type": "box", "layout": "horizontal",
        "contents": [
            _tappable_stage_box("Stage 2 ✅", breadth.stage2_count, "#27AE60", "stage"),
            _tappable_stage_box("Stage 1 ⚪", breadth.stage1_count, "#95A5A6", "stage"),
            _tappable_stage_box("Stage 3 ⚠️", breadth.stage3_count, "#E67E22", "stage"),
            _tappable_stage_box("Stage 4 ❌", breadth.stage4_count, "#E74C3C", "stage"),
        ],
    }

    signal_row = {
        "type": "box", "layout": "horizontal",
        "contents": [
            _tappable_kv_box("Breakout", str(breadth.breakout_count), "#F39C12", "patterns"),
            _tappable_kv_box("VCP", str(breadth.vcp_count), "#2980B9", "patterns"),
            _kv_box("52W High", str(breadth.new_highs_52w), "#8E44AD", "52wh"),
            _kv_box("52W Low", str(breadth.new_lows_52w), "#E74C3C", "52wl"),
        ],
    }

    body_contents = [
        stage_row,
        {"type": "text", "text": f"Stage 2: {breadth.stage2_pct}% of market",
         "size": "xs", "color": "#27AE60" if breadth.stage2_pct >= 30 else "#7F8C8D",
         "align": "center", "weight": "bold"},
        {"type": "separator"},
        {
            "type": "box", "layout": "horizontal",
            "contents": [
                _kv_box("Up", str(breadth.advancing), "#27AE60", "advancing"),
                _kv_box("Down", str(breadth.declining), "#E74C3C", "declining"),
                _kv_box("Flat", str(breadth.unchanged), "#7F8C8D", "flat"),
            ],
        },
        {"type": "separator"},
        {
            "type": "box", "layout": "vertical",
            "contents": [
                {"type": "text", "text": "% Above MA200", "size": "xxs", "color": "#7F8C8D"},
                {
                    "type": "box", "layout": "horizontal",
                    "contents": [
                        {"type": "box", "layout": "vertical", "flex": above_flex,
                         "backgroundColor": "#27AE60", "height": "8px", "contents": []},
                        {"type": "box", "layout": "vertical", "flex": below_flex,
                         "backgroundColor": "#E74C3C", "height": "8px", "contents": []},
                    ],
                },
                {
                    "type": "box", "layout": "horizontal",
                    "contents": [
                        {"type": "text", "text": f"Above {above_pct:.0f}% ({above_cnt})", "size": "xxs", "color": "#27AE60", "flex": 1},
                        {"type": "text", "text": f"Below {below_pct:.0f}% ({below_cnt})", "size": "xxs", "color": "#E74C3C", "flex": 1, "align": "end"},
                    ],
                },
            ],
        },
        {"type": "separator"},
        signal_row,
    ]

    _SECTOR_COLORS = {
        "AGRO": "#27AE60", "CONSUMP": "#F39C12", "FINCIAL": "#2980B9",
        "INDUS": "#8E44AD", "PROPCON": "#E67E22", "RESOURC": "#E74C3C",
        "SERVICE": "#1ABC9C", "TECH": "#3498DB",
    }

    if sector_trends:
        top3 = sorted(sector_trends, key=lambda s: s.stage2_pct, reverse=True)[:3]
        sector_rows: list = [
            {"type": "text", "text": "Top Sectors", "size": "xxs", "color": "#7F8C8D", "margin": "sm"},
        ]
        for s in top3:
            sc = _SECTOR_COLORS.get(s.sector, "#95A5A6")
            sector_rows.append({
                "type": "box", "layout": "horizontal",
                "action": {"type": "message", "label": s.sector, "text": f"sector {s.sector}"},
                "paddingTop": "4px", "paddingBottom": "4px",
                "contents": [
                    {"type": "text", "text": s.sector, "size": "xs", "weight": "bold", "color": sc, "flex": 3},
                    {"type": "text", "text": f"{s.stage2_count} S2 · {s.stage2_pct:.0f}%", "size": "xs", "color": "#27AE60", "flex": 4},
                ],
            })
        body_contents.append({"type": "separator"})
        body_contents.extend(sector_rows)

    body_contents.append({"type": "separator"})
    body_contents.append(_captain_advice_box(_captain_market_advice(breadth)))

    return {
        "type": "bubble",
        "size": "mega",
        "header": {
            "type": "box",
            "layout": "vertical",
            "action": {"type": "message", "label": "ดัชนี", "text": "index"},
            "contents": header_contents,
            "backgroundColor": header_bg,
            "paddingAll": "16px",
        },
        "body": {
            "type": "box",
            "layout": "vertical",
            "spacing": "sm",
            "contents": body_contents,
            "paddingAll": "12px",
        },
    }


def _stage_box(label: str, count: int, color: str) -> dict:
    return {
        "type": "box",
        "layout": "vertical",
        "flex": 1,
        "alignItems": "center",
        "contents": [
            {"type": "text", "text": str(count), "weight": "bold", "size": "xl", "color": color, "align": "center"},
            {"type": "text", "text": label, "size": "xxs", "color": "#7F8C8D", "align": "center"},
        ],
    }


def _kv_box(label: str, value: str, color: str, action_cmd: str = "") -> dict:
    box = {
        "type": "box",
        "layout": "vertical",
        "flex": 1,
        "alignItems": "center",
        "contents": [
            {"type": "text", "text": value, "weight": "bold", "size": "lg", "color": color, "align": "center"},
            {"type": "text", "text": label, "size": "xxs", "color": "#7F8C8D", "align": "center"},
        ],
    }
    if action_cmd:
        box["action"] = {"type": "message", "label": label, "text": action_cmd}
    return box


def _tappable_stage_box(label: str, count: int, color: str, cmd: str) -> dict:
    return {
        "type": "box", "layout": "vertical", "flex": 1, "alignItems": "center",
        "action": {"type": "message", "label": label[:20], "text": cmd},
        "contents": [
            {"type": "text", "text": str(count), "weight": "bold", "size": "xl", "color": color, "align": "center"},
            {"type": "text", "text": label, "size": "xxs", "color": "#7F8C8D", "align": "center"},
        ],
    }


def _tappable_kv_box(label: str, value: str, color: str, cmd: str) -> dict:
    return {
        "type": "box", "layout": "vertical", "flex": 1, "alignItems": "center",
        "action": {"type": "message", "label": label[:20], "text": cmd},
        "contents": [
            {"type": "text", "text": value, "weight": "bold", "size": "lg", "color": color, "align": "center"},
            {"type": "text", "text": label, "size": "xxs", "color": "#7F8C8D", "align": "center"},
        ],
    }


def build_stock_bubble(signal: StockSignal) -> dict:
    """Build a Flex Bubble card for a single stock in the carousel."""
    pcolor = PATTERN_COLOR.get(signal.pattern, "#7F8C8D")
    pattern_label = PATTERN_LABEL.get(signal.pattern, signal.pattern)
    stage_color = STAGE_COLOR.get(signal.stage, "#95A5A6")
    chg_color = _pct_color(signal.change_pct)
    chg_sign = "+" if signal.change_pct > 0 else ""

    return {
        "type": "bubble",
        "size": "kilo",
        "header": {
            "type": "box",
            "layout": "vertical",
            "contents": [
                {
                    "type": "box",
                    "layout": "horizontal",
                    "contents": [
                        {
                            "type": "text",
                            "text": signal.symbol,
                            "weight": "bold",
                            "size": "lg",
                            "color": "#FFFFFF",
                            "flex": 1,
                        },
                        {
                            "type": "text",
                            "text": f"S{signal.stage}",
                            "size": "xs",
                            "color": stage_color,
                            "align": "end",
                            "weight": "bold",
                        },
                    ],
                },
                {
                    "type": "text",
                    "text": pattern_label,
                    "size": "xs",
                    "color": pcolor,
                    "weight": "bold",
                },
            ],
            "backgroundColor": "#1A1A2E",
            "paddingAll": "12px",
        },
        "body": {
            "type": "box",
            "layout": "vertical",
            "spacing": "sm",
            "contents": [
                {
                    "type": "box",
                    "layout": "horizontal",
                    "contents": [
                        {"type": "text", "text": f"฿{signal.close:,.2f}", "weight": "bold", "size": "lg", "flex": 1},
                        {"type": "text", "text": f"{chg_sign}{signal.change_pct:.2f}%", "size": "sm", "color": chg_color, "align": "end"},
                    ],
                },
                {
                    "type": "box",
                    "layout": "horizontal",
                    "contents": [
                        _small_kv("Vol Ratio", f"{signal.volume_ratio:.1f}x"),
                        _small_kv("Score", str(int(signal.strength_score))),
                    ],
                },
                {
                    "type": "box",
                    "layout": "horizontal",
                    "contents": [
                        _small_kv("52W High", f"฿{signal.high_52w:,.0f}"),
                        _small_kv("52W Low", f"฿{signal.low_52w:,.0f}"),
                    ],
                },
            ],
            "paddingAll": "12px",
        },
        "footer": {
            "type": "box",
            "layout": "vertical",
            "contents": [
                {
                    "type": "button",
                    "action": {
                        "type": "uri",
                        "label": "ดูชาร์ต TradingView",
                        "uri": signal.tradingview_url,
                    },
                    "style": "primary",
                    "color": "#1565C0",
                    "height": "sm",
                },
                {
                    "type": "button",
                    "action": {
                        "type": "message",
                        "label": f"วิเคราะห์ {signal.symbol}",
                        "text": signal.symbol,
                    },
                    "style": "secondary",
                    "height": "sm",
                },
            ],
            "paddingAll": "8px",
            "spacing": "sm",
        },
    }


def _small_kv(label: str, value: str) -> dict:
    return {
        "type": "box",
        "layout": "vertical",
        "flex": 1,
        "contents": [
            {"type": "text", "text": label, "size": "xxs", "color": "#7F8C8D"},
            {"type": "text", "text": value, "size": "xs", "weight": "bold"},
        ],
    }


def build_single_stock_card(signal: StockSignal, in_watchlist: bool = False) -> dict:
    """Detailed Flex Bubble for a single stock.

    Layout:
      Header (dark, ultra-compact):
        SET:SYMBOL ........................ Score 67

      Body Section 1 (price hero + context):
        ฿1.78  +1.14%
        Stage 2 · Markup                            (no parenthetical)
        Vol ฿0M · 52W ฿1.49–฿2.10              −15.2%

      Body Section 2 (Trend Reference — restored per user request):
        SMA50    ฿1.65    +7.9%
        SMA200   ฿1.42    +25.4%

      Body Section 3 (Trade Levels — only when actionable):
        🎯 Pivot      ฿1.82       −2.2%
        ⛔ Stop       ฿1.74       −2.2%

      Body Section 4 (Margin):
        💰 Margin    IM50%   2.00× lev   /   Non-marginable

      Body Section 5: Captain Signal advice
    """
    stage_label_full, stage_color = _resolve_stage_label(signal)
    # Strip the parenthetical "(running)" / "(loading)" / etc. so the
    # displayed label is just "Stage 2 · Markup". Keeps the gold star
    # variants like "Stage 2 · Pivot Ready ✨" intact (no parenthetical
    # to strip).
    paren_idx = stage_label_full.find(" (")
    stage_label = stage_label_full[:paren_idx] if paren_idx > 0 else stage_label_full

    chg_color = _pct_color(signal.change_pct)
    chg_sign = "+" if signal.change_pct > 0 else ""

    # Score color tier
    score = int(signal.strength_score or 0)
    score_color = "#27AE60" if score >= 60 else "#F39C12" if score >= 40 else "#7F8C8D"

    # Pct from 52W high — already on signal
    pct_high = getattr(signal, "pct_from_52w_high", 0.0) or 0.0
    pct_high_color = "#27AE60" if pct_high >= -5 else "#E67E22" if pct_high >= -15 else "#7F8C8D"

    # Trade value (฿M) compact format
    tvm = getattr(signal, "trade_value_m", 0.0) or 0.0
    if tvm >= 1000:
        tvm_text = f"฿{tvm/1000:.1f}B"
    elif tvm >= 1:
        tvm_text = f"฿{tvm:.1f}M"
    else:
        tvm_text = f"฿{tvm*1000:.0f}K"

    # ── Header — JUST ticker + score (per user spec) ──
    header_contents = [
        {"type": "box", "layout": "horizontal", "contents": [
            {"type": "text", "text": f"SET:{signal.symbol}",
             "weight": "bold", "size": "xl", "color": "#FFFFFF",
             "flex": 5, "wrap": True},
            {"type": "text", "text": f"Score {score}",
             "size": "md", "color": score_color, "weight": "bold",
             "flex": 4, "align": "end"},
        ]},
    ]

    # ── Body Section 1: Price hero + sub-stage + context ──
    # NOTE on colors: body background is WHITE in mega bubble, so any
    # text needs DARK color to be visible. Earlier I set price/Pivot/
    # Stop values to "#FFFFFF" thinking body was dark — they
    # disappeared. Fixed: price hero uses #1A237E (deep blue), value
    # cells use #2C3E50 (dark slate). Backgrounds + headers stay dark
    # so their white text is fine.
    body_contents: list = [
        # Big price + change% on one row
        {"type": "box", "layout": "baseline", "contents": [
            {"type": "text", "text": f"฿{signal.close:,.2f}",
             "weight": "bold", "size": "xxl", "color": "#1A237E",
             "flex": 5},
            {"type": "text", "text": f"{chg_sign}{signal.change_pct:.2f}%",
             "weight": "bold", "size": "lg", "color": chg_color,
             "flex": 4, "align": "end"},
        ]},
        # Sub-stage (no parenthetical)
        {"type": "text", "text": stage_label,
         "size": "sm", "color": stage_color, "weight": "bold",
         "wrap": True, "margin": "xs"},
        # Volume + 52W range + pct-from-high context line.
        # Bumped from xxs → xs per user feedback (was too small to read).
        {"type": "box", "layout": "horizontal", "margin": "sm",
         "contents": [
            {"type": "text",
             "text": f"Vol {tvm_text} · 52W ฿{signal.low_52w:,.2f}–฿{signal.high_52w:,.2f}",
             "size": "xs", "color": "#7F8C8D", "flex": 7, "wrap": True},
            {"type": "text", "text": f"{pct_high:+.1f}%",
             "size": "xs", "color": pct_high_color, "weight": "bold",
             "flex": 2, "align": "end"},
        ]},
    ]

    # ── Body Section 2: SMA50 + SMA200 reference ──
    # User-requested restoration after the minimalist redesign. Echoes
    # the Pivot/Stop layout for visual consistency: SMA value + signed %
    # distance from close so users see how far above / below the trend
    # MAs price is sitting.
    sma_rows: list = []
    if signal.sma50 > 0:
        gap50 = (signal.close - signal.sma50) / signal.sma50 * 100
        gap50_color = "#27AE60" if gap50 >= 0 else "#E67E22"
        sma_rows.append({
            "type": "box", "layout": "horizontal", "contents": [
                {"type": "text", "text": "SMA50", "size": "sm",
                 "color": "#7F8C8D", "flex": 3},
                {"type": "text", "text": f"฿{signal.sma50:,.2f}",
                 "size": "sm", "weight": "bold", "color": "#2C3E50",
                 "flex": 3, "align": "end"},
                {"type": "text", "text": f"{gap50:+.1f}%",
                 "size": "xs", "color": gap50_color, "weight": "bold",
                 "flex": 2, "align": "end"},
            ],
        })
    if signal.sma200 > 0:
        gap200 = (signal.close - signal.sma200) / signal.sma200 * 100
        gap200_color = "#27AE60" if gap200 >= 0 else "#E67E22"
        sma_rows.append({
            "type": "box", "layout": "horizontal", "contents": [
                {"type": "text", "text": "SMA200", "size": "sm",
                 "color": "#7F8C8D", "flex": 3},
                {"type": "text", "text": f"฿{signal.sma200:,.2f}",
                 "size": "sm", "weight": "bold", "color": "#2C3E50",
                 "flex": 3, "align": "end"},
                {"type": "text", "text": f"{gap200:+.1f}%",
                 "size": "xs", "color": gap200_color, "weight": "bold",
                 "flex": 2, "align": "end"},
            ],
        })
    if sma_rows:
        body_contents.append({"type": "separator", "margin": "md"})
        body_contents.append({
            "type": "text", "text": "Trend Reference",
            "size": "xxs", "color": "#3498DB", "weight": "bold",
        })
        body_contents.extend(sma_rows)

    # ── Body Section 3: Pivot + Stop (only when actionable) ──
    pivot = getattr(signal, "pivot_price", 0.0) or 0.0
    pstop = getattr(signal, "pivot_stop", 0.0) or 0.0
    trade_rows: list = []
    if pivot > 0:
        gap_pivot = (signal.close - pivot) / pivot * 100
        gap_color = "#27AE60" if gap_pivot >= 0 else \
                    "#F39C12" if gap_pivot > -5 else "#7F8C8D"
        trade_rows.append({
            "type": "box", "layout": "horizontal", "contents": [
                {"type": "text", "text": "🎯 Pivot", "size": "sm",
                 "color": "#7F8C8D", "flex": 3},
                {"type": "text", "text": f"฿{pivot:,.2f}",
                 "size": "sm", "weight": "bold", "color": "#2C3E50",
                 "flex": 3, "align": "end"},
                {"type": "text", "text": f"{gap_pivot:+.1f}%",
                 "size": "xs", "color": gap_color, "weight": "bold",
                 "flex": 2, "align": "end"},
            ],
        })
        if pstop > 0:
            risk_pct = (pstop - signal.close) / signal.close * 100
            trade_rows.append({
                "type": "box", "layout": "horizontal", "contents": [
                    {"type": "text", "text": "⛔ Stop", "size": "sm",
                     "color": "#7F8C8D", "flex": 3},
                    {"type": "text", "text": f"฿{pstop:,.2f}",
                     "size": "sm", "weight": "bold", "color": "#2C3E50",
                     "flex": 3, "align": "end"},
                    {"type": "text", "text": f"{risk_pct:+.1f}%",
                     "size": "xs", "color": "#E74C3C", "weight": "bold",
                     "flex": 2, "align": "end"},
                ],
            })
    if trade_rows:
        body_contents.append({"type": "separator", "margin": "md"})
        body_contents.append({
            "type": "text", "text": "Trade Levels",
            "size": "xxs", "color": "#F39C12", "weight": "bold",
        })
        body_contents.extend(trade_rows)

    # ── Body Section 4: Margin tier ──
    _mim = getattr(signal, "margin_im_pct", 0) or 0
    body_contents.append({"type": "separator", "margin": "md"})
    if _mim:
        _lev = 100.0 / _mim
        m_color = "#1ABC9C" if _mim <= 60 else "#F39C12"
        body_contents.append({
            "type": "box", "layout": "horizontal", "contents": [
                {"type": "text", "text": "💰 Margin", "size": "sm",
                 "color": "#7F8C8D", "flex": 3},
                {"type": "text", "text": f"IM{_mim}%",
                 "size": "sm", "weight": "bold", "color": m_color,
                 "flex": 2, "align": "end"},
                {"type": "text", "text": f"{_lev:.2f}× lev",
                 "size": "xs", "color": m_color, "weight": "bold",
                 "flex": 3, "align": "end"},
            ],
        })
    else:
        body_contents.append({
            "type": "box", "layout": "horizontal", "contents": [
                {"type": "text", "text": "💰 Margin", "size": "sm",
                 "color": "#7F8C8D", "flex": 3},
                {"type": "text", "text": "Non-marginable",
                 "size": "sm", "weight": "bold", "color": "#7F8C8D",
                 "flex": 5, "align": "end", "wrap": True},
            ],
        })

    # ── Body Section 5: Captain Signal advice ──
    advice = _captain_stock_advice(signal)
    if advice:
        body_contents.append(_captain_advice_box(advice))

    _pat_label = {"breakout": "Breakout", "ath_breakout": "ATH Breakout",
                  "vcp": "VCP", "vcp_low_cheat": "VCP Low", "consolidating": "Consolidating"}
    _sign = "+" if signal.change_pct >= 0 else ""
    _share_text = (
        f"📊 {signal.symbol} — {signal.name}\n"
        f"Stage {signal.stage} | {_pat_label.get(signal.pattern, signal.pattern)}\n"
        f"฿{signal.close:,.2f}  {_sign}{signal.change_pct:.2f}%"
    )
    if getattr(signal, "stop_loss", 0) > 0:
        _share_text += f"\nStop Loss: ฿{signal.stop_loss:,.2f}"
    if signal.tradingview_url:
        _share_text += f"\n\n{signal.tradingview_url}"
    _share_text += "\n\n📱 Signalix: https://lin.ee/pXKkaZJ"
    share_url = f"https://line.me/R/share?text={quote(_share_text)}"

    wl_label = "－ Remove" if in_watchlist else "＋ Watchlist"
    wl_action_text = f"remove {signal.symbol}" if in_watchlist else f"add {signal.symbol}"

    footer_buttons = [
        {"type": "button",
         "action": {"type": "message", "label": wl_label, "text": wl_action_text},
         "style": "secondary", "height": "sm", "flex": 1},
        {"type": "button",
         "action": {"type": "uri", "label": "📤 Share", "uri": share_url},
         "style": "secondary", "height": "sm", "flex": 1},
    ]

    return {
        "type": "bubble",
        "size": "mega",
        "header": {
            "type": "box",
            "layout": "vertical",
            "action": {"type": "uri", "uri": signal.tradingview_url} if signal.tradingview_url else None,
            "contents": header_contents,
            "backgroundColor": "#0D0D1A",
            "paddingAll": "16px",
        },
        "body": {
            "type": "box",
            "layout": "vertical",
            "spacing": "sm",
            "contents": body_contents,
            "paddingAll": "16px",
        },
        "footer": {
            "type": "box",
            "layout": "vertical",
            "spacing": "sm",
            "contents": [
                {"type": "box", "layout": "horizontal", "spacing": "sm", "contents": footer_buttons},
                {"type": "text", "text": f"Data: {getattr(signal, 'data_date', '') or '—'} · Scanned: {signal.scanned_at[:16].replace('T', ' ')}", "size": "xxs", "color": "#AAAAAA", "align": "center"},
            ],
            "paddingAll": "12px",
        },
    }


def build_watchlist_carousel(signals: list[StockSignal],
                             global_assets: list[dict] | None = None) -> dict:
    """Build a carousel of watchlist cards. Mixes SET stock signals with
    non-SET global assets (indexes / ETFs / US stocks / crypto) in the same
    carousel. SET bubbles come first (Minervini context), then globals.
    LINE enforces max 12 bubbles per carousel — we cap at 10 overall to
    stay well within the 50KB envelope.
    """
    global_assets = global_assets or []
    if not signals and not global_assets:
        return {
            "type": "bubble", "size": "mega",
            "body": {"type": "box", "layout": "vertical", "contents": [
                {"type": "text", "text": "📌 Watchlist ว่างเปล่า", "weight": "bold", "size": "md", "color": "#FFFFFF"},
                {"type": "text", "text": "พิมพ์ add {ชื่อหุ้น} เพื่อเพิ่ม เช่น add PTT หรือ add BTC", "size": "sm", "color": "#7F8C8D", "wrap": True, "margin": "sm"},
            ], "backgroundColor": "#0D0D1A", "paddingAll": "20px"},
        }
    bubbles = [build_single_stock_card(s, in_watchlist=True) for s in signals]
    bubbles.extend(build_global_single_card(a, in_watchlist=True) for a in global_assets)
    bubbles = bubbles[:10]
    if len(bubbles) == 1:
        return bubbles[0]
    return {"type": "carousel", "contents": bubbles}


def _detail_row(label: str, value: str, badge: str, badge_color: str) -> dict:
    return {
        "type": "box",
        "layout": "horizontal",
        "contents": [
            {"type": "text", "text": label, "size": "sm", "color": "#7F8C8D", "flex": 1},
            {"type": "text", "text": value, "size": "sm", "align": "end", "flex": 2},
            {"type": "text", "text": badge, "size": "sm", "color": badge_color, "align": "end", "flex": 1},
        ],
    }


def build_stock_list_carousel(signals: list[StockSignal], title: str = "หุ้นเด่น") -> dict:
    """Build a Flex Carousel for a list of stocks (max 10 bubbles)."""
    bubbles = [build_stock_bubble(s) for s in signals[:10]]
    return {
        "type": "carousel",
        "contents": bubbles,
    }


def _score_badge_color(score: float) -> str:
    if score >= 80:
        return "#27AE60"
    if score >= 60:
        return "#E67E22"
    return "#7F8C8D"


def _fmt_price(price: float) -> str:
    if price >= 1000:
        return f"฿{price:,.0f}"
    if price >= 100:
        return f"฿{price:.1f}"
    return f"฿{price:.2f}"


def _stock_row(rank: int, signal: StockSignal) -> dict:
    """Single row in the ranked stock list. 8 columns:
        #  | Stock | Price | Chg% | Vol | Piv | Pat | Mgn

    Three visual cues encode the new 2-layer taxonomy + margin:
      • Rank number is colored by SUB_STAGE_COLOR[sub_stage] so
        PIVOT_READY (gold), IGNITION (green), OVEREXTENDED (red) etc.
        stand out at a glance even when buried mid-list by score.
      • Piv column shows signed % distance to the pivot trigger for
        the 5 actionable sub-stages (PREP/IGNITION/CONTRACTION/
        PIVOT_READY/MARKUP). Stocks without a pivot show "—".
      • Mgn column shows Krungsri margin tier ("M50"…"M80") or "—"
        for non-marginable stocks. Color tiered: green ≤60 (best
        leverage), amber 70/80, grey for non-marginable.
    Pat column keeps the legacy 2-letter pattern code (BO/VCP/Coil/...)
    per user preference for muscle memory.
    """
    chg_pct = signal.change_pct or 0.0
    vol_ratio = signal.volume_ratio or 0.0
    close = signal.close or 0.0
    chg_color = _pct_color(chg_pct)
    chg_sign = "+" if chg_pct > 0 else ""
    vol_color = "#27AE60" if vol_ratio >= 2.0 else "#F39C12" if vol_ratio >= 1.0 else "#7F8C8D"
    pattern_short = {
        "breakout": "BO", "ath_breakout": "ATH", "vcp": "VCP",
        "vcp_low_cheat": "VCPl", "consolidating": "Coil", "going_down": "DN",
    }.get(signal.pattern, "–")

    # Rank colored by sub-stage — instant visual signal for actionable
    # sub-stages buried mid-list by score. Falls back to neutral grey
    # for old Firestore docs that don't have sub_stage populated.
    sub_stage = getattr(signal, "sub_stage", "") or ""
    rank_color = SUB_STAGE_COLOR.get(sub_stage, "#7F8C8D")

    # Piv column — signed % distance to pivot trigger. Color tiers:
    # green at/above trigger, amber within -5%, neutral otherwise.
    piv = getattr(signal, "pivot_price", 0.0) or 0.0
    if piv > 0 and close > 0:
        delta = (close - piv) / piv * 100.0
        piv_text = f"{'+' if delta > 0 else ''}{delta:.1f}%"
        if delta >= 0:
            piv_color = "#27AE60"
        elif delta > -5:
            piv_color = "#F39C12"
        else:
            piv_color = "#7F8C8D"
    else:
        piv_text = "—"
        piv_color = "#7F8C8D"

    # Mgn column — Krungsri margin tier. Lower = better leverage.
    mim = getattr(signal, "margin_im_pct", 0) or 0
    if mim:
        mgn_text  = f"M{mim}"
        mgn_color = "#27AE60" if mim <= 60 else "#F39C12"
    else:
        mgn_text  = "—"
        mgn_color = "#7F8C8D"

    return {
        "type": "box",
        "layout": "horizontal",
        "action": {"type": "message", "text": signal.symbol},
        "paddingTop": "5px",
        "paddingBottom": "5px",
        "contents": [
            {"type": "text", "text": str(rank), "size": "xxs", "color": rank_color, "flex": 1, "gravity": "center", "weight": "bold"},
            {"type": "text", "text": signal.symbol, "size": "sm", "weight": "bold", "flex": 4, "gravity": "center"},
            {"type": "text", "text": _fmt_price(close), "size": "xxs", "color": "#CCCCCC", "flex": 3, "align": "end", "gravity": "center"},
            {"type": "text", "text": f"{chg_sign}{chg_pct:.1f}%", "size": "xs", "color": chg_color, "weight": "bold", "flex": 3, "align": "end", "gravity": "center"},
            {"type": "text", "text": f"{vol_ratio:.1f}x", "size": "xxs", "color": vol_color, "flex": 2, "align": "end", "gravity": "center"},
            {"type": "text", "text": piv_text, "size": "xxs", "color": piv_color, "flex": 2, "align": "end", "gravity": "center"},
            {"type": "text", "text": pattern_short, "size": "xxs", "color": PATTERN_COLOR.get(signal.pattern, "#7F8C8D"), "flex": 2, "align": "end", "gravity": "center"},
            {"type": "text", "text": mgn_text, "size": "xxs", "color": mgn_color, "flex": 2, "align": "end", "gravity": "center", "weight": "bold"},
        ],
    }


def build_ranked_stock_list_bubble(
    signals: list[StockSignal],
    title: str,
    max_per_bubble: int = 10,
    next_cmd: str = "",
    rank_offset: int = 0,
    subtitle: str = "Sorted by Strength Score",
) -> dict:
    """
    5-bubble carousel × 10 rows = 50 stocks ≈ 42KB < LINE's 50KB carousel limit.
    Each bubble shows its card number and continuous rank numbers.
    Last bubble gets 'ดูเพิ่มเติม ▼' button when next_cmd is set.
    """
    if not signals:
        return {"type": "bubble", "size": "mega", "body": {"type": "box", "layout": "vertical",
                "contents": [{"type": "text", "text": "ไม่มีหุ้น", "color": "#7F8C8D"}]}}

    MAX_BUBBLES = 5
    cap = min(len(signals), max_per_bubble * MAX_BUBBLES)
    chunks = [signals[i:i + max_per_bubble] for i in range(0, cap, max_per_bubble)]
    n = len(chunks)

    col_header = {
        "type": "box", "layout": "horizontal", "spacing": "sm", "paddingBottom": "4px",
        "contents": [
            {"type": "text", "text": "#", "size": "xxs", "color": "#7F8C8D", "flex": 1},
            {"type": "text", "text": "Stock", "size": "xxs", "color": "#7F8C8D", "flex": 4},
            {"type": "text", "text": "Price", "size": "xxs", "color": "#7F8C8D", "flex": 3, "align": "end"},
            {"type": "text", "text": "Chg%", "size": "xxs", "color": "#7F8C8D", "flex": 3, "align": "end"},
            {"type": "text", "text": "Vol", "size": "xxs", "color": "#7F8C8D", "flex": 2, "align": "end"},
            {"type": "text", "text": "Piv", "size": "xxs", "color": "#7F8C8D", "flex": 2, "align": "end"},
            {"type": "text", "text": "Pat", "size": "xxs", "color": "#7F8C8D", "flex": 2, "align": "end"},
            {"type": "text", "text": "Mgn", "size": "xxs", "color": "#7F8C8D", "flex": 2, "align": "end"},
        ],
    }

    scan_ts = _fmt_scan_time(signals[0].scanned_at) if signals else ""

    def _make_bubble(chunk, card_idx, start_rank, is_last):
        end_rank = start_rank + len(chunk) - 1
        card_subtitle = f"Card {card_idx + 1}/{n}  ·  #{start_rank}–{end_rank}  ·  {subtitle}"
        rows = [col_header, {"type": "separator"}]
        for i, sig in enumerate(chunk):
            rows.append(_stock_row(start_rank + i, sig))

        footer_contents = []
        if scan_ts:
            footer_contents.append({"type": "text", "text": scan_ts, "size": "xxs", "color": "#95A5A6", "align": "center"})
        footer_contents.append(
            {"type": "text", "text": "Tap any stock for full analysis", "size": "xxs", "color": "#7F8C8D", "align": "center"}
        )
        if is_last and next_cmd:
            footer_contents.append({
                "type": "button",
                "action": {"type": "message", "label": "ดูเพิ่มเติม ▼", "text": next_cmd},
                "style": "primary", "color": "#1A237E", "height": "sm", "margin": "sm",
            })

        return {
            "type": "bubble", "size": "mega",
            "header": {
                "type": "box", "layout": "vertical",
                "contents": [
                    {"type": "text", "text": title, "weight": "bold", "size": "md", "color": "#FFFFFF"},
                    {"type": "text", "text": card_subtitle, "size": "xxs", "color": "#BBDDFF"},
                ],
                "backgroundColor": "#1A237E", "paddingAll": "12px",
            },
            "body": {
                "type": "box", "layout": "vertical", "spacing": "none",
                "contents": rows, "paddingAll": "12px",
            },
            "footer": {
                "type": "box", "layout": "vertical", "spacing": "xs",
                "contents": footer_contents, "paddingAll": "8px",
            },
        }

    bubbles = []
    for idx, chunk in enumerate(chunks):
        start_rank = rank_offset + idx * max_per_bubble + 1
        is_last = (idx == n - 1)
        bubbles.append(_make_bubble(chunk, idx, start_rank, is_last))

    if len(bubbles) == 1:
        return bubbles[0]
    return {"type": "carousel", "contents": bubbles}


def build_compact_stock_bubble(signal: StockSignal) -> dict:
    """Compact notification bubble — minimal info, tap to get full analysis."""
    pcolor = PATTERN_COLOR.get(signal.pattern, "#7F8C8D")
    pattern_label = PATTERN_LABEL.get(signal.pattern, signal.pattern)
    stage_color = STAGE_COLOR.get(signal.stage, "#95A5A6")
    chg_color = _pct_color(signal.change_pct)
    chg_sign = "+" if signal.change_pct > 0 else ""

    return {
        "type": "bubble",
        "size": "nano",
        "header": {
            "type": "box",
            "layout": "horizontal",
            "contents": [
                {"type": "text", "text": signal.symbol, "weight": "bold", "size": "sm", "color": "#FFFFFF", "flex": 1},
                {"type": "text", "text": f"S{signal.stage}", "size": "xxs", "color": stage_color, "align": "end", "weight": "bold"},
            ],
            "backgroundColor": "#1A1A2E",
            "paddingAll": "8px",
        },
        "body": {
            "type": "box",
            "layout": "vertical",
            "spacing": "xs",
            "contents": [
                {"type": "text", "text": pattern_label, "size": "xxs", "color": pcolor, "weight": "bold"},
                {
                    "type": "box",
                    "layout": "horizontal",
                    "contents": [
                        {"type": "text", "text": f"฿{signal.close:,.2f}", "size": "xs", "weight": "bold", "flex": 1},
                        {"type": "text", "text": f"{chg_sign}{signal.change_pct:.1f}%", "size": "xxs", "color": chg_color, "align": "end"},
                    ],
                },
                {"type": "text", "text": f"Vol: {signal.volume_ratio:.1f}x", "size": "xxs", "color": "#F39C12" if signal.volume_ratio >= 1.5 else "#7F8C8D"},
                {"type": "text", "text": signal.scanned_at[11:16], "size": "xxs", "color": "#AAAAAA"},
            ],
            "paddingAll": "8px",
        },
        "footer": {
            "type": "box",
            "layout": "vertical",
            "contents": [
                {
                    "type": "button",
                    "action": {"type": "message", "label": "วิเคราะห์", "text": signal.symbol},
                    "style": "primary",
                    "color": "#1565C0",
                    "height": "sm",
                },
            ],
            "paddingAll": "4px",
        },
    }


def build_compact_stock_carousel(signals: list[StockSignal], title: str = "หุ้นเด่น") -> dict:
    """Compact carousel for broadcast notifications (max 10 nano bubbles)."""
    bubbles = [build_compact_stock_bubble(s) for s in signals[:10]]
    return {"type": "carousel", "contents": bubbles}


def build_remaining_symbols_text(signals: list[StockSignal], title: str) -> str:
    """Build plain-text list of all symbols (beyond first 10) for overflow display."""
    remaining = signals[10:]
    if not remaining:
        return ""
    lines = [f"📋 {title} ทั้งหมด {len(signals)} หุ้น (แสดง 10 แรก)", "—" * 20]
    for i in range(0, len(remaining), 5):
        lines.append("  ".join(s.symbol for s in remaining[i:i + 5]))
    lines.append("\nพิมพ์ชื่อหุ้นเพื่อดูรายละเอียด")
    return "\n".join(lines)


def _ma_row(label: str, value: float, close: float) -> dict:
    """One horizontal row: label | value | above/below badge."""
    if value <= 0:
        return {}
    above = close >= value
    badge_text = "✓" if above else "✗"
    badge_color = "#27AE60" if above else "#E74C3C"
    return {"type": "box", "layout": "horizontal", "contents": [
        {"type": "text", "text": label, "size": "xxs", "color": "#7F8C8D", "flex": 2},
        {"type": "text", "text": f"{value:,.2f}", "size": "xxs", "color": "#2C3E50", "align": "center", "flex": 3},
        {"type": "text", "text": badge_text, "size": "xxs", "color": badge_color, "weight": "bold", "align": "end", "flex": 1},
    ]}


def build_global_snapshot_card(snapshot: dict[str, dict]) -> dict:
    """Bulk global view — carousel of section bubbles.

    Earlier iteration used one giant scroll-to-bottom bubble with all 50
    assets. Users reported "the list is too long, we need a sub-card" —
    sections are the natural breaking point so we split into one bubble
    per section. Each bubble fits on screen (5-9 rows), user swipes
    between sections.

    Section order matches GLOBAL_SECTION_ORDER (US Indexes first because
    they set the daily tone, FX/Commodities right after for Thai-relevant
    macros, then ETFs/stocks, crypto last). Within each section, rows
    are sorted by today's % change so the section's leader pops to top.

    Each row is tappable → sends the code as a text message so the
    single-asset handler can render the detail card.

    snapshot shape: {code: {name, class, section, close, change_pct,
    scanned_at}} produced by data.fetch_global_snapshot().
    """
    from data import GLOBAL_SECTION_ORDER, GLOBAL_SYMBOLS

    # Class icons: glyph at the start of every row. fx + commodity were
    # added with the 50-asset expansion.
    CLASS_ICON = {
        "index": "📊", "etf": "📈", "stock": "🏢",
        "crypto": "₿", "fx": "💱", "commodity": "🛢",
    }

    def _fmt_price(asset_class: str, close: float) -> str:
        if asset_class == "fx":
            return f"{close:,.4f}"
        if asset_class == "crypto" and close < 100:
            return f"{close:,.4f}"
        return f"{close:,.2f}"

    # Group snapshot entries by section using GLOBAL_SYMBOLS as the source
    # of truth. snapshot entries that lack a section (legacy / fetch race)
    # drop into "other" so they're still visible somewhere.
    by_section: dict[str, list[tuple[str, dict]]] = {}
    for code, d in snapshot.items():
        section = (GLOBAL_SYMBOLS.get(code) or {}).get("section") or "other"
        by_section.setdefault(section, []).append((code, d))

    # Subtitle shared across all bubbles — shows the scan timestamp once
    # so users don't see it repeated per bubble. Uses the first available
    # snapshot entry's scanned_at since fetch_global_snapshot stamps them
    # all with the same value.
    ts_subtitle = ""
    if snapshot:
        ts = next(iter(snapshot.values())).get("scanned_at", "")
        if ts:
            try:
                dt = datetime.fromisoformat(ts)
                ts_subtitle = dt.strftime("%H:%M %d/%m/%y")
            except Exception:
                pass

    bubbles: list[dict] = []
    for section_id, section_label in GLOBAL_SECTION_ORDER:
        items = by_section.get(section_id) or []
        if not items:
            continue
        items.sort(key=lambda kv: -(kv[1].get("change_pct") or 0))

        # Bubble header summary: count + best mover for at-a-glance scan
        # while swiping. e.g. "8 assets · TWII +3.23%"
        leader_code, leader = items[0]
        leader_chg = leader.get("change_pct") or 0.0
        leader_sign = "+" if leader_chg > 0 else ""
        bubble_subtitle = (f"{len(items)} assets · top {leader_code} "
                           f"{leader_sign}{leader_chg:.2f}%")
        if ts_subtitle:
            bubble_subtitle += f" · {ts_subtitle}"

        rows: list[dict] = []
        for code, d in items:
            chg = d.get("change_pct") or 0.0
            color = "#27AE60" if chg > 0 else ("#E74C3C" if chg < 0 else "#7F8C8D")
            sign = "+" if chg > 0 else ""
            asset_class = d.get("class", "")
            icon = CLASS_ICON.get(asset_class, "•")
            close = d.get("close") or 0.0
            price_text = _fmt_price(asset_class, close)
            rows.append({
                "type": "box", "layout": "horizontal",
                "action": {"type": "message", "label": code[:20], "text": code},
                "paddingTop": "6px", "paddingBottom": "6px",
                "contents": [
                    {"type": "text", "text": f"{icon} {code}", "flex": 3, "size": "sm",
                     "weight": "bold", "color": "#FFFFFF"},
                    {"type": "text", "text": d.get("name", ""), "flex": 5, "size": "xxs",
                     "color": "#9E9E9E", "wrap": False},
                    {"type": "text", "text": price_text, "flex": 3, "size": "xs",
                     "color": "#FFFFFF", "align": "end"},
                    {"type": "text", "text": f"{sign}{chg:.2f}%", "flex": 2, "size": "xs",
                     "color": color, "weight": "bold", "align": "end"},
                ],
            })
            rows.append({"type": "separator", "color": "#2A2A2A"})

        bubbles.append({
            "type": "bubble", "size": "mega",
            "header": {
                "type": "box", "layout": "vertical",
                "contents": [
                    {"type": "text", "text": section_label, "weight": "bold",
                     "size": "lg", "color": "#FFFFFF"},
                    {"type": "text", "text": bubble_subtitle, "size": "xxs",
                     "color": "#B2DFDB", "wrap": True},
                ],
                "backgroundColor": "#0D0D1A",
                "paddingAll": "14px",
            },
            "body": {
                "type": "box", "layout": "vertical", "spacing": "none",
                "backgroundColor": "#1A1A1A",
                "contents": rows,
                "paddingAll": "12px",
            },
        })

    if not bubbles:
        return {
            "type": "bubble", "size": "mega",
            "header": {
                "type": "box", "layout": "vertical",
                "contents": [{"type": "text", "text": "🌏 Global Snapshot",
                              "weight": "bold", "size": "lg", "color": "#FFFFFF"}],
                "backgroundColor": "#0D0D1A", "paddingAll": "14px",
            },
            "body": {
                "type": "box", "layout": "vertical",
                "backgroundColor": "#1A1A1A",
                "contents": [{"type": "text", "text": "ไม่มีข้อมูลขณะนี้",
                              "size": "sm", "color": "#7F8C8D", "align": "center"}],
                "paddingAll": "20px",
            },
        }

    return {"type": "carousel", "contents": bubbles}


def build_global_single_card(asset: dict, in_watchlist: bool = False) -> dict:
    """Detail bubble for a single non-SET asset (index / ETF / US stock /
    crypto). No Minervini stage or pattern — those categories don't
    translate across asset classes — just the reference levels a user
    wants when they tap a row on the Global Snapshot card:
      • current price + day change%
      • day range (low → high)
      • 52W range + % from 52W high (ATH proximity)
      • volume (blank for indexes where yfinance returns 0)
      • "View on TradingView" external link
      • "+ Watchlist" / "− Remove" toggle (in_watchlist=True flips the label)

    asset shape: output of data.fetch_global_asset(code).
    """
    CLASS_ICON = {
        "index": "📊", "etf": "📈", "stock": "🏢",
        "crypto": "₿", "fx": "💱", "commodity": "🛢",
    }
    CLASS_COLOR = {
        "index": "#1A237E", "etf": "#006064",
        "stock": "#0D47A1", "crypto": "#F7931A",
        "fx": "#4527A0",       # purple — distinct from index navy
        "commodity": "#5D4037", # brown/earth — distinct from all of the above
    }

    chg = asset.get("change_pct") or 0.0
    chg_color = "#27AE60" if chg > 0 else ("#E74C3C" if chg < 0 else "#7F8C8D")
    chg_sign = "+" if chg > 0 else ""
    close = asset.get("close") or 0.0
    code = asset.get("code", "")
    asset_class = asset.get("class", "")
    icon = CLASS_ICON.get(asset_class, "•")
    accent = CLASS_COLOR.get(asset_class, "#263238")

    # Price formatting: FX rates always 4 decimals (USD/THB ≈ 36.4500 not
    # 36.45); low-value crypto also 4 decimals; everything else 2.
    def _fmt_price(x: float) -> str:
        if asset_class == "fx":
            return f"{x:,.4f}"
        if asset_class == "crypto" and x < 100:
            return f"{x:,.4f}"
        return f"{x:,.2f}"

    # 52W high proximity — "ATH context" for trend traders.
    week52_high = asset.get("week52_high") or close
    pct_from_high = ((close - week52_high) / week52_high * 100) if week52_high else 0
    week52_low = asset.get("week52_low") or close
    pct_from_low = ((close - week52_low) / week52_low * 100) if week52_low else 0

    # TradingView URL — different symbol namespace per asset class.
    # For indexes/stocks we can just send the yf ticker minus "^"; for
    # crypto we prefix with CRYPTO:. Keeps the deep link best-effort; TV
    # search handles mismatches gracefully.
    yf_tk = asset.get("yf", code)
    if asset_class == "crypto":
        tv_symbol = f"CRYPTO:{yf_tk.replace('-', '')}"
    elif asset_class == "fx":
        # yf 'THB=X' / 'JPY=X' → TradingView 'FX:USDTHB' / 'FX:USDJPY'.
        # 'DX-Y.NYB' is the dollar index — TV uses 'TVC:DXY'.
        if yf_tk == "DX-Y.NYB":
            tv_symbol = "TVC:DXY"
        else:
            curr = yf_tk.replace("=X", "")
            tv_symbol = f"FX:USD{curr}"
    elif asset_class == "commodity":
        # yf futures format 'GC=F' (gold), 'CL=F' (oil), 'HG=F' (copper),
        # 'NG=F' (nat gas) → TradingView prefers 'COMEX:GC1!' / 'NYMEX:CL1!'.
        # We use a simple TVC: prefix that TV resolves to chart-of-record.
        tv_map = {"GC=F": "TVC:GOLD", "CL=F": "TVC:USOIL",
                  "HG=F": "TVC:COPPER", "NG=F": "TVC:NATGAS"}
        tv_symbol = tv_map.get(yf_tk, yf_tk.replace("=F", ""))
    elif yf_tk.startswith("^"):
        tv_symbol = f"INDEX:{yf_tk.lstrip('^')}"
    elif yf_tk.endswith(".SS"):
        tv_symbol = f"SSE:{yf_tk.replace('.SS', '')}"
    else:
        tv_symbol = yf_tk
    tv_url = f"https://www.tradingview.com/symbols/{tv_symbol}/"

    def _kv_row(label: str, value: str, value_color: str = "#FFFFFF",
                label_color: str = "#9E9E9E") -> dict:
        return {
            "type": "box", "layout": "horizontal",
            "paddingTop": "4px", "paddingBottom": "4px",
            "contents": [
                {"type": "text", "text": label, "size": "xs",
                 "color": label_color, "flex": 3},
                {"type": "text", "text": value, "size": "sm",
                 "color": value_color, "flex": 4, "align": "end",
                 "weight": "bold"},
            ],
        }

    vol = asset.get("volume") or 0
    if vol <= 0:
        vol_display = "—"
    elif vol >= 1_000_000:
        vol_display = f"{vol/1_000_000:,.1f}M"
    elif vol >= 1_000:
        vol_display = f"{vol/1_000:,.1f}K"
    else:
        vol_display = f"{vol:,.0f}"

    # Conditional emphasis: red on > -10% drawdown, green on > +10% extended.
    # Plain white default reads correctly on the dark body.
    body_rows = [
        _kv_row("Day range",
                f"{_fmt_price(asset.get('day_low', 0))} → {_fmt_price(asset.get('day_high', 0))}"),
        _kv_row("52W range",
                f"{_fmt_price(week52_low)} → {_fmt_price(week52_high)}"),
        _kv_row("From 52W high", f"{pct_from_high:+.2f}%",
                value_color="#E74C3C" if pct_from_high < -10 else "#FFFFFF"),
        _kv_row("From 52W low", f"{pct_from_low:+.2f}%",
                value_color="#27AE60" if pct_from_low > 10 else "#FFFFFF"),
        _kv_row("Volume", vol_display),
    ]

    # ── Stage + pattern badges (when computable) ─────────────────────
    # Same Minervini analysis the SET-stock card runs. None for assets
    # with too-short history (<200 bars); falls back gracefully.
    stage = asset.get("stage")
    pattern = asset.get("pattern")
    stage_weakening = asset.get("stage_weakening", False)
    stage_pattern_row = None
    if stage is not None:
        stage_label = STAGE_LABEL.get(stage, f"Stage {stage}")
        stage_color = STAGE_COLOR.get(stage, "#7F8C8D")
        if stage_weakening:
            stage_label = f"{stage_label} ⚠"
        contents = [
            {"type": "text", "text": stage_label, "size": "xs",
             "color": stage_color, "weight": "bold", "flex": 3},
        ]
        if pattern and pattern != "consolidating":
            pcolor = PATTERN_COLOR.get(pattern, "#7F8C8D")
            plabel = PATTERN_LABEL.get(pattern, pattern)
            contents.append({
                "type": "text", "text": plabel, "size": "xs",
                "color": pcolor, "weight": "bold", "flex": 3,
                "align": "end",
            })
        stage_pattern_row = {
            "type": "box", "layout": "horizontal",
            "paddingTop": "4px", "paddingBottom": "8px",
            "contents": contents,
        }

    # SMA row — show MA50 gap when meaningful (helps users see the
    # weakening modifier in context).
    sma50 = asset.get("sma50") or 0
    if sma50 > 0:
        gap50 = (close - sma50) / sma50 * 100
        body_rows.append(_kv_row(
            "vs SMA50", f"{gap50:+.2f}%",
            value_color="#27AE60" if gap50 > 0 else "#E74C3C",
        ))

    body_contents: list = [
        # Price + change% hero row
        {"type": "box", "layout": "baseline",
         "paddingBottom": "10px",
         "contents": [
             {"type": "text", "text": _fmt_price(close),
              "weight": "bold", "size": "xxl", "color": "#FFFFFF",
              "flex": 5},
             {"type": "text", "text": f"{chg_sign}{chg:.2f}%",
              "weight": "bold", "size": "lg", "color": chg_color,
              "align": "end", "flex": 3},
         ]},
    ]
    if stage_pattern_row:
        body_contents.append(stage_pattern_row)
    body_contents.append({"type": "separator", "color": "#333333"})
    body_contents.append({"type": "box", "layout": "vertical",
                          "paddingTop": "10px", "spacing": "none",
                          "contents": body_rows})

    return {
        "type": "bubble", "size": "mega",
        "header": {
            "type": "box", "layout": "vertical",
            "backgroundColor": accent, "paddingAll": "14px",
            # Tap header → open TradingView (matches build_single_stock_card
            # pattern). Dropping the redundant footer button — header tap
            # is the standard Signalix gesture for "open chart externally".
            "action": {"type": "uri", "uri": tv_url},
            "contents": [
                {"type": "text", "text": f"{icon} {code}  📊",
                 "weight": "bold", "size": "xl", "color": "#FFFFFF"},
                {"type": "text", "text": asset.get("name", ""),
                 "size": "xs", "color": "#E3F2FD", "wrap": True},
                {"type": "text", "text": "Tap header → TradingView",
                 "size": "xxs", "color": "#B2DFDB"},
            ],
        },
        "body": {
            # Dark body matches Signalix's other detail cards.
            "type": "box", "layout": "vertical", "paddingAll": "14px",
            "backgroundColor": "#1A1A1A",
            "spacing": "none",
            "contents": body_contents,
        },
        "footer": {
            "type": "box", "layout": "vertical", "spacing": "sm",
            "paddingAll": "10px",
            "contents": [
                {"type": "button", "style": "link", "height": "sm",
                 "action": {"type": "message",
                            "label": ("− Remove" if in_watchlist else "＋ Watchlist"),
                            "text": f"{'remove' if in_watchlist else 'add'} {code}"}},
                # Back to Global only when the user came from a bulk view;
                # in the watchlist carousel it'd look out of place.
                *(
                    [{"type": "button", "style": "link", "height": "sm",
                      "action": {"type": "message",
                                 "label": "← Back to Global", "text": "global"}}]
                    if not in_watchlist else []
                ),
            ],
        },
    }


def build_index_carousel(indexes: dict[str, dict]) -> dict:
    """Build a carousel of index bubbles with full stock-like analysis for all indexes."""
    INDEX_COLORS = {
        "SET": "#1A237E", "SET50": "#0D47A1", "SET100": "#1565C0",
        "MAI": "#4A148C", "sSET": "#006064", "SETESG": "#2E7D32",
    }
    STAGE_COLORS = {1: "#7F8C8D", 2: "#27AE60", 3: "#F39C12", 4: "#E74C3C"}

    bubbles = []
    for name, data in indexes.items():
        close = data.get("close", 0.0)
        chg = data.get("change_pct", 0.0)
        chg_sign = "+" if chg > 0 else ""
        chg_color = _pct_color(chg)
        tv_url = INDEX_TV_URLS.get(name, "https://www.tradingview.com")
        rsi = data.get("rsi")
        has_analysis = rsi is not None

        # Header background: green/red if changed, else index colour
        if chg > 0.3:
            bg = "#1B5E20"
        elif chg < -0.3:
            bg = "#B71C1C"
        else:
            bg = INDEX_COLORS.get(name, "#1A237E")

        body_contents = [
            # Price row
            {"type": "box", "layout": "horizontal", "contents": [
                {"type": "text", "text": f"{close:,.2f}", "weight": "bold",
                 "size": "xxl" if name == "SET" else "xl", "flex": 3},
                {"type": "text", "text": f"{chg_sign}{chg:.2f}%", "size": "sm",
                 "color": chg_color, "weight": "bold", "align": "end", "flex": 2},
            ]},
        ]

        if has_analysis:
            stage = data.get("stage")
            ma50 = data.get("ma50", 0.0)
            ma150 = data.get("ma150", 0.0)
            ma200 = data.get("ma200", 0.0)
            ma200_rising = data.get("ma200_rising", False)
            above_ma200 = data.get("above_ma200")
            macd_hist = data.get("macd_hist", 0.0)
            macd_bullish = data.get("macd_bullish_cross", False)
            macd_bearish = data.get("macd_bearish_cross", False)
            high_52w = data.get("high_52w", 0.0)
            low_52w = data.get("low_52w", 0.0)
            pct_from_high = data.get("pct_from_52w_high", 0.0)
            implication = data.get("implication", "")

            # Stage badge + pattern (index-aware pattern detection from
            # analyze_index — same name space as stocks, but the volume
            # gate is relaxed because index volume is aggregate).
            stage_pattern_row = []
            if stage:
                stage_color = STAGE_COLORS.get(stage, "#7F8C8D")
                stage_pattern_row.append({
                    "type": "text", "text": f"Stage {stage}",
                    "size": "xs", "color": stage_color, "weight": "bold", "flex": 1,
                })
            pattern = data.get("pattern")
            if pattern in ("breakout", "ath_breakout", "breakout_attempt", "vcp", "vcp_low_cheat"):
                pcolor = PATTERN_COLOR.get(pattern, "#7F8C8D")
                plabel = PATTERN_LABEL.get(pattern, pattern)
                stage_pattern_row.append({
                    "type": "text", "text": plabel,
                    "size": "xs", "color": pcolor, "weight": "bold",
                    "flex": 2, "align": "end",
                })
            if stage_pattern_row:
                body_contents.append({"type": "box", "layout": "horizontal",
                                       "contents": stage_pattern_row})

            body_contents.append({"type": "separator"})

            # MA table
            for row in [_ma_row("MA50", ma50, close), _ma_row("MA150", ma150, close), _ma_row("MA200", ma200, close)]:
                if row:
                    body_contents.append(row)
            if ma200_rising:
                body_contents.append({"type": "text", "text": "MA200 กำลังขึ้น ↑", "size": "xxs", "color": "#27AE60"})

            body_contents.append({"type": "separator"})

            # RSI
            rsi_color = "#E74C3C" if rsi > 70 else ("#27AE60" if rsi < 30 else "#F39C12")
            rsi_label = "Overbought" if rsi > 70 else ("Oversold" if rsi < 30 else "ปกติ")
            body_contents.append({"type": "box", "layout": "horizontal", "contents": [
                {"type": "text", "text": "RSI (14)", "size": "xxs", "color": "#7F8C8D", "flex": 2},
                {"type": "text", "text": f"{rsi:.0f} {rsi_label}", "size": "xxs",
                 "color": rsi_color, "weight": "bold", "align": "end", "flex": 3},
            ]})

            # MACD
            if macd_bullish:
                macd_label, macd_color = "🟢 MACD Cross Up", "#27AE60"
            elif macd_bearish:
                macd_label, macd_color = "🔴 MACD Cross Down", "#E74C3C"
            elif macd_hist > 0:
                macd_label, macd_color = "MACD เป็นบวก ↑", "#27AE60"
            else:
                macd_label, macd_color = "MACD เป็นลบ ↓", "#E74C3C"
            body_contents.append({"type": "text", "text": macd_label, "size": "xxs", "color": macd_color})

            body_contents.append({"type": "separator"})

            # 52W range
            body_contents.append({"type": "box", "layout": "horizontal", "contents": [
                {"type": "text", "text": "52W High", "size": "xxs", "color": "#7F8C8D", "flex": 2},
                {"type": "text", "text": f"{high_52w:,.2f} ({pct_from_high:+.1f}%)",
                 "size": "xxs", "color": "#E74C3C" if pct_from_high < -20 else "#2C3E50",
                 "align": "end", "flex": 3},
            ]})
            body_contents.append({"type": "box", "layout": "horizontal", "contents": [
                {"type": "text", "text": "52W Low", "size": "xxs", "color": "#7F8C8D", "flex": 2},
                {"type": "text", "text": f"{low_52w:,.2f}", "size": "xxs", "color": "#2C3E50", "align": "end", "flex": 3},
            ]})

            # Implication summary
            if implication:
                body_contents.append({"type": "separator"})
                body_contents.append({"type": "text", "text": implication,
                                       "size": "xxs", "color": "#7F8C8D", "wrap": True})

        # Format scan timestamp for footer
        scanned_at_str = data.get("scanned_at", "")
        ts_text = _fmt_scan_time(scanned_at_str)

        bubble_size = "mega" if has_analysis else "kilo"
        footer_contents = []
        if ts_text:
            footer_contents.append({"type": "text", "text": ts_text, "size": "xxs", "color": "#95A5A6", "align": "center"})
        footer_contents.append({"type": "button", "action": {"type": "uri", "label": "ดูชาร์ต", "uri": tv_url},
                                 "style": "primary", "color": "#1565C0", "height": "sm"})

        bubbles.append({
            "type": "bubble",
            "size": bubble_size,
            "header": {
                "type": "box",
                "layout": "vertical",
                "contents": [
                    {"type": "text", "text": name, "weight": "bold", "size": "lg", "color": "#FFFFFF"},
                ],
                "backgroundColor": bg,
                "paddingAll": "12px",
            },
            "body": {
                "type": "box",
                "layout": "vertical",
                "spacing": "xs",
                "contents": body_contents,
                "paddingAll": "12px",
            },
            "footer": {
                "type": "box",
                "layout": "vertical",
                "spacing": "xs",
                "contents": footer_contents,
                "paddingAll": "8px",
            },
        })
    return {"type": "carousel", "contents": bubbles}


def build_sector_carousel(sectors: list[SectorSummary]) -> dict:
    """Build a carousel showing SET sector breadth."""
    SECTOR_COLORS = {
        "AGRO": "#27AE60", "CONSUMP": "#F39C12", "FINCIAL": "#2980B9",
        "INDUS": "#8E44AD", "PROPCON": "#E67E22", "RESOURC": "#E74C3C",
        "SERVICE": "#1ABC9C", "TECH": "#3498DB", "OTHER": "#95A5A6",
    }
    SECTOR_THAI = {
        "AGRO": "เกษตร/อาหาร", "CONSUMP": "สินค้าผู้บริโภค", "FINCIAL": "การเงิน",
        "INDUS": "อุตสาหกรรม", "PROPCON": "อสังหา/ก่อสร้าง", "RESOURC": "พลังงาน/ทรัพยากร",
        "SERVICE": "บริการ", "TECH": "เทคโนโลยี", "OTHER": "อื่นๆ",
    }
    bubbles = []
    for sec in sectors[:12]:
        color = SECTOR_COLORS.get(sec.sector, "#95A5A6")
        thai = SECTOR_THAI.get(sec.sector, sec.sector)
        adv_sign = "+" if sec.advancing >= sec.declining else ""
        bubbles.append({
            "type": "bubble",
            "size": "kilo",
            "header": {
                "type": "box",
                "layout": "vertical",
                "contents": [
                    {"type": "text", "text": sec.sector, "weight": "bold", "size": "sm", "color": "#FFFFFF"},
                    {"type": "text", "text": thai, "size": "xxs", "color": "#CCCCCC"},
                ],
                "backgroundColor": color,
                "paddingAll": "10px",
            },
            "body": {
                "type": "box",
                "layout": "vertical",
                "spacing": "xs",
                "contents": [
                    _small_kv("หุ้น Stage 2", f"{sec.stage2_count}/{sec.total} ({sec.stage2_pct}%)"),
                    _small_kv("Breakout", str(sec.breakout_count)),
                    _small_kv("Avg Score", str(sec.avg_strength)),
                    _small_kv("ขึ้น/ลง", f"{sec.advancing}/{sec.declining}"),
                ],
                "paddingAll": "10px",
            },
        })
    return {"type": "carousel", "contents": bubbles}


def build_sector_overview_card(sectors: list[SectorSummary],
                               sector_indexes: Optional[dict] = None) -> dict:
    """Single mega bubble — each sector row is tappable, no footer buttons.

    Columns: Sector | Index price/Δ% (NEW) | Stage2% | Trend N
    sector_indexes is the {sector_code: {close, change_pct, ...}} dict
    populated by data.fetch_sector_index_prices and cached in
    main._last_sector_indexes. When absent or per-sector empty, the index
    column shows '—' and the row still renders so users can still tap the
    sector for the drill-down.

    OTHER (unmapped stocks) is hidden — it adds noise without actionable insight.
    """
    SECTOR_COLORS = {
        "AGRO": "#27AE60", "CONSUMP": "#F39C12", "FINCIAL": "#2980B9",
        "INDUS": "#8E44AD", "PROPCON": "#E67E22", "RESOURC": "#E74C3C",
        "SERVICE": "#1ABC9C", "TECH": "#3498DB",
    }
    SECTOR_THAI = {
        "AGRO": "เกษตร/อาหาร", "CONSUMP": "สินค้าบริโภค", "FINCIAL": "การเงิน",
        "INDUS": "อุตสาหกรรม", "PROPCON": "อสังหาฯ/ก่อสร้าง", "RESOURC": "ทรัพยากร",
        "SERVICE": "บริการ", "TECH": "เทคโนโลยี",
    }
    sector_indexes = sector_indexes or {}
    rows = []
    visible = [s for s in sectors if s.sector != "OTHER"]
    visible.sort(key=lambda s: s.stage2_pct, reverse=True)
    for sec in visible:
        color = SECTOR_COLORS.get(sec.sector, "#95A5A6")
        trend = "▲" if sec.advancing > sec.declining else ("▼" if sec.declining > sec.advancing else "─")
        trend_color = "#27AE60" if trend == "▲" else ("#E74C3C" if trend == "▼" else "#7F8C8D")
        s2_color = "#27AE60" if sec.stage2_pct >= 30 else ("#F39C12" if sec.stage2_pct >= 20 else "#7F8C8D")

        idx = sector_indexes.get(sec.sector) or {}
        idx_close = idx.get("close") or 0.0
        idx_chg = idx.get("change_pct") or 0.0
        idx_color = "#27AE60" if idx_chg > 0 else ("#E74C3C" if idx_chg < 0 else "#7F8C8D")
        if idx_close > 0:
            idx_text = f"{idx_close:,.0f} {'+' if idx_chg > 0 else ''}{idx_chg:.1f}%"
        else:
            idx_text = "—"

        rows.append({
            "type": "box", "layout": "horizontal",
            "paddingTop": "6px", "paddingBottom": "6px",
            "action": {"type": "message", "label": sec.sector, "text": f"sector {sec.sector}"},
            "contents": [
                {"type": "text", "text": sec.sector, "size": "sm", "weight": "bold", "color": color, "flex": 3},
                {"type": "text", "text": idx_text, "size": "xs", "color": idx_color, "flex": 4, "align": "end"},
                {"type": "text", "text": f"S2:{sec.stage2_pct:.0f}%", "size": "xs", "color": s2_color, "weight": "bold", "flex": 2, "align": "end"},
                {"type": "text", "text": f"{trend} {sec.total}", "size": "xs", "color": trend_color, "flex": 2, "align": "end"},
            ],
        })
        rows.append({"type": "separator"})

    return {
        "type": "bubble",
        "size": "mega",
        "header": {
            "type": "box", "layout": "vertical",
            "contents": [
                {"type": "text", "text": "🏭 Sector Overview", "weight": "bold", "size": "lg", "color": "#FFFFFF"},
                {"type": "text", "text": "แตะชื่อกลุ่มเพื่อดูรายชื่อหุ้น · Index | S2% | Trend", "size": "xxs", "color": "#CCCCCC"},
            ],
            "backgroundColor": "#0D0D1A",
            "paddingAll": "14px",
        },
        "body": {
            "type": "box", "layout": "vertical", "spacing": "none",
            "contents": [
                {
                    "type": "box", "layout": "horizontal", "paddingBottom": "4px",
                    "contents": [
                        {"type": "text", "text": "Sector", "size": "xxs", "color": "#AAAAAA", "flex": 3},
                        {"type": "text", "text": "Index Δ%", "size": "xxs", "color": "#AAAAAA", "flex": 4, "align": "end"},
                        {"type": "text", "text": "S2%", "size": "xxs", "color": "#AAAAAA", "flex": 2, "align": "end"},
                        {"type": "text", "text": "Trend N", "size": "xxs", "color": "#AAAAAA", "flex": 2, "align": "end"},
                    ],
                },
                {"type": "separator"},
                *rows,
            ],
            "paddingAll": "14px",
        },
    }


def build_stage_picker_card(
    breadth: Optional[MarketBreadth] = None,
    signals: Optional[list] = None,
    *,
    scope_label: str = "",
    cmd_prefix: str = "",
) -> dict:
    """4-bubble carousel for stage picker (Stage 1–4).

    Each bubble surfaces its sub-stages with counts (computed from
    `signals` when provided) and a prescriptive verb top-bar:
    Stage 1 = WATCH, Stage 2 = TRADE, Stage 3 = TRIM, Stage 4 = AVOID.

    Stage 2 bubble gets a dual footer: full-list button + jump-to-
    Pivot-Ready button (the most actionable cohort).

    Scope params:
      • scope_label = "" (default, market-wide), "SET100", "SET50", "MAI".
        Shown in the bubble header so the user knows which universe.
      • cmd_prefix  = "" (default), "set100 ", "set50 ", "mai ". Tappable
        sub-stage rows + footer buttons emit `<prefix><cmd>` so taps
        keep the user inside the same scope (e.g. tapping Pivot Ready
        from a SET100 picker emits `set100 ready`, not `ready`).

    Counts in `breadth` and `signals` should already be SCOPED to the
    same universe (caller's responsibility). For market-wide use,
    pass the global breadth + signals.
    """
    STAGE_BG = {1: "#555555", 2: "#1B5E20", 3: "#E65100", 4: "#B71C1C"}
    STAGE_VERB = {1: "WATCH", 2: "TRADE", 3: "TRIM", 4: "AVOID"}
    STAGE_ICON = {1: "⚪", 2: "🟢", 3: "🟡", 4: "🔴"}
    STAGE_DESC = {
        1: "Basing — สะสมตัว",
        2: "Uptrend ✅",
        3: "Topping ⚠️",
        4: "Downtrend ❌",
    }

    # Sub-stages per parent, in display order. Each tuple = (icon,
    # short label, sub_stage constant, filter command tail).
    SUBS = {
        1: [
            ("⚪", "Base",       "STAGE_1_BASE",         "base"),
            ("🌱", "Prep",       "STAGE_1_PREP",         "prep"),
        ],
        2: [
            ("🎯", "Pivot Ready ✨", "STAGE_2_PIVOT_READY",  "ready"),
            ("🚀", "Ignition",       "STAGE_2_IGNITION",     "ignition"),
            ("✅", "Markup",         "STAGE_2_MARKUP",       "markup"),
            ("👀", "Contraction",    "STAGE_2_CONTRACTION",  "contraction"),
            ("⚠",  "Overextended",   "STAGE_2_OVEREXTENDED", "overextended"),
        ],
        3: [
            ("🟡", "Volatile",     "STAGE_3_VOLATILE",  "volatile"),
            ("🟠", "Distribution", "STAGE_3_DIST_DIST", "dist"),
        ],
        4: [
            ("🔴", "Breakdown",   "STAGE_4_BREAKDOWN", "breakdown"),
            ("🔴", "Downtrend",   "STAGE_4_DOWNTREND", "downtrend"),
        ],
    }

    stage_counts: dict[int, int] = {1: 0, 2: 0, 3: 0, 4: 0}
    if breadth:
        stage_counts = {
            1: getattr(breadth, "stage1_count", 0),
            2: getattr(breadth, "stage2_count", 0),
            3: getattr(breadth, "stage3_count", 0),
            4: getattr(breadth, "stage4_count", 0),
        }

    # Tally sub-stage counts from signals (if provided). Falls back to
    # zeros when signals empty so the bubble still renders cleanly.
    sub_counts: dict[str, int] = {}
    if signals:
        from collections import Counter
        sub_counts = Counter(getattr(s, "sub_stage", "") or "" for s in signals)

    def _sub_row(icon: str, label: str, sub_const: str, cmd_tail: str) -> dict:
        cnt = sub_counts.get(sub_const, 0)
        color = SUB_STAGE_COLOR.get(sub_const, "#7F8C8D")
        # Scope-aware command — when cmd_prefix is set, taps drill into
        # the same scope (set100/set50/mai) instead of falling back to
        # the market-wide filter.
        cmd = f"{cmd_prefix}{cmd_tail}"
        return {
            "type": "box", "layout": "horizontal",
            "action": {"type": "message", "label": cmd_tail, "text": cmd},
            "paddingTop": "3px", "paddingBottom": "3px",
            "contents": [
                {"type": "text", "text": f"{icon} {label}",
                 "size": "xxs", "color": color, "flex": 5, "weight": "bold"},
                {"type": "text", "text": str(cnt),
                 "size": "xxs", "color": "#CCCCCC", "flex": 2, "align": "end"},
            ],
        }

    bubbles = []
    for s in range(1, 5):
        count = stage_counts.get(s, 0)
        sub_rows = [_sub_row(*tup) for tup in SUBS[s]]

        # Footer button — Stage 2 gets dual buttons (full + Pivot Ready);
        # other stages get a single full-list button.
        full_cmd = f"{cmd_prefix}stage{s}"
        ready_cmd = f"{cmd_prefix}ready"
        footer_btns = [
            {"type": "button",
             "action": {"type": "message",
                        "label": f"ดู Stage {s} ({count})",
                        "text": full_cmd},
             "style": "primary", "color": STAGE_BG[s], "height": "sm"},
        ]
        if s == 2:
            ready_count = sub_counts.get("STAGE_2_PIVOT_READY", 0)
            footer_btns.append({
                "type": "button",
                "action": {"type": "message",
                           "label": f"🎯 Pivot Ready ({ready_count})",
                           "text": ready_cmd},
                "style": "secondary", "height": "sm", "margin": "xs",
            })

        # Header text — append scope label when set, e.g. "🟢 Stage 2 · SET100"
        header_label = f"{STAGE_ICON[s]} Stage {s}"
        if scope_label:
            header_label = f"{header_label} · {scope_label}"

        bubbles.append({
            "type": "bubble",
            "size": "kilo",
            "header": {
                "type": "box",
                "layout": "vertical",
                "contents": [
                    {"type": "box", "layout": "horizontal", "contents": [
                        {"type": "text", "text": header_label,
                         "weight": "bold", "size": "md", "color": "#FFFFFF",
                         "flex": 3, "wrap": True},
                        {"type": "text", "text": STAGE_VERB[s],
                         "size": "xxs", "color": "#FFD54F", "weight": "bold",
                         "flex": 2, "align": "end", "gravity": "center"},
                    ]},
                    {"type": "text", "text": STAGE_DESC[s],
                     "size": "xxs", "color": "#FFFFFF", "wrap": True},
                ],
                "backgroundColor": STAGE_BG[s],
                "paddingAll": "12px",
            },
            "body": {
                "type": "box",
                "layout": "vertical",
                "spacing": "xs",
                "contents": [
                    {"type": "text", "text": f"{count} หุ้น",
                     "size": "sm", "weight": "bold",
                     "color": STAGE_COLOR.get(s, "#333333")},
                    {"type": "separator", "margin": "xs"},
                    *sub_rows,
                ],
                "paddingAll": "10px",
            },
            "footer": {
                "type": "box",
                "layout": "vertical",
                "contents": footer_btns,
                "paddingAll": "6px",
            },
        })
    return {"type": "carousel", "contents": bubbles}


def build_stages_dashboard_card(
    signals: list,
    breadth: Optional[MarketBreadth] = None,
    *,
    scope_label: str = "",
    cmd_prefix: str = "",
) -> dict:
    """Single-bubble 11-row overview matrix — the post-2-layer-classifier
    'one screen, all states' dashboard. Triggered by `stages`,
    `set100 stages`, `set50 stages`, etc.

    Layout (per parent stage section): parent header (count + verb badge)
    followed by sub-stage rows (icon + label + count). Every row is
    tappable → its filter command. Color-coded per SUB_STAGE_COLOR.

    Scope params:
      • scope_label = "" (default, market-wide), "SET100", "SET50", "MAI".
        Shown in the header so the user knows which universe.
      • cmd_prefix  = "" / "set100 " / "set50 " / "mai ". Tappable rows
        emit `<prefix><cmd>` so taps stay inside the same universe.
    """
    from collections import Counter
    sub_counts = Counter(getattr(s, "sub_stage", "") or "" for s in (signals or []))

    stage_counts: dict[int, int] = {1: 0, 2: 0, 3: 0, 4: 0}
    if breadth:
        stage_counts = {
            1: getattr(breadth, "stage1_count", 0),
            2: getattr(breadth, "stage2_count", 0),
            3: getattr(breadth, "stage3_count", 0),
            4: getattr(breadth, "stage4_count", 0),
        }
    total = sum(stage_counts.values()) or len(signals or [])

    STAGE_VERB = {1: "WATCH", 2: "TRADE", 3: "TRIM", 4: "AVOID"}
    STAGE_BG = {1: "#7F8C8D", 2: "#1B5E20", 3: "#E65100", 4: "#B71C1C"}

    SECTIONS = [
        (1, "⚪ STAGE 1", [
            ("⚪", "Base",          "STAGE_1_BASE",         "base",         False),
            ("🌱", "Prep",          "STAGE_1_PREP",         "prep",         True),
        ]),
        (2, "🟢 STAGE 2", [
            ("🎯", "Pivot Ready ✨", "STAGE_2_PIVOT_READY",  "ready",        True),
            ("🚀", "Ignition",       "STAGE_2_IGNITION",     "ignition",     True),
            ("✅", "Markup",         "STAGE_2_MARKUP",       "markup",       False),
            ("👀", "Contraction",    "STAGE_2_CONTRACTION",  "contraction",  False),
            ("⚠",  "Overextended",   "STAGE_2_OVEREXTENDED", "overextended", False),
        ]),
        (3, "🟡 STAGE 3", [
            ("🟡", "Volatile",      "STAGE_3_VOLATILE",  "volatile", False),
            ("🟠", "Distribution",  "STAGE_3_DIST_DIST", "dist",     False),
        ]),
        (4, "🔴 STAGE 4", [
            ("🔴", "Breakdown",     "STAGE_4_BREAKDOWN", "breakdown", False),
            ("🔴", "Downtrend",     "STAGE_4_DOWNTREND", "downtrend", False),
        ]),
    ]

    def _sub_row(icon: str, label: str, sub_const: str, cmd_tail: str,
                 highlight: bool) -> dict:
        cnt = sub_counts.get(sub_const, 0)
        color = SUB_STAGE_COLOR.get(sub_const, "#7F8C8D")
        # Highlight (gold pop) for actionable rows: PREP, PIVOT_READY,
        # IGNITION. Surfaces the user's "what to do today" cohorts at
        # the top of each stage section.
        suffix = " ←" if highlight and cnt > 0 else ""
        cmd = f"{cmd_prefix}{cmd_tail}"
        return {
            "type": "box", "layout": "horizontal",
            "action": {"type": "message", "label": cmd_tail, "text": cmd},
            "paddingTop": "4px", "paddingBottom": "4px", "paddingStart": "12px",
            "contents": [
                {"type": "text", "text": f"{icon} {label}{suffix}",
                 "size": "xxs", "color": color, "flex": 6,
                 "weight": "bold" if highlight else "regular"},
                {"type": "text", "text": str(cnt),
                 "size": "xxs", "color": "#CCCCCC", "flex": 2, "align": "end",
                 "weight": "bold" if highlight else "regular"},
            ],
        }

    def _section_header(stage_int: int, label: str, count: int) -> list:
        cmd = f"{cmd_prefix}stage{stage_int}"
        return [
            {"type": "separator", "margin": "sm"},
            {"type": "box", "layout": "horizontal",
             "action": {"type": "message", "label": f"stage{stage_int}",
                        "text": cmd},
             "paddingTop": "8px", "paddingBottom": "4px",
             "contents": [
                {"type": "text", "text": label,
                 "size": "sm", "weight": "bold",
                 "color": STAGE_BG.get(stage_int, "#FFFFFF"),
                 "flex": 4},
                {"type": "text", "text": STAGE_VERB.get(stage_int, ""),
                 "size": "xxs", "color": "#FFD54F", "weight": "bold",
                 "flex": 2, "align": "end", "gravity": "center"},
                {"type": "text", "text": str(count),
                 "size": "sm", "weight": "bold", "color": "#FFFFFF",
                 "flex": 2, "align": "end"},
            ]},
        ]

    body_contents = [
        {"type": "text", "text": f"{total} stocks scanned",
         "size": "xxs", "color": "#7F8C8D", "align": "center"},
    ]
    for stage_int, label, subs in SECTIONS:
        body_contents.extend(_section_header(stage_int, label, stage_counts.get(stage_int, 0)))
        for tup in subs:
            body_contents.append(_sub_row(*tup))

    body_contents.extend([
        {"type": "separator", "margin": "md"},
        {"type": "text",
         "text": "← = actionable today · tap any row to filter",
         "size": "xxs", "color": "#7F8C8D", "wrap": True, "align": "center"},
    ])

    # Header — append scope label when set ("📊 State Distribution · SET100")
    title = "📊 State Distribution"
    if scope_label:
        title = f"{title} · {scope_label}"

    return {
        "type": "bubble",
        "size": "mega",
        "header": {
            "type": "box",
            "layout": "vertical",
            "contents": [
                {"type": "text", "text": title,
                 "weight": "bold", "size": "lg", "color": "#FFFFFF",
                 "wrap": True},
                {"type": "text", "text": "11 sub-stages · one screen",
                 "size": "xxs", "color": "#BBDDFF"},
            ],
            "backgroundColor": "#0D47A1",
            "paddingAll": "14px",
        },
        "body": {
            "type": "box",
            "layout": "vertical",
            "spacing": "none",
            "contents": body_contents,
            "paddingAll": "12px",
        },
    }


def build_pattern_overview_card(signals: list[StockSignal], breadth=None) -> dict:
    """Overview card listing all patterns with counts. Each row tappable → pattern stock list."""
    from collections import Counter
    s2 = [s for s in signals if s.stage == 2]
    counts = Counter(s.pattern for s in s2)
    # consolidating can be any stage
    consol = sum(1 for s in signals if s.pattern == "consolidating")

    PATTERNS = [
        ("breakout",      "🚀 Breakout",       "ทะลุแนวต้าน + Volume",        "#F39C12", "breakout"),
        ("ath_breakout",  "🏆 ATH Breakout",   "New All-Time High",           "#E74C3C", "ath"),
        ("vcp",           "🔍 VCP",            "Volatility Contraction",      "#2980B9", "vcp"),
        ("vcp_low_cheat", "🎯 VCP Low Cheat",  "เข้าใกล้ pivot ก่อน break",    "#1ABC9C", "vcp low cheat"),
        ("consolidating", "⚙️ Consolidating",  "Stage 2 สะสม รอสัญญาณ",     "#95A5A6", "consolidating"),
    ]

    rows: list = [
        {
            "type": "box", "layout": "horizontal", "paddingBottom": "4px",
            "contents": [
                {"type": "text", "text": "Pattern", "size": "xxs", "color": "#AAAAAA", "flex": 5},
                {"type": "text", "text": "Description", "size": "xxs", "color": "#AAAAAA", "flex": 5},
                {"type": "text", "text": "N", "size": "xxs", "color": "#AAAAAA", "flex": 2, "align": "end"},
            ],
        },
        {"type": "separator"},
    ]
    for key, label, desc, color, cmd in PATTERNS:
        cnt = consol if key == "consolidating" else counts.get(key, 0)
        rows.append({
            "type": "box", "layout": "horizontal",
            "paddingTop": "8px", "paddingBottom": "8px",
            "action": {"type": "message", "label": label[:20], "text": cmd},
            "contents": [
                {"type": "text", "text": label, "size": "xs", "weight": "bold", "color": color, "flex": 5},
                {"type": "text", "text": desc, "size": "xxs", "color": "#AAAAAA", "flex": 5, "wrap": True},
                {"type": "text", "text": str(cnt), "size": "sm", "weight": "bold", "color": color, "flex": 2, "align": "end"},
            ],
        })
        rows.append({"type": "separator"})

    return {
        "type": "bubble", "size": "mega",
        "header": {
            "type": "box", "layout": "vertical",
            "contents": [
                {"type": "text", "text": "📈 รูปแบบราคา (Patterns)", "size": "xs", "color": "#DDDDDD"},
                {"type": "text", "text": "แตะรูปแบบเพื่อดูรายชื่อหุ้น", "size": "xxs", "color": "#BBDDFF"},
            ],
            "backgroundColor": "#1A237E", "paddingAll": "14px",
        },
        "body": {
            "type": "box", "layout": "vertical", "spacing": "none",
            "contents": rows, "paddingAll": "14px",
        },
    }


def build_watchlist_stock_card(signal: StockSignal, fundamentals: dict) -> dict:
    """Deep insight card: Technical + Fundamental data for watchlist view."""
    pcolor = PATTERN_COLOR.get(signal.pattern, "#7F8C8D")
    pattern_label = PATTERN_LABEL.get(signal.pattern, signal.pattern)
    # Sub-stage primary classification (with backward-compat fallback to
    # STAGE_LABEL + weakening suffix for old Firestore docs).
    stage_label, _stage_color = _resolve_stage_label(signal)
    sub_stage = getattr(signal, "sub_stage", "") or ""
    sub_stage_action = SUB_STAGE_ACTION.get(sub_stage, "") if sub_stage else ""
    chg_color = _pct_color(signal.change_pct)
    chg_sign = "+" if signal.change_pct > 0 else ""

    ma_rows = []
    if signal.sma50:
        gap50 = (signal.close - signal.sma50) / signal.sma50 * 100
        ma_rows.append(_detail_row("SMA50", f"฿{signal.sma50:,.2f}", f"{gap50:+.1f}%", _pct_color(gap50)))
    if signal.sma200:
        gap200 = (signal.close - signal.sma200) / signal.sma200 * 100
        ma_rows.append(_detail_row("SMA200", f"฿{signal.sma200:,.2f}", f"{gap200:+.1f}%", _pct_color(gap200)))
    # Pivot row (same gesture as single-stock card).
    pivot = getattr(signal, "pivot_price", 0.0) or 0.0
    pstop = getattr(signal, "pivot_stop", 0.0) or 0.0
    if pivot > 0:
        gap_pivot = (signal.close - pivot) / pivot * 100
        ma_rows.append(_detail_row("🎯 Pivot", f"฿{pivot:,.2f}",
                                    f"{gap_pivot:+.1f}%", _pct_color(gap_pivot)))
        if pstop > 0:
            risk_pct = (pstop - signal.close) / signal.close * 100
            ma_rows.append(_detail_row("⛔ Stop", f"฿{pstop:,.2f}",
                                        f"{risk_pct:+.1f}%", "#E74C3C"))

    # Fundamental rows
    fund_rows = []
    pe = fundamentals.get("pe_ratio")
    if pe:
        fund_rows.append(_detail_row("P/E", f"{pe:.1f}x", "", "#7F8C8D"))
    pb = fundamentals.get("pb_ratio")
    if pb:
        fund_rows.append(_detail_row("P/B", f"{pb:.2f}x", "", "#7F8C8D"))
    div = fundamentals.get("dividend_yield")
    if div:
        fund_rows.append(_detail_row("Dividend", f"{div:.2f}%", "", "#27AE60"))
    mcap = fundamentals.get("market_cap_bn")
    if mcap:
        fund_rows.append(_detail_row("Mkt Cap", f"฿{mcap:.1f}Bn", "", "#7F8C8D"))
    eps = fundamentals.get("eps")
    if eps:
        fund_rows.append(_detail_row("EPS", f"฿{eps:.2f}", "", "#7F8C8D"))

    body_contents = [
        {"type": "box", "layout": "horizontal", "contents": [
            {"type": "text", "text": "ราคา", "size": "sm", "color": "#7F8C8D", "flex": 1},
            {"type": "text", "text": f"฿{signal.close:,.2f}", "size": "sm", "weight": "bold", "align": "end"},
        ]},
        {"type": "box", "layout": "horizontal", "contents": [
            {"type": "text", "text": "Volume Ratio", "size": "sm", "color": "#7F8C8D", "flex": 1},
            {"type": "text", "text": f"{signal.volume_ratio:.2f}x", "size": "sm", "weight": "bold", "align": "end",
             "color": "#F39C12" if signal.volume_ratio >= 1.5 else "#FFFFFF"},
        ]},
        {"type": "separator"},
        *ma_rows,
        {"type": "separator"},
        {"type": "box", "layout": "horizontal", "contents": [
            {"type": "text", "text": "Strength Score", "size": "sm", "color": "#7F8C8D", "flex": 1},
            {"type": "text", "text": f"{int(signal.strength_score)}/100", "size": "sm", "weight": "bold", "color": "#F39C12", "align": "end"},
        ]},
    ]

    # Risk / reward extra rows
    if getattr(signal, "trade_value_m", 0) > 0:
        body_contents.append({"type": "box", "layout": "horizontal", "contents": [
            {"type": "text", "text": "มูลค่าซื้อขาย", "size": "sm", "color": "#7F8C8D", "flex": 1},
            {"type": "text", "text": f"฿{signal.trade_value_m:.1f}M", "size": "sm", "weight": "bold", "align": "end"},
        ]})
    if getattr(signal, "pct_from_52w_high", 0) != 0:
        body_contents.append({"type": "box", "layout": "horizontal", "contents": [
            {"type": "text", "text": "ต่ำกว่า 52W High", "size": "sm", "color": "#7F8C8D", "flex": 1},
            {"type": "text", "text": f"{signal.pct_from_52w_high:.1f}%", "size": "sm", "weight": "bold", "align": "end",
             "color": "#27AE60" if signal.pct_from_52w_high >= -5 else "#E67E22"},
        ]})
    if getattr(signal, "stop_loss", 0) > 0:
        body_contents += [
            {"type": "separator"},
            {"type": "text", "text": "⚖️ Risk Management (ATR-based)", "size": "xxs", "color": "#F39C12", "weight": "bold"},
            _detail_row("Stop Loss", f"฿{signal.stop_loss:,.2f}",
                        f"-{(signal.close - signal.stop_loss) / signal.close * 100:.1f}%", "#E74C3C"),
            _detail_row("Target (2:1)", f"฿{signal.target_price:,.2f}",
                        f"+{(signal.target_price - signal.close) / signal.close * 100:.1f}%", "#27AE60"),
        ]
        # Margin tier (same gesture as the single-stock card).
        _mim2 = getattr(signal, "margin_im_pct", 0) or 0
        if _mim2:
            _lev2 = 100.0 / _mim2 if _mim2 > 0 else 1.0
            body_contents.append(
                _detail_row("Margin", f"IM{_mim2}%",
                            f"{_lev2:.2f}× leverage",
                            "#1ABC9C" if _mim2 <= 60 else "#F39C12")
            )
        else:
            body_contents.append(
                _detail_row("Margin", "Non-marginable", "100% cash", "#7F8C8D")
            )

    if fund_rows:
        body_contents += [
            {"type": "separator"},
            {"type": "text", "text": "Fundamentals", "size": "xs", "color": "#F39C12", "weight": "bold"},
            *fund_rows,
        ]

    return {
        "type": "bubble",
        "size": "mega",
        "header": {
            "type": "box",
            "layout": "vertical",
            "contents": [
                {"type": "box", "layout": "horizontal", "contents": [
                    {"type": "text", "text": f"📌 {signal.symbol}", "weight": "bold", "size": "xl", "color": "#FFFFFF", "flex": 1},
                    {"type": "text", "text": f"{chg_sign}{signal.change_pct:.2f}%", "size": "md", "color": chg_color, "align": "end", "weight": "bold"},
                ]},
                {"type": "box", "layout": "horizontal", "contents": [
                    {"type": "text", "text": stage_label, "size": "xs", "color": _stage_color, "flex": 1, "weight": "bold"},
                    {"type": "text", "text": pattern_label, "size": "xs", "color": pcolor, "weight": "bold", "align": "end"},
                ]},
                # Sub-stage recommendation row — same gesture as single-stock card.
                *(
                    [{"type": "text", "text": f"💡 {sub_stage_action}",
                      "size": "xxs", "color": "#FFD54F", "wrap": True,
                      "margin": "xs"}]
                    if sub_stage_action else []
                ),
            ],
            "backgroundColor": "#0D0D1A",
            "paddingAll": "16px",
        },
        "body": {
            "type": "box",
            "layout": "vertical",
            "spacing": "sm",
            "contents": body_contents,
            "paddingAll": "16px",
        },
        "footer": {
            "type": "box",
            "layout": "vertical",
            "spacing": "sm",
            "contents": [
                {"type": "button", "action": {"type": "uri", "label": "🔗 TradingView", "uri": signal.tradingview_url}, "style": "primary", "color": "#1565C0"},
                {"type": "box", "layout": "horizontal", "spacing": "sm", "contents": [
                    {"type": "button", "action": {"type": "message", "label": f"ⓘ {STAGE_LABEL.get(signal.stage, 'Stage')}", "text": f"explain stage{signal.stage}"}, "style": "secondary", "height": "sm", "flex": 1},
                    {"type": "button", "action": {"type": "message", "label": f"ⓘ {PATTERN_LABEL.get(signal.pattern, signal.pattern)}", "text": f"explain {signal.pattern}"}, "style": "secondary", "height": "sm", "flex": 1},
                ]},
            ],
            "paddingAll": "12px",
        },
    }


def build_explain_card(metric: str, explanation: str) -> dict:
    """Build a simple explanation bubble for a metric."""
    METRIC_ICONS = {
        "stage": "📊", "score": "💪", "vcp": "🔍",
        "breakout": "🚀", "ath_breakout": "🏆",
        "consolidating": "⚙️", "going_down": "📉",
    }
    icon = next((v for k, v in METRIC_ICONS.items() if k in metric), "ℹ️")
    return {
        "type": "bubble",
        "size": "mega",
        "header": {
            "type": "box",
            "layout": "vertical",
            "contents": [
                {"type": "text", "text": f"{icon} {metric.replace('_', ' ').title()}", "weight": "bold", "size": "lg", "color": "#FFFFFF"},
            ],
            "backgroundColor": "#1A237E",
            "paddingAll": "16px",
        },
        "body": {
            "type": "box",
            "layout": "vertical",
            "contents": [
                {"type": "text", "text": explanation, "size": "sm", "wrap": True, "color": "#333333"},
            ],
            "paddingAll": "16px",
        },
    }


def build_guide_carousel() -> dict:
    """5-bubble onboarding carousel covering every Signalix feature so a
    first-time user can scroll through and understand what's available:

      1. Quick Reference   — every text command grouped by category
      2. Global Assets     — 50 curated tickers + watchlist gesture
      3. Stage Analysis    — Minervini parent stages 1-4
      4. State Machine     — 9 sub-stages with recommendations + pivot
      5. Score & Volume    — how stocks are ranked

    Order: WHAT (commands) → adjacent context (globals near Thai market)
    → HOW each domain works (parent stages → sub-stages) → HOW ranking works.
    """
    # ── Bubble 1: Quick Reference ───────────────────────────────────
    # Compact section headers between command rows so the bubble reads
    # as several small categories instead of one long flat list.
    def _section(label: str, color: str = "#F39C12") -> dict:
        return {
            "type": "text", "text": label, "size": "xs",
            "weight": "bold", "color": color,
            "margin": "md",
        }

    quickref_bubble = {
        "type": "bubble",
        "size": "mega",
        "header": {
            "type": "box",
            "layout": "vertical",
            "contents": [
                {"type": "text", "text": "⚡ Quick Reference", "weight": "bold", "size": "lg", "color": "#FFFFFF"},
                {"type": "text", "text": "Tap any command to run it", "size": "xs", "color": "#7F8C8D"},
            ],
            "backgroundColor": "#0D0D1A",
            "paddingAll": "16px",
        },
        "body": {
            "type": "box",
            "layout": "vertical",
            "spacing": "none",
            "contents": [
                # Marginable is the actionable trading universe — promoted
                # to the top so traders see it first. Krungsri's list of
                # 321 SET stocks the broker accepts margin orders on.
                _section("💰 TRADING UNIVERSE (Marginable)", "#F1C40F"),
                _cmd_row("marginable", "Marginable breadth (all 321) 💰"),
                _cmd_row("marginable stage", "Marginable picker (4 stages)"),
                _cmd_row("marginable stages", "Marginable dashboard (11 rows)"),
                _cmd_row("marginable pivot", "Marginable pivot candidates 🎯"),
                _cmd_row("marginable ready", "Marginable Pivot Ready ✨"),
                _cmd_row("marginable ignition", "Marginable Ignition 🚀"),
                _cmd_row("margin50", "All IM50 stocks (2.0× leverage)"),

                _section("MARKET OVERVIEW"),
                _cmd_row("market", "SET Market Breadth"),
                _cmd_row("index", "All Indexes Snapshot"),
                _cmd_row("set50", "SET50 Breadth"),
                _cmd_row("set100", "SET100 Breadth"),
                _cmd_row("sector", "Sector Trends"),

                _section("ACTIONABLE BUY", "#27AE60"),
                _cmd_row("ready", "Stage 2 Pivot Ready 🎯"),
                _cmd_row("ignition", "Stage 2 Ignition (breakout) 🚀"),
                _cmd_row("contraction", "Stage 2 Contraction (pullback)"),
                _cmd_row("markup", "Stage 2 Markup (running)"),
                _cmd_row("prep", "Stage 1 Prep (watchlist)"),
                _cmd_row("pivot", "At Pivot Trigger 🎯"),

                _section("DEFENSE / EXIT", "#E67E22"),
                _cmd_row("volatile", "Stage 3 Volatile (take profit)"),
                _cmd_row("dist", "Stage 3 Distribution (defend)"),
                _cmd_row("breakdown", "Stage 4 Breakdown (cut loss)"),

                _section("LEGACY PATTERNS"),
                _cmd_row("breakout", "Confirmed Breakouts"),
                _cmd_row("ath", "ATH Breakout"),
                _cmd_row("vcp", "VCP Setups"),
                _cmd_row("weakening", "Stage 2 Weakening ⚠"),

                _section("GLOBAL & WATCHLIST", "#1ABC9C"),
                _cmd_row("global", "World/Crypto Snapshot 🌏"),
                _cmd_row("watchlist", "Your Watchlist"),

                {"type": "separator", "margin": "md"},
                {"type": "text",
                 "text": "💡 Type any ticker (ADVANC, BTC, SPX, GOOG…) for instant detail",
                 "size": "xxs", "color": "#7F8C8D", "wrap": True, "margin": "sm"},
            ],
            "paddingAll": "16px",
        },
    }

    _stage_hero_url = PATTERN_IMAGES.get("stage_cycle", "")
    stage_bubble = {
        "type": "bubble",
        "size": "mega",
        **( {"hero": {"type": "image", "url": _stage_hero_url, "size": "full",
                      "aspectRatio": "20:13", "aspectMode": "cover",
                      "action": {"type": "message", "text": "stage2"}}}
            if _stage_hero_url else {} ),
        "header": {
            "type": "box",
            "layout": "vertical",
            "contents": [
                {"type": "text", "text": "📊 Stage Analysis", "weight": "bold", "size": "lg", "color": "#FFFFFF"},
                {"type": "text", "text": "Parent stages 1-4 (Minervini)", "size": "xxs", "color": "#7F8C8D"},
            ],
            "backgroundColor": "#1A237E",
            "paddingAll": "16px",
        },
        "body": {
            "type": "box",
            "layout": "vertical",
            "spacing": "sm",
            "contents": [
                {"type": "text", "text": "วิเคราะห์ตาม SMA50/150/200", "size": "xs", "color": "#7F8C8D", "wrap": True},
                {"type": "separator"},
                _guide_row("⚪ Stage 1", "Basing — สะสมตัว รอ breakout", "#95A5A6", cmd="stage1"),
                _guide_row("🟢 Stage 2", "Uptrend ✅ — โซนซื้อที่ดีที่สุด", "#27AE60", cmd="stage2"),
                _guide_row("🟡 Stage 3", "Topping ⚠️ — ระวัง smart money ขาย", "#E67E22", cmd="stage3"),
                _guide_row("🔴 Stage 4", "Downtrend ❌ — หลีกเลี่ยง", "#E74C3C", cmd="stage4"),
                {"type": "separator"},
                {"type": "text", "text": "Stage 2 เงื่อนไข:", "weight": "bold", "size": "xs", "color": "#27AE60"},
                {"type": "text", "text": "ราคา > SMA150 > SMA200\nSMA200 กำลังขึ้น\nราคา ≥ 52W low × 1.25\nราคา ≥ 52W high × 0.75", "size": "xxs", "color": "#555555", "wrap": True},
                {"type": "separator", "margin": "md"},
                {"type": "text", "text": "🔄 ดูสับ-สเตจทั้ง 9 รัฐ พร้อมคำแนะนำ →", "size": "xs", "color": "#1ABC9C", "wrap": True, "weight": "bold"},
                {"type": "text", "text": "เลื่อนไปการ์ด State Machine ถัดไป", "size": "xxs", "color": "#7F8C8D"},
            ],
            "paddingAll": "16px",
        },
    }

    # ── Bubble 4: State Machine (replaces former Pattern Guide) ─────
    # Surfaces the 9-state finite state machine with prescriptive
    # recommendations per state. Each row is tappable → filter command.
    # Source: SUB_STAGE_LABEL / SUB_STAGE_ACTION / SUB_STAGE_COLOR maps.
    state_machine_bubble = {
        "type": "bubble",
        "size": "mega",
        "header": {
            "type": "box",
            "layout": "vertical",
            "contents": [
                {"type": "text", "text": "🔄 State Machine", "weight": "bold", "size": "lg", "color": "#FFFFFF"},
                {"type": "text", "text": "9 sub-stages · tap to filter", "size": "xxs", "color": "#B2DFDB"},
            ],
            "backgroundColor": "#0D47A1",
            "paddingAll": "16px",
        },
        "body": {
            "type": "box",
            "layout": "vertical",
            "spacing": "sm",
            "contents": [
                {"type": "text", "text": "Each state → 'what to do' recommendation",
                 "size": "xs", "color": "#7F8C8D", "wrap": True},
                {"type": "separator"},
                _guide_row("⚪ Stage 1 · Base",       "Ignore — no setup yet",            SUB_STAGE_COLOR["STAGE_1_BASE"],         cmd="base"),
                _guide_row("🌱 Stage 1 · Prep",       "Watchlist — pre-Stage-2 🎯",       SUB_STAGE_COLOR["STAGE_1_PREP"],         cmd="prep"),
                # Stage 2 in priority order (OVEREXTENDED top — wins ties)
                _guide_row("⚠ Stage 2 · Overextend",  "WARNING — no new buys",            SUB_STAGE_COLOR["STAGE_2_OVEREXTENDED"], cmd="overextended"),
                _guide_row("🎯 Stage 2 · Pivot Ready", "ACTIONABLE — pivot trigger ✨",    SUB_STAGE_COLOR["STAGE_2_PIVOT_READY"],  cmd="ready"),
                _guide_row("🚀 Stage 2 · Ignition",   "TRADABLE — fresh momentum",        SUB_STAGE_COLOR["STAGE_2_IGNITION"],     cmd="ignition"),
                _guide_row("👀 Stage 2 · Contraction","WATCH — base building",            SUB_STAGE_COLOR["STAGE_2_CONTRACTION"],  cmd="contraction"),
                _guide_row("✅ Stage 2 · Markup",     "HOLD — let profits run",           SUB_STAGE_COLOR["STAGE_2_MARKUP"],       cmd="markup"),
                _guide_row("🟡 Stage 3 · Volatile",   "Take Profit / Tighten Stop",       SUB_STAGE_COLOR["STAGE_3_VOLATILE"],     cmd="volatile"),
                _guide_row("🟠 Stage 3 · Distribu'n", "Defend — no new buys",             SUB_STAGE_COLOR["STAGE_3_DIST_DIST"],    cmd="dist"),
                _guide_row("🔴 Stage 4 · Breakdown",  "Cut Loss — exit",                  SUB_STAGE_COLOR["STAGE_4_BREAKDOWN"],    cmd="breakdown"),
                _guide_row("🔴 Stage 4 · Downtrend",  "Delete — remove from watch",       SUB_STAGE_COLOR["STAGE_4_DOWNTREND"],    cmd="downtrend"),
                {"type": "separator", "margin": "md"},
                {"type": "text", "text": "🎯 Pivot point", "weight": "bold", "size": "xs", "color": "#F39C12"},
                {"type": "text",
                 "text": "Stage 1 PREP + ทุก Stage 2 sub-stage จะมี pivot price (buy trigger) คำนวณจาก high สูงสุด 15 แท่งล่าสุด + stop จาก low ต่ำสุด 10 แท่ง",
                 "size": "xxs", "color": "#555555", "wrap": True},
                _guide_row("🎯 At Pivot", "หุ้นที่ใกล้/ถึง trigger ทุกรัฐ", "#F39C12", cmd="pivot"),
                {"type": "separator", "margin": "md"},
                {"type": "text", "text": "💰 Margin tiers (Krungsri)", "weight": "bold", "size": "xs", "color": "#F1C40F"},
                {"type": "text",
                 "text": "IM% = Initial Margin needed. ยิ่งต่ำ ยิ่ง leverage มาก:\nIM50 → 2.00× | IM60 → 1.67× | IM70 → 1.43× | IM80 → 1.25×",
                 "size": "xxs", "color": "#555555", "wrap": True},
                _guide_row("💰 Marginable", "Universe ที่กู้ได้ (321 ตัว)",  "#F1C40F", cmd="marginable"),
                _guide_row("🎯 Marginable Pivot", "Setup เฉพาะที่กู้ได้",     "#F1C40F", cmd="marginable pivot"),
                _guide_row("✨ Marginable Ready", "Pivot Ready เฉพาะที่กู้ได้", "#F1C40F", cmd="marginable ready"),
                {"type": "text", "text": "💡 sub_stage = primary classification. ⚠ stage_weakening = legacy modifier (close < SMA50 ใน Stage 2 — type 'weakening')",
                 "size": "xxs", "color": "#7F8C8D", "wrap": True, "margin": "sm"},
            ],
            "paddingAll": "16px",
        },
    }

    # ── Bubble 4: Global Assets ─────────────────────────────────────
    # 50 curated assets across 8 sections — Thai retail's most-watched
    # global tickers. Each section has 1-2 example codes that tap-fire
    # the detail card so users learn the gesture.
    global_bubble = {
        "type": "bubble",
        "size": "mega",
        "header": {
            "type": "box",
            "layout": "vertical",
            "contents": [
                {"type": "text", "text": "🌏 Global Assets (50)", "weight": "bold", "size": "lg", "color": "#FFFFFF"},
                {"type": "text", "text": "Indexes · FX · Commodities · ETFs · Stocks · Crypto", "size": "xxs", "color": "#B2DFDB", "wrap": True},
            ],
            "backgroundColor": "#006064",
            "paddingAll": "16px",
        },
        "body": {
            "type": "box",
            "layout": "vertical",
            "spacing": "sm",
            "contents": [
                {"type": "text", "text": "พิมพ์ 'global' เพื่อดูทั้งหมดเป็นกลุ่ม",
                 "size": "xs", "color": "#7F8C8D", "wrap": True},
                {"type": "separator"},

                {"type": "text", "text": "🇺🇸 US Indexes", "weight": "bold", "size": "xs", "color": "#1A237E"},
                _guide_row("SPX · NDX · DJI", "S&P / Nasdaq 100 / Dow", "#1A237E", cmd="NDX"),
                _guide_row("RUT · VIX", "Small caps / Fear gauge", "#1A237E", cmd="VIX"),

                {"type": "text", "text": "🌏 Asia Pacific", "weight": "bold", "size": "xs", "color": "#1A237E", "margin": "sm"},
                _guide_row("TWII · KOSPI · NI225", "Taiwan / Korea / Japan", "#1A237E", cmd="TWII"),
                _guide_row("HSI · SSE · NIFTY · STI · JKSE", "HK / China / India / SG / ID", "#1A237E", cmd="HSI"),

                {"type": "text", "text": "💱 FX & Macro", "weight": "bold", "size": "xs", "color": "#4527A0", "margin": "sm"},
                _guide_row("USDTHB · DXY", "USD/THB · Dollar Index", "#4527A0", cmd="USDTHB"),
                _guide_row("USDJPY · USDCNY", "Yen / Yuan", "#4527A0", cmd="USDJPY"),

                {"type": "text", "text": "🛢 Commodities", "weight": "bold", "size": "xs", "color": "#5D4037", "margin": "sm"},
                _guide_row("GOLD · OIL · COPPER · NATGAS", "Futures (drives PTT/SET energy)", "#5D4037", cmd="GOLD"),

                {"type": "text", "text": "📈 ETFs", "weight": "bold", "size": "xs", "color": "#006064", "margin": "sm"},
                _guide_row("QQQ · SPY · VOO · SMH", "Nasdaq / S&P / Semis", "#006064", cmd="SMH"),
                _guide_row("GLD · FXI · EWY · INDA", "Gold / China / Korea / India", "#006064", cmd="GLD"),

                {"type": "text", "text": "🏢 US Mega-cap", "weight": "bold", "size": "xs", "color": "#0D47A1", "margin": "sm"},
                _guide_row("NVDA · AAPL · MSFT · GOOG", "Top tech mega-caps", "#0D47A1", cmd="NVDA"),
                _guide_row("META · TSLA · AMZN · BRK-B", "Meta / Tesla / Amazon / Berkshire", "#0D47A1", cmd="AMZN"),

                {"type": "text", "text": "🏢 Themes", "weight": "bold", "size": "xs", "color": "#0D47A1", "margin": "sm"},
                _guide_row("TSM · AMD · AVGO", "Semis (DELTA proxies)", "#0D47A1", cmd="TSM"),
                _guide_row("JPM · V · GEV · NFLX", "Banks / Payments / Energy / Media", "#0D47A1", cmd="JPM"),

                {"type": "text", "text": "₿ Crypto", "weight": "bold", "size": "xs", "color": "#F7931A", "margin": "sm"},
                _guide_row("BTC · ETH · SOL", "Top 3 by mcap", "#F7931A", cmd="BTC"),
                _guide_row("BNB · XRP", "Binance / Ripple", "#F7931A", cmd="BNB"),

                {"type": "separator"},
                {"type": "text",
                 "text": "💡 Tap any code → detail card.\n'add BTC' / 'add USDTHB' → watchlist.\n'global' → grouped snapshot.",
                 "size": "xxs", "color": "#7F8C8D", "wrap": True, "margin": "sm"},
            ],
            "paddingAll": "16px",
        },
    }

    score_vol_bubble = {
        "type": "bubble",
        "size": "mega",
        "header": {
            "type": "box",
            "layout": "vertical",
            "contents": [{"type": "text", "text": "💪 Score & Volume", "weight": "bold", "size": "lg", "color": "#FFFFFF"}],
            "backgroundColor": "#1B5E20",
            "paddingAll": "16px",
        },
        "body": {
            "type": "box",
            "layout": "vertical",
            "spacing": "sm",
            "contents": [
                {"type": "text", "text": "Strength Score (0–100)", "weight": "bold", "size": "sm", "color": "#F39C12"},
                {"type": "text", "text": "By sub-stage (primary):", "size": "xxs", "color": "#7F8C8D"},
                _guide_row("🎯 Stage 2 · Pivot Ready", "+65 คะแนน  (actionable trigger)", SUB_STAGE_COLOR["STAGE_2_PIVOT_READY"]),
                _guide_row("🚀 Stage 2 · Ignition",    "+60 คะแนน  (fresh momentum)",     SUB_STAGE_COLOR["STAGE_2_IGNITION"]),
                _guide_row("✅ Stage 2 · Markup",      "+50 คะแนน  (riding short MAs)",   SUB_STAGE_COLOR["STAGE_2_MARKUP"]),
                _guide_row("👀 Stage 2 · Contraction", "+45 คะแนน  (base building)",      SUB_STAGE_COLOR["STAGE_2_CONTRACTION"]),
                _guide_row("🌱 Stage 1 · Prep",        "+30 คะแนน  (watchlist)",          SUB_STAGE_COLOR["STAGE_1_PREP"]),
                _guide_row("🟡 Stage 3 · Volatile",    "+20 คะแนน  (defensive)",          SUB_STAGE_COLOR["STAGE_3_VOLATILE"]),
                _guide_row("⚠ Stage 2 · Overextend",  "+10 คะแนน  (warning — deprio)",    SUB_STAGE_COLOR["STAGE_2_OVEREXTENDED"]),
                _guide_row("⚪ Stage 1 · Base",        "+10 คะแนน",                       SUB_STAGE_COLOR["STAGE_1_BASE"]),
                _guide_row("🟠 Stage 3 · Distrib'n",  "+5 คะแนน",                        SUB_STAGE_COLOR["STAGE_3_DIST_DIST"]),
                _guide_row("🔴 Stage 4 · ทุกแบบ",     "+0 คะแนน",                        SUB_STAGE_COLOR["STAGE_4_DOWNTREND"]),
                {"type": "separator", "margin": "md"},
                {"type": "text", "text": "+ บวกเพิ่ม:", "weight": "bold", "size": "xs", "color": "#F39C12"},
                _guide_row("Volume bonus",  "สูงสุด +15 คะแนน",  "#E67E22"),
                _guide_row("52W proximity", "สูงสุด +20 คะแนน",  "#9B59B6"),
                {"type": "separator"},
                {"type": "text", "text": "Volume Ratio", "weight": "bold", "size": "sm", "color": "#F39C12"},
                {"type": "text", "text": "= Volume วันนี้ ÷ ค่าเฉลี่ย 20 วัน\n1.0x ปกติ  |  1.4x+ สูง  |  2.0x+ สูงมาก", "size": "xxs", "color": "#555555", "wrap": True},
            ],
            "paddingAll": "16px",
        },
    }

    # Bubble order: Quick Reference → Global → Stage Analysis → State
    # Machine → Score. Stage Analysis explains parent stages (1-4); State
    # Machine drills into the 9-state taxonomy that REPLACED the former
    # Pattern Guide (per the FSM refactor — sub_stage is now the primary
    # classification; pattern field is auto-derived).
    return {"type": "carousel", "contents": [
        quickref_bubble, global_bubble, stage_bubble, state_machine_bubble, score_vol_bubble,
    ]}


def build_stage_cycle_card() -> dict:
    """Comprehensive Minervini 4-stage cycle card with sub-stage breakdown.

    Each parent-stage section shows the parent stage row (tappable to
    that stage's filter) followed by its sub-stages (each tappable to
    its sub-stage filter) — surfaces the full 9-state taxonomy in a
    natural Stage 1 → 2 → 3 → 4 reading order.
    """
    def _parent_row(icon: str, stage_name: str, color: str, detail: str,
                    badge: str, cmd: str) -> list:
        return [
            {
                "type": "box",
                "layout": "horizontal",
                "action": {"type": "message", "label": stage_name, "text": cmd},
                "contents": [
                    {"type": "text", "text": icon, "size": "sm", "flex": 1, "gravity": "center"},
                    {"type": "text", "text": stage_name, "size": "sm", "weight": "bold", "color": color, "flex": 3},
                    {"type": "text", "text": badge, "size": "xxs", "color": "#7F8C8D", "flex": 3, "align": "end", "wrap": True},
                ],
            },
            {"type": "text", "text": detail, "size": "xxs", "color": "#555555", "wrap": True, "margin": "xs"},
        ]

    def _sub_row(label: str, action: str, color: str, cmd: str) -> dict:
        return {
            "type": "box", "layout": "horizontal",
            "action": {"type": "message", "label": label, "text": cmd},
            "paddingStart": "12px", "margin": "xs",
            "contents": [
                {"type": "text", "text": label, "size": "xxs", "color": color, "flex": 4, "weight": "bold"},
                {"type": "text", "text": action, "size": "xxs", "color": "#555555", "flex": 5, "align": "end", "wrap": True},
            ],
        }

    bubble: dict = {
        "type": "bubble",
        "size": "giga",
        "header": {
            "type": "box",
            "layout": "vertical",
            "contents": [
                {"type": "text", "text": "📊 Minervini 4-Stage Cycle", "weight": "bold", "size": "lg", "color": "#FFFFFF"},
                {"type": "text", "text": "Parent stage + 9 sub-stages · tap any row", "size": "xs", "color": "#BBDDFF"},
            ],
            "backgroundColor": "#1A237E",
            "paddingAll": "16px",
        },
        "body": {
            "type": "box",
            "layout": "vertical",
            "spacing": "sm",
            "contents": [
                {"type": "text", "text": "Cycle: 1 → 2 → 3 → 4 → 1", "size": "xs", "color": "#7F8C8D", "align": "center"},
                {"type": "separator"},

                *_parent_row("⚪", "Stage 1 – Basing", "#95A5A6",
                    "SMA200 flattening, price consolidating.\nWait for Stage 2 entry before buying.",
                    "Wait", "stage1"),
                _sub_row("⚪ Base",  "Ignore — no setup yet",     SUB_STAGE_COLOR["STAGE_1_BASE"], cmd="base"),
                _sub_row("🌱 Prep", "Watchlist — pre-Stage 2 🎯", SUB_STAGE_COLOR["STAGE_1_PREP"], cmd="prep"),
                {"type": "separator", "margin": "md"},

                *_parent_row("🟢", "Stage 2 – Uptrend", "#27AE60",
                    "SMA50 > SMA200 + Price > SMA200 + ROC(SMA200) > 0\n"
                    "+ 52W proximity gates. Five sub-stages below — "
                    "OVEREXTENDED wins ties (defensive), then most actionable.",
                    "Buy/Hold", "stage2"),
                _sub_row("⚠ Overextended", "WARNING — no new buys",     SUB_STAGE_COLOR["STAGE_2_OVEREXTENDED"], cmd="overextended"),
                _sub_row("🎯 Pivot Ready", "ACTIONABLE — pivot trigger ✨", SUB_STAGE_COLOR["STAGE_2_PIVOT_READY"], cmd="ready"),
                _sub_row("🚀 Ignition",    "TRADABLE — fresh momentum",  SUB_STAGE_COLOR["STAGE_2_IGNITION"],    cmd="ignition"),
                _sub_row("👀 Contraction", "WATCH — base building",      SUB_STAGE_COLOR["STAGE_2_CONTRACTION"], cmd="contraction"),
                _sub_row("✅ Markup",      "HOLD — let profits run",     SUB_STAGE_COLOR["STAGE_2_MARKUP"],      cmd="markup"),
                {"type": "separator", "margin": "md"},

                *_parent_row("🟡", "Stage 3 – Distribution", "#E67E22",
                    "Smart money selling, price breaks SMA150.\nReduce position, no new buys.",
                    "Trim", "stage3"),
                _sub_row("🟡 Volatile",     "Take Profit / Tighten Stop", SUB_STAGE_COLOR["STAGE_3_VOLATILE"],  cmd="volatile"),
                _sub_row("🟠 Distribution", "Defend — no new buys",       SUB_STAGE_COLOR["STAGE_3_DIST_DIST"], cmd="dist"),
                {"type": "separator", "margin": "md"},

                *_parent_row("🔴", "Stage 4 – Downtrend", "#E74C3C",
                    "Price below declining SMA150 & SMA200.\nSell remaining; wait for Stage 1 base before re-entry.",
                    "Sell/Avoid", "stage4"),
                _sub_row("🔴 Breakdown", "Cut Loss — exit",            SUB_STAGE_COLOR["STAGE_4_BREAKDOWN"], cmd="breakdown"),
                _sub_row("🔴 Downtrend", "Delete — remove from watch", SUB_STAGE_COLOR["STAGE_4_DOWNTREND"], cmd="downtrend"),
                {"type": "separator", "margin": "md"},

                {"type": "text", "text": "🎯 Pivot Point", "weight": "bold", "size": "xs", "color": "#F39C12"},
                {"type": "text",
                 "text": "Stage 1 PREP + ทุก Stage 2 sub-stage จะมี pivot price (15-bar high) + stop (10-bar low) — type 'pivot' to see candidates.",
                 "size": "xxs", "color": "#555555", "wrap": True},
                {"type": "text", "text": "Risk Rule: Cut loss immediately at -7-8% from entry, or at pivot_stop. Never average down in Stage 4.",
                 "size": "xxs", "color": "#E74C3C", "wrap": True, "margin": "sm"},
            ],
            "paddingAll": "16px",
        },
    }
    hero_url = PATTERN_IMAGES.get("stage_cycle", "")
    if hero_url:
        bubble["hero"] = {
            "type": "image",
            "url": hero_url,
            "size": "full",
            "aspectMode": "cover",
            "aspectRatio": "20:13",
        }
    return bubble


def build_pivot_explainer_card() -> dict:
    """Rich explanation of the pivot-point system: what it is, how it's
    computed, which sub-stages get one, and how to use it. Shown when
    user types 'explain pivot' or taps a Pivot row in another card.
    """
    return {
        "type": "bubble",
        "size": "mega",
        "header": {
            "type": "box",
            "layout": "vertical",
            "contents": [
                {"type": "text", "text": "🎯 Pivot Point", "weight": "bold", "size": "lg", "color": "#FFFFFF"},
                {"type": "text", "text": "Buy trigger + setup invalidation stop", "size": "xxs", "color": "#FFD54F"},
            ],
            "backgroundColor": "#F39C12",
            "paddingAll": "16px",
        },
        "body": {
            "type": "box",
            "layout": "vertical",
            "spacing": "sm",
            "contents": [
                {"type": "text", "text": "What it is", "weight": "bold", "size": "sm", "color": "#F39C12"},
                {"type": "text",
                 "text": "Pivot = local resistance (15-bar high). Buy when price closes ABOVE pivot — that's the breakout trigger.\nStop = recent setup floor (10-bar low). If price falls back below stop, the setup is invalidated; cut loss.",
                 "size": "xxs", "color": "#555555", "wrap": True},
                {"type": "separator", "margin": "md"},

                {"type": "text", "text": "Who gets a pivot (5 actionable states)", "weight": "bold", "size": "sm", "color": "#F39C12"},
                _guide_row("🌱 Stage 1 · Prep",       "Loading toward Stage 2",          SUB_STAGE_COLOR["STAGE_1_PREP"],         cmd="prep"),
                _guide_row("🚀 Stage 2 · Ignition",   "Fresh momentum kickoff",          SUB_STAGE_COLOR["STAGE_2_IGNITION"],     cmd="ignition"),
                _guide_row("👀 Stage 2 · Contraction","Base building — pivot forming",   SUB_STAGE_COLOR["STAGE_2_CONTRACTION"],  cmd="contraction"),
                _guide_row("🎯 Stage 2 · Pivot Ready","ACTIONABLE — trigger active ✨",   SUB_STAGE_COLOR["STAGE_2_PIVOT_READY"],  cmd="ready"),
                _guide_row("✅ Stage 2 · Markup",     "Riding short MAs — re-entry",     SUB_STAGE_COLOR["STAGE_2_MARKUP"],       cmd="markup"),
                {"type": "text", "text": "⚠ OVEREXTENDED + Stage 3, 4 — no pivot (warning / defensive / downtrend zones).",
                 "size": "xxs", "color": "#7F8C8D", "wrap": True},
                {"type": "separator", "margin": "md"},

                {"type": "text", "text": "How to use it", "weight": "bold", "size": "sm", "color": "#F39C12"},
                {"type": "text",
                 "text": "1. Type 'pivot' → list of all candidates sorted by closeness to pivot.\n"
                         "2. Stocks at gap=+0.00% are AT the trigger right now (breaking out).\n"
                         "3. Buy when close > pivot (intraday wicks alone don't count).\n"
                         "4. Stop loss = pivot_stop (or your -1.5×ATR — whichever fits).\n"
                         "5. Target = R:R ≥ 2:1 vs distance to stop.",
                 "size": "xxs", "color": "#555555", "wrap": True},
                {"type": "separator", "margin": "md"},

                _guide_row("🎯 Show pivot list", "หุ้นที่ใกล้/ถึง trigger", "#F39C12", cmd="pivot"),
                {"type": "text", "text": "💡 Pivot is a price-only signal — no volume gate. Combine with breakout/early filters for the highest-conviction setups.",
                 "size": "xxs", "color": "#1ABC9C", "wrap": True, "margin": "sm"},
            ],
            "paddingAll": "16px",
        },
    }


def build_pattern_detail_card(pattern: str) -> dict:
    """Rich explanation card for a specific trading pattern with entry/stop/target rules."""
    PATTERN_DETAILS = {
        "breakout": {
            "icon": "🚀", "color": "#1B5E20",
            "title": "Breakout Pattern",
            "desc": "ราคาปิดสูงกว่า resistance สูงสุด 52 สัปดาห์\nพร้อม Volume สูงกว่าค่าเฉลี่ย 20 วัน ≥ 1.4x",
            "entry": "ซื้อเมื่อราคาปิดเหนือ 52-week high บน Volume สูง",
            "stop": "ต่ำกว่า Breakout level หรือ ATR × 1.5",
            "target": "R:R 2:1 ขึ้นไป ใช้ Trailing Stop หลัง breakout",
            "note": "Breakout ที่ดี: Stage 2 + Volume ≥ 1.5x + ไม่มี overhead supply",
        },
        "ath_breakout": {
            "icon": "🏆", "color": "#E65100",
            "title": "ATH Breakout",
            "desc": "Breakout ที่ราคาทำ All-Time High ใหม่\nไม่มี overhead supply เลย — หุ้นแข็งแกร่งที่สุด",
            "entry": "ซื้อเมื่อราคาปิดสูงกว่า ATH เดิม บน Volume สูง",
            "stop": "ATR × 1.5 หรือ -7% จาก entry",
            "target": "ไม่มี ceiling — ใช้ Trailing Stop (ATR × 2-3)",
            "note": "ATH Breakout = แนวโน้มแข็งแกร่งที่สุด มักวิ่งต่อได้ดี",
        },
        "vcp": {
            "icon": "🔍", "color": "#0D47A1",
            "title": "VCP – Volatility Contraction",
            "desc": "ความผันผวนหดตัวเรื่อยๆ 3+ ครั้ง พร้อม Volume แห้ง\nรูปแบบ: ลง 15% → 10% → 6% → 3%",
            "entry": "ซื้อเมื่อราคา breakout จาก contraction สุดท้าย + Volume ขึ้น",
            "stop": "ต่ำกว่า Low ของ VCP contraction สุดท้าย",
            "target": "R:R 3:1 ขึ้นไป (VCP มักให้ risk ต่ำมาก)",
            "note": "VCP ที่ดี: Stage 2 เท่านั้น, Volume แห้งก่อน breakout",
        },
        "vcp_low_cheat": {
            "icon": "🎯", "color": "#00695C",
            "title": "VCP Low Cheat",
            "desc": "Entry แบบ aggressive ใน VCP\nราคาอยู่ที่ Low ของ contraction สุดท้าย + Volume แห้งมาก",
            "entry": "ซื้อที่ Low ของ VCP + ตั้ง Stop แคบมาก (<3%)",
            "stop": "ต่ำกว่า Low ของ contraction สุดท้าย",
            "target": "R:R 4:1 หรือมากกว่า (เพราะ Risk น้อยมาก)",
            "note": "High risk/reward แต่ต้องการ discipline สูง",
        },
        "consolidating": {
            "icon": "⚙️", "color": "#4A148C",
            "title": "Consolidating",
            "desc": "หุ้นพักตัวในกรอบแคบ ATR กำลังหด\nกำลัง build base สำหรับ breakout ต่อไป",
            "entry": "ยังไม่ซื้อ รอ breakout จาก consolidation",
            "stop": "N/A — ยังไม่ entry",
            "target": "ดูหลัง breakout เกิดขึ้น",
            "note": "Base ยิ่งยาว breakout ยิ่งแรง (Tight base = good setup)",
        },
    }
    info = PATTERN_DETAILS.get(pattern, {
        "icon": "📉", "color": "#B71C1C", "title": pattern,
        "desc": "Pattern ขาลง ไม่แนะนำให้ซื้อ",
        "entry": "หลีกเลี่ยง", "stop": "N/A", "target": "N/A", "note": "",
    })

    rows = [
        {"type": "text", "text": info["desc"], "size": "sm", "wrap": True, "color": "#333333"},
        {"type": "separator"},
        {"type": "text", "text": "🎯 Entry", "size": "xs", "weight": "bold", "color": "#27AE60"},
        {"type": "text", "text": info["entry"], "size": "xs", "wrap": True, "color": "#555555"},
        {"type": "text", "text": "🛑 Stop Loss", "size": "xs", "weight": "bold", "color": "#E74C3C"},
        {"type": "text", "text": info["stop"], "size": "xs", "wrap": True, "color": "#555555"},
        {"type": "text", "text": "💰 Target", "size": "xs", "weight": "bold", "color": "#F39C12"},
        {"type": "text", "text": info["target"], "size": "xs", "wrap": True, "color": "#555555"},
    ]
    if info["note"]:
        rows += [
            {"type": "separator"},
            {"type": "text", "text": f"💡 {info['note']}", "size": "xxs", "wrap": True, "color": "#7F8C8D"},
        ]

    bubble: dict = {
        "type": "bubble",
        "size": "mega",
        "header": {
            "type": "box",
            "layout": "vertical",
            "action": {"type": "message", "label": info["title"], "text": pattern},
            "contents": [
                {"type": "text", "text": f"{info['icon']} {info['title']}", "weight": "bold", "size": "lg", "color": "#FFFFFF"},
                {"type": "text", "text": "Tap to see matching stocks", "size": "xxs", "color": "#FFFFFF", "margin": "xs"},
            ],
            "backgroundColor": info["color"],
            "paddingAll": "16px",
        },
        "body": {
            "type": "box",
            "layout": "vertical",
            "spacing": "sm",
            "contents": rows,
            "paddingAll": "16px",
        },
    }
    hero_url = PATTERN_IMAGES.get(pattern, "")
    if hero_url:
        bubble["hero"] = {
            "type": "image",
            "url": hero_url,
            "size": "full",
            "aspectMode": "cover",
            "aspectRatio": "20:13",
        }
    return bubble


def _guide_row(label: str, desc: str, color: str = "#333333", cmd: str = "") -> dict:
    box: dict = {
        "type": "box",
        "layout": "horizontal",
        "paddingTop": "4px", "paddingBottom": "4px",
        "contents": [
            {"type": "text", "text": label, "size": "xs", "color": color, "weight": "bold", "flex": 2},
            {"type": "text", "text": desc, "size": "xs", "color": "#555555", "flex": 3, "wrap": True},
        ],
    }
    if cmd:
        box["action"] = {"type": "message", "label": label[:20], "text": cmd}
    return box


def build_welcome_card(display_name: str) -> dict:
    """Welcome message for new LINE followers."""
    return {
        "type": "bubble",
        "size": "mega",
        "header": {
            "type": "box",
            "layout": "vertical",
            "contents": [
                {"type": "text", "text": "🎉 ยินดีต้อนรับสู่", "size": "sm", "color": "#FFFFFF"},
                {"type": "text", "text": "Signalix", "weight": "bold", "size": "xxl", "color": "#F39C12"},
                {"type": "text", "text": "Thai SET Stock Signal", "size": "xs", "color": "#CCCCCC"},
            ],
            "backgroundColor": "#0D0D1A",
            "paddingAll": "20px",
        },
        "body": {
            "type": "box",
            "layout": "vertical",
            "spacing": "md",
            "contents": [
                {"type": "text", "text": f"สวัสดี {display_name}! 👋", "weight": "bold", "size": "md"},
                {"type": "text", "text": "คุณสามารถถามข้อมูลหุ้นได้เลย เช่น:", "size": "sm", "color": "#7F8C8D", "wrap": True},
                {
                    "type": "box",
                    "layout": "vertical",
                    "spacing": "xs",
                    "contents": [
                        _cmd_row("ตลาด", "ภาพรวมตลาด SET"),
                        _cmd_row("sector", "แนวโน้มกลุ่มหุ้น"),
                        _cmd_row("breakout", "หุ้น Breakout"),
                        _cmd_row("global", "🌏 ตลาดโลก / Crypto"),
                        _cmd_row("watchlist", "รายการโปรด"),
                        _cmd_row("guide", "📖 คู่มือทั้งหมด"),
                    ],
                },
                {"type": "text",
                 "text": "💡 พิมพ์ ticker ตรงๆ เช่น ADVANC, BTC, SPX เพื่อดูรายละเอียดทันที",
                 "size": "xxs", "color": "#7F8C8D", "wrap": True, "margin": "md"},
            ],
            "paddingAll": "16px",
        },
    }




def build_performance_review_card(rows: list[dict]) -> dict:
    """Show breakout log performance: symbol, entry, current gain, stage now."""
    if not rows:
        return {
            "type": "bubble", "size": "mega",
            "body": {"type": "box", "layout": "vertical", "backgroundColor": "#0D0D1A",
                     "paddingAll": "20px", "contents": [
                         {"type": "text", "text": "📊 Breakout Performance", "weight": "bold", "size": "lg", "color": "#FFFFFF"},
                         {"type": "text", "text": "ยังไม่มีข้อมูล Breakout", "size": "sm", "color": "#7F8C8D", "margin": "sm"},
                     ]},
        }

    _gain_color = lambda g: "#27AE60" if g >= 10 else ("#F39C12" if g >= 0 else "#E74C3C")

    col_header = {
        "type": "box", "layout": "horizontal", "paddingBottom": "4px",
        "contents": [
            {"type": "text", "text": "Stock", "size": "xxs", "color": "#AAAAAA", "flex": 3},
            {"type": "text", "text": "Pat", "size": "xxs", "color": "#AAAAAA", "flex": 2, "align": "center"},
            {"type": "text", "text": "Entry", "size": "xxs", "color": "#AAAAAA", "flex": 2, "align": "end"},
            {"type": "text", "text": "Gain%", "size": "xxs", "color": "#AAAAAA", "flex": 2, "align": "end"},
            {"type": "text", "text": "Now", "size": "xxs", "color": "#AAAAAA", "flex": 1, "align": "end"},
        ],
    }
    data_rows: list = [col_header, {"type": "separator"}]
    for r in rows[:25]:
        pat_short = {"breakout": "BO", "ath_breakout": "ATH", "vcp": "VCP"}.get(r.get("pattern", ""), "–")
        gain = r.get("gain_pct", 0)
        gain_sign = "+" if gain >= 0 else ""
        stage_color = STAGE_COLOR.get(r.get("current_stage", 1), "#95A5A6")
        data_rows.append({
            "type": "box", "layout": "horizontal",
            "action": {"type": "message", "text": r["symbol"]},
            "paddingTop": "5px", "paddingBottom": "5px",
            "contents": [
                {"type": "text", "text": r["symbol"], "size": "sm", "weight": "bold", "flex": 3, "gravity": "center"},
                {"type": "text", "text": pat_short, "size": "xxs", "color": "#2980B9", "flex": 2, "align": "center", "gravity": "center"},
                {"type": "text", "text": f"฿{r['breakout_price']:,.2f}", "size": "xxs", "color": "#7F8C8D", "flex": 2, "align": "end", "gravity": "center"},
                {"type": "text", "text": f"{gain_sign}{gain:.1f}%", "size": "xs", "weight": "bold",
                 "color": _gain_color(gain), "flex": 2, "align": "end", "gravity": "center"},
                {"type": "text", "text": f"S{r.get('current_stage', '?')}", "size": "xxs",
                 "color": stage_color, "flex": 1, "align": "end", "gravity": "center"},
            ],
        })
        data_rows.append({"type": "separator"})

    return {
        "type": "bubble", "size": "mega",
        "header": {
            "type": "box", "layout": "vertical",
            "backgroundColor": "#0D0D1A", "paddingAll": "16px",
            "contents": [
                {"type": "text", "text": "📊 Breakout Performance Review", "weight": "bold", "size": "lg", "color": "#FFFFFF"},
                {"type": "text", "text": "ผลงานนับจากวันที่ Breakout", "size": "xxs", "color": "#CCCCCC"},
            ],
        },
        "body": {
            "type": "box", "layout": "vertical", "spacing": "none",
            "contents": data_rows, "paddingAll": "12px",
        },
        "footer": {
            "type": "box", "layout": "vertical", "paddingAll": "8px",
            "contents": [
                {"type": "text", "text": f"แสดง {min(len(rows), 25)} รายการ · แตะชื่อหุ้นเพื่อดูรายละเอียด",
                 "size": "xxs", "color": "#7F8C8D", "align": "center", "wrap": True},
            ],
        },
    }


def build_score_card(user_data: dict) -> dict:
    """Show a user's Captain Signal score and rank."""
    score = user_data.get("score", 0)
    history = user_data.get("score_history", [])[-5:]

    if score >= 60:
        rank, rank_color, rank_emoji = "Admiral", "#F39C12", "🎖️"
    elif score >= 30:
        rank, rank_color, rank_emoji = "Commander", "#2980B9", "⭐"
    elif score >= 10:
        rank, rank_color, rank_emoji = "Navigator", "#27AE60", "🧭"
    else:
        rank, rank_color, rank_emoji = "Cadet", "#95A5A6", "⚓"

    history_rows = []
    for h in reversed(history):
        delta = h.get("delta", 0)
        sign = "+" if delta >= 0 else ""
        color = "#27AE60" if delta >= 0 else "#E74C3C"
        history_rows.append({"type": "box", "layout": "horizontal", "paddingTop": "3px", "paddingBottom": "3px",
                               "contents": [
                                   {"type": "text", "text": h.get("date", "")[-5:], "size": "xxs", "color": "#7F8C8D", "flex": 2},
                                   {"type": "text", "text": h.get("symbol", ""), "size": "xxs", "weight": "bold", "flex": 3},
                                   {"type": "text", "text": h.get("reason", "").replace("_", " "), "size": "xxs", "color": "#AAAAAA", "flex": 4, "wrap": True},
                                   {"type": "text", "text": f"{sign}{delta}", "size": "xxs", "color": color, "weight": "bold", "flex": 2, "align": "end"},
                               ]})

    return {
        "type": "bubble", "size": "mega",
        "header": {
            "type": "box", "layout": "vertical", "backgroundColor": "#0D0D1A", "paddingAll": "16px",
            "contents": [
                {"type": "text", "text": "⭐ Captain's Score", "weight": "bold", "size": "lg", "color": "#FFFFFF"},
            ],
        },
        "body": {
            "type": "box", "layout": "vertical", "spacing": "md", "paddingAll": "16px",
            "contents": [
                {"type": "box", "layout": "horizontal", "contents": [
                    {"type": "text", "text": f"{rank_emoji} {rank}", "size": "xl", "weight": "bold", "color": rank_color, "flex": 1},
                    {"type": "text", "text": str(score), "size": "xxl", "weight": "bold", "color": "#F39C12", "align": "end"},
                ]},
                {"type": "text", "text": "pts", "size": "xs", "color": "#7F8C8D", "align": "end"},
                _captain_advice_box("สะสมคะแนนโดยดูหุ้น Stage 2 Breakout (+1) หลีกเลี่ยงการดูหุ้น Stage 4 ซ้ำๆ (-1 เมื่อดูเกิน 2 ครั้งต่อสัปดาห์) กัปตันเชียร์ครับ!"),
                *([
                    {"type": "separator"},
                    {"type": "text", "text": "Recent Activity", "size": "xs", "color": "#7F8C8D"},
                    *history_rows,
                ] if history_rows else []),
            ],
        },
    }


def build_simple_tappable_list(signals: list[StockSignal], title: str, max_items: int = 100) -> dict:
    """Compact tappable list bubble for stage/filter results. Each row sends symbol to bot."""
    PAT_ABBR = {"breakout": "BO", "ath_breakout": "ATH", "vcp": "VCP",
                "vcp_low_cheat": "VCPl", "consolidating": "CON"}
    top = signals[:max_items]
    rows = []
    for i, s in enumerate(top, 1):
        sign = "+" if s.change_pct >= 0 else ""
        chg_color = "#27AE60" if s.change_pct >= 0 else "#E74C3C"
        pat = PAT_ABBR.get(s.pattern, s.pattern[:3].upper())
        rows.append({
            "type": "box", "layout": "horizontal",
            "action": {"type": "message", "label": s.symbol, "text": s.symbol},
            "paddingTop": "5px", "paddingBottom": "5px",
            "contents": [
                {"type": "text", "text": f"{i}.", "size": "xxs", "color": "#7F8C8D", "flex": 1},
                {"type": "text", "text": s.symbol, "size": "sm", "weight": "bold", "color": "#FFFFFF", "flex": 3},
                {"type": "text", "text": f"฿{s.close:,.0f}", "size": "xs", "color": "#CCCCCC", "flex": 3, "align": "end"},
                {"type": "text", "text": f"{sign}{s.change_pct:.1f}%", "size": "xs", "color": chg_color, "flex": 2, "align": "end"},
                {"type": "text", "text": pat, "size": "xxs", "color": "#95A5A6", "flex": 2, "align": "end"},
            ],
        })
        if i % 10 == 0 and i < len(top):
            rows.append({"type": "separator"})
    total = len(signals)
    shown = len(top)
    footer_note = f"แสดง {shown}/{total}  ·  แตะชื่อหุ้นเพื่อดูรายละเอียด" if total > shown else f"{total} หุ้น  ·  แตะชื่อหุ้นเพื่อดูรายละเอียด"
    return {
        "type": "bubble", "size": "mega",
        "header": {
            "type": "box", "layout": "vertical",
            "contents": [{"type": "text", "text": title, "weight": "bold", "size": "md", "color": "#FFFFFF"}],
            "backgroundColor": "#1A237E", "paddingAll": "12px",
        },
        "body": {
            "type": "box", "layout": "vertical", "spacing": "none",
            "contents": rows, "paddingAll": "12px",
        },
        "footer": {
            "type": "box", "layout": "vertical", "paddingAll": "8px",
            "contents": [{"type": "text", "text": footer_note, "size": "xxs", "color": "#7F8C8D", "align": "center", "wrap": True}],
        },
    }


# ─── Send helpers ──────────────────────────────────────────────────────────────

def _flex_message(alt_text: str, container: dict) -> FlexMessage:
    return FlexMessage(
        alt_text=alt_text,
        contents=FlexContainer.from_dict(container),
    )


def reply_flex(reply_token: str, alt_text: str, container: dict) -> bool:
    """Reply to a LINE message with a Flex Message. Returns True on success."""
    api = _get_api()
    try:
        api.reply_message(
            ReplyMessageRequest(
                reply_token=reply_token,
                messages=[_flex_message(alt_text, container)],
            )
        )
        return True
    except Exception as exc:
        logger.error("Failed to reply flex: %s", exc)
        return False


def reply_text(reply_token: str, text: str) -> None:
    """Reply to a LINE message with plain text."""
    api = _get_api()
    try:
        api.reply_message(
            ReplyMessageRequest(
                reply_token=reply_token,
                messages=[TextMessage(text=text)],
            )
        )
    except Exception as exc:
        logger.error("Failed to reply text: %s", exc)


def reply_flex_and_text(reply_token: str, alt_text: str, container: dict, extra_text: str) -> None:
    """Reply with a Flex Message followed by a text message (2 messages in 1 reply)."""
    api = _get_api()
    try:
        messages = [_flex_message(alt_text, container)]
        if extra_text:
            messages.append(TextMessage(text=extra_text))
        api.reply_message(
            ReplyMessageRequest(reply_token=reply_token, messages=messages)
        )
    except Exception as exc:
        logger.error("Failed to reply flex+text: %s", exc)


def send_to_user(user_id: str, alt_text: str, container: dict) -> None:
    """Push a Flex Message to a specific user."""
    api = _get_api()
    try:
        api.multicast(
            MulticastRequest(
                to=[user_id],
                messages=[_flex_message(alt_text, container)],
            )
        )
    except Exception as exc:
        logger.error("Failed to push to %s: %s", user_id, exc)


def broadcast_flex(alt_text: str, container: dict) -> None:
    """Broadcast a Flex Message to all followers of the LINE Official Account."""
    api = _get_api()
    try:
        api.broadcast(
            BroadcastRequest(messages=[_flex_message(alt_text, container)])
        )
    except Exception as exc:
        logger.error("Failed to broadcast: %s", exc)


def broadcast_text(text: str) -> None:
    """Broadcast a plain text message to all followers."""
    api = _get_api()
    try:
        api.broadcast(BroadcastRequest(messages=[TextMessage(text=text)]))
    except Exception as exc:
        logger.error("Failed to broadcast text: %s", exc)


def multicast_flex(user_ids: list[str], alt_text: str, container: dict) -> None:
    """Send Flex Message to a list of user IDs (up to 500 at a time)."""
    if not user_ids:
        return
    api = _get_api()
    try:
        # LINE multicast accepts max 500 per call
        for i in range(0, len(user_ids), 500):
            chunk = user_ids[i:i + 500]
            api.multicast(
                MulticastRequest(
                    to=chunk,
                    messages=[_flex_message(alt_text, container)],
                )
            )
    except Exception as exc:
        logger.error("Failed to multicast: %s", exc)
