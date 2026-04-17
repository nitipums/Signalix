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
from typing import Optional

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

# ─── Pattern display config ───────────────────────────────────────────────────

PATTERN_LABEL = {
    "breakout": "Breakout",
    "ath_breakout": "ATH Breakout",
    "vcp": "VCP",
    "vcp_low_cheat": "VCP Low Cheat",
    "consolidating": "Consolidating",
    "going_down": "Going Down",
}

PATTERN_COLOR = {
    "breakout": "#27AE60",
    "ath_breakout": "#F39C12",
    "vcp": "#2980B9",
    "vcp_low_cheat": "#1ABC9C",
    "consolidating": "#95A5A6",
    "going_down": "#E74C3C",
}

STAGE_COLOR = {1: "#95A5A6", 2: "#27AE60", 3: "#E67E22", 4: "#E74C3C"}
STAGE_LABEL = {1: "Stage 1 – Basing", 2: "Stage 2 – Uptrend", 3: "Stage 3 – Topping", 4: "Stage 4 – Downtrend"}


# ─── LINE client helpers ───────────────────────────────────────────────────────

def _get_api() -> tuple[MessagingApi, ApiClient]:
    settings = get_settings()
    config = Configuration(access_token=settings.line_channel_access_token)
    client = ApiClient(configuration=config)
    return MessagingApi(client), client


def get_webhook_handler() -> WebhookHandler:
    settings = get_settings()
    return WebhookHandler(settings.line_channel_secret)


# ─── Flex Message builders ────────────────────────────────────────────────────

def _pct_color(pct: float) -> str:
    if pct > 0:
        return "#27AE60"
    if pct < 0:
        return "#E74C3C"
    return "#7F8C8D"


def build_market_breadth_card(breadth: MarketBreadth) -> dict:
    """Build a Flex Bubble card for market breadth summary."""
    chg_color = _pct_color(getattr(breadth, "set_index_change_pct", 0.0))
    chg_sign = "+" if getattr(breadth, "set_index_change_pct", 0.0) > 0 else ""
    set_close = getattr(breadth, "set_index_close", 0.0)
    set_chg = getattr(breadth, "set_index_change_pct", 0.0)
    above_pct = getattr(breadth, "above_ma200_pct", 0.0)
    below_pct = round(100 - above_pct, 1)
    above_cnt = getattr(breadth, "above_ma200", 0)
    below_cnt = getattr(breadth, "below_ma200", 0)

    index_row = []
    if set_close > 0:
        index_row = [
            {
                "type": "box",
                "layout": "horizontal",
                "contents": [
                    _kv_box("SET Index", f"{set_close:,.2f}", "#FFFFFF"),
                    _kv_box("เปลี่ยนแปลง", f"{chg_sign}{set_chg:.2f}%", chg_color),
                ],
            },
            {"type": "separator"},
        ]

    return {
        "type": "bubble",
        "size": "mega",
        "header": {
            "type": "box",
            "layout": "vertical",
            "contents": [
                {
                    "type": "text",
                    "text": "📊 ภาพรวมตลาด SET",
                    "weight": "bold",
                    "size": "lg",
                    "color": "#FFFFFF",
                },
                {
                    "type": "text",
                    "text": breadth.scanned_at[:16].replace("T", " "),
                    "size": "xs",
                    "color": "#DDDDDD",
                },
            ],
            "backgroundColor": "#1A237E",
            "paddingAll": "16px",
        },
        "body": {
            "type": "box",
            "layout": "vertical",
            "spacing": "md",
            "contents": [
                # SET index level (shown only when data available)
                *index_row,
                # Stage distribution
                {
                    "type": "box",
                    "layout": "horizontal",
                    "contents": [
                        _stage_box("Stage 2", breadth.stage2_count, "#27AE60"),
                        _stage_box("Stage 1", breadth.stage1_count, "#95A5A6"),
                        _stage_box("Stage 3", breadth.stage3_count, "#E67E22"),
                        _stage_box("Stage 4", breadth.stage4_count, "#E74C3C"),
                    ],
                },
                {"type": "separator"},
                # Advancing / Declining
                {
                    "type": "box",
                    "layout": "horizontal",
                    "contents": [
                        _kv_box("ขึ้น", str(breadth.advancing), "#27AE60"),
                        _kv_box("ลง", str(breadth.declining), "#E74C3C"),
                        _kv_box("ทรงตัว", str(breadth.unchanged), "#7F8C8D"),
                    ],
                },
                {"type": "separator"},
                # MA200 above/below
                {
                    "type": "box",
                    "layout": "horizontal",
                    "contents": [
                        _kv_box("เหนือ MA200", f"{above_pct:.0f}%\n({above_cnt})", "#27AE60"),
                        _kv_box("ต่ำกว่า MA200", f"{below_pct:.0f}%\n({below_cnt})", "#E74C3C"),
                    ],
                },
                {"type": "separator"},
                # Patterns
                {
                    "type": "box",
                    "layout": "horizontal",
                    "contents": [
                        _kv_box("Breakout", str(breadth.breakout_count), "#F39C12"),
                        _kv_box("VCP", str(breadth.vcp_count), "#2980B9"),
                        _kv_box("New High", str(breadth.new_highs_52w), "#8E44AD"),
                    ],
                },
                {
                    "type": "text",
                    "text": f"Stage 2: {breadth.stage2_pct}% ของตลาด",
                    "size": "xs",
                    "color": "#7F8C8D",
                    "align": "center",
                },
            ],
            "paddingAll": "16px",
        },
        "footer": {
            "type": "box",
            "layout": "vertical",
            "contents": [
                {
                    "type": "button",
                    "action": {"type": "message", "label": "ⓘ Stage คืออะไร?", "text": "explain stage"},
                    "style": "secondary",
                    "height": "sm",
                },
            ],
            "paddingAll": "8px",
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


def _kv_box(label: str, value: str, color: str) -> dict:
    return {
        "type": "box",
        "layout": "vertical",
        "flex": 1,
        "alignItems": "center",
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


def build_single_stock_card(signal: StockSignal) -> dict:
    """Build a detailed Flex Bubble for a single stock query."""
    pcolor = PATTERN_COLOR.get(signal.pattern, "#7F8C8D")
    pattern_label = PATTERN_LABEL.get(signal.pattern, signal.pattern)
    stage_label = STAGE_LABEL.get(signal.stage, f"Stage {signal.stage}")
    chg_color = _pct_color(signal.change_pct)
    chg_sign = "+" if signal.change_pct > 0 else ""

    ma_rows = []
    if signal.sma50:
        gap50 = (signal.close - signal.sma50) / signal.sma50 * 100
        ma_rows.append(_detail_row("SMA50", f"฿{signal.sma50:,.2f}", f"{gap50:+.1f}%", _pct_color(gap50)))
    if signal.sma150:
        gap150 = (signal.close - signal.sma150) / signal.sma150 * 100
        ma_rows.append(_detail_row("SMA150", f"฿{signal.sma150:,.2f}", f"{gap150:+.1f}%", _pct_color(gap150)))
    if signal.sma200:
        gap200 = (signal.close - signal.sma200) / signal.sma200 * 100
        ma_rows.append(_detail_row("SMA200", f"฿{signal.sma200:,.2f}", f"{gap200:+.1f}%", _pct_color(gap200)))

    return {
        "type": "bubble",
        "size": "mega",
        "header": {
            "type": "box",
            "layout": "vertical",
            "contents": [
                {
                    "type": "box",
                    "layout": "horizontal",
                    "contents": [
                        {"type": "text", "text": f"SET:{signal.symbol}", "weight": "bold", "size": "xl", "color": "#FFFFFF", "flex": 1},
                        {"type": "text", "text": f"{chg_sign}{signal.change_pct:.2f}%", "size": "md", "color": chg_color, "align": "end", "weight": "bold"},
                    ],
                },
                {
                    "type": "box",
                    "layout": "horizontal",
                    "contents": [
                        {"type": "text", "text": stage_label, "size": "xs", "color": "#AAAAAA", "flex": 1},
                        {"type": "text", "text": pattern_label, "size": "xs", "color": pcolor, "weight": "bold", "align": "end"},
                    ],
                },
            ],
            "backgroundColor": "#0D0D1A",
            "paddingAll": "16px",
        },
        "body": {
            "type": "box",
            "layout": "vertical",
            "spacing": "md",
            "contents": [
                # Price
                {
                    "type": "box",
                    "layout": "horizontal",
                    "contents": [
                        {"type": "text", "text": "ราคา", "size": "sm", "color": "#7F8C8D", "flex": 1},
                        {"type": "text", "text": f"฿{signal.close:,.2f}", "size": "sm", "weight": "bold", "align": "end"},
                    ],
                },
                # Volume ratio
                {
                    "type": "box",
                    "layout": "horizontal",
                    "contents": [
                        {"type": "text", "text": "Volume Ratio", "size": "sm", "color": "#7F8C8D", "flex": 1},
                        {
                            "type": "text",
                            "text": f"{signal.volume_ratio:.2f}x",
                            "size": "sm",
                            "weight": "bold",
                            "align": "end",
                            "color": "#F39C12" if signal.volume_ratio >= 1.5 else "#FFFFFF",
                        },
                    ],
                },
                # 52-week range
                {
                    "type": "box",
                    "layout": "horizontal",
                    "contents": [
                        {"type": "text", "text": "52W High/Low", "size": "sm", "color": "#7F8C8D", "flex": 1},
                        {"type": "text", "text": f"฿{signal.high_52w:,.2f} / ฿{signal.low_52w:,.2f}", "size": "sm", "align": "end"},
                    ],
                },
                {"type": "separator"},
                # Moving averages
                *ma_rows,
                {"type": "separator"},
                # Score
                {
                    "type": "box",
                    "layout": "horizontal",
                    "contents": [
                        {"type": "text", "text": "Strength Score", "size": "sm", "color": "#7F8C8D", "flex": 1},
                        {"type": "text", "text": f"{int(signal.strength_score)}/100", "size": "sm", "weight": "bold", "color": "#F39C12", "align": "end"},
                    ],
                },
            ],
            "paddingAll": "16px",
        },
        "footer": {
            "type": "box",
            "layout": "vertical",
            "spacing": "sm",
            "contents": [
                {
                    "type": "button",
                    "action": {
                        "type": "uri",
                        "label": "🔗 ดูชาร์ต TradingView",
                        "uri": signal.tradingview_url,
                    },
                    "style": "primary",
                    "color": "#1565C0",
                },
                {
                    "type": "box",
                    "layout": "horizontal",
                    "spacing": "sm",
                    "contents": [
                        {
                            "type": "button",
                            "action": {"type": "message", "label": f"ⓘ {STAGE_LABEL.get(signal.stage, 'Stage')}", "text": f"explain stage{signal.stage}"},
                            "style": "secondary",
                            "height": "sm",
                            "flex": 1,
                        },
                        {
                            "type": "button",
                            "action": {"type": "message", "label": f"ⓘ {PATTERN_LABEL.get(signal.pattern, signal.pattern)}", "text": f"explain {signal.pattern}"},
                            "style": "secondary",
                            "height": "sm",
                            "flex": 1,
                        },
                        {
                            "type": "button",
                            "action": {"type": "message", "label": "ⓘ Score", "text": "explain score"},
                            "style": "secondary",
                            "height": "sm",
                            "flex": 1,
                        },
                    ],
                },
            ],
            "paddingAll": "12px",
        },
    }


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


def build_index_carousel(indexes: dict[str, dict]) -> dict:
    """Build a carousel of index bubbles (SET, SET50, SET100, MAI, sSET)."""
    INDEX_COLORS = {
        "SET": "#1A237E", "SET50": "#0D47A1", "SET100": "#1565C0",
        "MAI": "#4A148C", "sSET": "#006064",
    }
    bubbles = []
    for name, data in indexes.items():
        close = data.get("close", 0.0)
        chg = data.get("change_pct", 0.0)
        chg_sign = "+" if chg > 0 else ""
        chg_color = _pct_color(chg)
        tv_url = INDEX_TV_URLS.get(name, "https://www.tradingview.com")
        bg = INDEX_COLORS.get(name, "#1A237E")
        bubbles.append({
            "type": "bubble",
            "size": "kilo",
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
                "spacing": "sm",
                "contents": [
                    {"type": "text", "text": f"{close:,.2f}", "weight": "bold", "size": "xl"},
                    {"type": "text", "text": f"{chg_sign}{chg:.2f}%", "size": "md", "color": chg_color, "weight": "bold"},
                ],
                "paddingAll": "12px",
            },
            "footer": {
                "type": "box",
                "layout": "vertical",
                "contents": [
                    {"type": "button", "action": {"type": "uri", "label": "ดูชาร์ต", "uri": tv_url}, "style": "primary", "color": "#1565C0", "height": "sm"},
                ],
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
                        _cmd_row("ตลาด", "ภาพรวมตลาด"),
                        _cmd_row("ดัชนี", "SET50/MAI/sSET"),
                        _cmd_row("sector", "แนวโน้มกลุ่มหุ้น"),
                        _cmd_row("breakout", "หุ้น Breakout"),
                        _cmd_row("watchlist", "รายการโปรด"),
                        _cmd_row("help", "ดูคำสั่งทั้งหมด"),
                    ],
                },
            ],
            "paddingAll": "16px",
        },
    }


def build_help_card() -> dict:
    """Help menu card."""
    return {
        "type": "bubble",
        "size": "mega",
        "header": {
            "type": "box",
            "layout": "vertical",
            "contents": [
                {"type": "text", "text": "📖 คำสั่ง Signalix", "weight": "bold", "size": "lg", "color": "#FFFFFF"},
            ],
            "backgroundColor": "#1A237E",
            "paddingAll": "16px",
        },
        "body": {
            "type": "box",
            "layout": "vertical",
            "spacing": "md",
            "contents": [
                {"type": "text", "text": "ภาพรวมตลาด", "weight": "bold", "size": "sm", "color": "#F39C12"},
                _cmd_row("ตลาด / market", "Market Breadth"),
                _cmd_row("ดัชนี / index", "SET50, SET100, MAI, sSET"),
                _cmd_row("sector / เซกเตอร์", "แนวโน้มกลุ่มอุตสาหกรรม"),
                {"type": "separator"},
                {"type": "text", "text": "รายชื่อหุ้น", "weight": "bold", "size": "sm", "color": "#F39C12"},
                _cmd_row("breakout", "Breakout stocks"),
                _cmd_row("ath", "ATH Breakout stocks"),
                _cmd_row("vcp", "VCP Pattern stocks"),
                _cmd_row("stage2", "Stage 2 stocks"),
                _cmd_row("stage1", "Stage 1 stocks"),
                {"type": "separator"},
                {"type": "text", "text": "Watchlist", "weight": "bold", "size": "sm", "color": "#F39C12"},
                _cmd_row("watchlist", "ดู Watchlist"),
                _cmd_row("add PTT", "เพิ่ม PTT"),
                _cmd_row("remove PTT", "ลบ PTT"),
                {"type": "separator"},
                {"type": "text", "text": "วิเคราะห์หุ้นรายตัว", "weight": "bold", "size": "sm", "color": "#F39C12"},
                {"type": "text", "text": "พิมพ์ชื่อหุ้น เช่น PTT, ADVANC, KBANK", "size": "xs", "color": "#7F8C8D", "wrap": True},
            ],
            "paddingAll": "16px",
        },
    }


def _cmd_row(cmd: str, desc: str) -> dict:
    return {
        "type": "box",
        "layout": "horizontal",
        "contents": [
            {"type": "text", "text": cmd, "size": "xs", "weight": "bold", "color": "#2980B9", "flex": 2},
            {"type": "text", "text": desc, "size": "xs", "color": "#7F8C8D", "flex": 3},
        ],
    }


# ─── Send helpers ──────────────────────────────────────────────────────────────

def _flex_message(alt_text: str, container: dict) -> FlexMessage:
    return FlexMessage(
        alt_text=alt_text,
        contents=FlexContainer.from_dict(container),
    )


def reply_flex(reply_token: str, alt_text: str, container: dict) -> None:
    """Reply to a LINE message with a Flex Message."""
    api, client = _get_api()
    try:
        api.reply_message(
            ReplyMessageRequest(
                reply_token=reply_token,
                messages=[_flex_message(alt_text, container)],
            )
        )
    except Exception as exc:
        logger.error("Failed to reply flex: %s", exc)
    finally:
        client.close()


def reply_text(reply_token: str, text: str) -> None:
    """Reply to a LINE message with plain text."""
    api, client = _get_api()
    try:
        api.reply_message(
            ReplyMessageRequest(
                reply_token=reply_token,
                messages=[TextMessage(text=text)],
            )
        )
    except Exception as exc:
        logger.error("Failed to reply text: %s", exc)
    finally:
        client.close()


def reply_flex_and_text(reply_token: str, alt_text: str, container: dict, extra_text: str) -> None:
    """Reply with a Flex Message followed by a text message (2 messages in 1 reply)."""
    api, client = _get_api()
    try:
        messages = [_flex_message(alt_text, container)]
        if extra_text:
            messages.append(TextMessage(text=extra_text))
        api.reply_message(
            ReplyMessageRequest(reply_token=reply_token, messages=messages)
        )
    except Exception as exc:
        logger.error("Failed to reply flex+text: %s", exc)
    finally:
        client.close()


def send_to_user(user_id: str, alt_text: str, container: dict) -> None:
    """Push a Flex Message to a specific user."""
    api, client = _get_api()
    try:
        api.multicast(
            MulticastRequest(
                to=[user_id],
                messages=[_flex_message(alt_text, container)],
            )
        )
    except Exception as exc:
        logger.error("Failed to push to %s: %s", user_id, exc)
    finally:
        client.close()


def broadcast_flex(alt_text: str, container: dict) -> None:
    """Broadcast a Flex Message to all followers of the LINE Official Account."""
    api, client = _get_api()
    try:
        api.broadcast(
            BroadcastRequest(messages=[_flex_message(alt_text, container)])
        )
    except Exception as exc:
        logger.error("Failed to broadcast: %s", exc)
    finally:
        client.close()


def multicast_flex(user_ids: list[str], alt_text: str, container: dict) -> None:
    """Send Flex Message to a list of user IDs (up to 500 at a time)."""
    if not user_ids:
        return
    api, client = _get_api()
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
    finally:
        client.close()
