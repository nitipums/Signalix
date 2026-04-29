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

def _pct_color(pct: float) -> str:
    if pct > 0:
        return "#27AE60"
    if pct < 0:
        return "#E74C3C"
    return "#7F8C8D"


def build_market_breadth_card(breadth: MarketBreadth) -> dict:
    """Build a Flex Bubble card for market breadth summary with SET index as hero header."""
    set_close = getattr(breadth, "set_index_close", 0.0)
    set_chg = getattr(breadth, "set_index_change_pct", 0.0)
    chg_color = _pct_color(set_chg)
    chg_sign = "+" if set_chg > 0 else ""
    above_pct = getattr(breadth, "above_ma200_pct", 0.0)
    below_pct = round(100 - above_pct, 1)
    above_cnt = getattr(breadth, "above_ma200", 0)
    below_cnt = getattr(breadth, "below_ma200", 0)

    # Header background: green if up, red if down, navy if neutral
    if set_chg > 0:
        header_bg = "#1B5E20"
    elif set_chg < 0:
        header_bg = "#B71C1C"
    else:
        header_bg = "#1A237E"

    # Header contents: SET index is the hero element
    header_contents = [
        {"type": "text", "text": "📊 ภาพรวมตลาด SET", "size": "xs", "color": "#DDDDDD"},
    ]
    if set_close > 0:
        header_contents += [
            {
                "type": "box",
                "layout": "horizontal",
                "contents": [
                    {"type": "text", "text": f"{set_close:,.2f}", "weight": "bold", "size": "xxl", "color": "#FFFFFF", "flex": 1},
                    {"type": "text", "text": f"{chg_sign}{set_chg:.2f}%", "size": "lg", "color": chg_color, "weight": "bold", "align": "end"},
                ],
            },
        ]
    header_contents.append(
        {"type": "text", "text": breadth.scanned_at[:16].replace("T", " "), "size": "xxs", "color": "#DDDDDD"}
    )

    # MA200 visual bar
    above_flex = max(1, int(above_pct))
    below_flex = max(1, 100 - above_flex)

    return {
        "type": "bubble",
        "size": "mega",
        "header": {
            "type": "box",
            "layout": "vertical",
            "contents": header_contents,
            "backgroundColor": header_bg,
            "paddingAll": "16px",
        },
        "body": {
            "type": "box",
            "layout": "vertical",
            "spacing": "md",
            "contents": [
                # Stage distribution — Stage 2 most prominent
                {
                    "type": "box",
                    "layout": "horizontal",
                    "contents": [
                        _stage_box("Stage 2 ✅", breadth.stage2_count, "#27AE60"),
                        _stage_box("Stage 1 ⚪", breadth.stage1_count, "#95A5A6"),
                        _stage_box("Stage 3 ⚠️", breadth.stage3_count, "#E67E22"),
                        _stage_box("Stage 4 ❌", breadth.stage4_count, "#E74C3C"),
                    ],
                },
                {
                    "type": "text",
                    "text": f"Stage 2: {breadth.stage2_pct}% ของตลาด",
                    "size": "xs",
                    "color": "#27AE60" if breadth.stage2_pct >= 30 else "#7F8C8D",
                    "align": "center",
                    "weight": "bold",
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
                # MA200 visual bar
                {
                    "type": "box",
                    "layout": "vertical",
                    "contents": [
                        {"type": "text", "text": "% เหนือ MA200", "size": "xxs", "color": "#7F8C8D"},
                        {
                            "type": "box",
                            "layout": "horizontal",
                            "contents": [
                                {"type": "box", "layout": "vertical", "flex": above_flex,
                                 "backgroundColor": "#27AE60", "height": "8px", "contents": []},
                                {"type": "box", "layout": "vertical", "flex": below_flex,
                                 "backgroundColor": "#E74C3C", "height": "8px", "contents": []},
                            ],
                        },
                        {
                            "type": "box",
                            "layout": "horizontal",
                            "contents": [
                                {"type": "text", "text": f"เหนือ {above_pct:.0f}% ({above_cnt})", "size": "xxs", "color": "#27AE60", "flex": 1},
                                {"type": "text", "text": f"ต่ำกว่า {below_pct:.0f}% ({below_cnt})", "size": "xxs", "color": "#E74C3C", "flex": 1, "align": "end"},
                            ],
                        },
                    ],
                },
                {"type": "separator"},
                # Key signals
                {
                    "type": "box",
                    "layout": "horizontal",
                    "contents": [
                        _kv_box("Breakout", str(breadth.breakout_count), "#F39C12"),
                        _kv_box("VCP", str(breadth.vcp_count), "#2980B9"),
                        _kv_box("52W High", str(breadth.new_highs_52w), "#8E44AD"),
                        _kv_box("52W Low", str(breadth.new_lows_52w), "#E74C3C"),
                    ],
                },
            ],
            "paddingAll": "16px",
        },
        "footer": {
            "type": "box",
            "layout": "horizontal",
            "spacing": "sm",
            "contents": [
                {"type": "button", "action": {"type": "message", "label": "Stage 2", "text": "stage2"}, "style": "primary", "color": "#1B5E20", "height": "sm", "flex": 1},
                {"type": "button", "action": {"type": "message", "label": "Breakout", "text": "breakout"}, "style": "primary", "color": "#E65100", "height": "sm", "flex": 1},
                {"type": "button", "action": {"type": "message", "label": "VCP", "text": "vcp"}, "style": "primary", "color": "#0D47A1", "height": "sm", "flex": 1},
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
                # Trade value
                *([{
                    "type": "box",
                    "layout": "horizontal",
                    "contents": [
                        {"type": "text", "text": "มูลค่าซื้อขาย", "size": "sm", "color": "#7F8C8D", "flex": 1},
                        {"type": "text", "text": f"฿{signal.trade_value_m:.1f}M", "size": "sm", "weight": "bold", "align": "end"},
                    ],
                }] if getattr(signal, "trade_value_m", 0) > 0 else []),
                # % from 52W high
                *([{
                    "type": "box",
                    "layout": "horizontal",
                    "contents": [
                        {"type": "text", "text": "ต่ำกว่า 52W High", "size": "sm", "color": "#7F8C8D", "flex": 1},
                        {"type": "text", "text": f"{signal.pct_from_52w_high:.1f}%", "size": "sm", "weight": "bold", "align": "end",
                         "color": "#27AE60" if signal.pct_from_52w_high >= -5 else "#E67E22"},
                    ],
                }] if getattr(signal, "pct_from_52w_high", 0) != 0 else []),
                # Margin section
                *([
                    {"type": "separator"},
                    {"type": "text", "text": "⚖️ Margin (ATR-based)", "size": "xxs", "color": "#F39C12", "weight": "bold"},
                    _detail_row("Stop Loss", f"฿{signal.stop_loss:,.2f}",
                                f"-{(signal.close - signal.stop_loss) / signal.close * 100:.1f}%", "#E74C3C"),
                    _detail_row("Target (2:1)", f"฿{signal.target_price:,.2f}",
                                f"+{(signal.target_price - signal.close) / signal.close * 100:.1f}%", "#27AE60"),
                ] if getattr(signal, "stop_loss", 0) > 0 else []),
                # Breakout history
                *([{
                    "type": "box",
                    "layout": "horizontal",
                    "contents": [
                        {"type": "text", "text": "Breakout (1 ปี)", "size": "sm", "color": "#7F8C8D", "flex": 1},
                        {"type": "text", "text": f"{signal.breakout_count_1y} ครั้ง", "size": "sm", "weight": "bold", "color": "#2980B9", "align": "end"},
                    ],
                }] if getattr(signal, "breakout_count_1y", 0) > 0 else []),
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
                {"type": "text", "text": f"อัปเดต: {signal.scanned_at[:16].replace('T', ' ')}", "size": "xxs", "color": "#AAAAAA", "align": "center"},
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


def _stock_row(rank: int, signal: StockSignal) -> dict:
    """Single row in the consolidated ranked stock list bubble."""
    chg_color = _pct_color(signal.change_pct)
    chg_sign = "+" if signal.change_pct > 0 else ""
    stage_color = STAGE_COLOR.get(signal.stage, "#95A5A6")
    pattern_short = {
        "breakout": "BO", "ath_breakout": "ATH", "vcp": "VCP",
        "vcp_low_cheat": "VCP-L", "consolidating": "Coil", "going_down": "DN",
    }.get(signal.pattern, "?")
    return {
        "type": "box",
        "layout": "horizontal",
        "action": {"type": "message", "text": signal.symbol},
        "spacing": "xs",
        "paddingTop": "4px",
        "paddingBottom": "4px",
        "contents": [
            {"type": "text", "text": str(rank), "size": "xxs", "color": "#7F8C8D", "flex": 1, "gravity": "center"},
            {"type": "text", "text": signal.symbol, "size": "xs", "weight": "bold", "flex": 4, "gravity": "center"},
            {"type": "text", "text": f"S{signal.stage}", "size": "xxs", "color": stage_color, "flex": 2, "align": "center", "gravity": "center"},
            {"type": "text", "text": pattern_short, "size": "xxs", "color": PATTERN_COLOR.get(signal.pattern, "#7F8C8D"), "flex": 2, "align": "center", "gravity": "center"},
            {"type": "text", "text": f"{chg_sign}{signal.change_pct:.1f}%", "size": "xxs", "color": chg_color, "flex": 3, "align": "end", "gravity": "center"},
            {"type": "text", "text": str(int(signal.strength_score)), "size": "xxs", "color": "#F39C12", "flex": 2, "align": "end", "gravity": "center"},
        ],
    }


def build_ranked_stock_list_bubble(signals: list[StockSignal], title: str, max_per_bubble: int = 40) -> dict:
    """
    Single scrollable bubble showing all stocks ranked by strength_score.
    Each row is tappable (sends symbol name → triggers single stock lookup).
    For >40 stocks returns a carousel of bubbles.
    """
    if not signals:
        return {"type": "bubble", "size": "mega", "body": {"type": "box", "layout": "vertical",
                "contents": [{"type": "text", "text": "ไม่มีหุ้น", "color": "#7F8C8D"}]}}

    def _make_bubble(chunk: list[StockSignal], offset: int, total: int) -> dict:
        header_text = title
        if total > max_per_bubble:
            page = offset // max_per_bubble + 1
            pages = (total + max_per_bubble - 1) // max_per_bubble
            header_text = f"{title} ({page}/{pages})"

        # Column header
        col_header = {
            "type": "box",
            "layout": "horizontal",
            "spacing": "xs",
            "paddingBottom": "4px",
            "contents": [
                {"type": "text", "text": "#", "size": "xxs", "color": "#7F8C8D", "flex": 1},
                {"type": "text", "text": "หุ้น", "size": "xxs", "color": "#7F8C8D", "flex": 4},
                {"type": "text", "text": "Stage", "size": "xxs", "color": "#7F8C8D", "flex": 2, "align": "center"},
                {"type": "text", "text": "รูปแบบ", "size": "xxs", "color": "#7F8C8D", "flex": 2, "align": "center"},
                {"type": "text", "text": "เปลี่ยน", "size": "xxs", "color": "#7F8C8D", "flex": 3, "align": "end"},
                {"type": "text", "text": "คะแนน", "size": "xxs", "color": "#7F8C8D", "flex": 2, "align": "end"},
            ],
        }

        rows = [col_header, {"type": "separator"}]
        for i, sig in enumerate(chunk):
            rows.append(_stock_row(offset + i + 1, sig))

        footer_text = f"แตะชื่อหุ้นเพื่อดูรายละเอียด | ทั้งหมด {total} หุ้น"

        return {
            "type": "bubble",
            "size": "giga",
            "header": {
                "type": "box",
                "layout": "vertical",
                "contents": [
                    {"type": "text", "text": header_text, "weight": "bold", "size": "md", "color": "#FFFFFF"},
                    {"type": "text", "text": f"เรียงตาม Strength Score สูงสุด", "size": "xxs", "color": "#DDDDDD"},
                ],
                "backgroundColor": "#1A237E",
                "paddingAll": "12px",
            },
            "body": {
                "type": "box",
                "layout": "vertical",
                "spacing": "none",
                "contents": rows,
                "paddingAll": "12px",
            },
            "footer": {
                "type": "box",
                "layout": "vertical",
                "contents": [
                    {"type": "text", "text": footer_text, "size": "xxs", "color": "#7F8C8D", "wrap": True, "align": "center"},
                ],
                "paddingAll": "8px",
            },
        }

    total = len(signals)
    if total <= max_per_bubble:
        return _make_bubble(signals, 0, total)

    # Split into carousel of bubbles (LINE allows max 12 per carousel)
    bubbles = []
    for i in range(0, min(total, max_per_bubble * 12), max_per_bubble):
        chunk = signals[i:i + max_per_bubble]
        bubbles.append(_make_bubble(chunk, i, total))
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


def build_index_carousel(indexes: dict[str, dict]) -> dict:
    """Build a carousel of index bubbles (SET, SET50, SET100, MAI, sSET) with MACD/RSI for SET."""
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

        # Dynamic header bg based on price direction
        if chg > 0:
            bg = "#1B5E20" if name == "SET" else INDEX_COLORS.get(name, "#1A237E")
        elif chg < 0:
            bg = "#B71C1C" if name == "SET" else INDEX_COLORS.get(name, "#1A237E")
        else:
            bg = INDEX_COLORS.get(name, "#1A237E")

        # Build body contents — enriched for SET, minimal for others
        body_contents = [
            {"type": "text", "text": f"{close:,.2f}", "weight": "bold", "size": "xxl" if name == "SET" else "xl"},
            {"type": "text", "text": f"{chg_sign}{chg:.2f}%", "size": "md", "color": chg_color, "weight": "bold"},
        ]

        # Add MACD/RSI/trend for any index that has the enriched analysis data
        rsi = data.get("rsi")
        macd_hist = data.get("macd_hist")
        implication = data.get("implication", "")
        above_ma200 = data.get("above_ma200")
        macd_bullish = data.get("macd_bullish_cross", False)
        macd_bearish = data.get("macd_bearish_cross", False)

        if rsi is not None:
            body_contents.append({"type": "separator"})
            # Trend
            if above_ma200 is True:
                trend_text = "▲ อยู่เหนือ MA200 (Uptrend)"
                trend_color = "#27AE60"
            elif above_ma200 is False:
                trend_text = "▼ ต่ำกว่า MA200 (Downtrend)"
                trend_color = "#E74C3C"
            else:
                trend_text = "— Trend ไม่ชัดเจน"
                trend_color = "#7F8C8D"
            body_contents.append({"type": "text", "text": trend_text, "size": "xs", "color": trend_color, "weight": "bold"})

            # RSI
            rsi_color = "#E74C3C" if rsi > 70 else ("#27AE60" if rsi < 30 else "#F39C12")
            body_contents.append({"type": "box", "layout": "horizontal", "contents": [
                {"type": "text", "text": "RSI (14)", "size": "xs", "color": "#7F8C8D", "flex": 1},
                {"type": "text", "text": f"{rsi:.0f}", "size": "xs", "color": rsi_color, "weight": "bold", "align": "end"},
            ]})

            # MACD signal
            if macd_hist is not None:
                if macd_bullish:
                    macd_label = "🟢 MACD Cross Up"
                    macd_color = "#27AE60"
                elif macd_bearish:
                    macd_label = "🔴 MACD Cross Down"
                    macd_color = "#E74C3C"
                elif macd_hist > 0:
                    macd_label = "MACD เป็นบวก ↑"
                    macd_color = "#27AE60"
                else:
                    macd_label = "MACD เป็นลบ ↓"
                    macd_color = "#E74C3C"
                body_contents.append({"type": "text", "text": macd_label, "size": "xs", "color": macd_color})

            # Implication
            if implication:
                body_contents.append({"type": "separator"})
                body_contents.append({"type": "text", "text": implication, "size": "xxs", "color": "#7F8C8D", "wrap": True})

        bubble_size = "mega" if rsi is not None else "kilo"
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


def build_sector_overview_card(sectors: list[SectorSummary]) -> dict:
    """Single mega bubble with all sectors as table rows + quick-tap buttons."""
    SECTOR_COLORS = {
        "AGRO": "#27AE60", "CONSUMP": "#F39C12", "FINCIAL": "#2980B9",
        "INDUS": "#8E44AD", "PROPCON": "#E67E22", "RESOURC": "#E74C3C",
        "SERVICE": "#1ABC9C", "TECH": "#3498DB",
    }
    rows = []
    for sec in sectors:
        color = SECTOR_COLORS.get(sec.sector, "#95A5A6")
        trend = "▲" if sec.advancing > sec.declining else ("▼" if sec.declining > sec.advancing else "=")
        trend_color = "#27AE60" if trend == "▲" else ("#E74C3C" if trend == "▼" else "#7F8C8D")
        rows.append({
            "type": "box",
            "layout": "horizontal",
            "contents": [
                {"type": "text", "text": sec.sector, "size": "xs", "weight": "bold", "color": color, "flex": 3},
                {"type": "text", "text": f"S2:{sec.stage2_pct}%", "size": "xxs", "color": "#27AE60" if sec.stage2_pct >= 20 else "#7F8C8D", "flex": 2, "align": "end"},
                {"type": "text", "text": f"{trend}{sec.advancing}/{sec.declining}", "size": "xxs", "color": trend_color, "flex": 2, "align": "end"},
            ],
        })

    # Top 4 by stage2_pct as tap buttons
    top4 = sorted(sectors, key=lambda s: s.stage2_pct, reverse=True)[:4]
    btn_row = [
        {
            "type": "button",
            "action": {"type": "message", "label": s.sector, "text": f"sector {s.sector}"},
            "style": "link",
            "height": "sm",
            "flex": 1,
            "color": SECTOR_COLORS.get(s.sector, "#2980B9"),
        }
        for s in top4
    ]

    return {
        "type": "bubble",
        "size": "mega",
        "header": {
            "type": "box",
            "layout": "vertical",
            "contents": [
                {"type": "text", "text": "🏭 Sector Overview", "weight": "bold", "size": "lg", "color": "#FFFFFF"},
                {"type": "text", "text": "กลุ่มอุตสาหกรรม SET — แตะกลุ่มเพื่อดูหุ้น", "size": "xxs", "color": "#CCCCCC"},
            ],
            "backgroundColor": "#0D0D1A",
            "paddingAll": "14px",
        },
        "body": {
            "type": "box",
            "layout": "vertical",
            "spacing": "xs",
            "contents": [
                {
                    "type": "box",
                    "layout": "horizontal",
                    "contents": [
                        {"type": "text", "text": "Sector", "size": "xxs", "color": "#AAAAAA", "flex": 3},
                        {"type": "text", "text": "Stage2%", "size": "xxs", "color": "#AAAAAA", "flex": 2, "align": "end"},
                        {"type": "text", "text": "▲/▼", "size": "xxs", "color": "#AAAAAA", "flex": 2, "align": "end"},
                    ],
                },
                {"type": "separator"},
                *rows,
            ],
            "paddingAll": "14px",
        },
        "footer": {
            "type": "box",
            "layout": "vertical",
            "spacing": "xs",
            "contents": [
                {"type": "text", "text": "ดูหุ้นใน Sector:", "size": "xxs", "color": "#7F8C8D"},
                {"type": "box", "layout": "horizontal", "contents": btn_row[:2]},
                {"type": "box", "layout": "horizontal", "contents": btn_row[2:4]} if len(btn_row) > 2 else {"type": "box", "layout": "vertical", "contents": []},
            ],
            "paddingAll": "10px",
        },
    }


def build_stage_picker_card(breadth: Optional[MarketBreadth] = None) -> dict:
    """4-bubble carousel for stage picker (Stage 1–4) with counts from breadth."""
    STAGE_BG = {1: "#555555", 2: "#1B5E20", 3: "#E65100", 4: "#B71C1C"}
    STAGE_DESC = {
        1: "Basing — สะสมตัว\nรอ breakout",
        2: "Uptrend ✅\nโซนซื้อที่ดีที่สุด",
        3: "Topping ⚠️\nระวัง",
        4: "Downtrend ❌\nหลีกเลี่ยง",
    }
    STAGE_ICON = {1: "⚪", 2: "🟢", 3: "🟡", 4: "🔴"}

    stage_counts: dict[int, int] = {1: 0, 2: 0, 3: 0, 4: 0}
    if breadth:
        stage_counts = {
            1: getattr(breadth, "stage1_count", 0),
            2: getattr(breadth, "stage2_count", 0),
            3: getattr(breadth, "stage3_count", 0),
            4: getattr(breadth, "stage4_count", 0),
        }

    bubbles = []
    for s in range(1, 5):
        count = stage_counts.get(s, 0)
        bubbles.append({
            "type": "bubble",
            "size": "kilo",
            "header": {
                "type": "box",
                "layout": "vertical",
                "contents": [
                    {"type": "text", "text": f"{STAGE_ICON[s]} Stage {s}", "weight": "bold", "size": "md", "color": "#FFFFFF"},
                ],
                "backgroundColor": STAGE_BG[s],
                "paddingAll": "12px",
            },
            "body": {
                "type": "box",
                "layout": "vertical",
                "spacing": "xs",
                "contents": [
                    {"type": "text", "text": STAGE_DESC[s], "size": "xxs", "color": "#555555", "wrap": True},
                    {"type": "text", "text": f"{count} หุ้น", "size": "sm", "weight": "bold", "color": STAGE_COLOR.get(s, "#333333")},
                ],
                "paddingAll": "10px",
            },
            "footer": {
                "type": "box",
                "layout": "vertical",
                "contents": [
                    {"type": "button", "action": {"type": "message", "label": f"ดู Stage {s}", "text": f"stage{s}"}, "style": "primary", "color": STAGE_BG[s], "height": "sm"},
                ],
                "paddingAll": "6px",
            },
        })
    return {"type": "carousel", "contents": bubbles}


def build_watchlist_stock_card(signal: StockSignal, fundamentals: dict) -> dict:
    """Deep insight card: Technical + Fundamental data for watchlist view."""
    pcolor = PATTERN_COLOR.get(signal.pattern, "#7F8C8D")
    pattern_label = PATTERN_LABEL.get(signal.pattern, signal.pattern)
    stage_label = STAGE_LABEL.get(signal.stage, f"Stage {signal.stage}")
    chg_color = _pct_color(signal.change_pct)
    chg_sign = "+" if signal.change_pct > 0 else ""

    ma_rows = []
    if signal.sma50:
        gap50 = (signal.close - signal.sma50) / signal.sma50 * 100
        ma_rows.append(_detail_row("SMA50", f"฿{signal.sma50:,.2f}", f"{gap50:+.1f}%", _pct_color(gap50)))
    if signal.sma200:
        gap200 = (signal.close - signal.sma200) / signal.sma200 * 100
        ma_rows.append(_detail_row("SMA200", f"฿{signal.sma200:,.2f}", f"{gap200:+.1f}%", _pct_color(gap200)))

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
            {"type": "text", "text": "⚖️ Margin (ATR-based)", "size": "xxs", "color": "#F39C12", "weight": "bold"},
            _detail_row("Stop Loss", f"฿{signal.stop_loss:,.2f}",
                        f"-{(signal.close - signal.stop_loss) / signal.close * 100:.1f}%", "#E74C3C"),
            _detail_row("Target (2:1)", f"฿{signal.target_price:,.2f}",
                        f"+{(signal.target_price - signal.close) / signal.close * 100:.1f}%", "#27AE60"),
        ]

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
                    {"type": "text", "text": stage_label, "size": "xs", "color": "#AAAAAA", "flex": 1},
                    {"type": "text", "text": pattern_label, "size": "xs", "color": pcolor, "weight": "bold", "align": "end"},
                ]},
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
    """4-bubble carousel: Stage / Pattern / Score+Volume / Quick Reference."""
    stage_bubble = {
        "type": "bubble",
        "size": "mega",
        "header": {
            "type": "box",
            "layout": "vertical",
            "contents": [{"type": "text", "text": "📊 Stage Analysis", "weight": "bold", "size": "lg", "color": "#FFFFFF"}],
            "backgroundColor": "#1A237E",
            "paddingAll": "16px",
        },
        "body": {
            "type": "box",
            "layout": "vertical",
            "spacing": "sm",
            "contents": [
                {"type": "text", "text": "Minervini Stage วิเคราะห์ตาม MA50/150/200", "size": "xs", "color": "#7F8C8D", "wrap": True},
                {"type": "separator"},
                _guide_row("⚪ Stage 1", "Basing — สะสมตัว รอ breakout", "#95A5A6"),
                _guide_row("🟢 Stage 2", "Uptrend ✅ — โซนซื้อที่ดีที่สุด", "#27AE60"),
                _guide_row("🟡 Stage 3", "Topping ⚠️ — ระวัง smart money ขาย", "#E67E22"),
                _guide_row("🔴 Stage 4", "Downtrend ❌ — หลีกเลี่ยง", "#E74C3C"),
                {"type": "separator"},
                {"type": "text", "text": "Stage 2 เงื่อนไข:", "weight": "bold", "size": "xs", "color": "#27AE60"},
                {"type": "text", "text": "ราคา > MA150 > MA200\nMA200 กำลังขึ้น\nราคา ≥ 52W low × 1.25\nราคา ≥ 52W high × 0.75", "size": "xxs", "color": "#555555", "wrap": True},
            ],
            "paddingAll": "16px",
        },
    }

    pattern_bubble = {
        "type": "bubble",
        "size": "mega",
        "header": {
            "type": "box",
            "layout": "vertical",
            "contents": [{"type": "text", "text": "📈 Pattern Guide", "weight": "bold", "size": "lg", "color": "#FFFFFF"}],
            "backgroundColor": "#0D47A1",
            "paddingAll": "16px",
        },
        "body": {
            "type": "box",
            "layout": "vertical",
            "spacing": "sm",
            "contents": [
                _guide_row("🚀 Breakout", "ราคา > 52W high + Vol ≥ 1.4x avg", "#27AE60"),
                _guide_row("🏆 ATH Breakout", "Breakout + ใกล้/สูงกว่า All-Time High", "#F39C12"),
                _guide_row("🔍 VCP", "ความผันผวนหดตัว 3+ ครั้ง + Vol แห้ง", "#2980B9"),
                _guide_row("🎯 VCP Low Cheat", "Entry ที่ low ของ VCP + Vol แห้ง", "#1ABC9C"),
                {"type": "separator"},
                {"type": "text", "text": "Strategy: ซื้อ Breakout/VCP ใน Stage 2 เท่านั้น", "size": "xxs", "color": "#7F8C8D", "wrap": True},
            ],
            "paddingAll": "16px",
        },
        "footer": {
            "type": "box",
            "layout": "vertical",
            "spacing": "xs",
            "contents": [
                {"type": "box", "layout": "horizontal", "spacing": "xs", "contents": [
                    {"type": "button", "action": {"type": "message", "label": "Breakout", "text": "breakout"}, "style": "primary", "color": "#1B5E20", "height": "sm", "flex": 1},
                    {"type": "button", "action": {"type": "message", "label": "ATH", "text": "ath"}, "style": "primary", "color": "#E65100", "height": "sm", "flex": 1},
                    {"type": "button", "action": {"type": "message", "label": "VCP", "text": "vcp"}, "style": "primary", "color": "#0D47A1", "height": "sm", "flex": 1},
                ]},
                {"type": "button", "action": {"type": "message", "label": "Stage 2 Stocks", "text": "stage2"}, "style": "secondary", "height": "sm"},
            ],
            "paddingAll": "8px",
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
                _guide_row("Stage 2", "+40 คะแนน", "#27AE60"),
                _guide_row("ATH Breakout", "+25 คะแนน", "#F39C12"),
                _guide_row("Breakout", "+20 คะแนน", "#27AE60"),
                _guide_row("VCP", "+15 คะแนน", "#2980B9"),
                _guide_row("Volume bonus", "สูงสุด +15 คะแนน", "#E67E22"),
                _guide_row("52W proximity", "สูงสุด +20 คะแนน", "#9B59B6"),
                {"type": "separator"},
                {"type": "text", "text": "Volume Ratio", "weight": "bold", "size": "sm", "color": "#F39C12"},
                {"type": "text", "text": "= Volume วันนี้ ÷ ค่าเฉลี่ย 20 วัน\n1.0x ปกติ  |  1.4x+ สูง  |  2.0x+ สูงมาก", "size": "xxs", "color": "#555555", "wrap": True},
            ],
            "paddingAll": "16px",
        },
    }

    quickref_bubble = {
        "type": "bubble",
        "size": "mega",
        "header": {
            "type": "box",
            "layout": "vertical",
            "contents": [{"type": "text", "text": "⚡ Quick Reference", "weight": "bold", "size": "lg", "color": "#FFFFFF"}],
            "backgroundColor": "#4A148C",
            "paddingAll": "16px",
        },
        "body": {
            "type": "box",
            "layout": "vertical",
            "spacing": "xs",
            "contents": [
                _cmd_button("ตลาด", "Market Breadth"),
                _cmd_button("sector", "กลุ่มอุตสาหกรรม"),
                _cmd_button("watchlist", "Watchlist ของคุณ"),
            ],
            "paddingAll": "16px",
        },
    }

    return {"type": "carousel", "contents": [stage_bubble, pattern_bubble, score_vol_bubble, quickref_bubble]}


def build_stage_cycle_card() -> dict:
    """Comprehensive Minervini 4-stage cycle card with trading rules for each stage."""
    def _stage_trading_row(stage_icon: str, stage_name: str, color: str, when_buy: str, action: str) -> list:
        return [
            {
                "type": "box",
                "layout": "horizontal",
                "contents": [
                    {"type": "text", "text": stage_icon, "size": "sm", "flex": 1, "gravity": "center"},
                    {"type": "text", "text": stage_name, "size": "sm", "weight": "bold", "color": color, "flex": 3},
                    {"type": "text", "text": action, "size": "xxs", "color": "#7F8C8D", "flex": 3, "align": "end", "wrap": True},
                ],
            },
            {"type": "text", "text": when_buy, "size": "xxs", "color": "#555555", "wrap": True, "margin": "xs"},
            {"type": "separator"},
        ]

    return {
        "type": "bubble",
        "size": "giga",
        "header": {
            "type": "box",
            "layout": "vertical",
            "contents": [
                {"type": "text", "text": "📊 Minervini 4-Stage Cycle", "weight": "bold", "size": "lg", "color": "#FFFFFF"},
                {"type": "text", "text": "วิธีซื้อขายตาม Stage Analysis", "size": "xs", "color": "#DDDDDD"},
            ],
            "backgroundColor": "#1A237E",
            "paddingAll": "16px",
        },
        "body": {
            "type": "box",
            "layout": "vertical",
            "spacing": "sm",
            "contents": [
                {"type": "text", "text": "วงจร Stage: 1 → 2 → 3 → 4 → 1 (วนซ้ำ)", "size": "xs", "color": "#7F8C8D", "align": "center"},
                {"type": "separator"},
                *_stage_trading_row("⚪", "Stage 1 – Basing", "#95A5A6",
                    "ราคาพักตัว MA200 แบนราบ ยังไม่ทำ breakout\nรอจนกว่าจะเข้า Stage 2 ก่อนซื้อ",
                    "ห้ามซื้อ (รอ)"),
                *_stage_trading_row("🟢", "Stage 2 – Uptrend", "#27AE60",
                    "✅ โซนซื้อ: ราคา > MA150 > MA200 ที่กำลังขึ้น\n"
                    "Entry: Breakout/VCP + Volume ≥ 1.4x avg\n"
                    "Stop Loss: -7–8% จาก entry หรือ ATR × 1.5\n"
                    "Target: R:R 2:1 ขึ้นไป",
                    "ซื้อ/ถือ"),
                *_stage_trading_row("🟡", "Stage 3 – Distribution", "#E67E22",
                    "⚠️ Smart money กำลังขาย ราคาหลุด MA150\n"
                    "ลดขนาด Position และตั้ง Trailing Stop\n"
                    "ห้ามซื้อเพิ่ม ระวัง False breakout",
                    "ขายบางส่วน"),
                *_stage_trading_row("🔴", "Stage 4 – Downtrend", "#E74C3C",
                    "❌ ราคาต่ำกว่า MA150 และ MA200 ที่กำลังลง\n"
                    "ขายออกทั้งหมดถ้ายังถืออยู่\n"
                    "รอให้กลับมา Stage 1 ก่อนสนใจใหม่",
                    "ขายหมด/หลีกเลี่ยง"),
                {"type": "text", "text": "⚠️ Margin: ตัด Loss ทันที -7-8% จาก entry | ห้ามเฉลี่ยลง", "size": "xxs", "color": "#E74C3C", "wrap": True},
            ],
            "paddingAll": "16px",
        },
        "footer": {
            "type": "box",
            "layout": "horizontal",
            "spacing": "sm",
            "contents": [
                {"type": "button", "action": {"type": "message", "label": "Stage 2 Stocks", "text": "stage2"}, "style": "primary", "color": "#1B5E20", "height": "sm", "flex": 1},
                {"type": "button", "action": {"type": "message", "label": "Breakout Stocks", "text": "breakout"}, "style": "primary", "color": "#E65100", "height": "sm", "flex": 1},
            ],
            "paddingAll": "8px",
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

    return {
        "type": "bubble",
        "size": "mega",
        "header": {
            "type": "box",
            "layout": "vertical",
            "contents": [
                {"type": "text", "text": f"{info['icon']} {info['title']}", "weight": "bold", "size": "lg", "color": "#FFFFFF"},
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


def _guide_row(label: str, desc: str, color: str = "#333333") -> dict:
    return {
        "type": "box",
        "layout": "horizontal",
        "contents": [
            {"type": "text", "text": label, "size": "xs", "color": color, "weight": "bold", "flex": 2},
            {"type": "text", "text": desc, "size": "xs", "color": "#555555", "flex": 3, "wrap": True},
        ],
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
            "spacing": "sm",
            "contents": [
                {"type": "text", "text": "ภาพรวมตลาด", "weight": "bold", "size": "sm", "color": "#F39C12"},
                _cmd_button("ตลาด", "Market Breadth"),
                _cmd_button("ดัชนี", "SET50, SET100, MAI, sSET"),
                _cmd_button("sector", "แนวโน้มกลุ่มอุตสาหกรรม"),
                {"type": "separator"},
                {"type": "text", "text": "รายชื่อหุ้น", "weight": "bold", "size": "sm", "color": "#F39C12"},
                _cmd_button("breakout", "Breakout stocks"),
                _cmd_button("ath", "ATH Breakout stocks"),
                _cmd_button("vcp", "VCP Pattern stocks"),
                _cmd_button("stage2", "Stage 2 stocks"),
                _cmd_button("stage1", "Stage 1 stocks"),
                {"type": "separator"},
                {"type": "text", "text": "Watchlist", "weight": "bold", "size": "sm", "color": "#F39C12"},
                _cmd_button("watchlist", "ดู Watchlist"),
                {"type": "separator"},
                {"type": "text", "text": "คู่มือ & อธิบาย", "weight": "bold", "size": "sm", "color": "#F39C12"},
                _cmd_button("guide", "คู่มือ Stage/Pattern/Score"),
                _cmd_button("stage", "เลือก Stage"),
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


def _cmd_button(cmd: str, desc: str) -> dict:
    """Tappable command button — sends cmd text as a message when tapped."""
    send_text = cmd.split(" / ")[0].strip()
    label = f"{send_text}  —  {desc}"
    if len(label) > 40:
        label = label[:39] + "…"
    return {
        "type": "button",
        "action": {"type": "message", "label": label, "text": send_text},
        "style": "link",
        "height": "sm",
        "color": "#2980B9",
    }


# ─── Send helpers ──────────────────────────────────────────────────────────────

def _flex_message(alt_text: str, container: dict) -> FlexMessage:
    return FlexMessage(
        alt_text=alt_text,
        contents=FlexContainer.from_dict(container),
    )


def reply_flex(reply_token: str, alt_text: str, container: dict) -> None:
    """Reply to a LINE message with a Flex Message."""
    api = _get_api()
    try:
        api.reply_message(
            ReplyMessageRequest(
                reply_token=reply_token,
                messages=[_flex_message(alt_text, container)],
            )
        )
    except Exception as exc:
        logger.error("Failed to reply flex: %s", exc)


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
