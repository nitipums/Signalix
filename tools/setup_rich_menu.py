#!/usr/bin/env python3
"""
tools/setup_rich_menu.py — Create/update LINE Rich Menu for Signalix.

Usage:
  python tools/setup_rich_menu.py --token YOUR_LINE_CHANNEL_ACCESS_TOKEN

Menu layout (2 rows × 3 cols):
  ┌──────────────┬──────────────┬──────────────┐
  │  📊 ตลาด     │  📈 Stage    │  📌 Watch    │
  │  (ตลาด)      │  (stage)     │  (watchlist) │
  ├──────────────┼──────────────┼──────────────┤
  │  📖 Help     │  🎓 Guide    │  🔔 Subscribe│
  │  (help)      │  (guide)     │  (subscribe) │
  └──────────────┴──────────────┴──────────────┘
"""

import argparse
import io
import json
import sys

import httpx

# Rich menu dimensions (LINE requirement: width must be ≥ 800px, height 250–1686px)
MENU_WIDTH = 2500
MENU_HEIGHT = 843
COL_W = MENU_WIDTH // 3
ROW_H = MENU_HEIGHT // 2

# 6 slots: row 0-1, col 0-2
SLOTS = [
    {"row": 0, "col": 0, "icon": "📊", "label": "ตลาด",      "cmd": "ตลาด"},
    {"row": 0, "col": 1, "icon": "📈", "label": "Stage",     "cmd": "stage"},
    {"row": 0, "col": 2, "icon": "📌", "label": "Watch",     "cmd": "watchlist"},
    {"row": 1, "col": 0, "icon": "📖", "label": "Help",      "cmd": "help"},
    {"row": 1, "col": 1, "icon": "🎓", "label": "Guide",     "cmd": "guide"},
    {"row": 1, "col": 2, "icon": "🔔", "label": "Subscribe", "cmd": "subscribe"},
]

AREA_COLORS = ["#1A237E", "#0D47A1", "#1B5E20", "#4A148C", "#880E4F", "#E65100"]
TEXT_COLOR = "#FFFFFF"


def _build_areas() -> list[dict]:
    areas = []
    for slot in SLOTS:
        x = slot["col"] * COL_W
        y = slot["row"] * ROW_H
        areas.append({
            "bounds": {"x": x, "y": y, "width": COL_W, "height": ROW_H},
            "action": {"type": "message", "label": slot["label"], "text": slot["cmd"]},
        })
    return areas


def _generate_image() -> bytes:
    """Generate a simple PNG background image for the rich menu using Pillow."""
    try:
        from PIL import Image, ImageDraw, ImageFont
    except ImportError:
        print("Pillow not installed — run: pip install Pillow")
        sys.exit(1)

    img = Image.new("RGB", (MENU_WIDTH, MENU_HEIGHT), "#0D0D1A")
    draw = ImageDraw.Draw(img)

    for i, slot in enumerate(SLOTS):
        x0 = slot["col"] * COL_W
        y0 = slot["row"] * ROW_H
        x1 = x0 + COL_W - 2
        y1 = y0 + ROW_H - 2
        color = AREA_COLORS[i]
        draw.rectangle([x0 + 4, y0 + 4, x1 - 4, y1 - 4], fill=color)

        # Try to use a TTF font, fall back to default
        icon_font = default_font = None
        try:
            icon_font = ImageFont.truetype("/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf", 80)
            default_font = ImageFont.truetype("/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf", 55)
        except Exception:
            icon_font = ImageFont.load_default()
            default_font = ImageFont.load_default()

        cx = x0 + COL_W // 2
        cy = y0 + ROW_H // 2

        # Icon (fallback to ASCII if emoji render broken)
        draw.text((cx, cy - 70), slot["icon"], font=icon_font, fill=TEXT_COLOR, anchor="mm")
        draw.text((cx, cy + 30), slot["label"], font=default_font, fill=TEXT_COLOR, anchor="mm")

    buf = io.BytesIO()
    img.save(buf, format="PNG")
    return buf.getvalue()


def _upload_image(api_base: str, headers: dict, rich_menu_id: str, image_bytes: bytes) -> None:
    resp = httpx.post(
        f"{api_base}/richmenu/{rich_menu_id}/content",
        headers={**headers, "Content-Type": "image/png"},
        content=image_bytes,
        timeout=30,
    )
    resp.raise_for_status()
    print(f"  Image uploaded ({len(image_bytes)//1024} KB)")


def setup_rich_menu(token: str) -> None:
    api_base = "https://api.line.me/v2/bot"
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}

    # ── Delete existing rich menus ──
    r = httpx.get(f"{api_base}/richmenu/list", headers=headers, timeout=10)
    r.raise_for_status()
    for rm in r.json().get("richmenus", []):
        httpx.delete(f"{api_base}/richmenu/{rm['richMenuId']}", headers=headers, timeout=10)
        print(f"  Deleted old rich menu: {rm['richMenuId']}")

    # ── Create new rich menu ──
    payload = {
        "size": {"width": MENU_WIDTH, "height": MENU_HEIGHT},
        "selected": True,
        "name": "Signalix Main Menu",
        "chatBarText": "เมนู Signalix",
        "areas": _build_areas(),
    }
    r = httpx.post(f"{api_base}/richmenu", headers=headers, content=json.dumps(payload), timeout=10)
    r.raise_for_status()
    rich_menu_id = r.json()["richMenuId"]
    print(f"  Created rich menu: {rich_menu_id}")

    # ── Generate and upload image ──
    print("  Generating menu image...")
    image_bytes = _generate_image()
    _upload_image(api_base, {"Authorization": f"Bearer {token}"}, rich_menu_id, image_bytes)

    # ── Set as default ──
    r = httpx.post(f"{api_base}/user/all/richmenu/{rich_menu_id}", headers=headers, timeout=10)
    r.raise_for_status()
    print(f"  Set as default rich menu for all users")
    print(f"\nDone! Rich menu ID: {rich_menu_id}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Setup Signalix LINE Rich Menu")
    parser.add_argument("--token", required=True, help="LINE Channel Access Token")
    args = parser.parse_args()
    setup_rich_menu(args.token)


if __name__ == "__main__":
    main()
