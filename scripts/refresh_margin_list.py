#!/usr/bin/env python3
"""Refresh data_static/margin_securities.json from the latest Krungsri
Securities Marginable Securities List PDF.

Krungsri publishes a new PDF roughly weekly with a date-stamped filename
under https://www.krungsrisecurities.com/upload/. This script can:
  • parse a local PDF you've downloaded (--pdf path/to/file.pdf), or
  • fetch a URL directly (--url https://...)

Outputs the parsed JSON to data_static/margin_securities.json (overwrites).
Always commit the JSON change so deploys pick it up — there's no live
fetch in the running service.

Usage:
    python3 scripts/refresh_margin_list.py --url https://www.krungsrisecurities.com/upload/Marginable_Securities_List_DDMMYYYY_xxx.pdf
    python3 scripts/refresh_margin_list.py --pdf ./downloads/latest.pdf
    python3 scripts/refresh_margin_list.py --pdf ./latest.pdf --as-of 2025-04-09

Dependencies (one-time):
    pip install pdfplumber

Source: https://www.krungsrisecurities.com  → "บริการมาร์จิ้น" → "หลักทรัพย์ที่ให้กู้ยืม"
"""
from __future__ import annotations

import argparse
import json
import re
import sys
import urllib.request
from datetime import date
from pathlib import Path


REPO = Path(__file__).resolve().parent.parent
JSON_OUT = REPO / "data_static" / "margin_securities.json"

TIER_PAT = re.compile(r"^IM(\d+)%")
SYM_PAT  = re.compile(r"(?:^|\s)\d+\s+([A-Z][A-Z0-9\-]*)(\*{0,2})")
# Tiers we keep — IM100 means non-marginable; represent as ABSENCE from the
# dict so consumers know "no margin available" without storing zeros.
KEEP_TIERS = {50, 60, 70, 80}


def parse_pdf(pdf_path: Path) -> dict:
    """Extract {symbol: {im_pct, short_sell}} from the Krungsri PDF."""
    try:
        import pdfplumber
    except ImportError:
        print("ERROR: pdfplumber not installed. Run: pip install pdfplumber",
              file=sys.stderr)
        sys.exit(1)

    with pdfplumber.open(pdf_path) as pdf:
        text = "\n".join(p.extract_text() or "" for p in pdf.pages)

    current_tier: int | None = None
    data: dict[str, dict] = {}
    for line in text.split("\n"):
        m = TIER_PAT.match(line.strip())
        if m:
            current_tier = int(m.group(1))
            continue
        if current_tier is None:
            continue
        for sm in SYM_PAT.finditer(line):
            sym = sm.group(1)
            ast = len(sm.group(2))
            if sym == "IM" or current_tier not in KEEP_TIERS:
                continue
            data[sym] = {"im_pct": current_tier, "short_sell": ast >= 2}
    return data


def main() -> int:
    ap = argparse.ArgumentParser()
    src = ap.add_mutually_exclusive_group(required=True)
    src.add_argument("--pdf", type=Path, help="Path to local PDF")
    src.add_argument("--url", type=str, help="Direct URL to Krungsri PDF")
    ap.add_argument("--as-of", default=str(date.today()),
                    help="Effective date (YYYY-MM-DD); default today")
    ap.add_argument("--source-name", default="Krungsri Securities",
                    help="Source label for the JSON metadata")
    args = ap.parse_args()

    if args.url:
        # Download to a temp file
        tmp = REPO / "data_static" / "_latest.pdf"
        print(f"Fetching {args.url} ...")
        urllib.request.urlretrieve(args.url, tmp)
        pdf_path = tmp
        source_label = f"{args.source_name} — {args.url}"
    else:
        pdf_path = args.pdf
        source_label = f"{args.source_name} — {pdf_path.name}"

    print(f"Parsing {pdf_path} ...")
    data = parse_pdf(pdf_path)
    if not data:
        print("ERROR: no securities parsed — PDF format may have changed",
              file=sys.stderr)
        return 2

    tier_counts: dict[int, int] = {}
    short_count = 0
    for v in data.values():
        tier_counts[v["im_pct"]] = tier_counts.get(v["im_pct"], 0) + 1
        if v["short_sell"]:
            short_count += 1

    out = {
        "as_of": args.as_of,
        "source": source_label,
        "source_url": args.url or "",
        "notes": (
            "im_pct = Initial Margin requirement %. Lower = more leverage. "
            "IM50 → up to 2.0× leverage. IM60 → 1.67×. IM70 → 1.43×. IM80 → 1.25×. "
            "Symbols not in this dict are NOT marginable. "
            "short_sell=True (was '**' in PDF) indicates short-sell eligibility."
        ),
        "securities": data,
    }
    JSON_OUT.parent.mkdir(parents=True, exist_ok=True)
    with JSON_OUT.open("w", encoding="utf-8") as f:
        json.dump(out, f, indent=2, ensure_ascii=False)

    print(f"\n  Wrote {len(data)} securities to {JSON_OUT.relative_to(REPO)}")
    print(f"  Tiers: {dict(sorted(tier_counts.items()))}")
    print(f"  Short-sell eligible: {short_count}")
    print(f"\n  → git add {JSON_OUT.relative_to(REPO)} && commit + deploy")

    # Clean up temp PDF if downloaded
    if args.url:
        tmp.unlink(missing_ok=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
