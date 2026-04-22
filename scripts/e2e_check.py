#!/usr/bin/env python3
"""
E2E smoke test against a live Signalix deployment.

Exercises every user-facing command path via GET /test/query and prints
pass/fail per card. Designed to run after a deploy to catch regressions
in filter logic, stale state, or missing handlers.

Usage:
  python3 scripts/e2e_check.py [base_url] [scan_secret]

Env overrides: SIGNALIX_BASE_URL, SIGNALIX_SCAN_SECRET.
"""
import json
import os
import sys
import time
import urllib.request

DEFAULT_BASE = "https://signalix-563764992953.asia-southeast1.run.app"


def fetch(url, headers=None, timeout=60):
    req = urllib.request.Request(url, headers=headers or {})
    t0 = time.time()
    r = urllib.request.urlopen(req, timeout=timeout)
    return json.loads(r.read()), round(time.time() - t0, 2)


def query(base, secret, cmd):
    from urllib.parse import quote
    return fetch(f"{base}/test/query?cmd={quote(cmd)}", headers={"x-scan-secret": secret})


def check(label, ok, detail=""):
    mark = "✓" if ok else "✗"
    print(f"  {mark} {label:30s} {detail}")
    return 1 if ok else 0


def main():
    base = sys.argv[1] if len(sys.argv) > 1 else os.environ.get("SIGNALIX_BASE_URL", DEFAULT_BASE)
    secret = sys.argv[2] if len(sys.argv) > 2 else os.environ.get("SIGNALIX_SCAN_SECRET", "signalix-scan-2024")

    print(f"\nSignalix E2E check against {base}\n")
    fails = 0

    # ── Baseline endpoints ──
    print("Baseline")
    try:
        h, dt = fetch(f"{base}/health")
        fails += check("health/status", h.get("status") == "ok", f"{h.get('cached_stocks')} stocks, last_scan={h.get('last_scan_time','-')[:19]}  ({dt}s)")
        fails += check("health/has_signals", (h.get("cached_stocks") or 0) > 0)
    except Exception as e:
        fails += check("health", False, f"err: {e}")

    try:
        st, dt = fetch(f"{base}/test/settrade?sample_size=10")
        cov = st.get("bulk_quote_sample", {}).get("coverage_pct", 0)
        fails += check("settrade/api_available", bool(st.get("api_available")))
        fails += check("settrade/bulk_coverage", cov >= 70, f"coverage={cov}%  ({dt}s)")
    except Exception as e:
        fails += check("settrade", False, f"err: {e}")

    # ── Market breadth ──
    print("\nMarket breadth")
    try:
        q, dt = query(base, secret, "market")
        b = q.get("breadth") or {}
        fails += check("market/has_set_index", (b.get("set_index_close") or 0) > 0, f"SET={b.get('set_index_close')}")
        fails += check("market/has_advdec_sum", (b.get("advancing", 0) + b.get("declining", 0) + b.get("unchanged", 0)) > 0)
    except Exception as e:
        fails += check("market", False, f"err: {e}")

    # ── List cards — must not be silently empty when breadth says otherwise ──
    print("\nList cards")
    for cmd in ["advancing", "declining", "flat"]:
        try:
            q, dt = query(base, secret, cmd)
            cnt = q.get("count", 0)
            first = [r.get("symbol") for r in q.get("first_5", [])]
            # advancing/declining should almost always have >0 during market day;
            # flat can be 0 depending on data shape — report but don't fail.
            if cmd == "flat":
                fails += check(f"{cmd}/runs", q.get("kind") == "list", f"count={cnt} first={first}")
            else:
                fails += check(f"{cmd}/non_empty", cnt > 0, f"count={cnt} first={first}")
        except Exception as e:
            fails += check(cmd, False, f"err: {e}")

    for stage in ("stage1", "stage2", "stage3", "stage4"):
        try:
            q, dt = query(base, secret, stage)
            cnt = q.get("count", 0)
            fails += check(f"{stage}/non_empty", cnt > 0, f"count={cnt}")
        except Exception as e:
            fails += check(stage, False, f"err: {e}")

    # Pattern lists: can legitimately be empty on some days (no breakouts).
    # Report without failing, but flag if runs at all.
    for pattern in ("breakout", "ath", "vcp", "consolidating"):
        try:
            q, dt = query(base, secret, pattern)
            cnt = q.get("count", 0)
            first = [r.get("symbol") for r in q.get("first_5", [])]
            fails += check(f"{pattern}/runs", q.get("kind") == "list", f"count={cnt} first={first}")
        except Exception as e:
            fails += check(pattern, False, f"err: {e}")

    # ── Sector ──
    try:
        q, dt = query(base, secret, "sector FINCIAL")
        fails += check("sector_FINCIAL/non_empty", q.get("count", 0) > 0, f"count={q.get('count')}")
    except Exception as e:
        fails += check("sector_FINCIAL", False, f"err: {e}")

    # ── Single-stock cards ──
    print("\nSingle stock cards")
    for sym in ("LHFG", "WHAIR", "PTT", "BBL"):
        try:
            q, dt = query(base, secret, sym)
            s = q.get("signal") or {}
            fails += check(f"{sym}/has_signal", bool(s), f"close={s.get('close')} pattern={s.get('pattern')} data_date={s.get('data_date')}")
        except Exception as e:
            fails += check(sym, False, f"err: {e}")

    # ── ATH sanity: WHAIR shouldn't be ath_breakout any more ──
    print("\nATH correctness")
    try:
        q, dt = query(base, secret, "WHAIR")
        pattern = (q.get("signal") or {}).get("pattern")
        fails += check("WHAIR/not_ath_breakout", pattern != "ath_breakout", f"pattern={pattern}")
    except Exception as e:
        fails += check("WHAIR/ath", False, f"err: {e}")

    print(f"\n{'=' * 50}")
    if fails:
        print(f"FAILED: {fails} check(s) failed")
        sys.exit(1)
    print("All checks passed")


if __name__ == "__main__":
    main()
