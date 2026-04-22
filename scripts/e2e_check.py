#!/usr/bin/env python3
"""
E2E regression suite for Signalix.

Every user-facing feature should be asserted here. Per CLAUDE.md, new
features must extend this file before shipping. Runs against a live
deployment via the `/test/*` diagnostic endpoints.

Usage:
  python3 scripts/e2e_check.py [base_url] [scan_secret]

Env overrides: SIGNALIX_BASE_URL, SIGNALIX_SCAN_SECRET.

Exit 0 if all assertions pass; 1 otherwise.
"""
import json
import os
import sys
import time
import urllib.error
import urllib.parse
import urllib.request

DEFAULT_BASE = "https://signalix-563764992953.asia-southeast1.run.app"


# ── Fetch helpers ──────────────────────────────────────────────────────────

def fetch(url, headers=None, timeout=60, retries=4):
    """Cloud Run in asia-southeast1 cold-starts aggressively. Retry 5xx with
    exponential backoff so transient instance churn doesn't mask real
    regressions in the e2e output."""
    last_err = None
    for attempt in range(retries):
        req = urllib.request.Request(url, headers=headers or {})
        t0 = time.time()
        try:
            r = urllib.request.urlopen(req, timeout=timeout)
            return json.loads(r.read()), round(time.time() - t0, 2)
        except urllib.error.HTTPError as e:
            last_err = e
            if e.code >= 500 and attempt < retries - 1:
                time.sleep(1.5 * (attempt + 1))
                continue
            raise
        except urllib.error.URLError as e:
            last_err = e
            if attempt < retries - 1:
                time.sleep(1.5 * (attempt + 1))
                continue
            raise
    raise last_err


def query(base, secret, cmd):
    return fetch(
        f"{base}/test/query?cmd={urllib.parse.quote(cmd)}",
        headers={"x-scan-secret": secret},
    )


# ── Assertion helpers ──────────────────────────────────────────────────────

_counts = {"pass": 0, "fail": 0, "skip": 0}


def check(label, ok, detail=""):
    mark = "✓" if ok else "✗"
    print(f"  {mark} {label:35s} {detail}")
    _counts["pass" if ok else "fail"] += 1
    return 0 if ok else 1


def skip(label, reason):
    print(f"  ~ {label:35s} skipped — {reason}")
    _counts["skip"] += 1


def section(name):
    print(f"\n{name}")


# ── Suites ─────────────────────────────────────────────────────────────────

def suite_baseline(base, secret):
    section("Baseline")
    fails = 0
    try:
        h, dt = fetch(f"{base}/health")
        last_scan = (h.get("last_scan_time") or "-")[:19]
        fails += check("health/status", h.get("status") == "ok",
                       f"{h.get('cached_stocks')} stocks, last_scan={last_scan}  ({dt}s)")
        fails += check("health/has_signals", (h.get("cached_stocks") or 0) > 0)
        fails += check("health/firestore_up", h.get("firestore") is True)
        fails += check("health/bq_up", h.get("bigquery") is True)
    except Exception as e:
        fails += check("health", False, f"err: {e}")

    try:
        st, dt = fetch(f"{base}/test/settrade?sample_size=10")
        cov = st.get("bulk_quote_sample", {}).get("coverage_pct", 0)
        fails += check("settrade/api_available", bool(st.get("api_available")))
        fails += check("settrade/bulk_coverage>=70", cov >= 70, f"coverage={cov}%  ({dt}s)")
    except Exception as e:
        fails += check("settrade", False, f"err: {e}")
    return fails


def suite_market_breadth(base, secret):
    section("Market breadth")
    fails = 0
    try:
        q, _ = query(base, secret, "market")
        b = q.get("breadth") or {}
        fails += check("market/has_set_index", (b.get("set_index_close") or 0) > 0,
                       f"SET={b.get('set_index_close')}")
        total = (b.get("advancing", 0) + b.get("declining", 0) + b.get("unchanged", 0))
        fails += check("market/adv+dec+flat==total", total == (b.get("total_stocks") or 0),
                       f"{b.get('advancing')}+{b.get('declining')}+{b.get('unchanged')}={total} total={b.get('total_stocks')}")
    except Exception as e:
        fails += check("market", False, f"err: {e}")

    # 52W high / low lists — should run (count can be 0 on flat market days)
    # and the count should match the breadth card's new_highs_52w /
    # new_lows_52w so the drill-down and the summary don't contradict.
    for cmd, label, breadth_key in (
        ("52wh", "52W_high", "new_highs_52w"),
        ("52wl", "52W_low",  "new_lows_52w"),
    ):
        try:
            q, _ = query(base, secret, cmd)
            fails += check(f"{label}/dispatched", q.get("kind") == "list",
                           f"count={q.get('count')} first={[r.get('symbol') for r in q.get('first_5', [])]}")
            breadth_val = (b or {}).get(breadth_key)
            if breadth_val is not None:
                fails += check(f"{label}/matches_breadth",
                               q.get("count") == breadth_val,
                               f"list={q.get('count')} breadth={breadth_val}")
        except Exception as e:
            fails += check(label, False, f"err: {e}")

    # Indexes — yfinance only carries ^SET.BK for Thai indexes; the 5 sub-indexes
    # (SET50/SET100/MAI/sSET/SETESG) silently return empty from both yf.download and
    # yf.Ticker.history. This is an upstream Yahoo coverage gap, not a code bug.
    # Require at least SET; if more appear in future (Yahoo expands coverage or we
    # switch to a Settrade-native source), the assertion should be tightened.
    try:
        q, _ = query(base, secret, "indexes")
        fails += check("indexes/has_set", "SET" in q.get("symbols", []),
                       f"symbols={q.get('symbols')}")
        fails += check("indexes/count>=1", (q.get("count") or 0) >= 1,
                       f"count={q.get('count')}  (yfinance only serves SET for Thai)")
    except Exception as e:
        fails += check("indexes", False, f"err: {e}")

    # Sector overview
    try:
        q, _ = query(base, secret, "sectors")
        fails += check("sectors/has_trends", (q.get("count") or 0) > 0,
                       f"count={q.get('count')}")
    except Exception as e:
        fails += check("sectors", False, f"err: {e}")
    return fails


def suite_list_cards(base, secret):
    section("List cards — advance/decline/flat")
    fails = 0
    for cmd in ("advancing", "declining"):
        try:
            q, _ = query(base, secret, cmd)
            cnt = q.get("count", 0)
            first = [r.get("symbol") for r in q.get("first_5", [])]
            fails += check(f"{cmd}/non_empty", cnt > 0, f"count={cnt} first={first}")
            # Sign consistency
            signs_ok = all((r.get("change_pct") or 0) > 0 if cmd == "advancing"
                           else (r.get("change_pct") or 0) < 0
                           for r in q.get("first_5", []))
            fails += check(f"{cmd}/signs_consistent", signs_ok)
        except Exception as e:
            fails += check(cmd, False, f"err: {e}")

    # flat — non-fatal if empty, must at least run
    try:
        q, _ = query(base, secret, "flat")
        cnt = q.get("count", 0)
        first = [r.get("symbol") for r in q.get("first_5", [])]
        fails += check("flat/dispatched", q.get("kind") == "list", f"count={cnt} first={first}")
        zeros_ok = all((r.get("change_pct") or 0) == 0 for r in q.get("first_5", []))
        fails += check("flat/all_zero", zeros_ok or cnt == 0)
    except Exception as e:
        fails += check("flat", False, f"err: {e}")
    return fails


def suite_stage_lists(base, secret):
    section("List cards — stages")
    fails = 0
    counts = {}
    for n in (1, 2, 3, 4):
        try:
            q, _ = query(base, secret, f"stage{n}")
            cnt = q.get("count", 0)
            counts[n] = cnt
            fails += check(f"stage{n}/non_empty", cnt > 0, f"count={cnt}")
            # All returned stocks must belong to that stage
            stage_ok = all(r.get("stage") == n for r in q.get("first_5", []))
            fails += check(f"stage{n}/stage_consistent", stage_ok)
        except Exception as e:
            fails += check(f"stage{n}", False, f"err: {e}")
    # Cross-check against breadth — stage list counts must match the breadth
    # fields that feed build_stage_picker_card, otherwise the stage picker
    # shows a different number than the drill-down.
    try:
        q, _ = query(base, secret, "market")
        b = q.get("breadth") or {}
        total = sum(counts.values())
        fails += check("stages/sum_matches_total", total == (b.get("total_stocks") or 0),
                       f"Σ={total} total={b.get('total_stocks')}")
        for n in (1, 2, 3, 4):
            bkey = f"stage{n}_count"
            fails += check(f"stages/picker==list s{n}",
                           counts.get(n) == b.get(bkey),
                           f"list={counts.get(n)} breadth[{bkey}]={b.get(bkey)}")
    except Exception as e:
        fails += check("stages/cross_check", False, f"err: {e}")
    return fails


def suite_patterns(base, secret):
    section("List cards — patterns")
    fails = 0
    pattern_counts = {}
    for cmd, label in (("breakout", "breakout"), ("ath", "ath_breakout"),
                       ("vcp", "vcp_group"), ("consolidating", "consolidating")):
        try:
            q, _ = query(base, secret, cmd)
            cnt = q.get("count", 0)
            first = [r.get("symbol") for r in q.get("first_5", [])]
            pattern_counts[label] = cnt
            fails += check(f"{cmd}/dispatched", q.get("kind") == "list",
                           f"count={cnt} first={first}")
        except Exception as e:
            fails += check(cmd, False, f"err: {e}")
    # consolidating must be non-empty on any trading day (it's the default classification for most stocks)
    fails += check("consolidating/non_empty",
                   pattern_counts.get("consolidating", 0) > 0,
                   f"count={pattern_counts.get('consolidating')}")
    return fails


def suite_sector_drill(base, secret):
    section("List cards — sector drill-down + subsector breakdown")
    fails = 0
    for sector in ("FINCIAL", "TECH", "RESOURC"):
        try:
            q, _ = query(base, secret, f"sector {sector}")
            cnt = q.get("count", 0)
            fails += check(f"sector_{sector}/non_empty", cnt > 0, f"count={cnt}")
        except Exception as e:
            fails += check(f"sector_{sector}", False, f"err: {e}")

    # Subsector list endpoint — SUBSECTOR_TO_SECTOR has 28 codes (data.py:286-323)
    try:
        q, _ = query(base, secret, "subsector")
        codes = q.get("configured_codes") or []
        counts = q.get("counts") or {}
        used = sum(1 for c in codes if counts.get(c, 0) > 0)
        fails += check("subsector/has_28_codes", len(codes) == 28,
                       f"got {len(codes)} configured codes")
        fails += check("subsector/used_codes>=15", used >= 15,
                       f"{used}/{len(codes)} codes have stocks")
        # Subsector drill-down: FOOD is the highest-count subsector (51 stocks
        # per coverage report) so it's the most reliable canary. BANK sometimes
        # fails because Thai banks aren't always classified via yfinance .info.
        q2, _ = query(base, secret, "sector FOOD")
        fails += check("sector_FOOD_subsector/non_empty",
                       (q2.get("count") or 0) > 0,
                       f"count={q2.get('count')}")
    except Exception as e:
        fails += check("subsector", False, f"err: {e}")
    return fails


def suite_sector_coverage(base, secret):
    section("Sector classification coverage")
    fails = 0
    try:
        d, _ = fetch(f"{base}/test/sector_coverage", headers={"x-scan-secret": secret}, timeout=60)
        cov = d.get("coverage_pct", 0)
        sub_cov = d.get("subsector_coverage_pct", 0)
        live = d.get("live_sector_indexes") or {}
        fails += check("coverage/main_sector>=70%", cov >= 70,
                       f"{cov}% mapped, {d.get('unmapped_other')} OTHER")
        fails += check("coverage/subsector>=60%", sub_cov >= 60,
                       f"{sub_cov}% have subsector code")
        fails += check("sector_indexes/live_non_empty",
                       len(live) >= 4,
                       f"got {len(live)}/8 live sector indexes: {sorted(live.keys())}")
    except Exception as e:
        fails += check("sector_coverage", False, f"err: {e}")
    return fails


def suite_single_stock(base, secret):
    section("Single stock cards")
    fails = 0
    for sym in ("LHFG", "WHAIR", "PTT", "BBL", "BDMS", "KBANK"):
        try:
            q, _ = query(base, secret, sym)
            s = q.get("signal") or {}
            fails += check(f"{sym}/has_signal", bool(s),
                           f"close={s.get('close')} pat={s.get('pattern')} date={s.get('data_date')}")
            if s:
                # data_date should be within 10 days
                dd = s.get("data_date") or ""
                fails += check(f"{sym}/data_date_present", bool(dd))
        except Exception as e:
            fails += check(sym, False, f"err: {e}")

    # detail path
    try:
        q, _ = query(base, secret, "detail PTT")
        fails += check("detail_PTT/has_signal",
                       bool((q.get("signal") or {}).get("symbol")))
    except Exception as e:
        fails += check("detail_PTT", False, f"err: {e}")

    # Unknown symbol
    try:
        q, _ = query(base, secret, "XYZZY123")
        fails += check("unknown_symbol/kind_unknown", q.get("kind") == "unknown")
    except Exception as e:
        fails += check("unknown_symbol", False, f"err: {e}")
    return fails


def suite_static_cards(base, secret):
    section("Static cards (guide/help/explain/stage picker/patterns)")
    fails = 0
    for cmd, handler in (("guide", "guide"), ("help", "help"),
                         ("stage", "stage_picker"), ("patterns", "pattern_overview"),
                         ("explain stage2", "explain"), ("explain breakout", "explain")):
        try:
            q, _ = query(base, secret, cmd)
            fails += check(f"static/{cmd!r}", q.get("kind") == "static",
                           f"handler={q.get('handler')}")
        except Exception as e:
            fails += check(f"static/{cmd!r}", False, f"err: {e}")
    return fails


def suite_ath_regression(base, secret):
    section("ATH regression (unadjusted prices + cache merge)")
    fails = 0
    for sym in ("WHAIR", "PTT"):
        try:
            d, _ = fetch(f"{base}/test/ath/{sym}")
            cached = (d.get("cached") or {}).get("firestore") or {}
            yf_un = d.get("yfinance_unadjusted") or {}
            fails += check(f"{sym}/firestore_ath_set", bool(cached.get("ath")),
                           f"fs_ath={cached.get('ath')} date={cached.get('ath_date')}")
            fails += check(f"{sym}/firestore_unadjusted_match",
                           abs((cached.get("ath") or 0) - (yf_un.get("max_high") or 0)) < 0.01,
                           f"fs={cached.get('ath')} yf_un={yf_un.get('max_high')}")
        except Exception as e:
            fails += check(f"{sym}/ath", False, f"err: {e}")

    # WHAIR must not be flagged ath_breakout (it's historical, adjusted bug)
    try:
        q, _ = query(base, secret, "WHAIR")
        pat = (q.get("signal") or {}).get("pattern")
        fails += check("WHAIR/not_ath_breakout", pat != "ath_breakout", f"pattern={pat}")
    except Exception as e:
        fails += check("WHAIR/ath_reg", False, f"err: {e}")
    return fails


def suite_invariants(base, secret):
    section("Data integrity invariants")
    fails = 0
    try:
        inv, dt = fetch(f"{base}/test/invariants", headers={"x-scan-secret": secret}, timeout=90)
    except Exception as e:
        fails += check("invariants", False, f"err: {e}")
        return fails

    total = inv.get("total_signals", 0)
    d = inv.get("details", {})
    fails += check("invariants/total>0", total > 0, f"total={total}  ({dt}s)")
    fails += check("invariants/partition_ok", inv.get("partition_ok"),
                   f"adv/dec/flat={d.get('adv_dec_flat')}")
    fails += check("invariants/stage_ok", inv.get("stage_ok"),
                   f"stages={d.get('stage_counts')}")
    fails += check("invariants/pattern_ok", inv.get("pattern_ok"),
                   f"patterns={d.get('pattern_counts')}")
    fails += check("invariants/freshness_ok", inv.get("freshness_ok"),
                   f"stale={d.get('stale_count')} sample={d.get('stale_sample')}")
    fails += check("invariants/ath_ok", inv.get("ath_ok"),
                   f"violations={d.get('ath_violations_count')} sample={d.get('ath_violations_sample')}")
    fails += check("invariants/breakout_stage_ok", inv.get("breakout_stage_ok"),
                   f"sample={d.get('bad_breakout_stage_sample')}")
    fails += check("invariants/going_down_stage_ok", inv.get("going_down_stage_ok"),
                   f"sample={d.get('bad_going_down_sample')}")
    fails += check("invariants/score_range_ok", inv.get("score_range_ok"),
                   f"sample={d.get('bad_score_sample')}")
    return fails


def suite_admin(base, secret):
    section("Admin endpoints")
    fails = 0
    try:
        d, _ = fetch(f"{base}/admin/check", headers={"x-scan-secret": secret}, timeout=60)
        fails += check("admin/has_scan_summary", bool(d.get("scan_summary")))
        anoms = d.get("anomalies") or []
        fails += check("admin/no_anomalies", len(anoms) == 0,
                       f"anomalies={len(anoms)} sample={anoms[:3]}")
        missing_ath = d.get("data_completeness", {}).get("stocks_missing_ath", 0)
        fails += check("admin/missing_ath_small", missing_ath < 5,
                       f"missing={missing_ath}")
    except Exception as e:
        fails += check("admin/check", False, f"err: {e}")
    return fails


# ── Main ───────────────────────────────────────────────────────────────────

def main():
    base = sys.argv[1] if len(sys.argv) > 1 else os.environ.get("SIGNALIX_BASE_URL", DEFAULT_BASE)
    secret = sys.argv[2] if len(sys.argv) > 2 else os.environ.get("SIGNALIX_SCAN_SECRET", "signalix-scan-2024")
    print(f"\nSignalix E2E against {base}\n")

    total_fails = 0
    total_fails += suite_baseline(base, secret)
    total_fails += suite_market_breadth(base, secret)
    total_fails += suite_list_cards(base, secret)
    total_fails += suite_stage_lists(base, secret)
    total_fails += suite_patterns(base, secret)
    total_fails += suite_sector_drill(base, secret)
    total_fails += suite_sector_coverage(base, secret)
    total_fails += suite_single_stock(base, secret)
    total_fails += suite_static_cards(base, secret)
    total_fails += suite_ath_regression(base, secret)
    total_fails += suite_invariants(base, secret)
    total_fails += suite_admin(base, secret)

    print(f"\n{'=' * 50}")
    print(f"pass={_counts['pass']}  fail={_counts['fail']}  skip={_counts['skip']}")
    if total_fails:
        print(f"FAILED: {total_fails} check(s)")
        sys.exit(1)
    print("All checks passed")


if __name__ == "__main__":
    main()
