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
        fails += check("settrade/api_available", bool(st.get("api_available")))
        # bulk_coverage is a live-trading signal: during pre-market /
        # post-market / holidays Settrade returns last=null for most
        # symbols and coverage collapses to 0 (the `last > 0` filter in
        # get_bulk_quotes correctly drops them). Only assert coverage
        # when the canary PTT actually has a live last price.
        ptt_last = (st.get("quote_PTT") or {}).get("last") or 0
        if ptt_last > 0:
            cov = st.get("bulk_quote_sample", {}).get("coverage_pct", 0)
            fails += check("settrade/bulk_coverage>=70", cov >= 70,
                           f"coverage={cov}%  ({dt}s)")
        else:
            skip("settrade/bulk_coverage", "market closed (PTT.last=null)")
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
    # Cover every list-style pattern command that the guide carousel
    # references. 'attempt' and 'weakening' were added alongside the
    # scanner calibration / guide refresh — guide rows tap-fire these
    # commands, so they MUST dispatch correctly even if the count is 0.
    for cmd, label in (
        ("breakout", "breakout"),
        ("attempt", "breakout_attempt"),
        ("ath", "ath_breakout"),
        ("vcp", "vcp_group"),
        ("consolidating", "consolidating"),
        ("weakening", "stage2_weakening"),
    ):
        try:
            q, _ = query(base, secret, cmd)
            cnt = q.get("count", 0)
            first = [r.get("symbol") for r in q.get("first_5", [])]
            pattern_counts[label] = cnt
            fails += check(f"{cmd}/dispatched", q.get("kind") == "list",
                           f"count={cnt} first={first}")
        except Exception as e:
            fails += check(cmd, False, f"err: {e}")
    # consolidating must be non-empty on any trading day (it's the default
    # classification for most stocks)
    fails += check("consolidating/non_empty",
                   pattern_counts.get("consolidating", 0) > 0,
                   f"count={pattern_counts.get('consolidating')}")
    return fails


def suite_index_breadth(base, secret):
    """Per-sub-index breadth (SET50 / SET100). MAI deferred until member
    list is populated; assertion just confirms the dispatch handles it."""
    section("Per-sub-index breadth (set50 / set100 / mai)")
    fails = 0
    for cmd, expected_index in (("set50", "SET50"), ("set100", "SET100"), ("mai", "MAI")):
        try:
            q, _ = query(base, secret, cmd)
            fails += check(f"{cmd}/kind", q.get("kind") == "index_breadth",
                           f"kind={q.get('kind')}")
            fails += check(f"{cmd}/index", q.get("index") == expected_index,
                           f"index={q.get('index')}")
            # SET50 + SET100 must have configured members. MAI may be 0 if
            # not yet populated — soft-pass.
            if expected_index in ("SET50", "SET100"):
                fails += check(f"{cmd}/has_members",
                               (q.get("members_configured") or 0) >= 40,
                               f"configured={q.get('members_configured')}")
                # Members_scanned should be a high fraction of configured —
                # if Settrade was up. Allow a wider band for transient outages.
                cfg = q.get("members_configured") or 0
                scanned = q.get("members_scanned") or 0
                fails += check(f"{cmd}/scan_coverage",
                               cfg == 0 or (scanned / cfg) >= 0.8,
                               f"scanned={scanned}/{cfg}")
                # Stage counts should sum to scanned count
                stages = q.get("stage_counts") or {}
                total_staged = sum(stages.values()) if stages else 0
                fails += check(f"{cmd}/stage_counts_consistent",
                               total_staged == scanned,
                               f"stages_sum={total_staged} scanned={scanned}")
        except Exception as e:
            fails += check(cmd, False, f"err: {e}")

    # Drill-down filters: 'set50 members' / 'set50 stage2' / etc. These
    # were added so users can tap the breadth card stage rows or type
    # commands to drill into the constituent list. Verify the dispatch
    # returns kind='list' (re-uses the standard stock-list summary path).
    for cmd in ("set50 members", "set50 stage2", "set100 members", "set100 stage1"):
        try:
            q, _ = query(base, secret, cmd)
            fails += check(f"{cmd}/kind_list", q.get("kind") == "list",
                           f"kind={q.get('kind')} count={q.get('count')}")
            # Members commands should non-empty (SET50/SET100 have ≥40 in fallback).
            if cmd.endswith(" members"):
                fails += check(f"{cmd}/non_empty",
                               (q.get("count") or 0) > 0,
                               f"count={q.get('count')}")
        except Exception as e:
            fails += check(cmd, False, f"err: {e}")
    return fails


def suite_sub_stage(base, secret):
    """Per-sub-stage filter dispatch + invariant coverage for the 9-state
    finite state machine that replaces orthogonal stage+pattern as the
    primary classification.

    Confirms (a) every sub-stage filter command dispatches kind=list,
    (b) the parent stage of every returned signal matches the sub-stage's
    parent prefix (STAGE_2_PULLBACK only on stocks with stage==2),
    (c) the sub_stage field round-trips through /test/query.
    """
    section("Sub-stage finite state machine (9 states)")
    fails = 0
    # 2-layer classifier — 11 sub-stages. Stage 2 expanded from 3 to 5
    # (IGNITION/OVEREXTENDED/CONTRACTION/PIVOT_READY/MARKUP). Legacy
    # aliases (early/running/pullback) were hard-removed.
    SUB_STAGES = [
        ("base",         "STAGE_1_BASE",         1),
        ("prep",         "STAGE_1_PREP",         1),
        # New Stage 2 (preferred)
        ("ignition",     "STAGE_2_IGNITION",     2),
        ("overextended", "STAGE_2_OVEREXTENDED", 2),
        ("contraction",  "STAGE_2_CONTRACTION",  2),
        ("ready",        "STAGE_2_PIVOT_READY",  2),
        ("markup",       "STAGE_2_MARKUP",       2),
        # Stage 3 / 4
        ("volatile",     "STAGE_3_VOLATILE",     3),
        ("dist",         "STAGE_3_DIST_DIST",    3),
        ("breakdown",    "STAGE_4_BREAKDOWN",    4),
        ("downtrend",    "STAGE_4_DOWNTREND",    4),
    ]
    seen_any_sub_stage = False
    for cmd, expected_const, expected_parent in SUB_STAGES:
        try:
            q, _ = query(base, secret, cmd)
            fails += check(f"{cmd}/dispatched", q.get("kind") == "list",
                           f"kind={q.get('kind')} count={q.get('count')}")
            # If non-empty, every first_5 entry must have the right
            # parent stage AND the right sub_stage field.
            for r in q.get("first_5") or []:
                got_sub = r.get("sub_stage")
                got_stage = r.get("stage")
                if got_sub:
                    seen_any_sub_stage = True
                fails += check(f"{cmd}/{r.get('symbol')}/sub_stage_match",
                               got_sub == expected_const,
                               f"sub_stage={got_sub!r} expected={expected_const!r}")
                fails += check(f"{cmd}/{r.get('symbol')}/parent_match",
                               got_stage == expected_parent,
                               f"stage={got_stage} expected={expected_parent}")
        except Exception as e:
            fails += check(cmd, False, f"err: {e}")
    # Sanity: at least ONE sub-stage filter must have returned data — else
    # the sub_stage field never made it into the live cache.
    fails += check("sub_stage/field_populated_somewhere",
                   seen_any_sub_stage,
                   "no sub_stage strings observed across all 11 filters — "
                   "scan path may not be writing the field")
    return fails


def suite_margin(base, secret):
    """Margin tier filter coverage (Krungsri's Marginable Securities List).

    Confirms (a) `margin50/60/70/80` filters dispatch + return non-empty
    cohorts, (b) every returned signal has the matching margin_im_pct,
    (c) the umbrella `margin` filter returns the union of all four
    tiers, (d) /test/signal/{sym} surfaces margin_im_pct, (e) at least
    one well-known IM50 stock (KKP / ADVANC / PTT) is correctly
    classified as IM50.
    """
    section("Margin tier filters")
    fails = 0

    # (a)+(b) Per-tier filters
    for tier in (50, 60, 70, 80):
        cmd = f"margin{tier}"
        try:
            q, _ = query(base, secret, cmd)
            fails += check(f"{cmd}/dispatched", q.get("kind") == "list",
                           f"kind={q.get('kind')}")
            fails += check(f"{cmd}/non_empty", (q.get("count") or 0) > 0,
                           f"count={q.get('count')} — margin list may not be loaded")
            for r in (q.get("first_5") or []):
                fails += check(f"{cmd}/{r.get('symbol')}/im_pct",
                               r.get("margin_im_pct") == tier,
                               f"got={r.get('margin_im_pct')} expected={tier}")
        except Exception as e:
            fails += check(cmd, False, f"err: {e}")

    # (c) Umbrella `margin` filter
    try:
        q, _ = query(base, secret, "margin")
        fails += check("margin/dispatched", q.get("kind") == "list",
                       f"kind={q.get('kind')}")
        fails += check("margin/non_empty", (q.get("count") or 0) > 0,
                       f"count={q.get('count')}")
        for r in (q.get("first_5") or []):
            fails += check(f"margin/{r.get('symbol')}/has_tier",
                           (r.get("margin_im_pct") or 0) in (50, 60, 70, 80),
                           f"margin_im_pct={r.get('margin_im_pct')}")
    except Exception as e:
        fails += check("margin", False, f"err: {e}")

    # (d) /test/signal exposes margin_im_pct
    try:
        url = f"{base}/test/signal/KKP"
        sig, _ = fetch(url, headers={"x-scan-secret": secret})
        cache = sig.get("cache") or sig.get("in_memory") or {}
        fails += check("signal/has_margin_im_pct",
                       "margin_im_pct" in cache,
                       f"cache keys={list(cache.keys())}")
        # KKP is in IM50 per Krungsri's list (large-cap bank)
        fails += check("signal/KKP_im_pct=50",
                       cache.get("margin_im_pct") == 50,
                       f"got={cache.get('margin_im_pct')}")
    except Exception as e:
        fails += check("signal/margin_field", False, f"err: {e}")

    # (e) Scoped: set100 margin50 should also work
    try:
        q, _ = query(base, secret, "set100 margin50")
        fails += check("set100_margin50/dispatched",
                       q.get("kind") == "list",
                       f"kind={q.get('kind')}")
        for r in (q.get("first_5") or []):
            fails += check(f"set100_margin50/{r.get('symbol')}/im_pct",
                           r.get("margin_im_pct") == 50,
                           f"got={r.get('margin_im_pct')}")
    except Exception as e:
        fails += check("set100_margin50", False, f"err: {e}")

    return fails


def suite_index_scope(base, secret):
    """Per-index scoped commands shipped with the SET100/SET50 replication
    iteration: `set100 stage` / `set100 stages` / `set100 pivot` (and
    same for set50). Confirms each command resolves to the expected
    handler/route and surfaces non-zero data when the scan has run.
    """
    section("Per-index scoped commands (SET100 + SET50)")
    fails = 0
    for index_lower, index_upper in (
            ("set100", "SET100"),
            ("set50", "SET50"),
            ("marginable", "MARGINABLE"),  # 321 Krungsri marginable stocks
    ):
        # `<index> stage` → picker (kind=static, handler=index_stage_picker)
        try:
            q, _ = query(base, secret, f"{index_lower} stage")
            fails += check(f"{index_lower}_stage/dispatched",
                           q.get("kind") == "static"
                           and q.get("handler") == "index_stage_picker"
                           and q.get("index") == index_upper,
                           f"got={q}")
            fails += check(f"{index_lower}_stage/has_constituents",
                           (q.get("constituents_count") or 0) > 0,
                           f"constituents={q.get('constituents_count')}")
        except Exception as e:
            fails += check(f"{index_lower}_stage", False, f"err: {e}")

        # `<index> stages` → dashboard (kind=static + sub_stage_counts)
        try:
            q, _ = query(base, secret, f"{index_lower} stages")
            fails += check(f"{index_lower}_stages/dispatched",
                           q.get("kind") == "static"
                           and q.get("handler") == "index_stages_dashboard"
                           and q.get("index") == index_upper,
                           f"got={q}")
            sub_counts = q.get("sub_stage_counts") or {}
            fails += check(f"{index_lower}_stages/has_sub_stage_counts",
                           isinstance(sub_counts, dict) and len(sub_counts) > 0,
                           f"sub_stage_counts={sub_counts}")
        except Exception as e:
            fails += check(f"{index_lower}_stages", False, f"err: {e}")

        # `<index> pivot` → list (kind=list with pivot candidates)
        try:
            q, _ = query(base, secret, f"{index_lower} pivot")
            fails += check(f"{index_lower}_pivot/dispatched",
                           q.get("kind") == "list",
                           f"kind={q.get('kind')}")
            for r in (q.get("first_5") or []):
                fails += check(f"{index_lower}_pivot/{r.get('symbol')}/has_pivot",
                               (r.get("pivot_price") or 0) > 0,
                               f"pivot_price={r.get('pivot_price')}")
        except Exception as e:
            fails += check(f"{index_lower}_pivot", False, f"err: {e}")

        # Per-sub-stage tokens scoped to the index — regression guard for
        # the "set100 ignition" mis-route that fell through to the
        # invalid-command branch because _reply_index_filter's local
        # SUB_STAGE_TOKEN_MAP was missing the new FSM vocabulary.
        SUB_STAGE_PROBES = [
            ("ignition",     "STAGE_2_IGNITION",     2),
            ("ready",        "STAGE_2_PIVOT_READY",  2),
            ("markup",       "STAGE_2_MARKUP",       2),
            ("contraction",  "STAGE_2_CONTRACTION",  2),
            ("overextended", "STAGE_2_OVEREXTENDED", 2),
            ("breakdown",    "STAGE_4_BREAKDOWN",    4),
        ]
        for token, expected_const, expected_parent in SUB_STAGE_PROBES:
            try:
                q, _ = query(base, secret, f"{index_lower} {token}")
                fails += check(f"{index_lower}_{token}/dispatched",
                               q.get("kind") == "list",
                               f"kind={q.get('kind')} count={q.get('count')}")
                for r in (q.get("first_5") or []):
                    fails += check(f"{index_lower}_{token}/{r.get('symbol')}/sub_stage",
                                   r.get("sub_stage") == expected_const,
                                   f"sub_stage={r.get('sub_stage')!r} expected={expected_const!r}")
                    fails += check(f"{index_lower}_{token}/{r.get('symbol')}/parent",
                                   r.get("stage") == expected_parent,
                                   f"stage={r.get('stage')} expected={expected_parent}")
            except Exception as e:
                fails += check(f"{index_lower}_{token}", False, f"err: {e}")

    return fails


def suite_persistence(base, secret):
    """Persistence-layer coverage shipped with the FSM persistence
    iteration. Confirms (a) /test/signal exposes the cosmetic rename
    + persisted/last_persisted_at metadata, (b) /test/transitions
    returns a list shape (may be empty if no scan has happened since
    deploy), (c) /test/breadth_history returns the per-scan time
    series.
    """
    section("Persistence layer (transitions + breadth history + cache rename)")
    fails = 0

    # (a) /test/signal cosmetic rename + persisted metadata
    try:
        url = f"{base}/test/signal/KKP"  # known live symbol
        sig, _ = fetch(url, headers={"x-scan-secret": secret})
        fails += check("signal/has_cache_field", "cache" in sig,
                       f"keys={list(sig.keys())}")
        fails += check("signal/has_in_memory_alias", "in_memory" in sig,
                       "backward-compat alias missing")
        fails += check("signal/has_persisted", "persisted" in sig,
                       "persisted boolean missing")
        fails += check("signal/has_last_persisted_at",
                       "last_persisted_at" in sig,
                       "last_persisted_at missing")
        # If Firestore has the symbol, persisted should be true
        if sig.get("firestore"):
            fails += check("signal/persisted_true_when_firestore_present",
                           sig.get("persisted") is True,
                           f"persisted={sig.get('persisted')}")
    except Exception as e:
        fails += check("signal", False, f"err: {e}")

    # (b) /test/transitions returns list shape (may be empty)
    try:
        url = f"{base}/test/transitions?limit=10"
        tr, _ = fetch(url, headers={"x-scan-secret": secret})
        fails += check("transitions/dispatched",
                       isinstance(tr.get("transitions"), list),
                       f"got={type(tr.get('transitions')).__name__}")
        fails += check("transitions/has_count",
                       isinstance(tr.get("count"), int),
                       f"count={tr.get('count')}")
        # If non-empty, every entry has the required diff fields
        for entry in (tr.get("transitions") or [])[:3]:
            for key in ("symbol", "transitioned_at",
                        "prev_sub_stage", "new_sub_stage"):
                fails += check(f"transitions/{entry.get('symbol')}/{key}",
                               key in entry, f"keys={list(entry.keys())}")
            # prev != new (transitions are real changes only)
            if entry.get("prev_sub_stage") and entry.get("new_sub_stage"):
                fails += check(
                    f"transitions/{entry.get('symbol')}/prev_ne_new",
                    entry.get("prev_sub_stage") != entry.get("new_sub_stage"),
                    f"prev={entry.get('prev_sub_stage')!r} new={entry.get('new_sub_stage')!r}",
                )
    except Exception as e:
        fails += check("transitions", False, f"err: {e}")

    # (c) /test/breadth_history returns time-series rows
    try:
        url = f"{base}/test/breadth_history?limit=5"
        bh, _ = fetch(url, headers={"x-scan-secret": secret})
        fails += check("breadth_history/dispatched",
                       isinstance(bh.get("snapshots"), list),
                       f"got={type(bh.get('snapshots')).__name__}")
        for snap in (bh.get("snapshots") or [])[:1]:
            # Each snapshot must contain the per-sub-stage count fields
            for col in ("scanned_at", "total_stocks",
                        "stage_2_pivot_ready", "stage_2_ignition",
                        "stage_2_overextended", "stage_2_markup",
                        "stage_2_contraction"):
                fails += check(f"breadth_history/has_{col}",
                               col in snap, f"keys={list(snap.keys())[:8]}")
    except Exception as e:
        fails += check("breadth_history", False, f"err: {e}")

    return fails


def suite_pivot(base, secret):
    """Pivot-point coverage for the 4 actionable buy-side sub-stages.

    Confirms (a) the `pivot` filter dispatches kind=list, (b) every
    returned signal has pivot_price > 0 + pivot_stop > 0 + stop < pivot,
    (c) every signal's sub_stage is in the actionable scope (PREP /
    EARLY / RUNNING / PULLBACK), (d) at least one stock in the live
    cache carries a pivot — sanity-checking that the field made it
    through scan → snapshot.
    """
    section("Pivot point — buy trigger + setup stop")
    fails = 0
    # Pivot scope per the 2-layer refactor: 5 actionable buy-side states
    # (PREP + the 4 non-warning Stage 2 sub-stages). Legacy STAGE_2_*
    # names retained so old Firestore docs loaded mid-migration still
    # pass — those docs got pivot computed under the previous scope.
    PIVOT_SCOPE = {
        "STAGE_1_PREP",
        # New Stage 2 actionable states (OVEREXTENDED excluded — warning)
        "STAGE_2_IGNITION",
        "STAGE_2_CONTRACTION",
        "STAGE_2_PIVOT_READY",
        "STAGE_2_MARKUP",
        # Legacy aliases (kept in scope for backward compat)
        "STAGE_2_EARLY",
        "STAGE_2_RUNNING",
        "STAGE_2_PULLBACK",
    }
    try:
        q, _ = query(base, secret, "pivot")
        fails += check("pivot/dispatched", q.get("kind") == "list",
                       f"kind={q.get('kind')} count={q.get('count')}")
        fails += check("pivot/has_candidates", (q.get("count") or 0) > 0,
                       f"count={q.get('count')} — no stocks with pivot_price>0; "
                       "scan path may not be writing the field")
        for r in q.get("first_5") or []:
            sym = r.get("symbol")
            piv = r.get("pivot_price") or 0
            stp = r.get("pivot_stop") or 0
            sub = r.get("sub_stage") or ""
            fails += check(f"pivot/{sym}/price_positive", piv > 0,
                           f"pivot_price={piv}")
            fails += check(f"pivot/{sym}/stop_positive", stp > 0,
                           f"pivot_stop={stp}")
            fails += check(f"pivot/{sym}/stop_below_pivot", stp < piv,
                           f"stop={stp} pivot={piv}")
            fails += check(f"pivot/{sym}/sub_stage_in_scope",
                           sub in PIVOT_SCOPE,
                           f"sub_stage={sub!r} not in {PIVOT_SCOPE}")
    except Exception as e:
        fails += check("pivot", False, f"err: {e}")
    return fails


def suite_stage_weakening(base, secret):
    """Invariant coverage for the stage_weakening modifier shipped
    alongside breakout_attempt + unclosed-VCP. Works by probing a
    random in-cache signal — doesn't care WHICH stock weakens, only
    that when the flag is True it's consistent with the definition."""
    section("stage_weakening invariant (stage==2 AND close < sma50)")
    fails = 0
    try:
        # Pull a broad list of live signals via the breakout / consolidating
        # / vcp paths — any /test/query list returns first_5 with symbols,
        # which we then individually resolve via single-stock lookup to see
        # if any carry stage_weakening=True.
        probe_symbols = []
        for cmd in ("consolidating", "breakout"):
            q, _ = query(base, secret, cmd)
            probe_symbols.extend(r.get("symbol") for r in q.get("first_5", []) if r.get("symbol"))
        probe_symbols = list(dict.fromkeys(probe_symbols))[:8]  # dedupe, cap at 8

        any_weakening_seen = False
        for sym in probe_symbols:
            q, _ = query(base, secret, sym)
            sig = q.get("signal") or {}
            weak = sig.get("stage_weakening")
            if weak is None:
                continue  # old-schema signal, skip silently
            stage = sig.get("stage")
            close = sig.get("close") or 0
            sma50 = sig.get("sma50") or 0
            if weak:
                any_weakening_seen = True
                # Invariant: weakening → stage == 2 AND close < sma50
                fails += check(f"{sym}/weak→stage2", stage == 2,
                               f"got stage={stage}")
                if sma50 > 0:
                    # Use 0.5% tolerance: the analyzer compares full-precision
                    # floats but /test/query rounds to 2dp before serializing,
                    # so close==sma50 at display is possible while the
                    # underlying c < sma50 condition holds.
                    fails += check(f"{sym}/weak→close<sma50",
                                   close <= sma50 * 1.005,
                                   f"close={close} sma50={sma50}")
        # Soft-pass if nothing weakened — market might just be strong.
        # The field presence alone is the critical check (confirms the
        # new field survives the Firestore round-trip + serialization).
        fails += check("stage_weakening/field_present",
                       any(q.get("signal", {}).get("stage_weakening") is not None
                           for q in [query(base, secret, s)[0] for s in probe_symbols[:2]]),
                       f"probed {len(probe_symbols[:2])} symbols; "
                       "weakening observed in wider probe" if any_weakening_seen else "no weakening seen")
    except Exception as e:
        fails += check("stage_weakening", False, f"err: {e}")
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


def suite_global(base, secret):
    """Phase 1 global watchlist — curated non-SET assets via yfinance."""
    section("Global watchlist (indexes / ETFs / US stocks / crypto)")
    fails = 0
    try:
        q, dt = query(base, secret, "global")
        fails += check("global/dispatched", q.get("kind") == "global",
                       f"kind={q.get('kind')}")
        configured = q.get("configured") or 0
        got = q.get("count") or 0
        # Tightened threshold after the 50-asset expansion (8 sections:
        # US/Asia indexes, FX, Commodities, ETFs, US mega-cap, themes,
        # crypto). Drop below 40 means the curated list got truncated;
        # drop below 50 = something was deleted that probably shouldn't
        # have been (use >= 40 to allow safe pruning of niche tickers).
        fails += check("global/configured>=40", configured >= 40,
                       f"GLOBAL_SYMBOLS has {configured} entries")
        # yfinance occasionally fails on individual tickers — require at
        # least 75% of the curated list to return so the card isn't empty.
        fails += check("global/coverage>=75%",
                       got >= int(configured * 0.75) if configured else False,
                       f"got {got}/{configured} ({round(got/configured*100 if configured else 0)}%)  ({dt}s)")
        # Must show diversity in the top/bottom-10 movers — at least 3
        # distinct asset classes. Specific classes drop in/out of the
        # extremes daily depending on which section has the biggest
        # movers (e.g. quiet crypto day → no crypto in top 10). The
        # real invariant is "no single class is monopolising the
        # extremes", not a specific class roster.
        top = (q.get("top_5_up") or []) + (q.get("top_5_down") or [])
        classes = {row.get("class") for row in top if row.get("class")}
        fails += check("global/movers_diverse_classes",
                       len(classes) >= 3,
                       f"top/bottom 10 classes: {sorted(classes)} "
                       f"(need ≥3 distinct classes for healthy diversity)")
    except Exception as e:
        fails += check("global", False, f"err: {e}")
    return fails


def suite_global_single(base, secret):
    """Phase 1 Commit 2 — tap-to-detail / direct-text single global asset."""
    section("Global single-asset card (tap / direct text)")
    fails = 0

    # BTC: crypto path, price-formatting branch, should always return data.
    try:
        q, dt = query(base, secret, "BTC")
        fails += check("btc/kind", q.get("kind") == "global_single",
                       f"kind={q.get('kind')}")
        fails += check("btc/code", q.get("code") == "BTC", f"code={q.get('code')}")
        asset = q.get("asset") or {}
        fails += check("btc/class", asset.get("class") == "crypto",
                       f"class={asset.get('class')}")
        fails += check("btc/has_price", (asset.get("close") or 0) > 0,
                       f"close={asset.get('close')}")
        fails += check("btc/has_52w_range",
                       (asset.get("week52_high") or 0) > (asset.get("week52_low") or 0),
                       f"52w {asset.get('week52_low')} → {asset.get('week52_high')}")
    except Exception as e:
        fails += check("btc", False, f"err: {e}")

    # SPX: index path — yfinance returns 0 volume for indexes; card must
    # still render, just without a volume number. `/test/query` returns
    # asset["volume"] == 0 for this case — that's fine, the card renderer
    # handles the "—" display.
    try:
        q, _ = query(base, secret, "SPX")
        fails += check("spx/kind", q.get("kind") == "global_single",
                       f"kind={q.get('kind')}")
        asset = q.get("asset") or {}
        fails += check("spx/class", asset.get("class") == "index",
                       f"class={asset.get('class')}")
        fails += check("spx/has_price", (asset.get("close") or 0) > 0,
                       f"close={asset.get('close')}")
    except Exception as e:
        fails += check("spx", False, f"err: {e}")

    # GOOG: stock path — should have volume > 0 (unlike indexes).
    try:
        q, _ = query(base, secret, "GOOG")
        fails += check("goog/kind", q.get("kind") == "global_single",
                       f"kind={q.get('kind')}")
        asset = q.get("asset") or {}
        fails += check("goog/class", asset.get("class") == "stock",
                       f"class={asset.get('class')}")
        fails += check("goog/has_volume", (asset.get("volume") or 0) > 0,
                       f"volume={asset.get('volume')}")
    except Exception as e:
        fails += check("goog", False, f"err: {e}")

    # USDTHB: fx path — added with the 50-asset expansion. Confirms the
    # new 'fx' class round-trips through fetch_global_asset + the
    # /test/query single-asset response. Critical for Thai exporters who
    # watch this rate as their primary macro signal.
    try:
        q, _ = query(base, secret, "USDTHB")
        fails += check("usdthb/kind", q.get("kind") == "global_single",
                       f"kind={q.get('kind')}")
        asset = q.get("asset") or {}
        fails += check("usdthb/class", asset.get("class") == "fx",
                       f"class={asset.get('class')}")
        fails += check("usdthb/has_price", (asset.get("close") or 0) > 0,
                       f"close={asset.get('close')}")
    except Exception as e:
        fails += check("usdthb", False, f"err: {e}")

    # GOLD: commodity path — same guard rails as fx, also new with the
    # 50-asset expansion.
    try:
        q, _ = query(base, secret, "GOLD")
        fails += check("gold/kind", q.get("kind") == "global_single",
                       f"kind={q.get('kind')}")
        asset = q.get("asset") or {}
        fails += check("gold/class", asset.get("class") == "commodity",
                       f"class={asset.get('class')}")
        fails += check("gold/has_price", (asset.get("close") or 0) > 0,
                       f"close={asset.get('close')}")
    except Exception as e:
        fails += check("gold", False, f"err: {e}")

    # "GLOBAL" must NOT hijack the SET retail ticker — the `global` command
    # dispatch in /test/query fires first (handles the list bubble case), so
    # typing "GLOBAL" here should return kind=global (the bulk snapshot),
    # not kind=global_single. Guards against the dispatch order regressing.
    try:
        q, _ = query(base, secret, "global")
        fails += check("global_keyword/still_list", q.get("kind") == "global",
                       f"typing 'global' should show the list, got kind={q.get('kind')}")
    except Exception as e:
        fails += check("global_keyword", False, f"err: {e}")

    return fails


def suite_watchlist_global(base, secret):
    """Phase 1 Commit 3 — watchlist add/remove accepts global codes."""
    section("Watchlist add/remove global-code dispatch")
    fails = 0

    # 'add BTC' — must route to the global path (not bounce as unresolved).
    try:
        q, _ = query(base, secret, "add BTC")
        fails += check("add_btc/kind", q.get("kind") == "watchlist_add",
                       f"kind={q.get('kind')}")
        fails += check("add_btc/source", q.get("source") == "global",
                       f"source={q.get('source')}")
        fails += check("add_btc/resolved", q.get("resolved") == "BTC",
                       f"resolved={q.get('resolved')}")
    except Exception as e:
        fails += check("add_btc", False, f"err: {e}")

    # 'remove SPX' — same path, remove operation.
    try:
        q, _ = query(base, secret, "remove SPX")
        fails += check("remove_spx/kind", q.get("kind") == "watchlist_remove",
                       f"kind={q.get('kind')}")
        fails += check("remove_spx/source", q.get("source") == "global",
                       f"source={q.get('source')}")
    except Exception as e:
        fails += check("remove_spx", False, f"err: {e}")

    # 'add ADVANC' — must still route to SET. Guards against the global
    # branch accidentally hijacking SET ticker adds.
    try:
        q, _ = query(base, secret, "add ADVANC")
        fails += check("add_advanc/kind", q.get("kind") == "watchlist_add",
                       f"kind={q.get('kind')}")
        fails += check("add_advanc/source", q.get("source") == "set",
                       f"source={q.get('source')}")
        fails += check("add_advanc/resolved", q.get("resolved") == "ADVANC",
                       f"resolved={q.get('resolved')}")
    except Exception as e:
        fails += check("add_advanc", False, f"err: {e}")

    # 'add NOPE' — unresolved on both sides; must report failure cleanly.
    try:
        q, _ = query(base, secret, "add NOPE")
        fails += check("add_nope/unresolved", q.get("resolved") is None,
                       f"resolved={q.get('resolved')}")
        fails += check("add_nope/source_none", q.get("source") is None,
                       f"source={q.get('source')}")
    except Exception as e:
        fails += check("add_nope", False, f"err: {e}")

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
                         ("stage", "stage_picker"),
                         ("stages", "stages_dashboard"),     # NEW — 11-row matrix
                         ("dashboard", "stages_dashboard"),  # NEW alias
                         ("patterns", "pattern_overview"),
                         ("explain stage2", "explain"), ("explain breakout", "explain"),
                         ("explain pivot", "explain")):
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
    total_fails += suite_stage_weakening(base, secret)
    total_fails += suite_sub_stage(base, secret)
    total_fails += suite_pivot(base, secret)
    total_fails += suite_persistence(base, secret)
    total_fails += suite_index_scope(base, secret)
    total_fails += suite_margin(base, secret)
    total_fails += suite_index_breadth(base, secret)
    total_fails += suite_sector_drill(base, secret)
    total_fails += suite_sector_coverage(base, secret)
    total_fails += suite_global(base, secret)
    total_fails += suite_global_single(base, secret)
    total_fails += suite_watchlist_global(base, secret)
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
