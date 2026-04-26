#!/usr/bin/env python3
"""One-time BigQuery schema migration for the FSM persistence iteration.

Adds the new dataclass fields (sub_stage, sma10, sma20, sma200_roc20,
pivot_price, pivot_stop, stage_weakening) to `scan_results` and creates
the `breadth_snapshots` table for per-scan dashboard history.

Idempotent: safe to run repeatedly. Uses `ADD COLUMN IF NOT EXISTS` so
re-runs after partial migrations succeed cleanly.

Usage:
    python3 scripts/bq_migrate_signals.py [--project signalix-prod]

Auth: relies on GOOGLE_APPLICATION_CREDENTIALS or `gcloud auth
application-default login` (Claude's deploy SA works).
"""
from __future__ import annotations

import argparse
import logging
import os
import sys

logging.basicConfig(level=logging.INFO,
                    format="[%(asctime)s] %(levelname)s %(message)s",
                    datefmt="%H:%M:%S")
log = logging.getLogger("bq_migrate")


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--project", default=os.environ.get("GCP_PROJECT", "signalix-prod"))
    ap.add_argument("--dataset", default="signalix")
    args = ap.parse_args()

    try:
        from google.cloud import bigquery
    except ImportError:
        log.error("google-cloud-bigquery not installed; pip install google-cloud-bigquery")
        return 1

    client = bigquery.Client(project=args.project)
    log.info("Connected to BigQuery project=%s dataset=%s", args.project, args.dataset)

    # ── 1. Add new columns to scan_results ──────────────────────────
    # New fields added during the FSM refactor and pivot iterations.
    # All NULLABLE so existing rows remain valid (they'll have NULL
    # for these columns until a new scan writes them).
    new_cols = [
        ("sub_stage",        "STRING"),
        ("sma10",            "FLOAT64"),
        ("sma20",            "FLOAT64"),
        ("sma200_roc20",     "FLOAT64"),
        ("pivot_price",      "FLOAT64"),
        ("pivot_stop",       "FLOAT64"),
        ("stage_weakening",  "BOOL"),
    ]
    table_id = f"{args.project}.{args.dataset}.scan_results"
    log.info("Migrating table %s — adding %d columns", table_id, len(new_cols))
    for col_name, col_type in new_cols:
        ddl = f"ALTER TABLE `{table_id}` ADD COLUMN IF NOT EXISTS {col_name} {col_type}"
        try:
            client.query(ddl).result(timeout=60)
            log.info("  ✓ %s %s", col_name, col_type)
        except Exception as exc:
            log.error("  ✗ %s %s — %s", col_name, col_type, exc)
            return 2

    # ── 2. Create breadth_snapshots table for per-scan time series ─
    # One row per scan. Stores parent-stage counts AND per-sub-stage
    # counts so dashboard history queries can show the 11-row matrix
    # at any point in time.
    breadth_id = f"{args.project}.{args.dataset}.breadth_snapshots"
    breadth_schema = [
        bigquery.SchemaField("scanned_at",      "TIMESTAMP", mode="REQUIRED"),
        bigquery.SchemaField("scan_type",       "STRING",    mode="NULLABLE"),
        bigquery.SchemaField("mode",            "STRING",    mode="NULLABLE"),
        bigquery.SchemaField("total_stocks",    "INT64",     mode="NULLABLE"),
        # Parent stage counts
        bigquery.SchemaField("stage1_count",    "INT64",     mode="NULLABLE"),
        bigquery.SchemaField("stage2_count",    "INT64",     mode="NULLABLE"),
        bigquery.SchemaField("stage3_count",    "INT64",     mode="NULLABLE"),
        bigquery.SchemaField("stage4_count",    "INT64",     mode="NULLABLE"),
        # Sub-stage counts (11 sub-stages, plus legacy aliases for
        # backward compat on docs that still carry old strings)
        bigquery.SchemaField("stage_1_base",         "INT64", mode="NULLABLE"),
        bigquery.SchemaField("stage_1_prep",         "INT64", mode="NULLABLE"),
        bigquery.SchemaField("stage_2_ignition",     "INT64", mode="NULLABLE"),
        bigquery.SchemaField("stage_2_overextended", "INT64", mode="NULLABLE"),
        bigquery.SchemaField("stage_2_contraction",  "INT64", mode="NULLABLE"),
        bigquery.SchemaField("stage_2_pivot_ready",  "INT64", mode="NULLABLE"),
        bigquery.SchemaField("stage_2_markup",       "INT64", mode="NULLABLE"),
        bigquery.SchemaField("stage_3_volatile",     "INT64", mode="NULLABLE"),
        bigquery.SchemaField("stage_3_dist_dist",    "INT64", mode="NULLABLE"),
        bigquery.SchemaField("stage_4_breakdown",    "INT64", mode="NULLABLE"),
        bigquery.SchemaField("stage_4_downtrend",    "INT64", mode="NULLABLE"),
        # Market-wide breadth
        bigquery.SchemaField("advancing",       "INT64",     mode="NULLABLE"),
        bigquery.SchemaField("declining",       "INT64",     mode="NULLABLE"),
        bigquery.SchemaField("unchanged",       "INT64",     mode="NULLABLE"),
        bigquery.SchemaField("new_highs_52w",   "INT64",     mode="NULLABLE"),
        bigquery.SchemaField("new_lows_52w",    "INT64",     mode="NULLABLE"),
        bigquery.SchemaField("breakout_count",  "INT64",     mode="NULLABLE"),
        bigquery.SchemaField("vcp_count",       "INT64",     mode="NULLABLE"),
        bigquery.SchemaField("above_ma200",     "INT64",     mode="NULLABLE"),
        bigquery.SchemaField("below_ma200",     "INT64",     mode="NULLABLE"),
        # SET index reference
        bigquery.SchemaField("set_index_close",      "FLOAT64", mode="NULLABLE"),
        bigquery.SchemaField("set_index_change_pct", "FLOAT64", mode="NULLABLE"),
    ]
    try:
        client.get_table(breadth_id)
        log.info("Table %s already exists (skipping create)", breadth_id)
    except Exception:
        log.info("Creating table %s with %d columns", breadth_id, len(breadth_schema))
        table = bigquery.Table(breadth_id, schema=breadth_schema)
        # Partition by day for cheap time-range queries
        table.time_partitioning = bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.DAY,
            field="scanned_at",
        )
        client.create_table(table)
        log.info("  ✓ created %s (partitioned by scanned_at day)", breadth_id)

    log.info("Migration complete.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
