# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project overview

Signalix is a LINE Bot for Thai SET stock market scanning using Minervini stage analysis. It runs as a FastAPI service on Cloud Run, triggered on a schedule by Cloud Scheduler, and broadcasts Flex Message cards to LINE subscribers.

## Git workflow

- Always commit and push to the current working branch after finishing any task or set of changes.
- Development branch: `claude/add-claude-documentation-afUya`
- First push of a branch: `git push -u origin <branch>`.

## Commands

- Install deps: `pip install -r requirements.txt`
- Run locally: `uvicorn main:app --reload --port 8080` (requires `.env` — copy from `.env.example`)
- Docker build/run: `docker build -t signalix . && docker run -p 8080:8080 --env-file .env signalix`
- Trigger a scan manually: `curl -X POST http://localhost:8080/scan -H "X-Scan-Secret: $SCAN_SECRET" -H "Content-Type: application/json" -d '{"scan_type":"full","broadcast":false,"mode":"full"}'`
- Data completeness / anomaly / export CLI: `python tools/check_data.py --check | --anomalies | --export csv|json | --export-history` (needs GCP creds)
- Rebuild LINE Rich Menu: `python tools/setup_rich_menu.py --token $LINE_CHANNEL_ACCESS_TOKEN`
- One-shot GCP bootstrap (from Cloud Shell): `./setup.sh` (reads `env.yaml`, enables APIs, writes secrets, deploys Cloud Run, creates scheduler jobs)
- Deploy: `gcloud builds submit --config cloudbuild.yaml` or push to `main` if the GitHub trigger is wired up.

No test suite exists in the repo — verify changes by running the app locally and hitting `/scan` / `/health` / `/admin/check`.

## Architecture

### Request flow

- **`POST /scan`** (Cloud Scheduler → protected by `X-Scan-Secret` header) is the core pipeline. It fetches OHLCV, runs `scan_stock` per symbol, computes breadth + sector trends, persists results, and optionally broadcasts a LINE card. All heavy work runs via `loop.run_in_executor` so the event loop never blocks.
- **`POST /webhook/line`** validates the HMAC signature, then dispatches text commands in `_handle_text_query` (Thai + English keywords: `ตลาด`, `stage`, `breakout`, `vcp`, `watchlist`, `add {sym}`, `remove {sym}`, `detail {sym}`, `explain {metric}`, `help`, `guide`, …). Single-word input is resolved to a ticker via `resolve_symbol` (handles aliases like `SCG→SCC`, `AIS→ADVANC`).
- **`POST /sync_ath`** is a chunked one-shot helper to populate Firestore `ath_cache` (+ BigQuery history) from yfinance `period="max"`. Call with `?chunk=N&chunk_size=20` until `next_chunk` is null.
- **`GET /admin/check`** returns scan summary + data-completeness + anomaly report (also gated by `X-Scan-Secret`).
- **`/health`** and **`/test/settrade`** are open diagnostic endpoints.
- On startup, `_background_scan` runs a full scan ~5s after boot so the in-memory cache (`_last_signals`, `_last_breadth`, `_last_indexes`, `_last_sector_trends`, `_ath_cache`) is warm; Firestore is pre-loaded first so webhook replies work before the scan finishes.

### Module layout

- `main.py` — FastAPI app, routing, in-memory cache, LINE command dispatch. Owns the `_last_*` globals and `_ath_cache`.
- `analyzer.py` — Pure-pandas stage/pattern logic: `classify_stage`, `detect_pattern`, `_detect_vcp`, `_strength_score`, `scan_stock`, `run_full_scan`, `compute_market_breadth`, `compute_sector_trends`, `analyze_index` (MACD/RSI for indexes).
- `data.py` — All I/O: yfinance + Settrade fetch, BigQuery OHLCV store, Firestore (`ath_cache`, `signals`, `market_breadth`, `users`, `fundamentals_cache`), symbol list (`SET_STOCKS`), `SECTOR_MAP`, `INDEX_SYMBOLS`, `INDEX_TV_URLS`, `SYMBOL_ALIASES`.
- `notifier.py` — LINE v3 SDK wrapper + ~25 Flex Message builders (`build_market_breadth_card`, `build_compact_stock_carousel`, `build_single_stock_card`, `build_watchlist_stock_card`, `build_guide_carousel`, `build_pattern_detail_card`, `build_stage_cycle_card`, `build_welcome_card`, `build_help_card`, etc.). `init_notifier` must be called once at startup.
- `settrade_client.py` — `settrade-v2` SDK wrapper (`get_ohlcv`, `get_quote`). Investor instance is `lru_cache`d; returns `None` gracefully if creds missing.
- `config.py` — `Settings` via `pydantic-settings` loaded from env / `.env`; `get_settings()` is `lru_cache`d.
- `tools/check_data.py`, `tools/setup_rich_menu.py` — standalone CLI utilities (not imported by the service).
- `cloud_scheduler.yaml` — Reference cron definitions (actual jobs are created by `setup.sh`).

### Scan modes (critical)

`/scan` takes a `mode` field:

- `mode="intraday"` — `fetch_latest_candles()`: load BQ history for all symbols, merge with a small `yfinance period="5d"` batch to graft today's candle. **No BigQuery write.** Used for 10:15 / 12:15 / 15:15 ICT scans.
- `mode="full"` — `run_full_scan()` → `fetch_all_stocks()`: full 1y fetch (BQ cache if fresh ≤5 days and ≥95% symbols, else yfinance). Appends new candles to BigQuery via `append_new_candles_to_bq`. Used only for the 16:45 ICT post-close scan.

`scan_type` (`full`/`breadth`/`breakout`/`vcp`) controls **what is broadcast**, not how data is fetched.

### Data-source fallback chain

`fetch_ohlcv`: SET Trade API → yfinance (`.BK` suffix, `^SET.BK` for index).
`fetch_all_stocks`: BigQuery cache (if fresh + complete) → yfinance batch download.
ATH: `load_ath_from_bq()` (preferred) → `load_ath_cache(_db)` (Firestore). The in-memory `_ath_cache` is passed into `scan_stock` as `ath_override` so breakout detection uses true ATHs rather than window max.

### BigQuery schema

Single table `{project}.{dataset}.ohlcv` with columns `(symbol STRING, date DATE, open/high/low/close FLOAT64, volume INT64)`. Clustered by `(symbol, date)` and **intentionally not partitioned** — avoids the 5k partition-modification/day quota when backfilling many symbols. Writes are append-only and dedup by querying `MAX(date)` per symbol before loading.

### Firestore collections

`ath_cache/{symbol}`, `signals/{symbol}` (serialised `StockSignal.__dict__`), `market_breadth/{timestamp}`, `users/{line_user_id}` (with `subscribed`, `watchlist[]`, `display_name`), `fundamentals_cache/{symbol}` (24h TTL).

## Key conventions

- **Every new `StockSignal` field MUST have a default value.** `load_signals_from_firestore` filters by `dataclasses.fields(StockSignal)` and constructs from old docs — missing fields would break warm-start reads.
- Symbols are stored bare (`"PTT"`). yfinance tickers are built on demand via `_to_yf_ticker` (`"PTT.BK"`, `"^SET.BK"`). Never mix the two in caches or Firestore keys.
- Scan-secret endpoints use `secrets.compare_digest` — never `==`.
- Don't call blocking I/O (yfinance, BigQuery, Firestore) directly inside request handlers; wrap in `loop.run_in_executor(...)`.
- `init_bq` and `init_notifier` are idempotent singletons — do not re-init per request.
- Bangkok time everywhere user-facing: `BANGKOK_TZ = pytz.timezone("Asia/Bangkok")`. Cloud Scheduler cron expressions in `cloud_scheduler.yaml` are written in **UTC** with ICT equivalents in the comments.
- Thai text in card builders and command keywords is intentional — preserve both Thai and English aliases when extending `_handle_text_query`.
- LINE Flex carousels cap at 10 bubbles per message; `build_ranked_stock_list_bubble` exists for longer lists.
- Indexes available for analysis: `SET`, `SET50`, `SET100`, `MAI`, `sSET` (see `INDEX_SYMBOLS`). Sectors: `AGRO`, `CONSUMP`, `FINCIAL`, `INDUS`, `PROPCON`, `RESOURC`, `SERVICE`, `TECH`.
- Secrets are mounted from GCP Secret Manager (see `cloudbuild.yaml`); local dev reads `.env`.
