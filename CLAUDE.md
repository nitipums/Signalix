# Signalix — Claude Code Instructions

## Git workflow
- Always commit and push to the current working branch after finishing any task or set of changes.
- Development branch: `claude/optimize-performance-ui-CqM7o`
- Use `git push -u origin <branch>` on first push of a branch.
- **ALWAYS merge to `main` and push `main` after every task completes.** This is required every time — no exceptions.
  ```
  git checkout main && git merge claude/optimize-performance-ui-CqM7o && git push origin main
  git checkout claude/optimize-performance-ui-CqM7o
  ```

## Project overview
LINE Bot for Thai SET stock market scanning using Minervini stage analysis.
- `main.py` — FastAPI app, LINE webhook, scan endpoint
- `analyzer.py` — Stage/pattern detection, StockSignal, scoring, analyze_index
- `data.py` — BigQuery/Firestore I/O, Settrade + yfinance data fetching
- `notifier.py` — LINE Flex Message card builders
- `cloud_scheduler.yaml` — GCP Cloud Scheduler timing

## Key conventions
- Scan `mode="intraday"` uses BQ history + yfinance 5d merge (no BQ write)
- Scan `mode="full"` does full fetch + BigQuery append (16:45 ICT only)
- All new `StockSignal` fields must have default values for Firestore backward compat
