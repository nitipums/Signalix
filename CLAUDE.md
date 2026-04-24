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

## Testing policy
- **Every new feature or behaviour change must extend `scripts/e2e_check.py`** with assertions that cover the positive path, the empty/error path, and any new invariant the feature introduces.
- If the feature touches a text command or card that the LINE user sees, also extend `/test/query` in `main.py` so the e2e script can probe it without hitting the real LINE webhook.
- After deploying any change that affects scan, cards, or data flow: wait for Cloud Run rollout, run `python3 scripts/e2e_check.py`, and only hand the change back to the user once all assertions pass. Report the pass/fail table and any deliberate skips.
- Prefer adding a test before shipping the feature (so a red test marks the gap); at minimum, ship feature + test together in the same commit or the immediate follow-up.

## Deploy pipeline
- `cloudbuild.yaml` runs two steps: (1) `gcloud run deploy` with `--cpu-boost`, (2) post-deploy `POST /scan` (broadcast=false) to pre-warm the new revision's in-memory cache.
- Automated via a Cloud Build GitHub trigger — `scripts/setup_cloud_build_trigger.sh` registers it one time. Every push to `main` thereafter auto-deploys.
- First-time setup needs the Cloud Build GitHub App installed on `nitipums/Signalix` (https://github.com/apps/google-cloud-build). Run `bash scripts/setup_cloud_build_trigger.sh` once after that.
- `main.py` startup event must stay synchronous (`await _warm_from_firestore()`) so Cloud Run only marks an instance ready after warmup — don't convert back to `asyncio.create_task`.
- Pre-launch cost posture: **no `--min-instances`** — Cloud Run scales to zero when idle; cold start re-hydrates from Firestore in ~5s. When the bot launches publicly, add `--min-instances=1` to `cloudbuild.yaml` (see commented hint in that file) to keep a replica always warm.
- Pre-launch push posture: **2 scheduled scans/day (10:15 + 16:45 ICT)**, and each scan broadcasts **only the market breadth card** — `_broadcast_full_report` in `main.py` is intentionally stripped to a single `_broadcast_breadth` call. The breakout list, trend-change alert, and per-user watchlist multicast are all disabled. Users can still pull these views on demand via the `breakout`, `vcp`, and `watchlist` text commands (reply_flex, free). On launch, restore the fuller `_broadcast_full_report` body (git log for `cost: limit broadcast to breadth-only`) and add back the 12:15 / 15:15 Cloud Scheduler jobs in `cloud_scheduler.yaml`.

## Claude-side deploy
- `bash scripts/claude_deploy.sh` — Claude's end-to-end deploy path. Bootstraps gcloud on first run, activates the service-account key at `/home/user/.config/gcloud-signalix/deploy-key.json`, reads project ID from `/home/user/.config/gcloud-signalix/project`, and runs `gcloud builds submit --config=cloudbuild.yaml`. Streams the build log — takes ~5-8 min; verify with `python3 scripts/e2e_check.py` after.
- The key path and project file are **outside the repo** and `.gitignore` blocks common key filename patterns (`*-credentials.json`, `gcloud-key*.json`, `deploy-key.json`). Never copy the key into the repo tree.
- Service account `signalix-claude-deploy@<project>.iam.gserviceaccount.com` has `cloudbuild.builds.editor`, `run.admin`, `iam.serviceAccountUser`, `secretmanager.secretAccessor`, `logging.viewer`, `storage.objectViewer` on the Signalix project only. Rotate the key via `gcloud iam service-accounts keys create/delete` if a session feels off.
- If gcloud is already installed and authenticated as a user (not SA), the script still runs — it just re-activates the SA for the session.
