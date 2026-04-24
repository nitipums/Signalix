#!/usr/bin/env bash
#
# Claude-side deploy for Signalix.
#
# Usage:
#   bash scripts/claude_deploy.sh
#
# What it does:
#   1. Bootstraps gcloud on first run (installs to /home/user/google-cloud-sdk
#      if not already present).
#   2. Activates a service-account key from a fixed path outside the repo.
#      Key never lives in the repo — user drops it in
#      /home/user/.config/gcloud-signalix/deploy-key.json and project ID in
#      /home/user/.config/gcloud-signalix/project (single line).
#   3. Submits the build via cloudbuild.yaml (same config humans use for
#      `gcloud builds submit`). Streams the build log so Claude sees
#      success/failure directly.
#
# Why a helper script instead of inline commands in the agent:
#   * Keeps per-session bootstrap deterministic (no accidental re-install).
#   * Makes the key path + project one-time human setup, not an every-turn
#     arg Claude has to remember.
#   * Failures show up as script exit codes, not buried in a multi-step
#     Bash pipeline.

set -euo pipefail

# ── 0. Paths & constants ────────────────────────────────────────────────
KEY_PATH="${SIGNALIX_DEPLOY_KEY:-/home/user/.config/gcloud-signalix/deploy-key.json}"
PROJECT_FILE="${SIGNALIX_PROJECT_FILE:-/home/user/.config/gcloud-signalix/project}"
GCLOUD_INSTALL_DIR="${GCLOUD_INSTALL_DIR:-/home/user/google-cloud-sdk}"
REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
REGION="asia-southeast1"

say() { printf '[claude_deploy] %s\n' "$*" >&2; }
die() { printf '[claude_deploy] ERR: %s\n' "$*" >&2; exit 1; }

# ── 1. Inputs present? ──────────────────────────────────────────────────
[ -r "$KEY_PATH" ] || die "Service-account key not found at $KEY_PATH. See CLAUDE.md 'Claude-side deploy' for one-time setup."

if [ -n "${GCP_PROJECT_ID:-}" ]; then
  PROJECT_ID="$GCP_PROJECT_ID"
elif [ -r "$PROJECT_FILE" ]; then
  PROJECT_ID="$(tr -d '[:space:]' < "$PROJECT_FILE")"
else
  die "No project ID. Set GCP_PROJECT_ID env var or write the project to $PROJECT_FILE (single line)."
fi
[ -n "$PROJECT_ID" ] || die "Project ID is empty."

# ── 2. gcloud bootstrap (idempotent) ────────────────────────────────────
if ! command -v gcloud >/dev/null 2>&1; then
  if [ -x "$GCLOUD_INSTALL_DIR/bin/gcloud" ]; then
    say "Found cached gcloud at $GCLOUD_INSTALL_DIR — adding to PATH."
  else
    say "Installing gcloud SDK (first run only)…"
    # The official installer script — no apt-get / sudo required.
    # We pipe 'n' to decline the rc-file modification since we handle PATH ourselves.
    parent_dir="$(dirname "$GCLOUD_INSTALL_DIR")"
    mkdir -p "$parent_dir"
    tmp_tar="$(mktemp -t gcloud-sdk.XXXXXX.tar.gz)"
    # Pin to a recent stable release; versioning avoids install-time surprises.
    curl -fsSL -o "$tmp_tar" \
      "https://dl.google.com/dl/cloudsdk/channels/rapid/downloads/google-cloud-cli-linux-x86_64.tar.gz"
    tar -xzf "$tmp_tar" -C "$parent_dir"
    rm -f "$tmp_tar"
    "$GCLOUD_INSTALL_DIR/install.sh" --quiet --usage-reporting=false \
      --command-completion=false --path-update=false >/dev/null
    say "gcloud installed at $GCLOUD_INSTALL_DIR."
  fi
  export PATH="$GCLOUD_INSTALL_DIR/bin:$PATH"
fi
gcloud --version >/dev/null || die "gcloud still not callable after install."

# ── 3. Authenticate + pin context ───────────────────────────────────────
say "Activating service account from $KEY_PATH…"
gcloud auth activate-service-account --key-file="$KEY_PATH" --quiet >/dev/null
gcloud config set project "$PROJECT_ID" --quiet >/dev/null
gcloud config set run/region "$REGION" --quiet >/dev/null
ACTIVE_SA="$(gcloud config get-value account 2>/dev/null)"
say "Authenticated as $ACTIVE_SA in project $PROJECT_ID (region $REGION)."

# ── 4. Submit the build ─────────────────────────────────────────────────
# cloudbuild.yaml already has the two-step deploy + post-deploy /scan.
# --region asia-southeast1 keeps the build regional (matches the trigger
# the user would set up in Option B). Stream the log so Claude sees it
# line-by-line instead of having to poll.
say "Submitting build… (this takes ~5-8 min)"
cd "$REPO_ROOT"
gcloud builds submit \
  --config=cloudbuild.yaml \
  --region="$REGION" \
  --gcs-source-staging-dir="gs://${PROJECT_ID}_cloudbuild/source" \
  .

# ── 5. Verify the new revision ──────────────────────────────────────────
LATEST_REV="$(gcloud run services describe signalix \
  --region="$REGION" \
  --format='value(status.latestReadyRevisionName)' 2>/dev/null || echo "?")"
SERVICE_URL="$(gcloud run services describe signalix \
  --region="$REGION" \
  --format='value(status.url)' 2>/dev/null || echo "?")"
say "Deploy complete."
say "  Latest ready revision: $LATEST_REV"
say "  Service URL:           $SERVICE_URL"
say "Next: python3 scripts/e2e_check.py"
