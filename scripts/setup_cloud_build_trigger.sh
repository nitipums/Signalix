#!/usr/bin/env bash
#
# One-shot setup: register a Cloud Build trigger that auto-deploys Signalix
# on every push to main. Run this ONCE; every subsequent git push origin main
# will execute cloudbuild.yaml (build → deploy to Cloud Run with
# min-instances=1 → post-deploy /scan warmup).
#
# ──────────────────────────────────────────────────────────────────────────
# PREREQUISITE (one-time, manual on GitHub.com):
#
#   1. Install the Google Cloud Build GitHub App on the nitipums account:
#        https://github.com/apps/google-cloud-build
#   2. Grant it access to the Signalix repository (or to all repos).
#
# Without that, the gcloud command below will fail with an
# "unauthenticated" error because Cloud Build can't see your repo.
# ──────────────────────────────────────────────────────────────────────────
#
# Then run:
#   bash scripts/setup_cloud_build_trigger.sh
#
# To verify after:
#   gcloud builds triggers list --region=asia-southeast1
#
# To delete later:
#   gcloud builds triggers delete signalix-main-deploy --region=asia-southeast1

set -euo pipefail

PROJECT_ID="${GCP_PROJECT_ID:-$(gcloud config get-value project 2>/dev/null)}"
if [ -z "$PROJECT_ID" ]; then
  echo "ERR: set GCP_PROJECT_ID env var or run 'gcloud config set project <id>' first" >&2
  exit 1
fi

REPO_OWNER="nitipums"
REPO_NAME="Signalix"
TRIGGER_NAME="signalix-main-deploy"
REGION="asia-southeast1"

echo "Project:  $PROJECT_ID"
echo "Repo:     $REPO_OWNER/$REPO_NAME"
echo "Branch:   ^main$"
echo "Config:   cloudbuild.yaml"
echo "Region:   $REGION"
echo

# Grant Cloud Build's service account the permissions it needs to
# (a) deploy to Cloud Run, (b) read secrets for the post-deploy scan.
# Idempotent — safe to re-run.
PROJECT_NUMBER=$(gcloud projects describe "$PROJECT_ID" --format='value(projectNumber)')
CB_SA="${PROJECT_NUMBER}@cloudbuild.gserviceaccount.com"
echo "Ensuring Cloud Build SA ($CB_SA) has required roles..."
for role in \
    roles/run.admin \
    roles/iam.serviceAccountUser \
    roles/secretmanager.secretAccessor; do
  gcloud projects add-iam-policy-binding "$PROJECT_ID" \
    --member="serviceAccount:$CB_SA" \
    --role="$role" \
    --condition=None >/dev/null 2>&1 || true
done

# Create the trigger. If one with the same name already exists, update it.
if gcloud builds triggers describe "$TRIGGER_NAME" --region="$REGION" >/dev/null 2>&1; then
  echo "Trigger '$TRIGGER_NAME' already exists — skipping create. To update, delete then re-run."
else
  gcloud builds triggers create github \
    --project="$PROJECT_ID" \
    --name="$TRIGGER_NAME" \
    --region="$REGION" \
    --repo-owner="$REPO_OWNER" \
    --repo-name="$REPO_NAME" \
    --branch-pattern='^main$' \
    --build-config='cloudbuild.yaml' \
    --description='Auto-deploy Signalix on push to main — full build + post-deploy /scan warmup'
  echo
  echo "✓ Trigger '$TRIGGER_NAME' created."
fi

echo
echo "Next push to $REPO_OWNER/$REPO_NAME main will auto-deploy."
echo "Monitor builds: https://console.cloud.google.com/cloud-build/builds?project=$PROJECT_ID"
