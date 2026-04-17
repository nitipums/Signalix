#!/bin/bash
# Signalix one-command setup script
# Run this once in Google Cloud Shell:
#   chmod +x setup.sh && ./setup.sh

set -e

PROJECT_ID=$(gcloud config get-value project)
REGION="asia-southeast1"
SERVICE_NAME="signalix"

echo "==================================="
echo "  Signalix Setup"
echo "  Project: $PROJECT_ID"
echo "  Region:  $REGION"
echo "==================================="

# ── 1. Enable APIs ────────────────────────────────────────────────────────────
echo ""
echo "[1/6] Enabling required APIs..."
gcloud services enable \
  run.googleapis.com \
  firestore.googleapis.com \
  cloudscheduler.googleapis.com \
  secretmanager.googleapis.com \
  cloudbuild.googleapis.com \
  artifactregistry.googleapis.com \
  --quiet

echo "      ✅ APIs enabled"

# ── 2. Artifact Registry ──────────────────────────────────────────────────────
echo ""
echo "[2/6] Creating Artifact Registry..."
gcloud artifacts repositories create signalix-repo \
  --repository-format=docker \
  --location=$REGION \
  --quiet 2>/dev/null || echo "      (already exists, skipping)"

echo "      ✅ Artifact Registry ready"

# ── 3. Firestore ──────────────────────────────────────────────────────────────
echo ""
echo "[3/6] Creating Firestore database..."
gcloud firestore databases create \
  --location=$REGION \
  --quiet 2>/dev/null || echo "      (already exists, skipping)"

echo "      ✅ Firestore ready"

# ── 4. Load credentials from env.yaml ────────────────────────────────────────
echo ""
echo "[4/6] Reading credentials from env.yaml..."

if [ ! -f "env.yaml" ]; then
  echo ""
  echo "  ❌ env.yaml not found!"
  echo "  Please create it first:"
  echo ""
  echo "  cat > env.yaml << 'EOF'"
  echo "  GCP_PROJECT_ID: $PROJECT_ID"
  echo "  LINE_CHANNEL_ACCESS_TOKEN: \"your-token-here\""
  echo "  LINE_CHANNEL_SECRET: \"your-secret-here\""
  echo "  SCAN_SECRET: signalix-scan-2024"
  echo "  ENVIRONMENT: production"
  echo "  EOF"
  echo ""
  exit 1
fi

echo "      ✅ env.yaml found"

# ── 5. Deploy to Cloud Run ────────────────────────────────────────────────────
echo ""
echo "[5/6] Deploying to Cloud Run (this takes ~3-5 minutes)..."

gcloud run deploy $SERVICE_NAME \
  --source . \
  --region $REGION \
  --allow-unauthenticated \
  --env-vars-file env.yaml \
  --memory 512Mi \
  --timeout 60 \
  --quiet

SERVICE_URL=$(gcloud run services describe $SERVICE_NAME \
  --region $REGION \
  --format "value(status.url)")

echo "      ✅ Deployed: $SERVICE_URL"

# ── 6. Cloud Scheduler jobs ───────────────────────────────────────────────────
echo ""
echo "[6/6] Creating Cloud Scheduler jobs..."

SCAN_SECRET_VAL=$(grep 'SCAN_SECRET' env.yaml | awk '{print $2}' | tr -d '"')

create_job() {
  local NAME=$1
  local SCHEDULE=$2
  local SCAN_TYPE=$3
  local DESC=$4

  gcloud scheduler jobs delete $NAME --location=$REGION --quiet 2>/dev/null || true

  gcloud scheduler jobs create http $NAME \
    --location=$REGION \
    --schedule="$SCHEDULE" \
    --time-zone="Asia/Bangkok" \
    --uri="$SERVICE_URL/scan" \
    --http-method=POST \
    --headers="Content-Type=application/json,X-Scan-Secret=$SCAN_SECRET_VAL" \
    --message-body="{\"scan_type\": \"$SCAN_TYPE\", \"broadcast\": true}" \
    --description="$DESC" \
    --quiet

  echo "      ✅ $NAME ($SCHEDULE ICT)"
}

create_job "signalix-morning"   "30 3 * * 1-5"  "breakout" "10:30 ICT Morning breakout scan"
create_job "signalix-midday"    "0 5 * * 1-5"   "breadth"  "12:00 ICT Midday market breadth"
create_job "signalix-preclose"  "15 8 * * 1-5"  "vcp"      "15:15 ICT Pre-close VCP scan"
create_job "signalix-postclose" "30 9 * * 1-5"  "full"     "16:30 ICT Post-close full report"

# ── Done ──────────────────────────────────────────────────────────────────────
echo ""
echo "==================================="
echo "  ✅ Signalix is live!"
echo "==================================="
echo ""
echo "  Service URL: $SERVICE_URL"
echo ""
echo "  Next step — set LINE Webhook URL:"
echo "  $SERVICE_URL/webhook/line"
echo ""
echo "  Test health check:"
echo "  curl $SERVICE_URL/health"
echo ""
