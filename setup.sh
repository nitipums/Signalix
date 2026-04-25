#!/bin/bash
# Signalix one-command setup script
# Run once in Google Cloud Shell:
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

# ── Load env.yaml ─────────────────────────────────────────────────────────────
if [ ! -f "env.yaml" ]; then
  echo ""
  echo "❌ env.yaml not found! สร้างก่อนครับ:"
  echo ""
  echo "cat > env.yaml << 'EOF'"
  echo "GCP_PROJECT_ID: $PROJECT_ID"
  echo "LINE_CHANNEL_ACCESS_TOKEN: \"your-token-here\""
  echo "LINE_CHANNEL_SECRET: \"your-secret-here\""
  echo "SCAN_SECRET: signalix-scan-2024"
  echo "EOF"
  exit 1
fi

LINE_TOKEN=$(grep 'LINE_CHANNEL_ACCESS_TOKEN' env.yaml | sed 's/.*: *//' | tr -d '"')
LINE_SECRET=$(grep 'LINE_CHANNEL_SECRET' env.yaml | sed 's/.*: *//' | tr -d '"')
SCAN_SECRET=$(grep 'SCAN_SECRET' env.yaml | sed 's/.*: *//' | tr -d '"')
SETTRADE_APP_ID=$(grep 'SETTRADE_APP_ID' env.yaml | sed 's/.*: *//' | tr -d '"')
SETTRADE_APP_SECRET=$(grep 'SETTRADE_APP_SECRET' env.yaml | sed 's/.*: *//' | tr -d '"')
SETTRADE_BROKER_ID=$(grep 'SETTRADE_BROKER_ID' env.yaml | sed 's/.*: *//' | tr -d '"')
SETTRADE_APP_CODE=$(grep 'SETTRADE_APP_CODE' env.yaml | sed 's/.*: *//' | tr -d '"')
SETTRADE_ACCOUNT_NO=$(grep 'SETTRADE_ACCOUNT_NO' env.yaml | sed 's/.*: *//' | tr -d '"')
SETTRADE_PIN=$(grep 'SETTRADE_PIN' env.yaml | sed 's/.*: *//' | tr -d '"')

# ── 1. Enable APIs ────────────────────────────────────────────────────────────
echo ""
echo "[1/7] Enabling APIs..."
gcloud services enable \
  run.googleapis.com \
  firestore.googleapis.com \
  cloudscheduler.googleapis.com \
  secretmanager.googleapis.com \
  cloudbuild.googleapis.com \
  artifactregistry.googleapis.com \
  --quiet
echo "      ✅ Done"

# ── 2. Secret Manager ─────────────────────────────────────────────────────────
echo ""
echo "[2/7] Storing secrets in Secret Manager..."

store_secret() {
  local NAME=$1
  local VALUE=$2
  echo -n "$VALUE" | gcloud secrets create $NAME --data-file=- --quiet 2>/dev/null || \
  echo -n "$VALUE" | gcloud secrets versions add $NAME --data-file=- --quiet
  echo "      ✅ $NAME"
}

store_secret "LINE_CHANNEL_ACCESS_TOKEN" "$LINE_TOKEN"
store_secret "LINE_CHANNEL_SECRET" "$LINE_SECRET"
store_secret "SCAN_SECRET" "$SCAN_SECRET"
[ -n "$SETTRADE_APP_ID" ]     && store_secret "SETTRADE_APP_ID"     "$SETTRADE_APP_ID"
[ -n "$SETTRADE_APP_SECRET" ] && store_secret "SETTRADE_APP_SECRET" "$SETTRADE_APP_SECRET"
[ -n "$SETTRADE_BROKER_ID" ]  && store_secret "SETTRADE_BROKER_ID"  "$SETTRADE_BROKER_ID"
[ -n "$SETTRADE_APP_CODE" ]   && store_secret "SETTRADE_APP_CODE"   "$SETTRADE_APP_CODE"
# Trading secrets are referenced unconditionally by cloudbuild.yaml; create
# placeholders so deploys never fail. Trading stays inert until real values land
# (settrade_client._get_equity_account returns None when PIN/account are empty).
store_secret "SETTRADE_ACCOUNT_NO" "${SETTRADE_ACCOUNT_NO:-PLACEHOLDER-NOT-SET}"
store_secret "SETTRADE_PIN"        "${SETTRADE_PIN:-PLACEHOLDER-NOT-SET}"

# ── 3. Grant Cloud Build access to secrets ────────────────────────────────────
echo ""
echo "[3/7] Granting permissions..."
PROJECT_NUMBER=$(gcloud projects describe $PROJECT_ID --format="value(projectNumber)")
CB_SA="$PROJECT_NUMBER@cloudbuild.gserviceaccount.com"
CR_SA="$PROJECT_NUMBER-compute@developer.gserviceaccount.com"

gcloud projects add-iam-policy-binding $PROJECT_ID \
  --member="serviceAccount:$CB_SA" \
  --role="roles/run.admin" --quiet 2>/dev/null || true

gcloud projects add-iam-policy-binding $PROJECT_ID \
  --member="serviceAccount:$CB_SA" \
  --role="roles/secretmanager.secretAccessor" --quiet 2>/dev/null || true

gcloud projects add-iam-policy-binding $PROJECT_ID \
  --member="serviceAccount:$CR_SA" \
  --role="roles/secretmanager.secretAccessor" --quiet 2>/dev/null || true

gcloud projects add-iam-policy-binding $PROJECT_ID \
  --member="serviceAccount:$CB_SA" \
  --role="roles/iam.serviceAccountUser" --quiet 2>/dev/null || true

echo "      ✅ Done"

# ── 4. Firestore ──────────────────────────────────────────────────────────────
echo ""
echo "[4/7] Creating Firestore database..."
gcloud firestore databases create --location=$REGION --quiet 2>/dev/null || \
  echo "      (already exists)"
echo "      ✅ Done"

# ── 5. Deploy to Cloud Run ────────────────────────────────────────────────────
echo ""
echo "[5/7] Deploying to Cloud Run (~3-5 min)..."
gcloud run deploy $SERVICE_NAME \
  --source . \
  --region $REGION \
  --allow-unauthenticated \
  --memory 512Mi \
  --timeout 60 \
  --update-secrets=LINE_CHANNEL_ACCESS_TOKEN=LINE_CHANNEL_ACCESS_TOKEN:latest,LINE_CHANNEL_SECRET=LINE_CHANNEL_SECRET:latest,SCAN_SECRET=SCAN_SECRET:latest \
  --set-env-vars=GCP_PROJECT_ID=$PROJECT_ID,ENVIRONMENT=production \
  --quiet

SERVICE_URL=$(gcloud run services describe $SERVICE_NAME \
  --region $REGION --format "value(status.url)")
echo "      ✅ $SERVICE_URL"

# ── 6. Cloud Scheduler ────────────────────────────────────────────────────────
echo ""
echo "[6/7] Creating Cloud Scheduler jobs..."

create_job() {
  local NAME=$1 SCHEDULE=$2 SCAN_TYPE=$3
  gcloud scheduler jobs delete $NAME --location=$REGION --quiet 2>/dev/null || true
  gcloud scheduler jobs create http $NAME \
    --location=$REGION \
    --schedule="$SCHEDULE" \
    --time-zone="Asia/Bangkok" \
    --uri="$SERVICE_URL/scan" \
    --http-method=POST \
    --headers="Content-Type=application/json,X-Scan-Secret=$SCAN_SECRET" \
    --message-body="{\"scan_type\": \"$SCAN_TYPE\", \"broadcast\": true}" \
    --quiet
  echo "      ✅ $NAME"
}

create_job "signalix-morning"   "30 10 * * 1-5" "breakout"
create_job "signalix-midday"    "0 12 * * 1-5"  "breadth"
create_job "signalix-preclose"  "15 15 * * 1-5" "vcp"
create_job "signalix-postclose" "30 16 * * 1-5" "full"

# ── 7. Cloud Build Trigger (GitHub → auto-deploy) ─────────────────────────────
echo ""
echo "[7/7] Setting up auto-deploy from GitHub..."
echo ""
echo "  ทำขั้นตอนนี้ใน GCP Console:"
echo "  1. ไปที่ Cloud Build → Triggers"
echo "     https://console.cloud.google.com/cloud-build/triggers?project=$PROJECT_ID"
echo "  2. กด 'Connect Repository' → GitHub"
echo "  3. เลือก nitipums/Signalix"
echo "  4. กด 'Create a trigger'"
echo "     - Branch: main"
echo "     - Config: cloudbuild.yaml"
echo "  ✅ หลังจากนี้ push code ที่ GitHub ปุ๊บ deploy อัตโนมัติเลย!"

# ── Done ──────────────────────────────────────────────────────────────────────
echo ""
echo "==================================="
echo "  ✅ Signalix พร้อมใช้งาน!"
echo "==================================="
echo ""
echo "  URL:     $SERVICE_URL"
echo "  Webhook: $SERVICE_URL/webhook/line"
echo "  Health:  $SERVICE_URL/health"
echo ""
echo "  ตั้ง LINE Webhook URL ที่:"
echo "  https://developers.line.biz/console/"
echo "  → Channel → Messaging API → Webhook URL"
echo "  → ใส่: $SERVICE_URL/webhook/line"
echo ""
