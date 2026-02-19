#!/bin/bash
# ============================================================================
# deploy.sh — Automated Snowflake Native App Deployment
# ============================================================================
#
# Usage:
#   ./deploy.sh                     # Deploy with defaults
#   ./deploy.sh --connection myconn # Use a named SnowSQL connection
#   ./deploy.sh --drop              # Drop existing app first, then redeploy
#   ./deploy.sh --smoke             # Run smoke tests after deploy
#
# Prerequisites:
#   - SnowSQL installed and configured (~/.snowsql/config)
#   - Account with CREATE APPLICATION PACKAGE privileges
#   - Cortex enabled on the Snowflake account
# ============================================================================

set -euo pipefail

# ── Configuration ───────────────────────────────────────────────────────────
APP_PACKAGE="NATIVE_OBS_PKG"
APP_NAME="NATIVE_OBS_APP"
STAGE_SCHEMA="STAGE_CONTENT"
STAGE_NAME="APP_STAGE"
FULL_STAGE="@${APP_PACKAGE}.${STAGE_SCHEMA}.${STAGE_NAME}"

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
NATIVE_APP_DIR="${SCRIPT_DIR}/native_app"

# ── Argument parsing ───────────────────────────────────────────────────────
SNOWSQL_CONN=""
DROP_FIRST=false
RUN_SMOKE=false

while [[ $# -gt 0 ]]; do
  case $1 in
    --connection|-c) SNOWSQL_CONN="--connection $2"; shift 2 ;;
    --drop)          DROP_FIRST=true;  shift ;;
    --smoke)         RUN_SMOKE=true;   shift ;;
    --help|-h)
      head -18 "$0" | tail -15
      exit 0
      ;;
    *) echo "Unknown option: $1"; exit 1 ;;
  esac
done

# ── Helper ──────────────────────────────────────────────────────────────────
run_sql() {
  # shellcheck disable=SC2086
  snowsql $SNOWSQL_CONN -o exit_on_error=true -o friendly=false -o header=false \
    -o timing=false -o output_format=plain -q "$1"
}

run_file() {
  # shellcheck disable=SC2086
  snowsql $SNOWSQL_CONN -o exit_on_error=true -f "$1"
}

banner() {
  echo ""
  echo "══════════════════════════════════════════════════════════════════"
  echo "  $1"
  echo "══════════════════════════════════════════════════════════════════"
}

# ── Pre-flight checks ──────────────────────────────────────────────────────
if ! command -v snowsql &> /dev/null; then
  echo "❌  SnowSQL not found. Install with:  brew install --cask snowflake-snowsql"
  exit 1
fi

if [ ! -f "${NATIVE_APP_DIR}/manifest.yml" ]; then
  echo "❌  manifest.yml not found in ${NATIVE_APP_DIR}"
  exit 1
fi

# ── Step 0: Drop existing app (optional) ──────────────────────────────────
if [ "$DROP_FIRST" = true ]; then
  banner "DROPPING EXISTING APPLICATION"
  run_sql "DROP APPLICATION IF EXISTS ${APP_NAME} CASCADE;" || true
  run_sql "DROP APPLICATION PACKAGE IF EXISTS ${APP_PACKAGE};" || true
  echo "✅  Existing app dropped"
fi

# ── Step 1: Create application package & stage ────────────────────────────
banner "STEP 1: Creating Application Package & Stage"

run_sql "
  CREATE APPLICATION PACKAGE IF NOT EXISTS ${APP_PACKAGE};
  CREATE SCHEMA IF NOT EXISTS ${APP_PACKAGE}.${STAGE_SCHEMA};
  CREATE STAGE IF NOT EXISTS ${FULL_STAGE}
    DIRECTORY = (ENABLE = TRUE)
    COMMENT = 'Native Observability App deployment stage';
"
echo "✅  Package & stage ready"

# ── Step 2: Upload app files to stage ─────────────────────────────────────
banner "STEP 2: Uploading App Files"

echo "   📄 manifest.yml"
run_sql "PUT file://${NATIVE_APP_DIR}/manifest.yml ${FULL_STAGE}/ AUTO_COMPRESS=FALSE OVERWRITE=TRUE;"

echo "   📄 setup.sql"
run_sql "PUT file://${NATIVE_APP_DIR}/setup.sql ${FULL_STAGE}/ AUTO_COMPRESS=FALSE OVERWRITE=TRUE;"

echo "   📁 scripts/"
for f in "${NATIVE_APP_DIR}"/scripts/*.sql; do
  echo "   📄 scripts/$(basename "$f")"
  run_sql "PUT file://${f} ${FULL_STAGE}/scripts/ AUTO_COMPRESS=FALSE OVERWRITE=TRUE;"
done

echo "   📁 streamlit/"
echo "   📄 streamlit/Home.py"
run_sql "PUT file://${NATIVE_APP_DIR}/streamlit/Home.py ${FULL_STAGE}/streamlit/ AUTO_COMPRESS=FALSE OVERWRITE=TRUE;"

for f in "${NATIVE_APP_DIR}"/streamlit/pages/*.py; do
  echo "   📄 streamlit/pages/$(basename "$f")"
  run_sql "PUT file://${f} ${FULL_STAGE}/streamlit/pages/ AUTO_COMPRESS=FALSE OVERWRITE=TRUE;"
done

echo "✅  All files uploaded"

# ── Step 3: Verify staged files ───────────────────────────────────────────
banner "STEP 3: Verifying Staged Files"
run_sql "LIST ${FULL_STAGE}/ PATTERN='.*';"
echo "✅  Stage contents verified"

# ── Step 4: Create or upgrade the application ────────────────────────────
banner "STEP 4: Installing Application"

APP_EXISTS=$(run_sql "SELECT COUNT(*) FROM INFORMATION_SCHEMA.DATABASES WHERE DATABASE_NAME = '${APP_NAME}';" 2>/dev/null | tr -d '[:space:]' || echo "0")

if [ "$APP_EXISTS" = "0" ]; then
  echo "   Creating new application..."
  run_sql "
    CREATE APPLICATION ${APP_NAME}
      FROM APPLICATION PACKAGE ${APP_PACKAGE}
      USING '${FULL_STAGE}';
  "
  echo "✅  Application created"
else
  echo "   Upgrading existing application..."
  run_sql "ALTER APPLICATION ${APP_NAME} UPGRADE USING '${FULL_STAGE}';"
  echo "✅  Application upgraded"
fi

# ── Step 5: Smoke tests (optional) ───────────────────────────────────────
if [ "$RUN_SMOKE" = true ]; then
  banner "STEP 5: Running Smoke Tests"
  run_file "${SCRIPT_DIR}/tests/sql/smoke_checks.sql"
  echo "✅  Smoke tests complete"
fi

# ── Done ──────────────────────────────────────────────────────────────────
banner "🎉 DEPLOYMENT COMPLETE"

echo ""
echo "  App Package : ${APP_PACKAGE}"
echo "  Application : ${APP_NAME}"
echo "  Stage       : ${FULL_STAGE}"
echo ""
echo "  ➤  Open Snowsight → Data Products → Apps → ${APP_NAME}"
echo "  ➤  Or run:  snowsql -q \"CALL ${APP_NAME}.APP_CORE.sp_healthcheck();\""
echo ""
