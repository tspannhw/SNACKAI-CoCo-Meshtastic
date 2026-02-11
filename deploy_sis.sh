#!/bin/bash
# ============================================================================
# Deploy Geospatial Dashboard to Streamlit in Snowflake
# ============================================================================

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
APP_FILE="geospatial_dashboard_sis.py"
STAGE_PATH="@DEMO.DEMO.STREAMLIT_APPS/geospatial"

RED='\033[0;31m'
GREEN='\033[0;32m'
BLUE='\033[0;34m'
NC='\033[0m'

log_info() { echo -e "${BLUE}[INFO]${NC} $1"; }
log_success() { echo -e "${GREEN}[SUCCESS]${NC} $1"; }
log_error() { echo -e "${RED}[ERROR]${NC} $1"; }

# Check for snow CLI
if ! command -v snow &> /dev/null; then
    log_error "Snowflake CLI (snow) not found"
    echo "Install with: pip install snowflake-cli-labs"
    exit 1
fi

log_info "Deploying Meshtastic Geospatial Dashboard to Snowflake..."

# Step 1: Create stage if not exists
log_info "Creating stage..."
snow sql -q "CREATE STAGE IF NOT EXISTS DEMO.DEMO.STREAMLIT_APPS DIRECTORY = (ENABLE = TRUE);" 2>/dev/null || true

# Step 2: Upload the app file
log_info "Uploading ${APP_FILE} to ${STAGE_PATH}..."
snow stage copy "${SCRIPT_DIR}/${APP_FILE}" "${STAGE_PATH}/" --overwrite

# Step 3: Create or replace the Streamlit app
log_info "Creating Streamlit app..."
snow sql -q "
CREATE OR REPLACE STREAMLIT DEMO.DEMO.MESHTASTIC_GEOSPATIAL_DASHBOARD
    ROOT_LOCATION = '${STAGE_PATH}'
    MAIN_FILE = '${APP_FILE}'
    QUERY_WAREHOUSE = 'INGEST'
    TITLE = 'Meshtastic Geospatial Intelligence'
    COMMENT = 'AI-enhanced mesh network geospatial analysis';
"

# Step 4: Grant access
log_info "Granting access..."
snow sql -q "GRANT USAGE ON STREAMLIT DEMO.DEMO.MESHTASTIC_GEOSPATIAL_DASHBOARD TO ROLE ACCOUNTADMIN;"

# Step 5: Get app URL
log_success "Deployment complete!"
echo ""
echo "Access your app at:"
snow sql -q "SELECT 'https://app.snowflake.com/' || CURRENT_ORGANIZATION_NAME() || '/' || CURRENT_ACCOUNT_NAME() || '/#/streamlit-apps/DEMO.DEMO.MESHTASTIC_GEOSPATIAL_DASHBOARD' as APP_URL;"

echo ""
log_info "Or open Snowsight → Streamlit → MESHTASTIC_GEOSPATIAL_DASHBOARD"
