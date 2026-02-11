#!/bin/bash
# =============================================================================
# Meshtastic Mobile iOS - Deployment Script
# =============================================================================
# Deploys app to TestFlight or App Store Connect
#
# Usage:
#   ./deploy.sh testflight          # Upload to TestFlight
#   ./deploy.sh appstore            # Submit to App Store Review
#   ./deploy.sh validate            # Validate archive only
# =============================================================================

set -e

# Configuration
PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PROJECT_NAME="MeshtasticMobile"
BUILD_DIR="${PROJECT_DIR}/build"
ARCHIVE_DIR="${BUILD_DIR}/archives"
EXPORT_DIR="${BUILD_DIR}/export"

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
NC='\033[0m'

log_info() { echo -e "${BLUE}[INFO]${NC} $1"; }
log_success() { echo -e "${GREEN}[SUCCESS]${NC} $1"; }
log_warning() { echo -e "${YELLOW}[WARNING]${NC} $1"; }
log_error() { echo -e "${RED}[ERROR]${NC} $1"; }
log_step() { echo -e "${CYAN}[STEP]${NC} $1"; }

# Check requirements
check_requirements() {
    log_info "Checking requirements..."
    
    # Check for API key or credentials
    if [ -n "${APP_STORE_CONNECT_API_KEY}" ]; then
        if [ ! -f "${APP_STORE_CONNECT_API_KEY}" ]; then
            log_error "API key file not found: ${APP_STORE_CONNECT_API_KEY}"
            exit 1
        fi
        log_info "Using App Store Connect API Key"
        AUTH_METHOD="apikey"
    elif [ -n "${APPLE_ID}" ] && [ -n "${APPLE_APP_PASSWORD}" ]; then
        log_info "Using Apple ID authentication"
        AUTH_METHOD="appleid"
    else
        log_error "No authentication configured"
        echo ""
        echo "Configure one of the following:"
        echo ""
        echo "Option 1 - API Key (Recommended for CI/CD):"
        echo "  export APP_STORE_CONNECT_API_KEY=\"/path/to/AuthKey_XXXXXX.p8\""
        echo "  export APP_STORE_CONNECT_API_KEY_ID=\"XXXXXXXXXX\""
        echo "  export APP_STORE_CONNECT_API_ISSUER=\"xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx\""
        echo ""
        echo "Option 2 - Apple ID:"
        echo "  export APPLE_ID=\"your@email.com\""
        echo "  export APPLE_APP_PASSWORD=\"xxxx-xxxx-xxxx-xxxx\"  # App-specific password"
        exit 1
    fi
    
    # Check for xcrun altool or xcrun notarytool
    if ! command -v xcrun &> /dev/null; then
        log_error "Xcode command line tools not found"
        exit 1
    fi
}

# Find latest archive
find_latest_archive() {
    ARCHIVE=$(find "${ARCHIVE_DIR}" -name "*.xcarchive" -type d | sort -r | head -1)
    
    if [ -z "$ARCHIVE" ]; then
        log_error "No archive found in ${ARCHIVE_DIR}"
        echo "Run ./archive.sh first"
        exit 1
    fi
    
    log_info "Using archive: $(basename "$ARCHIVE")"
    echo "$ARCHIVE"
}

# Find latest IPA
find_latest_ipa() {
    IPA=$(find "${EXPORT_DIR}" -name "*.ipa" | sort -r | head -1)
    
    if [ -z "$IPA" ]; then
        log_error "No IPA found in ${EXPORT_DIR}"
        echo "Run ./archive.sh --export-ipa first"
        exit 1
    fi
    
    log_info "Using IPA: $(basename "$IPA")"
    echo "$IPA"
}

# Validate archive/IPA
validate_app() {
    log_step "Validating app..."
    
    IPA=$(find_latest_ipa)
    
    if [ "$AUTH_METHOD" == "apikey" ]; then
        xcrun altool --validate-app \
            -f "$IPA" \
            --type ios \
            --apiKey "${APP_STORE_CONNECT_API_KEY_ID}" \
            --apiIssuer "${APP_STORE_CONNECT_API_ISSUER}" \
            --show-progress
    else
        xcrun altool --validate-app \
            -f "$IPA" \
            --type ios \
            -u "${APPLE_ID}" \
            -p "${APPLE_APP_PASSWORD}" \
            --show-progress
    fi
    
    log_success "Validation passed"
}

# Upload to TestFlight
upload_testflight() {
    log_step "Uploading to TestFlight..."
    
    # First ensure we have an IPA
    IPA=$(find_latest_ipa)
    
    # Validate first
    validate_app
    
    log_info "Uploading to App Store Connect..."
    
    if [ "$AUTH_METHOD" == "apikey" ]; then
        xcrun altool --upload-app \
            -f "$IPA" \
            --type ios \
            --apiKey "${APP_STORE_CONNECT_API_KEY_ID}" \
            --apiIssuer "${APP_STORE_CONNECT_API_ISSUER}" \
            --show-progress
    else
        xcrun altool --upload-app \
            -f "$IPA" \
            --type ios \
            -u "${APPLE_ID}" \
            -p "${APPLE_APP_PASSWORD}" \
            --show-progress
    fi
    
    log_success "Upload to TestFlight complete!"
    echo ""
    echo "Next steps:"
    echo "  1. Go to App Store Connect"
    echo "  2. Select your app → TestFlight"
    echo "  3. Wait for processing (~15-30 min)"
    echo "  4. Add testers or release to external testing"
}

# Submit to App Store
submit_appstore() {
    log_step "Submitting to App Store..."
    
    log_warning "This will submit for App Store Review!"
    echo ""
    read -p "Are you sure you want to continue? (y/N) " -n 1 -r
    echo ""
    
    if [[ ! $REPLY =~ ^[Yy]$ ]]; then
        log_info "Cancelled"
        exit 0
    fi
    
    # Upload same as TestFlight
    upload_testflight
    
    log_success "App uploaded!"
    echo ""
    echo "Complete the submission:"
    echo "  1. Go to App Store Connect"
    echo "  2. Select your app → App Store"
    echo "  3. Select the build"
    echo "  4. Fill in release information"
    echo "  5. Submit for Review"
}

# Full deployment pipeline
full_deploy() {
    local TARGET="$1"
    
    log_info "Starting full deployment pipeline..."
    echo ""
    
    # Step 1: Build
    log_step "Step 1/4: Building..."
    "${PROJECT_DIR}/Scripts/build.sh" release
    
    # Step 2: Test
    log_step "Step 2/4: Running tests..."
    "${PROJECT_DIR}/Scripts/test.sh" unit || {
        log_warning "Tests failed, continue anyway? (y/N)"
        read -n 1 -r
        echo ""
        if [[ ! $REPLY =~ ^[Yy]$ ]]; then
            exit 1
        fi
    }
    
    # Step 3: Archive
    log_step "Step 3/4: Creating archive..."
    "${PROJECT_DIR}/Scripts/archive.sh" --export-ipa
    
    # Step 4: Deploy
    log_step "Step 4/4: Deploying to ${TARGET}..."
    case "$TARGET" in
        testflight)
            upload_testflight
            ;;
        appstore)
            submit_appstore
            ;;
    esac
    
    log_success "Deployment pipeline complete!"
}

# Print usage
print_usage() {
    echo "Usage: $0 <command>"
    echo ""
    echo "Commands:"
    echo "  testflight     Upload to TestFlight"
    echo "  appstore       Submit to App Store Review"
    echo "  validate       Validate IPA only"
    echo "  full-tf        Full pipeline → TestFlight"
    echo "  full-as        Full pipeline → App Store"
    echo "  help           Show this help"
    echo ""
    echo "Required Environment Variables (Option 1 - API Key):"
    echo "  APP_STORE_CONNECT_API_KEY       Path to AuthKey_XXXXXX.p8"
    echo "  APP_STORE_CONNECT_API_KEY_ID    Key ID (10 chars)"
    echo "  APP_STORE_CONNECT_API_ISSUER    Issuer ID (UUID)"
    echo ""
    echo "Required Environment Variables (Option 2 - Apple ID):"
    echo "  APPLE_ID                        Your Apple ID email"
    echo "  APPLE_APP_PASSWORD              App-specific password"
    echo ""
    echo "Examples:"
    echo "  # Using API Key"
    echo "  export APP_STORE_CONNECT_API_KEY=\"~/keys/AuthKey_ABC123.p8\""
    echo "  export APP_STORE_CONNECT_API_KEY_ID=\"ABC123XYZ\""
    echo "  export APP_STORE_CONNECT_API_ISSUER=\"12345678-1234-1234-1234-123456789012\""
    echo "  $0 testflight"
    echo ""
    echo "  # Full pipeline"
    echo "  export APPLE_TEAM_ID=\"XXXXXXXXXX\""
    echo "  $0 full-tf"
}

# Main
main() {
    cd "${PROJECT_DIR}"
    
    case "${1:-help}" in
        testflight|tf)
            check_requirements
            upload_testflight
            ;;
        appstore|as)
            check_requirements
            submit_appstore
            ;;
        validate)
            check_requirements
            validate_app
            ;;
        full-tf|full-testflight)
            check_requirements
            full_deploy "testflight"
            ;;
        full-as|full-appstore)
            check_requirements
            full_deploy "appstore"
            ;;
        help|--help|-h)
            print_usage
            ;;
        *)
            log_error "Unknown command: $1"
            print_usage
            exit 1
            ;;
    esac
}

main "$@"
