#!/bin/bash
# =============================================================================
# Meshtastic Mobile iOS - Archive Script
# =============================================================================
# Creates an archive (.xcarchive) for App Store or Ad Hoc distribution
#
# Usage:
#   ./archive.sh                    # Create release archive
#   ./archive.sh --export-ipa       # Create archive and export IPA
#   ./archive.sh --adhoc            # Create Ad Hoc distribution IPA
# =============================================================================

set -e

# Configuration
PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PROJECT_NAME="MeshtasticMobile"
SCHEME="MeshtasticMobile"
BUILD_DIR="${PROJECT_DIR}/build"
ARCHIVE_DIR="${BUILD_DIR}/archives"
EXPORT_DIR="${BUILD_DIR}/export"

# Build info
BUILD_NUMBER="${BUILD_NUMBER:-$(date +%Y%m%d%H%M)}"
VERSION="${VERSION:-1.0.0}"

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

log_info() { echo -e "${BLUE}[INFO]${NC} $1"; }
log_success() { echo -e "${GREEN}[SUCCESS]${NC} $1"; }
log_warning() { echo -e "${YELLOW}[WARNING]${NC} $1"; }
log_error() { echo -e "${RED}[ERROR]${NC} $1"; }

# Check requirements
check_requirements() {
    if [ -z "${APPLE_TEAM_ID}" ]; then
        log_error "APPLE_TEAM_ID environment variable not set"
        echo "Export your Apple Developer Team ID:"
        echo "  export APPLE_TEAM_ID=\"XXXXXXXXXX\""
        exit 1
    fi
    
    if ! command -v xcodebuild &> /dev/null; then
        log_error "Xcode not found"
        exit 1
    fi
    
    log_info "Team ID: ${APPLE_TEAM_ID}"
    log_info "Version: ${VERSION} (${BUILD_NUMBER})"
}

# Create archive
create_archive() {
    log_info "Creating archive..."
    
    mkdir -p "${ARCHIVE_DIR}"
    
    ARCHIVE_PATH="${ARCHIVE_DIR}/${PROJECT_NAME}_${VERSION}_${BUILD_NUMBER}.xcarchive"
    
    xcodebuild archive \
        -project "${PROJECT_DIR}/${PROJECT_NAME}.xcodeproj" \
        -scheme "${SCHEME}" \
        -destination "generic/platform=iOS" \
        -archivePath "${ARCHIVE_PATH}" \
        DEVELOPMENT_TEAM="${APPLE_TEAM_ID}" \
        CURRENT_PROJECT_VERSION="${BUILD_NUMBER}" \
        MARKETING_VERSION="${VERSION}" \
        CODE_SIGN_STYLE="Automatic" \
        | xcpretty 2>/dev/null || xcodebuild archive \
            -project "${PROJECT_DIR}/${PROJECT_NAME}.xcodeproj" \
            -scheme "${SCHEME}" \
            -destination "generic/platform=iOS" \
            -archivePath "${ARCHIVE_PATH}" \
            DEVELOPMENT_TEAM="${APPLE_TEAM_ID}" \
            CURRENT_PROJECT_VERSION="${BUILD_NUMBER}" \
            MARKETING_VERSION="${VERSION}" \
            CODE_SIGN_STYLE="Automatic"
    
    if [ -d "${ARCHIVE_PATH}" ]; then
        log_success "Archive created: ${ARCHIVE_PATH}"
        echo "${ARCHIVE_PATH}"
    else
        log_error "Archive creation failed"
        exit 1
    fi
}

# Create export options plist for App Store
create_appstore_export_options() {
    cat > "${BUILD_DIR}/ExportOptions.plist" << EOF
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
    <key>method</key>
    <string>app-store-connect</string>
    <key>teamID</key>
    <string>${APPLE_TEAM_ID}</string>
    <key>uploadSymbols</key>
    <true/>
    <key>destination</key>
    <string>upload</string>
    <key>signingStyle</key>
    <string>automatic</string>
</dict>
</plist>
EOF
    log_info "Created App Store export options"
}

# Create export options plist for Ad Hoc
create_adhoc_export_options() {
    cat > "${BUILD_DIR}/ExportOptions.plist" << EOF
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
    <key>method</key>
    <string>ad-hoc</string>
    <key>teamID</key>
    <string>${APPLE_TEAM_ID}</string>
    <key>uploadSymbols</key>
    <true/>
    <key>compileBitcode</key>
    <false/>
    <key>signingStyle</key>
    <string>automatic</string>
    <key>thinning</key>
    <string>&lt;none&gt;</string>
</dict>
</plist>
EOF
    log_info "Created Ad Hoc export options"
}

# Export IPA from archive
export_ipa() {
    local ARCHIVE_PATH="$1"
    local EXPORT_METHOD="${2:-app-store}"
    
    log_info "Exporting IPA (${EXPORT_METHOD})..."
    
    mkdir -p "${EXPORT_DIR}"
    
    if [ "$EXPORT_METHOD" == "adhoc" ]; then
        create_adhoc_export_options
    else
        create_appstore_export_options
    fi
    
    xcodebuild -exportArchive \
        -archivePath "${ARCHIVE_PATH}" \
        -exportOptionsPlist "${BUILD_DIR}/ExportOptions.plist" \
        -exportPath "${EXPORT_DIR}" \
        | xcpretty 2>/dev/null || xcodebuild -exportArchive \
            -archivePath "${ARCHIVE_PATH}" \
            -exportOptionsPlist "${BUILD_DIR}/ExportOptions.plist" \
            -exportPath "${EXPORT_DIR}"
    
    IPA_FILE=$(find "${EXPORT_DIR}" -name "*.ipa" | head -1)
    
    if [ -n "$IPA_FILE" ]; then
        log_success "IPA exported: ${IPA_FILE}"
        
        # Show IPA info
        IPA_SIZE=$(du -h "$IPA_FILE" | cut -f1)
        log_info "IPA size: ${IPA_SIZE}"
    else
        log_error "IPA export failed"
        exit 1
    fi
}

# Print usage
print_usage() {
    echo "Usage: $0 [options]"
    echo ""
    echo "Options:"
    echo "  (none)         Create archive only"
    echo "  --export-ipa   Create archive and export App Store IPA"
    echo "  --adhoc        Create archive and export Ad Hoc IPA"
    echo "  --help         Show this help"
    echo ""
    echo "Required Environment Variables:"
    echo "  APPLE_TEAM_ID  Your Apple Developer Team ID"
    echo ""
    echo "Optional Environment Variables:"
    echo "  VERSION        App version (default: 1.0.0)"
    echo "  BUILD_NUMBER   Build number (default: timestamp)"
    echo ""
    echo "Examples:"
    echo "  APPLE_TEAM_ID=ABC123 $0"
    echo "  APPLE_TEAM_ID=ABC123 VERSION=1.2.0 $0 --export-ipa"
}

# Main
main() {
    cd "${PROJECT_DIR}"
    
    case "$1" in
        --help|-h)
            print_usage
            exit 0
            ;;
        --export-ipa)
            check_requirements
            ARCHIVE_PATH=$(create_archive)
            export_ipa "$ARCHIVE_PATH" "app-store"
            ;;
        --adhoc)
            check_requirements
            ARCHIVE_PATH=$(create_archive)
            export_ipa "$ARCHIVE_PATH" "adhoc"
            ;;
        "")
            check_requirements
            create_archive
            ;;
        *)
            log_error "Unknown option: $1"
            print_usage
            exit 1
            ;;
    esac
    
    log_success "Archive complete!"
    echo ""
    echo "Output:"
    echo "  Archives: ${ARCHIVE_DIR}/"
    echo "  Exports:  ${EXPORT_DIR}/"
}

main "$@"
