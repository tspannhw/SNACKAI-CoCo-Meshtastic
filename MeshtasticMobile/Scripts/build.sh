#!/bin/bash
# =============================================================================
# Meshtastic Mobile iOS - Build Script
# =============================================================================
# Usage:
#   ./build.sh              # Build for simulator (default)
#   ./build.sh simulator    # Build for iOS Simulator
#   ./build.sh device       # Build for physical device
#   ./build.sh release      # Build release configuration
#   ./build.sh clean        # Clean build artifacts
# =============================================================================

set -e  # Exit on error

# Configuration
PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PROJECT_NAME="MeshtasticMobile"
SCHEME="MeshtasticMobile"
BUILD_DIR="${PROJECT_DIR}/build"
DERIVED_DATA="${BUILD_DIR}/DerivedData"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

log_info() { echo -e "${BLUE}[INFO]${NC} $1"; }
log_success() { echo -e "${GREEN}[SUCCESS]${NC} $1"; }
log_warning() { echo -e "${YELLOW}[WARNING]${NC} $1"; }
log_error() { echo -e "${RED}[ERROR]${NC} $1"; }

# Check for Xcode
check_xcode() {
    if ! command -v xcodebuild &> /dev/null; then
        log_error "Xcode command line tools not found"
        echo "Install with: xcode-select --install"
        exit 1
    fi
    
    XCODE_VERSION=$(xcodebuild -version | head -1)
    log_info "Using $XCODE_VERSION"
}

# Clean build
clean_build() {
    log_info "Cleaning build artifacts..."
    rm -rf "${BUILD_DIR}"
    rm -rf "${PROJECT_DIR}/${PROJECT_NAME}.xcodeproj/xcuserdata"
    
    if [ -d "${PROJECT_DIR}/${PROJECT_NAME}.xcodeproj" ]; then
        xcodebuild clean \
            -project "${PROJECT_DIR}/${PROJECT_NAME}.xcodeproj" \
            -scheme "${SCHEME}" \
            -quiet 2>/dev/null || true
    fi
    
    log_success "Clean complete"
}

# Build for simulator
build_simulator() {
    local CONFIG="${1:-Debug}"
    log_info "Building for iOS Simulator (${CONFIG})..."
    
    mkdir -p "${BUILD_DIR}"
    
    # Get available simulator
    SIMULATOR_ID=$(xcrun simctl list devices available | grep "iPhone" | head -1 | grep -oE '[A-F0-9-]{36}')
    
    if [ -z "$SIMULATOR_ID" ]; then
        log_warning "No iPhone simulator found, using generic destination"
        DESTINATION="generic/platform=iOS Simulator"
    else
        DESTINATION="platform=iOS Simulator,id=${SIMULATOR_ID}"
    fi
    
    xcodebuild build \
        -project "${PROJECT_DIR}/${PROJECT_NAME}.xcodeproj" \
        -scheme "${SCHEME}" \
        -destination "${DESTINATION}" \
        -configuration "${CONFIG}" \
        -derivedDataPath "${DERIVED_DATA}" \
        CODE_SIGNING_ALLOWED=NO \
        | xcpretty 2>/dev/null || xcodebuild build \
            -project "${PROJECT_DIR}/${PROJECT_NAME}.xcodeproj" \
            -scheme "${SCHEME}" \
            -destination "${DESTINATION}" \
            -configuration "${CONFIG}" \
            -derivedDataPath "${DERIVED_DATA}" \
            CODE_SIGNING_ALLOWED=NO
    
    log_success "Simulator build complete"
    log_info "App location: ${DERIVED_DATA}/Build/Products/${CONFIG}-iphonesimulator/${PROJECT_NAME}.app"
}

# Build for device
build_device() {
    local CONFIG="${1:-Debug}"
    log_info "Building for iOS Device (${CONFIG})..."
    
    mkdir -p "${BUILD_DIR}"
    
    # Check for provisioning profile
    if [ -z "${APPLE_TEAM_ID}" ]; then
        log_warning "APPLE_TEAM_ID not set, using automatic signing"
    fi
    
    xcodebuild build \
        -project "${PROJECT_DIR}/${PROJECT_NAME}.xcodeproj" \
        -scheme "${SCHEME}" \
        -destination "generic/platform=iOS" \
        -configuration "${CONFIG}" \
        -derivedDataPath "${DERIVED_DATA}" \
        ${APPLE_TEAM_ID:+DEVELOPMENT_TEAM="${APPLE_TEAM_ID}"} \
        | xcpretty 2>/dev/null || xcodebuild build \
            -project "${PROJECT_DIR}/${PROJECT_NAME}.xcodeproj" \
            -scheme "${SCHEME}" \
            -destination "generic/platform=iOS" \
            -configuration "${CONFIG}" \
            -derivedDataPath "${DERIVED_DATA}" \
            ${APPLE_TEAM_ID:+DEVELOPMENT_TEAM="${APPLE_TEAM_ID}"}
    
    log_success "Device build complete"
}

# Build release
build_release() {
    log_info "Building Release configuration..."
    build_device "Release"
}

# Generate Xcode project if needed
generate_project() {
    if [ ! -d "${PROJECT_DIR}/${PROJECT_NAME}.xcodeproj" ]; then
        log_info "Generating Xcode project..."
        
        # Check if Package.swift exists
        if [ -f "${PROJECT_DIR}/Package.swift" ]; then
            cd "${PROJECT_DIR}"
            swift package generate-xcodeproj
            log_success "Xcode project generated"
        else
            log_error "No Package.swift found. Create Xcode project manually."
            exit 1
        fi
    fi
}

# Print usage
print_usage() {
    echo "Usage: $0 [command]"
    echo ""
    echo "Commands:"
    echo "  simulator   Build for iOS Simulator (default)"
    echo "  device      Build for physical iOS device"
    echo "  release     Build release configuration"
    echo "  clean       Clean build artifacts"
    echo "  help        Show this help message"
    echo ""
    echo "Environment Variables:"
    echo "  APPLE_TEAM_ID    Your Apple Developer Team ID"
    echo ""
    echo "Examples:"
    echo "  $0                    # Build for simulator"
    echo "  $0 device             # Build for device"
    echo "  APPLE_TEAM_ID=ABC123 $0 release"
}

# Main
main() {
    cd "${PROJECT_DIR}"
    check_xcode
    
    case "${1:-simulator}" in
        simulator)
            generate_project
            build_simulator "Debug"
            ;;
        device)
            generate_project
            build_device "Debug"
            ;;
        release)
            generate_project
            build_release
            ;;
        clean)
            clean_build
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
