#!/bin/bash
# =============================================================================
# Meshtastic Mobile iOS - Deploy to Personal Device
# =============================================================================
# Deploys the app directly to your iPhone/iPad via USB for development testing
#
# Usage:
#   ./deploy-device.sh              # Build and install to connected device
#   ./deploy-device.sh --list       # List connected devices
#   ./deploy-device.sh --wireless   # Deploy via Wi-Fi (after initial USB setup)
# =============================================================================

set -e

# Configuration
PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PROJECT_NAME="MeshtasticMobile"
SCHEME="MeshtasticMobile"
BUILD_DIR="${PROJECT_DIR}/build"
DERIVED_DATA="${BUILD_DIR}/DerivedData"

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

# Check for Xcode
check_xcode() {
    if ! command -v xcodebuild &> /dev/null; then
        log_error "Xcode command line tools not found"
        echo "Install with: xcode-select --install"
        exit 1
    fi
    log_info "Xcode: $(xcodebuild -version | head -1)"
}

# List connected devices
list_devices() {
    log_info "Connected iOS devices:"
    echo ""
    
    # Using xcrun xctrace
    xcrun xctrace list devices 2>/dev/null | grep -E "iPhone|iPad" || true
    
    echo ""
    log_info "Device IDs (for deployment):"
    system_profiler SPUSBDataType 2>/dev/null | grep -A 5 "iPhone\|iPad" | grep "Serial Number" | awk '{print $NF}' || true
    
    # Also try ios-deploy if available
    if command -v ios-deploy &> /dev/null; then
        echo ""
        log_info "Via ios-deploy:"
        ios-deploy -c 2>/dev/null || true
    fi
}

# Get first connected device
get_device_id() {
    # Try to get device ID from system_profiler
    DEVICE_ID=$(system_profiler SPUSBDataType 2>/dev/null | grep -A 11 "iPhone\|iPad" | grep "Serial Number" | head -1 | awk '{print $NF}')
    
    if [ -z "$DEVICE_ID" ]; then
        # Try xcrun devicectl
        DEVICE_ID=$(xcrun devicectl list devices 2>/dev/null | grep -E "iPhone|iPad" | head -1 | awk '{print $1}')
    fi
    
    if [ -z "$DEVICE_ID" ]; then
        log_error "No iOS device connected"
        echo ""
        echo "Please:"
        echo "  1. Connect your iPhone/iPad via USB"
        echo "  2. Unlock your device"
        echo "  3. Trust this computer when prompted"
        echo ""
        echo "Then run: $0 --list"
        exit 1
    fi
    
    echo "$DEVICE_ID"
}

# Check/setup code signing
setup_signing() {
    log_step "Checking code signing..."
    
    # Check for Apple Team ID
    if [ -z "${APPLE_TEAM_ID}" ]; then
        log_warning "APPLE_TEAM_ID not set"
        echo ""
        echo "To find your Team ID:"
        echo "  1. Open Xcode → Preferences → Accounts"
        echo "  2. Select your Apple ID"
        echo "  3. Click 'Manage Certificates'"
        echo "  4. Your Team ID is shown (10 characters)"
        echo ""
        echo "Or find it at: https://developer.apple.com/account"
        echo ""
        echo "Set it with:"
        echo "  export APPLE_TEAM_ID=\"XXXXXXXXXX\""
        echo ""
        
        # Try to use automatic signing
        log_info "Attempting automatic signing..."
        SIGNING_STYLE="automatic"
    else
        log_info "Team ID: ${APPLE_TEAM_ID}"
        SIGNING_STYLE="automatic"
    fi
}

# Build for device
build_for_device() {
    log_step "Building for device..."
    
    mkdir -p "${BUILD_DIR}"
    
    # Build command
    BUILD_CMD="xcodebuild build \
        -project \"${PROJECT_DIR}/${PROJECT_NAME}.xcodeproj\" \
        -scheme \"${SCHEME}\" \
        -destination \"generic/platform=iOS\" \
        -configuration Debug \
        -derivedDataPath \"${DERIVED_DATA}\" \
        CODE_SIGN_STYLE=Automatic \
        CODE_SIGNING_REQUIRED=YES \
        CODE_SIGNING_ALLOWED=YES"
    
    if [ -n "${APPLE_TEAM_ID}" ]; then
        BUILD_CMD="${BUILD_CMD} DEVELOPMENT_TEAM=\"${APPLE_TEAM_ID}\""
    fi
    
    eval "${BUILD_CMD}" 2>&1 | xcpretty 2>/dev/null || eval "${BUILD_CMD}"
    
    log_success "Build complete"
}

# Install to device using xcodebuild
install_to_device() {
    local DEVICE_ID="$1"
    
    log_step "Installing to device ${DEVICE_ID}..."
    
    # Find the .app bundle
    APP_PATH=$(find "${DERIVED_DATA}/Build/Products/Debug-iphoneos" -name "*.app" -type d | head -1)
    
    if [ -z "$APP_PATH" ]; then
        log_error "Could not find built .app bundle"
        exit 1
    fi
    
    log_info "App: $(basename "$APP_PATH")"
    
    # Method 1: Try devicectl (Xcode 15+)
    if command -v xcrun &> /dev/null; then
        log_info "Installing via devicectl..."
        xcrun devicectl device install app --device "${DEVICE_ID}" "${APP_PATH}" 2>/dev/null && {
            log_success "Installed successfully!"
            return 0
        } || true
    fi
    
    # Method 2: Try ios-deploy
    if command -v ios-deploy &> /dev/null; then
        log_info "Installing via ios-deploy..."
        ios-deploy --bundle "${APP_PATH}" --id "${DEVICE_ID}" && {
            log_success "Installed successfully!"
            return 0
        } || true
    fi
    
    # Method 3: Direct xcodebuild install
    log_info "Installing via xcodebuild..."
    xcodebuild install \
        -project "${PROJECT_DIR}/${PROJECT_NAME}.xcodeproj" \
        -scheme "${SCHEME}" \
        -destination "id=${DEVICE_ID}" \
        -derivedDataPath "${DERIVED_DATA}" \
        DSTROOT="${BUILD_DIR}/install" \
        2>&1 | xcpretty 2>/dev/null || \
    xcodebuild install \
        -project "${PROJECT_DIR}/${PROJECT_NAME}.xcodeproj" \
        -scheme "${SCHEME}" \
        -destination "id=${DEVICE_ID}" \
        -derivedDataPath "${DERIVED_DATA}" \
        DSTROOT="${BUILD_DIR}/install"
    
    log_success "Installation complete"
}

# Build and run on device
build_and_run() {
    local DEVICE_ID="$1"
    
    log_step "Building and running on device..."
    
    BUILD_CMD="xcodebuild build \
        -project \"${PROJECT_DIR}/${PROJECT_NAME}.xcodeproj\" \
        -scheme \"${SCHEME}\" \
        -destination \"id=${DEVICE_ID}\" \
        -configuration Debug \
        -derivedDataPath \"${DERIVED_DATA}\" \
        CODE_SIGN_STYLE=Automatic"
    
    if [ -n "${APPLE_TEAM_ID}" ]; then
        BUILD_CMD="${BUILD_CMD} DEVELOPMENT_TEAM=\"${APPLE_TEAM_ID}\""
    fi
    
    eval "${BUILD_CMD}" 2>&1 | xcpretty 2>/dev/null || eval "${BUILD_CMD}"
    
    log_success "Build and deploy complete!"
    echo ""
    echo "The app should now be on your device."
    echo "Look for 'MeshtasticMobile' on your home screen."
}

# Setup wireless debugging
setup_wireless() {
    log_step "Setting up wireless debugging..."
    
    DEVICE_ID=$(get_device_id)
    
    log_info "Enabling wireless debugging for device ${DEVICE_ID}..."
    
    # Pair device for wireless
    xcrun devicectl device pair --device "${DEVICE_ID}" 2>/dev/null || {
        log_warning "Could not enable wireless debugging automatically"
        echo ""
        echo "To enable manually:"
        echo "  1. Connect device via USB"
        echo "  2. Open Xcode → Window → Devices and Simulators"
        echo "  3. Select your device"
        echo "  4. Check 'Connect via network'"
        echo ""
    }
    
    log_success "Wireless debugging configured"
}

# Print setup instructions
print_setup() {
    echo ""
    echo "═══════════════════════════════════════════════════════════════════"
    echo "  DEVELOPER MODE SETUP FOR PERSONAL iPHONE"
    echo "═══════════════════════════════════════════════════════════════════"
    echo ""
    echo "STEP 1: Enable Developer Mode on iPhone (iOS 16+)"
    echo "─────────────────────────────────────────────────"
    echo "  1. Go to Settings → Privacy & Security"
    echo "  2. Scroll down to 'Developer Mode'"
    echo "  3. Enable Developer Mode"
    echo "  4. Restart your iPhone when prompted"
    echo "  5. After restart, confirm enabling Developer Mode"
    echo ""
    echo "STEP 2: Trust Your Mac"
    echo "──────────────────────"
    echo "  1. Connect iPhone to Mac via USB cable"
    echo "  2. Unlock your iPhone"
    echo "  3. Tap 'Trust' when prompted on iPhone"
    echo "  4. Enter your iPhone passcode"
    echo ""
    echo "STEP 3: Apple Developer Account (Free or Paid)"
    echo "───────────────────────────────────────────────"
    echo "  FREE Account Limitations:"
    echo "    - App expires after 7 days (must reinstall)"
    echo "    - Maximum 3 apps on device"
    echo "    - No push notifications"
    echo "    - No App Store distribution"
    echo ""
    echo "  PAID Account (\$99/year):"
    echo "    - App valid for 1 year"
    echo "    - Unlimited apps"
    echo "    - Full capabilities"
    echo "    - TestFlight & App Store distribution"
    echo ""
    echo "STEP 4: Configure Xcode Signing"
    echo "────────────────────────────────"
    echo "  1. Open Xcode"
    echo "  2. Xcode → Preferences → Accounts"
    echo "  3. Click '+' → Add Apple ID"
    echo "  4. Sign in with your Apple ID"
    echo ""
    echo "STEP 5: Deploy to Device"
    echo "────────────────────────"
    echo "  export APPLE_TEAM_ID=\"YOUR_TEAM_ID\"  # Optional but recommended"
    echo "  ./Scripts/deploy-device.sh"
    echo ""
    echo "═══════════════════════════════════════════════════════════════════"
}

# Print usage
print_usage() {
    echo "Usage: $0 [option]"
    echo ""
    echo "Options:"
    echo "  (none)        Build and install to connected device"
    echo "  --list        List connected iOS devices"
    echo "  --wireless    Setup wireless debugging"
    echo "  --setup       Show Developer Mode setup instructions"
    echo "  --help        Show this help"
    echo ""
    echo "Environment Variables:"
    echo "  APPLE_TEAM_ID   Your Apple Developer Team ID (optional)"
    echo ""
    echo "Examples:"
    echo "  $0                          # Deploy to connected iPhone"
    echo "  $0 --list                   # List devices"
    echo "  APPLE_TEAM_ID=ABC123 $0     # Deploy with specific team"
}

# Main
main() {
    cd "${PROJECT_DIR}"
    
    case "${1:-deploy}" in
        --list|-l)
            check_xcode
            list_devices
            ;;
        --wireless|-w)
            check_xcode
            setup_wireless
            ;;
        --setup|-s)
            print_setup
            ;;
        --help|-h)
            print_usage
            ;;
        deploy|"")
            check_xcode
            setup_signing
            
            DEVICE_ID=$(get_device_id)
            log_info "Found device: ${DEVICE_ID}"
            
            build_and_run "$DEVICE_ID"
            
            echo ""
            log_success "App deployed to your iPhone!"
            echo ""
            echo "If the app doesn't appear:"
            echo "  1. Check Settings → General → VPN & Device Management"
            echo "  2. Trust your developer certificate"
            echo "  3. Try opening the app again"
            ;;
        *)
            log_error "Unknown option: $1"
            print_usage
            exit 1
            ;;
    esac
}

main "$@"
