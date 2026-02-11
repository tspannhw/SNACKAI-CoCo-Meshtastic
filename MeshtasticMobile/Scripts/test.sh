#!/bin/bash
# =============================================================================
# Meshtastic Mobile iOS - Test Script
# =============================================================================
# Usage:
#   ./test.sh              # Run all tests
#   ./test.sh unit         # Run unit tests only
#   ./test.sh ui           # Run UI tests only
#   ./test.sh coverage     # Run tests with coverage report
# =============================================================================

set -e

# Configuration
PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PROJECT_NAME="MeshtasticMobile"
SCHEME="MeshtasticMobile"
BUILD_DIR="${PROJECT_DIR}/build"
DERIVED_DATA="${BUILD_DIR}/DerivedData"
COVERAGE_DIR="${BUILD_DIR}/coverage"

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

# Get simulator
get_simulator() {
    # Prefer iPhone 15 Pro, fallback to any iPhone
    SIMULATOR=$(xcrun simctl list devices available | grep "iPhone 15 Pro" | head -1 | grep -oE '[A-F0-9-]{36}') || true
    
    if [ -z "$SIMULATOR" ]; then
        SIMULATOR=$(xcrun simctl list devices available | grep "iPhone" | head -1 | grep -oE '[A-F0-9-]{36}')
    fi
    
    if [ -z "$SIMULATOR" ]; then
        log_error "No iPhone simulator available"
        exit 1
    fi
    
    echo "$SIMULATOR"
}

# Run unit tests
run_unit_tests() {
    log_info "Running unit tests..."
    
    SIMULATOR_ID=$(get_simulator)
    log_info "Using simulator: $SIMULATOR_ID"
    
    mkdir -p "${BUILD_DIR}"
    
    xcodebuild test \
        -project "${PROJECT_DIR}/${PROJECT_NAME}.xcodeproj" \
        -scheme "${SCHEME}" \
        -destination "platform=iOS Simulator,id=${SIMULATOR_ID}" \
        -derivedDataPath "${DERIVED_DATA}" \
        -only-testing:"${PROJECT_NAME}Tests" \
        CODE_SIGNING_ALLOWED=NO \
        2>&1 | xcpretty --report junit --output "${BUILD_DIR}/unit-tests.xml" || \
    xcodebuild test \
        -project "${PROJECT_DIR}/${PROJECT_NAME}.xcodeproj" \
        -scheme "${SCHEME}" \
        -destination "platform=iOS Simulator,id=${SIMULATOR_ID}" \
        -derivedDataPath "${DERIVED_DATA}" \
        -only-testing:"${PROJECT_NAME}Tests" \
        CODE_SIGNING_ALLOWED=NO
    
    log_success "Unit tests complete"
}

# Run UI tests
run_ui_tests() {
    log_info "Running UI tests..."
    
    SIMULATOR_ID=$(get_simulator)
    
    # Boot simulator if needed
    xcrun simctl boot "$SIMULATOR_ID" 2>/dev/null || true
    
    mkdir -p "${BUILD_DIR}"
    
    xcodebuild test \
        -project "${PROJECT_DIR}/${PROJECT_NAME}.xcodeproj" \
        -scheme "${SCHEME}" \
        -destination "platform=iOS Simulator,id=${SIMULATOR_ID}" \
        -derivedDataPath "${DERIVED_DATA}" \
        -only-testing:"${PROJECT_NAME}UITests" \
        CODE_SIGNING_ALLOWED=NO \
        2>&1 | xcpretty --report junit --output "${BUILD_DIR}/ui-tests.xml" || \
    xcodebuild test \
        -project "${PROJECT_DIR}/${PROJECT_NAME}.xcodeproj" \
        -scheme "${SCHEME}" \
        -destination "platform=iOS Simulator,id=${SIMULATOR_ID}" \
        -derivedDataPath "${DERIVED_DATA}" \
        -only-testing:"${PROJECT_NAME}UITests" \
        CODE_SIGNING_ALLOWED=NO
    
    log_success "UI tests complete"
}

# Run all tests
run_all_tests() {
    log_info "Running all tests..."
    
    SIMULATOR_ID=$(get_simulator)
    
    mkdir -p "${BUILD_DIR}"
    
    xcodebuild test \
        -project "${PROJECT_DIR}/${PROJECT_NAME}.xcodeproj" \
        -scheme "${SCHEME}" \
        -destination "platform=iOS Simulator,id=${SIMULATOR_ID}" \
        -derivedDataPath "${DERIVED_DATA}" \
        CODE_SIGNING_ALLOWED=NO \
        2>&1 | xcpretty --report junit --output "${BUILD_DIR}/all-tests.xml" || \
    xcodebuild test \
        -project "${PROJECT_DIR}/${PROJECT_NAME}.xcodeproj" \
        -scheme "${SCHEME}" \
        -destination "platform=iOS Simulator,id=${SIMULATOR_ID}" \
        -derivedDataPath "${DERIVED_DATA}" \
        CODE_SIGNING_ALLOWED=NO
    
    log_success "All tests complete"
}

# Run tests with coverage
run_coverage() {
    log_info "Running tests with coverage..."
    
    SIMULATOR_ID=$(get_simulator)
    
    mkdir -p "${BUILD_DIR}" "${COVERAGE_DIR}"
    
    xcodebuild test \
        -project "${PROJECT_DIR}/${PROJECT_NAME}.xcodeproj" \
        -scheme "${SCHEME}" \
        -destination "platform=iOS Simulator,id=${SIMULATOR_ID}" \
        -derivedDataPath "${DERIVED_DATA}" \
        -enableCodeCoverage YES \
        CODE_SIGNING_ALLOWED=NO \
        2>&1 | xcpretty || \
    xcodebuild test \
        -project "${PROJECT_DIR}/${PROJECT_NAME}.xcodeproj" \
        -scheme "${SCHEME}" \
        -destination "platform=iOS Simulator,id=${SIMULATOR_ID}" \
        -derivedDataPath "${DERIVED_DATA}" \
        -enableCodeCoverage YES \
        CODE_SIGNING_ALLOWED=NO
    
    # Generate coverage report
    log_info "Generating coverage report..."
    
    PROFDATA=$(find "${DERIVED_DATA}" -name "*.profdata" | head -1)
    BINARY=$(find "${DERIVED_DATA}" -name "${PROJECT_NAME}" -type f | grep -v dSYM | head -1)
    
    if [ -n "$PROFDATA" ] && [ -n "$BINARY" ]; then
        xcrun llvm-cov report \
            "$BINARY" \
            -instr-profile="$PROFDATA" \
            > "${COVERAGE_DIR}/coverage-report.txt"
        
        xcrun llvm-cov export \
            "$BINARY" \
            -instr-profile="$PROFDATA" \
            -format=lcov \
            > "${COVERAGE_DIR}/coverage.lcov"
        
        log_success "Coverage report: ${COVERAGE_DIR}/coverage-report.txt"
        
        # Print summary
        echo ""
        echo "Coverage Summary:"
        echo "================="
        cat "${COVERAGE_DIR}/coverage-report.txt" | tail -5
    else
        log_warning "Could not generate coverage report"
    fi
    
    log_success "Coverage tests complete"
}

# Print usage
print_usage() {
    echo "Usage: $0 [command]"
    echo ""
    echo "Commands:"
    echo "  all        Run all tests (default)"
    echo "  unit       Run unit tests only"
    echo "  ui         Run UI tests only"
    echo "  coverage   Run tests with coverage report"
    echo "  help       Show this help message"
    echo ""
    echo "Output:"
    echo "  JUnit reports: ${BUILD_DIR}/*.xml"
    echo "  Coverage:      ${COVERAGE_DIR}/"
}

# Main
main() {
    cd "${PROJECT_DIR}"
    
    case "${1:-all}" in
        all)
            run_all_tests
            ;;
        unit)
            run_unit_tests
            ;;
        ui)
            run_ui_tests
            ;;
        coverage)
            run_coverage
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
