#!/bin/bash
#
# Meshtastic Dashboard Management Script
# Usage: ./manage.sh [start|stop|restart|status|logs|build|test|clean]
#

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
LOG_DIR="$SCRIPT_DIR/logs"
PID_DIR="$SCRIPT_DIR/.pids"

API_PORT="${API_PORT:-5000}"
DASHBOARD_PORT="${DASHBOARD_PORT:-3000}"
SNOWFLAKE_CONNECTION="${SNOWFLAKE_CONNECTION_NAME:-tspann1}"

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
NC='\033[0m'

log_info() { echo -e "${BLUE}[INFO]${NC} $1"; }
log_success() { echo -e "${GREEN}[OK]${NC} $1"; }
log_warn() { echo -e "${YELLOW}[WARN]${NC} $1"; }
log_error() { echo -e "${RED}[ERROR]${NC} $1"; }

init_dirs() {
    mkdir -p "$LOG_DIR" "$PID_DIR"
}

is_running() {
    local pid_file="$PID_DIR/$1.pid"
    if [[ -f "$pid_file" ]]; then
        local pid=$(cat "$pid_file")
        if ps -p "$pid" > /dev/null 2>&1; then
            return 0
        fi
    fi
    return 1
}

get_pid() {
    local pid_file="$PID_DIR/$1.pid"
    if [[ -f "$pid_file" ]]; then
        cat "$pid_file"
    fi
}

start_api() {
    if is_running "api"; then
        log_warn "API server already running (PID: $(get_pid api))"
        return 0
    fi
    
    log_info "Starting API server on port $API_PORT..."
    cd "$SCRIPT_DIR"
    
    SNOWFLAKE_CONNECTION_NAME="$SNOWFLAKE_CONNECTION" \
    nohup python3 api_server.py \
        >> "$LOG_DIR/api_server.log" 2>&1 &
    
    echo $! > "$PID_DIR/api.pid"
    sleep 2
    
    if is_running "api"; then
        log_success "API server started (PID: $(get_pid api))"
    else
        log_error "Failed to start API server"
        return 1
    fi
}

start_dashboard() {
    if is_running "dashboard"; then
        log_warn "Dashboard already running (PID: $(get_pid dashboard))"
        return 0
    fi
    
    log_info "Starting dashboard on port $DASHBOARD_PORT..."
    cd "$SCRIPT_DIR"
    
    if [[ ! -d "build" ]]; then
        log_warn "Build directory not found, building..."
        npm run build >> "$LOG_DIR/build.log" 2>&1
    fi
    
    nohup npx serve -s build -l "$DASHBOARD_PORT" \
        >> "$LOG_DIR/dashboard.log" 2>&1 &
    
    echo $! > "$PID_DIR/dashboard.pid"
    sleep 3
    
    if is_running "dashboard"; then
        log_success "Dashboard started (PID: $(get_pid dashboard))"
    else
        log_error "Failed to start dashboard"
        return 1
    fi
}

start_mqtt() {
    if is_running "mqtt"; then
        log_warn "MQTT consumer already running (PID: $(get_pid mqtt))"
        return 0
    fi
    
    if [[ ! -f "$SCRIPT_DIR/mqtt_consumer.py" ]]; then
        log_warn "MQTT consumer not found, skipping..."
        return 0
    fi
    
    log_info "Starting MQTT consumer..."
    cd "$SCRIPT_DIR"
    
    SNOWFLAKE_CONNECTION_NAME="$SNOWFLAKE_CONNECTION" \
    nohup python3 mqtt_consumer.py \
        >> "$LOG_DIR/mqtt_consumer.log" 2>&1 &
    
    echo $! > "$PID_DIR/mqtt.pid"
    sleep 2
    
    if is_running "mqtt"; then
        log_success "MQTT consumer started (PID: $(get_pid mqtt))"
    else
        log_warn "MQTT consumer may have failed to start"
    fi
}

stop_service() {
    local service=$1
    local pid_file="$PID_DIR/$service.pid"
    
    if [[ -f "$pid_file" ]]; then
        local pid=$(cat "$pid_file")
        if ps -p "$pid" > /dev/null 2>&1; then
            log_info "Stopping $service (PID: $pid)..."
            kill "$pid" 2>/dev/null || true
            sleep 2
            if ps -p "$pid" > /dev/null 2>&1; then
                kill -9 "$pid" 2>/dev/null || true
            fi
            log_success "$service stopped"
        fi
        rm -f "$pid_file"
    fi
}

do_start() {
    init_dirs
    echo -e "${CYAN}🎮 Starting Meshtastic Pac-Man Dashboard${NC}"
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    
    start_api
    start_dashboard
    start_mqtt
    
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    echo -e "${GREEN}Dashboard:${NC} http://localhost:$DASHBOARD_PORT"
    echo -e "${GREEN}API:${NC}       http://localhost:$API_PORT"
    echo ""
}

do_stop() {
    echo -e "${CYAN}🛑 Stopping Meshtastic Dashboard${NC}"
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    
    stop_service "mqtt"
    stop_service "dashboard"
    stop_service "api"
    
    pkill -f "serve -s build" 2>/dev/null || true
    pkill -f "api_server.py" 2>/dev/null || true
    pkill -f "mqtt_consumer.py" 2>/dev/null || true
    
    log_success "All services stopped"
}

do_status() {
    echo -e "${CYAN}📊 Meshtastic Dashboard Status${NC}"
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    
    if is_running "api"; then
        echo -e "API Server:     ${GREEN}● Running${NC} (PID: $(get_pid api)) - Port $API_PORT"
    else
        echo -e "API Server:     ${RED}○ Stopped${NC}"
    fi
    
    if is_running "dashboard"; then
        echo -e "Dashboard:      ${GREEN}● Running${NC} (PID: $(get_pid dashboard)) - Port $DASHBOARD_PORT"
    else
        echo -e "Dashboard:      ${RED}○ Stopped${NC}"
    fi
    
    if is_running "mqtt"; then
        echo -e "MQTT Consumer:  ${GREEN}● Running${NC} (PID: $(get_pid mqtt))"
    else
        echo -e "MQTT Consumer:  ${YELLOW}○ Not Running${NC}"
    fi
    
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    
    if is_running "api"; then
        echo ""
        log_info "Health check..."
        curl -s "http://localhost:$API_PORT/api/health" 2>/dev/null | python3 -m json.tool 2>/dev/null || echo "API not responding"
    fi
}

do_logs() {
    local service="${1:-all}"
    
    echo -e "${CYAN}📜 Viewing logs${NC}"
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    
    case "$service" in
        api)
            tail -f "$LOG_DIR/api_server.log"
            ;;
        dashboard)
            tail -f "$LOG_DIR/dashboard.log"
            ;;
        mqtt)
            tail -f "$LOG_DIR/mqtt_consumer.log"
            ;;
        all|*)
            tail -f "$LOG_DIR"/*.log
            ;;
    esac
}

do_build() {
    echo -e "${CYAN}🔨 Building dashboard${NC}"
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    
    cd "$SCRIPT_DIR"
    npm install
    npm run build
    
    log_success "Build complete"
}

do_test() {
    echo -e "${CYAN}🧪 Running tests${NC}"
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    
    cd "$SCRIPT_DIR"
    
    if [[ -d "tests" ]]; then
        python3 -m pytest tests/ -v --tb=short
    else
        log_warn "No tests directory found"
    fi
    
    npm test -- --watchAll=false --passWithNoTests 2>/dev/null || true
}

do_clean() {
    echo -e "${CYAN}🧹 Cleaning up${NC}"
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    
    do_stop
    
    rm -rf "$PID_DIR"
    rm -rf "$LOG_DIR"/*.log
    rm -rf "$SCRIPT_DIR/build"
    rm -rf "$SCRIPT_DIR/node_modules/.cache"
    
    log_success "Cleanup complete"
}

do_health() {
    echo -e "${CYAN}🏥 Health Check${NC}"
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    
    echo "API Health:"
    curl -s "http://localhost:$API_PORT/api/health" | python3 -m json.tool 2>/dev/null || echo "  Not available"
    
    echo ""
    echo "Dashboard:"
    curl -s -o /dev/null -w "  HTTP Status: %{http_code}\n" "http://localhost:$DASHBOARD_PORT" 2>/dev/null || echo "  Not available"
    
    echo ""
    echo "Snowflake Connection:"
    echo "  Name: $SNOWFLAKE_CONNECTION"
}

show_help() {
    echo -e "${CYAN}🎮 Meshtastic Pac-Man Dashboard Manager${NC}"
    echo ""
    echo "Usage: $0 <command> [options]"
    echo ""
    echo "Commands:"
    echo "  start       Start all services (API, Dashboard, MQTT)"
    echo "  stop        Stop all services"
    echo "  restart     Restart all services"
    echo "  status      Show service status"
    echo "  logs [svc]  View logs (api|dashboard|mqtt|all)"
    echo "  build       Build React dashboard"
    echo "  test        Run tests"
    echo "  health      Health check"
    echo "  clean       Stop services and clean files"
    echo ""
    echo "Environment Variables:"
    echo "  API_PORT               API server port (default: 5000)"
    echo "  DASHBOARD_PORT         Dashboard port (default: 3000)"
    echo "  SNOWFLAKE_CONNECTION_NAME  Snowflake connection"
    echo ""
    echo "Examples:"
    echo "  $0 start"
    echo "  $0 logs api"
    echo "  $0 status"
}

case "${1:-help}" in
    start)
        do_start
        ;;
    stop)
        do_stop
        ;;
    restart)
        do_stop
        sleep 2
        do_start
        ;;
    status)
        do_status
        ;;
    logs)
        do_logs "$2"
        ;;
    build)
        do_build
        ;;
    test)
        do_test
        ;;
    health)
        do_health
        ;;
    clean)
        do_clean
        ;;
    help|--help|-h|*)
        show_help
        ;;
esac
