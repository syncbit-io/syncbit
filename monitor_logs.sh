#!/bin/bash

# SyncBit Log Monitor Script
# This script helps monitor the logs from all services

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Log files
CONTROLLER_LOG="controller.log"
AGENT1_LOG="agent1.log"
AGENT2_LOG="agent2.log"

echo -e "${GREEN}SyncBit Log Monitor${NC}"
echo "=================="
echo
echo "Available log files:"
echo "1. Controller log: $CONTROLLER_LOG"
echo "2. Agent 1 log: $AGENT1_LOG"
echo "3. Agent 2 log: $AGENT2_LOG"
echo "4. All logs (combined)"
echo

# Function to monitor single log
monitor_single_log() {
    local logfile=$1
    local name=$2

    if [[ ! -f "$logfile" ]]; then
        echo -e "${RED}Log file $logfile not found!${NC}"
        return 1
    fi

    echo -e "${BLUE}Monitoring $name ($logfile)...${NC}"
    echo -e "${YELLOW}Press Ctrl+C to stop${NC}"
    echo
    tail -f "$logfile"
}

# Function to monitor all logs
monitor_all_logs() {
    echo -e "${BLUE}Monitoring all logs...${NC}"
    echo -e "${YELLOW}Press Ctrl+C to stop${NC}"
    echo

    # Use multitail if available, otherwise use tail
    if command -v multitail &> /dev/null; then
        multitail -l "tail -f $CONTROLLER_LOG" -l "tail -f $AGENT1_LOG" -l "tail -f $AGENT2_LOG"
    else
        # Fallback to combined tail with labels
        tail -f "$CONTROLLER_LOG" "$AGENT1_LOG" "$AGENT2_LOG"
    fi
}

# Function to show log status
show_log_status() {
    echo -e "${BLUE}Log file status:${NC}"

    for logfile in "$CONTROLLER_LOG" "$AGENT1_LOG" "$AGENT2_LOG"; do
        if [[ -f "$logfile" ]]; then
            size=$(stat -f%z "$logfile" 2>/dev/null || stat -c%s "$logfile" 2>/dev/null || echo "unknown")
            lines=$(wc -l < "$logfile" 2>/dev/null || echo "unknown")
            echo -e "${GREEN}✓${NC} $logfile (${size} bytes, ${lines} lines)"
        else
            echo -e "${RED}✗${NC} $logfile (not found)"
        fi
    done
    echo
}

# Function to show recent logs
show_recent_logs() {
    local lines=${1:-20}

    echo -e "${YELLOW}=== Recent Controller Logs (last $lines lines) ===${NC}"
    if [[ -f "$CONTROLLER_LOG" ]]; then
        tail -n "$lines" "$CONTROLLER_LOG"
    else
        echo "No controller log found"
    fi
    echo

    echo -e "${YELLOW}=== Recent Agent 1 Logs (last $lines lines) ===${NC}"
    if [[ -f "$AGENT1_LOG" ]]; then
        tail -n "$lines" "$AGENT1_LOG"
    else
        echo "No agent 1 log found"
    fi
    echo

    echo -e "${YELLOW}=== Recent Agent 2 Logs (last $lines lines) ===${NC}"
    if [[ -f "$AGENT2_LOG" ]]; then
        tail -n "$lines" "$AGENT2_LOG"
    else
        echo "No agent 2 log found"
    fi
    echo
}

# Function to search logs
search_logs() {
    local pattern=$1
    if [[ -z "$pattern" ]]; then
        echo -e "${RED}Please provide a search pattern${NC}"
        return 1
    fi

    echo -e "${BLUE}Searching for pattern: '$pattern'${NC}"
    echo

    for logfile in "$CONTROLLER_LOG" "$AGENT1_LOG" "$AGENT2_LOG"; do
        if [[ -f "$logfile" ]]; then
            echo -e "${YELLOW}=== Results from $logfile ===${NC}"
            grep -n --color=always "$pattern" "$logfile" || echo "No matches found"
            echo
        fi
    done
}

# Main menu
if [[ $# -eq 0 ]]; then
    show_log_status

    echo "Usage: $0 [command] [options]"
    echo
    echo "Commands:"
    echo "  controller     Monitor controller log"
    echo "  agent1         Monitor agent 1 log"
    echo "  agent2         Monitor agent 2 log"
    echo "  all           Monitor all logs"
    echo "  status        Show log file status"
    echo "  recent [N]    Show recent N lines from all logs (default: 20)"
    echo "  search PATTERN Search for pattern in all logs"
    echo
    echo "Examples:"
    echo "  $0 controller     # Monitor controller log"
    echo "  $0 all           # Monitor all logs"
    echo "  $0 recent 50     # Show last 50 lines from all logs"
    echo "  $0 search ERROR  # Search for ERROR in all logs"
    exit 0
fi

case "$1" in
    "controller")
        monitor_single_log "$CONTROLLER_LOG" "Controller"
        ;;
    "agent1")
        monitor_single_log "$AGENT1_LOG" "Agent 1"
        ;;
    "agent2")
        monitor_single_log "$AGENT2_LOG" "Agent 2"
        ;;
    "all")
        monitor_all_logs
        ;;
    "status")
        show_log_status
        ;;
    "recent")
        lines=${2:-20}
        show_recent_logs "$lines"
        ;;
    "search")
        if [[ -z "$2" ]]; then
            echo -e "${RED}Please provide a search pattern${NC}"
            exit 1
        fi
        search_logs "$2"
        ;;
    *)
        echo -e "${RED}Unknown command: $1${NC}"
        echo "Run $0 without arguments to see usage"
        exit 1
        ;;
esac
