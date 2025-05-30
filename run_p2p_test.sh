#!/bin/bash

# SyncBit P2P Replication Test Runner
# This script demonstrates how to test P2P functionality with replication factor 2

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

echo -e "${GREEN}SyncBit P2P Replication Test${NC}"
echo "============================="
echo
echo "This test will:"
echo "1. Start the controller and 2 agents"
echo "2. Submit jobs with replication factor 2"
echo "3. Monitor P2P file distribution"
echo "4. Verify both agents receive the files"
echo "5. Show comprehensive stats and metrics"
echo

# Build the binaries first
echo -e "${BLUE}Building SyncBit binaries...${NC}"
if ! go build -o bin/syncbitd ./cmd/syncbitd; then
    echo -e "${RED}Failed to build syncbitd${NC}"
    exit 1
fi

if ! go build -o bin/syncbit ./cmd/syncbit; then
    echo -e "${RED}Failed to build syncbit${NC}"
    exit 1
fi

echo -e "${GREEN}Binaries built successfully${NC}"
echo

# Start the E2E test in background
echo -e "${BLUE}Starting comprehensive P2P test in background...${NC}"
./test_e2e.sh > e2e_test.log 2>&1 &
TEST_PID=$!

echo "E2E test started with PID: $TEST_PID"
echo "Test log: e2e_test.log"
echo

# Wait for services to start
echo -e "${BLUE}Waiting for services to start up...${NC}"
sleep 10

# Function to check if service is running
check_service() {
    local url=$1
    local name=$2

    if curl -s -f "$url/health" >/dev/null 2>&1; then
        echo -e "${GREEN}✓ $name is running${NC}"
        return 0
    else
        echo -e "${RED}✗ $name is not responding${NC}"
        return 1
    fi
}

# Check if all services are running
echo -e "${YELLOW}Checking service status...${NC}"
check_service "http://localhost:8080" "Controller"
check_service "http://localhost:8081" "Agent 1"
check_service "http://localhost:8082" "Agent 2"

echo

# Monitor the test progress using our monitor script
echo -e "${BLUE}Monitoring test progress...${NC}"
echo "You can monitor logs in real-time using:"
echo "  ./monitor_logs.sh all"
echo "  ./monitor_logs.sh controller"
echo "  ./monitor_logs.sh agent1"
echo "  ./monitor_logs.sh agent2"
echo

echo -e "${BLUE}Showing real-time progress for 30 seconds...${NC}"

# Show progress for 30 seconds, then exit
for i in {1..6}; do
    echo -e "${YELLOW}=== Progress Update $i/6 ===${NC}"

    # Show controller stats
    echo -e "${BLUE}Controller Stats:${NC}"
    curl -s "http://localhost:8080/stats" | jq . 2>/dev/null || echo "Controller not ready"

    echo -e "${BLUE}Agent States:${NC}"
    echo "Agent 1:"
    curl -s "http://localhost:8081/state" | jq . 2>/dev/null || echo "Agent 1 not ready"
    echo "Agent 2:"
    curl -s "http://localhost:8082/state" | jq . 2>/dev/null || echo "Agent 2 not ready"

    echo
    sleep 5
done

echo -e "${GREEN}P2P Test Monitoring Complete${NC}"
echo
echo -e "${YELLOW}The test is still running in the background.${NC}"
echo "Monitor options:"
echo "  tail -f e2e_test.log                    # Follow full test log"
echo "  ./monitor_logs.sh all                   # Monitor all service logs"
echo "  ./monitor_logs.sh recent 50             # Show recent 50 lines from all logs"
echo "  curl http://localhost:8080/stats        # Get controller stats"
echo "  curl http://localhost:8081/cache/stats  # Get agent 1 cache stats"
echo "  curl http://localhost:8082/cache/stats  # Get agent 2 cache stats"
echo
echo "To stop the test:"
echo "  kill $TEST_PID"
echo
echo -e "${BLUE}Test PID: $TEST_PID${NC}"
echo -e "${BLUE}Test log: e2e_test.log${NC}"
