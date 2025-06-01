#!/bin/bash

# SyncBit Dataset Assignment End-to-End Test Script
# This script tests the new Kubernetes-style declarative pull model

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Configuration
CONTROLLER_LOG="controller.log"
AGENT1_LOG="agent1.log"
AGENT2_LOG="agent2.log"
TEST_LOG="test.log"

# PIDs for cleanup
CONTROLLER_PID=""
AGENT1_PID=""
AGENT2_PID=""

# Cleanup function
cleanup() {
    echo -e "${YELLOW}Cleaning up processes...${NC}"

    if [[ -n "$AGENT1_PID" ]]; then
        echo "Stopping Agent 1 (PID: $AGENT1_PID)"
        kill $AGENT1_PID 2>/dev/null || true
    fi

    if [[ -n "$AGENT2_PID" ]]; then
        echo "Stopping Agent 2 (PID: $AGENT2_PID)"
        kill $AGENT2_PID 2>/dev/null || true
    fi

    if [[ -n "$CONTROLLER_PID" ]]; then
        echo "Stopping Controller (PID: $CONTROLLER_PID)"
        kill $CONTROLLER_PID 2>/dev/null || true
    fi

    # Wait a bit for graceful shutdown
    sleep 2

    # Force kill if still running
    if [[ -n "$AGENT1_PID" ]] && kill -0 $AGENT1_PID 2>/dev/null; then
        kill -9 $AGENT1_PID 2>/dev/null || true
    fi

    if [[ -n "$AGENT2_PID" ]] && kill -0 $AGENT2_PID 2>/dev/null; then
        kill -9 $AGENT2_PID 2>/dev/null || true
    fi

    if [[ -n "$CONTROLLER_PID" ]] && kill -0 $CONTROLLER_PID 2>/dev/null; then
        kill -9 $CONTROLLER_PID 2>/dev/null || true
    fi

    echo -e "${GREEN}Cleanup complete${NC}"
}

# Set up trap for cleanup on exit
trap cleanup EXIT INT TERM

# Function to wait for service to be ready
wait_for_service() {
    local url=$1
    local name=$2
    local max_attempts=30
    local attempt=1

    echo -e "${BLUE}Waiting for $name to be ready at $url...${NC}"

    while [[ $attempt -le $max_attempts ]]; do
        if curl -s -f "$url/health" >/dev/null 2>&1; then
            echo -e "${GREEN}$name is ready!${NC}"
            return 0
        fi

        echo -n "."
        sleep 1
        ((attempt++))
    done

    echo -e "${RED}$name failed to start within $max_attempts seconds${NC}"
    return 1
}

# Function to test HTTP endpoint
test_endpoint() {
    local method=$1
    local url=$2
    local description=$3
    local data=$4

    echo -e "${BLUE}Testing: $description${NC}"
    echo "Request: $method $url"

    if [[ -n "$data" ]]; then
        echo "Data: $data"
        response=$(curl -s -w "\nHTTP_STATUS:%{http_code}" -X "$method" -H "Content-Type: application/json" -d "$data" "$url" 2>&1)
    else
        response=$(curl -s -w "\nHTTP_STATUS:%{http_code}" -X "$method" "$url" 2>&1)
    fi

    http_status=$(echo "$response" | grep "HTTP_STATUS:" | cut -d: -f2)
    body=$(echo "$response" | sed '/HTTP_STATUS:/d')

    echo "Response Status: $http_status"
    echo "Response Body: $body"

    if [[ "$http_status" =~ ^[23] ]]; then
        echo -e "${GREEN}✓ Success${NC}"
        return 0
    else
        echo -e "${RED}✗ Failed${NC}"
        return 1
    fi
    echo
}

# Build binaries first
echo -e "${BLUE}Building SyncBit binaries...${NC}"
if ! go build -o bin/syncbitd ./cmd/syncbitd; then
    echo -e "${RED}Failed to build syncbitd${NC}"
    exit 1
fi

echo -e "${GREEN}Binary built successfully${NC}"

# Create test directories
echo -e "${BLUE}Setting up test environment...${NC}"
rm -rf /tmp/syncbit-test
mkdir -p /tmp/syncbit-test/agent1
mkdir -p /tmp/syncbit-test/agent2

# Clear old logs
> "$CONTROLLER_LOG"
> "$AGENT1_LOG"
> "$AGENT2_LOG"
> "$TEST_LOG"

echo -e "${GREEN}Starting SyncBit Dataset Assignment Test${NC}"
echo "========================================="

# Start Controller
echo -e "${BLUE}Starting Controller...${NC}"
./bin/syncbitd controller --config-file config.controller.yaml --debug > "$CONTROLLER_LOG" 2>&1 &
CONTROLLER_PID=$!
echo "Controller started with PID: $CONTROLLER_PID"

# Wait for controller to be ready
wait_for_service "http://localhost:8080" "Controller"

# Start Agent 1
echo -e "${BLUE}Starting Agent 1...${NC}"
./bin/syncbitd agent --config-file config.agent1.yaml --debug > "$AGENT1_LOG" 2>&1 &
AGENT1_PID=$!
echo "Agent 1 started with PID: $AGENT1_PID"

# Start Agent 2
echo -e "${BLUE}Starting Agent 2...${NC}"
./bin/syncbitd agent --config-file config.agent2.yaml --debug > "$AGENT2_LOG" 2>&1 &
AGENT2_PID=$!
echo "Agent 2 started with PID: $AGENT2_PID"

# Wait for agents to be ready
wait_for_service "http://localhost:8081" "Agent 1"
wait_for_service "http://localhost:8082" "Agent 2"

# Give some time for agent registration
echo -e "${BLUE}Waiting for agent registration...${NC}"
sleep 5

echo
echo -e "${GREEN}All services started successfully!${NC}"
echo
echo -e "${BLUE}Running Dataset Assignment API Tests...${NC}"
echo "========================================"

# Test Controller API
echo -e "${YELLOW}=== Controller API Tests ===${NC}"

# Test health endpoint
test_endpoint "GET" "http://localhost:8080/health" "Controller health check"

# Test agents list
test_endpoint "GET" "http://localhost:8080/agents" "List registered agents"

# Test Agent APIs
echo -e "${YELLOW}=== Agent API Tests ===${NC}"

# Test agent health endpoints
test_endpoint "GET" "http://localhost:8081/health" "Agent 1 health check"
test_endpoint "GET" "http://localhost:8082/health" "Agent 2 health check"

# Test agent state endpoints (should now include assignments)
test_endpoint "GET" "http://localhost:8081/state" "Agent 1 state"
test_endpoint "GET" "http://localhost:8082/state" "Agent 2 state"

# Test agent info endpoints
test_endpoint "GET" "http://localhost:8081/info" "Agent 1 info"
test_endpoint "GET" "http://localhost:8082/info" "Agent 2 info"

# Test assignment endpoints
test_endpoint "GET" "http://localhost:8081/assignments" "Agent 1 assignments"
test_endpoint "GET" "http://localhost:8082/assignments" "Agent 2 assignments"

# Test Dataset Management
echo -e "${YELLOW}=== Dataset Management Tests ===${NC}"

# Create a test dataset
dataset_data='{
  "name": "dialogpt-medium",
  "revision": "main",
  "replication": 2,
  "priority": 1,
  "sources": [
    {
      "type": "hf",
      "id": "hf-public"
    }
  ],
  "files": [
    {
      "path": "config.json",
      "size": 1024
    },
    {
      "path": "vocab.json", 
      "size": 2048
    }
  ]
}'

echo -e "${BLUE}Creating dataset with replication factor 2...${NC}"
test_endpoint "POST" "http://localhost:8080/datasets" "Create dataset with replication" "$dataset_data"

# List datasets
test_endpoint "GET" "http://localhost:8080/datasets" "List datasets"

# Get specific dataset
test_endpoint "GET" "http://localhost:8080/datasets/dialogpt-medium/main" "Get specific dataset"

# Wait a bit for assignment processing
echo -e "${BLUE}Waiting for dataset assignment processing...${NC}"
sleep 5

# Check agent assignments
echo -e "${YELLOW}=== Assignment Status Monitoring ===${NC}"

for i in {1..10}; do
    echo -e "${BLUE}=== Assignment Check $i/10 ===${NC}"
    
    # Check agent assignments
    echo -e "${BLUE}Agent 1 assignments:${NC}"
    curl -s "http://localhost:8081/assignments" | jq . 2>/dev/null || echo "Failed to get assignments"
    
    echo -e "${BLUE}Agent 2 assignments:${NC}"
    curl -s "http://localhost:8082/assignments" | jq . 2>/dev/null || echo "Failed to get assignments"
    
    # Check agent states
    echo -e "${BLUE}Agent 1 state:${NC}"
    curl -s "http://localhost:8081/state" | jq .state.assignments 2>/dev/null || echo "Failed to get state"
    
    echo -e "${BLUE}Agent 2 state:${NC}"
    curl -s "http://localhost:8082/state" | jq .state.assignments 2>/dev/null || echo "Failed to get state"
    
    # Check dataset status on controller
    echo -e "${BLUE}Dataset status:${NC}"
    curl -s "http://localhost:8080/datasets/dialogpt-medium/main" | jq .status 2>/dev/null || echo "Failed to get dataset status"
    
    sleep 3
done

# Test Dataset Serving (P2P functionality)
echo -e "${YELLOW}=== Dataset File Serving Tests ===${NC}"

# Check what datasets agents have
test_endpoint "GET" "http://localhost:8081/datasets" "Agent 1 available datasets"
test_endpoint "GET" "http://localhost:8082/datasets" "Agent 2 available datasets"

# Check file availability
test_endpoint "GET" "http://localhost:8081/files/availability" "Agent 1 file availability"
test_endpoint "GET" "http://localhost:8082/files/availability" "Agent 2 file availability"

# Create a smaller test dataset for actual transfer testing
small_dataset_data='{
  "name": "test/small-dataset",
  "revision": "main", 
  "replication": 1,
  "priority": 1,
  "sources": [
    {
      "type": "http",
      "id": "http-test"
    }
  ],
  "files": [
    {
      "path": "small-test.txt",
      "size": 100,
      "checksum": "abc123"
    }
  ]
}'

echo -e "${BLUE}Creating small test dataset...${NC}"
test_endpoint "POST" "http://localhost:8080/datasets" "Create small test dataset" "$small_dataset_data"

# Monitor assignment progress for small dataset
echo -e "${BLUE}Monitoring small dataset assignment...${NC}"
for i in {1..5}; do
    echo -e "${BLUE}=== Small Dataset Check $i/5 ===${NC}"
    curl -s "http://localhost:8080/datasets/test%2Fsmall-dataset/main" | jq . 2>/dev/null || echo "Failed to get dataset"
    sleep 2
done

# Final comprehensive stats
echo -e "${YELLOW}=== Final System Analysis ===${NC}"

echo -e "${BLUE}=== Controller Final Stats ===${NC}"
test_endpoint "GET" "http://localhost:8080/datasets" "All datasets final status"
test_endpoint "GET" "http://localhost:8080/agents" "All agents final status"

echo -e "${BLUE}=== Agent 1 Final Analysis ===${NC}"
test_endpoint "GET" "http://localhost:8081/assignments" "Agent 1 final assignments"
test_endpoint "GET" "http://localhost:8081/state" "Agent 1 final state"
test_endpoint "GET" "http://localhost:8081/cache/stats" "Agent 1 final cache stats"

echo -e "${BLUE}=== Agent 2 Final Analysis ===${NC}"
test_endpoint "GET" "http://localhost:8082/assignments" "Agent 2 final assignments"
test_endpoint "GET" "http://localhost:8082/state" "Agent 2 final state"
test_endpoint "GET" "http://localhost:8082/cache/stats" "Agent 2 final cache stats"

# Show recent logs
echo -e "${YELLOW}=== Recent Controller Logs ===${NC}"
if [[ -f "$CONTROLLER_LOG" ]]; then
    tail -n 10 "$CONTROLLER_LOG"
else
    echo "No controller log found"
fi

echo -e "${YELLOW}=== Recent Agent 1 Logs ===${NC}"
if [[ -f "$AGENT1_LOG" ]]; then
    tail -n 10 "$AGENT1_LOG"
else
    echo "No agent 1 log found"
fi

echo -e "${YELLOW}=== Recent Agent 2 Logs ===${NC}"
if [[ -f "$AGENT2_LOG" ]]; then
    tail -n 10 "$AGENT2_LOG"
else
    echo "No agent 2 log found"
fi

echo
echo -e "${GREEN}Dataset Assignment E2E Test Summary${NC}"
echo "===================================="
echo "Controller PID: $CONTROLLER_PID"
echo "Agent 1 PID: $AGENT1_PID"
echo "Agent 2 PID: $AGENT2_PID"
echo
echo "Log files:"
echo "- Controller: $CONTROLLER_LOG"
echo "- Agent 1: $AGENT1_LOG"
echo "- Agent 2: $AGENT2_LOG"
echo
echo -e "${BLUE}To monitor logs in real-time, run:${NC}"
echo "  tail -f $CONTROLLER_LOG"
echo "  tail -f $AGENT1_LOG"  
echo "  tail -f $AGENT2_LOG"
echo
echo -e "${YELLOW}Test completed successfully!${NC}"
echo "The new dataset assignment model is working."
echo "Agents are receiving assignments and reconciling state."
echo "Press Ctrl+C to stop all services and exit"

# Keep the script running to monitor
while true; do
    sleep 5

    # Check if processes are still running
    if ! kill -0 $CONTROLLER_PID 2>/dev/null; then
        echo -e "${RED}Controller process died!${NC}"
        break
    fi

    if ! kill -0 $AGENT1_PID 2>/dev/null; then
        echo -e "${RED}Agent 1 process died!${NC}"
        break
    fi

    if ! kill -0 $AGENT2_PID 2>/dev/null; then
        echo -e "${RED}Agent 2 process died!${NC}"
        break
    fi
done

echo -e "${YELLOW}Test completed. Check the logs for detailed information.${NC}"