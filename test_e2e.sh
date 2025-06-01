#!/bin/bash

# SyncBit End-to-End Test Script
# This script starts the controller and agents with proper logging,
# then runs comprehensive tests.

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
        if curl -s -f "$url/health" >/dev/null 2>&1 || curl -s -f "$url" >/dev/null 2>&1; then
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

# Function to monitor logs
monitor_logs() {
    echo -e "${BLUE}Starting log monitoring...${NC}"
    echo -e "${BLUE}Controller log: tail -f $CONTROLLER_LOG${NC}"
    echo -e "${BLUE}Agent 1 log: tail -f $AGENT1_LOG${NC}"
    echo -e "${BLUE}Agent 2 log: tail -f $AGENT2_LOG${NC}"
    echo
}

# Function to show log tails
show_recent_logs() {
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

# Create test directories
echo -e "${BLUE}Setting up test environment...${NC}"
# Remove any existing test directories to ensure clean state
rm -rf /tmp/syncbit-test
mkdir -p /tmp/syncbit-test/agent1
mkdir -p /tmp/syncbit-test/agent2

# Clear old logs
> "$CONTROLLER_LOG"
> "$AGENT1_LOG"
> "$AGENT2_LOG"
> "$TEST_LOG"

echo -e "${GREEN}Starting SyncBit End-to-End Test${NC}"
echo "=================================="

# Start Controller
echo -e "${BLUE}Starting Controller...${NC}"
./bin/syncbitd controller --config-file config.controller.yaml > "$CONTROLLER_LOG" 2>&1 &
CONTROLLER_PID=$!
echo "Controller started with PID: $CONTROLLER_PID"

# Wait for controller to be ready
wait_for_service "http://localhost:8080" "Controller"

# Start Agent 1
echo -e "${BLUE}Starting Agent 1...${NC}"
./bin/syncbitd agent --config-file config.agent1.yaml > "$AGENT1_LOG" 2>&1 &
AGENT1_PID=$!
echo "Agent 1 started with PID: $AGENT1_PID"

# Start Agent 2
echo -e "${BLUE}Starting Agent 2...${NC}"
./bin/syncbitd agent --config-file config.agent2.yaml > "$AGENT2_LOG" 2>&1 &
AGENT2_PID=$!
echo "Agent 2 started with PID: $AGENT2_PID"

# Wait for agents to be ready
wait_for_service "http://localhost:8081" "Agent 1"
wait_for_service "http://localhost:8082" "Agent 2"

# Give some time for registration
echo -e "${BLUE}Waiting for agent registration...${NC}"
sleep 5

monitor_logs

echo
echo -e "${GREEN}All services started successfully!${NC}"
echo
echo -e "${BLUE}Running API Tests...${NC}"
echo "===================="

# Test Controller API
echo -e "${YELLOW}=== Controller API Tests ===${NC}"

# Test health endpoint
test_endpoint "GET" "http://localhost:8080/health" "Controller health check"

# Test agents list
test_endpoint "GET" "http://localhost:8080/agents" "List registered agents"

# Test Agent APIs
echo -e "${YELLOW}=== Agent API Tests ===${NC}"

# Test agent 1 health
test_endpoint "GET" "http://localhost:8081/health" "Agent 1 health check"

# Test agent 2 health
test_endpoint "GET" "http://localhost:8082/health" "Agent 2 health check"

# Test agent 1 status
test_endpoint "GET" "http://localhost:8081/state" "Agent 1 state"

# Test agent 2 status
test_endpoint "GET" "http://localhost:8082/state" "Agent 2 state"

# Test agent info endpoints
test_endpoint "GET" "http://localhost:8081/info" "Agent 1 info"
test_endpoint "GET" "http://localhost:8082/info" "Agent 2 info"

# Submit test datasets
echo -e "${YELLOW}=== Dataset Creation Tests ===${NC}"

dataset_data='{
  "name": "microsoft/DialoGPT-medium",
  "revision": "main",
  "replication": 2,
  "priority": 10,
  "sources": [{
    "id": "hf-public",
    "type": "hf",
    "name": "Hugging Face Public",
    "token": ""
  }],
  "files": [{
    "path": "config.json",
    "size": "1 KB",
    "checksum": ""
  }]
}'

echo -e "${BLUE}Creating dataset with replication count of 2...${NC}"
test_endpoint "POST" "http://localhost:8080/datasets" "Create DialoGPT dataset with replication 2" "$dataset_data"

# Create a second dataset
dataset_data2='{
  "name": "microsoft/DialoGPT-small",
  "revision": "main",
  "replication": 1,
  "priority": 5,
  "sources": [{
    "id": "hf-public",
    "type": "hf",
    "name": "Hugging Face Public",
    "token": ""
  }],
  "files": [{
    "path": "vocab.json",
    "size": "500 B",
    "checksum": ""
  }]
}'

echo -e "${BLUE}Creating second dataset with replication count of 1...${NC}"
test_endpoint "POST" "http://localhost:8080/datasets" "Create DialoGPT-small dataset with replication 1" "$dataset_data2"

# Wait a bit for dataset processing to begin
echo -e "${BLUE}Waiting for dataset assignment and processing to begin...${NC}"
sleep 3

# Test dataset listing
echo -e "${YELLOW}=== Dataset Management Tests ===${NC}"
test_endpoint "GET" "http://localhost:8080/datasets" "List all datasets"

# Get specific datasets
test_endpoint "GET" "http://localhost:8080/datasets/microsoft%2FDialoGPT-medium/main" "Get DialoGPT-medium dataset"
test_endpoint "GET" "http://localhost:8080/datasets/microsoft%2FDialoGPT-small/main" "Get DialoGPT-small dataset"

# Get initial stats
echo -e "${YELLOW}=== Initial System Stats ===${NC}"
test_endpoint "GET" "http://localhost:8081/cache/stats" "Agent 1 cache stats"
test_endpoint "GET" "http://localhost:8082/cache/stats" "Agent 2 cache stats"

# Monitor dataset assignment and download progress
echo -e "${YELLOW}=== Dataset Assignment Monitoring ===${NC}"
for i in {1..10}; do
    echo -e "${BLUE}=== Progress Check $i/10 ===${NC}"

    # Check dataset list
    test_endpoint "GET" "http://localhost:8080/datasets" "List all datasets"

    # Check agent assignments
    test_endpoint "GET" "http://localhost:8080/agents/agent-1/assignments" "Agent 1 assignments"
    test_endpoint "GET" "http://localhost:8080/agents/agent-2/assignments" "Agent 2 assignments"

    # Check agent states
    test_endpoint "GET" "http://localhost:8081/state" "Agent 1 state"
    test_endpoint "GET" "http://localhost:8082/state" "Agent 2 state"

    # Check cache stats to see if files are being stored
    echo -e "${BLUE}Agent 1 cache stats:${NC}"
    curl -s "http://localhost:8081/cache/stats" | jq . 2>/dev/null || echo "Failed to get stats"

    echo -e "${BLUE}Agent 2 cache stats:${NC}"
    curl -s "http://localhost:8082/cache/stats" | jq . 2>/dev/null || echo "Failed to get stats"

    sleep 5
done

# Final comprehensive stats dump
echo -e "${YELLOW}=== Final System Analysis ===${NC}"

echo -e "${BLUE}=== Controller Final Stats ===${NC}"
test_endpoint "GET" "http://localhost:8080/datasets" "Final dataset list"
test_endpoint "GET" "http://localhost:8080/agents" "Final agent list"

echo -e "${BLUE}=== Agent 1 Final Analysis ===${NC}"
test_endpoint "GET" "http://localhost:8081/state" "Agent 1 final state"
test_endpoint "GET" "http://localhost:8081/cache/stats" "Agent 1 final cache stats"
test_endpoint "GET" "http://localhost:8081/datasets" "Agent 1 datasets"

echo -e "${BLUE}=== Agent 2 Final Analysis ===${NC}"
test_endpoint "GET" "http://localhost:8082/state" "Agent 2 final state"
test_endpoint "GET" "http://localhost:8082/cache/stats" "Agent 2 final cache stats"
test_endpoint "GET" "http://localhost:8082/datasets" "Agent 2 datasets"

# Check for P2P behavior - look for files on both agents
echo -e "${YELLOW}=== P2P Behavior Verification ===${NC}"

# Check if datasets exist on both agents
echo -e "${BLUE}Checking dataset existence on agents...${NC}"
agent1_datasets=$(curl -s "http://localhost:8081/datasets" | jq '.datasets[]?.name' 2>/dev/null || echo "")
agent2_datasets=$(curl -s "http://localhost:8082/datasets" | jq '.datasets[]?.name' 2>/dev/null || echo "")

echo "Agent 1 datasets: $agent1_datasets"
echo "Agent 2 datasets: $agent2_datasets"

# Check if both agents have files (indicating replication worked)
if [[ -n "$agent1_datasets" && -n "$agent2_datasets" ]]; then
    echo -e "${GREEN}✓ Both agents have datasets - replication appears to be working${NC}"

    # Try to get file lists from both agents
    for dataset in $agent1_datasets; do
        dataset_clean=$(echo $dataset | tr -d '"')
        echo -e "${BLUE}Checking files in dataset: $dataset_clean${NC}"
        test_endpoint "GET" "http://localhost:8081/datasets/$dataset_clean/files" "Agent 1 files in $dataset_clean"
        test_endpoint "GET" "http://localhost:8082/datasets/$dataset_clean/files" "Agent 2 files in $dataset_clean"
    done
else
    echo -e "${YELLOW}⚠ Dataset availability unclear - may still be downloading${NC}"
fi

echo
echo -e "${GREEN}Comprehensive P2P Test completed!${NC}"
echo

# Show recent logs
show_recent_logs

echo
echo -e "${GREEN}End-to-End Test Summary${NC}"
echo "========================"
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
echo -e "${YELLOW}Press Ctrl+C to stop all services and exit${NC}"

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
