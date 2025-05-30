#!/bin/bash

# SyncBit Client Test Script
# This script submits various test jobs to a running SyncBit system

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Configuration
CONTROLLER_URL="http://localhost:8080"
DOWNLOAD_DIR="/tmp/syncbit-test/downloads"

# Function to make HTTP requests
api_request() {
    local method=$1
    local endpoint=$2
    local data=$3
    local description=$4

    echo -e "${BLUE}$description${NC}"
    echo "Request: $method $CONTROLLER_URL$endpoint"

    if [[ -n "$data" ]]; then
        echo "Data: $data"
        response=$(curl -s -w "\nHTTP_STATUS:%{http_code}" -X "$method" \
                   -H "Content-Type: application/json" \
                   -d "$data" "$CONTROLLER_URL$endpoint" 2>&1)
    else
        response=$(curl -s -w "\nHTTP_STATUS:%{http_code}" -X "$method" \
                   "$CONTROLLER_URL$endpoint" 2>&1)
    fi

    http_status=$(echo "$response" | grep "HTTP_STATUS:" | cut -d: -f2)
    body=$(echo "$response" | sed '/HTTP_STATUS:/d')

    echo "Response Status: $http_status"
    echo "Response Body: $body"

    if [[ "$http_status" =~ ^[23] ]]; then
        echo -e "${GREEN}✓ Success${NC}"
        # Extract job ID if this was a job submission
        if [[ "$endpoint" == "/jobs" && "$method" == "POST" ]]; then
            JOB_ID=$(echo "$body" | jq -r '.id // empty' 2>/dev/null || echo "")
            if [[ -n "$JOB_ID" ]]; then
                echo "Job ID: $JOB_ID"
                return 0
            fi
        fi
        return 0
    else
        echo -e "${RED}✗ Failed${NC}"
        return 1
    fi
    echo
}

# Function to wait for job completion
wait_for_job() {
    local job_id=$1
    local max_wait=${2:-60}
    local wait_count=0

    echo -e "${BLUE}Waiting for job $job_id to complete (max ${max_wait}s)...${NC}"

    while [[ $wait_count -lt $max_wait ]]; do
        response=$(curl -s "$CONTROLLER_URL/jobs/$job_id" 2>/dev/null || echo "")

        if [[ -n "$response" ]]; then
            status=$(echo "$response" | jq -r '.status // "unknown"' 2>/dev/null || echo "unknown")
            echo -n "."

            case "$status" in
                "completed")
                    echo -e "\n${GREEN}Job $job_id completed successfully!${NC}"
                    return 0
                    ;;
                "failed")
                    echo -e "\n${RED}Job $job_id failed!${NC}"
                    error=$(echo "$response" | jq -r '.error // "unknown error"' 2>/dev/null || echo "unknown error")
                    echo "Error: $error"
                    return 1
                    ;;
                "cancelled")
                    echo -e "\n${YELLOW}Job $job_id was cancelled${NC}"
                    return 1
                    ;;
            esac
        fi

        sleep 2
        ((wait_count += 2))
    done

    echo -e "\n${YELLOW}Timeout waiting for job $job_id${NC}"
    return 1
}

# Create download directory
mkdir -p "$DOWNLOAD_DIR"

echo -e "${GREEN}SyncBit Client Test${NC}"
echo "=================="
echo "Controller: $CONTROLLER_URL"
echo "Download Directory: $DOWNLOAD_DIR"
echo

# Test 1: Check system status
echo -e "${YELLOW}=== Test 1: System Status Check ===${NC}"
api_request "GET" "/health" "" "Checking controller health"
api_request "GET" "/agents" "" "Listing registered agents"
echo

# Test 2: Submit a small file download job
echo -e "${YELLOW}=== Test 2: Small File Download ===${NC}"
job_data='{
  "type": "download",
  "handler": "hf",
  "config": {
    "provider_id": "hf-public",
    "repo": "microsoft/DialoGPT-medium",
    "revision": "main",
    "files": ["config.json"],
    "local_path": "'$DOWNLOAD_DIR'/small-test"
  }
}'

if api_request "POST" "/jobs" "$job_data" "Submitting small file download job"; then
    if [[ -n "$JOB_ID" ]]; then
        wait_for_job "$JOB_ID" 30
    fi
fi
echo

# Test 3: Submit multiple file download job
echo -e "${YELLOW}=== Test 3: Multiple File Download ===${NC}"
job_data='{
  "type": "download",
  "handler": "hf",
  "config": {
    "provider_id": "hf-public",
    "repo": "microsoft/DialoGPT-medium",
    "revision": "main",
    "files": ["config.json", "vocab.json", "tokenizer_config.json"],
    "local_path": "'$DOWNLOAD_DIR'/multi-test"
  }
}'

if api_request "POST" "/jobs" "$job_data" "Submitting multiple file download job"; then
    if [[ -n "$JOB_ID" ]]; then
        wait_for_job "$JOB_ID" 60
    fi
fi
echo

# Test 4: Submit a larger model download
echo -e "${YELLOW}=== Test 4: Larger Model Download ===${NC}"
job_data='{
  "type": "download",
  "handler": "hf",
  "config": {
    "provider_id": "hf-public",
    "repo": "distilbert-base-uncased",
    "revision": "main",
    "files": ["config.json", "pytorch_model.bin"],
    "local_path": "'$DOWNLOAD_DIR'/large-test"
  }
}'

if api_request "POST" "/jobs" "$job_data" "Submitting larger model download job"; then
    if [[ -n "$JOB_ID" ]]; then
        wait_for_job "$JOB_ID" 120
    fi
fi
echo

# Test 5: List all jobs to see the history
echo -e "${YELLOW}=== Test 5: Job History ===${NC}"
api_request "GET" "/jobs" "" "Listing all jobs"
echo

# Test 6: Check agents status after jobs
echo -e "${YELLOW}=== Test 6: Final System Status ===${NC}"
api_request "GET" "/agents" "" "Checking agent status after jobs"

# Check individual agent states
echo -e "${YELLOW}=== Agent State Check ===${NC}"
echo -e "${BLUE}Checking Agent 1 state...${NC}"
curl -s "http://localhost:8081/state" | jq . 2>/dev/null || echo "Failed to get agent 1 state"

echo -e "${BLUE}Checking Agent 2 state...${NC}"
curl -s "http://localhost:8082/state" | jq . 2>/dev/null || echo "Failed to get agent 2 state"

# Check if any files were actually downloaded
echo -e "${YELLOW}=== Downloaded Files Check ===${NC}"
echo "Checking downloaded files in $DOWNLOAD_DIR:"
if [[ -d "$DOWNLOAD_DIR" ]]; then
    find "$DOWNLOAD_DIR" -type f -exec ls -lh {} \; 2>/dev/null || echo "No files found"
else
    echo "Download directory does not exist"
fi

echo
echo -e "${GREEN}Client tests completed!${NC}"
echo
echo -e "${BLUE}To check logs, run:${NC}"
echo "  ./monitor_logs.sh recent 50"
echo "  ./monitor_logs.sh search ERROR"
echo "  ./monitor_logs.sh all"
