#!/bin/bash

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

echo -e "${BLUE}=== SyncBit Download Integrity Test ===${NC}"
echo

# Use a real HuggingFace file for testing
# This is a small config file from a popular model
# We'll use a simple dataset name and map it to the real repo in the code
TEST_REPO="microsoft/DialoGPT-medium"
TEST_DATASET_NAME="dialogpt-medium"  # Simple name without slashes
TEST_FILE="config.json"
TEST_URL="https://huggingface.co/${TEST_REPO}/resolve/main/${TEST_FILE}"

echo -e "${BLUE}Building SyncBit binaries...${NC}"
go build -o bin/syncbitd ./cmd/syncbitd

echo -e "${BLUE}Setting up test environment...${NC}"
rm -rf /tmp/syncbit-test
mkdir -p /tmp/syncbit-test/{agent1,agent2}

# Clear logs
> controller.log
> agent1.log  
> agent2.log

echo -e "${GREEN}Starting services...${NC}"

# Start Controller with debug
./bin/syncbitd controller --config-file config.controller.yaml --debug > controller.log 2>&1 &
CONTROLLER_PID=$!
echo "Controller started with PID: $CONTROLLER_PID"

sleep 2

# Start Agent 1 with debug
./bin/syncbitd agent --config-file config.agent1.yaml --debug > agent1.log 2>&1 &
AGENT1_PID=$!
echo "Agent 1 started with PID: $AGENT1_PID"

# Wait for services to be ready
sleep 5

cleanup() {
    echo -e "${YELLOW}Cleaning up...${NC}"
    kill $CONTROLLER_PID $AGENT1_PID 2>/dev/null || true
    sleep 2
    kill -9 $CONTROLLER_PID $AGENT1_PID 2>/dev/null || true
}

trap cleanup EXIT

echo -e "${BLUE}Creating test dataset...${NC}"

# Create dataset with the HuggingFace test file using source field
curl -s -X POST http://localhost:8080/datasets \
  -H "Content-Type: application/json" \
  -d "{
    \"name\": \"$TEST_DATASET_NAME\",
    \"source\": \"$TEST_REPO\",
    \"revision\": \"main\",
    \"replication\": 1,
    \"priority\": 1,
    \"sources\": [
      {
        \"type\": \"hf\",
        \"id\": \"hf-public\"
      }
    ],
    \"files\": [
      {
        \"path\": \"$TEST_FILE\",
        \"size\": 1500,
        \"checksum\": \"\"
      }
    ]
  }" | jq

echo -e "\n${BLUE}Waiting for download to complete...${NC}"
sleep 10

# Wait for reconciliation and download
for i in {1..10}; do
    echo -e "${BLUE}Check $i/10: Waiting for download...${NC}"
    
    # Check if transfer completed
    if grep -q "File transfer completed" agent1.log; then
        echo -e "${GREEN}✓ Download completed!${NC}"
        break
    fi
    
    sleep 5
done

echo -e "\n${BLUE}=== Download Results ===${NC}"

# Show recent agent logs
echo -e "\n${YELLOW}Agent 1 Recent Logs:${NC}"
tail -10 agent1.log | grep -E "(transfer|download|block|error|completed)" || echo "No relevant logs"

echo -e "\n${BLUE}=== File Integrity Test ===${NC}"

# Find the downloaded file in the cache
CACHE_DIR="/tmp/syncbit-test/agent1"
echo "Looking for cached file in: $CACHE_DIR"

# Look for files in the cache directory
if [ -d "$CACHE_DIR" ]; then
    find "$CACHE_DIR" -name "*" -type f | head -10
fi

# Download the same file directly with curl for comparison
echo -e "\n${BLUE}Downloading reference file with curl...${NC}"
curl -s "$TEST_URL" -o /tmp/reference_file.json

echo -e "\n${BLUE}Computing checksums...${NC}"

# Get reference checksum
REFERENCE_CHECKSUM=$(shasum -a 256 /tmp/reference_file.json | cut -d' ' -f1)
echo "Reference file checksum: $REFERENCE_CHECKSUM"

# Try to find and checksum the cached file
CACHED_FILE=""
if [ -d "$CACHE_DIR" ]; then
    # Look for the cached file in the new files directory structure
    CACHED_FILE=$(find "$CACHE_DIR/files" -name "$TEST_FILE" 2>/dev/null | head -1)
    if [ -z "$CACHED_FILE" ]; then
        # Fallback: look for any .bin files
        CACHED_FILE=$(find "$CACHE_DIR" -name "*.bin" -o -name "*$TEST_FILE*" -o -name "*integrity-test*" | head -1)
    fi
fi

if [ -n "$CACHED_FILE" ] && [ -f "$CACHED_FILE" ]; then
    echo "Found cached file: $CACHED_FILE"
    CACHED_CHECKSUM=$(shasum -a 256 "$CACHED_FILE" | cut -d' ' -f1)
    echo "Cached file checksum:   $CACHED_CHECKSUM"
    
    if [ "$REFERENCE_CHECKSUM" = "$CACHED_CHECKSUM" ]; then
        echo -e "${GREEN}✅ SUCCESS: Checksums match! Download integrity verified.${NC}"
    else
        echo -e "${RED}❌ FAILURE: Checksums do not match!${NC}"
        echo "Reference: $REFERENCE_CHECKSUM"
        echo "Cached:    $CACHED_CHECKSUM"
        
        # Show file sizes for debugging
        echo -e "\nFile sizes:"
        ls -la /tmp/reference_file.json
        ls -la "$CACHED_FILE"
    fi
else
    echo -e "${YELLOW}⚠️  Could not find cached file for comparison${NC}"
    echo "Checked directory: $CACHE_DIR"
    if [ -d "$CACHE_DIR" ]; then
        echo "Contents:"
        find "$CACHE_DIR" -type f | head -10
    fi
fi

# Clean up reference file
rm -f /tmp/reference_file.json

echo -e "\n${BLUE}=== Test Summary ===${NC}"
echo "Controller PID: $CONTROLLER_PID"
echo "Agent 1 PID: $AGENT1_PID"
echo -e "Test completed. Check logs for details."