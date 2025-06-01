#!/bin/bash

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

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

# Start Agents with debug
./bin/syncbitd agent --config-file config.agent1.yaml --debug > agent1.log 2>&1 &
AGENT1_PID=$!
echo "Agent 1 started with PID: $AGENT1_PID"

./bin/syncbitd agent --config-file config.agent2.yaml --debug > agent2.log 2>&1 &
AGENT2_PID=$!
echo "Agent 2 started with PID: $AGENT2_PID"

# Wait for services to be ready
sleep 5

echo -e "${BLUE}Creating test dataset...${NC}"

# Create a simple dataset without slashes
curl -s -X POST http://localhost:8080/datasets \
  -H "Content-Type: application/json" \
  -d '{
    "name": "test-model",
    "revision": "v1",
    "replication": 2,
    "priority": 1,
    "sources": [
      {
        "type": "http",
        "id": "http-test"
      }
    ],
    "files": [
      {
        "path": "model.bin",
        "size": 5242880,
        "checksum": "abc123"
      }
    ]
  }' | jq

echo -e "\n${BLUE}Waiting for controller to schedule assignments (30s)...${NC}"
sleep 30

echo -e "\n${GREEN}=== ASSIGNMENT STATUS ===${NC}"
echo -e "\n${YELLOW}Controller Datasets:${NC}"
curl -s http://localhost:8080/datasets | jq -r '.datasets[] | "- \(.name)@\(.revision): \(.status.phase), replicas: \(.status.ready_replicas)/\(.replication)"'

echo -e "\n${YELLOW}Agent 1:${NC}"
curl -s http://localhost:8081/assignments | jq -r '.assignments[] | "- \(.dataset)@\(.revision): \(.status.phase)"'

echo -e "\n${YELLOW}Agent 2:${NC}"
curl -s http://localhost:8082/assignments | jq -r '.assignments[] | "- \(.dataset)@\(.revision): \(.status.phase)"'

echo -e "\n${BLUE}Waiting for first reconciliation cycle (30s more)...${NC}"
sleep 30

echo -e "\n${GREEN}=== AFTER RECONCILIATION ===${NC}"

echo -e "\n${YELLOW}Recent Agent 1 logs:${NC}"
tail -20 agent1.log | grep -E "(Reconciling|dataset|error|Failed|Fetching)" || echo "No relevant logs"

echo -e "\n${YELLOW}Recent Agent 2 logs:${NC}"
tail -20 agent2.log | grep -E "(Reconciling|dataset|error|Failed|Fetching)" || echo "No relevant logs"

echo -e "\n${YELLOW}Agent 1 Status:${NC}"
curl -s http://localhost:8081/assignments | jq -r '.assignments[] | "- \(.dataset)@\(.revision): phase=\(.status.phase), files=\(.status.progress.files_ready)/\(.status.progress.files_total)"'

echo -e "\n${YELLOW}Agent 2 Status:${NC}"
curl -s http://localhost:8082/assignments | jq -r '.assignments[] | "- \(.dataset)@\(.revision): phase=\(.status.phase), files=\(.status.progress.files_ready)/\(.status.progress.files_total)"'

echo -e "\n${GREEN}Cleaning up...${NC}"
kill $CONTROLLER_PID $AGENT1_PID $AGENT2_PID 2>/dev/null || true

echo -e "${GREEN}Test completed!${NC}"