package controller

import (
	"net/url"
	"testing"
	"time"

	"syncbit/internal/core/types"
)

func TestNewController(t *testing.T) {
	controller, err := NewController("", true)
	if err != nil {
		t.Fatalf("Failed to create controller: %v", err)
	}

	if controller == nil {
		t.Fatal("Controller should not be nil")
	}

	if controller.cfg == nil {
		t.Fatal("Controller config should not be nil")
	}

	if controller.logger == nil {
		t.Fatal("Controller logger should not be nil")
	}

	if controller.server == nil {
		t.Fatal("Controller server should not be nil")
	}

	if controller.scheduler == nil {
		t.Fatal("Controller scheduler should not be nil")
	}
}

func TestAgentRegistration(t *testing.T) {
	controller, err := NewController("", true)
	if err != nil {
		t.Fatalf("Failed to create controller: %v", err)
	}

	// Create test agent
	agentURL, _ := url.Parse("http://localhost:8081")
	agent := &types.Agent{
		ID:            "test-agent-1",
		AdvertiseAddr: agentURL,
		State: types.AgentState{
			DiskUsed:      types.Bytes(1024),
			DiskAvailable: types.Bytes(10 * 1024 * 1024 * 1024), // 10GB
			Assignments:   []string{},
			LocalDatasets: make(map[string]types.LocalDataset),
			LastUpdated:   time.Now(),
		},
		LastHeartbeat: time.Now(),
	}

	// Register agent
	controller.agentsMu.Lock()
	controller.agents[agent.ID] = agent
	controller.agentsMu.Unlock()

	// Verify agent is registered
	controller.agentsMu.RLock()
	registeredAgent, exists := controller.agents[agent.ID]
	controller.agentsMu.RUnlock()

	if !exists {
		t.Fatal("Agent should be registered")
	}

	if registeredAgent.ID != agent.ID {
		t.Errorf("Expected agent ID %s, got %s", agent.ID, registeredAgent.ID)
	}

	if registeredAgent.AdvertiseAddr.String() != agentURL.String() {
		t.Errorf("Expected agent URL %s, got %s", agentURL.String(), registeredAgent.AdvertiseAddr.String())
	}
}

func TestDatasetManagement(t *testing.T) {
	controller, err := NewController("", true)
	if err != nil {
		t.Fatalf("Failed to create controller: %v", err)
	}

	// Create test dataset
	dataset := &types.Dataset{
		Name:        "test-dataset",
		Revision:    "main",
		Files:       []types.DatasetFile{},
		Replication: 2,
		Sources:     []types.ProviderConfig{},
		Priority:    1,
		Status: types.DatasetStatus{
			Phase:         types.DatasetPhasePending,
			ReadyReplicas: 0,
			TotalReplicas: 2,
			Assignments:   make(map[string]*types.AssignmentStatus),
			LastUpdated:   time.Now(),
		},
		CreatedAt: time.Now(),
		UpdatedAt: time.Now(),
	}

	// Register dataset
	controller.datasetsMu.Lock()
	controller.datasets[dataset.Name] = dataset
	controller.datasetsMu.Unlock()

	// Verify dataset is registered
	controller.datasetsMu.RLock()
	registeredDataset, exists := controller.datasets[dataset.Name]
	controller.datasetsMu.RUnlock()

	if !exists {
		t.Fatal("Dataset should be registered")
	}

	if registeredDataset.Name != dataset.Name {
		t.Errorf("Expected dataset name %s, got %s", dataset.Name, registeredDataset.Name)
	}

	if registeredDataset.Replication != dataset.Replication {
		t.Errorf("Expected replication %d, got %d", dataset.Replication, registeredDataset.Replication)
	}
}

func TestAssignmentManagement(t *testing.T) {
	controller, err := NewController("", true)
	if err != nil {
		t.Fatalf("Failed to create controller: %v", err)
	}

	// Create test assignment
	assignment := &types.DatasetAssignment{
		ID:       "test-assignment-1",
		AgentID:  "test-agent-1",
		Dataset:  "test-dataset",
		Revision: "main",
		Priority: 1,
		Status: types.AssignmentStatus{
			Phase:       types.AssignmentPhasePending,
			Files:       make(map[string]*types.FileStatus),
			Progress:    types.AssignmentProgress{},
			LastUpdated: time.Now(),
		},
		CreatedAt: time.Now(),
		UpdatedAt: time.Now(),
	}

	// Register assignment
	controller.assignmentsMu.Lock()
	controller.assignments[assignment.ID] = assignment
	controller.assignmentsMu.Unlock()

	// Verify assignment is registered
	controller.assignmentsMu.RLock()
	registeredAssignment, exists := controller.assignments[assignment.ID]
	controller.assignmentsMu.RUnlock()

	if !exists {
		t.Fatal("Assignment should be registered")
	}

	if registeredAssignment.ID != assignment.ID {
		t.Errorf("Expected assignment ID %s, got %s", assignment.ID, registeredAssignment.ID)
	}

	if registeredAssignment.AgentID != assignment.AgentID {
		t.Errorf("Expected agent ID %s, got %s", assignment.AgentID, registeredAssignment.AgentID)
	}

	if registeredAssignment.Dataset != assignment.Dataset {
		t.Errorf("Expected dataset %s, got %s", assignment.Dataset, registeredAssignment.Dataset)
	}
}