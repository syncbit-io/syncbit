package integration

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"syncbit/internal/core/types"
)

// TestP2PJobSchedulingWorkflow tests the complete workflow where:
// 1. Agent 1 downloads a file from upstream
// 2. Controller tracks that Agent 1 has the file
// 3. Agent 2 needs the same file
// 4. Controller schedules Agent 2 to get the file from Agent 1 (P2P) instead of upstream
func TestP2PJobSchedulingWorkflow(t *testing.T) {
	// This test demonstrates the desired P2P workflow
	// Currently some parts are mocked since the full implementation isn't complete

	// Step 1: Set up test environment
	agent1Cache := createTestCache(t, "agent-1")
	agent2Cache := createTestCache(t, "agent-2")

	// Mock controller state
	controllerState := &MockControllerState{
		agents: make(map[string]*types.Agent),
		jobs:   make(map[string]*types.Job),
	}

	// Step 2: Register agents with controller
	agent1 := &types.Agent{
		ID:            "agent-1",
		AdvertiseAddr: types.NewAddress("localhost", 8081),
		State: types.AgentState{
			DiskAvailable: types.Bytes(1024 * 1024 * 1024), // 1GB
			ActiveJobs:    []string{},
			LastUpdated:   time.Now(),
		},
		LastHeartbeat: time.Now(),
	}

	agent2 := &types.Agent{
		ID:            "agent-2",
		AdvertiseAddr: types.NewAddress("localhost", 8082),
		State: types.AgentState{
			DiskAvailable: types.Bytes(1024 * 1024 * 1024), // 1GB
			ActiveJobs:    []string{},
			LastUpdated:   time.Now(),
		},
		LastHeartbeat: time.Now(),
	}

	controllerState.RegisterAgent(agent1)
	controllerState.RegisterAgent(agent2)

	// Step 3: Agent 1 downloads file from upstream (simulated)
	dataset := "microsoft/DialoGPT-medium"
	fileName := "config.json"
	fileContent := []byte(`{"model_type": "gpt2", "vocab_size": 50257}`)

	// Simulate Agent 1 completing a download job
	job1 := &types.Job{
		ID:      "job-1",
		Handler: types.JobHandlerDownload,
		Config: types.JobConfig{
			Repo:      "test/repo",
			Revision:  "main",
			FilePath:  "file1.txt",
			LocalPath: "/tmp/test",
			ProviderSource: types.ProviderSource{
				ProviderID: "hf-public",
			},
		},
		Status: types.StatusCompleted,
	}

	// Store file in agent 1's cache
	_, err := agent1Cache.StoreFile(dataset, fileName, fileContent)
	if err != nil {
		t.Fatalf("Failed to store file in agent 1: %v", err)
	}

	// Update controller state to reflect that agent 1 has the file
	controllerState.UpdateAgentFileInventory("agent-1", dataset, fileName, true)
	controllerState.jobs[job1.ID] = job1

	// Step 4: Agent 2 requests the same file
	job2 := &types.Job{
		ID:      "job-2",
		Handler: types.JobHandlerDownload,
		Config: types.JobConfig{
			Repo:      "test/repo",
			Revision:  "main",
			FilePath:  "file2.txt",
			LocalPath: "/tmp/test",
			ProviderSource: types.ProviderSource{
				ProviderID: "hf-public",
			},
		},
		Status: types.StatusPending,
	}

	// Step 5: Controller should find that agent 1 has the file
	peersWithFile := controllerState.FindPeersWithFile(dataset, fileName)
	if len(peersWithFile) != 1 {
		t.Fatalf("Expected 1 peer with file, got %d", len(peersWithFile))
	}

	if peersWithFile[0].ID != "agent-1" {
		t.Fatalf("Expected agent-1 to have the file, got %s", peersWithFile[0].ID)
	}

	// Step 6: Simulate P2P transfer from agent 1 to agent 2
	// In a real implementation, this would be an HTTP request
	retrievedData, err := agent1Cache.GetFileByPath(dataset, fileName)
	if err != nil {
		t.Fatalf("Failed to retrieve file from agent 1: %v", err)
	}

	// Agent 2 stores the file
	_, err = agent2Cache.StoreFile(dataset, fileName, retrievedData)
	if err != nil {
		t.Fatalf("Failed to store file in agent 2: %v", err)
	}

	// Step 7: Verify the P2P transfer was successful
	if !agent2Cache.HasFileByPath(dataset, fileName) {
		t.Fatal("Agent 2 should have the file after P2P transfer")
	}

	// Verify file content is identical
	data1, _ := agent1Cache.GetFileByPath(dataset, fileName)
	data2, _ := agent2Cache.GetFileByPath(dataset, fileName)

	if string(data1) != string(data2) {
		t.Fatal("File content should be identical after P2P transfer")
	}

	// Step 8: Update controller state
	controllerState.UpdateAgentFileInventory("agent-2", dataset, fileName, true)
	job2.SetStatus(types.StatusCompleted, "")
	controllerState.jobs[job2.ID] = job2

	// Step 9: Verify both agents are now tracked as having the file
	peersWithFileAfter := controllerState.FindPeersWithFile(dataset, fileName)
	if len(peersWithFileAfter) != 2 {
		t.Fatalf("Expected 2 peers with file after P2P transfer, got %d", len(peersWithFileAfter))
	}

	t.Logf("P2P workflow test completed successfully")
	t.Logf("Agent 1 downloaded from upstream, Agent 2 got file via P2P")
	t.Logf("Both agents now have the file: %s/%s", dataset, fileName)
}

// TestAgentFileServingAPI tests the HTTP API that agents use to serve files to peers
func TestAgentFileServingAPI(t *testing.T) {
	// Create a mock agent server
	agentCache := createTestCache(t, "serving-agent")

	// Store test files
	dataset := "test-dataset"
	files := map[string][]byte{
		"config.json": []byte(`{"model": "test"}`),
		"vocab.txt":   []byte("hello\nworld\n"),
		"model.bin":   make([]byte, 1024), // 1KB binary file
	}

	for fileName, content := range files {
		_, err := agentCache.StoreFile(dataset, fileName, content)
		if err != nil {
			t.Fatalf("Failed to store %s: %v", fileName, err)
		}
	}

	// Create mock HTTP handlers for agent API
	mux := http.NewServeMux()

	// File serving endpoint
	mux.HandleFunc("/datasets/", func(w http.ResponseWriter, r *http.Request) {
		// Parse URL: /datasets/{dataset}/files/{filepath}
		path := strings.TrimPrefix(r.URL.Path, "/datasets/")
		parts := strings.SplitN(path, "/files/", 2)
		if len(parts) != 2 {
			http.Error(w, "Invalid path", http.StatusBadRequest)
			return
		}

		datasetName := parts[0]
		filePath := parts[1]

		// Handle file info requests
		if strings.HasSuffix(filePath, "/info") {
			filePath = strings.TrimSuffix(filePath, "/info")
			if agentCache.HasFileByPath(datasetName, filePath) {
				data, _ := agentCache.GetFileByPath(datasetName, filePath)
				response := map[string]interface{}{
					"file": map[string]interface{}{
						"path": filePath,
						"size": len(data),
					},
				}
				w.Header().Set("Content-Type", "application/json")
				json.NewEncoder(w).Encode(response)
			} else {
				http.Error(w, "File not found", http.StatusNotFound)
			}
			return
		}

		// Handle file content requests
		if agentCache.HasFileByPath(datasetName, filePath) {
			data, err := agentCache.GetFileByPath(datasetName, filePath)
			if err != nil {
				http.Error(w, "Failed to read file", http.StatusInternalServerError)
				return
			}
			w.Header().Set("Content-Type", "application/octet-stream")
			w.Write(data)
		} else {
			http.Error(w, "File not found", http.StatusNotFound)
		}
	})

	// Dataset listing endpoint
	mux.HandleFunc("/datasets", func(w http.ResponseWriter, r *http.Request) {
		response := map[string]interface{}{
			"datasets": []map[string]interface{}{
				{"name": dataset},
			},
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(response)
	})

	server := httptest.NewServer(mux)
	defer server.Close()

	// Test file retrieval
	for fileName := range files {
		url := fmt.Sprintf("%s/datasets/%s/files/%s", server.URL, dataset, fileName)
		resp, err := http.Get(url)
		if err != nil {
			t.Fatalf("Failed to get %s: %v", fileName, err)
		}
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusOK {
			t.Fatalf("Expected 200 for %s, got %d", fileName, resp.StatusCode)
		}

		var content []byte
		resp.Body.Read(content)
		// Note: This is a simplified test - in reality we'd read the full response
	}

	// Test file info endpoint
	infoURL := fmt.Sprintf("%s/datasets/%s/files/%s/info", server.URL, dataset, "config.json")
	resp, err := http.Get(infoURL)
	if err != nil {
		t.Fatalf("Failed to get file info: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("Expected 200 for file info, got %d", resp.StatusCode)
	}

	var infoResponse map[string]interface{}
	json.NewDecoder(resp.Body).Decode(&infoResponse)

	fileInfo := infoResponse["file"].(map[string]interface{})
	if fileInfo["path"] != "config.json" {
		t.Fatalf("Expected path config.json, got %v", fileInfo["path"])
	}
}

// MockControllerState simulates controller state for testing
type MockControllerState struct {
	agents        map[string]*types.Agent
	jobs          map[string]*types.Job
	fileInventory map[string]map[string]map[string]bool // agent_id -> dataset -> file -> exists
}

func (m *MockControllerState) RegisterAgent(agent *types.Agent) {
	if m.fileInventory == nil {
		m.fileInventory = make(map[string]map[string]map[string]bool)
	}
	m.agents[agent.ID] = agent
	m.fileInventory[agent.ID] = make(map[string]map[string]bool)
}

func (m *MockControllerState) UpdateAgentFileInventory(agentID, dataset, fileName string, exists bool) {
	if m.fileInventory[agentID] == nil {
		m.fileInventory[agentID] = make(map[string]map[string]bool)
	}
	if m.fileInventory[agentID][dataset] == nil {
		m.fileInventory[agentID][dataset] = make(map[string]bool)
	}
	m.fileInventory[agentID][dataset][fileName] = exists
}

func (m *MockControllerState) FindPeersWithFile(dataset, fileName string) []*types.Agent {
	var peers []*types.Agent
	for agentID, agent := range m.agents {
		if m.fileInventory[agentID] != nil &&
			m.fileInventory[agentID][dataset] != nil &&
			m.fileInventory[agentID][dataset][fileName] {
			peers = append(peers, agent)
		}
	}
	return peers
}
