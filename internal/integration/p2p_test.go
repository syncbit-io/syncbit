package integration

import (
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"syncbit/internal/cache"
	"syncbit/internal/config"
	"syncbit/internal/core/types"
	"syncbit/internal/provider"
)

// TestPeerToPeerFileSharing tests the complete P2P file sharing workflow
func TestPeerToPeerFileSharing(t *testing.T) {
	// Create two test agents (simplified for testing)
	agent1Cache := createTestCache(t, "agent-1")
	agent2Cache := createTestCache(t, "agent-2")

	// Step 1: Agent 1 has a file (simulating successful download)
	testData := []byte("This is test file content that will be shared via P2P")
	dataset := "microsoft/DialoGPT-medium"
	fileName := "config.json"

	// Store file in agent 1's cache
	_, err := agent1Cache.StoreFile(dataset, fileName, testData)
	if err != nil {
		t.Fatalf("Failed to store file in agent 1: %v", err)
	}

	// Verify agent 1 has the file
	if !agent1Cache.HasFileByPath(dataset, fileName) {
		t.Fatal("Agent 1 should have the file")
	}

	// Step 2: Simulate P2P transfer - Agent 2 gets file from Agent 1
	// In a real implementation, this would be an HTTP request between agents
	retrievedData, err := agent1Cache.GetFileByPath(dataset, fileName)
	if err != nil {
		t.Fatalf("Failed to retrieve file from agent 1: %v", err)
	}

	if string(retrievedData) != string(testData) {
		t.Fatalf("Retrieved data doesn't match original data")
	}

	// Step 3: Agent 2 stores the file received from agent 1
	_, err = agent2Cache.StoreFile(dataset, fileName, retrievedData)
	if err != nil {
		t.Fatalf("Failed to store file in agent 2: %v", err)
	}

	// Verify both agents now have the file
	if !agent1Cache.HasFileByPath(dataset, fileName) {
		t.Fatal("Agent 1 should still have the file")
	}

	if !agent2Cache.HasFileByPath(dataset, fileName) {
		t.Fatal("Agent 2 should now have the file")
	}

	// Verify file content is identical
	data1, err := agent1Cache.GetFileByPath(dataset, fileName)
	if err != nil {
		t.Fatalf("Failed to get file from agent 1: %v", err)
	}

	data2, err := agent2Cache.GetFileByPath(dataset, fileName)
	if err != nil {
		t.Fatalf("Failed to get file from agent 2: %v", err)
	}

	if string(data1) != string(data2) {
		t.Fatal("File content should be identical across agents")
	}
}

// TestPeerDiscoveryAndScheduling tests how the controller discovers peers with files
func TestPeerDiscoveryAndScheduling(t *testing.T) {
	// This test is currently limited because the peer discovery mechanism
	// isn't fully implemented in the controller yet
	t.Skip("Peer discovery mechanism not yet fully implemented")

	/*
		Future test would cover:
		1. Controller tracking which agents have which files
		2. Intelligent job scheduling to use peer sources when available
		3. Fallback to upstream providers when peers are unavailable
		4. Load balancing across multiple peers for the same file
	*/
}

// TestMultiAgentFileConsistency tests file consistency across multiple agents
func TestMultiAgentFileConsistency(t *testing.T) {
	// Create multiple caches (representing multiple agents)
	numAgents := 3
	caches := make([]*cache.Cache, numAgents)

	for i := 0; i < numAgents; i++ {
		caches[i] = createTestCache(t, fmt.Sprintf("agent-%d", i+1))
	}

	// Test data
	testData := []byte("Consistent file content across all agents")
	dataset := "consistency-test"
	fileName := "shared-file.txt"

	// Store file in first agent
	_, err := caches[0].StoreFile(dataset, fileName, testData)
	if err != nil {
		t.Fatalf("Failed to store file in agent 0: %v", err)
	}

	// Simulate P2P distribution to other agents
	for i := 1; i < len(caches); i++ {
		// Get file from first agent (simulating P2P transfer)
		retrievedData, err := caches[0].GetFileByPath(dataset, fileName)
		if err != nil {
			t.Fatalf("Failed to get file from agent 0 for agent %d: %v", i, err)
		}

		// Store in current agent
		_, err = caches[i].StoreFile(dataset, fileName, retrievedData)
		if err != nil {
			t.Fatalf("Failed to store file in agent %d: %v", i, err)
		}
	}

	// Verify all agents have the same file content
	for i, agentCache := range caches {
		if !agentCache.HasFileByPath(dataset, fileName) {
			t.Fatalf("Agent %d doesn't have the file", i)
		}

		data, err := agentCache.GetFileByPath(dataset, fileName)
		if err != nil {
			t.Fatalf("Failed to get file from agent %d: %v", i, err)
		}

		if string(data) != string(testData) {
			t.Fatalf("Agent %d has inconsistent file content", i)
		}
	}
}

// TestPeerProviderIntegration tests the peer provider functionality
func TestPeerProviderIntegration(t *testing.T) {
	// Create peer provider
	providerConfig := config.ProviderConfig{
		ID:   "test-peer",
		Type: "peer",
		Name: "Test Peer Provider",
	}

	transferConfig := config.TransferConfig{} // Empty for now

	peerProvider, err := provider.NewPeerProvider(providerConfig, transferConfig)
	if err != nil {
		t.Fatalf("Failed to create peer provider: %v", err)
	}

	// Test provider properties
	if peerProvider.GetID() != "test-peer" {
		t.Fatalf("Expected provider ID 'test-peer', got %s", peerProvider.GetID())
	}

	if peerProvider.GetName() != "Test Peer Provider" {
		t.Fatalf("Expected provider name 'Test Peer Provider', got %s", peerProvider.GetName())
	}

	// Test that provider has the expected methods
	// Note: Provider interface only has GetName() and GetID() methods
	// Peer-specific methods would need type assertion to access

	// NOTE: Testing actual peer communication would require:
	// 1. Running agent HTTP servers in test environment
	// 2. Setting up mock peer endpoints that serve files
	// 3. Testing file transfer between live agent instances
	// 4. Verifying cache consistency across peer network
}

// TestCacheIntegrationWithP2P tests how the cache system supports P2P transfers
func TestCacheIntegrationWithP2P(t *testing.T) {
	// Create two caches (representing two agents)
	cache1 := createTestCache(t, "agent1")
	cache2 := createTestCache(t, "agent2")

	// Test file sharing between caches
	dataset := "cache-p2p-test"
	fileName := "test-file.bin"
	testData := make([]byte, 1024*1024) // 1MB test file
	for i := range testData {
		testData[i] = byte(i % 256)
	}

	// Store file in cache 1
	fileKey1, err := cache1.StoreFile(dataset, fileName, testData)
	if err != nil {
		t.Fatalf("Failed to store file in cache 1: %v", err)
	}

	// Simulate P2P transfer: retrieve from cache 1 and store in cache 2
	retrievedData, err := cache1.GetFileByPath(dataset, fileName)
	if err != nil {
		t.Fatalf("Failed to retrieve file from cache 1: %v", err)
	}

	fileKey2, err := cache2.StoreFile(dataset, fileName, retrievedData)
	if err != nil {
		t.Fatalf("Failed to store file in cache 2: %v", err)
	}

	// Verify file integrity
	if fileKey1 == fileKey2 {
		t.Log("File keys match - good for deduplication")
	}

	// Verify both caches have the file and content is identical
	data1, err := cache1.GetFileByPath(dataset, fileName)
	if err != nil {
		t.Fatalf("Failed to get file from cache 1: %v", err)
	}

	data2, err := cache2.GetFileByPath(dataset, fileName)
	if err != nil {
		t.Fatalf("Failed to get file from cache 2: %v", err)
	}

	if len(data1) != len(data2) {
		t.Fatalf("File sizes don't match: %d vs %d", len(data1), len(data2))
	}

	for i := range data1 {
		if data1[i] != data2[i] {
			t.Fatalf("File content differs at byte %d", i)
		}
	}

	// Test cache statistics after P2P transfer
	stats1 := cache1.GetCacheStats()
	stats2 := cache2.GetCacheStats()

	if stats1.TotalFiles < 1 || stats2.TotalFiles < 1 {
		t.Fatal("Both caches should have at least 1 file")
	}
}

// TestAgentFileInventory tests tracking what files each agent has
func TestAgentFileInventory(t *testing.T) {
	// Create agent state to track file inventory
	agent1State := types.AgentState{
		DiskUsed:      types.Bytes(0),
		DiskAvailable: types.Bytes(1024 * 1024 * 1024), // 1GB
		ActiveJobs:    []string{},
		LastUpdated:   time.Now(),
	}

	agent2State := types.AgentState{
		DiskUsed:      types.Bytes(0),
		DiskAvailable: types.Bytes(1024 * 1024 * 1024), // 1GB
		ActiveJobs:    []string{},
		LastUpdated:   time.Now(),
	}

	// Create agents
	agent1 := &types.Agent{
		ID:            "agent-1",
		AdvertiseAddr: types.NewAddress("localhost", 8081),
		State:         agent1State,
		LastHeartbeat: time.Now(),
	}

	agent2 := &types.Agent{
		ID:            "agent-2",
		AdvertiseAddr: types.NewAddress("localhost", 8082),
		State:         agent2State,
		LastHeartbeat: time.Now(),
	}

	// Test agent properties
	if !agent1.IsHealthy(time.Minute) {
		t.Fatal("Agent 1 should be healthy")
	}

	if !agent2.IsHealthy(time.Minute) {
		t.Fatal("Agent 2 should be healthy")
	}

	if !agent1.HasCapacity(types.Bytes(100 * 1024 * 1024)) { // 100MB
		t.Fatal("Agent 1 should have capacity for 100MB")
	}

	if !agent1.IsIdle() {
		t.Fatal("Agent 1 should be idle (no active jobs)")
	}

	if agent1.GetLoad() != 0 {
		t.Fatalf("Agent 1 load should be 0, got %d", agent1.GetLoad())
	}

	// Test load tracking
	agent1.State.ActiveJobs = []string{"job-1", "job-2"}
	if agent1.GetLoad() != 2 {
		t.Fatalf("Agent 1 load should be 2, got %d", agent1.GetLoad())
	}

	if agent1.IsIdle() {
		t.Fatal("Agent 1 should not be idle with active jobs")
	}
}

// Helper functions

func createTestCache(t *testing.T, name string) *cache.Cache {
	cacheConfig := config.CacheConfig{
		RAMLimit:  types.Bytes(64 * 1024 * 1024), // 64MB for testing
		DiskLimit: types.Bytes(0),                // Unlimited
	}

	testCache, err := cache.NewCache(cacheConfig, filepath.Join(t.TempDir(), name))
	if err != nil {
		t.Fatalf("Failed to create cache for %s: %v", name, err)
	}

	return testCache
}
