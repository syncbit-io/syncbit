package agent

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"syncbit/internal/cache"
	"syncbit/internal/config"
	"syncbit/internal/core/logger"
	"syncbit/internal/core/types"
	"syncbit/internal/runner"
	"syncbit/internal/transport"
)

// TestAgent_JobProcessing tests the agent's ability to process jobs
func TestAgent_JobProcessing(t *testing.T) {
	agent := createTestAgent(t)

	// Create a test job
	testJob := &types.Job{
		ID:      "test-job-1",
		Handler: types.JobHandlerDownload,
		Config: types.JobConfig{
			Repo:      "test/repo",
			Revision:  "main",
			FilePath:  "config.json",
			LocalPath: "/tmp/test",
			ProviderSource: types.ProviderSource{
				ProviderID: "hf-public",
			},
		},
		Status: types.StatusPending,
	}

	// Submit job via API
	jobJSON, _ := json.Marshal(testJob)
	req := httptest.NewRequest("POST", "/jobs", strings.NewReader(string(jobJSON)))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()

	agent.handleSubmitJob(w, req)

	// Check response
	if w.Code != http.StatusAccepted {
		t.Fatalf("Expected status 202, got %d", w.Code)
	}

	// Verify job is tracked
	agent.jobMutex.RLock()
	trackedJob, exists := agent.jobs[testJob.ID]
	agent.jobMutex.RUnlock()

	if !exists {
		t.Fatal("Job not found in agent's job map")
	}

	// Job might be pending or running depending on timing
	if trackedJob.Status != types.StatusPending && trackedJob.Status != types.StatusRunning {
		t.Fatalf("Expected pending or running status, got %s", trackedJob.Status)
	}
}

// TestAgent_JobStatusUpdates tests job status tracking
func TestAgent_JobStatusUpdates(t *testing.T) {
	agent := createTestAgent(t)

	// Create and add a job
	job := &types.Job{
		ID:      "status-test-1",
		Handler: types.JobHandlerDownload,
		Config: types.JobConfig{
			Repo:      "test/repo",
			Revision:  "main",
			FilePath:  "test.txt",
			LocalPath: "/tmp/test",
			ProviderSource: types.ProviderSource{
				ProviderID: "hf-public",
			},
		},
		Status: types.StatusPending,
	}

	agent.jobMutex.Lock()
	agent.jobs[job.ID] = job
	agent.jobMutex.Unlock()

	// Test getting job status
	req := httptest.NewRequest("GET", "/jobs/status-test-1", nil)
	// Set the path value manually since we're not going through the router
	req.SetPathValue("id", "status-test-1")
	w := httptest.NewRecorder()

	agent.handleGetJob(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("Expected status 200, got %d", w.Code)
	}

	var response map[string]interface{}
	if err := json.Unmarshal(w.Body.Bytes(), &response); err != nil {
		t.Fatalf("Failed to parse response: %v", err)
	}

	if response["job_id"] != job.ID {
		t.Fatalf("Expected job ID %s, got %v", job.ID, response["job_id"])
	}

	if response["status"] != string(types.StatusPending) {
		t.Fatalf("Expected pending status, got %v", response["status"])
	}
}

// TestAgent_JobCancellation tests job cancellation
func TestAgent_JobCancellation(t *testing.T) {
	agent := createTestAgent(t)

	// Create and add a job
	job := &types.Job{
		ID:      "cancel-test-1",
		Handler: types.JobHandlerDownload,
		Config: types.JobConfig{
			Repo:      "test/repo",
			Revision:  "main",
			FilePath:  "test.txt",
			LocalPath: "/tmp/test",
			ProviderSource: types.ProviderSource{
				ProviderID: "hf-public",
			},
		},
		Status: types.StatusRunning,
	}

	agent.jobMutex.Lock()
	agent.jobs[job.ID] = job
	// Create a mock cancel function
	ctx, cancel := context.WithCancel(context.Background())
	agent.jobCancelFuncs[job.ID] = cancel
	agent.jobMutex.Unlock()

	// Test cancellation
	req := httptest.NewRequest("DELETE", "/jobs/cancel-test-1", nil)
	req.SetPathValue("id", "cancel-test-1")
	w := httptest.NewRecorder()

	agent.handleCancelJob(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("Expected status 200, got %d", w.Code)
	}

	// Verify job was marked as cancelled
	agent.jobMutex.RLock()
	cancelledJob := agent.jobs[job.ID]
	agent.jobMutex.RUnlock()

	if cancelledJob.Status != types.StatusCancelled {
		t.Fatalf("Expected cancelled status, got %s", cancelledJob.Status)
	}

	// Verify context was cancelled
	select {
	case <-ctx.Done():
		// Good, context was cancelled
	default:
		t.Fatal("Context should have been cancelled")
	}
}

// TestAgent_FileServing tests the agent's ability to serve files to peers
func TestAgent_FileServing(t *testing.T) {
	agent := createTestAgent(t)

	// Store a test file in the cache
	testData := []byte("test file content for peer sharing")
	dataset := "test-dataset"
	filePath := "test-file.txt"

	_, err := agent.cache.StoreFile(dataset, filePath, testData)
	if err != nil {
		t.Fatalf("Failed to store test file: %v", err)
	}

	// Test file retrieval endpoint
	req := httptest.NewRequest("GET", "/datasets/test-dataset/files/test-file.txt", nil)
	req.SetPathValue("name", "test-dataset")
	req.SetPathValue("path", "test-file.txt")
	w := httptest.NewRecorder()

	agent.handleGetFile(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("Expected status 200, got %d", w.Code)
	}

	retrievedData := w.Body.Bytes()
	if string(retrievedData) != string(testData) {
		t.Fatalf("Retrieved data doesn't match stored data")
	}
}

// TestAgent_FileInfo tests file information retrieval
func TestAgent_FileInfo(t *testing.T) {
	agent := createTestAgent(t)

	// Store a test file
	testData := []byte("test file for info endpoint")
	dataset := "info-dataset"
	filePath := "info-file.txt"

	_, err := agent.cache.StoreFile(dataset, filePath, testData)
	if err != nil {
		t.Fatalf("Failed to store test file: %v", err)
	}

	// Test file info endpoint
	req := httptest.NewRequest("GET", "/datasets/info-dataset/files/info-file.txt/info", nil)
	req.SetPathValue("name", "info-dataset")
	req.SetPathValue("path", "info-file.txt")
	w := httptest.NewRecorder()

	agent.handleGetFileInfo(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("Expected status 200, got %d", w.Code)
	}

	var response map[string]interface{}
	if err := json.Unmarshal(w.Body.Bytes(), &response); err != nil {
		t.Fatalf("Failed to parse response: %v", err)
	}

	fileInfo := response["file"].(map[string]interface{})
	if fileInfo["path"] != filePath {
		t.Fatalf("Expected file path %s, got %v", filePath, fileInfo["path"])
	}

	if int(fileInfo["size"].(float64)) != len(testData) {
		t.Fatalf("Expected file size %d, got %v", len(testData), fileInfo["size"])
	}
}

// TestAgent_DatasetListing tests listing available datasets
func TestAgent_DatasetListing(t *testing.T) {
	agent := createTestAgent(t)

	// Store files in multiple datasets
	agent.cache.StoreFile("dataset1", "file1.txt", []byte("data1"))
	agent.cache.StoreFile("dataset1", "file2.txt", []byte("data2"))
	agent.cache.StoreFile("dataset2", "file3.txt", []byte("data3"))

	// Test datasets listing endpoint
	req := httptest.NewRequest("GET", "/datasets", nil)
	w := httptest.NewRecorder()

	agent.handleListDatasets(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("Expected status 200, got %d", w.Code)
	}

	var response map[string]interface{}
	if err := json.Unmarshal(w.Body.Bytes(), &response); err != nil {
		t.Fatalf("Failed to parse response: %v", err)
	}

	datasets := response["datasets"].([]interface{})
	if len(datasets) < 2 {
		t.Fatalf("Expected at least 2 datasets, got %d", len(datasets))
	}
}

// TestAgent_FileListing tests listing files within a dataset
func TestAgent_FileListing(t *testing.T) {
	agent := createTestAgent(t)

	// Store multiple files in a dataset
	dataset := "file-list-dataset"
	agent.cache.StoreFile(dataset, "file1.txt", []byte("data1"))
	agent.cache.StoreFile(dataset, "file2.txt", []byte("data2"))
	agent.cache.StoreFile(dataset, "subdir/file3.txt", []byte("data3"))

	// Test files listing endpoint
	req := httptest.NewRequest("GET", "/datasets/file-list-dataset/files", nil)
	req.SetPathValue("name", "file-list-dataset")
	w := httptest.NewRecorder()

	agent.handleListFiles(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("Expected status 200, got %d", w.Code)
	}

	var response map[string]interface{}
	if err := json.Unmarshal(w.Body.Bytes(), &response); err != nil {
		t.Fatalf("Failed to parse response: %v", err)
	}

	files := response["files"].([]interface{})
	if len(files) < 3 {
		t.Fatalf("Expected at least 3 files, got %d", len(files))
	}
}

// TestAgent_HealthCheck tests the health endpoint
func TestAgent_HealthCheck(t *testing.T) {
	agent := createTestAgent(t)

	req := httptest.NewRequest("GET", "/health", nil)
	w := httptest.NewRecorder()

	agent.handleHealth(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("Expected status 200, got %d", w.Code)
	}

	var response map[string]interface{}
	if err := json.Unmarshal(w.Body.Bytes(), &response); err != nil {
		t.Fatalf("Failed to parse response: %v", err)
	}

	if response["status"] != "healthy" {
		t.Fatalf("Expected healthy status, got %v", response["status"])
	}
}

// TestAgent_StateReporting tests agent state endpoint
func TestAgent_StateReporting(t *testing.T) {
	agent := createTestAgent(t)

	req := httptest.NewRequest("GET", "/state", nil)
	w := httptest.NewRecorder()

	agent.handleGetState(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("Expected status 200, got %d", w.Code)
	}

	var response map[string]interface{}
	if err := json.Unmarshal(w.Body.Bytes(), &response); err != nil {
		t.Fatalf("Failed to parse response: %v", err)
	}

	// Check that required fields are present
	state := response["state"].(map[string]interface{})
	if _, exists := state["disk_available"]; !exists {
		t.Fatal("Response missing disk_available field")
	}

	if _, exists := state["active_jobs"]; !exists {
		t.Fatal("Response missing active_jobs field")
	}
}

// TestAgent_DownloadWithPeerSource tests downloading a job with a peer source
func TestAgent_DownloadWithPeerSource(t *testing.T) {
	agent := createTestAgent(t)

	// Create a download job with peer source (unified download system)
	job := &types.Job{
		ID:      "peer-test-1",
		Handler: types.JobHandlerDownload,
		Config: types.JobConfig{
			Repo:      "test/peer-repo",
			Revision:  "main",
			FilePath:  "test-peer-file.txt",
			LocalPath: "/tmp/peer-test",
			ProviderSource: types.ProviderSource{
				ProviderID: "peer-main",
				PeerAddr:   types.NewAddress("localhost", 8081),
			},
		},
		Status: types.StatusPending,
	}

	// Create the runner job
	runnerJob := agent.createDownloadJob(job)

	// Verify the job was created
	if runnerJob == nil {
		t.Fatal("Expected runner job to be created")
	}

	// Test job name matches
	if runnerJob.Name() != job.ID {
		t.Fatalf("Expected job name %s, got %s", job.ID, runnerJob.Name())
	}
}

// Helper functions for testing

func createTestAgent(t *testing.T) *Agent {
	// Create test cache
	cacheConfig := config.CacheConfig{
		RAMLimit:  types.Bytes(64 * 1024 * 1024), // 64MB for testing
		DiskLimit: types.Bytes(0),                // Unlimited
	}

	testCache, err := cache.NewCache(cacheConfig, t.TempDir())
	if err != nil {
		t.Fatalf("Failed to create test cache: %v", err)
	}

	// Create test agent config
	agentConfig := config.AgentConfig{
		ID: "test-agent",
		Storage: config.StorageConfig{
			BasePath: t.TempDir(),
			Cache:    cacheConfig,
		},
		Network: config.NetworkConfig{
			ListenAddr:        types.NewAddress("localhost", 8081),
			AdvertiseAddr:     types.NewAddress("localhost", 8081),
			HeartbeatInterval: "30s",
			PeerTimeout:       "30s",
		},
	}

	agent := &Agent{
		cfg:            &agentConfig,
		controllerAddr: types.NewAddress("localhost", 8080),
		log:            logger.NewLogger(logger.WithName("test-agent")),
		httpClient:     transport.NewHTTPTransfer(),
		cache:          testCache,
		jobs:           make(map[string]*types.Job),
		jobCancelFuncs: make(map[string]context.CancelFunc),
	}

	// Initialize job pool for testing
	ctx := context.Background()
	agent.pool = runner.NewPool(ctx, "test-pool",
		runner.WithPoolLogger(agent.log),
		runner.WithPoolWorkers(1),
	)

	return agent
}
