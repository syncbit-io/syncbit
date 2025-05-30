package controller

import (
	"context"
	"testing"
	"time"

	"syncbit/internal/core/logger"
	"syncbit/internal/core/types"
	"syncbit/internal/transport"
)

func TestJobScheduler_BasicScheduling(t *testing.T) {
	controller := &Controller{
		logger:       logger.NewLogger(logger.WithName("scheduling-test")),
		jobs:         make(map[string]*types.Job),
		agents:       make(map[string]*types.Agent),
		httpTransfer: transport.NewHTTPTransfer(),
	}

	// Add test agents
	agents := []*types.Agent{
		{
			ID:            "agent-1",
			AdvertiseAddr: types.NewAddress("localhost", 8081),
			State: types.AgentState{
				DiskAvailable: types.Bytes(50 * 1024 * 1024 * 1024), // 50GB
				ActiveJobs:    []string{},
			},
			LastHeartbeat: time.Now(),
		},
		{
			ID:            "agent-2",
			AdvertiseAddr: types.NewAddress("localhost", 8082),
			State: types.AgentState{
				DiskAvailable: types.Bytes(20 * 1024 * 1024 * 1024), // 20GB
				ActiveJobs:    []string{"job-1"},
			},
			LastHeartbeat: time.Now(),
		},
	}

	for _, agent := range agents {
		controller.RegisterAgent(agent)
	}

	scheduler := NewJobScheduler(controller)

	t.Run("ScheduleDownload", func(t *testing.T) {
		jobs, err := scheduler.ScheduleDownload(
			context.Background(),
			"test/repo",
			"main",
			[]string{"file1.txt", "file2.txt"},
			"hf-public",
			"/tmp/test",
		)

		if err != nil {
			t.Fatalf("Failed to schedule download: %v", err)
		}

		if len(jobs) != 2 {
			t.Fatalf("Expected 2 jobs, got %d", len(jobs))
		}

		// Verify jobs were created with correct configuration
		for i, job := range jobs {
			expectedFile := []string{"file1.txt", "file2.txt"}[i]
			if job.Config.FilePath != expectedFile {
				t.Errorf("Job %d: expected file %s, got %s", i, expectedFile, job.Config.FilePath)
			}
			if job.Config.ProviderSource.ProviderID != "hf-public" {
				t.Errorf("Job %d: expected provider hf-public, got %s", i, job.Config.ProviderSource.ProviderID)
			}
		}
	})

	t.Run("ScheduleDistributedDownload", func(t *testing.T) {
		distribution := types.DistributionRequest{
			Strategy:    "count",
			TargetCount: 2,
		}

		jobs, err := scheduler.ScheduleDistributedDownload(
			context.Background(),
			"test/repo",
			"main",
			[]string{"distributed-file.txt"},
			"hf-public",
			"/tmp/test",
			distribution,
		)

		if err != nil {
			t.Fatalf("Failed to schedule distributed download: %v", err)
		}

		if len(jobs) != 1 {
			t.Fatalf("Expected 1 job, got %d", len(jobs))
		}

		job := jobs[0]
		if job.Config.Distribution.Strategy != "count" {
			t.Errorf("Expected distribution strategy 'count', got %s", job.Config.Distribution.Strategy)
		}
		if job.Config.Distribution.TargetCount != 2 {
			t.Errorf("Expected target count 2, got %d", job.Config.Distribution.TargetCount)
		}
	})
}

func TestJobScheduler_AgentSelection(t *testing.T) {
	controller := &Controller{
		logger:       logger.NewLogger(logger.WithName("selection-test")),
		jobs:         make(map[string]*types.Job),
		agents:       make(map[string]*types.Agent),
		httpTransfer: transport.NewHTTPTransfer(),
	}

	scheduler := NewJobScheduler(controller)

	// Create test agents with different characteristics
	agents := []*types.Agent{
		{
			ID:            "high-capacity",
			AdvertiseAddr: types.NewAddress("localhost", 8081),
			State: types.AgentState{
				DiskAvailable: types.Bytes(50 * 1024 * 1024 * 1024),
				ActiveJobs:    []string{},
			},
			LastHeartbeat: time.Now(),
		},
		{
			ID:            "busy-agent",
			AdvertiseAddr: types.NewAddress("localhost", 8082),
			State: types.AgentState{
				DiskAvailable: types.Bytes(50 * 1024 * 1024 * 1024),
				ActiveJobs:    []string{"job-1", "job-2", "job-3"},
			},
			LastHeartbeat: time.Now(),
		},
		{
			ID:            "low-capacity",
			AdvertiseAddr: types.NewAddress("localhost", 8083),
			State: types.AgentState{
				DiskAvailable: types.Bytes(1 * 1024 * 1024 * 1024),
				ActiveJobs:    []string{},
			},
			LastHeartbeat: time.Now(),
		},
	}

	for _, agent := range agents {
		controller.RegisterAgent(agent)
	}

	// Create a test job
	job := types.NewJob("test-job", types.JobHandlerDownload, types.JobConfig{
		Repo:           "test/repo",
		Revision:       "main",
		FilePath:       "test-file.bin",
		LocalPath:      "/tmp/test",
		ProviderSource: types.ProviderSource{ProviderID: "hf-public"},
		Distribution:   types.DefaultDistributionRequest(),
	})

	// Test agent selection
	selectedAgent, err := scheduler.selectBestAgent(job)
	if err != nil {
		t.Fatalf("Failed to select agent: %v", err)
	}

	// Should select the high-capacity agent with no active jobs
	if selectedAgent.ID != "high-capacity" {
		t.Fatalf("Expected high-capacity agent to be selected, got %s", selectedAgent.ID)
	}

	t.Logf("Selected agent: %s", selectedAgent.ID)
}

func TestJobScheduler_GetJobStats(t *testing.T) {
	controller := &Controller{
		logger:       logger.NewLogger(logger.WithName("stats-test")),
		jobs:         make(map[string]*types.Job),
		agents:       make(map[string]*types.Agent),
		httpTransfer: transport.NewHTTPTransfer(),
	}

	scheduler := NewJobScheduler(controller)

	// Add some test jobs
	jobs := []*types.Job{
		types.NewJob("job-1", types.JobHandlerDownload, types.JobConfig{
			Repo:           "test/repo",
			Revision:       "main",
			FilePath:       "file1.txt",
			ProviderSource: types.ProviderSource{ProviderID: "hf-public"},
		}),
		types.NewJob("job-2", types.JobHandlerDownload, types.JobConfig{
			Repo:           "test/repo",
			Revision:       "main",
			FilePath:       "file2.txt",
			ProviderSource: types.ProviderSource{ProviderID: "hf-public"},
		}),
	}

	jobs[0].SetStatus(types.StatusCompleted, "")
	jobs[1].SetStatus(types.StatusRunning, "")

	for _, job := range jobs {
		controller.jobs[job.ID] = job
	}

	// Test stats retrieval
	stats := scheduler.GetJobStats()

	if stats.TotalJobs != 2 {
		t.Errorf("Expected 2 total jobs, got %d", stats.TotalJobs)
	}
}
