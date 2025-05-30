package controller

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"sort"
	"time"

	"syncbit/internal/core/logger"
	"syncbit/internal/core/types"
)

// JobScheduler handles intelligent job assignment and coordination
type JobScheduler struct {
	controller *Controller
	logger     *logger.Logger
}

// NewJobScheduler creates a new job scheduler
func NewJobScheduler(controller *Controller) *JobScheduler {
	return &JobScheduler{
		controller: controller,
		logger:     controller.logger,
	}
}

// ScheduleJob intelligently schedules a download job to the best available agent
func (js *JobScheduler) ScheduleJob(ctx context.Context, job *types.Job) error {
	js.logger.Info("Scheduling job", "job_id", job.ID, "repo", job.Config.Repo, "files", len(job.Config.Files))

	// 1. Check for duplicate downloads
	if err := js.checkForDuplicateDownloads(job); err != nil {
		return fmt.Errorf("duplicate download check failed: %w", err)
	}

	// 2. Find the best agent for this job
	targetAgent, err := js.selectBestAgent(job)
	if err != nil {
		return fmt.Errorf("failed to select agent: %w", err)
	}

	// 3. Assign the job to the selected agent
	return js.assignJobToAgent(job, targetAgent)
}

// checkForDuplicateDownloads ensures we don't download the same file multiple times
func (js *JobScheduler) checkForDuplicateDownloads(job *types.Job) error {
	js.controller.jobMutex.RLock()
	defer js.controller.jobMutex.RUnlock()

	for _, existingJob := range js.controller.jobs {
		// Skip completed or failed jobs
		if existingJob.IsComplete() {
			continue
		}

		// Check if same repo is being downloaded
		if existingJob.Config.Repo == job.Config.Repo && existingJob.Config.Revision == job.Config.Revision {
			// Check for overlapping files
			existingFiles := make(map[string]bool)
			for _, file := range existingJob.Config.Files {
				existingFiles[file] = true
			}

			for _, file := range job.Config.Files {
				if existingFiles[file] {
					return fmt.Errorf("file %s from repo %s is already being downloaded by job %s",
						file, job.Config.Repo, existingJob.ID)
				}
			}
		}
	}

	return nil
}

// AgentScore represents an agent's suitability for a job
type AgentScore struct {
	Agent     *types.Agent
	Score     float64
	DiskSpace types.Bytes
	Load      int
	LastSeen  time.Duration
}

// selectBestAgent chooses the optimal agent for a download job
func (js *JobScheduler) selectBestAgent(job *types.Job) (*types.Agent, error) {
	js.controller.agentMutex.RLock()
	defer js.controller.agentMutex.RUnlock()

	if len(js.controller.agents) == 0 {
		return nil, fmt.Errorf("no agents available")
	}

	var candidates []AgentScore
	now := time.Now()

	// Score all available agents
	for agentID, agent := range js.controller.agents {
		// Skip agents that haven't been seen recently
		lastSeen := now.Sub(agent.LastHeartbeat)
		if lastSeen > 5*time.Minute {
			js.logger.Debug("Skipping stale agent", "agent_id", agentID, "last_seen", lastSeen)
			continue
		}

		score := js.calculateAgentScore(agent, job, lastSeen)
		candidates = append(candidates, score)
	}

	if len(candidates) == 0 {
		return nil, fmt.Errorf("no healthy agents available")
	}

	// Sort by score (higher is better)
	sort.Slice(candidates, func(i, j int) bool {
		return candidates[i].Score > candidates[j].Score
	})

	best := candidates[0]
	js.logger.Info("Selected agent for job",
		"job_id", job.ID,
		"agent_id", best.Agent.ID,
		"score", best.Score,
		"disk_space", best.DiskSpace,
		"load", best.Load,
	)

	return best.Agent, nil
}

// calculateAgentScore computes a score for how suitable an agent is for a job
func (js *JobScheduler) calculateAgentScore(agent *types.Agent, job *types.Job, lastSeen time.Duration) AgentScore {
	score := AgentScore{
		Agent:     agent,
		DiskSpace: agent.State.DiskAvailable,
		Load:      agent.GetLoad(),
		LastSeen:  lastSeen,
	}

	// Base score starts at 100
	score.Score = 100.0

	// Factor 1: Available disk space (higher is better)
	if score.DiskSpace > types.Bytes(10*1024*1024*1024) { // > 10GB
		score.Score += 20.0
	} else if score.DiskSpace > types.Bytes(1*1024*1024*1024) { // > 1GB
		score.Score += 10.0
	} else if score.DiskSpace < types.Bytes(100*1024*1024) { // < 100MB
		score.Score -= 30.0
	}

	// Factor 2: Active job load (fewer is better)
	score.Score -= float64(score.Load) * 15.0

	// Factor 3: How recently we've seen this agent (more recent is better)
	if lastSeen < 30*time.Second {
		score.Score += 15.0
	} else if lastSeen < 2*time.Minute {
		score.Score += 5.0
	} else {
		score.Score -= 10.0
	}

	return score
}

// assignJobToAgent assigns a job to a specific agent
func (js *JobScheduler) assignJobToAgent(job *types.Job, agent *types.Agent) error {
	// Send the job to the agent via HTTP
	agentURL := fmt.Sprintf("%s/jobs", agent.AdvertiseAddr.URL())

	ctx := context.Background()
	if err := js.sendJobToAgent(ctx, agentURL, job); err != nil {
		return fmt.Errorf("failed to send job to agent %s: %w", agent.ID, err)
	}

	js.logger.Info("Job assigned to agent", "job_id", job.ID, "agent_id", agent.ID)
	return nil
}

// sendJobToAgent sends a job to an agent via HTTP
func (js *JobScheduler) sendJobToAgent(ctx context.Context, agentURL string, job *types.Job) error {
	// Create a simplified job structure for sending to the agent
	agentJob := struct {
		ID      string           `json:"id"`
		Handler types.JobHandler `json:"handler"`
		Config  types.JobConfig  `json:"config"`
	}{
		ID:      job.ID,
		Handler: job.Handler,
		Config:  job.Config,
	}

	httpClient := &http.Client{Timeout: 30 * time.Second}

	jsonData, err := json.Marshal(agentJob)
	if err != nil {
		return fmt.Errorf("failed to marshal job: %w", err)
	}

	resp, err := httpClient.Post(agentURL, "application/json", bytes.NewBuffer(jsonData))
	if err != nil {
		return fmt.Errorf("failed to send HTTP request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusAccepted {
		return fmt.Errorf("agent rejected job with status: %d", resp.StatusCode)
	}

	js.logger.Debug("Job successfully sent to agent", "job_id", agentJob.ID, "agent_url", agentURL)
	return nil
}

// GetJobStats returns statistics about job scheduling
func (js *JobScheduler) GetJobStats() JobSchedulerStats {
	js.controller.jobMutex.RLock()
	defer js.controller.jobMutex.RUnlock()

	stats := JobSchedulerStats{}

	for _, job := range js.controller.jobs {
		stats.TotalJobs++
		switch job.Status {
		case types.JobStatusPending:
			stats.PendingJobs++
		case types.JobStatusRunning:
			stats.RunningJobs++
		case types.JobStatusCompleted:
			stats.CompletedJobs++
		case types.JobStatusFailed:
			stats.FailedJobs++
		}
	}

	js.controller.agentMutex.RLock()
	stats.ActiveAgents = len(js.controller.agents)
	for _, agent := range js.controller.agents {
		if agent.IsHealthy(2 * time.Minute) {
			stats.HealthyAgents++
		}
	}
	js.controller.agentMutex.RUnlock()

	return stats
}

// JobSchedulerStats represents statistics about the job scheduler
type JobSchedulerStats struct {
	TotalJobs     int `json:"total_jobs"`
	PendingJobs   int `json:"pending_jobs"`
	RunningJobs   int `json:"running_jobs"`
	CompletedJobs int `json:"completed_jobs"`
	FailedJobs    int `json:"failed_jobs"`
	ActiveAgents  int `json:"active_agents"`
	HealthyAgents int `json:"healthy_agents"`
}
