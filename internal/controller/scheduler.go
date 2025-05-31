package controller

import (
	"context"
	"fmt"
	"net/http"
	"sort"
	"time"

	"syncbit/internal/api/request"
	"syncbit/internal/core/logger"
	"syncbit/internal/core/types"
	"syncbit/internal/transport"
)

// JobScheduler handles job assignment and coordination
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

// ScheduleDownload schedules download jobs for a dataset with multiple files
// This creates a download request that will be handled by the reconciliation loop
func (js *JobScheduler) ScheduleDownload(ctx context.Context, repo, revision string, files []string, providerID, localPath string) ([]*types.Job, error) {
	js.logger.Info("Scheduling download", "repo", repo, "revision", revision, "files", len(files), "provider", providerID)

	var createdJobs []*types.Job

	// Create one job per file to represent the desired state
	for _, filePath := range files {
		// Create job to represent desired state (will be managed by reconciliation)
		job := js.createDesiredStateJob(repo, revision, filePath, localPath, providerID, types.DefaultDistributionRequest())
		if job != nil {
			createdJobs = append(createdJobs, job)
		}
	}

	js.logger.Info("Download scheduling completed", "total_jobs", len(createdJobs), "requested_files", len(files))
	return createdJobs, nil
}

// ScheduleDistributedDownload schedules downloads with specific distribution requirements
func (js *JobScheduler) ScheduleDistributedDownload(ctx context.Context, repo, revision string, files []string, providerID, localPath string, distribution types.DistributionRequest) ([]*types.Job, error) {
	js.logger.Info("Scheduling distributed download", "repo", repo, "files", len(files), "distribution", distribution.Strategy)

	var createdJobs []*types.Job

	for _, filePath := range files {
		// Create job to represent desired state with specific distribution
		job := js.createDesiredStateJob(repo, revision, filePath, localPath, providerID, distribution)
		if job != nil {
			createdJobs = append(createdJobs, job)
		}
	}

	js.logger.Info("Distributed download scheduling completed", "total_jobs", len(createdJobs), "distribution", distribution.Strategy)
	return createdJobs, nil
}

// createDesiredStateJob creates a job that represents the desired state for a file
// The reconciliation loop will handle the actual job creation and distribution
func (js *JobScheduler) createDesiredStateJob(repo, revision, filePath, localPath, providerID string, distribution types.DistributionRequest) *types.Job {
	// Create a "desired state" job with upstream provider
	// The reconciliation loop will create actual execution jobs
	providerSource := types.ProviderSource{
		ProviderID: providerID,
	}

	jobConfig := types.JobConfig{
		Repo:           repo,
		Revision:       revision,
		FilePath:       filePath,
		LocalPath:      localPath,
		ProviderSource: providerSource,
		Distribution:   distribution,
	}

	// Generate job ID
	jobID := fmt.Sprintf("desired-%s-%s-%s-%d", repo, revision, filePath, time.Now().Unix())

	// Create job (this represents desired state, not immediate execution)
	job := types.NewJob(jobID, types.JobHandlerDownload, jobConfig)

	// Store job
	js.controller.jobMutex.Lock()
	js.controller.jobs[job.ID] = job
	js.controller.jobMutex.Unlock()

	js.logger.Info("Created desired state job",
		"job_id", job.ID,
		"file", filePath,
		"distribution", distribution.Strategy)

	return job
}

// ScheduleJob schedules a single job (for direct execution)
func (js *JobScheduler) ScheduleJob(ctx context.Context, job *types.Job) error {
	js.logger.Info("Scheduling single job", "job_id", job.ID, "file", job.Config.FilePath)

	// Validate job configuration
	if job.Config.FilePath == "" {
		return fmt.Errorf("job configuration is invalid: no file path specified")
	}

	if job.Config.ProviderSource.ProviderID == "" {
		return fmt.Errorf("job configuration is invalid: no provider source specified")
	}

	// Ensure distribution is set for backward compatibility
	if job.Config.Distribution.Strategy == "" {
		job.Config.Distribution = types.DefaultDistributionRequest()
	}

	// Find the best agent for this job
	targetAgent, err := js.selectBestAgent(job)
	if err != nil {
		return fmt.Errorf("failed to select agent: %w", err)
	}

	// Assign the job to the selected agent
	return js.AssignJobToAgent(ctx, job, targetAgent)
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

// AgentScore represents an agent's suitability for a job
type AgentScore struct {
	Agent     *types.Agent
	Score     float64
	DiskSpace types.Bytes
	Load      int
	LastSeen  time.Duration
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

// AssignJobToAgent assigns a job to a specific agent
func (js *JobScheduler) AssignJobToAgent(ctx context.Context, job *types.Job, agent *types.Agent) error {
	// Send the job to the agent via HTTP
	agentURL := fmt.Sprintf("%s/jobs", agent.AdvertiseAddr.String())

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

	// Use transport HTTP client
	httpTransfer := transport.NewHTTPTransfer()

	err := httpTransfer.Post(ctx, agentURL, func(resp *http.Response) error {
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusAccepted {
			return fmt.Errorf("agent rejected job with status: %d", resp.StatusCode)
		}

		js.logger.Debug("Job successfully sent to agent", "job_id", agentJob.ID, "agent_url", agentURL)
		return nil
	}, request.WithJSON(agentJob))

	if err != nil {
		return fmt.Errorf("failed to send HTTP request: %w", err)
	}

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
		case types.StatusPending:
			stats.PendingJobs++
		case types.StatusRunning:
			stats.RunningJobs++
		case types.StatusCompleted:
			stats.CompletedJobs++
		case types.StatusFailed:
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
