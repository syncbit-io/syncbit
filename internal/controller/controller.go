package controller

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"sort"
	"strings"
	"sync"
	"time"

	"syncbit/internal/api"
	"syncbit/internal/cache"
	"syncbit/internal/config"
	"syncbit/internal/core/logger"
	"syncbit/internal/core/types"
	"syncbit/internal/runner"
	"syncbit/internal/transport"
)

type ControllerOption func(*Controller)

// WithLogger sets the logger for the controller.
func WithLogger(logger *logger.Logger) ControllerOption {
	return func(c *Controller) {
		c.logger = logger
	}
}

// WithServer sets the server for the controller.
func WithServer(server *api.Server) ControllerOption {
	return func(c *Controller) {
		c.server = server
	}
}

// WithCache sets the cache for the controller.
func WithCache(cache *cache.Cache) ControllerOption {
	return func(c *Controller) {
		c.cache = cache
	}
}

// Controller is the main controller for the application.
type Controller struct {
	cfg          *types.ControllerConfig
	logger       *logger.Logger
	server       *api.Server
	pool         *runner.Pool
	httpTransfer *transport.HTTPTransfer

	// Agents
	agents     map[string]*types.Agent
	agentMutex sync.RWMutex

	// Jobs
	jobs     map[string]*types.Job
	jobMutex sync.RWMutex

	// Job scheduling
	scheduler *JobScheduler
	cache     *cache.Cache // Cache instance for tracking cache state
}

// NewController creates a new controller with the given options.
func NewController(configFile string, debug bool, opts ...ControllerOption) *Controller {
	// Load configuration
	cfg, err := config.LoadConfig(configFile)
	if err != nil {
		// Use defaults if config load fails
		cfg = &types.Config{
			Debug:      debug,
			Controller: &types.ControllerConfig{},
		}
	}

	// Apply debug flag
	if debug {
		cfg.Debug = true
	}

	// Ensure we have controller config
	if cfg.Controller == nil {
		defaults := types.DefaultControllerConfig()
		cfg.Controller = &defaults
	}

	// Use listen address from config, or default
	listenAddr := cfg.Controller.ListenAddr
	if listenAddr == nil {
		listenAddr, _ = url.Parse("http://0.0.0.0:8080")
	}

	c := &Controller{
		logger:       logger.NewLogger(logger.WithName("controller")),
		server:       api.NewServer(api.WithListen(listenAddr)),
		cfg:          cfg.Controller,
		httpTransfer: transport.NewHTTPTransfer(),
		jobs:         make(map[string]*types.Job),
		agents:       make(map[string]*types.Agent),
	}

	for _, opt := range opts {
		opt(c)
	}

	// Initialize the scheduler
	c.scheduler = NewJobScheduler(c)

	return c
}

// SubmitJob adds a new job to the controller
func (c *Controller) SubmitJob(ctx context.Context, job *types.Job) error {
	job.SetStatus(types.StatusPending, "")

	// Use scheduler to intelligently assign the job
	if err := c.scheduler.ScheduleJob(ctx, job); err != nil {
		c.logger.Error("Failed to schedule job", "job_id", job.ID, "error", err)
		job.SetStatus(types.StatusFailed, fmt.Sprintf("scheduling failed: %v", err))
		return err
	}

	// Store job after successful scheduling
	c.jobMutex.Lock()
	c.jobs[job.ID] = job
	c.jobMutex.Unlock()

	// Create a runner job for job distribution tracking
	runnerJob := runner.NewJob(job.ID, func(ctx context.Context, rjob *runner.Job) error {
		c.logger.Debug("Job ready for agent pickup", "job_id", job.ID)
		return nil
	})

	// Initialize job pool if not already done
	if c.pool == nil {
		c.pool = runner.NewPool(ctx, "controller-jobs",
			runner.WithPoolLogger(c.logger),
			runner.WithPoolQueueSize(1000),
			runner.WithPoolWorkers(5),
		)
	}

	// Submit to pool
	if err := c.pool.Submit(runnerJob); err != nil {
		c.logger.Error("Failed to submit job to pool", "job_id", job.ID, "error", err)
		job.SetStatus(types.StatusFailed, "failed to queue job")
		return err
	}

	c.logger.Info("Job submitted and scheduled", "job_id", job.ID, "handler", job.Handler)
	return nil
}

// SubmitDownload schedules downloads for multiple files from a repository
func (c *Controller) SubmitDownload(ctx context.Context, repo, revision string, files []string, providerID, localPath string) ([]*types.Job, error) {
	// Use scheduler to create and assign individual jobs per file
	jobs, err := c.scheduler.ScheduleDownload(ctx, repo, revision, files, providerID, localPath)
	if err != nil {
		c.logger.Error("Failed to schedule download", "repo", repo, "error", err)
		return nil, err
	}

	c.logger.Info("Download submitted", "repo", repo, "jobs_created", len(jobs), "files_requested", len(files))
	return jobs, nil
}

// SubmitDistributedDownload schedules downloads with specific distribution requirements
func (c *Controller) SubmitDistributedDownload(ctx context.Context, repo, revision string, files []string, providerID, localPath string, distribution types.DistributionRequest) ([]*types.Job, error) {
	// Use scheduler to create and assign jobs with distribution requirements
	jobs, err := c.scheduler.ScheduleDistributedDownload(ctx, repo, revision, files, providerID, localPath, distribution)
	if err != nil {
		c.logger.Error("Failed to schedule distributed download", "repo", repo, "distribution", distribution.Strategy, "error", err)
		return nil, err
	}

	c.logger.Info("Distributed download submitted",
		"repo", repo,
		"jobs_created", len(jobs),
		"files_requested", len(files),
		"distribution_strategy", distribution.Strategy)
	return jobs, nil
}

// GetJob retrieves a job by ID
func (c *Controller) GetJob(jobID string) (*types.Job, bool) {
	c.jobMutex.RLock()
	defer c.jobMutex.RUnlock()

	job, exists := c.jobs[jobID]
	return job, exists
}

// GetNextJob retrieves the next job from available pending jobs
func (c *Controller) GetNextJob() (*types.Job, error) {
	c.jobMutex.Lock()
	defer c.jobMutex.Unlock()

	// Find a pending job
	for _, job := range c.jobs {
		if job.Status == types.StatusPending {
			job.SetStatus(types.StatusRunning, "")
			c.logger.Info("Job assigned to agent", "job_id", job.ID)
			return job, nil
		}
	}

	// No jobs available
	return nil, nil
}

// UpdateJobStatus updates the status of a job
func (c *Controller) UpdateJobStatus(jobID string, status types.Status, errorMsg string) error {
	c.jobMutex.Lock()
	defer c.jobMutex.Unlock()

	job, exists := c.jobs[jobID]
	if !exists {
		c.logger.Error("Job not found", "job_id", jobID)
		return nil
	}

	job.SetStatus(status, errorMsg)
	c.logger.Info("Job status updated", "job_id", jobID, "status", status)
	return nil
}

// ListJobs returns all jobs
func (c *Controller) ListJobs() []*types.Job {
	c.jobMutex.RLock()
	defer c.jobMutex.RUnlock()

	jobs := make([]*types.Job, 0, len(c.jobs))
	for _, job := range c.jobs {
		jobs = append(jobs, job)
	}
	return jobs
}

// Run starts the controller and blocks until the server is stopped
func (c *Controller) Run(ctx context.Context) error {
	// Initialize job pool
	c.pool = runner.NewPool(ctx, "controller-jobs",
		runner.WithPoolLogger(c.logger),
		runner.WithPoolQueueSize(1000),
		runner.WithPoolWorkers(5),
	)

	// Start background goroutines
	go c.cleanupLoop(ctx)
	go c.reconciliationLoop(ctx)

	if err := c.RegisterHandlers(c.server); err != nil {
		return err
	}

	c.logger.Info("Controller started")
	return c.server.Run(ctx)
}

// cleanupLoop runs periodic cleanup tasks
func (c *Controller) cleanupLoop(ctx context.Context) {
	ticker := time.NewTicker(60 * time.Second) // Cleanup every minute
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			// Cleanup stale agents (haven't sent heartbeat in 5 minutes)
			c.CleanupStaleAgents(5 * time.Minute)
		}
	}
}

// reconciliationLoop runs periodic reconciliation tasks
func (c *Controller) reconciliationLoop(ctx context.Context) {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			c.performReconciliation(ctx)
		}
	}
}

// performReconciliation performs the actual reconciliation logic
func (c *Controller) performReconciliation(ctx context.Context) {
	c.logger.Debug("Starting reconciliation cycle")

	// Get all healthy agents
	healthyAgents := c.getHealthyAgents()
	if len(healthyAgents) == 0 {
		c.logger.Debug("No healthy agents available for reconciliation")
		return
	}

	// Get current state: what files are where, what jobs are active
	clusterState := c.buildClusterState(ctx, healthyAgents)

	// Analyze cluster state and create needed jobs
	jobsCreated := c.reconcileClusterState(ctx, clusterState)

	c.logger.Debug("Reconciliation cycle completed",
		"agents_checked", len(healthyAgents),
		"jobs_created", jobsCreated)
}

// ClusterState represents the current state of the cluster from controller perspective
type ClusterState struct {
	Agents        map[string]*types.Agent // agent_id -> agent
	FileLocations map[string][]string     // file_key -> []agent_id (agents that have the file)
	ActiveJobs    map[string]*types.Job   // job_id -> job (active jobs)
	JobsByFile    map[string][]*types.Job // file_key -> []job (active jobs for each file)
	AgentJobs     map[string][]string     // agent_id -> []job_id (active jobs per agent)
}

// buildClusterState builds a snapshot of current cluster state
func (c *Controller) buildClusterState(ctx context.Context, agents []*types.Agent) *ClusterState {
	state := &ClusterState{
		Agents:        make(map[string]*types.Agent),
		FileLocations: make(map[string][]string),
		ActiveJobs:    make(map[string]*types.Job),
		JobsByFile:    make(map[string][]*types.Job),
		AgentJobs:     make(map[string][]string),
	}

	// Index agents
	for _, agent := range agents {
		state.Agents[agent.ID] = agent
		state.AgentJobs[agent.ID] = []string{}
	}

	// Query file availability from all agents
	for _, agent := range agents {
		availability, err := c.queryAgentFileAvailabilityFull(ctx, agent.ID)
		if err != nil {
			c.logger.Debug("Failed to query file availability from agent",
				"agent_id", agent.ID, "error", err)
			continue
		}

		// Index file locations
		for dataset, files := range availability {
			for filepath, available := range files {
				if available {
					fileKey := fmt.Sprintf("%s/%s", dataset, filepath)
					state.FileLocations[fileKey] = append(state.FileLocations[fileKey], agent.ID)
				}
			}
		}
	}

	// Index active jobs
	c.jobMutex.RLock()
	for jobID, job := range c.jobs {
		if job.IsActive() {
			state.ActiveJobs[jobID] = job
			fileKey := job.GetFileKey()
			state.JobsByFile[fileKey] = append(state.JobsByFile[fileKey], job)

			// Find which agent this job is assigned to (from job assignment tracking)
			// For now, we'll determine this from job name patterns or add agent tracking to jobs
			// This is a simplified approach - in practice we'd track job assignments better
		}
	}
	c.jobMutex.RUnlock()

	return state
}

// reconcileClusterState analyzes cluster state and creates jobs to reach desired state
func (c *Controller) reconcileClusterState(ctx context.Context, state *ClusterState) int {
	jobsCreated := 0

	// For each file that should exist in the cluster (based on distribution requests),
	// ensure the right agents have it or are downloading it

	// First, collect all unique files that are mentioned in any active or completed jobs
	// This represents the "desired state" - files that have been requested for download
	desiredFiles := make(map[string]types.DistributionRequest) // file_key -> distribution

	c.jobMutex.RLock()
	for _, job := range c.jobs {
		fileKey := job.GetFileKey()
		// Use the distribution from the most recent job for this file
		desiredFiles[fileKey] = job.Config.Distribution
	}
	c.jobMutex.RUnlock()

	// For each desired file, ensure proper distribution
	for fileKey, distribution := range desiredFiles {
		created := c.reconcileFileDistribution(ctx, state, fileKey, distribution)
		jobsCreated += created
	}

	return jobsCreated
}

// reconcileFileDistribution ensures a specific file is properly distributed according to its distribution strategy
func (c *Controller) reconcileFileDistribution(ctx context.Context, state *ClusterState, fileKey string, distribution types.DistributionRequest) int {
	jobsCreated := 0

	// Determine which agents should have this file
	targetAgents := c.selectTargetAgentsForFile(state, distribution)
	if len(targetAgents) == 0 {
		return 0
	}

	// Get agents that currently have the file
	agentsWithFile := state.FileLocations[fileKey]
	agentsWithFileSet := make(map[string]bool)
	for _, agentID := range agentsWithFile {
		agentsWithFileSet[agentID] = true
	}

	// Get active jobs for this file
	activeJobsForFile := state.JobsByFile[fileKey]
	agentsWithActiveJobs := make(map[string]bool)
	for _, job := range activeJobsForFile {
		// We need a way to determine which agent a job is assigned to
		// For now, let's extract from job naming patterns or add tracking
		agentID := c.extractAgentFromJobID(job.ID)
		if agentID != "" {
			agentsWithActiveJobs[agentID] = true
		}
	}

	// For each target agent, ensure they have the file or an active job to get it
	for _, targetAgent := range targetAgents {
		agentID := targetAgent.ID

		// Skip if agent already has the file
		if agentsWithFileSet[agentID] {
			continue
		}

		// Skip if agent already has an active job for this file
		if agentsWithActiveJobs[agentID] {
			continue
		}

		// Create a job for this agent to get this file
		job := c.createJobForFile(ctx, fileKey, targetAgent, state, distribution)
		if job != nil {
			c.logger.Info("Created reconciliation job",
				"job_id", job.ID,
				"file", fileKey,
				"agent", agentID,
				"provider", job.Config.ProviderSource.ProviderID)
			jobsCreated++
		}
	}

	return jobsCreated
}

// selectTargetAgentsForFile determines which agents should have a file based on distribution strategy
func (c *Controller) selectTargetAgentsForFile(state *ClusterState, distribution types.DistributionRequest) []*types.Agent {
	var allAgents []*types.Agent
	for _, agent := range state.Agents {
		allAgents = append(allAgents, agent)
	}

	switch distribution.Strategy {
	case "single":
		if len(allAgents) > 0 {
			// Select best agent (could use scoring logic)
			return []*types.Agent{allAgents[0]}
		}
		return nil

	case "all":
		return allAgents

	case "count":
		count := distribution.TargetCount
		if count <= 0 || count > len(allAgents) {
			count = len(allAgents)
		}
		if count <= len(allAgents) {
			return allAgents[:count]
		}
		return allAgents

	case "specific":
		var selected []*types.Agent
		for _, targetID := range distribution.TargetAgents {
			if agent, exists := state.Agents[targetID]; exists {
				selected = append(selected, agent)
			}
		}
		return selected

	default:
		return nil
	}
}

// createJobForFile creates a job for an agent to download a specific file
func (c *Controller) createJobForFile(ctx context.Context, fileKey string, targetAgent *types.Agent, state *ClusterState, distribution types.DistributionRequest) *types.Job {
	// Parse file key to extract repo, revision, filepath
	parts := strings.Split(fileKey, "|")
	if len(parts) != 3 {
		c.logger.Error("Invalid file key format", "file_key", fileKey)
		return nil
	}

	repo := parts[0]
	revision := parts[1]
	filePath := parts[2]

	// Determine provider: peer if any agent has the file, upstream otherwise
	var providerSource types.ProviderSource
	agentsWithFile := state.FileLocations[fileKey]

	if len(agentsWithFile) > 0 {
		// Use peer provider - select best source agent (exclude target agent from being a source for itself)
		var availableSources []string
		for _, sourceAgentID := range agentsWithFile {
			if sourceAgentID != targetAgent.ID {
				availableSources = append(availableSources, sourceAgentID)
			}
		}

		if len(availableSources) > 0 {
			// Select the first available source agent (could add better selection logic here)
			sourceAgentID := availableSources[0]
			sourceAgent := state.Agents[sourceAgentID]
			if sourceAgent != nil {
				providerSource = types.ProviderSource{
					ProviderID: "peer-main",
					PeerAddr:   sourceAgent.AdvertiseAddr,
				}
				c.logger.Debug("Using peer provider for reconciliation job",
					"file_key", fileKey,
					"target_agent", targetAgent.ID,
					"source_agent", sourceAgentID)
			} else {
				// Fallback to upstream if source agent is not available
				providerSource = types.ProviderSource{
					ProviderID: "hf-public", // Default upstream provider
				}
				c.logger.Debug("Source agent not available, using upstream provider",
					"file_key", fileKey,
					"target_agent", targetAgent.ID)
			}
		} else {
			// Target agent already has the file, skip creating job
			c.logger.Debug("Target agent already has file, skipping reconciliation job",
				"file_key", fileKey,
				"target_agent", targetAgent.ID)
			return nil
		}
	} else {
		// No agents have the file, use upstream provider
		providerSource = types.ProviderSource{
			ProviderID: "hf-public", // Default upstream provider
		}
		c.logger.Debug("No agents have file, using upstream provider",
			"file_key", fileKey,
			"target_agent", targetAgent.ID)
	}

	// Create job config
	jobConfig := types.JobConfig{
		Repo:           repo,
		Revision:       revision,
		FilePath:       filePath,
		LocalPath:      "/tmp/syncbit-downloads", // Default local path
		ProviderSource: providerSource,
		Distribution:   distribution,
	}

	// Generate job ID (URL-safe)
	jobID := fmt.Sprintf("reconcile-%s-%s-%s-%s-%d",
		strings.ReplaceAll(repo, "/", "-"),
		revision,
		strings.ReplaceAll(filePath, "/", "-"),
		targetAgent.ID,
		time.Now().Unix())

	// Create and submit job
	job := types.NewJob(jobID, types.JobHandlerDownload, jobConfig)

	// Store job
	c.jobMutex.Lock()
	c.jobs[job.ID] = job
	c.jobMutex.Unlock()

	// Assign job to agent
	if err := c.scheduler.AssignJobToAgent(ctx, job, targetAgent); err != nil {
		c.logger.Error("Failed to assign reconciliation job to agent",
			"job_id", job.ID,
			"agent_id", targetAgent.ID,
			"error", err)

		// Remove job from storage if assignment failed
		c.jobMutex.Lock()
		delete(c.jobs, job.ID)
		c.jobMutex.Unlock()
		return nil
	}

	return job
}

// extractAgentFromJobID extracts agent ID from job naming patterns
// This is a temporary solution - ideally we'd track job assignments explicitly
func (c *Controller) extractAgentFromJobID(jobID string) string {
	// Look for patterns like "upstream-repo-file-agentID-timestamp" or "peer-repo-file-sourceID-targetID-timestamp"
	parts := strings.Split(jobID, "-")
	if len(parts) >= 4 {
		// Try to find an agent ID in the parts
		for i := 3; i < len(parts)-1; i++ { // Skip timestamp (last part)
			agentCandidate := parts[i]
			if c.isValidAgentID(agentCandidate) {
				return agentCandidate
			}
		}
	}
	return ""
}

// isValidAgentID checks if a string looks like a valid agent ID
func (c *Controller) isValidAgentID(candidateID string) bool {
	c.agentMutex.RLock()
	defer c.agentMutex.RUnlock()
	_, exists := c.agents[candidateID]
	return exists
}

// getHealthyAgents returns a list of healthy agents
func (c *Controller) getHealthyAgents() []*types.Agent {
	c.agentMutex.RLock()
	defer c.agentMutex.RUnlock()

	// Parse agent timeout from config
	agentTimeout := types.ParseDuration(c.cfg.AgentTimeout, 5*time.Minute)

	var healthyAgents []*types.Agent
	for _, agent := range c.agents {
		if agent.IsHealthy(agentTimeout) {
			healthyAgents = append(healthyAgents, agent)
		}
	}
	return healthyAgents
}

// AgentFileAvailability represents what files are available on each agent
type AgentFileAvailability struct {
	AgentID      string                     `json:"agent_id"`
	Availability map[string]map[string]bool `json:"availability"` // dataset -> filepath -> available
}

// queryAgentFileAvailabilityFull queries complete file availability from a specific agent
func (c *Controller) queryAgentFileAvailabilityFull(ctx context.Context, agentID string) (map[string]map[string]bool, error) {
	agent, exists := c.GetAgent(agentID)
	if !exists {
		return nil, fmt.Errorf("agent not found: %s", agentID)
	}

	// Make HTTP request to agent's file availability endpoint
	availabilityURL := fmt.Sprintf("%s/files/availability", agent.AdvertiseAddr.String())

	var response struct {
		Availability map[string]map[string]bool `json:"availability"`
	}

	err := c.httpTransfer.Get(ctx, availabilityURL, func(resp *http.Response) error {
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusOK {
			return fmt.Errorf("agent returned status %d", resp.StatusCode)
		}

		return json.NewDecoder(resp.Body).Decode(&response)
	})

	if err != nil {
		return nil, fmt.Errorf("failed to get file availability: %w", err)
	}

	return response.Availability, nil
}

// RegisterAgent registers a new agent or updates an existing one
func (c *Controller) RegisterAgent(agent *types.Agent) error {
	c.agentMutex.Lock()
	defer c.agentMutex.Unlock()

	agent.LastHeartbeat = time.Now()
	c.agents[agent.ID] = agent
	c.logger.Info("Agent registered", "agent_id", agent.ID, "addr", agent.AdvertiseAddr)
	return nil
}

// UpdateAgentState updates an agent's state (called during heartbeat)
func (c *Controller) UpdateAgentState(agentID string, state types.AgentState) error {
	c.agentMutex.Lock()
	defer c.agentMutex.Unlock()

	agent, exists := c.agents[agentID]
	if !exists {
		c.logger.Error("Agent not found for state update", "agent_id", agentID)
		return nil // Ignore updates for unknown agents
	}

	agent.State = state
	agent.LastHeartbeat = time.Now()

	c.logger.Debug("Agent state updated", "agent_id", agentID)
	return nil
}

// ListAgents returns all registered agents
func (c *Controller) ListAgents() []*types.Agent {
	c.agentMutex.RLock()
	defer c.agentMutex.RUnlock()

	agents := make([]*types.Agent, 0, len(c.agents))
	for _, agent := range c.agents {
		agents = append(agents, agent)
	}
	return agents
}

// GetAgent retrieves an agent by ID
func (c *Controller) GetAgent(agentID string) (*types.Agent, bool) {
	c.agentMutex.RLock()
	defer c.agentMutex.RUnlock()

	agent, exists := c.agents[agentID]
	return agent, exists
}

// GetAgentCacheStats retrieves cache statistics from an agent
func (c *Controller) GetAgentCacheStats(ctx context.Context, agentID string) (*cache.CacheStats, error) {
	agent, exists := c.GetAgent(agentID)
	if !exists {
		return nil, fmt.Errorf("agent not found: %s", agentID)
	}

	// Make HTTP request to agent's cache stats endpoint
	cacheStatsURL := fmt.Sprintf("%s/cache/stats", agent.AdvertiseAddr.String())

	var stats cache.CacheStats
	err := c.httpTransfer.Get(ctx, cacheStatsURL, func(resp *http.Response) error {
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusOK {
			return fmt.Errorf("agent returned status %d", resp.StatusCode)
		}

		return json.NewDecoder(resp.Body).Decode(&stats)
	})

	if err != nil {
		c.logger.Debug("Failed to get cache stats from agent", "agent_id", agentID, "error", err)
		return nil, fmt.Errorf("failed to get cache stats: %w", err)
	}

	c.logger.Debug("Retrieved cache stats from agent", "agent_id", agentID)
	return &stats, nil
}

// QueryAgentFileAvailability checks if specific files are available in agent caches
func (c *Controller) QueryAgentFileAvailability(ctx context.Context, dataset, filePath string) map[string]bool {
	c.agentMutex.RLock()
	defer c.agentMutex.RUnlock()

	availability := make(map[string]bool)

	for agentID, agent := range c.agents {
		if !agent.IsHealthy(2 * time.Minute) {
			continue
		}

		// Make HTTP request to check file availability
		fileCheckURL := fmt.Sprintf("%s/datasets/%s/files/%s/info", agent.AdvertiseAddr.String(), dataset, filePath)

		err := c.httpTransfer.Head(ctx, fileCheckURL, func(resp *http.Response) error {
			defer resp.Body.Close()
			availability[agentID] = resp.StatusCode == http.StatusOK
			return nil
		})

		if err != nil {
			c.logger.Debug("Failed to check file availability",
				"agent_id", agentID,
				"dataset", dataset,
				"file", filePath,
				"error", err)
			availability[agentID] = false
		} else {
			c.logger.Debug("File availability checked",
				"agent_id", agentID,
				"dataset", dataset,
				"file", filePath,
				"available", availability[agentID])
		}
	}

	return availability
}

// GetOptimalAgentsForFile returns agents that have the file cached, sorted by preference
func (c *Controller) GetOptimalAgentsForFile(ctx context.Context, dataset, filePath string) []*types.Agent {
	availability := c.QueryAgentFileAvailability(ctx, dataset, filePath)

	var optimalAgents []*types.Agent

	c.agentMutex.RLock()
	defer c.agentMutex.RUnlock()

	// First, collect agents that have the file cached
	for agentID, hasFile := range availability {
		if hasFile {
			if agent, exists := c.agents[agentID]; exists && agent.IsHealthy(2*time.Minute) {
				optimalAgents = append(optimalAgents, agent)
			}
		}
	}

	// Sort by additional criteria:
	// 1. Agent load (fewer active jobs is better)
	// 2. Available disk space (more is better)
	// 3. Last heartbeat (more recent is better)
	sort.Slice(optimalAgents, func(i, j int) bool {
		agentA, agentB := optimalAgents[i], optimalAgents[j]

		// Primary: Agent load (fewer active jobs is better)
		loadA, loadB := agentA.GetLoad(), agentB.GetLoad()
		if loadA != loadB {
			return loadA < loadB
		}

		// Secondary: Available disk space (more is better)
		diskA, diskB := agentA.State.DiskAvailable, agentB.State.DiskAvailable
		if diskA != diskB {
			return diskA > diskB
		}

		// Tertiary: Last heartbeat (more recent is better)
		return agentA.LastHeartbeat.After(agentB.LastHeartbeat)
	})

	c.logger.Debug("Optimal agents identified for file",
		"dataset", dataset,
		"file", filePath,
		"agent_count", len(optimalAgents))

	return optimalAgents
}

// GetCacheStats returns overall cache statistics from the controller's perspective
func (c *Controller) GetCacheStats(ctx context.Context) *CacheControllerStats {
	c.agentMutex.RLock()
	defer c.agentMutex.RUnlock()

	stats := &CacheControllerStats{
		TotalAgents: len(c.agents),
		// Controllers don't have cache config - they just track agent cache capabilities
		CacheEnabled: true, // Always true since we track agent caches
	}

	// Parse agent timeout from config
	agentTimeout := types.ParseDuration(c.cfg.AgentTimeout, 5*time.Minute)

	// Count healthy agents with cache capabilities
	for _, agent := range c.agents {
		if agent.IsHealthy(agentTimeout) {
			stats.HealthyAgents++

			// Check if agent has cache enabled by trying to get cache stats
			timeoutCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
			_, err := c.GetAgentCacheStats(timeoutCtx, agent.ID)
			cancel()

			if err == nil {
				stats.CacheEnabledAgents++
			}
		}
	}

	return stats
}

// CacheControllerStats represents cache statistics from the controller's perspective
type CacheControllerStats struct {
	TotalAgents        int  `json:"total_agents"`
	HealthyAgents      int  `json:"healthy_agents"`
	CacheEnabledAgents int  `json:"cache_enabled_agents"`
	CacheEnabled       bool `json:"cache_enabled"`
}

// CleanupStaleAgents removes agents that haven't sent heartbeats recently
func (c *Controller) CleanupStaleAgents(maxAge time.Duration) {
	c.agentMutex.Lock()
	defer c.agentMutex.Unlock()

	for agentID, agent := range c.agents {
		if !agent.IsHealthy(maxAge) {
			delete(c.agents, agentID)
			c.logger.Info("Removed stale agent", "agent_id", agentID, "last_heartbeat", agent.LastHeartbeat)
		}
	}
}
