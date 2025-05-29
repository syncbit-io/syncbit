package controller

import (
	"context"
	"fmt"
	"sync"
	"syncbit/internal/api"
	"syncbit/internal/config"
	"syncbit/internal/core/logger"
	"syncbit/internal/core/tracker"
	"syncbit/internal/core/types"
	"syncbit/internal/runner"
	"time"
)

// JobConfig represents the configuration for a download job
type JobConfig struct {
	ProviderID string   `json:"provider_id"`
	Repo       string   `json:"repo"`       // HuggingFace repository
	Revision   string   `json:"revision"`   // Git revision/branch
	Files      []string `json:"files"`      // List of files to download
	LocalPath  string   `json:"local_path"` // Local destination path
}

// Job represents a download job
type Job struct {
	ID      string    `json:"id"`
	Type    string    `json:"type"`    // "download"
	Handler string    `json:"handler"` // "hf" for HuggingFace
	Config  JobConfig `json:"config"`
	Status  string    `json:"status"` // "pending", "running", "completed", "failed"
	Error   string    `json:"error,omitempty"`

	// Internal fields for tracking
	runnerJob  *runner.Job `json:"-"` // Associated runner job
	assignedTo string      `json:"-"` // Agent ID this job is assigned to
}

// Agent represents a registered agent
type Agent struct {
	ID            string           `json:"id"`             // Unique agent identifier
	AdvertiseAddr types.Address    `json:"advertise_addr"` // Address other agents can reach this one at
	LastHeartbeat time.Time        `json:"last_heartbeat"` // Last time we heard from this agent
	State         AgentState       `json:"state"`          // Current agent state
	Tracker       *tracker.Tracker `json:"-"`              // Track agent health and activity
}

// AgentState represents the current state reported by an agent
type AgentState struct {
	DiskUsed      int64         `json:"disk_used"`      // Total disk usage in bytes
	DiskAvailable int64         `json:"disk_available"` // Available disk space in bytes
	DataSets      []DataSetInfo `json:"datasets"`       // What datasets this agent has
	ActiveJobs    []string      `json:"active_jobs"`    // Currently running job IDs
}

// DataSetInfo represents information about a dataset on an agent
type DataSetInfo struct {
	Name  string     `json:"name"`  // Dataset name (subdirectory)
	Files []FileInfo `json:"files"` // Files in this dataset
	Size  int64      `json:"size"`  // Total size of dataset in bytes
}

// FileInfo represents information about a file on an agent
type FileInfo struct {
	Path         string    `json:"path"`          // Relative path within dataset
	Size         int64     `json:"size"`          // File size in bytes
	Status       string    `json:"status"`        // "complete", "downloading", "partial"
	BlocksTotal  int       `json:"blocks_total"`  // Total number of blocks
	BlocksHave   int       `json:"blocks_have"`   // Number of blocks we have
	LastModified time.Time `json:"last_modified"` // Last modification time
}

type Config struct {
	Listen types.Address `yaml:"listen"` // Address to bind controller API
	Debug  bool          `yaml:"debug"`
	// Add more controller config fields as needed
}

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

// Controller is the main controller for the application.
type Controller struct {
	logger      *logger.Logger
	server      *api.Server
	cfg         *Config
	tracker     *tracker.Tracker  // Overall controller health tracking
	pool        *runner.Pool      // Job pool for managing job distribution
	jobs        map[string]*Job   // Map of job ID to job
	jobMutex    sync.RWMutex      // Protects jobs map
	agents      map[string]*Agent // Map of agent ID to agent
	agentMutex  sync.RWMutex      // Protects agents map
	jobRequests chan chan *Job    // Channel for job requests from agents
}

// NewController creates a new controller with the given options.
func NewController(configFile string, debug bool, opts ...ControllerOption) *Controller {
	cfg := &Config{
		Listen: types.NewAddress("0.0.0.0", 8080), // Default listen address
		Debug:  false,
	}

	// Load configuration with defaults
	config.LoadYAMLWithDefaults(configFile, cfg)

	if debug {
		cfg.Debug = true
	}

	c := &Controller{
		logger:      logger.NewLogger(logger.WithName("controller")),
		server:      api.NewServer(api.WithListen(cfg.Listen)),
		cfg:         cfg,
		tracker:     tracker.NewTracker("controller"),
		jobs:        make(map[string]*Job),
		agents:      make(map[string]*Agent),
		jobRequests: make(chan chan *Job, 100), // Buffer for agent job requests
	}

	for _, opt := range opts {
		opt(c)
	}

	return c
}

// SubmitJob adds a new job to the pool
func (c *Controller) SubmitJob(job *Job) error {
	c.jobMutex.Lock()
	defer c.jobMutex.Unlock()

	job.Status = "pending"
	c.jobs[job.ID] = job

	// Create a runner job that will handle job distribution
	runnerJob := runner.NewJob(job.ID, func(ctx context.Context, rjob *runner.Job) error {
		// This job represents the coordination of distributing the download to agents
		// For now, it just marks the job as ready for pickup by agents
		c.logger.Debug("Job ready for agent pickup", "job_id", job.ID)
		return nil
	})

	// Initialize job pool if not already done
	if c.pool == nil {
		ctx := context.Background() // TODO: Pass proper context
		c.pool = runner.NewPool(ctx, "controller-jobs",
			runner.WithPoolLogger(c.logger),
			runner.WithPoolQueueSize(1000),
			runner.WithPoolWorkers(5),
		)
	}

	// Submit to pool
	if err := c.pool.Submit(runnerJob); err != nil {
		c.logger.Error("Failed to submit job to pool", "job_id", job.ID, "error", err)
		job.Status = "failed"
		job.Error = "failed to queue job"
		return err
	}

	job.runnerJob = runnerJob
	c.logger.Info("Job submitted", "job_id", job.ID, "type", job.Type)
	return nil
}

// GetJob retrieves a job by ID
func (c *Controller) GetJob(jobID string) (*Job, bool) {
	c.jobMutex.RLock()
	defer c.jobMutex.RUnlock()

	job, exists := c.jobs[jobID]
	return job, exists
}

// GetNextJob retrieves the next job from available pending jobs
func (c *Controller) GetNextJob(ctx context.Context) (*Job, error) {
	c.jobMutex.Lock()
	defer c.jobMutex.Unlock()

	// Find a pending job that's not assigned to any agent
	for _, job := range c.jobs {
		if job.Status == "pending" && job.assignedTo == "" {
			job.Status = "running"
			// TODO: Set assignedTo to the requesting agent ID
			c.logger.Info("Job assigned to agent", "job_id", job.ID)
			return job, nil
		}
	}

	// No jobs available
	return nil, nil
}

// UpdateJobStatus updates the status of a job
func (c *Controller) UpdateJobStatus(jobID, status, errorMsg string) error {
	c.jobMutex.Lock()
	defer c.jobMutex.Unlock()

	job, exists := c.jobs[jobID]
	if !exists {
		c.logger.Error("Job not found", "job_id", jobID)
		return nil
	}

	job.Status = status
	job.Error = errorMsg
	job.assignedTo = "" // Clear assignment when job completes/fails

	// Update the runner job status if available
	if job.runnerJob != nil {
		if status == "completed" {
			job.runnerJob.Tracker().Update(nil) // Success
		} else if status == "failed" {
			job.runnerJob.Tracker().Update(context.Canceled) // Mark as failed
		}
	}

	c.logger.Info("Job status updated", "job_id", jobID, "status", status)
	return nil
}

// ListJobs returns all jobs
func (c *Controller) ListJobs() []*Job {
	c.jobMutex.RLock()
	defer c.jobMutex.RUnlock()

	jobs := make([]*Job, 0, len(c.jobs))
	for _, job := range c.jobs {
		jobs = append(jobs, job)
	}
	return jobs
}

// Run starts the controller and blocks until the server is stopped
func (c *Controller) Run(ctx context.Context) error {
	c.tracker.Start()

	// Initialize job pool
	c.pool = runner.NewPool(ctx, "controller-jobs",
		runner.WithPoolLogger(c.logger),
		runner.WithPoolQueueSize(1000),
		runner.WithPoolWorkers(5),
	)

	// Start background goroutines
	go c.cleanupLoop(ctx)

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

// RegisterAgent registers a new agent or updates an existing one
func (c *Controller) RegisterAgent(agent *Agent) error {
	c.agentMutex.Lock()
	defer c.agentMutex.Unlock()

	agent.LastHeartbeat = time.Now()

	// Initialize tracker for new agents
	if agent.Tracker == nil {
		agent.Tracker = tracker.NewTracker(fmt.Sprintf("agent-%s", agent.ID))
		agent.Tracker.Start()
	}

	c.agents[agent.ID] = agent
	c.logger.Info("Agent registered", "agent_id", agent.ID, "addr", agent.AdvertiseAddr)
	return nil
}

// UpdateAgentState updates an agent's state (called during heartbeat)
func (c *Controller) UpdateAgentState(agentID string, state AgentState) error {
	c.agentMutex.Lock()
	defer c.agentMutex.Unlock()

	agent, exists := c.agents[agentID]
	if !exists {
		c.logger.Error("Agent not found for state update", "agent_id", agentID)
		return nil // Ignore updates for unknown agents
	}

	agent.State = state
	agent.LastHeartbeat = time.Now()

	// Update agent tracker with health information
	if agent.Tracker != nil {
		agent.Tracker.SetTotal(agent.State.DiskUsed + agent.State.DiskAvailable)
		agent.Tracker.SetCurrent(agent.State.DiskUsed)
	}

	c.logger.Debug("Agent state updated", "agent_id", agentID, "datasets", len(state.DataSets))
	return nil
}

// ListAgents returns all registered agents
func (c *Controller) ListAgents() []*Agent {
	c.agentMutex.RLock()
	defer c.agentMutex.RUnlock()

	agents := make([]*Agent, 0, len(c.agents))
	for _, agent := range c.agents {
		agents = append(agents, agent)
	}
	return agents
}

// GetAgent retrieves an agent by ID
func (c *Controller) GetAgent(agentID string) (*Agent, bool) {
	c.agentMutex.RLock()
	defer c.agentMutex.RUnlock()

	agent, exists := c.agents[agentID]
	return agent, exists
}

// CleanupStaleAgents removes agents that haven't sent heartbeats recently
func (c *Controller) CleanupStaleAgents(maxAge time.Duration) {
	c.agentMutex.Lock()
	defer c.agentMutex.Unlock()

	cutoff := time.Now().Add(-maxAge)
	for agentID, agent := range c.agents {
		if agent.LastHeartbeat.Before(cutoff) {
			// Mark agent tracker as failed
			if agent.Tracker != nil {
				agent.Tracker.Update(context.DeadlineExceeded)
			}
			delete(c.agents, agentID)
			c.logger.Info("Removed stale agent", "agent_id", agentID, "last_heartbeat", agent.LastHeartbeat)
		}
	}
}
