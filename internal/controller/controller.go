package controller

import (
	"context"
	"fmt"
	"sync"
	"time"

	"syncbit/internal/api"
	"syncbit/internal/config"
	"syncbit/internal/core/logger"
	"syncbit/internal/core/tracker"
	"syncbit/internal/core/types"
	"syncbit/internal/runner"
)

type Config struct {
	Listen types.Address `yaml:"listen"` // Address to bind controller API
	Debug  bool          `yaml:"debug"`
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
	logger     *logger.Logger
	server     *api.Server
	cfg        *Config
	tracker    *tracker.Tracker        // Overall controller health tracking
	pool       *runner.Pool            // Job pool for managing job distribution
	jobs       map[string]*types.Job   // Map of job ID to job
	jobMutex   sync.RWMutex            // Protects jobs map
	agents     map[string]*types.Agent // Map of agent ID to agent
	agentMutex sync.RWMutex            // Protects agents map
	scheduler  *JobScheduler           // Intelligent job scheduling
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
		logger:  logger.NewLogger(logger.WithName("controller")),
		server:  api.NewServer(api.WithListen(cfg.Listen)),
		cfg:     cfg,
		tracker: tracker.NewTracker("controller"),
		jobs:    make(map[string]*types.Job),
		agents:  make(map[string]*types.Agent),
	}

	for _, opt := range opts {
		opt(c)
	}

	// Initialize the scheduler
	c.scheduler = NewJobScheduler(c)

	return c
}

// SubmitJob adds a new job to the controller
func (c *Controller) SubmitJob(job *types.Job) error {
	job.SetStatus(types.JobStatusPending, "")

	// Use scheduler to intelligently assign the job
	ctx := context.Background()
	if err := c.scheduler.ScheduleJob(ctx, job); err != nil {
		c.logger.Error("Failed to schedule job", "job_id", job.ID, "error", err)
		job.SetStatus(types.JobStatusFailed, fmt.Sprintf("scheduling failed: %v", err))
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
		job.SetStatus(types.JobStatusFailed, "failed to queue job")
		return err
	}

	c.logger.Info("Job submitted and scheduled", "job_id", job.ID, "handler", job.Handler)
	return nil
}

// GetJob retrieves a job by ID
func (c *Controller) GetJob(jobID string) (*types.Job, bool) {
	c.jobMutex.RLock()
	defer c.jobMutex.RUnlock()

	job, exists := c.jobs[jobID]
	return job, exists
}

// GetNextJob retrieves the next job from available pending jobs
func (c *Controller) GetNextJob(ctx context.Context) (*types.Job, error) {
	c.jobMutex.Lock()
	defer c.jobMutex.Unlock()

	// Find a pending job
	for _, job := range c.jobs {
		if job.Status == types.JobStatusPending {
			job.SetStatus(types.JobStatusRunning, "")
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
