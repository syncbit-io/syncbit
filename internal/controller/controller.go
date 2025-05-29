package controller

import (
	"context"
	"os"
	"sync"
	"syncbit/internal/api"
	"syncbit/internal/core/logger"

	"gopkg.in/yaml.v3"
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
}

type Config struct {
	Listen string `yaml:"listen"`
	Debug  bool   `yaml:"debug"`
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
	logger   *logger.Logger
	server   *api.Server
	cfg      *Config
	jobs     map[string]*Job // Map of job ID to job
	jobQueue chan *Job       // Queue of jobs waiting to be picked up
	jobMutex sync.RWMutex    // Protects jobs map
}

// NewController creates a new controller with the given options.
func NewController(configFile string, debug bool, opts ...ControllerOption) *Controller {
	cfg := &Config{}
	f, err := os.Open(configFile)
	if err == nil {
		defer f.Close()
		_ = yaml.NewDecoder(f).Decode(cfg)
	}
	if debug {
		cfg.Debug = true
	}
	c := &Controller{
		logger:   logger.NewLogger(logger.WithName("controller")),
		server:   api.NewServer(),
		cfg:      cfg,
		jobs:     make(map[string]*Job),
		jobQueue: make(chan *Job, 100), // Buffer up to 100 jobs
	}

	for _, opt := range opts {
		opt(c)
	}

	return c
}

// SubmitJob adds a new job to the queue
func (c *Controller) SubmitJob(job *Job) error {
	c.jobMutex.Lock()
	defer c.jobMutex.Unlock()

	job.Status = "pending"
	c.jobs[job.ID] = job

	select {
	case c.jobQueue <- job:
		c.logger.Info("Job submitted", "job_id", job.ID, "type", job.Type)
		return nil
	default:
		c.logger.Error("Job queue is full", "job_id", job.ID)
		job.Status = "failed"
		job.Error = "job queue is full"
		return nil // Still store the job but mark as failed
	}
}

// GetJob retrieves a job by ID
func (c *Controller) GetJob(jobID string) (*Job, bool) {
	c.jobMutex.RLock()
	defer c.jobMutex.RUnlock()

	job, exists := c.jobs[jobID]
	return job, exists
}

// GetNextJob retrieves the next job from the queue (blocks if no jobs available)
func (c *Controller) GetNextJob(ctx context.Context) (*Job, error) {
	select {
	case job := <-c.jobQueue:
		c.jobMutex.Lock()
		job.Status = "running"
		c.jobMutex.Unlock()
		c.logger.Info("Job assigned to agent", "job_id", job.ID)
		return job, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
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
	if err := c.RegisterHandlers(c.server); err != nil {
		return err
	}

	return c.server.Run(ctx)
}
