package agent

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"sync"
	"time"

	"syncbit/internal/api"
	"syncbit/internal/api/request"
	"syncbit/internal/cache"
	"syncbit/internal/config"
	"syncbit/internal/core/logger"
	"syncbit/internal/core/types"
	"syncbit/internal/provider"
	"syncbit/internal/runner"
	"syncbit/internal/transport"
)

type Agent struct {
	cfg            *types.AgentConfig
	providers      map[string]types.ProviderConfig
	log            *logger.Logger
	httpClient     *transport.HTTPTransfer
	server         *api.Server
	cache          *cache.Cache
	pool           *runner.Pool
	jobs           map[string]*types.Job
	jobCancelFuncs map[string]context.CancelFunc
	jobMutex       sync.RWMutex
	ctx            context.Context // Main app context for proper cancellation
}

type AgentOption func(*Agent)

func WithLogger(log *logger.Logger) AgentOption {
	return func(a *Agent) {
		a.log = log
	}
}

func WithServer(server *api.Server) AgentOption {
	return func(a *Agent) {
		a.server = server
	}
}

func NewAgent(configFile string, debug bool, opts ...AgentOption) *Agent {
	// Load configuration
	cfg, err := config.LoadConfig(configFile)
	if err != nil {
		// Use defaults if config load fails
		cfg = &types.Config{
			Debug:     debug,
			Providers: make(map[string]types.ProviderConfig),
			Agent:     &types.AgentConfig{},
		}
	}

	// Apply debug flag
	if debug {
		cfg.Debug = true
	}

	// Ensure we have agent config
	if cfg.Agent == nil {
		defaults := types.DefaultAgentConfig()
		cfg.Agent = &defaults
	}

	// Auto-generate agent ID if not set
	if cfg.Agent.ID == "" {
		if hostname, err := os.Hostname(); err == nil {
			cfg.Agent.ID = fmt.Sprintf("agent-%s", hostname)
		} else {
			cfg.Agent.ID = fmt.Sprintf("agent-%d", time.Now().Unix())
		}
	}

	// Use advertise address from config, or default to localhost
	advertiseAddr := cfg.Agent.AdvertiseAddr
	if advertiseAddr == nil {
		advertiseAddr, _ = url.Parse("http://localhost:8081")
	}

	// Use listen address from config, or default
	listenAddr := cfg.Agent.ListenAddr
	if listenAddr == nil {
		listenAddr, _ = url.Parse("http://0.0.0.0:8081")
	}

	// Create HTTP client for agent-controller communication
	httpClient := transport.NewHTTPTransfer()

	// Initialize cache
	agentCache, err := cache.NewCache(cfg.Agent.Storage.Cache, cfg.Agent.Storage.BasePath)
	if err != nil {
		// Continue with memory-only cache on error
		agentCache, _ = cache.NewCache(cfg.Agent.Storage.Cache, "")
	}

	a := &Agent{
		cfg:            cfg.Agent,
		providers:      cfg.Providers,
		log:            logger.NewLogger(logger.WithName("agent")),
		httpClient:     httpClient,
		server:         api.NewServer(api.WithListen(listenAddr)),
		cache:          agentCache,
		jobs:           make(map[string]*types.Job),
		jobCancelFuncs: make(map[string]context.CancelFunc),
	}

	for _, opt := range opts {
		opt(a)
	}

	return a
}

func (a *Agent) Run(ctx context.Context) error {
	// Store the main app context for use in job management
	a.ctx = ctx

	a.log.Info("Starting agent",
		"controller", a.cfg.ControllerURL,
		"agent_id", a.cfg.ID,
		"listen", a.cfg.ListenAddr,
		"advertise", a.cfg.AdvertiseAddr,
	)

	// Initialize providers from config
	if err := a.initProviders(); err != nil {
		a.log.Error("Failed to initialize providers", "error", err)
		return fmt.Errorf("failed to initialize providers: %w", err)
	}
	a.log.Info("Providers initialized successfully", "count", len(a.providers))

	// Initialize job pool
	a.pool = runner.NewPool(ctx, "agent-jobs",
		runner.WithPoolLogger(a.log),
		runner.WithPoolQueueSize(100),
		runner.WithPoolWorkers(3),
	)

	// Start job processing goroutine
	go a.processJobs(ctx)

	// Register API handlers
	if err := a.RegisterHandlers(a.server); err != nil {
		return fmt.Errorf("failed to register handlers: %w", err)
	}

	// Register with controller
	if err := a.registerWithController(ctx); err != nil {
		a.log.Error("Failed to register with controller", "error", err)
		return fmt.Errorf("failed to register with controller: %w", err)
	}

	// Start heartbeat goroutine
	heartbeatInterval := types.ParseDuration(a.cfg.HeartbeatInterval, 30*time.Second)
	go a.heartbeatLoop(ctx, heartbeatInterval)

	// Start the agent API server
	a.log.Info("Starting agent API server", "address", a.cfg.ListenAddr)
	return a.server.Run(ctx)
}

// initProviders initializes providers from configuration
func (a *Agent) initProviders() error {
	return provider.InitializeProviders(a.providers)
}

// processJobs handles completed jobs from the pool and reports status back to controller
func (a *Agent) processJobs(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case completedJob, ok := <-a.pool.Completed():
			if !ok {
				return
			}

			// Find the corresponding agent job
			a.jobMutex.Lock()
			agentJob, exists := a.jobs[completedJob.Name()]
			if !exists {
				a.jobMutex.Unlock()
				a.log.Error("Completed job not found in agent jobs map", "job_id", completedJob.Name())
				continue
			}

			// Clean up cancel function since job is complete
			delete(a.jobCancelFuncs, completedJob.Name())

			// Update job status based on runner result
			if completedJob.Tracker().IsSucceeded() {
				agentJob.SetStatus(types.StatusCompleted, "")
				a.log.Info("Job completed successfully",
					"job_id", agentJob.ID,
					"handler", agentJob.Handler,
					"file", agentJob.Config.FilePath,
					"bytes_processed", completedJob.Tracker().Current(),
				)
			} else if completedJob.Tracker().IsCanceled() {
				if agentJob.Status != types.StatusCancelled {
					agentJob.SetStatus(types.StatusCancelled, "Job was cancelled")
				}
				a.log.Info("Job was cancelled", "job_id", agentJob.ID, "handler", agentJob.Handler)
			} else {
				errorMsg := "unknown error"
				if completedJob.Tracker().Err() != nil {
					errorMsg = completedJob.Tracker().Err().Error()
				}
				agentJob.SetStatus(types.StatusFailed, errorMsg)
				a.log.Error("Job failed", "job_id", agentJob.ID, "error", errorMsg)
			}
			a.jobMutex.Unlock()

			// Report status back to controller
			go a.reportJobStatus(ctx, agentJob)
		}
	}
}

// reportJobStatus reports job completion status back to the controller
func (a *Agent) reportJobStatus(ctx context.Context, job *types.Job) {
	status := struct {
		Status string `json:"status"`
		Error  string `json:"error,omitempty"`
	}{
		Status: string(job.Status),
		Error:  job.Error,
	}

	url := fmt.Sprintf("%s/jobs/%s/status", a.cfg.ControllerURL, job.ID)

	err := a.httpClient.Post(ctx, url, func(resp *http.Response) error {
		defer resp.Body.Close()
		if resp.StatusCode != 200 {
			return fmt.Errorf("failed to report job status, controller returned: %d", resp.StatusCode)
		}
		a.log.Debug("Successfully reported job status to controller", "job_id", job.ID, "status", job.Status)
		return nil
	}, request.WithJSON(status))

	if err != nil {
		a.log.Error("Failed to report job status to controller", "job_id", job.ID, "error", err)
	}
}

// registerWithController registers this agent with the controller
func (a *Agent) registerWithController(ctx context.Context) error {
	registrationRequest := struct {
		ID            string `json:"id"`
		AdvertiseAddr string `json:"advertise_addr"`
	}{
		ID:            a.cfg.ID,
		AdvertiseAddr: a.cfg.AdvertiseAddr.String(),
	}

	url := fmt.Sprintf("%s/agents/register", a.cfg.ControllerURL.String())

	err := a.httpClient.Post(ctx, url, func(resp *http.Response) error {
		defer resp.Body.Close()
		if resp.StatusCode != 201 {
			return fmt.Errorf("registration failed with status: %d", resp.StatusCode)
		}
		a.log.Info("Successfully registered with controller", "agent_id", a.cfg.ID)
		return nil
	}, request.WithJSON(registrationRequest))

	return err
}

// heartbeatLoop sends periodic heartbeats to the controller
func (a *Agent) heartbeatLoop(ctx context.Context, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			a.log.Debug("Heartbeat loop stopping")
			return
		case <-ticker.C:
			if err := a.sendHeartbeat(ctx); err != nil {
				a.log.Error("Failed to send heartbeat", "error", err)
			}
		}
	}
}

// sendHeartbeat sends current agent state to the controller
func (a *Agent) sendHeartbeat(ctx context.Context) error {
	// Collect current active jobs
	a.jobMutex.RLock()
	activeJobs := make([]string, 0)
	for jobID, job := range a.jobs {
		if job.IsActive() {
			activeJobs = append(activeJobs, jobID)
		}
	}
	a.jobMutex.RUnlock()

	// Get cache stats for disk usage information
	cacheStats := a.cache.GetCacheStats()

	state := types.AgentState{
		DiskUsed:      types.Bytes(cacheStats.DiskUsage),
		DiskAvailable: types.Bytes(cacheStats.DiskLimit - cacheStats.DiskUsage),
		ActiveJobs:    activeJobs,
		LastUpdated:   time.Now(),
	}

	url := fmt.Sprintf("%s/agents/%s/heartbeat", a.cfg.ControllerURL, a.cfg.ID)

	err := a.httpClient.Post(ctx, url, func(resp *http.Response) error {
		defer resp.Body.Close()
		if resp.StatusCode != 200 {
			return fmt.Errorf("heartbeat failed with status: %d", resp.StatusCode)
		}
		a.log.Debug("Heartbeat sent successfully",
			"active_jobs", len(activeJobs),
			"disk_used", state.DiskUsed,
			"disk_available", state.DiskAvailable,
		)
		return nil
	}, request.WithJSON(state))

	return err
}
