package agent

import (
	"context"
	"fmt"
	"net/http"
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
	cfg            *config.AgentConfig
	daemonCfg      *config.DaemonConfig
	controllerAddr types.Address
	log            *logger.Logger
	httpClient     *transport.HTTPTransfer
	server         *api.Server
	cache          *cache.Cache
	pool           *runner.Pool
	jobs           map[string]*types.Job
	jobCancelFuncs map[string]context.CancelFunc
	jobMutex       sync.RWMutex
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
	// Create a structure to load the complete configuration file
	type AgentConfigFile struct {
		Daemon config.DaemonConfig `yaml:"daemon"`
		Agent  config.AgentConfig  `yaml:"agent"`
	}

	// Load the complete configuration file
	configData := &AgentConfigFile{}
	config.LoadYAMLWithDefaults(configFile, configData)

	// Extract daemon and agent configs
	daemonCfg := &configData.Daemon
	agentCfg := configData.Agent

	// Apply defaults if agent config is missing or incomplete
	if agentCfg.ID == "" {
		if hostname, err := os.Hostname(); err == nil {
			agentCfg.ID = fmt.Sprintf("agent-%s", hostname)
		} else {
			agentCfg.ID = fmt.Sprintf("agent-%d", time.Now().Unix())
		}
	}

	// Apply defaults for storage if not set
	if agentCfg.Storage.BasePath == "" {
		agentCfg.Storage.BasePath = "/var/lib/syncbit/data"
	}

	// Apply defaults for cache if not set
	if agentCfg.Storage.Cache.RAMLimit == 0 {
		agentCfg.Storage.Cache.RAMLimit = types.Bytes(4 * 1024 * 1024 * 1024) // 4GB
	}

	// Apply defaults for network if not set
	if (agentCfg.Network.ListenAddr == types.Address{}) {
		agentCfg.Network.ListenAddr = types.NewAddress("0.0.0.0", 8081)
	}

	// Apply defaults for heartbeat interval
	if agentCfg.Network.HeartbeatInterval == "" {
		agentCfg.Network.HeartbeatInterval = "30s"
	}

	// Apply defaults for peer timeout
	if agentCfg.Network.PeerTimeout == "" {
		agentCfg.Network.PeerTimeout = "30s"
	}

	// Auto-detect advertise address if not set
	if (agentCfg.Network.AdvertiseAddr == types.Address{}) {
		advertiseAddr := agentCfg.Network.ListenAddr
		if advertiseAddr.Host == "0.0.0.0" {
			advertiseAddr.Host = "localhost"
		}
		agentCfg.Network.AdvertiseAddr = advertiseAddr
	}

	// Apply debug flag
	if debug {
		daemonCfg.Debug = true
	}

	// Use controller address from daemon config
	controllerAddr := daemonCfg.ControllerAddr

	// Create HTTP client for agent-controller communication
	httpClient := transport.NewHTTPTransfer()

	// Initialize cache
	agentCache, err := cache.NewCache(agentCfg.Storage.Cache, agentCfg.Storage.BasePath)
	if err != nil {
		// Continue with memory-only cache on error
		agentCache, _ = cache.NewCache(agentCfg.Storage.Cache, "")
	}

	a := &Agent{
		cfg:            &agentCfg,
		daemonCfg:      daemonCfg,
		controllerAddr: controllerAddr,
		log:            logger.NewLogger(logger.WithName("agent")),
		httpClient:     httpClient,
		server:         api.NewServer(api.WithListen(agentCfg.Network.ListenAddr)),
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
	a.log.Info("Starting agent",
		"controller", a.controllerAddr.URL(),
		"agent_id", a.cfg.ID,
		"listen", a.cfg.Network.ListenAddr.URL(),
		"advertise", a.cfg.Network.AdvertiseAddr.URL(),
	)

	// Initialize providers from daemon config
	if err := provider.InitFromDaemonConfig(*a.daemonCfg); err != nil {
		a.log.Error("Failed to initialize providers", "error", err)
		return fmt.Errorf("failed to initialize providers: %w", err)
	}
	a.log.Info("Providers initialized successfully", "count", len(provider.ListProviders()))

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
	heartbeatInterval := a.cfg.Network.ParseHeartbeatInterval(30 * time.Second)
	go a.heartbeatLoop(ctx, heartbeatInterval)

	// Start the agent API server
	a.log.Info("Starting agent API server", "address", a.cfg.Network.ListenAddr.URL())
	return a.server.Run(ctx)
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

	url := fmt.Sprintf("%s/jobs/%s/status", a.controllerAddr.URL(), job.ID)

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
		AdvertiseAddr: a.cfg.Network.AdvertiseAddr.URL(),
	}

	url := fmt.Sprintf("%s/agents/register", a.controllerAddr.URL())

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

	url := fmt.Sprintf("%s/agents/%s/heartbeat", a.controllerAddr.URL(), a.cfg.ID)

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
