package agent

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"time"

	"syncbit/internal/api"
	"syncbit/internal/api/request"
	"syncbit/internal/config"
	"syncbit/internal/core/logger"
	"syncbit/internal/core/types"
	"syncbit/internal/provider"
	"syncbit/internal/transport"
)

// Job represents a job from the controller
type Job struct {
	ID      string    `json:"id"`
	Type    string    `json:"type"`
	Handler string    `json:"handler"`
	Config  JobConfig `json:"config"`
	Status  string    `json:"status"`
	Error   string    `json:"error,omitempty"`
}

// JobConfig represents the configuration for a download job
type JobConfig struct {
	ProviderID string   `json:"provider_id"`
	Repo       string   `json:"repo"`
	Revision   string   `json:"revision"`
	Files      []string `json:"files"`
	LocalPath  string   `json:"local_path"`
}

// Config represents the complete configuration file structure
type Config struct {
	Client ClientConfig        `yaml:"client"`
	Daemon config.DaemonConfig `yaml:"daemon"`
	Agent  config.AgentConfig  `yaml:"agent"`
}

// ClientConfig holds configuration for the client
type ClientConfig struct {
	ControllerAddr types.Address `yaml:"controller_addr"`
}

type Agent struct {
	cfg        *Config
	log        *logger.Logger
	httpClient *transport.HTTPTransfer
	server     *api.Server
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
	// Load configuration with defaults
	cfg := &Config{}
	config.LoadYAMLWithDefaults(configFile, cfg)

	// Apply defaults for empty sections
	if cfg.Agent.ID == "" {
		defaultAgentCfg := config.DefaultAgentConfig()
		cfg.Agent = defaultAgentCfg

		// Generate agent ID from hostname if not specified
		if hostname, err := os.Hostname(); err == nil {
			cfg.Agent.ID = fmt.Sprintf("agent-%s", hostname)
		} else {
			cfg.Agent.ID = fmt.Sprintf("agent-%d", time.Now().Unix())
		}
	}

	// Apply debug flag
	if debug {
		cfg.Daemon.Debug = true
	}

	// Use daemon controller address if client doesn't have one
	if (cfg.Client.ControllerAddr == types.Address{}) && (cfg.Daemon.ControllerAddr != types.Address{}) {
		cfg.Client.ControllerAddr = cfg.Daemon.ControllerAddr
	}

	// Auto-detect advertise address if not set
	if (cfg.Agent.Network.AdvertiseAddr == types.Address{}) {
		// Use listen address but replace 0.0.0.0 with localhost
		advertiseAddr := cfg.Agent.Network.ListenAddr
		if advertiseAddr.Host == "0.0.0.0" {
			advertiseAddr.Host = "localhost"
		}
		cfg.Agent.Network.AdvertiseAddr = advertiseAddr
	}

	// Create a simple HTTP client for agent-controller communication
	httpClient := transport.NewHTTPTransfer(
		transport.HTTPWithClient(&http.Client{
			Timeout: 30 * time.Second,
		}),
	)

	a := &Agent{
		cfg:        cfg,
		log:        logger.NewLogger(logger.WithName("agent")),
		httpClient: httpClient,
		server:     api.NewServer(api.WithListen(cfg.Agent.Network.ListenAddr)),
	}

	for _, opt := range opts {
		opt(a)
	}

	return a
}

func (a *Agent) Run(ctx context.Context) error {
	a.log.Info("Starting agent",
		"controller", a.cfg.Client.ControllerAddr.URL(),
		"agent_id", a.cfg.Agent.ID,
		"listen", a.cfg.Agent.Network.ListenAddr.URL(),
		"advertise", a.cfg.Agent.Network.AdvertiseAddr.URL(),
	)

	// Initialize providers from daemon config
	if err := provider.InitFromDaemonConfig(a.cfg.Daemon); err != nil {
		a.log.Error("Failed to initialize providers", "error", err)
		return fmt.Errorf("failed to initialize providers: %w", err)
	}
	a.log.Info("Initialized providers", "providers", provider.ListProviders())

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
	heartbeatInterval := a.cfg.Agent.Network.ParseHeartbeatInterval(30 * time.Second)
	go a.heartbeatLoop(ctx, heartbeatInterval)

	// Start the agent API server
	a.log.Info("Starting agent API server", "address", a.cfg.Agent.Network.ListenAddr.URL())
	return a.server.Run(ctx)
}

// registerWithController registers this agent with the controller
func (a *Agent) registerWithController(ctx context.Context) error {
	registrationRequest := struct {
		ID            string `json:"id"`
		AdvertiseAddr string `json:"advertise_addr"`
	}{
		ID:            a.cfg.Agent.ID,
		AdvertiseAddr: a.cfg.Agent.Network.AdvertiseAddr.URL(),
	}

	url := fmt.Sprintf("%s/agents/register", a.cfg.Client.ControllerAddr.URL())

	err := a.httpClient.Post(ctx, url, func(resp *http.Response) error {
		defer resp.Body.Close()

		if resp.StatusCode != 201 {
			return fmt.Errorf("registration failed with status: %d", resp.StatusCode)
		}

		// TODO: Parse registration response if needed
		a.log.Info("Successfully registered with controller", "agent_id", a.cfg.Agent.ID)
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
				// Continue trying - don't exit on heartbeat failures
			}
		}
	}
}

// sendHeartbeat sends current agent state to the controller
func (a *Agent) sendHeartbeat(ctx context.Context) error {
	// TODO: Implement proper state collection
	// For now, send basic empty state
	state := AgentState{
		DiskUsed:      0,
		DiskAvailable: 1024 * 1024 * 1024 * 1024, // 1TB placeholder
		DataSets:      []DataSetInfo{},
		ActiveJobs:    []string{},
	}

	url := fmt.Sprintf("%s/agents/%s/heartbeat", a.cfg.Client.ControllerAddr.URL(), a.cfg.Agent.ID)

	err := a.httpClient.Post(ctx, url, func(resp *http.Response) error {
		defer resp.Body.Close()

		if resp.StatusCode != 200 {
			return fmt.Errorf("heartbeat failed with status: %d", resp.StatusCode)
		}

		a.log.Debug("Heartbeat sent successfully")
		return nil
	}, request.WithJSON(state))

	return err
}
