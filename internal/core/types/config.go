package types

import (
	"net/url"
	"time"
)

// mustParseURL is a helper for parsing URLs in default configs
func mustParseURL(rawURL string) *url.URL {
	u, err := url.Parse(rawURL)
	if err != nil {
		panic("invalid default URL: " + rawURL)
	}
	return u
}

// Config is the top-level configuration structure
type Config struct {
	Debug     bool                      `yaml:"debug"`
	Providers map[string]ProviderConfig `yaml:"providers"`

	// Only one of these should be set depending on the binary
	Agent      *AgentConfig      `yaml:"agent,omitempty"`
	Controller *ControllerConfig `yaml:"controller,omitempty"`
	Client     *ClientConfig     `yaml:"client,omitempty"`
}

// AgentConfig holds configuration for agent instances
type AgentConfig struct {
	ID                string   `yaml:"id"`                 // Agent identifier
	ControllerURL     *url.URL `yaml:"controller_url"`     // Controller API URL
	ListenAddr        *url.URL `yaml:"listen_addr"`        // Address to bind agent API
	AdvertiseAddr     *url.URL `yaml:"advertise_addr"`     // Address for other agents to reach this one
	HeartbeatInterval string   `yaml:"heartbeat_interval"` // How often to report to controller

	// Storage configuration
	Storage StorageConfig `yaml:"storage"`
}

// ControllerConfig holds configuration for controller instances
type ControllerConfig struct {
	ListenAddr   *url.URL `yaml:"listen_addr"`   // Address to bind controller API
	AgentTimeout string   `yaml:"agent_timeout"` // How long before agents are considered stale
	SyncInterval string   `yaml:"sync_interval"` // How often to run reconciliation
}

// ClientConfig holds configuration for CLI client
type ClientConfig struct {
	ControllerURL *url.URL `yaml:"controller_url"` // Controller API URL
}

// StorageConfig holds storage and caching configuration
type StorageConfig struct {
	BasePath string      `yaml:"base_path"` // Base directory for all datasets
	Cache    CacheConfig `yaml:"cache"`     // Cache configuration
}

// CacheConfig holds cache-specific settings
type CacheConfig struct {
	RAMLimit  Bytes `yaml:"ram_limit"`  // Maximum RAM for file cache
	DiskLimit Bytes `yaml:"disk_limit"` // Maximum disk usage (0 = unlimited)
}

// ProviderConfig holds authentication and connection configuration for a provider
type ProviderConfig struct {
	ID   string `yaml:"id"`   // Unique identifier for this provider instance
	Type string `yaml:"type"` // Provider type (s3, hf, http, peer)
	Name string `yaml:"name"` // Human-readable name

	// Authentication settings
	Token string `yaml:"token"` // Auth token (HF token, API key, etc.)

	// AWS S3 specific settings
	Region  string `yaml:"region"`  // AWS region
	Profile string `yaml:"profile"` // AWS profile

	// HTTP specific settings
	Headers map[string]string `yaml:"headers"` // Default headers for HTTP requests

	// Transfer settings
	Transfer *TransferConfig `yaml:"transfer,omitempty"` // Per-provider transfer configuration
}

// TransferConfig holds configuration for transfer settings
type TransferConfig struct {
	RateLimit   int64             `yaml:"rate_limit"`  // Bytes per second rate limit
	PartSize    int64             `yaml:"part_size"`   // Part size for multipart downloads
	Concurrency int               `yaml:"concurrency"` // Number of concurrent parts
	Headers     map[string]string `yaml:"headers"`     // HTTP headers
}

// ParseDuration parses a duration string with fallback to default
func ParseDuration(durationStr string, defaultDuration time.Duration) time.Duration {
	if durationStr == "" {
		return defaultDuration
	}
	if dur, err := time.ParseDuration(durationStr); err == nil {
		return dur
	}
	return defaultDuration
}

// DefaultAgentConfig returns default agent configuration
func DefaultAgentConfig() AgentConfig {
	return AgentConfig{
		ListenAddr:        mustParseURL("http://0.0.0.0:8081"),
		HeartbeatInterval: "30s",
		Storage: StorageConfig{
			BasePath: "/var/lib/syncbit/data",
			Cache: CacheConfig{
				RAMLimit:  Bytes(4 * 1024 * 1024 * 1024), // 4GB
				DiskLimit: Bytes(0),                      // Unlimited
			},
		},
	}
}

// DefaultControllerConfig returns default controller configuration
func DefaultControllerConfig() ControllerConfig {
	return ControllerConfig{
		ListenAddr:   mustParseURL("http://0.0.0.0:8080"),
		AgentTimeout: "5m",
		SyncInterval: "30s",
	}
}

// DefaultClientConfig returns default client configuration
func DefaultClientConfig() ClientConfig {
	return ClientConfig{
		ControllerURL: mustParseURL("http://localhost:8080"),
	}
}

// DefaultTransferConfig returns default transfer configuration
func DefaultTransferConfig() TransferConfig {
	return TransferConfig{
		RateLimit:   0,               // No limit
		PartSize:    5 * 1024 * 1024, // 5MB
		Concurrency: 4,
		Headers:     make(map[string]string),
	}
}
