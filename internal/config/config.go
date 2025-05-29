package config

import (
	"syncbit/internal/core/types"
	"time"
)

// DaemonConfig holds configuration for the agent daemon
type DaemonConfig struct {
	Debug          bool                      `yaml:"debug"`
	AgentAddr      types.Address             `yaml:"agent_addr"`      // Address for agent API
	ControllerAddr types.Address             `yaml:"controller_addr"` // Address for controller API
	Providers      map[string]ProviderConfig `yaml:"providers"`       // Named provider configurations
	// Add more daemon config fields as needed
}

// AgentConfig holds configuration for agent-specific settings
type AgentConfig struct {
	ID      string        `yaml:"id"`      // Agent identifier
	Storage StorageConfig `yaml:"storage"` // Storage and cache settings
	Network NetworkConfig `yaml:"network"` // Network and peer settings
}

// StorageConfig holds storage and caching configuration
type StorageConfig struct {
	BasePath string      `yaml:"base_path"` // Base directory for all datasets
	Cache    CacheConfig `yaml:"cache"`     // Cache configuration
}

// CacheConfig holds cache-specific settings
type CacheConfig struct {
	RAMLimit  types.Bytes `yaml:"ram_limit"`  // Maximum RAM for block cache
	DiskLimit types.Bytes `yaml:"disk_limit"` // Maximum disk usage (0 = unlimited)
	BlockSize types.Bytes `yaml:"block_size"` // Block size for chunking
}

// NetworkConfig holds network and peer communication settings
type NetworkConfig struct {
	ListenAddr        types.Address `yaml:"listen_addr"`        // Address to bind agent API
	AdvertiseAddr     types.Address `yaml:"advertise_addr"`     // Address for other agents to reach this one
	HeartbeatInterval string        `yaml:"heartbeat_interval"` // How often to report to controller
	PeerTimeout       string        `yaml:"peer_timeout"`       // Timeout for peer requests
}

// ClientConfig holds configuration for the client
type ClientConfig struct {
	ControllerAddr types.Address `yaml:"controller_addr"` // Address for controller API
	// Add more client config fields as needed
}

// ProviderConfig holds authentication and connection configuration for a provider
// Resource identifiers (repo, bucket, URL) are specified at the job level
type ProviderConfig struct {
	ID   string `yaml:"id"`   // Unique identifier for this provider instance
	Type string `yaml:"type"` // Provider type (s3, hf, http)
	Name string `yaml:"name"` // Human-readable name

	// Authentication settings
	Token string `yaml:"token"` // Auth token (HF token, API key, etc.)

	// AWS S3 specific settings
	Region  string `yaml:"region"`  // AWS region (optional, can be job-specific)
	Profile string `yaml:"profile"` // AWS profile

	// HTTP specific settings
	Headers map[string]string `yaml:"headers"` // Default headers for HTTP requests
}

// TransferConfig holds configuration for transfer settings
type TransferConfig struct {
	RateLimit   int64             `yaml:"rate_limit"`  // Bytes per second rate limit
	PartSize    int64             `yaml:"part_size"`   // Part size for multipart downloads
	Concurrency int               `yaml:"concurrency"` // Number of concurrent parts
	Headers     map[string]string `yaml:"headers"`     // HTTP headers
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

// ParseDuration parses a duration string with fallback to default
func (n *NetworkConfig) ParseHeartbeatInterval(defaultDuration time.Duration) time.Duration {
	if n.HeartbeatInterval == "" {
		return defaultDuration
	}
	if dur, err := time.ParseDuration(n.HeartbeatInterval); err == nil {
		return dur
	}
	return defaultDuration
}

// ParsePeerTimeout parses peer timeout with fallback to default
func (n *NetworkConfig) ParsePeerTimeout(defaultDuration time.Duration) time.Duration {
	if n.PeerTimeout == "" {
		return defaultDuration
	}
	if dur, err := time.ParseDuration(n.PeerTimeout); err == nil {
		return dur
	}
	return defaultDuration
}

// DefaultAgentConfig returns default agent configuration
func DefaultAgentConfig() AgentConfig {
	return AgentConfig{
		Storage: StorageConfig{
			BasePath: "/var/lib/syncbit/data",
			Cache: CacheConfig{
				RAMLimit:  types.Bytes(4 * 1024 * 1024 * 1024), // 4GB
				DiskLimit: types.Bytes(0),                      // Unlimited
				BlockSize: types.Bytes(1024 * 1024),            // 1MB
			},
		},
		Network: NetworkConfig{
			ListenAddr:        types.NewAddress("0.0.0.0", 8081),
			AdvertiseAddr:     types.Address{}, // Empty, will be auto-detected
			HeartbeatInterval: "30s",
			PeerTimeout:       "30s",
		},
	}
}
