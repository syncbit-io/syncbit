package config

// DaemonConfig holds configuration for the agent daemon
type DaemonConfig struct {
	Debug         bool                      `yaml:"debug"`
	AgentURL      string                    `yaml:"agent_url"`
	ControllerURL string                    `yaml:"controller_url"`
	Providers     map[string]ProviderConfig `yaml:"providers"` // Named provider configurations
	// Add more daemon config fields as needed
}

// ClientConfig holds configuration for the client
type ClientConfig struct {
	ControllerURL string `yaml:"controller_url"`
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
		RateLimit:   0,                // No rate limit by default
		PartSize:    64 * 1024 * 1024, // 64MB parts
		Concurrency: 4,                // 4 concurrent parts
		Headers:     make(map[string]string),
	}
}
