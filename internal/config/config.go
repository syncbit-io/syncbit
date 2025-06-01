package config

import (
	"fmt"
	"net/url"
	"os"
	"path/filepath"

	"syncbit/internal/core/types"

	"github.com/goccy/go-yaml"
)

// LoadConfig loads configuration from a YAML file and applies defaults
func LoadConfig(configFile string) (*types.Config, error) {
	// Start with default configuration
	config := &types.Config{
		Debug:     false,
		Providers: make(map[string]types.ProviderConfig),
	}

	// Load from file if it exists
	if configFile != "" && fileExists(configFile) {
		data, err := os.ReadFile(configFile)
		if err != nil {
			return nil, fmt.Errorf("failed to read config file %s: %w", configFile, err)
		}

		if err := yaml.Unmarshal(data, config); err != nil {
			return nil, fmt.Errorf("failed to parse config file %s: %w", configFile, err)
		}
	}

	// Apply defaults by merging with default configs
	if config.Agent != nil {
		config.Agent = mergeAgentConfig(config.Agent, types.DefaultAgentConfig())
	}
	if config.Controller != nil {
		config.Controller = mergeControllerConfig(config.Controller, types.DefaultControllerConfig())
	}
	if config.Client != nil {
		config.Client = mergeClientConfig(config.Client, types.DefaultClientConfig())
	}

	return config, nil
}

// mergeAgentConfig merges loaded config with defaults, with loaded values taking precedence
func mergeAgentConfig(loaded *types.AgentConfig, defaults types.AgentConfig) *types.AgentConfig {
	// Create result from defaults using struct literals for clarity
	result := types.AgentConfig{
		ID:                coalesce(loaded.ID, defaults.ID),
		ControllerURL:     coalescePtr(loaded.ControllerURL, defaults.ControllerURL),
		ListenAddr:        coalescePtr(loaded.ListenAddr, defaults.ListenAddr),
		AdvertiseAddr:     coalescePtr(loaded.AdvertiseAddr, defaults.AdvertiseAddr),
		HeartbeatInterval: coalesce(loaded.HeartbeatInterval, defaults.HeartbeatInterval),
		Storage: types.StorageConfig{
			BasePath: coalesce(loaded.Storage.BasePath, defaults.Storage.BasePath),
			Cache: types.CacheConfig{
				RAMLimit:  coalesceBytes(loaded.Storage.Cache.RAMLimit, defaults.Storage.Cache.RAMLimit),
				DiskLimit: coalesceBytes(loaded.Storage.Cache.DiskLimit, defaults.Storage.Cache.DiskLimit),
			},
		},
	}

	// Auto-detect advertise address if not explicitly set
	if result.AdvertiseAddr == nil && result.ListenAddr != nil {
		result.AdvertiseAddr = autoDetectAdvertiseAddr(result.ListenAddr)
	}

	return &result
}

// Helper functions to reduce repetitive conditional logic
func coalesce[T comparable](loaded, defaultVal T) T {
	var zero T
	if loaded != zero {
		return loaded
	}
	return defaultVal
}

func coalescePtr[T any](loaded, defaultVal *T) *T {
	if loaded != nil {
		return loaded
	}
	return defaultVal
}

func coalesceBytes(loaded, defaultVal types.Bytes) types.Bytes {
	if loaded != 0 {
		return loaded
	}
	return defaultVal
}

// autoDetectAdvertiseAddr derives advertise address from listen address
func autoDetectAdvertiseAddr(listenAddr *url.URL) *url.URL {
	if listenAddr == nil {
		return nil
	}
	host := listenAddr.Hostname()
	port := listenAddr.Port()
	if host == "0.0.0.0" {
		host = "localhost"
	}
	parsed, _ := url.Parse(fmt.Sprintf("%s://%s:%s", listenAddr.Scheme, host, port))
	return parsed
}

// mergeControllerConfig merges loaded config with defaults
func mergeControllerConfig(loaded *types.ControllerConfig, defaults types.ControllerConfig) *types.ControllerConfig {
	return &types.ControllerConfig{
		ListenAddr:   coalescePtr(loaded.ListenAddr, defaults.ListenAddr),
		AgentTimeout: coalesce(loaded.AgentTimeout, defaults.AgentTimeout),
		SyncInterval: coalesce(loaded.SyncInterval, defaults.SyncInterval),
	}
}

// mergeClientConfig merges loaded config with defaults
func mergeClientConfig(loaded *types.ClientConfig, defaults types.ClientConfig) *types.ClientConfig {
	return &types.ClientConfig{
		ControllerURL: coalescePtr(loaded.ControllerURL, defaults.ControllerURL),
	}
}

// fileExists checks if a file exists
func fileExists(filename string) bool {
	info, err := os.Stat(filename)
	if os.IsNotExist(err) {
		return false
	}
	return !info.IsDir()
}

// ValidateProviders validates provider configurations using struct literal validation rules
func ValidateProviders(providers map[string]types.ProviderConfig) error {
	// Define validation rules as struct literals
	validationRules := struct {
		supportedTypes map[string]bool
		requiredFields map[string][]string
	}{
		supportedTypes: map[string]bool{
			"hf":   true,
			"s3":   true,
			"http": true,
			"peer": true,
		},
		requiredFields: map[string][]string{
			"hf":   {"type"},
			"s3":   {"type"},
			"http": {"type"},
			"peer": {"type"},
		},
	}

	for id, cfg := range providers {
		// Validate provider type
		if !validationRules.supportedTypes[cfg.Type] {
			return fmt.Errorf("unsupported provider type '%s' for provider '%s'", cfg.Type, id)
		}

		// Validate required fields
		if requiredFields, exists := validationRules.requiredFields[cfg.Type]; exists {
			for _, field := range requiredFields {
				switch field {
				case "type":
					if cfg.Type == "" {
						return fmt.Errorf("provider '%s' missing required field: %s", id, field)
					}
				}
			}
		}

		// Validate ID consistency
		if cfg.ID != "" && cfg.ID != id {
			return fmt.Errorf("provider ID '%s' doesn't match key '%s'", cfg.ID, id)
		}

		// Set ID if not specified
		if cfg.ID == "" {
			cfg.ID = id
		}

		// Apply default transfer config if not specified
		if cfg.Transfer == nil {
			defaultTransfer := types.DefaultTransferConfig()
			cfg.Transfer = &defaultTransfer
		}

		// Update the provider in the map with any modifications
		providers[id] = cfg
	}

	return nil
}

// ResolveConfigPath resolves a config file path, checking common locations
func ResolveConfigPath(configFile string) string {
	if configFile != "" {
		if filepath.IsAbs(configFile) || fileExists(configFile) {
			return configFile
		}
	}

	// Define common paths as struct literal
	commonPaths := []string{
		"config.yaml",
		"config.yml",
		"/etc/syncbit/config.yaml",
		"/etc/syncbit/config.yml",
	}

	for _, path := range commonPaths {
		if fileExists(path) {
			return path
		}
	}

	return configFile // Return original even if it doesn't exist
}
