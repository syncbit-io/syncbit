package provider

import (
	"fmt"
	"os"
	"strings"

	"syncbit/internal/config"
)

// InitFromDaemonConfig initializes the provider registry from daemon configuration
// This should be called during daemon startup after loading configuration
func InitFromDaemonConfig(cfg config.DaemonConfig) error {
	// Expand environment variables in provider configurations
	expandedProviders := make(map[string]config.ProviderConfig)

	for id, providerCfg := range cfg.Providers {
		expandedCfg := expandEnvVars(providerCfg)
		expandedProviders[id] = expandedCfg
	}

	return InitializeProviders(expandedProviders)
}

// expandEnvVars expands environment variables in provider configuration
func expandEnvVars(cfg config.ProviderConfig) config.ProviderConfig {
	cfg.Token = expandString(cfg.Token)
	cfg.Region = expandString(cfg.Region)
	cfg.Profile = expandString(cfg.Profile)

	// Expand headers
	if cfg.Headers != nil {
		expandedHeaders := make(map[string]string)
		for k, v := range cfg.Headers {
			expandedHeaders[k] = expandString(v)
		}
		cfg.Headers = expandedHeaders
	}

	return cfg
}

// expandString expands environment variables in a string
// Supports ${VAR} and $VAR syntax
func expandString(s string) string {
	if s == "" {
		return s
	}

	// Simple environment variable expansion
	// This handles ${VAR} syntax
	expanded := os.ExpandEnv(s)

	// Also handle $VAR syntax without braces
	if strings.Contains(expanded, "$") {
		expanded = os.ExpandEnv(expanded)
	}

	return expanded
}

// ValidateProviders validates that all configured providers can be created
func ValidateProviders(providers map[string]config.ProviderConfig) error {
	for id, cfg := range providers {
		// Check that provider type is supported
		if _, ok := providerFactories[cfg.Type]; !ok {
			return fmt.Errorf("unsupported provider type '%s' for provider '%s'", cfg.Type, id)
		}

		// Basic validation based on provider type
		switch cfg.Type {
		case "s3":
			// S3 providers are flexible - bucket and region can be specified per-job
			// Only validate that we have some way to authenticate
			// (Profile can be empty for default credentials)

		case "hf":
			// HF providers are flexible - repo and revision are specified per-job
			// Token is optional for public repositories

		case "http":
			// HTTP providers are flexible - URLs are specified per-job
			// Token and headers are optional
		}
	}

	return nil
}
