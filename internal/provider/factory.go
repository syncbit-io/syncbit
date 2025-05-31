package provider

import (
	"fmt"
	"sync"

	"syncbit/internal/core/types"
)

// Provider factory functions for creating new provider instances
var providerFactories = make(map[string]func(types.ProviderConfig, types.TransferConfig) (Provider, error))

// Global registry of configured provider instances
var (
	providerRegistry = make(map[string]Provider)
	registryMutex    sync.RWMutex
)

// Provider defines the interface for all providers
type Provider interface {
	GetName() string
	GetID() string
}

// RegisterProviderFactory registers a provider factory function by type
func RegisterProviderFactory(providerType string, factory func(types.ProviderConfig, types.TransferConfig) (Provider, error)) {
	providerFactories[providerType] = factory
}

// InitializeProviders initializes all providers from configuration
// This should be called at daemon startup
func InitializeProviders(providers map[string]types.ProviderConfig) error {
	registryMutex.Lock()
	defer registryMutex.Unlock()

	for providerID, cfg := range providers {
		// Ensure the provider ID matches the config
		if cfg.ID == "" {
			cfg.ID = providerID
		}

		factory, ok := providerFactories[cfg.Type]
		if !ok {
			return fmt.Errorf("unknown provider type: %s for provider %s", cfg.Type, providerID)
		}

		// Use provider-specific transfer config if available, otherwise use defaults
		transferCfg := types.DefaultTransferConfig()
		if cfg.Transfer != nil {
			transferCfg = *cfg.Transfer
		}

		provider, err := factory(cfg, transferCfg)
		if err != nil {
			return fmt.Errorf("failed to create provider %s: %w", providerID, err)
		}

		providerRegistry[providerID] = provider
	}

	return nil
}

// GetProvider retrieves a provider by ID from the registry
func GetProvider(providerID string) (Provider, error) {
	registryMutex.RLock()
	defer registryMutex.RUnlock()

	provider, ok := providerRegistry[providerID]
	if !ok {
		return nil, fmt.Errorf("provider not found: %s", providerID)
	}

	return provider, nil
}

// ListProviders returns all registered provider IDs
func ListProviders() []string {
	registryMutex.RLock()
	defer registryMutex.RUnlock()

	ids := make([]string, 0, len(providerRegistry))
	for id := range providerRegistry {
		ids = append(ids, id)
	}
	return ids
}

// Legacy function for backward compatibility
func NewProvider(cfg types.ProviderConfig, transferCfg types.TransferConfig) (Provider, error) {
	if cfg.Type == "" {
		return nil, fmt.Errorf("provider type is required")
	}

	factory, ok := providerFactories[cfg.Type]
	if !ok {
		return nil, fmt.Errorf("provider not found: %s", cfg.Type)
	}
	return factory(cfg, transferCfg)
}
