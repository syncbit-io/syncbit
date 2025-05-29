package config

import (
	"fmt"
	"os"

	"gopkg.in/yaml.v3"
)

// LoadYAML loads a YAML configuration file into the provided config structure.
// If the file doesn't exist or can't be parsed, it returns the original config unchanged
// and logs the error (but doesn't fail).
func LoadYAML(configFile string, config interface{}) error {
	if configFile == "" {
		return fmt.Errorf("config file path is empty")
	}

	f, err := os.Open(configFile)
	if err != nil {
		// File doesn't exist or can't be opened - this is often fine for optional config files
		return fmt.Errorf("failed to open config file %s: %w", configFile, err)
	}
	defer f.Close()

	if err := yaml.NewDecoder(f).Decode(config); err != nil {
		return fmt.Errorf("failed to decode YAML config from %s: %w", configFile, err)
	}

	return nil
}

// LoadYAMLWithDefaults loads a YAML configuration file into the provided config structure.
// If loading fails, it silently continues with the existing values in config.
// This is useful when you want to load optional configuration files without failing.
func LoadYAMLWithDefaults(configFile string, config interface{}) {
	if configFile == "" {
		return
	}

	f, err := os.Open(configFile)
	if err != nil {
		// Silently continue with defaults
		return
	}
	defer f.Close()

	// Ignore decode errors and continue with defaults
	_ = yaml.NewDecoder(f).Decode(config)
}

// SaveYAML saves a configuration structure to a YAML file.
func SaveYAML(configFile string, config interface{}) error {
	if configFile == "" {
		return fmt.Errorf("config file path is empty")
	}

	f, err := os.Create(configFile)
	if err != nil {
		return fmt.Errorf("failed to create config file %s: %w", configFile, err)
	}
	defer f.Close()

	encoder := yaml.NewEncoder(f)
	encoder.SetIndent(2) // Use 2-space indentation for readability

	if err := encoder.Encode(config); err != nil {
		return fmt.Errorf("failed to encode YAML config to %s: %w", configFile, err)
	}

	if err := encoder.Close(); err != nil {
		return fmt.Errorf("failed to finalize YAML encoding to %s: %w", configFile, err)
	}

	return nil
}

// FileExists checks if a configuration file exists and is readable.
func FileExists(configFile string) bool {
	if configFile == "" {
		return false
	}

	_, err := os.Stat(configFile)
	return err == nil
}

// LoadYAMLOrCreate loads a YAML configuration file, or creates it with default values if it doesn't exist.
func LoadYAMLOrCreate(configFile string, config interface{}) error {
	if FileExists(configFile) {
		return LoadYAML(configFile, config)
	}

	// File doesn't exist, create it with the current config values
	return SaveYAML(configFile, config)
}
