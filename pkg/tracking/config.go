package tracking

import (
	"fmt"
	"os"

	"gopkg.in/yaml.v2"
)

// LoadTrackingConfig loads tracking configuration from a YAML file
func LoadTrackingConfig(configPath string) (*TransferConfig, error) {
	data, err := os.ReadFile(configPath)
	if err != nil {
		return nil, fmt.Errorf("failed to read tracking config file: %v", err)
	}

	var config TransferConfig
	if err := yaml.Unmarshal(data, &config); err != nil {
		return nil, fmt.Errorf("failed to parse tracking config: %v", err)
	}

	// Set default values if not specified
	if config.Database == "" {
		config.Database = "sync_tracking"
	}
	if config.TransferCollection == "" {
		config.TransferCollection = "transfer_records"
	}
	if config.StateCollection == "" {
		config.StateCollection = "client_sync_states"
	}
	if config.BatchCollection == "" {
		config.BatchCollection = "transfer_batches"
	}

	return &config, nil
}

// DefaultTrackingConfig returns a default tracking configuration
func DefaultTrackingConfig() *TransferConfig {
	return &TransferConfig{
		Enabled:            false,
		MongoURI:           "mongodb://localhost:27017/?replicaSet=rs0",
		Database:           "sync_tracking",
		TransferCollection: "transfer_records",
		StateCollection:    "client_sync_states",
		BatchCollection:    "transfer_batches",
	}
}