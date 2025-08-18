package models

import "time"

// Config represents the server-side configuration
type Config struct {
	Server          ServerConfig          `yaml:"server"`
	MongoDB         MongoDBConfig         `yaml:"mongodb"`
	WebSocket       WebSocketConfig       `yaml:"websocket"`
	InternalCluster InternalClusterConfig `yaml:"internal_cluster"`
	Encryption      EncryptionConfig      `yaml:"encryption"`
	Checkpoint      CheckpointConfig      `yaml:"checkpoint"`
	Tracking        TrackingConfig        `yaml:"tracking"`
	Sequence        SequenceConfig        `yaml:"sequence"`
	Fence           FenceConfig           `yaml:"fence"`
}

// CloudSyncConfig represents the client-side configuration
type CloudSyncConfig struct {
	CloudSync       CloudSyncSettings     `yaml:"cloud_sync"`
	MongoDB         MongoDBConfig         `yaml:"mongodb"`
	Sync            SyncConfig            `yaml:"sync"`
	InternalCluster InternalClusterConfig `yaml:"internal_cluster"`
	Encryption      EncryptionConfig      `yaml:"encryption"`
	Watermarks      WatermarkConfig       `yaml:"watermarks"`
	Sequence        SequenceConfig        `yaml:"sequence"`
	Fence           FenceConfig           `yaml:"fence"`
}

// MongoDBConfig represents MongoDB connection settings
type MongoDBConfig struct {
	URI       string           `yaml:"uri"`
	Timeout   time.Duration    `yaml:"timeout"`
	Databases []DatabaseConfig `yaml:"databases"`
}

// ServerConfig represents HTTP server settings
type ServerConfig struct {
	Port         int           `yaml:"port"`
	Host         string        `yaml:"host"`
	ReadTimeout  time.Duration `yaml:"read_timeout"`
	WriteTimeout time.Duration `yaml:"write_timeout"`
	IdleTimeout  time.Duration `yaml:"idle_timeout"`
}

// WebSocketConfig represents WebSocket settings
type WebSocketConfig struct {
	Endpoint        string   `yaml:"endpoint"`
	AllowedOrigins  []string `yaml:"allowed_origins"`
	ReadBufferSize  int      `yaml:"read_buffer_size"`
	WriteBufferSize int      `yaml:"write_buffer_size"`
}

// CloudSyncSettings represents cloud sync connection settings
type CloudSyncSettings struct {
	HTTPURL string `yaml:"http_url"`
	WSURL   string `yaml:"ws_url"`
}

// SyncConfig represents synchronization settings
type SyncConfig struct {
	InitialSync        bool `yaml:"initial_sync"`
	RealtimeSync       bool `yaml:"realtime_sync"`
	BatchSize          int  `yaml:"batch_size"`
	ParallelCollections bool `yaml:"parallel_collections"`
	MaxWorkers         int  `yaml:"max_workers"`
}

// DatabaseConfig represents configuration for a specific database
type DatabaseConfig struct {
	Name        string             `yaml:"name"`
	Enabled     bool               `yaml:"enabled"`
	Priority    int                `yaml:"priority"`
	Collections []CollectionConfig `yaml:"collections"`
}

// CollectionConfig represents configuration for a specific collection
type CollectionConfig struct {
	Name           string         `yaml:"name" json:"name"`
	Enabled        bool           `yaml:"enabled" json:"enabled"`
	BatchSize      int            `yaml:"batch_size,omitempty" json:"batch_size,omitempty"`
	Priority       int            `yaml:"priority,omitempty" json:"priority,omitempty"`
	FieldFilter    FieldFilter    `yaml:"field_filter,omitempty" json:"field_filter,omitempty"`
	DocumentFilter DocumentFilter `yaml:"document_filter,omitempty" json:"document_filter,omitempty"`
}

// FieldFilter defines field-level filtering
type FieldFilter struct {
	IncludeFields []string `yaml:"include_fields,omitempty" json:"include_fields,omitempty"`
	ExcludeFields []string `yaml:"exclude_fields,omitempty" json:"exclude_fields,omitempty"`
	AlwaysInclude []string `yaml:"always_include,omitempty" json:"always_include,omitempty"`
}

// DocumentFilter defines document-level filtering based on field values
type DocumentFilter struct {
	Criteria []FilterCriteria `yaml:"criteria,omitempty" json:"criteria,omitempty"`
}

// FilterCriteria defines a single filter condition
type FilterCriteria struct {
	Field    string      `yaml:"field" json:"field"`
	Operator string      `yaml:"operator" json:"operator"` // "eq", "ne", "in", "nin", "gt", "gte", "lt", "lte", "regex"
	Value    interface{} `yaml:"value" json:"value"`
}

// EncryptionConfig defines encryption settings
type EncryptionConfig struct {
	Enabled   bool   `yaml:"enabled" json:"enabled"`
	Algorithm string `yaml:"algorithm" json:"algorithm"` // AES-256-GCM
	KeyID     string `yaml:"key_id" json:"key_id"`
	Key       string `yaml:"key" json:"key"` // Base64 encoded encryption key
}

// CheckpointConfig represents resume token and checkpoint settings
type CheckpointConfig struct {
	Enabled        bool   `yaml:"enabled"`
	Database       string `yaml:"database"`
	Collection     string `yaml:"collection"`
	SaveInterval   int    `yaml:"save_interval_seconds"`
	CleanupEnabled bool   `yaml:"cleanup_enabled"`
	RetentionDays  int    `yaml:"retention_days"`
}

// TrackingConfig holds configuration for the transfer tracking system
type TrackingConfig struct {
	Enabled            bool   `yaml:"enabled"`
	Database           string `yaml:"database"`
	TransferCollection string `yaml:"transfer_collection"`
	StateCollection    string `yaml:"state_collection"`
	BatchCollection    string `yaml:"batch_collection"`
}



// InternalClusterConfig defines internal clustering settings
type InternalClusterConfig struct {
	Enabled           bool                    `yaml:"enabled" json:"enabled"`
	EventCoordinator  EventCoordinatorConfig  `yaml:"event_coordinator" json:"event_coordinator"`
	EventBuffer       EventBufferConfig       `yaml:"event_buffer" json:"event_buffer"`
	WorkerPool        WorkerPoolConfig        `yaml:"worker_pool" json:"worker_pool"`
	Metrics           MetricsConfig           `yaml:"metrics" json:"metrics"`
}

// EventCoordinatorConfig holds configuration for the event coordinator
type EventCoordinatorConfig struct {
	InputQueueSize    int           `yaml:"input_queue_size" json:"input_queue_size"`
	OutputQueueSize   int           `yaml:"output_queue_size" json:"output_queue_size"`
	BatchSize         int           `yaml:"batch_size" json:"batch_size"`
	BatchTimeout      time.Duration `yaml:"batch_timeout" json:"batch_timeout"`
	DistributionMode  string        `yaml:"distribution_mode" json:"distribution_mode"` // "broadcast", "round_robin", "hash"
	EnableDedup       bool          `yaml:"enable_dedup" json:"enable_dedup"`
}

// EventBufferConfig holds configuration for the event buffer
type EventBufferConfig struct {
	MaxSize         int           `yaml:"max_size" json:"max_size"`
	TTL             time.Duration `yaml:"ttl" json:"ttl"`
	CleanupInterval time.Duration `yaml:"cleanup_interval" json:"cleanup_interval"`
}

// WorkerPoolConfig holds configuration for the worker pool
type WorkerPoolConfig struct {
	WorkerCount    int           `yaml:"worker_count" json:"worker_count"`
	QueueSize      int           `yaml:"queue_size" json:"queue_size"`
	ProcessTimeout time.Duration `yaml:"process_timeout" json:"process_timeout"`
	LoadBalancing  string        `yaml:"load_balancing" json:"load_balancing"` // "round_robin", "least_busy", "random"
}

// MetricsConfig holds configuration for metrics collection
type MetricsConfig struct {
	Enabled           bool          `yaml:"enabled" json:"enabled"`
	CollectionInterval time.Duration `yaml:"collection_interval" json:"collection_interval"`
	RetentionPeriod   time.Duration `yaml:"retention_period" json:"retention_period"`
	ExportEndpoint    string        `yaml:"export_endpoint" json:"export_endpoint"`
}

// WatermarkConfig holds configuration for watermark management
type WatermarkConfig struct {
	Enabled    bool   `yaml:"enabled" json:"enabled"`
	MongoURI   string `yaml:"mongo_uri" json:"mongo_uri"`
	Database   string `yaml:"database" json:"database"`
	Collection string `yaml:"collection" json:"collection"`
}

// SequenceConfig holds configuration for sequence generation
type SequenceConfig struct {
	Enabled    bool   `yaml:"enabled" json:"enabled"`
	MongoURI   string `yaml:"mongo_uri" json:"mongo_uri"`
	Database   string `yaml:"database" json:"database"`
	Collection string `yaml:"collection" json:"collection"`
	BatchSize  int64  `yaml:"batch_size" json:"batch_size"`
	NodeID     string `yaml:"node_id" json:"node_id"`
}

// FenceConfig holds configuration for cluster time fencing
type FenceConfig struct {
	Enabled  bool   `yaml:"enabled" json:"enabled"`
	MongoURI string `yaml:"mongo_uri" json:"mongo_uri"`
}