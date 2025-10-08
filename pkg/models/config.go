package models

import "time"

// Config represents the server-side configuration
type Config struct {
	Server          ServerConfig          `yaml:"server"`
	MongoDB         MongoDBConfig         `yaml:"mongodb"`
	WebSocket       WebSocketConfig       `yaml:"websocket"`
	Sync            SyncConfig            `yaml:"sync"`
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
	DataTimeout  time.Duration `yaml:"data_timeout"`
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
	InitialSync          bool            `yaml:"initial_sync"`
	RealtimeSync         bool            `yaml:"realtime_sync"`
	ResumableInitialSync bool            `yaml:"resumable_initial_sync"` // Enable resumable initial sync to avoid full re-sync on restart
	BatchSize            int             `yaml:"batch_size"`
	ParallelCollections  bool            `yaml:"parallel_collections"`
	MaxWorkers           int             `yaml:"max_workers"`
	Transport            TransportConfig `yaml:"transport"` // Transport configuration for initial data dump
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

// TrackingConfig holds configuration for the watermark-based tracking system
type TrackingConfig struct {
	Enabled         bool   `yaml:"enabled"`
	Database        string `yaml:"database"`
	StateCollection string `yaml:"state_collection"`
	BatchCollection string `yaml:"batch_collection"`
}

// InternalClusterConfig defines internal clustering settings
type InternalClusterConfig struct {
	Enabled          bool                   `yaml:"enabled" json:"enabled"`
	EventCoordinator EventCoordinatorConfig `yaml:"event_coordinator" json:"event_coordinator"`
	EventBuffer      EventBufferConfig      `yaml:"event_buffer" json:"event_buffer"`
	WorkerPool       WorkerPoolConfig       `yaml:"worker_pool" json:"worker_pool"`
	Metrics          MetricsConfig          `yaml:"metrics" json:"metrics"`
}

// EventCoordinatorConfig holds configuration for the event coordinator
type EventCoordinatorConfig struct {
	InputQueueSize   int           `yaml:"input_queue_size" json:"input_queue_size"`
	OutputQueueSize  int           `yaml:"output_queue_size" json:"output_queue_size"`
	BatchSize        int           `yaml:"batch_size" json:"batch_size"`
	BatchTimeout     time.Duration `yaml:"batch_timeout" json:"batch_timeout"`
	DistributionMode string        `yaml:"distribution_mode" json:"distribution_mode"` // "broadcast", "round_robin", "hash"
	EnableDedup      bool          `yaml:"enable_dedup" json:"enable_dedup"`
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
	Enabled            bool          `yaml:"enabled" json:"enabled"`
	CollectionInterval time.Duration `yaml:"collection_interval" json:"collection_interval"`
	RetentionPeriod    time.Duration `yaml:"retention_period" json:"retention_period"`
	ExportEndpoint     string        `yaml:"export_endpoint" json:"export_endpoint"`
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

// TransportConfig defines transport layer settings for data transfer
type TransportConfig struct {
	Mode              string            `yaml:"mode" json:"mode"`                                           // "tcp" or "http"
	TCPSender         TCPSenderConfig   `yaml:"tcp_sender,omitempty" json:"tcp_sender,omitempty"`           // TCP sender config for cloud-sync
	TCPReceiver       TCPReceiverConfig `yaml:"tcp_receiver,omitempty" json:"tcp_receiver,omitempty"`       // TCP receiver config for vm-sync
	HTTPFallback      bool              `yaml:"http_fallback" json:"http_fallback"`                         // Enable HTTP fallback if TCP fails
	CompressionType   string            `yaml:"compression_type" json:"compression_type"`                   // "zstd", "lz4", or "none"
	HealthMonitor     time.Duration     `yaml:"health_monitor" json:"health_monitor"`                       // TCP health monitor interval (default: 30s)
}

// TCPSenderConfig defines TCP sender configuration
type TCPSenderConfig struct {
	Address       string        `yaml:"address" json:"address"`               // Target address for TCP connection
	ParallelConns int           `yaml:"parallel_conns" json:"parallel_conns"` // Number of parallel connections
	WindowSize    int           `yaml:"window_size" json:"window_size"`       // Sliding window size
	BatchTimeout  time.Duration `yaml:"batch_timeout" json:"batch_timeout"`   // Timeout for batch operations
	ConnTimeout   time.Duration `yaml:"conn_timeout" json:"conn_timeout"`     // Connection timeout
	KeepAlive     time.Duration `yaml:"keep_alive" json:"keep_alive"`         // Keep alive duration
	MaxRetries    int           `yaml:"max_retries" json:"max_retries"`       // Maximum retry attempts
	RetryBackoff  time.Duration `yaml:"retry_backoff" json:"retry_backoff"`   // Retry backoff duration
	BufferSize    int           `yaml:"buffer_size" json:"buffer_size"`       // Buffer size for connections
	MaxBatchSize  int           `yaml:"max_batch_size" json:"max_batch_size"` // Maximum batch size
	TLSEnabled    bool          `yaml:"tls_enabled" json:"tls_enabled"`       // Enable TLS encryption
}

// TCPReceiverConfig defines TCP receiver configuration
type TCPReceiverConfig struct {
	ListenAddr        string        `yaml:"listen_addr" json:"listen_addr"`               // Address to listen on
	MaxConnections    int           `yaml:"max_connections" json:"max_connections"`       // Maximum concurrent connections
	ReadTimeout       time.Duration `yaml:"read_timeout" json:"read_timeout"`             // Read timeout
	WriteTimeout      time.Duration `yaml:"write_timeout" json:"write_timeout"`           // Write timeout
	BufferSize        int           `yaml:"buffer_size" json:"buffer_size"`               // Buffer size
	DiskCheckpoint    bool          `yaml:"disk_checkpoint" json:"disk_checkpoint"`       // Enable disk-based checkpointing
	CheckpointDir     string        `yaml:"checkpoint_dir" json:"checkpoint_dir"`         // Directory for checkpoints
	HeartbeatInterval time.Duration `yaml:"heartbeat_interval" json:"heartbeat_interval"` // Heartbeat interval
	MaxBatchSize      int           `yaml:"max_batch_size" json:"max_batch_size"`         // Maximum batch size
	TLSEnabled        bool          `yaml:"tls_enabled" json:"tls_enabled"`               // Enable TLS encryption
}
