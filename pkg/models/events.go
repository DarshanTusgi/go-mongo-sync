package models

import (
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"time"
)

// ChangeEvent represents a MongoDB change event with proper BSON type preservation
// Enhanced for exactly-once semantics and restart-safety
type ChangeEvent struct {
	OperationType string    `json:"operationType" bson:"operationType"`
	Database      string    `json:"database" bson:"database"`
	Collection    string    `json:"collection" bson:"collection"`
	DocumentKey   bson.Raw  `json:"documentKey" bson:"documentKey"`
	FullDocument  bson.Raw  `json:"fullDocument,omitempty" bson:"fullDocument,omitempty"`
	Timestamp     time.Time `json:"timestamp" bson:"timestamp"`

	// Resume token for resumable synchronization
	ResumeToken bson.Raw `json:"resumeToken,omitempty" bson:"_id,omitempty"`
	ClusterTime bson.Raw `json:"clusterTime,omitempty" bson:"clusterTime,omitempty"`

	// Sequence numbers for exactly-once delivery
	SequenceID int64  `json:"sequenceId,omitempty" bson:"sequenceId,omitempty"`
	BatchID    string `json:"batchId,omitempty" bson:"batchId,omitempty"`
	EventID    string `json:"eventId,omitempty" bson:"eventId,omitempty"`

	// Snapshot coordination fields
	IsSnapshot    bool                 `json:"isSnapshot,omitempty" bson:"isSnapshot,omitempty"`
	SnapshotToken bson.Raw             `json:"snapshotToken,omitempty" bson:"snapshotToken,omitempty"`
	AtClusterTime *primitive.Timestamp `json:"atClusterTime,omitempty" bson:"atClusterTime,omitempty"`

	// DDL/Invalidate handling
	IsInvalidate     bool   `json:"isInvalidate,omitempty" bson:"isInvalidate,omitempty"`
	InvalidateReason string `json:"invalidateReason,omitempty" bson:"invalidateReason,omitempty"`
}

// DataRequest represents a request for initial data
type DataRequest struct {
	Database        string `json:"database"`
	Collection      string `json:"collection"`
	CountOnly       bool   `json:"countOnly,omitempty"`
	PartitionIndex  *int   `json:"partitionIndex,omitempty"`
	UsePartitioning bool   `json:"usePartitioning,omitempty"`
	// Pagination support
	PageSize       int    `json:"pageSize,omitempty"`       // Number of documents per page (default: 1000)
	PageNumber     int    `json:"pageNumber,omitempty"`     // Page number (0-based, default: 0)
	LastDocumentID string `json:"lastDocumentId,omitempty"` // For cursor-based pagination
}

// DataResponse represents the response with data using BSON for type preservation
type DataResponse struct {
	Database      string             `json:"database" bson:"database"`
	Collection    string             `json:"collection" bson:"collection"`
	Documents     []bson.Raw         `json:"documents,omitempty" bson:"documents,omitempty"`
	Count         int64              `json:"count" bson:"count"`
	Error         string             `json:"error,omitempty" bson:"error,omitempty"`
	SnapshotFence *SnapshotFenceInfo `json:"snapshot_fence,omitempty" bson:"snapshot_fence,omitempty"`
	PartitionInfo *PartitionInfo     `json:"partition_info,omitempty" bson:"partition_info,omitempty"`
	Partitions    []*PartitionInfo   `json:"partitions,omitempty" bson:"partitions,omitempty"`
	// Index and collection metadata synchronization
	Indexes           []IndexInfo        `json:"indexes,omitempty" bson:"indexes,omitempty"`
	CollectionOptions *CollectionOptions `json:"collection_options,omitempty" bson:"collection_options,omitempty"`
	// Pagination metadata
	Pagination *PaginationInfo `json:"pagination,omitempty" bson:"pagination,omitempty"`
}

// SnapshotFenceInfo contains cluster time information for snapshot coordination
type SnapshotFenceInfo struct {
	ClusterTime   *primitive.Timestamp `json:"cluster_time,omitempty" bson:"cluster_time,omitempty"`
	OperationTime *primitive.Timestamp `json:"operation_time,omitempty" bson:"operation_time,omitempty"`
	CapturedAt    time.Time            `json:"captured_at" bson:"captured_at"`
}

// PartitionInfo contains information about a data partition
type PartitionInfo struct {
	PartitionIndex  int                 `json:"partition_index" bson:"partition_index"`
	TotalPartitions int                 `json:"total_partitions" bson:"total_partitions"`
	MinID           *primitive.ObjectID `json:"min_id,omitempty" bson:"min_id,omitempty"`
	MaxID           *primitive.ObjectID `json:"max_id,omitempty" bson:"max_id,omitempty"`
	IsFirst         bool                `json:"is_first" bson:"is_first"`
	IsLast          bool                `json:"is_last" bson:"is_last"`
	EstCount        int64               `json:"est_count" bson:"est_count"`
}

// IndexInfo represents a MongoDB index definition
type IndexInfo struct {
	Name                    string   `json:"name" bson:"name"`
	Keys                    bson.Raw `json:"keys" bson:"keys"`
	Unique                  bool     `json:"unique,omitempty" bson:"unique,omitempty"`
	Sparse                  bool     `json:"sparse,omitempty" bson:"sparse,omitempty"`
	Background              bool     `json:"background,omitempty" bson:"background,omitempty"`
	TTL                     *int32   `json:"expireAfterSeconds,omitempty" bson:"expireAfterSeconds,omitempty"`
	PartialFilterExpression bson.Raw `json:"partialFilterExpression,omitempty" bson:"partialFilterExpression,omitempty"`
	Collation               bson.Raw `json:"collation,omitempty" bson:"collation,omitempty"`
	Options                 bson.Raw `json:"options,omitempty" bson:"options,omitempty"`
}

// CollectionOptions represents MongoDB collection options
type CollectionOptions struct {
	Capped                       bool     `json:"capped,omitempty" bson:"capped,omitempty"`
	Size                         *int64   `json:"size,omitempty" bson:"size,omitempty"`
	Max                          *int64   `json:"max,omitempty" bson:"max,omitempty"`
	Validator                    bson.Raw `json:"validator,omitempty" bson:"validator,omitempty"`
	ValidationLevel              string   `json:"validationLevel,omitempty" bson:"validationLevel,omitempty"`
	ValidationAction             string   `json:"validationAction,omitempty" bson:"validationAction,omitempty"`
	Collation                    bson.Raw `json:"collation,omitempty" bson:"collation,omitempty"`
	ChangeStreamPreAndPostImages bson.Raw `json:"changeStreamPreAndPostImages,omitempty" bson:"changeStreamPreAndPostImages,omitempty"`
}

// PageResult represents the result of fetching a single page for push-based sync
type PageResult struct {
	PageNumber        int                `json:"pageNumber" bson:"pageNumber"`
	Documents         []bson.Raw         `json:"documents" bson:"documents"`
	Indexes           []IndexInfo        `json:"indexes,omitempty" bson:"indexes,omitempty"`
	CollectionOptions *CollectionOptions `json:"collectionOptions,omitempty" bson:"collectionOptions,omitempty"`
	SnapshotFence     *SnapshotFenceInfo `json:"snapshotFence,omitempty" bson:"snapshotFence,omitempty"`
	IsLastPage        bool               `json:"isLastPage" bson:"isLastPage"`
}

// PaginationInfo contains pagination metadata for data responses
type PaginationInfo struct {
	PageSize       int    `json:"pageSize" bson:"pageSize"`                                 // Number of documents per page
	PageNumber     int    `json:"pageNumber" bson:"pageNumber"`                             // Current page number (0-based)
	TotalPages     int    `json:"totalPages" bson:"totalPages"`                             // Total number of pages
	TotalDocuments int64  `json:"totalDocuments" bson:"totalDocuments"`                     // Total number of documents
	HasNextPage    bool   `json:"hasNextPage" bson:"hasNextPage"`                           // Whether there are more pages
	HasPrevPage    bool   `json:"hasPrevPage" bson:"hasPrevPage"`                           // Whether there are previous pages
	LastDocumentID string `json:"lastDocumentId,omitempty" bson:"lastDocumentId,omitempty"` // Last document ID for cursor-based pagination
}

// TelemetryData represents runtime resource usage telemetry from VM Sync
type TelemetryData struct {
	NodeID           string    `json:"nodeId" bson:"nodeId"`
	Timestamp        time.Time `json:"timestamp" bson:"timestamp"`
	CPUUsage         float64   `json:"cpuUsage" bson:"cpuUsage"`                 // CPU usage percentage (0-100)
	MemoryUsage      float64   `json:"memoryUsage" bson:"memoryUsage"`           // Memory usage percentage (0-100)
	MemoryTotal      uint64    `json:"memoryTotal" bson:"memoryTotal"`           // Total memory in bytes
	MemoryUsed       uint64    `json:"memoryUsed" bson:"memoryUsed"`             // Used memory in bytes
	IOReadBytes      uint64    `json:"ioReadBytes" bson:"ioReadBytes"`           // Bytes read from disk
	IOWriteBytes     uint64    `json:"ioWriteBytes" bson:"ioWriteBytes"`         // Bytes written to disk
	IOReadOps        uint64    `json:"ioReadOps" bson:"ioReadOps"`               // Number of read operations
	IOWriteOps       uint64    `json:"ioWriteOps" bson:"ioWriteOps"`             // Number of write operations
	NetworkRxBytes   uint64    `json:"networkRxBytes" bson:"networkRxBytes"`     // Network bytes received
	NetworkTxBytes   uint64    `json:"networkTxBytes" bson:"networkTxBytes"`     // Network bytes transmitted
	NetworkRxPackets uint64    `json:"networkRxPackets" bson:"networkRxPackets"` // Network packets received
	NetworkTxPackets uint64    `json:"networkTxPackets" bson:"networkTxPackets"` // Network packets transmitted
	Goroutines       int       `json:"goroutines" bson:"goroutines"`             // Number of active goroutines
	GCPauses         []float64 `json:"gcPauses" bson:"gcPauses"`                 // Recent GC pause times in milliseconds
	LoadAverage      []float64 `json:"loadAverage" bson:"loadAverage"`           // System load average [1m, 5m, 15m]
	DiskUsage        float64   `json:"diskUsage" bson:"diskUsage"`               // Disk usage percentage (0-100)
	ConnectionCount  int       `json:"connectionCount" bson:"connectionCount"`   // Active database connections
	SyncLatency      float64   `json:"syncLatency" bson:"syncLatency"`           // Current sync latency in milliseconds
	QueueDepth       int       `json:"queueDepth" bson:"queueDepth"`             // Current processing queue depth
}

// TelemetryMessage represents a telemetry message sent via WebSocket
type TelemetryMessage struct {
	Type      string         `json:"type"`      // "telemetry"
	Data      *TelemetryData `json:"data"`      // Telemetry data
	Timestamp time.Time      `json:"timestamp"` // Message timestamp
}

// AdaptiveConfig represents dynamic configuration adjustments
type AdaptiveConfig struct {
	FetchParallelism int       `json:"fetchParallelism"` // Number of concurrent collection dumps/streams
	PushParallelism  int       `json:"pushParallelism"`  // Number of concurrent transfer threads
	BatchSize        int       `json:"batchSize"`        // Batch size for inserts/updates
	BackPressure     bool      `json:"backPressure"`     // Whether to apply back-pressure
	ThrottleDelay    int       `json:"throttleDelay"`    // Throttle delay in milliseconds
	MaxQueueSize     int       `json:"maxQueueSize"`     // Maximum queue size before throttling
	AdaptedAt        time.Time `json:"adaptedAt"`        // When this configuration was applied
	Reason           string    `json:"reason"`           // Reason for adaptation
}

// AdaptiveConfigMessage represents a configuration update message
type AdaptiveConfigMessage struct {
	Type      string          `json:"type"`      // "adaptive_config"
	Config    *AdaptiveConfig `json:"config"`    // New configuration
	Timestamp time.Time       `json:"timestamp"` // Message timestamp
}
