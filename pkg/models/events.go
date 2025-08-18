package models

import (
	"time"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
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
	ResumeToken   bson.Raw  `json:"resumeToken,omitempty" bson:"_id,omitempty"`
	ClusterTime   bson.Raw  `json:"clusterTime,omitempty" bson:"clusterTime,omitempty"`
	
	// Sequence numbers for exactly-once delivery
	SequenceID    int64     `json:"sequenceId,omitempty" bson:"sequenceId,omitempty"`
	BatchID       string    `json:"batchId,omitempty" bson:"batchId,omitempty"`
	EventID       string    `json:"eventId,omitempty" bson:"eventId,omitempty"`
	
	// Snapshot coordination fields
	IsSnapshot    bool      `json:"isSnapshot,omitempty" bson:"isSnapshot,omitempty"`
	SnapshotToken bson.Raw  `json:"snapshotToken,omitempty" bson:"snapshotToken,omitempty"`
	AtClusterTime *primitive.Timestamp `json:"atClusterTime,omitempty" bson:"atClusterTime,omitempty"`
	
	// DDL/Invalidate handling
	IsInvalidate  bool      `json:"isInvalidate,omitempty" bson:"isInvalidate,omitempty"`
	InvalidateReason string `json:"invalidateReason,omitempty" bson:"invalidateReason,omitempty"`
}

// DataRequest represents a request for initial data
type DataRequest struct {
	Database       string `json:"database"`
	Collection     string `json:"collection"`
	CountOnly      bool   `json:"countOnly,omitempty"`
	PartitionIndex *int   `json:"partitionIndex,omitempty"`
	UsePartitioning bool  `json:"usePartitioning,omitempty"`
}

// DataResponse represents the response with data using BSON for type preservation
type DataResponse struct {
	Database      string                   `json:"database" bson:"database"`
	Collection    string                   `json:"collection" bson:"collection"`
	Documents     []bson.Raw               `json:"documents,omitempty" bson:"documents,omitempty"`
	Count         int64                    `json:"count" bson:"count"`
	Error         string                   `json:"error,omitempty" bson:"error,omitempty"`
	SnapshotFence *SnapshotFenceInfo       `json:"snapshot_fence,omitempty" bson:"snapshot_fence,omitempty"`
	PartitionInfo *PartitionInfo           `json:"partition_info,omitempty" bson:"partition_info,omitempty"`
	Partitions    []*PartitionInfo         `json:"partitions,omitempty" bson:"partitions,omitempty"`
	// Index and collection metadata synchronization
	Indexes       []IndexInfo              `json:"indexes,omitempty" bson:"indexes,omitempty"`
	CollectionOptions *CollectionOptions   `json:"collection_options,omitempty" bson:"collection_options,omitempty"`
}

// SnapshotFenceInfo contains cluster time information for snapshot coordination
type SnapshotFenceInfo struct {
	ClusterTime   *primitive.Timestamp `json:"cluster_time,omitempty" bson:"cluster_time,omitempty"`
	OperationTime *primitive.Timestamp `json:"operation_time,omitempty" bson:"operation_time,omitempty"`
	CapturedAt    time.Time            `json:"captured_at" bson:"captured_at"`
}

// PartitionInfo contains information about a data partition
type PartitionInfo struct {
	PartitionIndex  int                  `json:"partition_index" bson:"partition_index"`
	TotalPartitions int                  `json:"total_partitions" bson:"total_partitions"`
	MinID           *primitive.ObjectID  `json:"min_id,omitempty" bson:"min_id,omitempty"`
	MaxID           *primitive.ObjectID  `json:"max_id,omitempty" bson:"max_id,omitempty"`
	IsFirst         bool                 `json:"is_first" bson:"is_first"`
	IsLast          bool                 `json:"is_last" bson:"is_last"`
	EstCount        int64                `json:"est_count" bson:"est_count"`
}

// IndexInfo represents a MongoDB index definition
type IndexInfo struct {
	Name       string   `json:"name" bson:"name"`
	Keys       bson.Raw `json:"keys" bson:"keys"`
	Unique     bool     `json:"unique,omitempty" bson:"unique,omitempty"`
	Sparse     bool     `json:"sparse,omitempty" bson:"sparse,omitempty"`
	Background bool     `json:"background,omitempty" bson:"background,omitempty"`
	TTL        *int32   `json:"expireAfterSeconds,omitempty" bson:"expireAfterSeconds,omitempty"`
	PartialFilterExpression bson.Raw `json:"partialFilterExpression,omitempty" bson:"partialFilterExpression,omitempty"`
	Collation  bson.Raw `json:"collation,omitempty" bson:"collation,omitempty"`
	Options    bson.Raw `json:"options,omitempty" bson:"options,omitempty"`
}

// CollectionOptions represents MongoDB collection options
type CollectionOptions struct {
	Capped         bool     `json:"capped,omitempty" bson:"capped,omitempty"`
	Size           *int64   `json:"size,omitempty" bson:"size,omitempty"`
	Max            *int64   `json:"max,omitempty" bson:"max,omitempty"`
	Validator      bson.Raw `json:"validator,omitempty" bson:"validator,omitempty"`
	ValidationLevel string  `json:"validationLevel,omitempty" bson:"validationLevel,omitempty"`
	ValidationAction string `json:"validationAction,omitempty" bson:"validationAction,omitempty"`
	Collation      bson.Raw `json:"collation,omitempty" bson:"collation,omitempty"`
	ChangeStreamPreAndPostImages bson.Raw `json:"changeStreamPreAndPostImages,omitempty" bson:"changeStreamPreAndPostImages,omitempty"`
}