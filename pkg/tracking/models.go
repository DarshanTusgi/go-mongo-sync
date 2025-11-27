package tracking

import (
	"fmt"
	"time"

	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
)

// TransferRecord struct removed - replaced by watermark-based tracking

// WatermarkState represents the current watermark position for a collection
type WatermarkState struct {
	// MongoDB operation time (from change stream)
	OperationTime *primitive.Timestamp `bson:"operation_time,omitempty" json:"operation_time,omitempty"`
	// Resume token for change stream continuation
	ResumeToken interface{} `bson:"resume_token,omitempty" json:"resume_token,omitempty"`
	// Last processed document ID for initial sync
	LastDocumentID *primitive.ObjectID `bson:"last_document_id,omitempty" json:"last_document_id,omitempty"`
	// Sync mode: "initial", "incremental", "completed"
	SyncMode string `bson:"sync_mode" json:"sync_mode"`
	// Number of documents processed in current session
	DocumentsProcessed int64 `bson:"documents_processed" json:"documents_processed"`
	// Last update timestamp
	LastUpdated time.Time `bson:"last_updated" json:"last_updated"`
}

// ClientSyncState represents the watermark-based sync state for a client
type ClientSyncState struct {
	ID           primitive.ObjectID `bson:"_id,omitempty" json:"id"`
	ClientID     string             `bson:"client_id" json:"client_id"`
	Database     string             `bson:"database" json:"database"`
	Collection   string             `bson:"collection" json:"collection"`
	LastSyncedAt time.Time          `bson:"last_synced_at" json:"last_synced_at"`

	// Watermark-based tracking fields
	Watermark               *WatermarkState      `bson:"watermark,omitempty" json:"watermark,omitempty"`
	LastProcessedOpTime     *primitive.Timestamp `bson:"last_processed_optime,omitempty" json:"last_processed_optime,omitempty"`
	LastProcessedDocumentID *primitive.ObjectID  `bson:"last_processed_document_id,omitempty" json:"last_processed_document_id,omitempty"`

	// Legacy fields (kept for backward compatibility)
	LastSyncedDocumentID      *primitive.ObjectID `bson:"last_synced_document_id,omitempty" json:"last_synced_document_id,omitempty"`
	TotalDocumentsTransferred int64               `bson:"total_documents_transferred" json:"total_documents_transferred"`
	InitialSyncCompleted      bool                `bson:"initial_sync_completed" json:"initial_sync_completed"`

	CreatedAt time.Time `bson:"created_at" json:"created_at"`
	UpdatedAt time.Time `bson:"updated_at" json:"updated_at"`
}

// TransferBatch represents a watermark-based batch of documents transferred together
type TransferBatch struct {
	ID            primitive.ObjectID `bson:"_id,omitempty" json:"id"`
	BatchID       string             `bson:"batch_id" json:"batch_id"`
	ClientID      string             `bson:"client_id" json:"client_id"`
	Database      string             `bson:"database" json:"database"`
	Collection    string             `bson:"collection" json:"collection"`
	DocumentCount int                `bson:"document_count" json:"document_count"`

	// Watermark tracking fields
	StartWatermark *WatermarkState `bson:"start_watermark,omitempty" json:"start_watermark,omitempty"`
	EndWatermark   *WatermarkState `bson:"end_watermark,omitempty" json:"end_watermark,omitempty"`
	SyncMode       string          `bson:"sync_mode" json:"sync_mode"` // "initial", "incremental"

	StartedAt    time.Time  `bson:"started_at" json:"started_at"`
	CompletedAt  *time.Time `bson:"completed_at,omitempty" json:"completed_at,omitempty"`
	Status       string     `bson:"status" json:"status"` // "in_progress", "completed", "failed"
	ErrorMessage string     `bson:"error_message,omitempty" json:"error_message,omitempty"`
	CreatedAt    time.Time  `bson:"created_at" json:"created_at"`
	UpdatedAt    time.Time  `bson:"updated_at" json:"updated_at"`
}

// TransferConfig holds configuration for the watermark-based tracking system
type TransferConfig struct {
	MongoClient     *mongo.Client // Reuse existing MongoDB client
	MongoURI        string        `yaml:"mongo_uri"`
	Database        string        `yaml:"database"`
	StateCollection string        `yaml:"state_collection"`
	BatchCollection string        `yaml:"batch_collection"`
	Enabled         bool          `yaml:"enabled"`
}

// Constants for transfer status
const (
	TransferStatusInProgress = "in_progress"
	TransferStatusCompleted  = "completed"
	TransferStatusFailed     = "failed"
)

// Constants for watermark sync modes
const (
	SyncModeInitial     = "initial"
	SyncModeIncremental = "incremental"
	SyncModeCompleted   = "completed"
)

// Constants for watermark tracking
const (
	// Default batch size for watermark-based sync
	DefaultWatermarkBatchSize = 1000
	// Maximum time to wait for watermark updates
	WatermarkUpdateTimeout = 30 * time.Second
	// Watermark collection names
	DefaultStateCollection = "client_sync_states"
	DefaultBatchCollection = "transfer_batches"
)

// Helper methods for WatermarkState

// IsInitialSync returns true if this is an initial sync
func (w *WatermarkState) IsInitialSync() bool {
	return w.SyncMode == SyncModeInitial
}

// IsIncremental returns true if this is an incremental sync
func (w *WatermarkState) IsIncremental() bool {
	return w.SyncMode == SyncModeIncremental
}

// IsCompleted returns true if sync is completed
func (w *WatermarkState) IsCompleted() bool {
	return w.SyncMode == SyncModeCompleted
}

// UpdateProgress updates the watermark progress
func (w *WatermarkState) UpdateProgress(documentsProcessed int64) {
	w.DocumentsProcessed += documentsProcessed
	w.LastUpdated = time.Now()
}

// Helper methods for ClientSyncState

// GetWatermarkKey returns a unique key for this client's watermark
func (c *ClientSyncState) GetWatermarkKey() string {
	return fmt.Sprintf("%s:%s:%s", c.ClientID, c.Database, c.Collection)
}

// HasWatermark returns true if watermark is set
func (c *ClientSyncState) HasWatermark() bool {
	return c.Watermark != nil
}

// IsInitialSyncComplete returns true if initial sync is complete
func (c *ClientSyncState) IsInitialSyncComplete() bool {
	return c.HasWatermark() && c.Watermark.IsCompleted()
}
