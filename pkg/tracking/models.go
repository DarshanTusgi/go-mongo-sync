package tracking

import (
	"time"

	"go.mongodb.org/mongo-driver/bson/primitive"
)

// TransferRecord represents a record of data transferred to a specific client
type TransferRecord struct {
	ID               primitive.ObjectID `bson:"_id,omitempty" json:"id"`
	ClientID         string             `bson:"client_id" json:"client_id"`
	Database         string             `bson:"database" json:"database"`
	Collection       string             `bson:"collection" json:"collection"`
	DocumentID       primitive.ObjectID `bson:"document_id" json:"document_id"`
	TransferredAt    time.Time          `bson:"transferred_at" json:"transferred_at"`
	TransferBatchID  string             `bson:"transfer_batch_id" json:"transfer_batch_id"`
	Checksum         string             `bson:"checksum,omitempty" json:"checksum,omitempty"`
	CreatedAt        time.Time          `bson:"created_at" json:"created_at"`
	UpdatedAt        time.Time          `bson:"updated_at" json:"updated_at"`
}

// ClientSyncState represents the overall sync state for a client
type ClientSyncState struct {
	ID                    primitive.ObjectID `bson:"_id,omitempty" json:"id"`
	ClientID              string             `bson:"client_id" json:"client_id"`
	Database              string             `bson:"database" json:"database"`
	Collection            string             `bson:"collection" json:"collection"`
	LastSyncedAt          time.Time          `bson:"last_synced_at" json:"last_synced_at"`
	LastSyncedDocumentID  *primitive.ObjectID `bson:"last_synced_document_id,omitempty" json:"last_synced_document_id,omitempty"`
	TotalDocumentsTransferred int64          `bson:"total_documents_transferred" json:"total_documents_transferred"`
	InitialSyncCompleted  bool               `bson:"initial_sync_completed" json:"initial_sync_completed"`
	CreatedAt             time.Time          `bson:"created_at" json:"created_at"`
	UpdatedAt             time.Time          `bson:"updated_at" json:"updated_at"`
}

// TransferBatch represents a batch of documents transferred together
type TransferBatch struct {
	ID                primitive.ObjectID `bson:"_id,omitempty" json:"id"`
	BatchID           string             `bson:"batch_id" json:"batch_id"`
	ClientID          string             `bson:"client_id" json:"client_id"`
	Database          string             `bson:"database" json:"database"`
	Collection        string             `bson:"collection" json:"collection"`
	DocumentCount     int                `bson:"document_count" json:"document_count"`
	StartedAt         time.Time          `bson:"started_at" json:"started_at"`
	CompletedAt       *time.Time         `bson:"completed_at,omitempty" json:"completed_at,omitempty"`
	Status            string             `bson:"status" json:"status"` // "in_progress", "completed", "failed"
	ErrorMessage      string             `bson:"error_message,omitempty" json:"error_message,omitempty"`
	CreatedAt         time.Time          `bson:"created_at" json:"created_at"`
	UpdatedAt         time.Time          `bson:"updated_at" json:"updated_at"`
}

// TransferConfig holds configuration for the transfer tracking system
type TransferConfig struct {
	MongoURI           string `yaml:"mongo_uri"`
	Database           string `yaml:"database"`
	TransferCollection string `yaml:"transfer_collection"`
	StateCollection    string `yaml:"state_collection"`
	BatchCollection    string `yaml:"batch_collection"`
	Enabled            bool   `yaml:"enabled"`
}

// Constants for transfer status
const (
	TransferStatusInProgress = "in_progress"
	TransferStatusCompleted  = "completed"
	TransferStatusFailed     = "failed"
)