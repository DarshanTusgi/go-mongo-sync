package tracking

import (
	"context"
	"fmt"
	"log"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// TransferTracker manages watermark-based tracking operations
type TransferTracker struct {
	client          *mongo.Client
	config          *TransferConfig
	stateCollection *mongo.Collection
	batchCollection *mongo.Collection
}

// NewTransferTracker creates a new transfer tracker instance
func NewTransferTracker(config *TransferConfig) (*TransferTracker, error) {
	if !config.Enabled {
		return &TransferTracker{config: config}, nil
	}

	// Reuse existing MongoDB client if provided, otherwise create new connection
	var client *mongo.Client
	var err error
	if config.MongoClient != nil {
		client = config.MongoClient
	} else if config.MongoURI != "" {
		clientOptions := options.Client().ApplyURI(config.MongoURI)
		client, err = mongo.Connect(context.Background(), clientOptions)
		if err != nil {
			return nil, fmt.Errorf("failed to connect to MongoDB for tracking: %v", err)
		}

		// Test the connection
		if err = client.Ping(context.Background(), nil); err != nil {
			return nil, fmt.Errorf("failed to ping MongoDB for tracking: %v", err)
		}
	} else {
		return nil, fmt.Errorf("either MongoClient or MongoURI must be provided")
	}

	db := client.Database(config.Database)
	stateColl := db.Collection(config.StateCollection)
	batchColl := db.Collection(config.BatchCollection)

	tracker := &TransferTracker{
		client:          client,
		config:          config,
		stateCollection: stateColl,
		batchCollection: batchColl,
	}

	// Create indexes for better performance
	if err := tracker.createIndexes(); err != nil {
		log.Printf("Warning: Failed to create tracking indexes: %v", err)
	}

	log.Println("Transfer tracker initialized successfully")
	return tracker, nil
}

// createIndexes creates necessary indexes for tracking collections
func (tt *TransferTracker) createIndexes() error {
	ctx := context.Background()

	// Index for client sync state
	stateIndexes := []mongo.IndexModel{
		{
			Keys:    bson.D{primitive.E{Key: "client_id", Value: 1}, primitive.E{Key: "database", Value: 1}, primitive.E{Key: "collection", Value: 1}},
			Options: options.Index().SetUnique(true),
		},
	}

	// Index for transfer batches
	batchIndexes := []mongo.IndexModel{
		{
			Keys:    bson.D{primitive.E{Key: "batch_id", Value: 1}},
			Options: options.Index().SetUnique(true),
		},
		{
			Keys: bson.D{primitive.E{Key: "client_id", Value: 1}, primitive.E{Key: "status", Value: 1}},
		},
	}

	// Create indexes
	if _, err := tt.stateCollection.Indexes().CreateMany(ctx, stateIndexes); err != nil {
		return fmt.Errorf("failed to create state indexes: %v", err)
	}

	if _, err := tt.batchCollection.Indexes().CreateMany(ctx, batchIndexes); err != nil {
		return fmt.Errorf("failed to create batch indexes: %v", err)
	}

	return nil
}

// IsEnabled returns whether tracking is enabled
func (tt *TransferTracker) IsEnabled() bool {
	return tt.config.Enabled
}

// StartTransferBatch creates a new transfer batch record (legacy method, use StartWatermarkBatch for new implementations)
func (tt *TransferTracker) StartTransferBatch(clientID, database, collection string, documentCount int) (string, error) {
	if !tt.config.Enabled {
		log.Printf("DEBUG: StartTransferBatch called but tracking is disabled")
		return "", nil
	}

	// Try to get existing watermark for this client
	watermark, err := tt.GetWatermark(clientID, database, collection)
	if err != nil {
		log.Printf("Warning: Failed to get watermark for legacy batch: %v", err)
	}

	// If no watermark exists, create a basic one for legacy compatibility
	if watermark == nil {
		watermark = &WatermarkState{
			SyncMode:           SyncModeInitial,
			DocumentsProcessed: 0,
			LastUpdated:        time.Now(),
		}
	}

	// Use the new watermark-based batch creation
	return tt.StartWatermarkBatch(clientID, database, collection, documentCount, watermark)
}

// CompleteTransferBatch marks a transfer batch as completed
func (tt *TransferTracker) CompleteTransferBatch(batchID string) error {
	if !tt.config.Enabled || batchID == "" {
		return nil
	}

	ctx := context.Background()
	completedAt := time.Now()
	update := bson.M{
		"$set": bson.M{
			"status":       TransferStatusCompleted,
			"completed_at": completedAt,
			"updated_at":   completedAt,
		},
	}

	filter := bson.M{"batch_id": batchID}
	if _, err := tt.batchCollection.UpdateOne(ctx, filter, update); err != nil {
		return fmt.Errorf("failed to complete transfer batch: %v", err)
	}

	return nil
}

// FailTransferBatch marks a transfer batch as failed
func (tt *TransferTracker) FailTransferBatch(batchID string, errorMsg string) error {
	if !tt.config.Enabled || batchID == "" {
		return nil
	}

	ctx := context.Background()
	update := bson.M{
		"$set": bson.M{
			"status":        TransferStatusFailed,
			"error_message": errorMsg,
			"updated_at":    time.Now(),
		},
	}

	filter := bson.M{"batch_id": batchID}
	if _, err := tt.batchCollection.UpdateOne(ctx, filter, update); err != nil {
		return fmt.Errorf("failed to mark transfer batch as failed: %v", err)
	}

	return nil
}

// UpdateClientSyncState updates the sync state for a client (legacy method)
func (tt *TransferTracker) UpdateClientSyncState(clientID, database, collection string, lastDocumentID *primitive.ObjectID, documentsTransferred int64, initialSyncCompleted bool) error {
	if !tt.config.Enabled {
		return nil
	}

	ctx := context.Background()
	filter := bson.M{
		"client_id":  clientID,
		"database":   database,
		"collection": collection,
	}

	update := bson.M{
		"$set": bson.M{
			"last_synced_at":         time.Now(),
			"initial_sync_completed": initialSyncCompleted,
			"updated_at":             time.Now(),
		},
		"$inc": bson.M{
			"total_documents_transferred": documentsTransferred,
		},
	}

	if lastDocumentID != nil {
		update["$set"].(bson.M)["last_synced_document_id"] = *lastDocumentID
		// Also update watermark if it exists
		if state, err := tt.GetClientSyncState(clientID, database, collection); err == nil && state != nil && state.Watermark != nil {
			state.Watermark.LastDocumentID = lastDocumentID
			state.Watermark.DocumentsProcessed += documentsTransferred
			state.Watermark.LastUpdated = time.Now()
			update["$set"].(bson.M)["watermark"] = state.Watermark
		}
	}

	opts := options.Update().SetUpsert(true)
	if _, err := tt.stateCollection.UpdateOne(ctx, filter, update, opts); err != nil {
		return fmt.Errorf("failed to update client sync state: %v", err)
	}

	return nil
}

// UpdateClientSyncStateWithWatermark updates client sync state using watermark tracking
func (tt *TransferTracker) UpdateClientSyncStateWithWatermark(clientID, database, collection string, watermark *WatermarkState, documentsTransferred int64) error {
	if !tt.config.Enabled || watermark == nil {
		return nil
	}

	ctx := context.Background()
	filter := bson.M{
		"client_id":  clientID,
		"database":   database,
		"collection": collection,
	}

	// Update watermark progress
	watermark.UpdateProgress(documentsTransferred)

	update := bson.M{
		"$set": bson.M{
			"watermark":                  watermark,
			"last_synced_at":             time.Now(),
			"initial_sync_completed":     watermark.IsCompleted(),
			"last_processed_optime":      watermark.OperationTime,
			"last_processed_document_id": watermark.LastDocumentID,
			"updated_at":                 time.Now(),
		},
		"$inc": bson.M{
			"total_documents_transferred": documentsTransferred,
		},
	}

	opts := options.Update().SetUpsert(true)
	if _, err := tt.stateCollection.UpdateOne(ctx, filter, update, opts); err != nil {
		return fmt.Errorf("failed to update client sync state with watermark: %v", err)
	}

	log.Printf("Updated watermark for client %s: mode=%s, docs_processed=%d", clientID, watermark.SyncMode, watermark.DocumentsProcessed)
	return nil
}

// GetClientSyncState retrieves the sync state for a client
func (tt *TransferTracker) GetClientSyncState(clientID, database, collection string) (*ClientSyncState, error) {
	if !tt.config.Enabled {
		return nil, nil
	}

	ctx := context.Background()
	filter := bson.M{
		"client_id":  clientID,
		"database":   database,
		"collection": collection,
	}

	var state ClientSyncState
	err := tt.stateCollection.FindOne(ctx, filter).Decode(&state)
	if err != nil {
		if err == mongo.ErrNoDocuments {
			return nil, nil
		}
		return nil, fmt.Errorf("failed to get client sync state: %v", err)
	}

	return &state, nil
}

// Close closes the tracking database connection
func (tt *TransferTracker) Close() error {
	if tt.client != nil {
		return tt.client.Disconnect(context.Background())
	}
	return nil
}

// Watermark-based tracking methods

// InitializeWatermark initializes watermark tracking for a client collection
func (tt *TransferTracker) InitializeWatermark(clientID, database, collection string, syncMode string) (*WatermarkState, error) {
	if !tt.config.Enabled {
		return nil, nil
	}

	watermark := &WatermarkState{
		SyncMode:           syncMode,
		DocumentsProcessed: 0,
		LastUpdated:        time.Now(),
	}

	// Create or update client sync state with watermark
	ctx := context.Background()
	filter := bson.M{
		"client_id":  clientID,
		"database":   database,
		"collection": collection,
	}

	update := bson.M{
		"$set": bson.M{
			"watermark":      watermark,
			"last_synced_at": time.Now(),
			"updated_at":     time.Now(),
		},
		"$setOnInsert": bson.M{
			"client_id":                   clientID,
			"database":                    database,
			"collection":                  collection,
			"total_documents_transferred": int64(0),
			"initial_sync_completed":      false,
			"created_at":                  time.Now(),
		},
	}

	opts := options.Update().SetUpsert(true)
	if _, err := tt.stateCollection.UpdateOne(ctx, filter, update, opts); err != nil {
		return nil, fmt.Errorf("failed to initialize watermark: %v", err)
	}

	log.Printf("Initialized watermark for client %s, database %s, collection %s, mode %s", clientID, database, collection, syncMode)
	return watermark, nil
}

// UpdateWatermark updates the watermark position for a client
func (tt *TransferTracker) UpdateWatermark(clientID, database, collection string, watermark *WatermarkState) error {
	if !tt.config.Enabled || watermark == nil {
		return nil
	}

	ctx := context.Background()
	filter := bson.M{
		"client_id":  clientID,
		"database":   database,
		"collection": collection,
	}

	watermark.LastUpdated = time.Now()
	update := bson.M{
		"$set": bson.M{
			"watermark":      watermark,
			"last_synced_at": time.Now(),
			"updated_at":     time.Now(),
		},
	}

	if _, err := tt.stateCollection.UpdateOne(ctx, filter, update); err != nil {
		return fmt.Errorf("failed to update watermark: %v", err)
	}

	return nil
}

// GetWatermark retrieves the current watermark for a client
func (tt *TransferTracker) GetWatermark(clientID, database, collection string) (*WatermarkState, error) {
	if !tt.config.Enabled {
		return nil, nil
	}

	state, err := tt.GetClientSyncState(clientID, database, collection)
	if err != nil {
		return nil, err
	}
	if state == nil || state.Watermark == nil {
		return nil, nil
	}

	return state.Watermark, nil
}

// StartWatermarkBatch creates a new watermark-based transfer batch
func (tt *TransferTracker) StartWatermarkBatch(clientID, database, collection string, documentCount int, startWatermark *WatermarkState) (string, error) {
	if !tt.config.Enabled {
		return "", nil
	}

	batchID := fmt.Sprintf("%s_%s_%s_%d", clientID, database, collection, time.Now().Unix())
	batch := TransferBatch{
		BatchID:        batchID,
		ClientID:       clientID,
		Database:       database,
		Collection:     collection,
		DocumentCount:  documentCount,
		StartWatermark: startWatermark,
		SyncMode:       startWatermark.SyncMode,
		StartedAt:      time.Now(),
		Status:         TransferStatusInProgress,
		CreatedAt:      time.Now(),
		UpdatedAt:      time.Now(),
	}

	ctx := context.Background()
	if _, err := tt.batchCollection.InsertOne(ctx, batch); err != nil {
		return "", fmt.Errorf("failed to create watermark batch: %v", err)
	}

	log.Printf("Started watermark batch %s for client %s, mode %s, documents %d", batchID, clientID, startWatermark.SyncMode, documentCount)
	return batchID, nil
}

// CompleteWatermarkBatch completes a watermark batch and updates the watermark
func (tt *TransferTracker) CompleteWatermarkBatch(batchID string, endWatermark *WatermarkState) error {
	if !tt.config.Enabled || batchID == "" {
		return nil
	}

	ctx := context.Background()
	completedAt := time.Now()
	update := bson.M{
		"$set": bson.M{
			"status":        TransferStatusCompleted,
			"end_watermark": endWatermark,
			"completed_at":  completedAt,
			"updated_at":    completedAt,
		},
	}

	filter := bson.M{"batch_id": batchID}
	if _, err := tt.batchCollection.UpdateOne(ctx, filter, update); err != nil {
		return fmt.Errorf("failed to complete watermark batch: %v", err)
	}

	return nil
}
