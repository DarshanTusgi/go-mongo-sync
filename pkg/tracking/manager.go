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

// TransferTracker manages transfer tracking operations
type TransferTracker struct {
	client             *mongo.Client
	config             *TransferConfig
	transferCollection *mongo.Collection
	stateCollection    *mongo.Collection
	batchCollection    *mongo.Collection
}

// NewTransferTracker creates a new transfer tracker instance
func NewTransferTracker(config *TransferConfig) (*TransferTracker, error) {
	if !config.Enabled {
		return &TransferTracker{config: config}, nil
	}

	clientOptions := options.Client().ApplyURI(config.MongoURI)
	client, err := mongo.Connect(context.Background(), clientOptions)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to MongoDB for tracking: %v", err)
	}

	// Test the connection
	if err = client.Ping(context.Background(), nil); err != nil {
		return nil, fmt.Errorf("failed to ping MongoDB for tracking: %v", err)
	}

	db := client.Database(config.Database)
	transferColl := db.Collection(config.TransferCollection)
	stateColl := db.Collection(config.StateCollection)
	batchColl := db.Collection(config.BatchCollection)

	tracker := &TransferTracker{
		client:             client,
		config:             config,
		transferCollection: transferColl,
		stateCollection:    stateColl,
		batchCollection:    batchColl,
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

	// Index for transfer records
	transferIndexes := []mongo.IndexModel{
		{
			Keys: bson.D{{"client_id", 1}, {"database", 1}, {"collection", 1}},
		},
		{
			Keys: bson.D{{"document_id", 1}, {"client_id", 1}},
		},
		{
			Keys: bson.D{{"transfer_batch_id", 1}},
		},
	}

	// Index for client sync state
	stateIndexes := []mongo.IndexModel{
		{
			Keys: bson.D{{"client_id", 1}, {"database", 1}, {"collection", 1}},
			Options: options.Index().SetUnique(true),
		},
	}

	// Index for transfer batches
	batchIndexes := []mongo.IndexModel{
		{
			Keys: bson.D{{"batch_id", 1}},
			Options: options.Index().SetUnique(true),
		},
		{
			Keys: bson.D{{"client_id", 1}, {"status", 1}},
		},
	}

	// Create indexes
	if _, err := tt.transferCollection.Indexes().CreateMany(ctx, transferIndexes); err != nil {
		return fmt.Errorf("failed to create transfer indexes: %v", err)
	}

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

// IsDocumentTransferred checks if a document has already been transferred to a client
func (tt *TransferTracker) IsDocumentTransferred(clientID, database, collection string, documentID primitive.ObjectID) (bool, error) {
	if !tt.config.Enabled {
		return false, nil
	}

	ctx := context.Background()
	filter := bson.M{
		"client_id":   clientID,
		"database":    database,
		"collection":  collection,
		"document_id": documentID,
	}

	count, err := tt.transferCollection.CountDocuments(ctx, filter)
	if err != nil {
		return false, fmt.Errorf("failed to check document transfer status: %v", err)
	}

	return count > 0, nil
}

// GetUntransferredDocuments returns document IDs that haven't been transferred to a client
func (tt *TransferTracker) GetUntransferredDocuments(clientID, database, collection string, allDocumentIDs []primitive.ObjectID) ([]primitive.ObjectID, error) {
	if !tt.config.Enabled {
		return allDocumentIDs, nil
	}

	// If no document IDs provided, return empty slice
	if len(allDocumentIDs) == 0 {
		return []primitive.ObjectID{}, nil
	}

	ctx := context.Background()
	filter := bson.M{
		"client_id":  clientID,
		"database":   database,
		"collection": collection,
		"document_id": bson.M{"$in": allDocumentIDs},
	}

	cursor, err := tt.transferCollection.Find(ctx, filter, options.Find().SetProjection(bson.M{"document_id": 1}))
	if err != nil {
		return nil, fmt.Errorf("failed to query transferred documents: %v", err)
	}
	defer cursor.Close(ctx)

	transferredIDs := make(map[primitive.ObjectID]bool)
	for cursor.Next(ctx) {
		var record TransferRecord
		if err := cursor.Decode(&record); err != nil {
			continue
		}
		transferredIDs[record.DocumentID] = true
	}

	var untransferred []primitive.ObjectID
	for _, id := range allDocumentIDs {
		if !transferredIDs[id] {
			untransferred = append(untransferred, id)
		}
	}

	return untransferred, nil
}

// StartTransferBatch creates a new transfer batch record
func (tt *TransferTracker) StartTransferBatch(clientID, database, collection string, documentCount int) (string, error) {
	if !tt.config.Enabled {
		return "", nil
	}

	batchID := fmt.Sprintf("%s_%s_%s_%d", clientID, database, collection, time.Now().Unix())
	batch := TransferBatch{
		BatchID:       batchID,
		ClientID:      clientID,
		Database:      database,
		Collection:    collection,
		DocumentCount: documentCount,
		StartedAt:     time.Now(),
		Status:        TransferStatusInProgress,
		CreatedAt:     time.Now(),
		UpdatedAt:     time.Now(),
	}

	ctx := context.Background()
	if _, err := tt.batchCollection.InsertOne(ctx, batch); err != nil {
		return "", fmt.Errorf("failed to create transfer batch: %v", err)
	}

	return batchID, nil
}

// RecordTransfer records that a document has been successfully transferred
func (tt *TransferTracker) RecordTransfer(clientID, database, collection string, documentID primitive.ObjectID, batchID string) error {
	if !tt.config.Enabled {
		return nil
	}

	record := TransferRecord{
		ClientID:        clientID,
		Database:        database,
		Collection:      collection,
		DocumentID:      documentID,
		TransferredAt:   time.Now(),
		TransferBatchID: batchID,
		CreatedAt:       time.Now(),
		UpdatedAt:       time.Now(),
	}

	ctx := context.Background()
	if _, err := tt.transferCollection.InsertOne(ctx, record); err != nil {
		return fmt.Errorf("failed to record transfer: %v", err)
	}

	return nil
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

// UpdateClientSyncState updates the sync state for a client
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
			"last_synced_at":               time.Now(),
			"initial_sync_completed":        initialSyncCompleted,
			"updated_at":                   time.Now(),
		},
		"$inc": bson.M{
			"total_documents_transferred": documentsTransferred,
		},
	}

	if lastDocumentID != nil {
		update["$set"].(bson.M)["last_synced_document_id"] = *lastDocumentID
	}

	opts := options.Update().SetUpsert(true)
	if _, err := tt.stateCollection.UpdateOne(ctx, filter, update, opts); err != nil {
		return fmt.Errorf("failed to update client sync state: %v", err)
	}

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