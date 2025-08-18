package watermarks

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

// WatermarkManager manages VM-side watermarks for exactly-once semantics
type WatermarkManager struct {
	client     *mongo.Client
	config     *WatermarkConfig
	collection *mongo.Collection
}

// WatermarkConfig holds configuration for watermark management
type WatermarkConfig struct {
	Enabled    bool   `yaml:"enabled" json:"enabled"`
	MongoURI   string `yaml:"mongo_uri" json:"mongo_uri"`
	Database   string `yaml:"database" json:"database"`
	Collection string `yaml:"collection" json:"collection"`
}

// Watermark represents the VM-side watermark state
type Watermark struct {
	ID                    string               `bson:"_id" json:"id"`
	ClientID              string               `bson:"client_id" json:"client_id"`
	Database              string               `bson:"database" json:"database"`
	Collection            string               `bson:"collection" json:"collection"`
	LastAppliedEventID    string               `bson:"last_applied_event_id" json:"last_applied_event_id"`
	LastAppliedSequenceID int64                `bson:"last_applied_sequence_id" json:"last_applied_sequence_id"`
	LastAppliedClusterTime *primitive.Timestamp `bson:"last_applied_cluster_time" json:"last_applied_cluster_time"`
	LastAckedSequenceID   int64                `bson:"last_acked_sequence_id" json:"last_acked_sequence_id"`
	LastAckedBatchID      string               `bson:"last_acked_batch_id" json:"last_acked_batch_id"`
	SnapshotCompleted     bool                 `bson:"snapshot_completed" json:"snapshot_completed"`
	SnapshotClusterTime   *primitive.Timestamp `bson:"snapshot_cluster_time" json:"snapshot_cluster_time"`
	CreatedAt             time.Time            `bson:"created_at" json:"created_at"`
	UpdatedAt             time.Time            `bson:"updated_at" json:"updated_at"`
}

// NewWatermarkManager creates a new watermark manager
func NewWatermarkManager(config *WatermarkConfig) (*WatermarkManager, error) {
	if !config.Enabled {
		return &WatermarkManager{config: config}, nil
	}

	clientOptions := options.Client().ApplyURI(config.MongoURI)
	client, err := mongo.Connect(context.Background(), clientOptions)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to MongoDB for watermarks: %v", err)
	}

	// Test the connection
	if err = client.Ping(context.Background(), nil); err != nil {
		return nil, fmt.Errorf("failed to ping MongoDB for watermarks: %v", err)
	}

	coll := client.Database(config.Database).Collection(config.Collection)

	wm := &WatermarkManager{
		client:     client,
		config:     config,
		collection: coll,
	}

	// Create indexes for better performance
	if err := wm.createIndexes(); err != nil {
		log.Printf("Warning: Failed to create watermark indexes: %v", err)
	}

	log.Println("Watermark manager initialized successfully")
	return wm, nil
}

// createIndexes creates necessary indexes for watermark collection
func (wm *WatermarkManager) createIndexes() error {
	ctx := context.Background()

	indexes := []mongo.IndexModel{
		{
			Keys: bson.D{{"client_id", 1}, {"database", 1}, {"collection", 1}},
			Options: options.Index().SetUnique(true),
		},
		{
			Keys: bson.D{{"last_applied_sequence_id", 1}},
		},
		{
			Keys: bson.D{{"last_acked_sequence_id", 1}},
		},
	}

	_, err := wm.collection.Indexes().CreateMany(ctx, indexes)
	return err
}

// IsEnabled returns whether watermark tracking is enabled
func (wm *WatermarkManager) IsEnabled() bool {
	return wm.config.Enabled
}

// GetWatermark retrieves the watermark for a specific collection
func (wm *WatermarkManager) GetWatermark(clientID, database, collection string) (*Watermark, error) {
	if !wm.config.Enabled {
		return nil, nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	id := fmt.Sprintf("%s.%s.%s", clientID, database, collection)
	filter := bson.M{"_id": id}

	var watermark Watermark
	err := wm.collection.FindOne(ctx, filter).Decode(&watermark)
	if err == mongo.ErrNoDocuments {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("failed to get watermark: %v", err)
	}

	return &watermark, nil
}

// UpdateWatermark updates the watermark after successful event application
func (wm *WatermarkManager) UpdateWatermark(clientID, database, collection, eventID string, sequenceID int64, clusterTime *primitive.Timestamp) error {
	if !wm.config.Enabled {
		return nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	id := fmt.Sprintf("%s.%s.%s", clientID, database, collection)
	filter := bson.M{"_id": id}
	update := bson.M{
		"$set": bson.M{
			"last_applied_event_id":     eventID,
			"last_applied_sequence_id":  sequenceID,
			"last_applied_cluster_time": clusterTime,
			"updated_at":                time.Now(),
		},
		"$setOnInsert": bson.M{
			"client_id":  clientID,
			"database":   database,
			"collection": collection,
			"created_at": time.Now(),
		},
	}

	opts := options.Update().SetUpsert(true)
	_, err := wm.collection.UpdateOne(ctx, filter, update, opts)
	if err != nil {
		return fmt.Errorf("failed to update watermark: %v", err)
	}

	return nil
}

// AckSequence acknowledges a sequence number as processed
func (wm *WatermarkManager) AckSequence(clientID, database, collection, batchID string, sequenceID int64) error {
	if !wm.config.Enabled {
		return nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	id := fmt.Sprintf("%s.%s.%s", clientID, database, collection)
	filter := bson.M{"_id": id}
	update := bson.M{
		"$set": bson.M{
			"last_acked_sequence_id": sequenceID,
			"last_acked_batch_id":    batchID,
			"updated_at":             time.Now(),
		},
	}

	_, err := wm.collection.UpdateOne(ctx, filter, update)
	if err != nil {
		return fmt.Errorf("failed to ack sequence: %v", err)
	}

	return nil
}

// MarkSnapshotComplete marks the snapshot as completed for a collection
func (wm *WatermarkManager) MarkSnapshotComplete(clientID, database, collection string, clusterTime *primitive.Timestamp) error {
	if !wm.config.Enabled {
		return nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	id := fmt.Sprintf("%s.%s.%s", clientID, database, collection)
	filter := bson.M{"_id": id}
	update := bson.M{
		"$set": bson.M{
			"snapshot_completed":     true,
			"snapshot_cluster_time": clusterTime,
			"updated_at":             time.Now(),
		},
		"$setOnInsert": bson.M{
			"client_id":  clientID,
			"database":   database,
			"collection": collection,
			"created_at": time.Now(),
		},
	}

	opts := options.Update().SetUpsert(true)
	_, err := wm.collection.UpdateOne(ctx, filter, update, opts)
	if err != nil {
		return fmt.Errorf("failed to mark snapshot complete: %v", err)
	}

	return nil
}

// IsSequenceProcessed checks if a sequence number has already been processed
func (wm *WatermarkManager) IsSequenceProcessed(clientID, database, collection string, sequenceID int64) (bool, error) {
	if !wm.config.Enabled {
		return false, nil
	}

	watermark, err := wm.GetWatermark(clientID, database, collection)
	if err != nil {
		return false, err
	}
	if watermark == nil {
		return false, nil
	}

	return sequenceID <= watermark.LastAppliedSequenceID, nil
}

// Close closes the watermark manager
func (wm *WatermarkManager) Close() error {
	if wm.client != nil {
		return wm.client.Disconnect(context.Background())
	}
	return nil
}