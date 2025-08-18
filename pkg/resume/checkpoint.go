package resume

import (
	"context"
	"fmt"
	"sync"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// CheckpointManager manages resume tokens and synchronization checkpoints
type CheckpointManager struct {
	mu           sync.RWMutex
	client       *mongo.Client
	database     string
	collection   string
	checkpoints  map[string]*Checkpoint
	persistInterval time.Duration
	ctx          context.Context
	cancel       context.CancelFunc
	wg           sync.WaitGroup
}

// Checkpoint represents a synchronization checkpoint for a collection
type Checkpoint struct {
	ID               string             `bson:"_id" json:"id"`
	Database         string             `bson:"database" json:"database"`
	Collection       string             `bson:"collection" json:"collection"`
	ResumeToken      bson.Raw           `bson:"resume_token" json:"resume_token"`
	LastEventTime    time.Time          `bson:"last_event_time" json:"last_event_time"`
	LastEventID      primitive.ObjectID `bson:"last_event_id,omitempty" json:"last_event_id,omitempty"`
	ProcessedCount   int64              `bson:"processed_count" json:"processed_count"`
	LastUpdated      time.Time          `bson:"last_updated" json:"last_updated"`
	Status           string             `bson:"status" json:"status"` // "active", "paused", "error"
	ErrorMessage     string             `bson:"error_message,omitempty" json:"error_message,omitempty"`
	Version          int                `bson:"version" json:"version"`
}

// CheckpointConfig holds configuration for checkpoint management
type CheckpointConfig struct {
	MongoURI        string        `yaml:"mongo_uri" json:"mongo_uri"`
	Database        string        `yaml:"database" json:"database"`
	Collection      string        `yaml:"collection" json:"collection"`
	PersistInterval time.Duration `yaml:"persist_interval" json:"persist_interval"`
	Enabled         bool          `yaml:"enabled" json:"enabled"`
}

// DefaultCheckpointConfig returns default checkpoint configuration
func DefaultCheckpointConfig() *CheckpointConfig {
	return &CheckpointConfig{
		Database:        "sync_checkpoints",
		Collection:      "resume_tokens",
		PersistInterval: 5 * time.Second,
		Enabled:         true,
	}
}

// NewCheckpointManager creates a new checkpoint manager
func NewCheckpointManager(config *CheckpointConfig) (*CheckpointManager, error) {
	if config == nil {
		config = DefaultCheckpointConfig()
	}

	if !config.Enabled {
		return &CheckpointManager{checkpoints: make(map[string]*Checkpoint)}, nil
	}

	client, err := mongo.Connect(context.Background(), options.Client().ApplyURI(config.MongoURI))
	if err != nil {
		return nil, fmt.Errorf("failed to connect to MongoDB for checkpoints: %w", err)
	}

	ctx, cancel := context.WithCancel(context.Background())

	cm := &CheckpointManager{
		client:          client,
		database:        config.Database,
		collection:      config.Collection,
		checkpoints:     make(map[string]*Checkpoint),
		persistInterval: config.PersistInterval,
		ctx:             ctx,
		cancel:          cancel,
	}

	// Load existing checkpoints
	if err := cm.loadCheckpoints(); err != nil {
		return nil, fmt.Errorf("failed to load checkpoints: %w", err)
	}

	// Start background persistence
	cm.startPersistence()

	return cm, nil
}

// GetCheckpoint retrieves a checkpoint for a specific collection
func (cm *CheckpointManager) GetCheckpoint(database, collection string) *Checkpoint {
	cm.mu.RLock()
	defer cm.mu.RUnlock()

	key := fmt.Sprintf("%s.%s", database, collection)
	return cm.checkpoints[key]
}

// UpdateCheckpoint updates a checkpoint with new resume token and metadata
func (cm *CheckpointManager) UpdateCheckpoint(database, collection string, resumeToken bson.Raw, eventTime time.Time) error {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	key := fmt.Sprintf("%s.%s", database, collection)
	checkpoint := cm.checkpoints[key]

	if checkpoint == nil {
		checkpoint = &Checkpoint{
			ID:         key,
			Database:   database,
			Collection: collection,
			Status:     "active",
			Version:    1,
		}
		cm.checkpoints[key] = checkpoint
	}

	checkpoint.ResumeToken = resumeToken
	checkpoint.LastEventTime = eventTime
	checkpoint.ProcessedCount++
	checkpoint.LastUpdated = time.Now()
	checkpoint.Version++

	return nil
}

// SetCheckpointError marks a checkpoint as having an error
func (cm *CheckpointManager) SetCheckpointError(database, collection string, err error) {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	key := fmt.Sprintf("%s.%s", database, collection)
	checkpoint := cm.checkpoints[key]

	if checkpoint != nil {
		checkpoint.Status = "error"
		checkpoint.ErrorMessage = err.Error()
		checkpoint.LastUpdated = time.Now()
		checkpoint.Version++
	}
}

// ClearCheckpointError clears an error status from a checkpoint
func (cm *CheckpointManager) ClearCheckpointError(database, collection string) {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	key := fmt.Sprintf("%s.%s", database, collection)
	checkpoint := cm.checkpoints[key]

	if checkpoint != nil {
		checkpoint.Status = "active"
		checkpoint.ErrorMessage = ""
		checkpoint.LastUpdated = time.Now()
		checkpoint.Version++
	}
}

// GetAllCheckpoints returns all checkpoints
func (cm *CheckpointManager) GetAllCheckpoints() map[string]*Checkpoint {
	cm.mu.RLock()
	defer cm.mu.RUnlock()

	result := make(map[string]*Checkpoint)
	for k, v := range cm.checkpoints {
		// Create a copy to avoid race conditions
		copy := *v
		result[k] = &copy
	}
	return result
}

// loadCheckpoints loads existing checkpoints from MongoDB
func (cm *CheckpointManager) loadCheckpoints() error {
	if cm.client == nil {
		return nil // Checkpoints disabled
	}

	coll := cm.client.Database(cm.database).Collection(cm.collection)
	cursor, err := coll.Find(context.Background(), bson.M{})
	if err != nil {
		return err
	}
	defer cursor.Close(context.Background())

	for cursor.Next(context.Background()) {
		var checkpoint Checkpoint
		if err := cursor.Decode(&checkpoint); err != nil {
			return err
		}
		cm.checkpoints[checkpoint.ID] = &checkpoint
	}

	return cursor.Err()
}

// persistCheckpoints saves all checkpoints to MongoDB
func (cm *CheckpointManager) persistCheckpoints() error {
	if cm.client == nil {
		return nil // Checkpoints disabled
	}

	cm.mu.RLock()
	checkpoints := make([]*Checkpoint, 0, len(cm.checkpoints))
	for _, checkpoint := range cm.checkpoints {
		copy := *checkpoint
		checkpoints = append(checkpoints, &copy)
	}
	cm.mu.RUnlock()

	if len(checkpoints) == 0 {
		return nil
	}

	coll := cm.client.Database(cm.database).Collection(cm.collection)

	// Use bulk write for efficiency
	var operations []mongo.WriteModel
	for _, checkpoint := range checkpoints {
		operation := mongo.NewReplaceOneModel()
		operation.SetFilter(bson.M{"_id": checkpoint.ID})
		operation.SetReplacement(checkpoint)
		operation.SetUpsert(true)
		operations = append(operations, operation)
	}

	if len(operations) > 0 {
		_, err := coll.BulkWrite(context.Background(), operations)
		return err
	}

	return nil
}

// startPersistence starts the background persistence routine
func (cm *CheckpointManager) startPersistence() {
	if cm.client == nil {
		return // Checkpoints disabled
	}

	cm.wg.Add(1)
	go func() {
		defer cm.wg.Done()
		ticker := time.NewTicker(cm.persistInterval)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				if err := cm.persistCheckpoints(); err != nil {
					// Log error but continue
					fmt.Printf("Failed to persist checkpoints: %v\n", err)
				}
			case <-cm.ctx.Done():
				// Final persistence before shutdown
				cm.persistCheckpoints()
				return
			}
		}
	}()
}

// Close shuts down the checkpoint manager
func (cm *CheckpointManager) Close() error {
	if cm.cancel != nil {
		cm.cancel()
	}
	cm.wg.Wait()

	if cm.client != nil {
		return cm.client.Disconnect(context.Background())
	}
	return nil
}

// GetStats returns checkpoint statistics
func (cm *CheckpointManager) GetStats() map[string]interface{} {
	cm.mu.RLock()
	defer cm.mu.RUnlock()

	stats := map[string]interface{}{
		"total_checkpoints": len(cm.checkpoints),
		"active_checkpoints": 0,
		"error_checkpoints": 0,
		"total_processed": int64(0),
	}

	for _, checkpoint := range cm.checkpoints {
		if checkpoint.Status == "active" {
			stats["active_checkpoints"] = stats["active_checkpoints"].(int) + 1
		} else if checkpoint.Status == "error" {
			stats["error_checkpoints"] = stats["error_checkpoints"].(int) + 1
		}
		stats["total_processed"] = stats["total_processed"].(int64) + checkpoint.ProcessedCount
	}

	return stats
}