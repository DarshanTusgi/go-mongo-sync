package sequence

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// Generator manages sequence number generation for events
type Generator struct {
	mu         sync.Mutex
	client     *mongo.Client
	config     *GeneratorConfig
	collection *mongo.Collection
	counter    int64
	batchSize  int64
	maxSeq     int64
}

// GeneratorConfig holds configuration for sequence generation
type GeneratorConfig struct {
	Enabled    bool   `yaml:"enabled" json:"enabled"`
	MongoURI   string `yaml:"mongo_uri" json:"mongo_uri"`
	Database   string `yaml:"database" json:"database"`
	Collection string `yaml:"collection" json:"collection"`
	BatchSize  int64  `yaml:"batch_size" json:"batch_size"`
	NodeID     string `yaml:"node_id" json:"node_id"`
}

// SequenceCounter represents the sequence counter document
type SequenceCounter struct {
	ID        string    `bson:"_id" json:"id"`
	NodeID    string    `bson:"node_id" json:"node_id"`
	Sequence  int64     `bson:"sequence" json:"sequence"`
	UpdatedAt time.Time `bson:"updated_at" json:"updated_at"`
}

// NewGenerator creates a new sequence generator
func NewGenerator(config *GeneratorConfig) (*Generator, error) {
	if !config.Enabled {
		return &Generator{config: config}, nil
	}

	if config.BatchSize <= 0 {
		config.BatchSize = 1000 // Default batch size
	}

	clientOptions := options.Client().ApplyURI(config.MongoURI)
	client, err := mongo.Connect(context.Background(), clientOptions)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to MongoDB for sequence generator: %v", err)
	}

	// Test the connection
	if err = client.Ping(context.Background(), nil); err != nil {
		return nil, fmt.Errorf("failed to ping MongoDB for sequence generator: %v", err)
	}

	coll := client.Database(config.Database).Collection(config.Collection)

	gen := &Generator{
		client:     client,
		config:     config,
		collection: coll,
		batchSize:  config.BatchSize,
	}

	// Initialize the sequence counter
	if err := gen.initializeCounter(); err != nil {
		return nil, fmt.Errorf("failed to initialize sequence counter: %v", err)
	}

	log.Printf("Sequence generator initialized for node %s with batch size %d", config.NodeID, config.BatchSize)
	return gen, nil
}

// initializeCounter initializes the sequence counter for this node
func (g *Generator) initializeCounter() error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Create index for better performance
	indexModel := mongo.IndexModel{
		Keys: bson.D{{"node_id", 1}},
		Options: options.Index().SetUnique(true),
	}
	_, err := g.collection.Indexes().CreateOne(ctx, indexModel)
	if err != nil {
		log.Printf("Warning: Failed to create sequence index: %v", err)
	}

	// Get current sequence for this node
	filter := bson.M{"_id": g.config.NodeID}
	var counter SequenceCounter
	err = g.collection.FindOne(ctx, filter).Decode(&counter)
	if err == mongo.ErrNoDocuments {
		// Initialize new counter
		counter = SequenceCounter{
			ID:        g.config.NodeID,
			NodeID:    g.config.NodeID,
			Sequence:  0,
			UpdatedAt: time.Now(),
		}
		_, err = g.collection.InsertOne(ctx, counter)
		if err != nil {
			return fmt.Errorf("failed to insert initial counter: %v", err)
		}
		g.counter = 0
		g.maxSeq = 0
	} else if err != nil {
		return fmt.Errorf("failed to get sequence counter: %v", err)
	} else {
		g.counter = counter.Sequence
		g.maxSeq = counter.Sequence
	}

	return nil
}

// IsEnabled returns whether sequence generation is enabled
func (g *Generator) IsEnabled() bool {
	return g.config.Enabled
}

// NextSequence generates the next sequence number
func (g *Generator) NextSequence() (int64, error) {
	if !g.config.Enabled {
		return 0, nil
	}

	g.mu.Lock()
	defer g.mu.Unlock()

	// Check if we need to allocate a new batch
	if g.counter >= g.maxSeq {
		if err := g.allocateBatch(); err != nil {
			return 0, fmt.Errorf("failed to allocate sequence batch: %v", err)
		}
	}

	g.counter++
	return g.counter, nil
}

// allocateBatch allocates a new batch of sequence numbers
func (g *Generator) allocateBatch() error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	filter := bson.M{"_id": g.config.NodeID}
	update := bson.M{
		"$inc": bson.M{"sequence": g.batchSize},
		"$set": bson.M{"updated_at": time.Now()},
	}

	opts := options.FindOneAndUpdate().SetReturnDocument(options.After)
	var counter SequenceCounter
	err := g.collection.FindOneAndUpdate(ctx, filter, update, opts).Decode(&counter)
	if err != nil {
		return fmt.Errorf("failed to allocate sequence batch: %v", err)
	}

	// Update local counters
	g.maxSeq = counter.Sequence
	if g.counter == 0 {
		g.counter = counter.Sequence - g.batchSize
	}

	log.Printf("Allocated sequence batch: %d-%d for node %s", g.counter+1, g.maxSeq, g.config.NodeID)
	return nil
}

// GetCurrentSequence returns the current sequence number without incrementing
func (g *Generator) GetCurrentSequence() int64 {
	if !g.config.Enabled {
		return 0
	}

	g.mu.Lock()
	defer g.mu.Unlock()
	return g.counter
}

// ResetSequence resets the sequence counter (use with caution)
func (g *Generator) ResetSequence() error {
	if !g.config.Enabled {
		return nil
	}

	g.mu.Lock()
	defer g.mu.Unlock()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	filter := bson.M{"_id": g.config.NodeID}
	update := bson.M{
		"$set": bson.M{
			"sequence":   0,
			"updated_at": time.Now(),
		},
	}

	_, err := g.collection.UpdateOne(ctx, filter, update)
	if err != nil {
		return fmt.Errorf("failed to reset sequence: %v", err)
	}

	g.counter = 0
	g.maxSeq = 0

	log.Printf("Reset sequence counter for node %s", g.config.NodeID)
	return nil
}

// Close closes the sequence generator
func (g *Generator) Close() error {
	if g.client != nil {
		return g.client.Disconnect(context.Background())
	}
	return nil
}

// BatchInfo returns information about the current batch
type BatchInfo struct {
	NodeID      string `json:"node_id"`
	Current     int64  `json:"current"`
	Max         int64  `json:"max"`
	Remaining   int64  `json:"remaining"`
	BatchSize   int64  `json:"batch_size"`
}

// GetBatchInfo returns information about the current sequence batch
func (g *Generator) GetBatchInfo() *BatchInfo {
	if !g.config.Enabled {
		return nil
	}

	g.mu.Lock()
	defer g.mu.Unlock()

	return &BatchInfo{
		NodeID:    g.config.NodeID,
		Current:   g.counter,
		Max:       g.maxSeq,
		Remaining: g.maxSeq - g.counter,
		BatchSize: g.batchSize,
	}
}