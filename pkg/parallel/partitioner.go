package parallel

import (
	"context"
	"fmt"
	"log"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
)

// Partition represents a range of ObjectIds for parallel processing
type Partition struct {
	ID       int                `json:"id"`
	MinID    *primitive.ObjectID `json:"min_id,omitempty"`
	MaxID    *primitive.ObjectID `json:"max_id,omitempty"`
	IsFirst  bool               `json:"is_first"`
	IsLast   bool               `json:"is_last"`
	EstCount int64              `json:"est_count"`
}

// PartitionConfig holds configuration for partitioning
type PartitionConfig struct {
	TargetPartitionSize int64 // Target number of documents per partition
	MaxPartitions       int   // Maximum number of partitions to create
	MinPartitions       int   // Minimum number of partitions to create
}

// DefaultPartitionConfig returns sensible defaults
func DefaultPartitionConfig() *PartitionConfig {
	return &PartitionConfig{
		TargetPartitionSize: 10000, // 10K documents per partition
		MaxPartitions:       50,    // Max 50 partitions
		MinPartitions:       1,     // At least 1 partition
	}
}

// Partitioner handles ObjectId-based collection partitioning
type Partitioner struct {
	client *mongo.Client
	config *PartitionConfig
}

// NewPartitioner creates a new partitioner instance
func NewPartitioner(client *mongo.Client, config *PartitionConfig) *Partitioner {
	if config == nil {
		config = DefaultPartitionConfig()
	}
	return &Partitioner{
		client: client,
		config: config,
	}
}

// CreatePartitions creates ObjectId-based partitions for a collection
func (p *Partitioner) CreatePartitions(ctx context.Context, database, collection string) ([]*Partition, error) {
	coll := p.client.Database(database).Collection(collection)

	// Get collection stats
	totalCount, err := p.getCollectionCount(ctx, coll)
	if err != nil {
		return nil, fmt.Errorf("failed to get collection count: %w", err)
	}

	log.Printf("Collection %s.%s has %d documents", database, collection, totalCount)

	// If collection is small, return single partition
	if totalCount <= p.config.TargetPartitionSize {
		return []*Partition{{
			ID:       0,
			IsFirst:  true,
			IsLast:   true,
			EstCount: totalCount,
		}}, nil
	}

	// Calculate number of partitions
	numPartitions := int(totalCount / p.config.TargetPartitionSize)
	if numPartitions > p.config.MaxPartitions {
		numPartitions = p.config.MaxPartitions
	}
	if numPartitions < p.config.MinPartitions {
		numPartitions = p.config.MinPartitions
	}

	log.Printf("Creating %d partitions for collection %s.%s", numPartitions, database, collection)

	// Get ObjectId boundaries
	boundaries, err := p.getObjectIdBoundaries(ctx, coll, numPartitions)
	if err != nil {
		return nil, fmt.Errorf("failed to get ObjectId boundaries: %w", err)
	}

	// Create partitions
	partitions := make([]*Partition, numPartitions)
	for i := 0; i < numPartitions; i++ {
		partition := &Partition{
			ID:      i,
			IsFirst: i == 0,
			IsLast:  i == numPartitions-1,
		}

		// Set boundaries
		if i > 0 {
			partition.MinID = &boundaries[i-1]
		}
		if i < numPartitions-1 {
			partition.MaxID = &boundaries[i]
		}

		// Estimate count for this partition
		partition.EstCount = totalCount / int64(numPartitions)
		if i == numPartitions-1 {
			// Last partition gets remainder
			partition.EstCount = totalCount - (int64(i) * (totalCount / int64(numPartitions)))
		}

		partitions[i] = partition
	}

	return partitions, nil
}

// getCollectionCount gets the total document count for a collection
func (p *Partitioner) getCollectionCount(ctx context.Context, coll *mongo.Collection) (int64, error) {
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	return coll.CountDocuments(ctx, bson.D{})
}

// getObjectIdBoundaries gets ObjectId boundaries for partitioning
func (p *Partitioner) getObjectIdBoundaries(ctx context.Context, coll *mongo.Collection, numPartitions int) ([]primitive.ObjectID, error) {
	ctx, cancel := context.WithTimeout(ctx, 60*time.Second)
	defer cancel()

	// Use aggregation to sample ObjectIds and find boundaries
	pipeline := []bson.D{
		{{"$sample", bson.D{{"size", numPartitions * 100}}}}, // Sample more than needed
		{{"$project", bson.D{{"_id", 1}}}},
		{{"$sort", bson.D{{"_id", 1}}}},
	}

	cursor, err := coll.Aggregate(ctx, pipeline)
	if err != nil {
		return nil, fmt.Errorf("failed to aggregate for boundaries: %w", err)
	}
	defer cursor.Close(ctx)

	var samples []struct {
		ID primitive.ObjectID `bson:"_id"`
	}

	if err := cursor.All(ctx, &samples); err != nil {
		return nil, fmt.Errorf("failed to decode samples: %w", err)
	}

	if len(samples) == 0 {
		return nil, fmt.Errorf("no samples found")
	}

	// Calculate boundary indices
	boundaries := make([]primitive.ObjectID, numPartitions-1)
	step := len(samples) / numPartitions

	for i := 0; i < numPartitions-1; i++ {
		idx := (i + 1) * step
		if idx >= len(samples) {
			idx = len(samples) - 1
		}
		boundaries[i] = samples[idx].ID
	}

	return boundaries, nil
}

// BuildPartitionFilter creates a MongoDB filter for a specific partition
func (p *Partitioner) BuildPartitionFilter(partition *Partition) bson.D {
	filter := bson.D{}

	if partition.MinID != nil && partition.MaxID != nil {
		// Middle partition: minID < _id <= maxID
		filter = bson.D{
			{"_id", bson.D{
				{"$gt", *partition.MinID},
				{"$lte", *partition.MaxID},
			}},
		}
	} else if partition.MinID != nil {
		// Last partition: _id > minID
		filter = bson.D{
			{"_id", bson.D{{"$gt", *partition.MinID}}},
		}
	} else if partition.MaxID != nil {
		// First partition: _id <= maxID
		filter = bson.D{
			{"_id", bson.D{{"$lte", *partition.MaxID}}},
		}
	}
	// If both are nil, return empty filter (entire collection)

	return filter
}

// ValidatePartition checks if a partition is valid and estimates its size
func (p *Partitioner) ValidatePartition(ctx context.Context, database, collection string, partition *Partition) error {
	coll := p.client.Database(database).Collection(collection)
	filter := p.BuildPartitionFilter(partition)

	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	actualCount, err := coll.CountDocuments(ctx, filter)
	if err != nil {
		return fmt.Errorf("failed to count partition documents: %w", err)
	}

	log.Printf("Partition %d: estimated=%d, actual=%d", partition.ID, partition.EstCount, actualCount)
	partition.EstCount = actualCount

	return nil
}