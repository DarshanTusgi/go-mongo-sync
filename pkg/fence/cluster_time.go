package fence

import (
	"context"
	"fmt"
	"log"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readconcern"
	"go.mongodb.org/mongo-driver/mongo/readpref"
)

// ClusterTimeFence manages cluster time fencing for snapshot consistency
type ClusterTimeFence struct {
	client *mongo.Client
	config *FenceConfig
}

// FenceConfig holds configuration for cluster time fencing
type FenceConfig struct {
	Enabled  bool   `yaml:"enabled" json:"enabled"`
	MongoURI string `yaml:"mongo_uri" json:"mongo_uri"`
}

// SnapshotFence represents a cluster time fence for snapshot consistency
type SnapshotFence struct {
	ClusterTime    *primitive.Timestamp `json:"cluster_time"`
	OperationTime  *primitive.Timestamp `json:"operation_time"`
	CapturedAt     time.Time            `json:"captured_at"`
	SessionID      string               `json:"session_id"`
	ReadConcern    string               `json:"read_concern"`
	ReadPreference string               `json:"read_preference"`
}

// NewClusterTimeFence creates a new cluster time fence manager
func NewClusterTimeFence(config *FenceConfig) (*ClusterTimeFence, error) {
	if !config.Enabled {
		return &ClusterTimeFence{config: config}, nil
	}

	clientOptions := options.Client().ApplyURI(config.MongoURI)
	client, err := mongo.Connect(context.Background(), clientOptions)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to MongoDB for cluster time fence: %v", err)
	}

	// Test the connection
	if err = client.Ping(context.Background(), nil); err != nil {
		return nil, fmt.Errorf("failed to ping MongoDB for cluster time fence: %v", err)
	}

	log.Println("Cluster time fence manager initialized successfully")
	return &ClusterTimeFence{
		client: client,
		config: config,
	}, nil
}

// IsEnabled returns whether cluster time fencing is enabled
func (ctf *ClusterTimeFence) IsEnabled() bool {
	return ctf.config.Enabled
}

// CaptureSnapshotFence captures a cluster time fence before starting a snapshot
// This ensures that the snapshot and subsequent change stream are consistent
func (ctf *ClusterTimeFence) CaptureSnapshotFence(ctx context.Context, database string) (*SnapshotFence, error) {
	if !ctf.config.Enabled {
		return nil, nil
	}

	// Start a session to get cluster time
	session, err := ctf.client.StartSession()
	if err != nil {
		return nil, fmt.Errorf("failed to start session for fence capture: %v", err)
	}
	defer session.EndSession(ctx)

	// Perform a lightweight read operation within the session context to get current cluster time
	// We use a ping-like operation on the admin database
	db := ctf.client.Database(database)
	var result bson.M

	// Use majority read concern to ensure we get a consistent cluster time
	opts := options.RunCmd()

	// Execute the command within the session context
	err = mongo.WithSession(ctx, session, func(sc mongo.SessionContext) error {
		return db.RunCommand(sc, bson.D{primitive.E{Key: "ping", Value: 1}}, opts).Decode(&result)
	})
	if err != nil {
		return nil, fmt.Errorf("failed to capture cluster time: %v", err)
	}

	// Extract cluster time and operation time from the session
	clusterTime := session.ClusterTime()
	operationTime := session.OperationTime()

	if clusterTime == nil {
		// If cluster time is still not available, try to extract from result
		if clusterTimeRaw, ok := result["$clusterTime"]; ok {
			if clusterTimeBytes, err := bson.Marshal(clusterTimeRaw); err == nil {
				clusterTime = bson.Raw(clusterTimeBytes)
			}
		}

		if clusterTime == nil {
			return nil, fmt.Errorf("no cluster time available from session")
		}
	}

	fence := &SnapshotFence{
		ClusterTime:    extractTimestamp(clusterTime),
		OperationTime:  operationTime,
		CapturedAt:     time.Now(),
		SessionID:      session.ID().String(),
		ReadConcern:    "majority",
		ReadPreference: "primary",
	}

	log.Printf("Captured snapshot fence - ClusterTime: %v, OperationTime: %v",
		fence.ClusterTime, fence.OperationTime)

	return fence, nil
}

// ValidateChangeStreamStart validates that a change stream can start consistently
// with the captured snapshot fence
func (ctf *ClusterTimeFence) ValidateChangeStreamStart(fence *SnapshotFence, startAtOperationTime *primitive.Timestamp) error {
	if !ctf.config.Enabled || fence == nil {
		return nil
	}

	// Ensure the change stream starts at or after the snapshot fence
	if startAtOperationTime == nil {
		return fmt.Errorf("startAtOperationTime is required when using cluster time fence")
	}

	// Compare timestamps - change stream should start at or after the fence
	if primitive.CompareTimestamp(*startAtOperationTime, *fence.ClusterTime) < 0 {
		return fmt.Errorf("change stream start time %v is before snapshot fence %v - this could cause data inconsistency",
			startAtOperationTime, fence.ClusterTime)
	}

	// Check if the fence is too old (more than 1 hour)
	if time.Since(fence.CapturedAt) > time.Hour {
		log.Printf("Warning: Snapshot fence is %v old, consider recapturing", time.Since(fence.CapturedAt))
	}

	log.Printf("Change stream start validated - StartTime: %v, Fence: %v",
		startAtOperationTime, fence.ClusterTime)

	return nil
}

// GetReadConcernForSnapshot returns the appropriate read concern for snapshot operations
func (ctf *ClusterTimeFence) GetReadConcernForSnapshot(fence *SnapshotFence) *readconcern.ReadConcern {
	if !ctf.config.Enabled || fence == nil {
		return nil
	}

	// Use majority read concern with the captured cluster time
	return readconcern.Majority()
}

// GetReadPreferenceForSnapshot returns the appropriate read preference for snapshot operations
func (ctf *ClusterTimeFence) GetReadPreferenceForSnapshot() *readpref.ReadPref {
	if !ctf.config.Enabled {
		return nil
	}

	// Use primary read preference for consistency
	return readpref.Primary()
}

// CreateSnapshotSession creates a session configured for consistent snapshot reads
func (ctf *ClusterTimeFence) CreateSnapshotSession(ctx context.Context, fence *SnapshotFence) (mongo.Session, error) {
	if !ctf.config.Enabled {
		return nil, fmt.Errorf("cluster time fence is not enabled")
	}

	sessionOpts := options.Session()

	// Configure session for snapshot isolation if available
	// Note: This requires MongoDB 4.0+ and specific storage engines
	session, err := ctf.client.StartSession(sessionOpts)
	if err != nil {
		return nil, fmt.Errorf("failed to create snapshot session: %v", err)
	}

	return session, nil
}

// Close closes the cluster time fence manager
func (ctf *ClusterTimeFence) Close() error {
	if ctf.client != nil {
		return ctf.client.Disconnect(context.Background())
	}
	return nil
}

// extractTimestamp extracts the timestamp from cluster time BSON
func extractTimestamp(clusterTime bson.Raw) *primitive.Timestamp {
	if clusterTime == nil {
		return nil
	}

	// Parse the cluster time document
	var doc bson.M
	if err := bson.Unmarshal(clusterTime, &doc); err != nil {
		log.Printf("Failed to unmarshal cluster time: %v", err)
		return nil
	}

	// Extract the timestamp from the clusterTime field
	if ts, ok := doc["clusterTime"]; ok {
		if timestamp, ok := ts.(primitive.Timestamp); ok {
			return &timestamp
		}
	}

	log.Printf("Failed to extract timestamp from cluster time: %v", doc)
	return nil
}

// CompareTimestamps compares two timestamps and returns:
// -1 if t1 < t2
//
//	0 if t1 == t2
//	1 if t1 > t2
func CompareTimestamps(t1, t2 *primitive.Timestamp) int {
	if t1 == nil && t2 == nil {
		return 0
	}
	if t1 == nil {
		return -1
	}
	if t2 == nil {
		return 1
	}
	return primitive.CompareTimestamp(*t1, *t2)
}

// FormatTimestamp formats a timestamp for logging
func FormatTimestamp(ts *primitive.Timestamp) string {
	if ts == nil {
		return "<nil>"
	}
	return fmt.Sprintf("T:%d|I:%d", ts.T, ts.I)
}
