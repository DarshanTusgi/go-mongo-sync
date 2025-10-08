package resume

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

// TokenManager manages resume tokens per VM-sync client for buffer-free synchronization
// ELIMINATES MEMORY BUFFERS - Uses MongoDB resume tokens for fault tolerance
type TokenManager struct {
	mu              sync.RWMutex
	client          *mongo.Client
	database        string
	collection      string
	clientTokens    map[string]*ClientResumeState // clientID -> resume state
	persistInterval time.Duration
	cleanupInterval time.Duration
	ctx             context.Context
	cancel          context.CancelFunc
	wg              sync.WaitGroup
}

// ClientResumeState tracks resume token state per VM-sync client
type ClientResumeState struct {
	ClientID         string                           `bson:"_id" json:"client_id"`
	CollectionTokens map[string]*CollectionTokenState `bson:"collection_tokens" json:"collection_tokens"`
	LastSeen         time.Time                        `bson:"last_seen" json:"last_seen"`
	Status           string                           `bson:"status" json:"status"` // "connected", "disconnected", "error"
	CreatedAt        time.Time                        `bson:"created_at" json:"created_at"`
	UpdatedAt        time.Time                        `bson:"updated_at" json:"updated_at"`
}

// CollectionTokenState tracks resume token for a specific collection
type CollectionTokenState struct {
	Database      string    `bson:"database" json:"database"`
	Collection    string    `bson:"collection" json:"collection"`
	ResumeToken   bson.Raw  `bson:"resume_token,omitempty" json:"resume_token,omitempty"`
	LastEventTime time.Time `bson:"last_event_time" json:"last_event_time"`
	EventCount    int64     `bson:"event_count" json:"event_count"`
	LastUpdated   time.Time `bson:"last_updated" json:"last_updated"`
	IsActive      bool      `bson:"is_active" json:"is_active"`
}

// TokenManagerConfig configuration for token manager
type TokenManagerConfig struct {
	MongoURI        string        `yaml:"mongo_uri" json:"mongo_uri"`
	Database        string        `yaml:"database" json:"database"`
	Collection      string        `yaml:"collection" json:"collection"`
	PersistInterval time.Duration `yaml:"persist_interval" json:"persist_interval"`
	CleanupInterval time.Duration `yaml:"cleanup_interval" json:"cleanup_interval"`
	RetentionDays   int           `yaml:"retention_days" json:"retention_days"`
}

// NewTokenManager creates a new resume token manager
func NewTokenManager(config *TokenManagerConfig) (*TokenManager, error) {
	if config.PersistInterval == 0 {
		config.PersistInterval = 5 * time.Second
	}
	if config.CleanupInterval == 0 {
		config.CleanupInterval = 1 * time.Hour
	}

	// Connect to MongoDB
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	client, err := mongo.Connect(ctx, options.Client().ApplyURI(config.MongoURI))
	if err != nil {
		return nil, fmt.Errorf("failed to connect to MongoDB: %v", err)
	}

	if err := client.Ping(ctx, nil); err != nil {
		return nil, fmt.Errorf("failed to ping MongoDB: %v", err)
	}

	managerCtx, managerCancel := context.WithCancel(context.Background())
	tm := &TokenManager{
		client:          client,
		database:        config.Database,
		collection:      config.Collection,
		clientTokens:    make(map[string]*ClientResumeState),
		persistInterval: config.PersistInterval,
		cleanupInterval: config.CleanupInterval,
		ctx:             managerCtx,
		cancel:          managerCancel,
	}

	// Load existing tokens from MongoDB
	if err := tm.loadExistingTokens(); err != nil {
		log.Printf("WARNING: Failed to load existing tokens: %v", err)
	}

	// Start background persistence and cleanup
	tm.startBackgroundTasks()

	log.Printf("✅ TOKEN MANAGER: Initialized for buffer-free synchronization")
	return tm, nil
}

// RegisterClient registers a new VM-sync client
func (tm *TokenManager) RegisterClient(clientID string) error {
	tm.mu.Lock()
	defer tm.mu.Unlock()

	if _, exists := tm.clientTokens[clientID]; !exists {
		tm.clientTokens[clientID] = &ClientResumeState{
			ClientID:         clientID,
			CollectionTokens: make(map[string]*CollectionTokenState),
			LastSeen:         time.Now(),
			Status:           "connected",
			CreatedAt:        time.Now(),
			UpdatedAt:        time.Now(),
		}
		log.Printf("📝 TOKEN MANAGER: Registered client %s", clientID)
	} else {
		// Update existing client status
		tm.clientTokens[clientID].Status = "connected"
		tm.clientTokens[clientID].LastSeen = time.Now()
		tm.clientTokens[clientID].UpdatedAt = time.Now()
		log.Printf("🔄 TOKEN MANAGER: Reconnected client %s", clientID)
	}

	return nil
}

// UnregisterClient marks a client as disconnected (keeps tokens for resume)
func (tm *TokenManager) UnregisterClient(clientID string) error {
	tm.mu.Lock()
	defer tm.mu.Unlock()

	if clientState, exists := tm.clientTokens[clientID]; exists {
		clientState.Status = "disconnected"
		clientState.LastSeen = time.Now()
		clientState.UpdatedAt = time.Now()
		log.Printf("📤 TOKEN MANAGER: Client %s disconnected, tokens preserved for resume", clientID)
	}

	return nil
}

// UpdateClientToken updates resume token for a client's collection
// This is called for each change event to update the resume point
func (tm *TokenManager) UpdateClientToken(clientID, database, collection string, resumeToken bson.Raw, eventTime time.Time) error {
	tm.mu.Lock()
	defer tm.mu.Unlock()

	clientState, exists := tm.clientTokens[clientID]
	if !exists {
		return fmt.Errorf("client %s not registered", clientID)
	}

	collectionKey := fmt.Sprintf("%s.%s", database, collection)
	tokenState := clientState.CollectionTokens[collectionKey]

	if tokenState == nil {
		tokenState = &CollectionTokenState{
			Database:   database,
			Collection: collection,
			IsActive:   true,
		}
		clientState.CollectionTokens[collectionKey] = tokenState
	}

	// Update token state
	if resumeToken != nil && len(resumeToken) > 0 {
		tokenState.ResumeToken = resumeToken
	}
	tokenState.LastEventTime = eventTime
	tokenState.EventCount++
	tokenState.LastUpdated = time.Now()

	// Update client state
	clientState.LastSeen = time.Now()
	clientState.UpdatedAt = time.Now()

	return nil
}

// GetClientResumeToken returns the resume token for a client's collection
// Used when VM-sync reconnects to resume from where it left off
func (tm *TokenManager) GetClientResumeToken(clientID, database, collection string) (bson.Raw, error) {
	tm.mu.RLock()
	defer tm.mu.RUnlock()

	clientState, exists := tm.clientTokens[clientID]
	if !exists {
		return nil, fmt.Errorf("client %s not found", clientID)
	}

	collectionKey := fmt.Sprintf("%s.%s", database, collection)
	tokenState := clientState.CollectionTokens[collectionKey]

	if tokenState == nil || len(tokenState.ResumeToken) == 0 {
		return nil, nil // No token available - start from current time
	}

	log.Printf("🎯 TOKEN MANAGER: Resume token found for client %s, collection %s", clientID, collectionKey)
	return tokenState.ResumeToken, nil
}

// GetClientCollections returns all collections being tracked for a client
func (tm *TokenManager) GetClientCollections(clientID string) ([]string, error) {
	tm.mu.RLock()
	defer tm.mu.RUnlock()

	clientState, exists := tm.clientTokens[clientID]
	if !exists {
		return nil, fmt.Errorf("client %s not found", clientID)
	}

	collections := make([]string, 0, len(clientState.CollectionTokens))
	for collectionKey := range clientState.CollectionTokens {
		collections = append(collections, collectionKey)
	}

	return collections, nil
}

// GetClientStatus returns current status of a client and their collections
func (tm *TokenManager) GetClientStatus(clientID string) (*ClientResumeState, error) {
	tm.mu.RLock()
	defer tm.mu.RUnlock()

	clientState, exists := tm.clientTokens[clientID]
	if !exists {
		return nil, fmt.Errorf("client %s not found", clientID)
	}

	// Create a copy to avoid concurrent access issues
	statusCopy := &ClientResumeState{
		ClientID:         clientState.ClientID,
		CollectionTokens: make(map[string]*CollectionTokenState),
		LastSeen:         clientState.LastSeen,
		Status:           clientState.Status,
		CreatedAt:        clientState.CreatedAt,
		UpdatedAt:        clientState.UpdatedAt,
	}

	for key, tokenState := range clientState.CollectionTokens {
		statusCopy.CollectionTokens[key] = &CollectionTokenState{
			Database:      tokenState.Database,
			Collection:    tokenState.Collection,
			LastEventTime: tokenState.LastEventTime,
			EventCount:    tokenState.EventCount,
			LastUpdated:   tokenState.LastUpdated,
			IsActive:      tokenState.IsActive,
		}
	}

	return statusCopy, nil
}

// loadExistingTokens loads resume tokens from MongoDB on startup
func (tm *TokenManager) loadExistingTokens() error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	coll := tm.client.Database(tm.database).Collection(tm.collection)
	cursor, err := coll.Find(ctx, bson.M{})
	if err != nil {
		return fmt.Errorf("failed to load existing tokens: %v", err)
	}
	defer cursor.Close(ctx)

	count := 0
	for cursor.Next(ctx) {
		var clientState ClientResumeState
		if err := cursor.Decode(&clientState); err != nil {
			log.Printf("WARNING: Failed to decode client state: %v", err)
			continue
		}

		tm.clientTokens[clientState.ClientID] = &clientState
		count++
	}

	log.Printf("📂 TOKEN MANAGER: Loaded %d client resume states from MongoDB", count)
	return nil
}

// startBackgroundTasks starts persistence and cleanup goroutines
func (tm *TokenManager) startBackgroundTasks() {
	// Start persistence task
	tm.wg.Add(1)
	go tm.persistTokensPeriodically()

	// Start cleanup task
	tm.wg.Add(1)
	go tm.cleanupOldTokens()
}

// persistTokensPeriodically saves tokens to MongoDB periodically
func (tm *TokenManager) persistTokensPeriodically() {
	defer tm.wg.Done()

	ticker := time.NewTicker(tm.persistInterval)
	defer ticker.Stop()

	for {
		select {
		case <-tm.ctx.Done():
			// Final persistence before shutdown
			tm.persistTokens()
			return
		case <-ticker.C:
			tm.persistTokens()
		}
	}
}

// persistTokens saves current token state to MongoDB
func (tm *TokenManager) persistTokens() {
	tm.mu.RLock()
	clientStates := make(map[string]*ClientResumeState)
	for clientID, state := range tm.clientTokens {
		clientStates[clientID] = state
	}
	tm.mu.RUnlock()

	if len(clientStates) == 0 {
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	coll := tm.client.Database(tm.database).Collection(tm.collection)

	// Use bulk write for efficiency
	var operations []mongo.WriteModel
	for clientID, state := range clientStates {
		operation := mongo.NewReplaceOneModel().
			SetFilter(bson.M{"_id": clientID}).
			SetReplacement(state).
			SetUpsert(true)
		operations = append(operations, operation)
	}

	if len(operations) > 0 {
		_, err := coll.BulkWrite(ctx, operations)
		if err != nil {
			log.Printf("ERROR: Failed to persist resume tokens: %v", err)
		} else {
			log.Printf("💾 TOKEN MANAGER: Persisted %d client states to MongoDB", len(operations))
		}
	}
}

// cleanupOldTokens removes tokens for clients that haven't been seen recently
func (tm *TokenManager) cleanupOldTokens() {
	defer tm.wg.Done()

	ticker := time.NewTicker(tm.cleanupInterval)
	defer ticker.Stop()

	for {
		select {
		case <-tm.ctx.Done():
			return
		case <-ticker.C:
			tm.performCleanup()
		}
	}
}

// performCleanup removes old unused tokens
func (tm *TokenManager) performCleanup() {
	tm.mu.Lock()
	defer tm.mu.Unlock()

	cutoff := time.Now().AddDate(0, 0, -7) // 7 days retention

	var toDelete []string
	for clientID, state := range tm.clientTokens {
		if state.Status == "disconnected" && state.LastSeen.Before(cutoff) {
			toDelete = append(toDelete, clientID)
		}
	}

	// Remove from memory
	for _, clientID := range toDelete {
		delete(tm.clientTokens, clientID)
	}

	if len(toDelete) > 0 {
		// Remove from MongoDB
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		coll := tm.client.Database(tm.database).Collection(tm.collection)
		_, err := coll.DeleteMany(ctx, bson.M{
			"_id": bson.M{"$in": toDelete},
		})
		if err != nil {
			log.Printf("ERROR: Failed to cleanup old tokens: %v", err)
		} else {
			log.Printf("🧹 TOKEN MANAGER: Cleaned up %d old client tokens", len(toDelete))
		}
	}
}

// Stop gracefully shuts down the token manager
func (tm *TokenManager) Stop() {
	log.Println("🛑 TOKEN MANAGER: Shutting down...")

	// Cancel context to stop background tasks
	tm.cancel()

	// Wait for background tasks to complete
	tm.wg.Wait()

	// Final persistence
	tm.persistTokens()

	// Close MongoDB connection
	if tm.client != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		tm.client.Disconnect(ctx)
	}

	log.Println("✅ TOKEN MANAGER: Shutdown complete")
}

// GetStats returns statistics about token management
func (tm *TokenManager) GetStats() map[string]interface{} {
	tm.mu.RLock()
	defer tm.mu.RUnlock()

	stats := map[string]interface{}{
		"total_clients":        len(tm.clientTokens),
		"connected_clients":    0,
		"disconnected_clients": 0,
		"total_collections":    0,
		"active_collections":   0,
	}

	for _, state := range tm.clientTokens {
		if state.Status == "connected" {
			stats["connected_clients"] = stats["connected_clients"].(int) + 1
		} else {
			stats["disconnected_clients"] = stats["disconnected_clients"].(int) + 1
		}

		stats["total_collections"] = stats["total_collections"].(int) + len(state.CollectionTokens)
		for _, tokenState := range state.CollectionTokens {
			if tokenState.IsActive {
				stats["active_collections"] = stats["active_collections"].(int) + 1
			}
		}
	}

	return stats
}
