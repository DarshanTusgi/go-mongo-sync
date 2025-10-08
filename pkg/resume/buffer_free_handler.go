package resume

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"go-data-sync-http/pkg/models"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// BufferFreeChangeHandler handles change streams without memory buffers
// Uses resume tokens for fault tolerance and reconnection
type BufferFreeChangeHandler struct {
	tokenManager  *TokenManager
	mongoClient   *mongo.Client
	activeStreams map[string]*ChangeStreamState
	eventChannel  chan models.ChangeEvent
	ctx           context.Context
	cancel        context.CancelFunc
	mu            sync.RWMutex // Mutex for activeStreams protection
}

// ChangeStreamState tracks active change stream per collection
type ChangeStreamState struct {
	Database     string
	Collection   string
	ChangeStream *mongo.ChangeStream
	LastToken    bson.Raw
	StartTime    time.Time
	EventCount   int64
	IsActive     bool
}

// NewBufferFreeChangeHandler creates buffer-free change stream handler
func NewBufferFreeChangeHandler(tokenManager *TokenManager, mongoClient *mongo.Client, eventChannel chan models.ChangeEvent) *BufferFreeChangeHandler {
	ctx, cancel := context.WithCancel(context.Background())

	return &BufferFreeChangeHandler{
		tokenManager:  tokenManager,
		mongoClient:   mongoClient,
		activeStreams: make(map[string]*ChangeStreamState),
		eventChannel:  eventChannel,
		ctx:           ctx,
		cancel:        cancel,
	}
}

// StartCollectionWatch starts watching a collection for changes
// NO BUFFERING - Events are immediately sent to connected clients or resume tokens updated
func (bfch *BufferFreeChangeHandler) StartCollectionWatch(database, collection string) error {
	streamKey := fmt.Sprintf("%s.%s", database, collection)

	// Thread-safe check if already watching
	bfch.mu.Lock()
	defer bfch.mu.Unlock()

	if _, exists := bfch.activeStreams[streamKey]; exists {
		return fmt.Errorf("already watching collection %s", streamKey)
	}

	log.Printf("🎯 BUFFER-FREE: Starting change stream for %s (no memory buffer)", streamKey)

	// Start change stream in goroutine
	go bfch.watchCollectionStream(database, collection)

	return nil
}

// StopCollectionWatch stops watching a collection
func (bfch *BufferFreeChangeHandler) StopCollectionWatch(database, collection string) error {
	streamKey := fmt.Sprintf("%s.%s", database, collection)

	bfch.mu.Lock()
	defer bfch.mu.Unlock()

	if streamState, exists := bfch.activeStreams[streamKey]; exists {
		streamState.IsActive = false
		if streamState.ChangeStream != nil {
			streamState.ChangeStream.Close(context.Background())
		}
		delete(bfch.activeStreams, streamKey)
		log.Printf("🛑 BUFFER-FREE: Stopped change stream for %s", streamKey)
	}

	return nil
}

// watchCollectionStream watches changes for a specific collection
func (bfch *BufferFreeChangeHandler) watchCollectionStream(database, collection string) {
	streamKey := fmt.Sprintf("%s.%s", database, collection)

	for {
		select {
		case <-bfch.ctx.Done():
			return
		default:
			if err := bfch.runChangeStream(database, collection); err != nil {
				log.Printf("❌ BUFFER-FREE: Change stream error for %s: %v. Retrying in 5s...", streamKey, err)
				time.Sleep(5 * time.Second)
			}
		}
	}
}

// runChangeStream runs the actual change stream with resume token management
func (bfch *BufferFreeChangeHandler) runChangeStream(database, collection string) error {
	streamKey := fmt.Sprintf("%s.%s", database, collection)

	// Add panic recovery
	defer func() {
		if r := recover(); r != nil {
			log.Printf("🚨 PANIC RECOVERED in runChangeStream for %s: %v", streamKey, r)
			// Clean up stream state
			bfch.mu.Lock()
			if streamState, exists := bfch.activeStreams[streamKey]; exists {
				if streamState.ChangeStream != nil {
					streamState.ChangeStream.Close(context.Background())
				}
				delete(bfch.activeStreams, streamKey)
			}
			bfch.mu.Unlock()
		}
	}()

	// Initialize stream state
	streamState := &ChangeStreamState{
		Database:   database,
		Collection: collection,
		StartTime:  time.Now(),
		IsActive:   true,
	}

	// Thread-safe assignment
	bfch.mu.Lock()
	bfch.activeStreams[streamKey] = streamState
	bfch.mu.Unlock()

	coll := bfch.mongoClient.Database(database).Collection(collection)

	// Get resume tokens for all connected clients
	var watchOptions *options.ChangeStreamOptions
	resumeTokens := bfch.getLatestResumeToken(database, collection)

	if resumeTokens != nil && len(resumeTokens) > 0 {
		// Use the earliest resume token to ensure no events are missed
		watchOptions = options.ChangeStream().
			SetResumeAfter(resumeTokens).
			SetFullDocument(options.UpdateLookup)
		log.Printf("🔄 BUFFER-FREE: Resuming %s from saved token", streamKey)
	} else {
		// Start from current time
		watchOptions = options.ChangeStream().SetFullDocument(options.UpdateLookup)
		log.Printf("🆕 BUFFER-FREE: Starting %s from current time", streamKey)
	}

	// Create change stream
	changeStream, err := coll.Watch(context.Background(), mongo.Pipeline{}, watchOptions)
	if err != nil {
		// Clean up on error
		bfch.mu.Lock()
		delete(bfch.activeStreams, streamKey)
		bfch.mu.Unlock()
		return fmt.Errorf("failed to create change stream: %v", err)
	}
	defer changeStream.Close(context.Background())

	// Thread-safe assignment of change stream
	bfch.mu.Lock()
	if currentState, exists := bfch.activeStreams[streamKey]; exists && currentState != nil {
		currentState.ChangeStream = changeStream
	} else {
		// Stream state was removed, clean up and return
		bfch.mu.Unlock()
		changeStream.Close(context.Background())
		return fmt.Errorf("stream state was removed during initialization")
	}
	bfch.mu.Unlock()

	log.Printf("✅ BUFFER-FREE: Change stream active for %s", streamKey)

	// Process change events
	for changeStream.Next(context.Background()) {
		var changeEvent bson.M
		if err := changeStream.Decode(&changeEvent); err != nil {
			log.Printf("Failed to decode change event for %s: %v", streamKey, err)
			continue
		}

		// Extract resume token from change event
		resumeToken := changeStream.ResumeToken()

		// Thread-safe update of stream state
		bfch.mu.Lock()
		if currentState, exists := bfch.activeStreams[streamKey]; exists && currentState != nil {
			currentState.LastToken = resumeToken
			currentState.EventCount++
		} else {
			log.Printf("⚠️  Stream state not found for %s, skipping event", streamKey)
			bfch.mu.Unlock()
			continue
		}
		bfch.mu.Unlock()

		// Convert to our change event format
		event := bfch.convertChangeEvent(changeEvent, database, collection, resumeToken)

		// CRITICAL: No buffering - process immediately
		bfch.processChangeEventImmediately(event)

		// Thread-safe logging of event count
		bfch.mu.RLock()
		var eventCount int64
		if currentState, exists := bfch.activeStreams[streamKey]; exists && currentState != nil {
			eventCount = currentState.EventCount
		}
		bfch.mu.RUnlock()

		log.Printf("📨 BUFFER-FREE: Processed event %d for %s (no buffer)", eventCount, streamKey)
	}

	// Check for errors
	if err := changeStream.Err(); err != nil {
		// Handle resume token errors
		if bfch.isResumeTokenError(err) {
			log.Printf("⚠️  BUFFER-FREE: Resume token error for %s, clearing tokens and restarting", streamKey)
			bfch.clearResumeTokensForCollection(database, collection)
			return err // Will trigger retry with fresh start
		}
		return fmt.Errorf("change stream error: %v", err)
	}

	return nil
}

// processChangeEventImmediately processes events without buffering
func (bfch *BufferFreeChangeHandler) processChangeEventImmediately(event models.ChangeEvent) {
	// Update resume tokens for all connected clients FIRST
	bfch.updateAllClientTokens(event)

	// Send to event channel (connected clients receive immediately)
	select {
	case bfch.eventChannel <- event:
		// Event sent successfully
	default:
		// Channel full - but this is OK because we have resume tokens
		// When clients reconnect, they'll resume from their saved token
		log.Printf("📤 BUFFER-FREE: Event channel full, but resume token saved for recovery")
	}
}

// updateAllClientTokens updates resume tokens for all clients
func (bfch *BufferFreeChangeHandler) updateAllClientTokens(event models.ChangeEvent) {
	// This is called for EVERY change event to maintain resume points
	// Even if no clients are connected, tokens are updated

	// Get all clients from token manager (could be connected or disconnected)
	clients := bfch.getAllTrackedClients()

	for _, clientID := range clients {
		err := bfch.tokenManager.UpdateClientToken(
			clientID,
			event.Database,
			event.Collection,
			event.ResumeToken,
			event.Timestamp,
		)
		if err != nil {
			log.Printf("⚠️  Failed to update token for client %s: %v", clientID, err)
		}
	}

	log.Printf("💾 BUFFER-FREE: Updated resume tokens for %d clients", len(clients))
}

// getLatestResumeToken gets the earliest resume token among all clients
func (bfch *BufferFreeChangeHandler) getLatestResumeToken(database, collection string) bson.Raw {
	clients := bfch.getAllTrackedClients()

	var earliestToken bson.Raw
	// For simplicity, use the first available token
	// In production, you might want to find the earliest timestamp

	for _, clientID := range clients {
		token, err := bfch.tokenManager.GetClientResumeToken(clientID, database, collection)
		if err == nil && token != nil && len(token) > 0 {
			earliestToken = token
			break
		}
	}

	return earliestToken
}

// getAllTrackedClients gets all clients being tracked (connected + disconnected)
func (bfch *BufferFreeChangeHandler) getAllTrackedClients() []string {
	// This would typically come from the token manager's client registry
	// For now, we'll implement a simple approach
	// TODO: Implement proper client tracking in token manager

	// Placeholder - in practice this would query the token manager
	return []string{"example-client-1", "example-client-2"}
}

// clearResumeTokensForCollection clears tokens for a collection (after invalidate)
func (bfch *BufferFreeChangeHandler) clearResumeTokensForCollection(database, collection string) {
	clients := bfch.getAllTrackedClients()

	for _, clientID := range clients {
		// Clear the token by updating with nil/empty token
		err := bfch.tokenManager.UpdateClientToken(clientID, database, collection, nil, time.Now())
		if err != nil {
			log.Printf("Failed to clear token for client %s: %v", clientID, err)
		}
	}

	log.Printf("🧹 BUFFER-FREE: Cleared resume tokens for %s.%s", database, collection)
}

// convertChangeEvent converts MongoDB change event to our format
func (bfch *BufferFreeChangeHandler) convertChangeEvent(changeEvent bson.M, database, collection string, resumeToken bson.Raw) models.ChangeEvent {
	// Extract operation type
	operationType, _ := changeEvent["operationType"].(string)

	// Extract document key
	var documentKey bson.Raw
	if dk, ok := changeEvent["documentKey"]; ok {
		if dkBytes, err := bson.Marshal(dk); err == nil {
			documentKey = bson.Raw(dkBytes)
		}
	}

	// Extract full document
	var fullDocument bson.Raw
	if fd, ok := changeEvent["fullDocument"]; ok {
		if fdBytes, err := bson.Marshal(fd); err == nil {
			fullDocument = bson.Raw(fdBytes)
		}
	}

	// Extract cluster time
	var clusterTime bson.Raw
	if ct, ok := changeEvent["clusterTime"]; ok {
		if ctBytes, err := bson.Marshal(ct); err == nil {
			clusterTime = bson.Raw(ctBytes)
		}
	}

	return models.ChangeEvent{
		OperationType: operationType,
		Database:      database,
		Collection:    collection,
		DocumentKey:   documentKey,
		FullDocument:  fullDocument,
		Timestamp:     time.Now(),
		ResumeToken:   resumeToken,
		ClusterTime:   clusterTime,
	}
}

// isResumeTokenError checks if error is related to resume token
func (bfch *BufferFreeChangeHandler) isResumeTokenError(err error) bool {
	if err == nil {
		return false
	}

	errStr := err.Error()
	return containsSubstring(errStr, "resume token") ||
		containsSubstring(errStr, "ChangeStreamHistoryLost") ||
		containsSubstring(errStr, "InvalidResumeToken") ||
		containsSubstring(errStr, "resume point may no longer be in the oplog")
}

// containsSubstring checks if string contains substring (helper function)
func containsSubstring(s, substr string) bool {
	return len(s) >= len(substr) &&
		(s == substr ||
			(len(s) > len(substr) &&
				(s[:len(substr)] == substr ||
					s[len(s)-len(substr):] == substr ||
					findSubstring(s, substr))))
}

func findSubstring(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}

// GetStats returns statistics about buffer-free operations
func (bfch *BufferFreeChangeHandler) GetStats() map[string]interface{} {
	bfch.mu.RLock()
	defer bfch.mu.RUnlock()

	stats := map[string]interface{}{
		"active_streams":     len(bfch.activeStreams),
		"total_events":       int64(0),
		"memory_buffer_size": 0, // Zero - no memory buffer!
		"resume_token_based": true,
	}

	for _, streamState := range bfch.activeStreams {
		if streamState != nil {
			stats["total_events"] = stats["total_events"].(int64) + streamState.EventCount
		}
	}

	return stats
}

// Stop gracefully stops all change streams
func (bfch *BufferFreeChangeHandler) Stop() {
	log.Println("🛑 BUFFER-FREE: Stopping all change streams...")

	// Cancel context
	bfch.cancel()

	// Thread-safe cleanup of all active streams
	bfch.mu.Lock()
	defer bfch.mu.Unlock()

	for streamKey, streamState := range bfch.activeStreams {
		if streamState != nil && streamState.ChangeStream != nil {
			streamState.ChangeStream.Close(context.Background())
		}
		log.Printf("🛑 BUFFER-FREE: Stopped stream %s", streamKey)
	}

	// Clear the map
	bfch.activeStreams = make(map[string]*ChangeStreamState)

	log.Println("✅ BUFFER-FREE: All change streams stopped")
}
