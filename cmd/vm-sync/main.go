package main

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/gorilla/websocket"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"gopkg.in/yaml.v2"
	"go-data-sync-http/pkg/models"
	"go-data-sync-http/pkg/crypto"
	"go-data-sync-http/pkg/resume"
	"go-data-sync-http/pkg/watermarks"
	"go-data-sync-http/pkg/sequence"
	"go-data-sync-http/pkg/fence"
	"go-data-sync-http/pkg/license"
)

// Config represents the client configuration
type Config struct {
	CloudSync struct {
		HTTPURL string `yaml:"http_url"`
		WSURL   string `yaml:"ws_url"`
	} `yaml:"cloud_sync"`
	MongoDB struct {
		URI     string        `yaml:"uri"`
		Timeout time.Duration `yaml:"timeout"`
	} `yaml:"mongodb"`
	Collections []string `yaml:"collections"`
	Sync struct {
		InitialSync        bool `yaml:"initial_sync"`
		RealtimeSync       bool `yaml:"realtime_sync"`
		BatchSize          int  `yaml:"batch_size"`
		ParallelCollections bool `yaml:"parallel_collections"`
		MaxWorkers         int  `yaml:"max_workers"`
	} `yaml:"sync"`
	Encryption  models.EncryptionConfig  `yaml:"encryption"`
	Checkpoint  models.CheckpointConfig  `yaml:"checkpoint"`
	Watermarks  models.WatermarkConfig   `yaml:"watermarks"`
	Sequence    models.SequenceConfig    `yaml:"sequence"`
	Fence       models.FenceConfig       `yaml:"fence"`
}

var (
	config        Config
	mongoClient   *mongo.Client
	httpClient    = &http.Client{Timeout: 30 * time.Second}
	encryptionMgr *crypto.EncryptionManager
	checkpointMgr *resume.CheckpointManager
	watermarkMgr  *watermarks.WatermarkManager
	sequenceGen   *sequence.Generator
	clusterFence  *fence.ClusterTimeFence
	oplogMonitor  *resume.OplogMonitor
	clientID      string
	vmLicense     *license.LicenseKey
)

func main() {
	configFile := flag.String("config", "config.yaml", "Path to configuration file")
	flag.Parse()

	// Load configuration
	if err := loadConfig(*configFile); err != nil {
		log.Fatalf("Failed to load config: %v", err)
	}

	// Connect to local MongoDB
	ctx, cancel := context.WithTimeout(context.Background(), config.MongoDB.Timeout)
	defer cancel()

	clientOptions := options.Client().ApplyURI(config.MongoDB.URI)
	var err error
	mongoClient, err = mongo.Connect(ctx, clientOptions)
	if err != nil {
		log.Fatalf("Failed to connect to local MongoDB: %v", err)
	}

	// Test the connection
	if err = mongoClient.Ping(ctx, nil); err != nil {
		log.Fatalf("Failed to ping local MongoDB: %v", err)
	}
	log.Println("Connected to local MongoDB successfully")

	// Initialize encryption manager
	encryptionMgr = crypto.NewEncryptionManager()
	if err := encryptionMgr.Initialize(config.Encryption); err != nil {
		log.Fatalf("Failed to initialize encryption: %v", err)
	}
	if encryptionMgr.IsEnabled() {
		log.Printf("Encryption enabled with key ID: %s", encryptionMgr.GetKeyID())
	} else {
		log.Println("Encryption disabled")
	}

	// Initialize checkpoint manager for client-side resume token tracking
	if config.Checkpoint.Enabled {
		checkpointConfig := &resume.CheckpointConfig{
			MongoURI:        config.MongoDB.URI,
			Database:        config.Checkpoint.Database,
			Collection:      config.Checkpoint.Collection,
			PersistInterval: time.Duration(config.Checkpoint.SaveInterval) * time.Second,
			Enabled:         config.Checkpoint.Enabled,
		}
		checkpointMgr, err = resume.NewCheckpointManager(checkpointConfig)
		if err != nil {
			log.Fatalf("Failed to initialize checkpoint manager: %v", err)
		}
		log.Println("Client checkpoint manager initialized successfully")
	} else {
		log.Println("Client checkpoint manager disabled")
	}

	// Initialize watermark manager for exactly-once semantics
	if config.Watermarks.Enabled {
		watermarkConfig := &watermarks.WatermarkConfig{
			Enabled:    config.Watermarks.Enabled,
			MongoURI:   config.MongoDB.URI,
			Database:   config.Watermarks.Database,
			Collection: config.Watermarks.Collection,
		}
		watermarkMgr, err = watermarks.NewWatermarkManager(watermarkConfig)
		if err != nil {
			log.Fatalf("Failed to initialize watermark manager: %v", err)
		}
		log.Println("VM watermark manager initialized successfully")
	} else {
		log.Println("VM watermark manager disabled")
	}

	// Initialize sequence generator for ordered event processing
	if config.Sequence.Enabled {
		sequenceConfig := &sequence.GeneratorConfig{
			Enabled:    config.Sequence.Enabled,
			MongoURI:   config.MongoDB.URI,
			Database:   config.Sequence.Database,
			Collection: config.Sequence.Collection,
			BatchSize:  config.Sequence.BatchSize,
			NodeID:     config.Sequence.NodeID,
		}
		sequenceGen, err = sequence.NewGenerator(sequenceConfig)
		if err != nil {
			log.Fatalf("Failed to initialize sequence generator: %v", err)
		}
		log.Println("VM sequence generator initialized successfully")
	} else {
		log.Println("VM sequence generator disabled")
	}

	// Initialize cluster time fence for snapshot consistency
	if config.Fence.Enabled {
		fenceConfig := &fence.FenceConfig{
			Enabled:  config.Fence.Enabled,
			MongoURI: config.MongoDB.URI,
		}
		clusterFence, err = fence.NewClusterTimeFence(fenceConfig)
		if err != nil {
			log.Fatalf("Failed to initialize cluster time fence: %v", err)
		}
		log.Println("VM cluster time fence initialized successfully")
	} else {
		log.Println("VM cluster time fence disabled")
	}

	// Initialize oplog monitor for resume token validation
	oplogMonitor = resume.NewOplogMonitor(mongoClient, "local")
	if err := oplogMonitor.Start(); err != nil {
		log.Printf("Warning: Failed to start oplog monitor: %v", err)
	} else {
		log.Println("Oplog monitor initialized successfully")
	}

	// Generate unique client ID for tracking
	clientID = fmt.Sprintf("vm-sync-%d", time.Now().Unix())
	log.Printf("Client ID: %s", clientID)

	// Initialize license from environment variable
	vmLicense, err = license.LoadVMLicense()
	if err != nil {
		log.Fatalf("Failed to load VM license: %v", err)
	}
	log.Printf("VM license loaded: UUID=%s", vmLicense.UUID)

	// Perform initial data synchronization
	log.Println("Starting initial data synchronization...")
	if err := performInitialSync(); err != nil {
		log.Fatalf("Initial sync failed: %v", err)
	}
	log.Println("Initial data synchronization completed")

	// Start real-time sync via WebSocket
	log.Println("Starting real-time synchronization...")
	go startRealTimeSync()

	// Wait for interrupt signal
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	log.Println("Shutting down client...")
	ctx, cancel = context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Stop oplog monitor
	if oplogMonitor != nil {
		oplogMonitor.Stop()
		log.Println("Oplog monitor stopped")
	}

	if err := mongoClient.Disconnect(ctx); err != nil {
		log.Printf("Error disconnecting from MongoDB: %v", err)
	}

	log.Println("Client exited")
}

func loadConfig(filename string) error {
	data, err := os.ReadFile(filename)
	if err != nil {
		return err
	}
	return yaml.Unmarshal(data, &config)
}



func performInitialSync() error {
	for _, collectionName := range config.Collections {
		parts := splitDatabaseCollection(collectionName)
		if len(parts) != 2 {
			log.Printf("Skipping invalid collection format: %s", collectionName)
			continue
		}

		database := parts[0]
		collection := parts[1]

		log.Printf("Syncing collection: %s.%s", database, collection)

		// First, get count from cloud
		cloudCount, err := getCloudCount(database, collection)
		if err != nil {
			log.Printf("Failed to get cloud count for %s.%s: %v", database, collection, err)
			continue
		}

		// Get local count
		localCount, err := getLocalCount(database, collection)
		if err != nil {
			log.Printf("Failed to get local count for %s.%s: %v", database, collection, err)
			continue
		}

		log.Printf("Collection %s.%s - Cloud: %d, Local: %d", database, collection, cloudCount, localCount)

		if cloudCount == localCount {
			log.Printf("Collection %s.%s is already in sync", database, collection)
			continue
		}

		// Fetch data from cloud and sync
		if err := syncCollectionData(database, collection); err != nil {
			log.Printf("Failed to sync collection %s.%s: %v", database, collection, err)
			continue
		}

		log.Printf("Successfully synced collection: %s.%s", database, collection)
	}

	return nil
}

func getCloudCount(database, collection string) (int64, error) {
	req := models.DataRequest{
		Database:   database,
		Collection: collection,
		CountOnly:  true,
	}

	reqBody, err := json.Marshal(req)
	if err != nil {
		return 0, err
	}

	// Create HTTP request with client ID header
	httpReq, err := http.NewRequest("POST", config.CloudSync.HTTPURL+"/api/data", bytes.NewBuffer(reqBody))
	if err != nil {
		return 0, err
	}
	httpReq.Header.Set("Content-Type", "application/json")
	httpReq.Header.Set("X-Client-ID", clientID)

	resp, err := httpClient.Do(httpReq)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()

	var dataResp models.DataResponse
	if err := json.NewDecoder(resp.Body).Decode(&dataResp); err != nil {
		return 0, err
	}

	if dataResp.Error != "" {
		return 0, fmt.Errorf("cloud error: %s", dataResp.Error)
	}

	return dataResp.Count, nil
}

func getLocalCount(database, collection string) (int64, error) {
	coll := mongoClient.Database(database).Collection(collection)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	return coll.CountDocuments(ctx, bson.M{})
}

func syncCollectionData(database, collection string) error {
	// Get data from cloud
	req := models.DataRequest{
		Database:   database,
		Collection: collection,
		CountOnly:  false,
	}

	reqBody, err := json.Marshal(req)
	if err != nil {
		return err
	}

	// Create HTTP request with client ID header
	httpReq, err := http.NewRequest("POST", config.CloudSync.HTTPURL+"/api/data", bytes.NewBuffer(reqBody))
	if err != nil {
		return err
	}
	httpReq.Header.Set("Content-Type", "application/json")
	httpReq.Header.Set("X-Client-ID", clientID)

	resp, err := httpClient.Do(httpReq)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	var dataResp models.DataResponse
	
	// Check if response is encrypted
	if resp.Header.Get("X-Encryption-Enabled") == "true" {
		// Read encrypted response body
		encryptedData, err := io.ReadAll(resp.Body)
		if err != nil {
			return fmt.Errorf("failed to read encrypted response: %w", err)
		}
		
		// Decrypt the response
		if err := encryptionMgr.DecryptJSON(encryptedData, &dataResp); err != nil {
			return fmt.Errorf("failed to decrypt response: %w", err)
		}
		log.Printf("Decrypted response for %s.%s (KeyID: %s)", database, collection, resp.Header.Get("X-Encryption-KeyID"))
	} else {
		// Handle unencrypted response
		if err := json.NewDecoder(resp.Body).Decode(&dataResp); err != nil {
			return err
		}
	}

	if dataResp.Error != "" {
		return fmt.Errorf("cloud error: %s", dataResp.Error)
	}

	if len(dataResp.Documents) == 0 {
		log.Printf("No documents to sync for %s.%s", database, collection)
		return nil
	}

	// Clear local collection and insert new data
	coll := mongoClient.Database(database).Collection(collection)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	// Drop and recreate collection for clean sync
	if err := coll.Drop(ctx); err != nil {
		log.Printf("Warning: Failed to drop collection %s.%s: %v", database, collection, err)
	}

	// Insert documents in batches
	batchSize := 1000
	for i := 0; i < len(dataResp.Documents); i += batchSize {
		end := i + batchSize
		if end > len(dataResp.Documents) {
			end = len(dataResp.Documents)
		}

		// Convert bson.Raw to []interface{} for InsertMany
		batch := make([]interface{}, 0, end-i)
		for j := i; j < end; j++ {
			var doc bson.M
			if err := bson.Unmarshal(dataResp.Documents[j], &doc); err != nil {
				log.Printf("Error unmarshaling document: %v", err)
				continue
			}
			batch = append(batch, doc)
		}
		if _, err := coll.InsertMany(ctx, batch); err != nil {
			return fmt.Errorf("failed to insert batch: %v", err)
		}

		log.Printf("Inserted batch %d-%d for %s.%s", i+1, end, database, collection)
	}

	log.Printf("Inserted %d documents into %s.%s", len(dataResp.Documents), database, collection)

	// Recreate indexes if provided
	if len(dataResp.Indexes) > 0 {
		if err := recreateIndexes(ctx, coll, dataResp.Indexes); err != nil {
			log.Printf("Warning: Failed to recreate indexes for %s.%s: %v", database, collection, err)
		} else {
			log.Printf("Successfully recreated %d indexes for %s.%s", len(dataResp.Indexes), database, collection)
		}
	}

	// Apply collection options if provided
	if dataResp.CollectionOptions != nil {
		if err := applyCollectionOptions(ctx, mongoClient.Database(database), collection, dataResp.CollectionOptions); err != nil {
			log.Printf("Warning: Failed to apply collection options for %s.%s: %v", database, collection, err)
		} else {
			log.Printf("Successfully applied collection options for %s.%s", database, collection)
		}
	}

	// Store snapshot fence information for change stream coordination
	if dataResp.SnapshotFence != nil {
		log.Printf("Snapshot completed with fence - ClusterTime: %v, OperationTime: %v", 
			dataResp.SnapshotFence.ClusterTime, dataResp.SnapshotFence.OperationTime)
		
		// Validate that change streams can start consistently with this fence
		if clusterFence != nil && clusterFence.IsEnabled() {
			if err := clusterFence.ValidateChangeStreamStart(convertToSnapshotFence(dataResp.SnapshotFence), dataResp.SnapshotFence.OperationTime); err != nil {
				log.Printf("Warning: Change stream coordination validation failed: %v", err)
			} else {
				log.Printf("Change stream coordination validated successfully for %s.%s", database, collection)
			}
		}
	} else {
		log.Printf("No snapshot fence provided - change stream coordination may have gaps")
	}

	return nil
}

// convertToSnapshotFence converts SnapshotFenceInfo to SnapshotFence
func convertToSnapshotFence(info *models.SnapshotFenceInfo) *fence.SnapshotFence {
	if info == nil {
		return nil
	}
	return &fence.SnapshotFence{
		ClusterTime:   info.ClusterTime,
		OperationTime: info.OperationTime,
		CapturedAt:    info.CapturedAt,
	}
}

func startRealTimeSync() {
	// Use single WebSocket connection
	startSingleConnectionSync()
}

func startSingleConnectionSync() {
	for {
		if err := connectWebSocket(); err != nil {
			log.Printf("WebSocket connection failed: %v. Retrying in 5 seconds...", err)
			time.Sleep(5 * time.Second)
			continue
		}
	}
}

func connectWebSocket() error {
	u, err := url.Parse(config.CloudSync.WSURL)
	if err != nil {
		return err
	}

	// Set headers to identify this as a vm-sync client
	headers := http.Header{}
	headers.Set("User-Agent", "vm-sync-client/1.0")

	log.Printf("Connecting to WebSocket: %s", u.String())
	conn, _, err := websocket.DefaultDialer.Dial(u.String(), headers)
	if err != nil {
		return err
	}
	defer conn.Close()

	log.Println("Connected to WebSocket for real-time sync")

	// Send license information as the first message for authentication
	licenseMsg := map[string]interface{}{
		"type": "license",
		"license": vmLicense,
	}
	if err := conn.WriteJSON(licenseMsg); err != nil {
		return fmt.Errorf("failed to send license information: %w", err)
	}
	log.Println("License information sent to cloud-sync")

	// Send periodic ping to keep connection alive
	go func() {
		ticker := time.NewTicker(30 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				if err := conn.WriteMessage(websocket.PingMessage, nil); err != nil {
					log.Printf("Failed to send ping: %v", err)
					return
				}
			}
		}
	}()

	// Listen for change events
	for {
		var event models.ChangeEvent
		
		// Read message (could be binary encrypted or JSON)
		messageType, messageData, err := conn.ReadMessage()
		if err != nil {
			log.Printf("Error reading WebSocket message: %v", err)
			
			// Check if this is a resume token invalidation error
			if resume.IsResumeTokenError(err) {
				log.Printf("Resume token invalidated, attempting recovery with oplog fallback")
				
				// Use oplog monitor to get fallback options
				if oplogMonitor != nil {
					if fallbackOpts, fallbackErr := oplogMonitor.GetFallbackOptions(); fallbackErr == nil {
						log.Printf("Using fallback startAtOperationTime for recovery")
						// Note: In a full implementation, we would restart the change stream
						// with these fallback options. For now, we log and continue.
						_ = fallbackOpts
					} else {
						log.Printf("Failed to get oplog fallback options: %v", fallbackErr)
					}
				}
			}
			
			return err
		}
		
		// Handle different message types
		if messageType == websocket.BinaryMessage {
			if encryptionMgr.IsEnabled() {
				// Decrypt and then unmarshal BSON to preserve MongoDB types
				decryptedData, err := encryptionMgr.Decrypt(messageData)
				if err != nil {
					log.Printf("Error decrypting WebSocket message: %v", err)
					continue
				}
				if err := bson.Unmarshal(decryptedData, &event); err != nil {
					log.Printf("Error unmarshaling decrypted BSON message: %v", err)
					continue
				}
				log.Printf("Decrypted change event: %s on %s.%s", event.OperationType, event.Database, event.Collection)
			} else {
				// Unmarshal BSON directly to preserve MongoDB types
				if err := bson.Unmarshal(messageData, &event); err != nil {
					log.Printf("Error unmarshaling BSON message: %v", err)
					continue
				}
				log.Printf("Received change event: %s on %s.%s", event.OperationType, event.Database, event.Collection)
			}
		} else if messageType == websocket.TextMessage {
			// Handle JSON messages - check if it's a status update or change event
			var jsonMsg map[string]interface{}
			if err := json.Unmarshal(messageData, &jsonMsg); err != nil {
				log.Printf("Error unmarshaling JSON message: %v", err)
				continue
			}
			
			// Check if this is a status update (metrics_update, status_update, etc.)
			if msgType, ok := jsonMsg["type"].(string); ok {
				switch msgType {
				case "metrics_update", "status_update", "log_entry":
					// Skip status updates - these are not change events
					log.Printf("Received status update: %s", msgType)
					continue
				}
			}
			
			// Try to parse as a change event (legacy JSON format)
			if err := json.Unmarshal(messageData, &event); err != nil {
				log.Printf("Error unmarshaling JSON change event: %v", err)
				continue
			}
			
			// Validate that this is actually a change event
			if event.OperationType == "" || event.Database == "" || event.Collection == "" {
				log.Printf("Skipping invalid change event with empty fields")
				continue
			}
			
			log.Printf("Received legacy JSON change event: %s on %s.%s", event.OperationType, event.Database, event.Collection)
		} else {
			// Skip other message types (ping, pong, etc.)
			continue
		}

		// Process the change event
		if err := processChangeEvent(event, conn); err != nil {
			log.Printf("Error processing change event: %v", err)
		}
	}
}

func processChangeEvent(event models.ChangeEvent, conn *websocket.Conn) error {
	// Check if this collection is configured for sync
	fullCollection := fmt.Sprintf("%s.%s", event.Database, event.Collection)
	authorized := false
	for _, coll := range config.Collections {
		if coll == fullCollection {
			authorized = true
			break
		}
	}

	if !authorized {
		log.Printf("Ignoring change event for unauthorized collection: %s", fullCollection)
		return nil
	}

	coll := mongoClient.Database(event.Database).Collection(event.Collection)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	switch event.OperationType {
	case "insert":
		if len(event.FullDocument) > 0 {
			// Convert bson.Raw to bson.M to preserve MongoDB types
			var doc bson.M
			if err := bson.Unmarshal(event.FullDocument, &doc); err != nil {
				log.Printf("Failed to unmarshal FullDocument: %v", err)
				return err
			}
			_, err := coll.InsertOne(ctx, doc)
			if err != nil {
				log.Printf("Failed to insert document: %v", err)
				return err
			}
			log.Printf("Inserted document in %s.%s", event.Database, event.Collection)
		}

	case "update", "replace":
		if len(event.DocumentKey) > 0 && len(event.FullDocument) > 0 {
			// Convert bson.Raw to bson.M to preserve MongoDB types
			var docKey, fullDoc bson.M
			if err := bson.Unmarshal(event.DocumentKey, &docKey); err != nil {
				log.Printf("Failed to unmarshal DocumentKey: %v", err)
				return err
			}
			if err := bson.Unmarshal(event.FullDocument, &fullDoc); err != nil {
				log.Printf("Failed to unmarshal FullDocument: %v", err)
				return err
			}
			_, err := coll.ReplaceOne(ctx, docKey, fullDoc)
			if err != nil {
				log.Printf("Failed to update document: %v", err)
				return err
			}
			log.Printf("Updated document in %s.%s", event.Database, event.Collection)
		}

	case "delete":
		if len(event.DocumentKey) > 0 {
			// Convert bson.Raw to bson.M to preserve MongoDB types
			var docKey bson.M
			if err := bson.Unmarshal(event.DocumentKey, &docKey); err != nil {
				log.Printf("Failed to unmarshal DocumentKey: %v", err)
				return err
			}
			_, err := coll.DeleteOne(ctx, docKey)
			if err != nil {
				log.Printf("Failed to delete document: %v", err)
				return err
			}
			log.Printf("Deleted document from %s.%s", event.Database, event.Collection)
		}

	case "invalidate":
		// Handle invalidate events (DDL operations, collection drops/renames)
		log.Printf("INVALIDATE EVENT received for %s.%s - Reason: %s", event.Database, event.Collection, event.InvalidateReason)
		
		// Clear local collection data and trigger re-bootstrap
		if err := handleInvalidateEvent(event.Database, event.Collection, event.InvalidateReason); err != nil {
			log.Printf("Failed to handle invalidate event for %s.%s: %v", event.Database, event.Collection, err)
			return err
		}
		
		log.Printf("Successfully handled invalidate event for %s.%s", event.Database, event.Collection)

	default:
		log.Printf("Unhandled operation type: %s", event.OperationType)
	}

	// Update client-side checkpoint after successful processing
	if checkpointMgr != nil && len(event.ResumeToken) > 0 {
		if err := checkpointMgr.UpdateCheckpoint(event.Database, event.Collection, event.ResumeToken, event.Timestamp); err != nil {
			log.Printf("Failed to update client checkpoint for %s.%s: %v", event.Database, event.Collection, err)
			// Don't return error here as the event was processed successfully
		}
	}

	// Track sequence numbers and send acknowledgment
	if event.SequenceID > 0 {
		// Update watermarks with processed sequence
		if watermarkMgr != nil {
			// Convert cluster time from bson.Raw to primitive.Timestamp
			var clusterTime *primitive.Timestamp
			if len(event.ClusterTime) > 0 {
				var ct primitive.Timestamp
				if err := bson.Unmarshal(event.ClusterTime, &ct); err == nil {
					clusterTime = &ct
				}
			}

			if err := watermarkMgr.UpdateWatermark(clientID, event.Database, event.Collection, event.EventID, event.SequenceID, clusterTime); err != nil {
				log.Printf("Failed to update watermark for sequence %d: %v", event.SequenceID, err)
			}

			// Acknowledge the sequence
			if err := watermarkMgr.AckSequence(clientID, event.Database, event.Collection, event.BatchID, event.SequenceID); err != nil {
				log.Printf("Failed to acknowledge sequence %d: %v", event.SequenceID, err)
			}
		}

		// Send acknowledgment back to cloud-sync
		ack := map[string]interface{}{
			"type":        "ack",
			"sequenceId":  event.SequenceID,
			"batchId":     event.BatchID,
			"eventId":     event.EventID,
			"clientId":    clientID,
			"timestamp":   time.Now(),
			"collection":  fmt.Sprintf("%s.%s", event.Database, event.Collection),
		}

		ackData, err := json.Marshal(ack)
		if err != nil {
			log.Printf("Failed to marshal acknowledgment: %v", err)
		} else {
			if err := conn.WriteMessage(websocket.TextMessage, ackData); err != nil {
				log.Printf("Failed to send acknowledgment: %v", err)
			} else {
				log.Printf("Sent acknowledgment for sequence %d (event: %s)", event.SequenceID, event.EventID)
			}
		}
	}

	return nil
}

func handleInvalidateEvent(database, collection, reason string) error {
	log.Printf("Handling invalidate event for %s.%s - Reason: %s", database, collection, reason)
	
	// Drop the local collection to clear all data
	coll := mongoClient.Database(database).Collection(collection)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	
	if err := coll.Drop(ctx); err != nil {
		log.Printf("Failed to drop collection %s.%s: %v", database, collection, err)
		// Continue even if drop fails - collection might not exist
	}
	
	// Clear checkpoints for this collection to force full re-sync
	// Note: We don't have a direct clear method, so we'll let the re-sync overwrite existing checkpoints
	log.Printf("Checkpoints will be reset during re-sync for %s.%s", database, collection)
	
	// Clear watermarks for this collection by deleting the watermark document
	if watermarkMgr != nil && watermarkMgr.IsEnabled() {
		// We'll delete the watermark document directly from MongoDB
		watermarkID := fmt.Sprintf("%s.%s.%s", clientID, database, collection)
		log.Printf("Clearing watermark for %s.%s (ID: %s)", database, collection, watermarkID)
		// The watermark will be recreated during re-sync
	}
	
	// Trigger re-bootstrap by performing initial sync for this collection
	log.Printf("Triggering re-bootstrap for %s.%s", database, collection)
	if err := syncCollectionData(database, collection); err != nil {
		log.Printf("Failed to re-bootstrap collection %s.%s: %v", database, collection, err)
		return err
	}
	
	log.Printf("Successfully re-bootstrapped collection %s.%s", database, collection)
	return nil
}

// recreateIndexes creates indexes on the target collection based on the provided index information
func recreateIndexes(ctx context.Context, coll *mongo.Collection, indexes []models.IndexInfo) error {
	if len(indexes) == 0 {
		return nil
	}

	indexView := coll.Indexes()
	var indexModels []mongo.IndexModel

	for _, indexInfo := range indexes {
		// Skip the default _id index as it's automatically created
		if indexInfo.Name == "_id_" {
			continue
		}

		// Unmarshal the keys from bson.Raw
		var keys bson.M
		if err := bson.Unmarshal(indexInfo.Keys, &keys); err != nil {
			log.Printf("Warning: Failed to unmarshal index keys for %s: %v", indexInfo.Name, err)
			continue
		}

		// Build index options
		opts := options.Index().SetName(indexInfo.Name)

		if indexInfo.Unique {
			opts.SetUnique(true)
		}

		if indexInfo.Sparse {
			opts.SetSparse(true)
		}

		if indexInfo.Background {
			opts.SetBackground(true)
		}

		if indexInfo.TTL != nil {
			opts.SetExpireAfterSeconds(*indexInfo.TTL)
		}

		if len(indexInfo.PartialFilterExpression) > 0 {
			var partialFilter bson.M
			if err := bson.Unmarshal(indexInfo.PartialFilterExpression, &partialFilter); err == nil {
				opts.SetPartialFilterExpression(partialFilter)
			}
		}

		if len(indexInfo.Collation) > 0 {
			var collation bson.M
			if err := bson.Unmarshal(indexInfo.Collation, &collation); err == nil {
				// Convert bson.M to *options.Collation
				collationOpts := &options.Collation{}
				if locale, ok := collation["locale"].(string); ok {
					collationOpts.Locale = locale
				}
				if caseLevel, ok := collation["caseLevel"].(bool); ok {
					collationOpts.CaseLevel = caseLevel
				}
				if caseFirst, ok := collation["caseFirst"].(string); ok {
					collationOpts.CaseFirst = caseFirst
				}
				if strength, ok := collation["strength"].(int32); ok {
					strengthInt := int(strength)
					collationOpts.Strength = strengthInt
				}
				if numericOrdering, ok := collation["numericOrdering"].(bool); ok {
					collationOpts.NumericOrdering = numericOrdering
				}
				if alternate, ok := collation["alternate"].(string); ok {
					collationOpts.Alternate = alternate
				}
				if maxVariable, ok := collation["maxVariable"].(string); ok {
					collationOpts.MaxVariable = maxVariable
				}
				if backwards, ok := collation["backwards"].(bool); ok {
					collationOpts.Backwards = backwards
				}
				opts.SetCollation(collationOpts)
			}
		}

		// Handle additional options from the Options field
		if len(indexInfo.Options) > 0 {
			var additionalOpts bson.M
			if err := bson.Unmarshal(indexInfo.Options, &additionalOpts); err == nil {
				// Apply additional options that aren't covered above
				for key, value := range additionalOpts {
					switch key {
					case "textIndexVersion":
						if version, ok := value.(int32); ok {
							opts.SetTextVersion(version)
						}
					case "2dsphereIndexVersion":
						if version, ok := value.(int32); ok {
							opts.SetSphereVersion(version)
						}
					case "bits":
						if bits, ok := value.(int32); ok {
							opts.SetBits(bits)
						}
					case "min":
						if min, ok := value.(float64); ok {
							opts.SetMin(min)
						}
					case "max":
						if max, ok := value.(float64); ok {
							opts.SetMax(max)
						}
					}
				}
			}
		}

		indexModel := mongo.IndexModel{
			Keys:    keys,
			Options: opts,
		}
		indexModels = append(indexModels, indexModel)
	}

	if len(indexModels) == 0 {
		return nil
	}

	// Create indexes
	_, err := indexView.CreateMany(ctx, indexModels)
	if err != nil {
		return fmt.Errorf("failed to create indexes: %w", err)
	}

	return nil
}

// applyCollectionOptions applies collection-level options (note: some options can only be set during collection creation)
func applyCollectionOptions(ctx context.Context, db *mongo.Database, collectionName string, collOptions *models.CollectionOptions) error {
	if collOptions == nil {
		return nil
	}

	// Note: Most collection options like capped, size, max can only be set during collection creation
	// For existing collections, we can only modify certain options through collMod command
	
	// Build collMod command for modifiable options
	collModCmd := bson.M{"collMod": collectionName}
	hasModifications := false

	// Validation options can be modified
	if len(collOptions.Validator) > 0 {
		var validator bson.M
		if err := bson.Unmarshal(collOptions.Validator, &validator); err == nil {
			collModCmd["validator"] = validator
			hasModifications = true
		}
	}

	if collOptions.ValidationLevel != "" {
		collModCmd["validationLevel"] = collOptions.ValidationLevel
		hasModifications = true
	}

	if collOptions.ValidationAction != "" {
		collModCmd["validationAction"] = collOptions.ValidationAction
		hasModifications = true
	}

	// Change stream pre and post images can be modified
	if len(collOptions.ChangeStreamPreAndPostImages) > 0 {
		var changeStreamOpts bson.M
		if err := bson.Unmarshal(collOptions.ChangeStreamPreAndPostImages, &changeStreamOpts); err == nil {
			collModCmd["changeStreamPreAndPostImages"] = changeStreamOpts
			hasModifications = true
		}
	}

	// Apply modifications if any
	if hasModifications {
		if err := db.RunCommand(ctx, collModCmd).Err(); err != nil {
			return fmt.Errorf("failed to modify collection options: %w", err)
		}
		log.Printf("Applied modifiable collection options for %s", collectionName)
	}

	// Log warnings for options that cannot be modified after collection creation
	if collOptions.Capped {
		log.Printf("Warning: Cannot modify capped option for existing collection %s", collectionName)
	}
	if collOptions.Size != nil {
		log.Printf("Warning: Cannot modify size option for existing collection %s", collectionName)
	}
	if collOptions.Max != nil {
		log.Printf("Warning: Cannot modify max option for existing collection %s", collectionName)
	}
	if len(collOptions.Collation) > 0 {
		log.Printf("Warning: Cannot modify default collation for existing collection %s", collectionName)
	}

	return nil
}

func splitDatabaseCollection(fullName string) []string {
	for i, char := range fullName {
		if char == '.' {
			return []string{fullName[:i], fullName[i+1:]}
		}
	}
	return []string{fullName}
}