package main

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"go-data-sync-http/pkg/adaptive"
	"go-data-sync-http/pkg/auth"
	"go-data-sync-http/pkg/crypto"
	"go-data-sync-http/pkg/fence"
	"go-data-sync-http/pkg/models"
	"go-data-sync-http/pkg/resume"
	"go-data-sync-http/pkg/sequence"
	"go-data-sync-http/pkg/telemetry"
	"go-data-sync-http/pkg/transport"
	"go-data-sync-http/pkg/watermarks"

	"github.com/gorilla/mux"
	"github.com/gorilla/websocket"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/writeconcern"
	"gopkg.in/yaml.v2"
)

// Config represents the client configuration
type Config struct {
	Server struct {
		Port         int           `yaml:"port"`
		Host         string        `yaml:"host"`
		ReadTimeout  time.Duration `yaml:"read_timeout"`
		WriteTimeout time.Duration `yaml:"write_timeout"`
		IdleTimeout  time.Duration `yaml:"idle_timeout"`
	} `yaml:"server"`
	CloudSync struct {
		HTTPURL     string        `yaml:"http_url"`
		WSURL       string        `yaml:"ws_url"`
		HTTPTimeout time.Duration `yaml:"http_timeout"`
		// OAuth2 authentication (preferred over license)
		OAuth2 struct {
			Enabled      bool   `yaml:"enabled"`
			ClientID     string `yaml:"client_id"`
			ClientSecret string `yaml:"client_secret"`
			TokenURL     string `yaml:"token_url"`
		} `yaml:"oauth2"`
	} `yaml:"cloud_sync"`
	MongoDB struct {
		URI     string        `yaml:"uri"`
		Timeout time.Duration `yaml:"timeout"`
	} `yaml:"mongodb"`
	Collections []string `yaml:"collections"`
	Sync        struct {
		InitialSync          bool                   `yaml:"initial_sync"`
		RealtimeSync         bool                   `yaml:"realtime_sync"`
		ResumableInitialSync bool                   `yaml:"resumable_initial_sync"`
		BatchSize            int                    `yaml:"batch_size"`
		ParallelCollections  bool                   `yaml:"parallel_collections"`
		MaxWorkers           int                    `yaml:"max_workers"`
		Transport            models.TransportConfig `yaml:"transport"` // Transport configuration
	} `yaml:"sync"`
	Encryption models.EncryptionConfig `yaml:"encryption"`
	Checkpoint models.CheckpointConfig `yaml:"checkpoint"`
	Watermarks models.WatermarkConfig  `yaml:"watermarks"`
	Sequence   models.SequenceConfig   `yaml:"sequence"`
	Fence      models.FenceConfig      `yaml:"fence"`
}

// PageResult represents the result of fetching a single page
type PageResult struct {
	PageNumber        int
	Documents         []bson.Raw
	Error             error
	Indexes           []models.IndexInfo
	CollectionOptions *models.CollectionOptions
	SnapshotFence     *models.SnapshotFenceInfo
	IsLastPage        bool
}

// WorkerPool manages parallel page fetching
type WorkerPool struct {
	workerCount   int
	maxMemoryMB   int
	currentMemory int64
	memoryMutex   sync.RWMutex
}

var (
	config         Config
	mongoClient    *mongo.Client
	httpClient     *http.Client
	encryptionMgr  *crypto.EncryptionManager
	checkpointMgr  *resume.CheckpointManager
	watermarkMgr   *watermarks.WatermarkManager
	sequenceGen    *sequence.Generator
	clusterFence   *fence.ClusterTimeFence
	oplogMonitor   *resume.OplogMonitor
	clientID       string
	vmTokenManager *auth.VMTokenManager // OAuth2 token manager
	workerPool     *WorkerPool
	// Adaptive system components
	telemetryCollector *telemetry.Collector
	vmSyncIntegration  *adaptive.VMSyncIntegration
	// HTTP server for graceful shutdown
	httpServer *http.Server

	// WebSocket connection for graceful shutdown
	websocketConn      *websocket.Conn
	websocketConnMutex sync.RWMutex

	// TCP transport for high-performance data transfer
	tcpReceiver         transport.Receiver
	tcpTransportEnabled bool
	tcpTransportConfig  models.TransportConfig
)

// initializeTCPTransport initializes the TCP transport receiver for high-performance data transfer
func initializeTCPTransport() error {
	// Store transport config globally
	tcpTransportConfig = config.Sync.Transport

	// Check if TCP transport is enabled in config
	if tcpTransportConfig.Mode != "tcp" {
		log.Printf("TCP transport disabled, using mode: %s", tcpTransportConfig.Mode)
		return nil
	}

	// Validate TCP receiver configuration
	if tcpTransportConfig.TCPReceiver.ListenAddr == "" {
		return fmt.Errorf("TCP receiver listen address not configured")
	}

	// Create high-performance TCP receiver configuration for billion-document transfers
	receiverConfig := transport.ReceiverConfig{
		ListenAddr:        tcpTransportConfig.TCPReceiver.ListenAddr,
		MaxConnections:    tcpTransportConfig.TCPReceiver.MaxConnections,
		ReadTimeout:       tcpTransportConfig.TCPReceiver.ReadTimeout,
		WriteTimeout:      tcpTransportConfig.TCPReceiver.WriteTimeout,
		BufferSize:        tcpTransportConfig.TCPReceiver.BufferSize,
		DiskCheckpoint:    tcpTransportConfig.TCPReceiver.DiskCheckpoint,
		CheckpointDir:     tcpTransportConfig.TCPReceiver.CheckpointDir,
		HeartbeatInterval: tcpTransportConfig.TCPReceiver.HeartbeatInterval,
		MaxBatchSize:      tcpTransportConfig.TCPReceiver.MaxBatchSize,
	}

	// OPTIMIZED: Apply high-performance defaults for massive datasets (billions of documents)
	if receiverConfig.MaxConnections <= 0 {
		receiverConfig.MaxConnections = 20 // Increased for billion-document performance
	}
	if receiverConfig.ReadTimeout == 0 {
		receiverConfig.ReadTimeout = 120 * time.Second // Longer timeout for large transfers
	}
	if receiverConfig.WriteTimeout == 0 {
		receiverConfig.WriteTimeout = 60 * time.Second // Longer write timeout
	}
	if receiverConfig.BufferSize <= 0 {
		receiverConfig.BufferSize = 2 * 1024 * 1024 // 2MB buffer for billion-document transfers
	}
	if receiverConfig.HeartbeatInterval == 0 {
		receiverConfig.HeartbeatInterval = 30 * time.Second
	}
	if receiverConfig.MaxBatchSize <= 0 {
		receiverConfig.MaxBatchSize = 128 * 1024 * 1024 // 128MB max batch for massive transfers
	}
	if receiverConfig.CheckpointDir == "" {
		receiverConfig.CheckpointDir = "/tmp/vm-sync-tcp-checkpoints"
	}

	// Force enable disk checkpointing for billion-document reliability
	if !receiverConfig.DiskCheckpoint {
		log.Printf("💾 FORCING CHECKPOINT: Enabling disk checkpointing for billion-document reliability")
		receiverConfig.DiskCheckpoint = true
	}

	// Create TCP receiver
	receiver, err := transport.NewReceiver(receiverConfig)
	if err != nil {
		return fmt.Errorf("failed to create TCP receiver: %w", err)
	}

	// Set up batch handler for document processing with high-performance processing
	receiver.OnBatch(func(stream string, batchSeq uint64, documents [][]byte) error {
		return handleTCPBatchOptimized(stream, batchSeq, documents)
	})
	log.Printf("✅ TCP BATCH HANDLER: Registered handleTCPBatchOptimized")

	// Set up error handler
	receiver.OnError(func(err error) {
		log.Printf("🔴 TCP TRANSPORT ERROR: %v", err)
	})
	log.Printf("✅ TCP ERROR HANDLER: Registered error handler")

	// Start the TCP receiver
	log.Printf("🚀 TCP RECEIVER STARTING: listen_addr=%s", receiverConfig.ListenAddr)
	if err := receiver.Start(); err != nil {
		return fmt.Errorf("failed to start TCP receiver: %w", err)
	}

	tcpReceiver = receiver
	tcpTransportEnabled = true

	log.Printf("🚀 TCP RECEIVER OPTIMIZED: listen_addr=%s, max_connections=%d, buffer=%s, max_batch=%s, checkpoints=%s, compression=%s",
		receiverConfig.ListenAddr, receiverConfig.MaxConnections,
		formatBytes(receiverConfig.BufferSize), formatBytes(receiverConfig.MaxBatchSize),
		receiverConfig.CheckpointDir, tcpTransportConfig.CompressionType)
	return nil
}

// Global stream mapping to track which stream ID corresponds to which collection
var (
	streamCollectionMap = make(map[string]string) // streamID -> "database.collection"
	streamMapMutex      sync.RWMutex
	configCollections   []string // Store configured collections for mapping
)

// getOrAssignStreamMapping gets or assigns a stream to a collection
// This implements a round-robin assignment for TCP streams since we can't reverse the hash
func getOrAssignStreamMapping(stream string) (database, collection string, err error) {
	streamMapMutex.Lock()
	defer streamMapMutex.Unlock()

	// Check if we already have a mapping for this stream
	if mappedCollection, exists := streamCollectionMap[stream]; exists {
		parts := strings.Split(mappedCollection, ".")
		if len(parts) == 2 {
			return parts[0], parts[1], nil
		}
	}

	// Assign a new mapping using round-robin based on current map size
	if len(configCollections) == 0 {
		return "", "", fmt.Errorf("no collections configured")
	}

	// Use map size to determine which collection to assign (round-robin)
	collectionIndex := len(streamCollectionMap) % len(configCollections)
	selectedConfig := configCollections[collectionIndex]

	// Parse the selected configuration ("source:target" format)
	collParts := strings.Split(selectedConfig, ":")
	sourcePattern := strings.TrimSpace(collParts[0])

	if !strings.Contains(sourcePattern, ".") {
		return "", "", fmt.Errorf("invalid source collection format: %s", sourcePattern)
	}

	dbCollParts := strings.Split(sourcePattern, ".")
	if len(dbCollParts) != 2 {
		return "", "", fmt.Errorf("invalid source collection format: %s", sourcePattern)
	}

	// Store the mapping
	streamCollectionMap[stream] = sourcePattern

	log.Printf("🔍 STREAM ASSIGNMENT: %s -> %s (index %d)", stream, sourcePattern, collectionIndex)

	return dbCollParts[0], dbCollParts[1], nil
}

// mapSourceToTarget maps source collection to target collection based on configuration
func mapSourceToTarget(sourceCollection string) (targetDatabase, targetCollection string, err error) {
	log.Printf("🔍 DEBUG MAP: Input sourceCollection='%s'", sourceCollection)
	log.Printf("🔍 DEBUG MAP: Available config.Collections=%v", config.Collections)

	// Check each configured collection mapping
	for i, collMapping := range config.Collections {
		log.Printf("🔍 DEBUG MAP: Checking mapping %d: '%s'", i, collMapping)
		// Handle both formats: "source" and "source:target"
		parts := strings.Split(collMapping, ":")
		sourcePattern := strings.TrimSpace(parts[0])
		log.Printf("🔍 DEBUG MAP: sourcePattern='%s' vs sourceCollection='%s'", sourcePattern, sourceCollection)

		if sourcePattern == sourceCollection {
			log.Printf("✅ DEBUG MAP: MATCH FOUND for '%s'", sourceCollection)
			if len(parts) > 1 {
				// Use target mapping
				targetPattern := strings.TrimSpace(parts[1])
				targetParts := strings.Split(targetPattern, ".")
				log.Printf("🔍 DEBUG MAP: targetPattern='%s', targetParts=%v", targetPattern, targetParts)
				if len(targetParts) == 2 {
					log.Printf("✅ DEBUG MAP: SUCCESS -> targetDatabase='%s', targetCollection='%s'", targetParts[0], targetParts[1])
					return targetParts[0], targetParts[1], nil
				}
			} else {
				// Use same as source
				sourceParts := strings.Split(sourceCollection, ".")
				log.Printf("🔍 DEBUG MAP: No target mapping, using source. sourceParts=%v", sourceParts)
				if len(sourceParts) == 2 {
					log.Printf("✅ DEBUG MAP: SUCCESS (same as source) -> targetDatabase='%s', targetCollection='%s'", sourceParts[0], sourceParts[1])
					return sourceParts[0], sourceParts[1], nil
				}
			}
		}
	}

	log.Printf("🔴 DEBUG MAP: NO MAPPING FOUND for '%s'", sourceCollection)
	return "", "", fmt.Errorf("no mapping found for source collection: %s", sourceCollection)
}

// handleTCPBatchOptimized processes a batch of documents received via TCP with billion-document optimizations
func handleTCPBatchOptimized(stream string, batchSeq uint64, documents [][]byte) error {
	log.Printf("🔹 TCP BATCH HANDLER CALLED: stream=%s seq=%d docs=%d", stream, batchSeq, len(documents))

	if len(documents) == 0 {
		log.Printf("⚠️ TCP BATCH EMPTY: stream=%s seq=%d", stream, batchSeq)
		return nil
	}

	startTime := time.Now()
	totalBytes := 0
	for _, doc := range documents {
		totalBytes += len(doc)
	}

	log.Printf("📦 TCP BATCH RECEIVED: %s seq=%d, %d docs (%s)", stream, batchSeq, len(documents), formatBytes(totalBytes))

	// Parse stream name to extract database and collection
	// In the new approach, stream names are in format "database.collection"
	parts := strings.Split(stream, ".")
	if len(parts) < 2 {
		log.Printf("🔴 TCP BATCH INVALID STREAM: stream=%s (expected format: database.collection)", stream)
		return fmt.Errorf("invalid stream name format: %s", stream)
	}

	sourceDatabase := parts[0]
	sourceCollection := parts[1]

	// In the new approach, we use the same database and collection names as the source
	targetDatabase := sourceDatabase
	targetCollection := sourceCollection

	log.Printf("🔄 TCP MAPPING: %s.%s -> %s.%s", sourceDatabase, sourceCollection, targetDatabase, targetCollection)

	// Check if this is a metadata stream
	if len(parts) > 2 && parts[2] == "metadata" {
		return handleTCPMetadata(targetDatabase, targetCollection, documents)
	}

	// Check if this is an incremental stream
	isIncremental := len(parts) > 2 && parts[2] == "incremental"

	// OPTIMIZED: Convert BSON documents with streaming to prevent memory spikes
	bsonDocuments := make([]bson.Raw, 0, len(documents))
	for i, docBytes := range documents {
		// Process in chunks to manage memory for billion-document scenarios
		if i > 0 && i%1000 == 0 {
			// Brief pause every 1000 documents to prevent memory pressure
			runtime.GC() // Force garbage collection to manage memory
		}
		bsonDocuments = append(bsonDocuments, bson.Raw(docBytes))
	}

	// Create a PageResult structure for compatibility with existing logic
	pageResult := &PageResult{
		PageNumber: int(batchSeq),
		Documents:  bsonDocuments,
		Error:      nil,
		IsLastPage: false, // Will be determined by the sender
	}

	// OPTIMIZED: Process with batching for massive datasets
	var processingError error
	if isIncremental {
		// Process incremental data with optimized batching
		processingError = processIncrementalPageOptimized(targetDatabase, targetCollection, pageResult)
	} else {
		// Process initial bulk data with optimized batching
		processingError = processPageOptimized(targetDatabase, targetCollection, pageResult)
	}

	processingTime := time.Since(startTime)
	throughputMBps := float64(totalBytes) / processingTime.Seconds() / (1024 * 1024)

	if processingError != nil {
		log.Printf("🔴 TCP BATCH ERROR: %s seq=%d failed in %v: %v", stream, batchSeq, processingTime, processingError)
		log.Printf("🔴 TCP BATCH FAILED DETAILS: stream=%s db=%s coll=%s docs=%d bytes=%d",
			stream, targetDatabase, targetCollection, len(documents), totalBytes)
		return processingError
	}

	log.Printf("✅ TCP BATCH SUCCESS: %s seq=%d, %d docs processed in %v (%.2f MB/s)",
		stream, batchSeq, len(documents), processingTime, throughputMBps)

	return nil
}

// processPageOptimized processes a page with billion-document optimizations
func processPageOptimized(database, collection string, pageResult *PageResult) error {
	if len(pageResult.Documents) == 0 {
		return nil
	}

	// Use context with timeout for massive operations
	// Increased to 10 minutes to handle large batches in production
	ctx, cancel := context.WithTimeout(context.Background(), 600*time.Second)
	defer cancel()

	// Get target collection with majority write concern for durability
	wcMajority := writeconcern.New(writeconcern.WMajority(), writeconcern.WTimeout(30*time.Second))
	targetColl := mongoClient.Database(database).Collection(collection, options.Collection().SetWriteConcern(wcMajority))

	// OPTIMIZED: Process in smaller batches to prevent memory exhaustion
	const batchSize = 1000 // Process 1K documents at a time
	documents := pageResult.Documents
	totalProcessed := 0

	for i := 0; i < len(documents); i += batchSize {
		end := i + batchSize
		if end > len(documents) {
			end = len(documents)
		}

		batch := documents[i:end]
		batchNumber := (i / batchSize) + 1
		totalBatches := (len(documents) + batchSize - 1) / batchSize

		// Convert to interface{} slice for MongoDB insertion
		interfaceDocs := make([]interface{}, len(batch))
		for j, doc := range batch {
			interfaceDocs[j] = doc
		}

		// Insert batch with retries and unordered writes for fault tolerance
		var insertErr error
		var insertResult *mongo.InsertManyResult

		// Use unordered writes to continue inserting even if some docs fail
		insertOpts := options.InsertMany().SetOrdered(false)

		for retry := 0; retry < 3; retry++ {
			insertResult, insertErr = targetColl.InsertMany(ctx, interfaceDocs, insertOpts)

			if insertErr == nil {
				// All documents inserted successfully
				break
			}

			// Check if this is a bulk write exception (partial success)
			if bulkErr, ok := insertErr.(mongo.BulkWriteException); ok {
				insertedCount := len(bulkErr.WriteErrors)
				if insertResult != nil {
					insertedCount = len(insertResult.InsertedIDs)
				}

				// Log partial success details
				log.Printf("⚠️  PARTIAL SUCCESS: Inserted %d/%d docs in batch %d/%d. Errors: %d",
					insertedCount, len(batch), batchNumber, totalBatches, len(bulkErr.WriteErrors))

				// Log first few errors for diagnosis
				for i, writeErr := range bulkErr.WriteErrors {
					if i < 3 { // Log first 3 errors
						log.Printf("  Error %d: Index %d - %v", i+1, writeErr.Index, writeErr.WriteError)
					}
				}

				// Check if most documents were inserted (>90% success rate)
				if insertedCount >= int(float64(len(batch))*0.9) {
					log.Printf("✅ ACCEPTABLE PARTIAL SUCCESS: %d/%d docs inserted (%.1f%%)",
						insertedCount, len(batch), float64(insertedCount)/float64(len(batch))*100)
					break // Consider it success if 90%+ inserted
				}
			}

			log.Printf("⚠️  Retry %d/%d for batch %d/%d: %v", retry+1, 3, batchNumber, totalBatches, insertErr)
			time.Sleep(time.Duration(retry+1) * time.Second)
		}

		if insertErr != nil {
			// Final error - log detailed information
			if insertResult != nil && len(insertResult.InsertedIDs) > 0 {
				log.Printf("⚠️  CRITICAL: Batch %d/%d failed but %d docs were inserted before error",
					batchNumber, totalBatches, len(insertResult.InsertedIDs))
			}
			return fmt.Errorf("failed to insert batch %d/%d after retries (inserted %d/%d docs): %w",
				batchNumber, totalBatches, len(insertResult.InsertedIDs), len(batch), insertErr)
		}

		// Verify insert count matches expected
		if insertResult != nil && len(insertResult.InsertedIDs) != len(batch) {
			log.Printf("⚠️  INSERT MISMATCH: Expected %d insertions, got %d for batch %d/%d",
				len(batch), len(insertResult.InsertedIDs), batchNumber, totalBatches)
		}

		totalProcessed += len(batch)
		log.Printf("📦 BATCH %d/%d: %d docs inserted (%d/%d total)",
			batchNumber, totalBatches, len(batch), totalProcessed, len(documents))

		// Brief pause between batches to prevent overwhelming MongoDB
		if batchNumber < totalBatches {
			time.Sleep(10 * time.Millisecond)
		}

		// Force garbage collection every 10 batches to manage memory
		if batchNumber%10 == 0 {
			runtime.GC()
		}
	}

	return nil
}

// processIncrementalPageOptimized processes incremental data with enhanced operation type handling
func processIncrementalPageOptimized(database, collection string, pageResult *PageResult) error {
	// Convert PageResult to DataResponse format for consistent processing
	dataResponse := &models.DataResponse{
		Database:   database,
		Collection: collection,
		Documents:  pageResult.Documents,
		Count:      int64(len(pageResult.Documents)),
	}

	// Use the same processing logic as HTTP incremental sync
	return processIncrementalData(database, collection, dataResponse)
}

// parseStreamName parses a stream name to extract database and collection
// For TCP transport, stream names are in format "stream_<numeric_id>"
// We need to map these back to the actual source collections
func parseStreamName(stream string) []string {
	// Handle TCP stream format: "stream_1234567890"
	if strings.HasPrefix(stream, "stream_") {
		// For TCP streams, we need to use the stream index to map to configured collections
		// Since we can't reverse the hash, we'll use the order of processing
		// This is a limitation that should be fixed in the TCP protocol
		log.Printf("⚠️  TCP STREAM FORMAT: %s - using collection mapping fallback", stream)
		// Return empty to trigger the collection mapping logic
		return []string{}
	}

	// Handle direct format: "database.collection"
	return strings.Split(stream, ".")
}

// getMapKeys returns the keys of a map as a slice
func getMapKeys(m map[string]interface{}) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	return keys
}

// handleTCPMetadata processes metadata received via TCP
func handleTCPMetadata(database, collection string, documents [][]byte) error {
	if len(documents) != 1 {
		return fmt.Errorf("expected exactly one metadata document, got %d", len(documents))
	}

	// Parse metadata
	var metadata map[string]interface{}
	if err := bson.Unmarshal(documents[0], &metadata); err != nil {
		return fmt.Errorf("failed to unmarshal metadata: %v", err)
	}

	log.Printf("📊 METADATA PROCESSING: %s.%s metadata (%d bytes)", database, collection, len(documents[0]))
	log.Printf("🔍 VM METADATA: Keys in metadata: %v", getMapKeys(metadata))

	// In the new approach without mapping, use the same database and collection names as the source
	targetDatabase := database
	targetCollection := collection

	// Apply indexes if provided in metadata
	if indexesRaw, exists := metadata["indexes"]; exists && indexesRaw != nil {
		log.Printf("🔍 VM INDEXES: Found indexes field, type=%T", indexesRaw)

		var indexes bson.A
		if arr, ok := indexesRaw.(bson.A); ok {
			indexes = arr
			log.Printf("🔍 VM INDEXES: Successfully cast to bson.A, length=%d", len(indexes))
		} else if arr, ok := indexesRaw.([]interface{}); ok {
			indexes = arr
			log.Printf("🔍 VM INDEXES: Successfully cast to []interface{}, length=%d", len(indexes))
		} else {
			log.Printf("❌ VM INDEXES: Unexpected type %T, cannot process", indexesRaw)
			return fmt.Errorf("indexes field has unexpected type: %T", indexesRaw)
		}

		// Convert bson.A to []interface{} for index creation
		indexModels := make([]mongo.IndexModel, 0, len(indexes))
		for i, idx := range indexes {
			log.Printf("🔍 VM INDEX %d: type=%T, value=%+v", i, idx, idx)

			// Handle both bson.D and map[string]interface{} types
			var idxMap map[string]interface{}

			if idxDoc, ok := idx.(bson.D); ok {
				log.Printf("🔍 VM INDEX %d: Successfully cast to bson.D", i)
				// Convert bson.D to map[string]interface{}
				idxMap = make(map[string]interface{})
				for _, elem := range idxDoc {
					idxMap[elem.Key] = elem.Value
				}
			} else if m, ok := idx.(map[string]interface{}); ok {
				log.Printf("🔍 VM INDEX %d: Successfully cast to map[string]interface{}", i)
				idxMap = m
			} else {
				log.Printf("❌ VM INDEX %d: Unexpected type %T, skipping", i, idx)
				continue
			}

			// Now process the index map
			keys := bson.D{}
			options := options.Index()
			indexName := ""

			// Extract keys
			if keysRaw, ok := idxMap["keys"]; ok {
				if keysMap, ok := keysRaw.(map[string]interface{}); ok {
					// Convert map to bson.D
					for k, v := range keysMap {
						if intVal, ok := v.(int); ok {
							keys = append(keys, bson.E{Key: k, Value: intVal})
						} else if int32Val, ok := v.(int32); ok {
							keys = append(keys, bson.E{Key: k, Value: int32Val})
						} else if int64Val, ok := v.(int64); ok {
							keys = append(keys, bson.E{Key: k, Value: int64Val})
						} else {
							keys = append(keys, bson.E{Key: k, Value: v})
						}
					}
					log.Printf("🔍 VM INDEX %d: Found keys: %v", i, keys)
				} else if keysDoc, ok := keysRaw.(bson.D); ok {
					keys = keysDoc
					log.Printf("🔍 VM INDEX %d: Found keys (bson.D): %v", i, keys)
				} else {
					log.Printf("❌ VM INDEX %d: keys field has unexpected type: %T", i, keysRaw)
				}
			}

			// Extract name
			if nameRaw, ok := idxMap["name"]; ok {
				if name, ok := nameRaw.(string); ok {
					options.SetName(name)
					indexName = name
					log.Printf("🔍 VM INDEX %d: Found name: %s", i, name)
				}
			}

			// Extract unique
			if uniqueRaw, ok := idxMap["unique"]; ok {
				if unique, ok := uniqueRaw.(bool); ok {
					options.SetUnique(unique)
					log.Printf("🔍 VM INDEX %d: Found unique: %v", i, unique)
				}
			}

			if sparseRaw, ok := idxMap["sparse"]; ok {
				if sparse, ok := sparseRaw.(bool); ok {
					options.SetSparse(sparse)
					log.Printf("🔍 VM INDEX %d: Found sparse: %v", i, sparse)
				}
			}

			// Extract TTL (expireAfterSeconds)
			if ttlRaw, ok := idxMap["expireAfterSeconds"]; ok {
				if ttl, ok := ttlRaw.(int32); ok {
					options.SetExpireAfterSeconds(ttl)
					log.Printf("🔍 VM INDEX %d: Found TTL: %d seconds", i, ttl)
				} else if ttl64, ok := ttlRaw.(int64); ok {
					options.SetExpireAfterSeconds(int32(ttl64))
					log.Printf("🔍 VM INDEX %d: Found TTL: %d seconds", i, ttl64)
				}
			}

			// Extract partial filter expression (CRITICAL FIX)
			if partialFilterRaw, ok := idxMap["partialFilterExpression"]; ok {
				if partialFilterMap, ok := partialFilterRaw.(map[string]interface{}); ok {
					options.SetPartialFilterExpression(partialFilterMap)
					log.Printf("🔍 VM INDEX %d: Found partialFilterExpression: %v", i, partialFilterMap)
				} else if partialFilterBson, ok := partialFilterRaw.(bson.M); ok {
					options.SetPartialFilterExpression(partialFilterBson)
					log.Printf("🔍 VM INDEX %d: Found partialFilterExpression (bson.M): %v", i, partialFilterBson)
				} else if partialFilterDoc, ok := partialFilterRaw.(bson.D); ok {
					// Convert bson.D to bson.M for SetPartialFilterExpression
					partialFilterM := bson.M{}
					for _, elem := range partialFilterDoc {
						partialFilterM[elem.Key] = elem.Value
					}
					options.SetPartialFilterExpression(partialFilterM)
					log.Printf("🔍 VM INDEX %d: Found partialFilterExpression (bson.D): %v", i, partialFilterM)
				} else {
					log.Printf("⚠️ VM INDEX %d: partialFilterExpression has unexpected type: %T", i, partialFilterRaw)
				}
			}

			// Extract collation
			// Note: Collation handling is complex due to type conversions
			// For now, skip collation in TCP metadata flow (works fine in HTTP flow via recreateIndexes)
			if collationRaw, ok := idxMap["collation"]; ok {
				log.Printf("⚠️ VM INDEX %d: Collation found but skipped in TCP flow (type: %T)", i, collationRaw)
				// TODO: Implement collation extraction if needed
			}

			// Skip default _id_ index
			if indexName == "_id_" {
				log.Printf("⏭️ VM INDEX %d: Skipping default _id_ index", i)
				continue
			}

			if len(keys) > 0 {
				indexModels = append(indexModels, mongo.IndexModel{
					Keys:    keys,
					Options: options,
				})
				log.Printf("✅ VM INDEX %d: Created index model for '%s'", i, indexName)
			} else {
				log.Printf("❌ VM INDEX %d: No keys found, skipping index '%s'", i, indexName)
			}
		}

		if len(indexModels) > 0 {
			coll := mongoClient.Database(targetDatabase).Collection(targetCollection)
			ctx := context.Background()
			if _, err := coll.Indexes().CreateMany(ctx, indexModels); err != nil {
				// Check if it's an index conflict error
				if mongo.IsDuplicateKeyError(err) || strings.Contains(err.Error(), "IndexKeySpecsConflict") {
					log.Printf("⚠️ INDEX CONFLICT: Attempting to drop and recreate indexes for %s.%s", targetDatabase, targetCollection)

					// Drop conflicting indexes and retry
					for _, model := range indexModels {
						if model.Options != nil && model.Options.Name != nil {
							indexName := *model.Options.Name
							log.Printf("🗑️ Dropping index: %s", indexName)
							if _, dropErr := coll.Indexes().DropOne(ctx, indexName); dropErr != nil {
								if !strings.Contains(dropErr.Error(), "index not found") {
									log.Printf("⚠️ Failed to drop index %s: %v", indexName, dropErr)
								}
							}
						}
					}

					// Retry creating indexes
					log.Printf("🔄 Retrying index creation for %s.%s...", targetDatabase, targetCollection)
					if _, retryErr := coll.Indexes().CreateMany(ctx, indexModels); retryErr != nil {
						log.Printf("⚠️ Index creation still failed: %v - continuing anyway", retryErr)
						// Don't return error - let data sync continue
					} else {
						log.Printf("✅ VM INDEXES: Successfully created %d indexes after resolving conflicts", len(indexModels))
					}
				} else {
					log.Printf("⚠️ Index creation failed: %v - continuing anyway", err)
					// Don't return error - let data sync continue
				}
			} else {
				log.Printf("✅ VM INDEXES: Created %d indexes", len(indexModels))
			}
		} else {
			log.Printf("⚠️ VM INDEXES: No index models to create for %s.%s", targetDatabase, targetCollection)
		}
	} else {
		log.Printf("⚠️ VM METADATA: No indexes field found in metadata for %s.%s", targetDatabase, targetCollection)
	}

	// Process snapshot fence info
	if snapshotFenceData, ok := metadata["snapshotFence"]; ok && snapshotFenceData != nil {
		log.Printf("🔒 SNAPSHOT FENCE: Processing for %s.%s", targetDatabase, targetCollection)

		if fenceBytes, err := bson.Marshal(snapshotFenceData); err == nil {
			var snapshotFence models.SnapshotFenceInfo
			if err := bson.Unmarshal(fenceBytes, &snapshotFence); err == nil {
				// Store snapshot fence information for change stream coordination
				log.Printf("✅ METADATA: Snapshot fence processed - ClusterTime: %v, OperationTime: %v",
					snapshotFence.ClusterTime, snapshotFence.OperationTime)

				// Validate that change streams can start consistently with this fence
				if clusterFence != nil && clusterFence.IsEnabled() {
					if err := clusterFence.ValidateChangeStreamStart(convertToSnapshotFence(&snapshotFence), snapshotFence.OperationTime); err != nil {
						log.Printf("⚠️ METADATA: Change stream coordination validation failed: %v", err)
					} else {
						log.Printf("✅ METADATA: Change stream coordination validated for %s.%s", targetDatabase, targetCollection)
					}
				}
			} else {
				log.Printf("⚠️ METADATA: Failed to unmarshal snapshot fence: %v", err)
			}
		}
	}

	log.Printf("🎉 METADATA: Successfully processed all metadata for %s.%s", targetDatabase, targetCollection)
	return nil
}

// formatBytes formats byte count as human readable string
func formatBytes(bytes int) string {
	if bytes < 1024 {
		return fmt.Sprintf("%d B", bytes)
	} else if bytes < 1024*1024 {
		return fmt.Sprintf("%.1f KB", float64(bytes)/1024)
	} else {
		return fmt.Sprintf("%.2f MB", float64(bytes)/(1024*1024))
	}
}

func main() {
	configFile := flag.String("config", "config.yaml", "Path to configuration file")
	flag.Parse()

	// Load configuration
	if err := loadConfig(*configFile); err != nil {
		log.Fatalf("Failed to load config: %v", err)
	}

	// Initialize HTTP client with configurable timeout
	httpTimeout := config.CloudSync.HTTPTimeout
	if httpTimeout == 0 {
		httpTimeout = 300 * time.Second // Default to 5 minutes for large data transfers
	}
	httpClient = &http.Client{Timeout: httpTimeout}
	log.Printf("HTTP client initialized with timeout: %v", httpTimeout)

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

	// Generate unique client ID for tracking (with hostname and PID for multi-instance support)
	hostname, _ := os.Hostname()
	pid := os.Getpid()
	clientID = fmt.Sprintf("vm-sync-%s-%d-%d", hostname, pid, time.Now().Unix())
	log.Printf("Client ID: %s", clientID)

	// Initialize OAuth2 token manager (required authentication)
	if !config.CloudSync.OAuth2.Enabled {
		log.Fatalf("OAuth2 authentication is required but disabled in configuration")
	}

	vmTokenManager = auth.NewVMTokenManager(mongoClient, "vm_oauth2_auth", config.CloudSync.HTTPURL)

	// Load or store credentials based on configuration
	ctx = context.Background()
	if config.CloudSync.OAuth2.ClientID != "" && config.CloudSync.OAuth2.ClientSecret != "" {
		// Store credentials from configuration
		if err := vmTokenManager.StoreCredentials(ctx, clientID, config.CloudSync.OAuth2.ClientID, config.CloudSync.OAuth2.ClientSecret); err != nil {
			log.Fatalf("Failed to store OAuth2 credentials: %v", err)
		}
		log.Println("OAuth2 credentials stored successfully")
	} else {
		// Try to load existing credentials
		if err := vmTokenManager.LoadCredentials(ctx, clientID); err != nil {
			log.Fatalf("Failed to load OAuth2 credentials: %v. Either configure client_id/client_secret or register credentials via admin API", err)
		}
		log.Println("OAuth2 credentials loaded successfully")
	}

	log.Println("OAuth2 token manager initialized successfully")
	// Start automatic token refresh
	vmTokenManager.StartAutoRefresh(context.Background())

	// Initialize adaptive system components
	telemetryCollector, err = telemetry.NewCollector(clientID)
	if err != nil {
		log.Fatalf("Failed to initialize telemetry collector: %v", err)
	}
	log.Println("Telemetry collector initialized")

	// VM sync integration will be initialized after WebSocket connection is established
	log.Println("Adaptive system components initialized")

	// Initialize TCP transport if enabled
	if err := initializeTCPTransport(); err != nil {
		log.Printf("WARNING: Failed to initialize TCP transport: %v", err)
		log.Printf("DEGRADED MODE: Using HTTP transport for data transfer")
		tcpTransportEnabled = false
	} else if tcpTransportEnabled {
		log.Println("TCP transport receiver initialized successfully")
	}

	// Start HTTP server to receive data from cloud-sync
	log.Println("Starting HTTP server for push-based synchronization...")
	go startHTTPServer()

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

	// Gracefully shutdown HTTP server
	if httpServer != nil {
		log.Println("Shutting down HTTP server...")
		if err := httpServer.Shutdown(ctx); err != nil {
			log.Printf("Error shutting down HTTP server: %v", err)
		} else {
			log.Println("HTTP server stopped gracefully")
		}
	}

	// Shutdown TCP transport if enabled
	if tcpTransportEnabled && tcpReceiver != nil {
		log.Println("Shutting down TCP transport...")
		if err := tcpReceiver.Stop(); err != nil {
			log.Printf("Error stopping TCP receiver: %v", err)
		} else {
			log.Println("TCP transport stopped gracefully")
		}
	}

	// Stop oplog monitor
	if oplogMonitor != nil {
		oplogMonitor.Stop()
		log.Println("Oplog monitor stopped")
	}

	// Close WebSocket connection if exists
	websocketConnMutex.RLock()
	if websocketConn != nil {
		log.Println("🔌 Closing WebSocket connection...")
		// Send close message to cloud-sync
		closeMsg := websocket.FormatCloseMessage(websocket.CloseNormalClosure, "VM-sync shutting down")
		websocketConn.WriteControl(websocket.CloseMessage, closeMsg, time.Now().Add(5*time.Second))
		websocketConn.Close()
		log.Println("✅ WebSocket connection closed gracefully")
	}
	websocketConnMutex.RUnlock()

	if vmSyncIntegration != nil {
		if transmitter := vmSyncIntegration.GetTransmitter(); transmitter != nil {
			transmitter.MarkDisconnected()
		}
		log.Println("VM sync integration stopped")
	}

	if err := mongoClient.Disconnect(ctx); err != nil {
		log.Printf("Error disconnecting from MongoDB: %v", err)
	}

	log.Println("Client exited")
}

func startHTTPServer() {
	router := mux.NewRouter()

	// Push endpoint for receiving data from cloud-sync
	router.HandleFunc("/api/v1/push/{database}/{collection}", handlePushData).Methods("POST")

	// Checkpoint check endpoint
	router.HandleFunc("/api/v1/checkpoint/{collection}", handleCheckpointCheck).Methods("GET")

	// Clear collection endpoint
	router.HandleFunc("/api/v1/clear/{collection}", handleClearCollection).Methods("DELETE")

	// Health check endpoint
	router.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("OK"))
	}).Methods("GET")

	// Get port from config, environment variable, or use default
	port := config.Server.Port
	if port == 0 {
		// Fallback to environment variable if config not set
		if envPort := os.Getenv("VM_SYNC_PORT"); envPort != "" {
			if parsedPort, err := strconv.Atoi(envPort); err == nil {
				port = parsedPort
			} else {
				log.Printf("WARNING: Invalid VM_SYNC_PORT '%s', using default 8081", envPort)
				port = 8081
			}
		} else {
			port = 8081 // Default port
		}
	}

	// Get host from config or use default
	host := config.Server.Host
	if host == "" {
		host = "0.0.0.0" // Default to listen on all interfaces
	}

	// Create HTTP server instance for graceful shutdown with configured timeouts
	httpServer = &http.Server{
		Addr:         fmt.Sprintf("%s:%d", host, port),
		Handler:      router,
		ReadTimeout:  config.Server.ReadTimeout,
		WriteTimeout: config.Server.WriteTimeout,
		IdleTimeout:  config.Server.IdleTimeout,
	}

	// Apply default timeouts if not configured
	if httpServer.ReadTimeout == 0 {
		httpServer.ReadTimeout = 15 * time.Second
	}
	if httpServer.WriteTimeout == 0 {
		httpServer.WriteTimeout = 15 * time.Second
	}
	if httpServer.IdleTimeout == 0 {
		httpServer.IdleTimeout = 60 * time.Second
	}

	log.Printf("🚀 VM-SYNC HTTP SERVER: Starting on %s:%d", host, port)
	log.Printf("📋 HTTP CONFIG: ReadTimeout=%v, WriteTimeout=%v, IdleTimeout=%v",
		httpServer.ReadTimeout, httpServer.WriteTimeout, httpServer.IdleTimeout)
	log.Printf("🔗 ENDPOINTS: Push=/api/v1/push/{db}/{coll}, Health=/health")

	if err := httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		log.Fatalf("HTTP server failed: %v", err)
	}
}

func handlePushData(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	database := vars["database"]
	collection := vars["collection"]

	log.Printf("Received push data for %s.%s", database, collection)

	// Read the request body
	body, err := io.ReadAll(r.Body)
	if err != nil {
		log.Printf("Error reading request body: %v", err)
		http.Error(w, "Failed to read request body", http.StatusBadRequest)
		return
	}
	defer r.Body.Close()

	// Check if this is incremental sync (from change streams)
	syncType := r.Header.Get("X-Sync-Type")
	if syncType == "incremental" {
		// Handle incremental sync data (DataResponse format)
		var dataResponse models.DataResponse
		if err := json.Unmarshal(body, &dataResponse); err != nil {
			log.Printf("Error parsing incremental data response: %v", err)
			http.Error(w, "Failed to parse incremental data response", http.StatusBadRequest)
			return
		}
		log.Printf("🚀 RECEIVED INCREMENTAL DATA: %s.%s with %d documents", database, collection, len(dataResponse.Documents))

		// Process incremental data directly
		if err := processIncrementalData(database, collection, &dataResponse); err != nil {
			log.Printf("Error processing incremental data: %v", err)
			http.Error(w, "Failed to process incremental data", http.StatusInternalServerError)
			return
		}
	} else {
		// Handle initial sync data (PageResult format)
		var pageResult PageResult
		contentType := r.Header.Get("Content-Type")
		if contentType == "application/bson" {
			// BSON format (preserves MongoDB types)
			if err := bson.Unmarshal(body, &pageResult); err != nil {
				log.Printf("Error parsing BSON page result: %v", err)
				http.Error(w, "Failed to parse BSON page result", http.StatusBadRequest)
				return
			}
			log.Printf("📦 RECEIVED BSON DATA: %s.%s page %d with %d documents", database, collection, pageResult.PageNumber, len(pageResult.Documents))
		} else {
			// Legacy JSON format (for backward compatibility)
			if err := json.Unmarshal(body, &pageResult); err != nil {
				log.Printf("Error parsing JSON page result: %v", err)
				http.Error(w, "Failed to parse JSON page result", http.StatusBadRequest)
				return
			}
			log.Printf("📦 RECEIVED JSON DATA: %s.%s page %d with %d documents", database, collection, pageResult.PageNumber, len(pageResult.Documents))
		}

		// Process initial sync data
		if err := processPushedData(database, collection, &pageResult); err != nil {
			log.Printf("Error processing pushed data: %v", err)
			http.Error(w, "Failed to process data", http.StatusInternalServerError)
			return
		}
	}

	w.WriteHeader(http.StatusOK)
	w.Write([]byte("Data processed successfully"))
}

// handleCheckpointCheck checks if a checkpoint exists for the given collection
func handleCheckpointCheck(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	collectionKey := vars["collection"]

	parts := splitDatabaseCollection(collectionKey)
	if len(parts) != 2 {
		http.Error(w, "Invalid collection format, expected database.collection", http.StatusBadRequest)
		return
	}

	database, collection := parts[0], parts[1]

	if checkpointMgr == nil {
		http.Error(w, "Checkpoint manager not initialized", http.StatusInternalServerError)
		return
	}

	checkpoint := checkpointMgr.GetCheckpoint(database, collection)
	if checkpoint == nil {
		w.WriteHeader(http.StatusNotFound)
		w.Write([]byte("No checkpoint found"))
		return
	}

	w.WriteHeader(http.StatusOK)
	w.Write([]byte("Checkpoint exists"))
}

// handleClearCollection clears the specified collection and its checkpoint
func handleClearCollection(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	collectionKey := vars["collection"]

	parts := splitDatabaseCollection(collectionKey)
	if len(parts) != 2 {
		http.Error(w, "Invalid collection format, expected database.collection", http.StatusBadRequest)
		return
	}

	database, collection := parts[0], parts[1]

	// Clear the collection
	coll := mongoClient.Database(database).Collection(collection)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if err := coll.Drop(ctx); err != nil {
		log.Printf("Warning: Failed to drop collection %s.%s: %v", database, collection, err)
		// Don't return error here, continue to clear checkpoint
	}

	// Clear the checkpoint
	if checkpointMgr != nil {
		if err := checkpointMgr.DeleteCheckpoint(database, collection); err != nil {
			log.Printf("Warning: Failed to clear checkpoint for %s.%s: %v", database, collection, err)
		}
	}

	log.Printf("Cleared collection and checkpoint for %s.%s", database, collection)
	w.WriteHeader(http.StatusOK)
	w.Write([]byte("Collection cleared"))
}

// processIncrementalData processes incremental changes from change streams
func processIncrementalData(database, collection string, dataResponse *models.DataResponse) error {
	if len(dataResponse.Documents) == 0 {
		log.Printf("📋 INCREMENTAL SYNC: No documents to process for %s.%s", database, collection)
		return nil
	}

	// In the new approach without mapping, use the same database and collection names as the source
	targetDatabase := database
	targetCollection := collection

	log.Printf("🔄 INCREMENTAL MAPPING: %s.%s -> %s.%s", database, collection, targetDatabase, targetCollection)

	// Separate documents by operation type
	var regularDocs []bson.Raw
	var deleteOperations []bson.M

	for _, doc := range dataResponse.Documents {
		var docMap bson.M
		if err := bson.Unmarshal(doc, &docMap); err != nil {
			log.Printf("⚠️  Failed to unmarshal document: %v", err)
			continue
		}

		// Check if this is a delete operation marker
		if operation, ok := docMap["_operation"].(string); ok {
			switch operation {
			case "delete":
				// Extract document key for deletion
				if documentKey, ok := docMap["_documentKey"]; ok {
					if docKeyMap, ok := documentKey.(bson.M); ok {
						deleteOperations = append(deleteOperations, docKeyMap)
						log.Printf("🗑️  DELETE MARKER: Found delete operation for %s.%s", targetDatabase, targetCollection)
					} else {
						log.Printf("⚠️  Invalid document key format in delete marker")
					}
				} else {
					log.Printf("⚠️  Delete marker missing document key")
				}
			case "drop":
				log.Printf("🗑️  DROP OPERATION: Collection %s.%s will be dropped", targetDatabase, targetCollection)
				// Handle collection drop - this will drop the entire collection
				if err := handleDropCollection(targetDatabase, targetCollection); err != nil {
					log.Printf("⚠️  Failed to handle drop operation: %v", err)
				}
			case "rename":
				log.Printf("🔄 RENAME OPERATION: Collection %s.%s will be renamed", targetDatabase, targetCollection)
				// Handle collection rename - requires special handling
				if err := handleRenameCollection(docMap, targetDatabase, targetCollection); err != nil {
					log.Printf("⚠️  Failed to handle rename operation: %v", err)
				}
			case "dropDatabase":
				log.Printf("🗑️  DROP DATABASE OPERATION: Database %s will be dropped", targetDatabase)
				// Handle database drop - this is a critical operation
				if err := handleDropDatabase(targetDatabase); err != nil {
					log.Printf("⚠️  Failed to handle drop database operation: %v", err)
				}
			case "createIndexes", "dropIndexes", "modify":
				log.Printf("🔍 INDEX OPERATION: %s for %s.%s", operation, targetDatabase, targetCollection)
				// Handle index operations - these are metadata changes
				if err := handleIndexOperation(operation, docMap, targetDatabase, targetCollection); err != nil {
					log.Printf("⚠️  Failed to handle index operation: %v", err)
				}
			case "invalidate":
				log.Printf("⚠️  INVALIDATE OPERATION: %s.%s invalidated", targetDatabase, targetCollection)
				// Handle invalidate - usually triggers a full resync
				if err := handleInvalidateEvent(targetDatabase, targetCollection, "incremental_invalidate"); err != nil {
					log.Printf("⚠️  Failed to handle invalidate operation: %v", err)
				}
			default:
				log.Printf("⚠️  UNKNOWN OPERATION: %s for %s.%s - treating as regular document", operation, targetDatabase, targetCollection)
				// Treat unknown operations as regular documents
				regularDocs = append(regularDocs, doc)
			}
		} else {
			// Regular insert/update/replace operation
			regularDocs = append(regularDocs, doc)
		}
	}

	// Process regular documents (insert/update/replace) with upserts
	if len(regularDocs) > 0 {
		if err := insertDocumentsBatch(targetDatabase, targetCollection, regularDocs); err != nil {
			return fmt.Errorf("failed to process regular documents: %v", err)
		}
		log.Printf("✅ INCREMENTAL UPSERT: Successfully processed %d regular documents for %s.%s", len(regularDocs), targetDatabase, targetCollection)
	}

	// Process delete operations
	if len(deleteOperations) > 0 {
		if err := deleteDocumentsBatch(targetDatabase, targetCollection, deleteOperations); err != nil {
			return fmt.Errorf("failed to process delete operations: %v", err)
		}
		log.Printf("✅ INCREMENTAL DELETE: Successfully deleted %d documents from %s.%s", len(deleteOperations), targetDatabase, targetCollection)
	}

	log.Printf("✅ INCREMENTAL SYNC: Successfully processed %d total operations (%d upserts, %d deletes) for %s.%s",
		len(dataResponse.Documents), len(regularDocs), len(deleteOperations), targetDatabase, targetCollection)
	return nil
}

func processPushedData(database, collection string, pageResult *PageResult) error {
	if pageResult.Error != nil {
		return fmt.Errorf("page result contains error: %v", pageResult.Error)
	}

	// In the new approach without mapping, use the same database and collection names as the source
	targetDatabase := database
	targetCollection := collection

	log.Printf("🔄 HTTP MAPPING: %s.%s -> %s.%s", database, collection, targetDatabase, targetCollection)

	// Clear collection on first page (unless resumable sync is enabled and we're resuming)
	if pageResult.PageNumber == 1 {
		shouldDropCollection := true

		// Check if resumable initial sync is enabled
		if config.Sync.ResumableInitialSync {
			// Check if we have existing checkpoint data indicating a previous sync
			if checkpointMgr != nil {
				if checkpoint := checkpointMgr.GetCheckpoint(targetDatabase, targetCollection); checkpoint != nil {
					log.Printf("Found existing checkpoint for %s.%s, skipping collection drop for resumable sync", targetDatabase, targetCollection)
					shouldDropCollection = false
				}
			}
		}

		if shouldDropCollection {
			coll := mongoClient.Database(targetDatabase).Collection(targetCollection)
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()

			if err := coll.Drop(ctx); err != nil {
				log.Printf("Warning: Failed to drop collection %s.%s: %v", targetDatabase, targetCollection, err)
			}
			log.Printf("Cleared collection %s.%s for fresh sync", targetDatabase, targetCollection)
		} else {
			log.Printf("Resuming sync for %s.%s, keeping existing data", targetDatabase, targetCollection)
		}
	}

	// Insert documents if any
	if len(pageResult.Documents) > 0 {
		if err := insertDocumentsBatch(targetDatabase, targetCollection, pageResult.Documents); err != nil {
			return fmt.Errorf("failed to insert documents: %v", err)
		}
		log.Printf("Inserted %d documents for %s.%s (page %d)",
			len(pageResult.Documents), targetDatabase, targetCollection, pageResult.PageNumber)
	}

	// Handle final processing on last page
	if pageResult.IsLastPage {
		if err := handleFinalProcessing(targetDatabase, targetCollection, pageResult); err != nil {
			return fmt.Errorf("failed to handle final processing: %v", err)
		}
		log.Printf("Completed push-based sync for %s.%s", targetDatabase, targetCollection)
	}

	return nil
}

func loadConfig(filename string) error {
	data, err := os.ReadFile(filename)
	if err != nil {
		return err
	}
	expandedData := os.ExpandEnv(string(data))

	err = yaml.Unmarshal([]byte(expandedData), &config)
	if err != nil {
		return err
	}

	// Initialize global collections for stream mapping
	configCollections = config.Collections
	log.Printf("📋 CONFIG: Loaded %d collections for stream mapping", len(configCollections))

	return nil
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

// initWorkerPool initializes the worker pool with configuration
func initWorkerPool() {
	maxWorkers := config.Sync.MaxWorkers
	if maxWorkers <= 0 {
		maxWorkers = 4 // Default to 4 workers
	}

	// Calculate memory limit (default 512MB)
	maxMemoryMB := 512
	// Use batch size to estimate memory needs
	if config.Sync.BatchSize > 0 {
		// Estimate memory based on batch size (assume ~1KB per document)
		estimatedMB := (config.Sync.BatchSize * maxWorkers * 1024) / (1024 * 1024)
		if estimatedMB > 128 && estimatedMB < 2048 {
			maxMemoryMB = estimatedMB
		}
	}

	workerPool = &WorkerPool{
		workerCount:   maxWorkers,
		maxMemoryMB:   maxMemoryMB,
		currentMemory: 0,
	}

	log.Printf("Initialized worker pool: %d workers, %dMB memory limit", maxWorkers, maxMemoryMB)
}

// estimateMemoryUsage estimates memory usage for a page of documents
func (wp *WorkerPool) estimateMemoryUsage(documents []bson.Raw) int64 {
	var totalSize int64
	for _, doc := range documents {
		totalSize += int64(len(doc))
	}
	return totalSize
}

// canAllocateMemory checks if we can allocate memory for a page
func (wp *WorkerPool) canAllocateMemory(estimatedSize int64) bool {
	wp.memoryMutex.RLock()
	defer wp.memoryMutex.RUnlock()

	maxBytes := int64(wp.maxMemoryMB) * 1024 * 1024
	return wp.currentMemory+estimatedSize <= maxBytes
}

// allocateMemory reserves memory for a page
func (wp *WorkerPool) allocateMemory(size int64) {
	wp.memoryMutex.Lock()
	defer wp.memoryMutex.Unlock()
	wp.currentMemory += size
}

// releaseMemory frees memory after processing a page
func (wp *WorkerPool) releaseMemory(size int64) {
	wp.memoryMutex.Lock()
	defer wp.memoryMutex.Unlock()
	wp.currentMemory -= size
	if wp.currentMemory < 0 {
		wp.currentMemory = 0
	}
}

// fetchPageConcurrently fetches a single page of data
func fetchPageConcurrently(database, collection string, pageNumber, pageSize int, resultChan chan<- PageResult) {
	req := models.DataRequest{
		Database:   database,
		Collection: collection,
		CountOnly:  false,
		PageSize:   pageSize,
		PageNumber: pageNumber,
	}

	reqBody, err := json.Marshal(req)
	if err != nil {
		resultChan <- PageResult{PageNumber: pageNumber, Error: err}
		return
	}

	// Create HTTP request with client ID header
	httpReq, err := http.NewRequest("POST", config.CloudSync.HTTPURL+"/api/data", bytes.NewBuffer(reqBody))
	if err != nil {
		resultChan <- PageResult{PageNumber: pageNumber, Error: err}
		return
	}
	httpReq.Header.Set("Content-Type", "application/json")
	httpReq.Header.Set("X-Client-ID", clientID)

	resp, err := httpClient.Do(httpReq)
	if err != nil {
		resultChan <- PageResult{PageNumber: pageNumber, Error: err}
		return
	}
	defer resp.Body.Close()

	var dataResp models.DataResponse

	// Check if response is encrypted
	if resp.Header.Get("X-Encryption-Enabled") == "true" {
		// Read encrypted response body
		encryptedData, err := io.ReadAll(resp.Body)
		if err != nil {
			resultChan <- PageResult{PageNumber: pageNumber, Error: fmt.Errorf("failed to read encrypted response: %w", err)}
			return
		}

		// Decrypt the response
		if err := encryptionMgr.DecryptJSON(encryptedData, &dataResp); err != nil {
			resultChan <- PageResult{PageNumber: pageNumber, Error: fmt.Errorf("failed to decrypt response: %w", err)}
			return
		}
		log.Printf("Decrypted response for %s.%s page %d (KeyID: %s)", database, collection, pageNumber, resp.Header.Get("X-Encryption-KeyID"))
	} else {
		// Handle unencrypted response
		if err := json.NewDecoder(resp.Body).Decode(&dataResp); err != nil {
			resultChan <- PageResult{PageNumber: pageNumber, Error: err}
			return
		}
	}

	if dataResp.Error != "" {
		resultChan <- PageResult{PageNumber: pageNumber, Error: fmt.Errorf("cloud error: %s", dataResp.Error)}
		return
	}

	// Determine if this is the last page
	isLastPage := dataResp.Pagination == nil || !dataResp.Pagination.HasNextPage

	resultChan <- PageResult{
		PageNumber:        pageNumber,
		Documents:         dataResp.Documents,
		Error:             nil,
		Indexes:           dataResp.Indexes,
		CollectionOptions: dataResp.CollectionOptions,
		SnapshotFence:     dataResp.SnapshotFence,
		IsLastPage:        isLastPage,
	}
}

func syncCollectionData(database, collection string) error {
	// Initialize worker pool if not already done
	if workerPool == nil {
		initWorkerPool()
	}

	// Use pagination to handle large collections efficiently
	pageSize := 1000 // Default page size
	if config.Sync.BatchSize > 0 {
		pageSize = config.Sync.BatchSize
	}

	// Clear local collection for clean sync
	coll := mongoClient.Database(database).Collection(collection)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	// Drop and recreate collection for clean sync
	if err := coll.Drop(ctx); err != nil {
		log.Printf("Warning: Failed to drop collection %s.%s: %v", database, collection, err)
	}

	// First, get the first page to determine total count and get metadata
	firstPageResult, err := fetchSinglePage(database, collection, 0, pageSize)
	if err != nil {
		return fmt.Errorf("failed to fetch first page: %v", err)
	}

	if len(firstPageResult.Documents) == 0 {
		log.Printf("No documents to sync for %s.%s", database, collection)
		return nil
	}

	// Calculate total pages needed
	cloudCount, err := getCloudCount(database, collection)
	if err != nil {
		return fmt.Errorf("failed to get cloud count: %v", err)
	}

	totalPages := int((cloudCount + int64(pageSize) - 1) / int64(pageSize))
	log.Printf("Starting parallel sync for %s.%s: %d total pages with %d workers", database, collection, totalPages, workerPool.workerCount)

	// Process first page
	if err := insertDocumentsBatch(database, collection, firstPageResult.Documents); err != nil {
		return fmt.Errorf("failed to insert documents from page 0: %v", err)
	}
	log.Printf("Processed page 0 for %s.%s: %d documents", database, collection, len(firstPageResult.Documents))

	// Process remaining pages in parallel if there are more
	if totalPages > 1 {
		if err := processRemainingPagesParallel(database, collection, pageSize, totalPages, firstPageResult); err != nil {
			return err
		}
	}

	// Handle final processing (indexes, collection options, snapshot fence)
	if err := handleFinalProcessing(database, collection, firstPageResult); err != nil {
		return err
	}

	log.Printf("Successfully completed parallel sync for %s.%s", database, collection)
	return nil
}

// syncCollectionDataWithMapping syncs data from source collection to target collection
// This is used when source and target database/collection names are different
func syncCollectionDataWithMapping(sourceDatabase, sourceCollection, targetDatabase, targetCollection string) error {
	// Initialize worker pool if not already done
	if workerPool == nil {
		initWorkerPool()
	}

	// Use pagination to handle large collections efficiently
	pageSize := 1000 // Default page size
	if config.Sync.BatchSize > 0 {
		pageSize = config.Sync.BatchSize
	}

	// Clear target collection for clean sync
	targetColl := mongoClient.Database(targetDatabase).Collection(targetCollection)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	// Drop and recreate target collection for clean sync
	if err := targetColl.Drop(ctx); err != nil {
		log.Printf("Warning: Failed to drop target collection %s.%s: %v", targetDatabase, targetCollection, err)
	}

	// First, get the first page from source to determine total count and get metadata
	firstPageResult, err := fetchSinglePage(sourceDatabase, sourceCollection, 0, pageSize)
	if err != nil {
		return fmt.Errorf("failed to fetch first page from source: %v", err)
	}

	if len(firstPageResult.Documents) == 0 {
		log.Printf("No documents to sync from %s.%s to %s.%s", sourceDatabase, sourceCollection, targetDatabase, targetCollection)
		return nil
	}

	// Calculate total pages needed
	cloudCount, err := getCloudCount(sourceDatabase, sourceCollection)
	if err != nil {
		return fmt.Errorf("failed to get cloud count: %v", err)
	}

	totalPages := int((cloudCount + int64(pageSize) - 1) / int64(pageSize))
	log.Printf("Starting parallel sync from %s.%s to %s.%s: %d total pages with %d workers", sourceDatabase, sourceCollection, targetDatabase, targetCollection, totalPages, workerPool.workerCount)

	// Process first page to target
	if err := insertDocumentsBatch(targetDatabase, targetCollection, firstPageResult.Documents); err != nil {
		return fmt.Errorf("failed to insert documents from page 0 to target: %v", err)
	}
	log.Printf("Processed page 0 from %s.%s to %s.%s: %d documents", sourceDatabase, sourceCollection, targetDatabase, targetCollection, len(firstPageResult.Documents))

	// Process remaining pages in parallel if there are more
	if totalPages > 1 {
		if err := processRemainingPagesMappingParallel(sourceDatabase, sourceCollection, targetDatabase, targetCollection, pageSize, totalPages, firstPageResult); err != nil {
			return err
		}
	}

	// Handle final processing (indexes, collection options, snapshot fence) on target collection
	if err := handleFinalProcessingWithMapping(targetDatabase, targetCollection, firstPageResult); err != nil {
		return err
	}

	log.Printf("Successfully completed parallel sync from %s.%s to %s.%s", sourceDatabase, sourceCollection, targetDatabase, targetCollection)
	return nil
}

// processRemainingPagesMappingParallel processes remaining pages with source to target mapping
func processRemainingPagesMappingParallel(sourceDatabase, sourceCollection, targetDatabase, targetCollection string, pageSize, totalPages int, firstPageResult *PageResult) error {
	// Channel for page results
	resultChan := make(chan PageResult, workerPool.workerCount*2)
	var wg sync.WaitGroup

	// Start workers for remaining pages (1 to totalPages-1)
	pageQueue := make(chan int, totalPages-1)
	for i := 1; i < totalPages; i++ {
		pageQueue <- i
	}
	close(pageQueue)

	// Launch worker goroutines
	for i := 0; i < workerPool.workerCount; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for pageNum := range pageQueue {
				fetchPageConcurrently(sourceDatabase, sourceCollection, pageNum, pageSize, resultChan)
			}
		}()
	}

	// Close result channel when all workers are done
	go func() {
		wg.Wait()
		close(resultChan)
	}()

	// Process results as they come in
	totalProcessed := len(firstPageResult.Documents) // Already processed page 0
	for result := range resultChan {
		if result.Error != nil {
			return fmt.Errorf("error fetching page %d: %v", result.PageNumber, result.Error)
		}

		if len(result.Documents) > 0 {
			// Estimate memory usage and wait if necessary
			memorySize := workerPool.estimateMemoryUsage(result.Documents)
			for !workerPool.canAllocateMemory(memorySize) {
				log.Printf("Memory limit reached, waiting before processing page %d...", result.PageNumber)
				time.Sleep(100 * time.Millisecond)
			}

			workerPool.allocateMemory(memorySize)

			// Process documents from this page to target collection
			if err := insertDocumentsBatch(targetDatabase, targetCollection, result.Documents); err != nil {
				workerPool.releaseMemory(memorySize)
				return fmt.Errorf("failed to insert documents from page %d to target: %v", result.PageNumber, err)
			}

			workerPool.releaseMemory(memorySize)
			totalProcessed += len(result.Documents)
			log.Printf("Processed page %d from %s.%s to %s.%s: %d documents (total: %d)", result.PageNumber, sourceDatabase, sourceCollection, targetDatabase, targetCollection, len(result.Documents), totalProcessed)
		}
	}

	return nil
}

// handleFinalProcessingWithMapping handles final processing for mapped collections
func handleFinalProcessingWithMapping(targetDatabase, targetCollection string, pageResult *PageResult) error {
	targetColl := mongoClient.Database(targetDatabase).Collection(targetCollection)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	// Recreate indexes if provided
	if len(pageResult.Indexes) > 0 {
		if err := recreateIndexes(ctx, targetColl, pageResult.Indexes); err != nil {
			log.Printf("Warning: Failed to recreate indexes for %s.%s: %v", targetDatabase, targetCollection, err)
		} else {
			log.Printf("Successfully recreated %d indexes for %s.%s", len(pageResult.Indexes), targetDatabase, targetCollection)
		}
	}

	// Apply collection options if provided
	if pageResult.CollectionOptions != nil {
		if err := applyCollectionOptions(ctx, mongoClient.Database(targetDatabase), targetCollection, pageResult.CollectionOptions); err != nil {
			log.Printf("Warning: Failed to apply collection options for %s.%s: %v", targetDatabase, targetCollection, err)
		} else {
			log.Printf("Successfully applied collection options for %s.%s", targetDatabase, targetCollection)
		}
	}

	// Store snapshot fence information for change stream coordination
	if pageResult.SnapshotFence != nil {
		log.Printf("Snapshot completed with fence - ClusterTime: %v, OperationTime: %v",
			pageResult.SnapshotFence.ClusterTime, pageResult.SnapshotFence.OperationTime)

		// Validate that change streams can start consistently with this fence
		if clusterFence != nil && clusterFence.IsEnabled() {
			if err := clusterFence.ValidateChangeStreamStart(convertToSnapshotFence(pageResult.SnapshotFence), pageResult.SnapshotFence.OperationTime); err != nil {
				log.Printf("Warning: Change stream coordination validation failed: %v", err)
			} else {
				log.Printf("Change stream coordination validated successfully for %s.%s", targetDatabase, targetCollection)
			}
		}
	} else {
		log.Printf("No snapshot fence provided - change stream coordination may have gaps")
	}

	return nil
}

func fetchSinglePage(database, collection string, pageNumber, pageSize int) (*PageResult, error) {
	resultChan := make(chan PageResult, 1)
	fetchPageConcurrently(database, collection, pageNumber, pageSize, resultChan)
	result := <-resultChan
	if result.Error != nil {
		return nil, result.Error
	}
	return &result, nil
}

func processRemainingPagesParallel(database, collection string, pageSize, totalPages int, firstPageResult *PageResult) error {
	// Channel for page results
	resultChan := make(chan PageResult, workerPool.workerCount*2)
	var wg sync.WaitGroup

	// Start workers for remaining pages (1 to totalPages-1)
	pageQueue := make(chan int, totalPages-1)
	for i := 1; i < totalPages; i++ {
		pageQueue <- i
	}
	close(pageQueue)

	// Launch worker goroutines
	for i := 0; i < workerPool.workerCount; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for pageNum := range pageQueue {
				fetchPageConcurrently(database, collection, pageNum, pageSize, resultChan)
			}
		}()
	}

	// Close result channel when all workers are done
	go func() {
		wg.Wait()
		close(resultChan)
	}()

	// Process results as they come in
	totalProcessed := len(firstPageResult.Documents) // Already processed page 0
	for result := range resultChan {
		if result.Error != nil {
			return fmt.Errorf("error fetching page %d: %v", result.PageNumber, result.Error)
		}

		if len(result.Documents) > 0 {
			// Estimate memory usage and wait if necessary
			memorySize := workerPool.estimateMemoryUsage(result.Documents)
			for !workerPool.canAllocateMemory(memorySize) {
				log.Printf("Memory limit reached, waiting before processing page %d...", result.PageNumber)
				time.Sleep(100 * time.Millisecond)
			}

			workerPool.allocateMemory(memorySize)

			// Process documents from this page
			if err := insertDocumentsBatch(database, collection, result.Documents); err != nil {
				workerPool.releaseMemory(memorySize)
				return fmt.Errorf("failed to insert documents from page %d: %v", result.PageNumber, err)
			}

			workerPool.releaseMemory(memorySize)
			totalProcessed += len(result.Documents)
			log.Printf("Processed page %d for %s.%s: %d documents (total: %d)", result.PageNumber, database, collection, len(result.Documents), totalProcessed)
		}
	}

	return nil
}

func handleFinalProcessing(database, collection string, pageResult *PageResult) error {
	coll := mongoClient.Database(database).Collection(collection)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	// Recreate indexes if provided
	if len(pageResult.Indexes) > 0 {
		if err := recreateIndexes(ctx, coll, pageResult.Indexes); err != nil {
			log.Printf("Warning: Failed to recreate indexes for %s.%s: %v", database, collection, err)
		} else {
			log.Printf("Successfully recreated %d indexes for %s.%s", len(pageResult.Indexes), database, collection)
		}
	}

	// Apply collection options if provided
	if pageResult.CollectionOptions != nil {
		if err := applyCollectionOptions(ctx, mongoClient.Database(database), collection, pageResult.CollectionOptions); err != nil {
			log.Printf("Warning: Failed to apply collection options for %s.%s: %v", database, collection, err)
		} else {
			log.Printf("Successfully applied collection options for %s.%s", database, collection)
		}
	}

	// Store snapshot fence information for change stream coordination
	if pageResult.SnapshotFence != nil {
		log.Printf("Snapshot completed with fence - ClusterTime: %v, OperationTime: %v",
			pageResult.SnapshotFence.ClusterTime, pageResult.SnapshotFence.OperationTime)

		// Validate that change streams can start consistently with this fence
		if clusterFence != nil && clusterFence.IsEnabled() {
			if err := clusterFence.ValidateChangeStreamStart(convertToSnapshotFence(pageResult.SnapshotFence), pageResult.SnapshotFence.OperationTime); err != nil {
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

// insertDocumentsBatch inserts a batch of documents efficiently with duplicate key handling
// deleteDocumentsBatch deletes a batch of documents by their document keys
// handleDropCollection handles collection drop operations
func handleDropCollection(database, collection string) error {
	coll := mongoClient.Database(database).Collection(collection)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if err := coll.Drop(ctx); err != nil {
		return fmt.Errorf("failed to drop collection %s.%s: %v", database, collection, err)
	}
	log.Printf("✅ DROPPED COLLECTION: %s.%s", database, collection)
	return nil
}

// handleRenameCollection handles collection rename operations
func handleRenameCollection(docMap bson.M, database, collection string) error {
	// Collection rename is complex and may not be directly supported in all scenarios
	// For now, log the operation and continue
	log.Printf("🔄 RENAME COLLECTION: %s.%s (rename operations require manual handling)", database, collection)
	return nil
}

// handleDropDatabase handles database drop operations
func handleDropDatabase(database string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	if err := mongoClient.Database(database).Drop(ctx); err != nil {
		return fmt.Errorf("failed to drop database %s: %v", database, err)
	}
	log.Printf("✅ DROPPED DATABASE: %s", database)
	return nil
}

// handleIndexOperation handles index creation, deletion, and modification operations
func handleIndexOperation(operation string, docMap bson.M, database, collection string) error {
	// Index operations are metadata changes that may need special handling
	// For now, log the operation - in production, you might want to replicate index changes
	log.Printf("🔍 INDEX OPERATION: %s for %s.%s", operation, database, collection)

	// You could implement actual index replication here if needed:
	// - Parse operationDescription for index definitions
	// - Apply createIndex/dropIndex operations to target
	// - Handle index modifications

	return nil
}

func deleteDocumentsBatch(database, collection string, documentKeys []bson.M) error {
	if len(documentKeys) == 0 {
		return nil
	}

	coll := mongoClient.Database(database).Collection(collection)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	// Delete documents in smaller batches to avoid memory issues
	batchSize := 1000
	for i := 0; i < len(documentKeys); i += batchSize {
		end := i + batchSize
		if end > len(documentKeys) {
			end = len(documentKeys)
		}

		// Create bulk write operations for deletion
		bulkOps := make([]mongo.WriteModel, 0, end-i)
		for j := i; j < end; j++ {
			deleteOp := mongo.NewDeleteOneModel()
			deleteOp.SetFilter(documentKeys[j])
			bulkOps = append(bulkOps, deleteOp)
		}

		if len(bulkOps) > 0 {
			// Use unordered bulk write for better performance
			opts := options.BulkWrite().SetOrdered(false)
			result, err := coll.BulkWrite(ctx, bulkOps, opts)
			if err != nil {
				return fmt.Errorf("failed to delete batch: %v", err)
			}
			log.Printf("✅ DELETED: %d documents from %s.%s (matched: %d)", result.DeletedCount, database, collection, result.MatchedCount)
		}
	}

	return nil
}

func insertDocumentsBatch(database, collection string, documents []bson.Raw) error {
	if len(documents) == 0 {
		return nil
	}

	coll := mongoClient.Database(database).Collection(collection)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	// Insert documents in smaller batches to avoid memory issues
	batchSize := 1000
	for i := 0; i < len(documents); i += batchSize {
		end := i + batchSize
		if end > len(documents) {
			end = len(documents)
		}

		// Use upserts for resumable sync compatibility
		bulkOps := make([]mongo.WriteModel, 0, end-i)
		for j := i; j < end; j++ {
			var doc bson.M
			if err := bson.Unmarshal(documents[j], &doc); err != nil {
				log.Printf("Error unmarshaling document: %v", err)
				continue
			}

			// Create upsert operation using _id as filter
			if id, ok := doc["_id"]; ok {
				filter := bson.M{"_id": id}
				upsertOp := mongo.NewReplaceOneModel()
				upsertOp.SetFilter(filter)
				upsertOp.SetReplacement(doc)
				upsertOp.SetUpsert(true)
				bulkOps = append(bulkOps, upsertOp)
			} else {
				log.Printf("Warning: Document without _id field, skipping upsert")
			}
		}

		if len(bulkOps) > 0 {
			// Use unordered bulk write for better performance
			opts := options.BulkWrite().SetOrdered(false)
			if _, err := coll.BulkWrite(ctx, bulkOps, opts); err != nil {
				return fmt.Errorf("failed to upsert batch: %v", err)
			}
			log.Printf("✅ UPSERTED: %d documents for %s.%s", len(bulkOps), database, collection)
		}
	}

	return nil
}

// Legacy function - keeping for compatibility but replacing with parallel version
func syncCollectionDataSequential(database, collection string) error {
	// This function has been replaced by the parallel version above
	// Keeping for reference only
	return fmt.Errorf("sequential sync has been replaced by parallel sync")
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
	// FIX PROBLEM #4: Use exponential backoff for reconnection attempts
	initialDelay := 2 * time.Second
	maxDelay := 30 * time.Second
	currentDelay := initialDelay
	attempt := 0

	for {
		attempt++
		if err := connectWebSocket(); err != nil {
			log.Printf("❌ RECONNECTION ATTEMPT %d failed: %v. Retrying in %v...", attempt, err, currentDelay)

			// Mark telemetry as disconnected during reconnection attempts
			if vmSyncIntegration != nil && vmSyncIntegration.GetTransmitter() != nil {
				vmSyncIntegration.GetTransmitter().MarkDisconnected()
			}

			// Exponential backoff: 2s, 4s, 8s, 16s, 30s (capped)
			time.Sleep(currentDelay)
			currentDelay = currentDelay * 2
			if currentDelay > maxDelay {
				currentDelay = maxDelay
			}
			continue
		}

		// Connection successful, break the retry loop
		log.Println("✅ WebSocket connection established successfully")
		// Reset delay for future reconnections
		currentDelay = initialDelay
		break
	}
}

func connectWebSocket() error {
	u, err := url.Parse(config.CloudSync.WSURL)
	if err != nil {
		return err
	}

	// Set headers to identify this as a vm-sync client and provide self-discovery info
	headers := http.Header{}
	headers.Set("User-Agent", "vm-sync-client/1.0")

	// SELF-DISCOVERY: Tell cloud-sync where to reach vm-sync back via TCP
	vmSyncDomain := os.Getenv("VM_SYNC_DOMAIN")
	if vmSyncDomain == "" {
		// Auto-discover external IP by asking a public service
		log.Printf("🔍 VM_SYNC_DOMAIN not set, auto-discovering external IP...")
		if externalIP, err := getExternalIP(); err == nil && externalIP != "" {
			vmSyncDomain = externalIP
			log.Printf("✅ AUTO-DISCOVERY: Found external IP: %s", vmSyncDomain)
		} else {
			vmSyncDomain = "localhost"
			log.Printf("⚠️ AUTO-DISCOVERY failed (%v), using fallback: %s", err, vmSyncDomain)
		}
	}

	headers.Set("X-VM-Sync-Domain", vmSyncDomain)
	headers.Set("X-VM-Sync-TCP-Port", "9000")
	headers.Set("X-VM-Sync-HTTP-Port", "8081")
	log.Printf("📡 SELF-DISCOVERY: Sending domain info to cloud-sync: %s (TCP:9000, HTTP:8081)", vmSyncDomain)

	log.Printf("Connecting to WebSocket: %s", u.String())
	conn, _, err := websocket.DefaultDialer.Dial(u.String(), headers)
	if err != nil {
		return err
	}

	// Store connection globally for graceful shutdown
	websocketConnMutex.Lock()
	websocketConn = conn
	websocketConnMutex.Unlock()

	// Note: Connection will be managed by the adaptive integration, not closed here

	log.Println("Connected to WebSocket for real-time sync")

	// Authenticate using OAuth2 (required)
	if vmTokenManager == nil {
		return fmt.Errorf("OAuth2 token manager not initialized")
	}

	// OAuth2 authentication
	// Always get a fresh token for WebSocket authentication to avoid expired token issues
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	token, err := vmTokenManager.GetFreshToken(ctx) // Force fresh token
	cancel()
	if err != nil {
		return fmt.Errorf("failed to get fresh OAuth2 token: %w", err)
	}

	// Send OAuth2 authentication message
	oauth2Msg := map[string]interface{}{
		"type":  "oauth2_auth",
		"token": token,
	}
	if err := conn.WriteJSON(oauth2Msg); err != nil {
		return fmt.Errorf("failed to send OAuth2 authentication: %w", err)
	}
	log.Println("OAuth2 authentication sent to cloud-sync")

	// Wait for authentication response
	conn.SetReadDeadline(time.Now().Add(30 * time.Second))
	_, responseData, err := conn.ReadMessage()
	if err != nil {
		return fmt.Errorf("failed to receive OAuth2 auth response: %w", err)
	}
	conn.SetReadDeadline(time.Time{}) // Clear deadline

	var authResponse map[string]interface{}
	if err := json.Unmarshal(responseData, &authResponse); err != nil {
		return fmt.Errorf("failed to parse OAuth2 auth response: %w", err)
	}

	if responseType, ok := authResponse["type"].(string); ok && responseType == "auth_success" {
		log.Println("OAuth2 authentication successful")
	} else {
		return fmt.Errorf("OAuth2 authentication failed: %v", authResponse["message"])
	}

	// If VM sync integration already exists, check if it's using HTTP or WebSocket
	if vmSyncIntegration != nil {
		if vmSyncIntegration.IsUsingHTTP() {
			log.Println("Existing HTTP VM sync integration is already running (no WebSocket connection needed)")
		} else {
			log.Println("Updating existing WebSocket VM sync integration with new connection")
			if transmitter := vmSyncIntegration.GetTransmitter(); transmitter != nil {
				transmitter.UpdateConnection(conn)
			}
		}
	} else {
		// Initialize VM sync integration with HTTP-based telemetry (no WebSocket dependency)
		vmSyncIntegration, err = adaptive.NewHTTPVMSyncIntegration(clientID, config.CloudSync.HTTPURL, vmTokenManager)
		if err != nil {
			log.Printf("Failed to initialize HTTP VM sync integration: %v", err)
			return err
		}
		log.Println("VM sync integration initialized with HTTP telemetry (WebSocket-free)")

		// Start telemetry collection and transmission
		if err := vmSyncIntegration.Start(); err != nil {
			log.Printf("Failed to start VM sync integration: %v", err)
		} else {
			log.Println("HTTP telemetry VM sync integration started successfully")
		}
	}

	// Send periodic ping to keep connection alive
	go func() {
		ticker := time.NewTicker(30 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				if err := conn.WriteMessage(websocket.PingMessage, nil); err != nil {
					log.Printf("❌ PING FAILED: %v", err)

					// Mark telemetry as disconnected when ping fails
					if vmSyncIntegration != nil {
						if vmSyncIntegration.IsUsingHTTP() {
							if httpTransmitter := vmSyncIntegration.GetHTTPTransmitter(); httpTransmitter != nil {
								httpTransmitter.MarkDisconnected()
								log.Printf("HTTP telemetry connection marked as disconnected")
							}
						} else if transmitter := vmSyncIntegration.GetTransmitter(); transmitter != nil {
							transmitter.MarkDisconnected()
							log.Printf("WebSocket telemetry connection marked as disconnected")
						}
					}

					// STABILITY FIX: Close the connection to trigger reconnection
					// This ensures the main read loop exits and reconnects with a fresh connection
					log.Printf("🔄 RECONNECTING: Closing broken WebSocket connection to trigger reconnection")
					conn.Close()
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

			// Mark telemetry as disconnected when WebSocket read fails
			if vmSyncIntegration != nil {
				if vmSyncIntegration.IsUsingHTTP() {
					if httpTransmitter := vmSyncIntegration.GetHTTPTransmitter(); httpTransmitter != nil {
						httpTransmitter.MarkDisconnected()
					}
				} else if transmitter := vmSyncIntegration.GetTransmitter(); transmitter != nil {
					transmitter.MarkDisconnected()
				}
			}

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

			// Return error to trigger reconnection
			log.Printf("WebSocket connection lost, will attempt to reconnect")
			return err
		}

		// Handle different message types
		if messageType == websocket.BinaryMessage {
			log.Printf("📥 WEBSOCKET: Received binary message (%d bytes)", len(messageData))
			if encryptionMgr.IsEnabled() {
				// Decrypt and then unmarshal BSON to preserve MongoDB types
				decryptedData, err := encryptionMgr.Decrypt(messageData)
				if err != nil {
					log.Printf("Error decrypting WebSocket message: %v", err)
					continue
				}
				if err := bson.Unmarshal(decryptedData, &event); err != nil {
					log.Printf("Error unmarshaling decrypted BSON message: %v", err)
					log.Printf("🔍 DEBUG: Decrypted data length: %d bytes", len(decryptedData))
					continue
				}
				log.Printf("✅ REALTIME: Decrypted change event: %s on %s.%s", event.OperationType, event.Database, event.Collection)
			} else {
				// Unmarshal BSON directly to preserve MongoDB types
				if err := bson.Unmarshal(messageData, &event); err != nil {
					log.Printf("Error unmarshaling BSON message: %v", err)
					log.Printf("🔍 DEBUG: Raw BSON data length: %d bytes", len(messageData))
					// Try to decode the raw BSON to see what fields are available
					var rawDoc map[string]interface{}
					if bsonErr := bson.Unmarshal(messageData, &rawDoc); bsonErr == nil {
						log.Printf("🔍 DEBUG: Available BSON fields: %v", func() []string {
							keys := make([]string, 0, len(rawDoc))
							for k := range rawDoc {
								keys = append(keys, k)
							}
							return keys
						}())
					}
					continue
				}
				log.Printf("✅ REALTIME: Received change event: %s on %s.%s (FullDoc: %d bytes, DocKey: %d bytes)",
					event.OperationType, event.Database, event.Collection, len(event.FullDocument), len(event.DocumentKey))
			}
		} else if messageType == websocket.TextMessage {
			// Handle JSON messages - check if it's a status update or change event
			log.Printf("💬 DEBUG: Received TextMessage (%d bytes): %s", len(messageData), string(messageData))
			var jsonMsg map[string]interface{}
			if err := json.Unmarshal(messageData, &jsonMsg); err != nil {
				log.Printf("Error unmarshaling JSON message: %v", err)
				continue
			}

			// Check if this is a status update (metrics_update, status_update, etc.)
			if msgType, ok := jsonMsg["type"].(string); ok {
				log.Printf("🔍 DEBUG: Message type detected: %s", msgType)
				switch msgType {
				case "metrics_update", "status_update", "log_entry":
					// Skip status updates - these are not change events
					log.Printf("Received status update: %s", msgType)
					continue
				case "initial_sync_trigger":
					// Handle initial sync trigger from API
					log.Printf("🚀 INITIAL SYNC: Received trigger from cloud-sync API")
					if err := handleInitialSyncTrigger(jsonMsg, conn); err != nil {
						log.Printf("❌ INITIAL SYNC: Failed to handle trigger: %v", err)
					} else {
						log.Printf("✅ INITIAL SYNC: Successfully completed full database replacement")
					}
					continue
				}
			} else {
				log.Printf("⚠️ DEBUG: No 'type' field found in JSON message")
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
	var targetDatabase, targetCollection string

	for _, coll := range config.Collections {
		// Handle both formats: "source" and "source:target"
		parts := strings.Split(coll, ":")
		sourceCollection := parts[0]

		if sourceCollection == fullCollection {
			authorized = true
			if len(parts) > 1 {
				// Use target mapping
				targetParts := strings.Split(parts[1], ".")
				if len(targetParts) == 2 {
					targetDatabase = targetParts[0]
					targetCollection = targetParts[1]
				}
			} else {
				// Use same database and collection names
				targetDatabase = event.Database
				targetCollection = event.Collection
			}
			break
		}
	}

	if !authorized {
		log.Printf("Ignoring change event for unauthorized collection: %s", fullCollection)
		return nil
	}

	// Use target database and collection for local operations
	coll := mongoClient.Database(targetDatabase).Collection(targetCollection)
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
			log.Printf("Inserted document in %s.%s (mapped from %s.%s)", targetDatabase, targetCollection, event.Database, event.Collection)
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
			log.Printf("Updated document in %s.%s (mapped from %s.%s)", targetDatabase, targetCollection, event.Database, event.Collection)
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
			log.Printf("Deleted document from %s.%s (mapped from %s.%s)", targetDatabase, targetCollection, event.Database, event.Collection)
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
			"type":       "ack",
			"sequenceId": event.SequenceID,
			"batchId":    event.BatchID,
			"eventId":    event.EventID,
			"clientId":   clientID,
			"timestamp":  time.Now(),
			"collection": fmt.Sprintf("%s.%s", event.Database, event.Collection),
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

	// Create indexes with conflict handling
	_, err := indexView.CreateMany(ctx, indexModels)
	if err != nil {
		// Check if it's an index conflict error
		if mongo.IsDuplicateKeyError(err) || strings.Contains(err.Error(), "IndexKeySpecsConflict") {
			log.Printf("⚠️ Index conflict detected, attempting to drop and recreate indexes")

			// Drop conflicting indexes and retry
			for _, model := range indexModels {
				indexName := *model.Options.Name
				log.Printf("🗑️ Dropping conflicting index: %s", indexName)
				if _, dropErr := indexView.DropOne(ctx, indexName); dropErr != nil {
					// Ignore error if index doesn't exist
					if !strings.Contains(dropErr.Error(), "index not found") {
						log.Printf("⚠️ Warning: Failed to drop index %s: %v", indexName, dropErr)
					}
				}
			}

			// Retry creating indexes after dropping conflicts
			log.Printf("🔄 Retrying index creation after dropping conflicts...")
			if _, retryErr := indexView.CreateMany(ctx, indexModels); retryErr != nil {
				log.Printf("⚠️ Warning: Index creation failed even after dropping conflicts: %v", retryErr)
				// Continue anyway - indexes are not critical for data sync
				return nil
			}
			log.Printf("✅ Successfully created indexes after resolving conflicts")
		} else {
			// For other errors, just log and continue
			log.Printf("⚠️ Warning: Failed to create indexes: %v", err)
			// Continue anyway - indexes are not critical for data sync
			return nil
		}
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

// handleInitialSyncTrigger handles the initial_sync_trigger message from cloud-sync API
// This performs a complete database replacement (clear + full sync)
func handleInitialSyncTrigger(jsonMsg map[string]interface{}, conn *websocket.Conn) error {
	log.Printf("🔄 INITIAL SYNC: Processing API trigger message")

	// Extract client ID and other parameters from the message
	clientID, _ := jsonMsg["client_id"].(string)
	force, _ := jsonMsg["force"].(bool)
	timestamp, _ := jsonMsg["timestamp"].(string)

	log.Printf("🎯 INITIAL SYNC: client_id=%s, force=%v, timestamp=%s", clientID, force, timestamp)

	// Send acknowledgment first
	ackMsg := map[string]interface{}{
		"type":      "initial_sync_ack",
		"status":    "started",
		"client_id": clientID,
		"timestamp": time.Now().Format(time.RFC3339),
		"message":   "Initial sync started - performing full database replacement",
	}

	if err := conn.WriteJSON(ackMsg); err != nil {
		log.Printf("❌ INITIAL SYNC: Failed to send acknowledgment: %v", err)
	} else {
		log.Printf("✅ INITIAL SYNC: Acknowledgment sent to cloud-sync")
	}

	// PERFORMANCE FIX: Perform full database replacement for all configured collections IN PARALLEL
	log.Printf("🗑️ INITIAL SYNC: Starting PARALLEL full database replacement for %d collections", len(config.Collections))

	totalCollections := len(config.Collections)
	var successCount int32 = 0
	var failedCollections []string
	var failedMutex sync.Mutex

	// Use WaitGroup for parallel processing
	var wg sync.WaitGroup

	// PARALLEL PROCESSING: Process each collection concurrently
	for i, collectionName := range config.Collections {
		wg.Add(1)
		go func(index int, collName string) {
			defer wg.Done()

			// Parse collection mapping format: "source.db.collection" or "source.db.collection:target.db.collection"
			var sourceCollection string
			if strings.Contains(collName, ":") {
				// Format: "source.db.collection:target.db.collection"
				mappingParts := strings.Split(collName, ":")
				sourceCollection = mappingParts[0]
			} else {
				// Format: "source.db.collection" (no target mapping)
				sourceCollection = collName
			}

			// Split source collection into database and collection
			parts := splitDatabaseCollection(sourceCollection)
			if len(parts) != 2 {
				log.Printf("⚠️ INITIAL SYNC: Skipping invalid collection format: %s", collName)
				failedMutex.Lock()
				failedCollections = append(failedCollections, collName)
				failedMutex.Unlock()
				return
			}

			database := parts[0]
			collection := parts[1]

			log.Printf("🔄 INITIAL SYNC PARALLEL: [%d/%d] Processing %s.%s", index+1, totalCollections, database, collection)

			// In the new approach without mapping, use the same database and collection names as the source
			targetDatabase := database
			targetCollection := collection

			log.Printf("🎯 INITIAL SYNC PARALLEL: Mapped %s.%s -> %s.%s", database, collection, targetDatabase, targetCollection)

			// Step 1: Clear target collection (COMPLETE REPLACEMENT)
			log.Printf("🗑️ INITIAL SYNC PARALLEL: Dropping collection %s.%s", targetDatabase, targetCollection)
			targetColl := mongoClient.Database(targetDatabase).Collection(targetCollection)
			ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
			if err := targetColl.Drop(ctx); err != nil {
				log.Printf("⚠️ INITIAL SYNC: Failed to drop %s.%s (might not exist): %v", targetDatabase, targetCollection, err)
				// Continue even if drop fails - collection might not exist
			}
			cancel()
			log.Printf("✅ INITIAL SYNC PARALLEL: Collection %s.%s cleared", targetDatabase, targetCollection)

			// Step 2: Clear checkpoints and watermarks for clean state
			if checkpointMgr != nil {
				log.Printf("🔄 INITIAL SYNC PARALLEL: Clearing checkpoints for %s.%s", targetDatabase, targetCollection)
				// Checkpoints will be recreated during sync
			}
			if watermarkMgr != nil {
				log.Printf("🔄 INITIAL SYNC PARALLEL: Clearing watermarks for %s.%s", targetDatabase, targetCollection)
				// Watermarks will be recreated during sync
			}

			// Step 3: Perform full sync from cloud (get ALL data + metadata)
			syncStartTime := time.Now()
			log.Printf("🚀 INITIAL SYNC PARALLEL: Starting full data + metadata sync for %s.%s -> %s.%s", database, collection, targetDatabase, targetCollection)
			if err := syncCollectionDataWithMapping(database, collection, targetDatabase, targetCollection); err != nil {
				log.Printf("❌ INITIAL SYNC: Failed to sync collection %s.%s -> %s.%s: %v", database, collection, targetDatabase, targetCollection, err)
				failedMutex.Lock()
				failedCollections = append(failedCollections, collName)
				failedMutex.Unlock()
				return
			}
			syncDuration := time.Since(syncStartTime)
			log.Printf("⏱️ INITIAL SYNC PARALLEL: Sync completed for %s.%s in %v", targetDatabase, targetCollection, syncDuration)

			// CRITICAL: Verify that indexes and metadata were properly transferred
			verifyTargetColl := mongoClient.Database(targetDatabase).Collection(targetCollection)
			verifyCtx, verifyCancel := context.WithTimeout(context.Background(), 30*time.Second)
			indexView := verifyTargetColl.Indexes()
			indexCursor, err := indexView.List(verifyCtx)
			if err == nil {
				indexCount := 0
				for indexCursor.Next(verifyCtx) {
					indexCount++
				}
				indexCursor.Close(verifyCtx)
				log.Printf("✅ INITIAL SYNC PARALLEL: Verified %d indexes created for %s.%s", indexCount, targetDatabase, targetCollection)
			} else {
				log.Printf("⚠️ INITIAL SYNC: Failed to verify indexes for %s.%s: %v", targetDatabase, targetCollection, err)
			}
			verifyCancel()

			// Increment success count atomically
			atomic.AddInt32(&successCount, 1)
			log.Printf("✅ INITIAL SYNC PARALLEL: [%d/%d] Successfully replaced %s.%s", index+1, totalCollections, targetDatabase, targetCollection)
		}(i, collectionName)
	}

	// Wait for all collections to finish processing
	log.Printf("⏳ INITIAL SYNC: Waiting for all %d collections to complete...", totalCollections)
	wg.Wait()
	log.Printf("✅ INITIAL SYNC: All collections processed")

	// Send final status back to cloud-sync
	finalStatus := "completed"
	if len(failedCollections) > 0 {
		finalStatus = "partial_success"
		if successCount == 0 {
			finalStatus = "failed"
		}
	}

	finalMsg := map[string]interface{}{
		"type":              "initial_sync_result",
		"status":            finalStatus,
		"client_id":         clientID,
		"timestamp":         time.Now().Format(time.RFC3339),
		"total_collections": totalCollections,
		"success_count":     successCount,
		"failed_count":      len(failedCollections),
		"message":           fmt.Sprintf("Initial sync %s: %d/%d collections processed", finalStatus, successCount, totalCollections),
	}

	if len(failedCollections) > 0 {
		finalMsg["failed_collections"] = failedCollections
	}

	if err := conn.WriteJSON(finalMsg); err != nil {
		log.Printf("❌ INITIAL SYNC: Failed to send final status: %v", err)
	} else {
		log.Printf("✅ INITIAL SYNC: Final status sent to cloud-sync")
	}

	if finalStatus == "completed" {
		log.Printf("🎉 INITIAL SYNC: Successfully completed full database replacement - ALL collections replaced")
	} else if finalStatus == "partial_success" {
		log.Printf("⚠️ INITIAL SYNC: Partial success - %d/%d collections replaced", successCount, totalCollections)
	} else {
		log.Printf("❌ INITIAL SYNC: Failed - no collections were successfully replaced")
		return fmt.Errorf("initial sync failed: %d collections failed", len(failedCollections))
	}

	return nil
}

// getExternalIP discovers the external IP address of this vm-sync instance
func getExternalIP() (string, error) {
	// Try multiple services in case one is down
	services := []string{
		"https://checkip.amazonaws.com",
		"https://ipinfo.io/ip",
		"https://api.ipify.org",
	}

	client := &http.Client{Timeout: 5 * time.Second}

	for _, service := range services {
		resp, err := client.Get(service)
		if err != nil {
			continue
		}
		defer resp.Body.Close()

		if resp.StatusCode == 200 {
			body, err := io.ReadAll(resp.Body)
			if err != nil {
				continue
			}
			ip := strings.TrimSpace(string(body))
			// Basic IP validation
			if net.ParseIP(ip) != nil {
				return ip, nil
			}
		}
	}

	return "", fmt.Errorf("failed to discover external IP from any service")
}
