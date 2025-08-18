package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/gorilla/mux"
	"github.com/gorilla/websocket"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"gopkg.in/yaml.v2"

	"go-data-sync-http/pkg/models"
	"go-data-sync-http/pkg/filtering"
	"go-data-sync-http/pkg/crypto"
	"go-data-sync-http/pkg/cluster"
	"go-data-sync-http/pkg/resume"
	"go-data-sync-http/pkg/tracking"
	"go-data-sync-http/pkg/sequence"
	"go-data-sync-http/pkg/fence"
	"go-data-sync-http/pkg/parallel"
	"go-data-sync-http/pkg/metrics"
	"go-data-sync-http/pkg/logging"
	"go-data-sync-http/pkg/license"
)





type ClientInfo struct {
	ClientType string // "vm-sync", "dashboard", "unknown"
	ClientID   string
	ConnectedAt time.Time
	License    *license.LicenseKey // License information for vm-sync clients
}

var (
	upgrader = websocket.Upgrader{
		CheckOrigin: func(r *http.Request) bool {
			return true // Allow connections from any origin
		},
	}
	clients         = make(map[*websocket.Conn]ClientInfo)
	broadcast       = make(chan models.ChangeEvent)
	statusUpdates   = make(chan map[string]interface{})
	mongoClient     *mongo.Client
	config          models.Config
	filterEngine    *filtering.FilterEngine
	encryptionMgr   *crypto.EncryptionManager
	internalCluster *cluster.InternalCluster
	checkpointMgr   *resume.CheckpointManager
	transferTracker *tracking.TransferTracker
	sequenceGen     *sequence.Generator
	clusterFence    *fence.ClusterTimeFence
	partitioner     *parallel.Partitioner
	metricsCollector *metrics.MetricsCollector
	alertManager     *metrics.AlertManager
	metricsAPI       *metrics.MetricsAPI
	activeWatchers   = make(map[string]bool) // Track active change streams
	watchersMutex    sync.RWMutex             // Protect activeWatchers map
	appLogger        *logging.Logger          // Application logger for dashboard
	licenseValidator *license.LicenseValidator // License validator for WebSocket connections
	
	// Sync control state
	syncPaused       bool
	syncPausedMutex  sync.RWMutex
	restartChan      = make(chan bool, 1)
	shutdownChan     = make(chan bool, 1)
)

func main() {
	configFile := flag.String("config", "config.yaml", "Path to configuration file")
	flag.Parse()

	// Load configuration
	if err := loadConfig(*configFile); err != nil {
		log.Fatalf("Failed to load config: %v", err)
	}

	// Connect to MongoDB
	ctx, cancel := context.WithTimeout(context.Background(), config.MongoDB.Timeout)
	defer cancel()

	clientOptions := options.Client().ApplyURI(config.MongoDB.URI)
	var err error
	mongoClient, err = mongo.Connect(ctx, clientOptions)
	if err != nil {
		log.Fatalf("Failed to connect to MongoDB: %v", err)
	}

	// Test the connection
	if err = mongoClient.Ping(ctx, nil); err != nil {
		log.Fatalf("Failed to ping MongoDB: %v", err)
	}
	log.Println("Connected to MongoDB successfully")
	
	// Initialize application logger early so we can use it
	appLogger = logging.NewLogger(5000) // Keep last 5000 log entries
	appLogger.Info("cloud-sync", "startup", "MongoDB connection established successfully", map[string]interface{}{
		"uri": config.MongoDB.URI,
	})

	// Initialize filter engine
	filterEngine = filtering.NewFilterEngine()

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

	// Initialize checkpoint manager
	if config.Checkpoint.Enabled {
		checkpointConfig := &resume.CheckpointConfig{
			MongoURI:        config.MongoDB.URI,
			Database:        config.Checkpoint.Database,
			Collection:      config.Checkpoint.Collection,
			PersistInterval: time.Duration(config.Checkpoint.SaveInterval) * time.Second,
			Enabled:         config.Checkpoint.Enabled,
		}
		var err error
		checkpointMgr, err = resume.NewCheckpointManager(checkpointConfig)
		if err != nil {
			log.Fatalf("Failed to initialize checkpoint manager: %v", err)
		}
		log.Println("Checkpoint manager initialized successfully")
	} else {
		log.Println("Checkpoint manager disabled")
	}

	// Initialize transfer tracker
	trackingConfig := convertTrackingConfig(config.Tracking)
	// Use the same MongoDB URI as the main service
	trackingConfig.MongoURI = config.MongoDB.URI
	transferTracker, err = tracking.NewTransferTracker(trackingConfig)
	if err != nil {
		log.Fatalf("Failed to initialize transfer tracker: %v", err)
	}
	if transferTracker.IsEnabled() {
		log.Println("Transfer tracking enabled")
	} else {
		log.Println("Transfer tracking disabled")
	}

	// Initialize sequence generator
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
		log.Printf("Sequence generator initialized for node %s", config.Sequence.NodeID)
	} else {
		log.Println("Sequence generator disabled")
	}

	// Initialize cluster time fence
	if config.Fence.Enabled {
		fenceConfig := &fence.FenceConfig{
			Enabled:  config.Fence.Enabled,
			MongoURI: config.MongoDB.URI,
		}
		clusterFence, err = fence.NewClusterTimeFence(fenceConfig)
		if err != nil {
			log.Fatalf("Failed to initialize cluster time fence: %v", err)
		}
		log.Println("Cluster time fence initialized")
	} else {
		log.Println("Cluster time fence disabled")
	}

	// Initialize partitioner
	partitioner = parallel.NewPartitioner(mongoClient, parallel.DefaultPartitionConfig())
	log.Println("Partitioner initialized with default configuration")

	// Initialize metrics system
	metricsCollector = metrics.NewMetricsCollector(1000) // Keep last 1000 metrics
	alertManager = metrics.NewAlertManager(metricsCollector, 1000) // Keep last 1000 alerts
	metricsAPI = metrics.NewMetricsAPI(metricsCollector, alertManager)

	// Add default alert rules
	defaultRules := metrics.DefaultAlertRules()
	for _, rule := range defaultRules {
		alertManager.AddRule(rule)
	}

	// Start alert manager
	go alertManager.Start(context.Background(), 30*time.Second) // Check every 30 seconds
	log.Println("Metrics system initialized with default alert rules")

	// Logger already initialized earlier
	appLogger.Info("cloud-sync", "startup", "Application logger initialized for dashboard", nil)

	// Initialize license validator
	licenseValidator, err = license.NewLicenseValidator()
	if err != nil {
		log.Fatalf("Failed to initialize license validator: %v", err)
	}
	log.Println("License validator initialized successfully")
	appLogger.Info("cloud-sync", "startup", "License validator initialized", map[string]interface{}{
		"cloud_license": licenseValidator.GetCloudLicense().String(),
	})

	// Start metrics calculation goroutine
	go func() {
		ticker := time.NewTicker(10 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				metricsCollector.CalculateThroughput()
			}
		}
	}()

	// Initialize internal cluster if enabled
	if config.InternalCluster.Enabled {
		internalCluster = cluster.NewInternalCluster(config.InternalCluster)
		log.Printf("Internal cluster enabled with %d workers", config.InternalCluster.WorkerPool.WorkerCount)
		
		// Start internal cluster
		if err := internalCluster.Start(); err != nil {
			log.Fatalf("Failed to start internal cluster: %v", err)
		}
		log.Println("Internal cluster started successfully")
	}

	// Start change stream monitoring
	go monitorChangeStreams()

	// Start WebSocket broadcast handler
	go handleBroadcast()
	
	// Start status broadcaster for dashboard updates
	go startStatusBroadcaster()

	// Setup HTTP routes
	router := mux.NewRouter()
	router.HandleFunc(config.WebSocket.Endpoint, handleWebSocket)
	router.HandleFunc("/api/data", handleDataRequest).Methods("POST")
	router.HandleFunc("/api/partitions", handlePartitionsRequest).Methods("POST")
	router.HandleFunc("/health", handleHealth).Methods("GET")

	// Dashboard routes
	router.HandleFunc("/dashboard", handleDashboard).Methods("GET")
	router.HandleFunc("/api/logs", handleLogs).Methods("GET")
	router.HandleFunc("/api/metrics/charts", handleChartData).Methods("GET")
	router.HandleFunc("/api/control/{action}", handleControl).Methods("POST")
	
	// Serve static files for dashboard
	router.PathPrefix("/static/").Handler(http.StripPrefix("/static/", http.FileServer(http.Dir("./web/static/"))))

	// Register metrics API routes
	metricsAPI.RegisterRoutes(router)

	// Start HTTP server
	server := &http.Server{
		Addr:    fmt.Sprintf("%s:%d", config.Server.Host, config.Server.Port),
		Handler: router,
	}

	go func() {
		log.Printf("Starting HTTP server on port %d", config.Server.Port)
		log.Printf("WebSocket endpoint: ws://%s:%d%s", config.Server.Host, config.Server.Port, config.WebSocket.Endpoint)
		log.Printf("Data API endpoint: http://%s:%d/api/data", config.Server.Host, config.Server.Port)
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("Failed to start server: %v", err)
		}
	}()

	// Wait for interrupt signal
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	log.Println("Shutting down server...")
	ctx, cancel = context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if err := server.Shutdown(ctx); err != nil {
		log.Printf("Server forced to shutdown: %v", err)
	}

	// Shutdown internal cluster if enabled
	if config.InternalCluster.Enabled && internalCluster != nil {
		log.Println("Shutting down internal cluster...")
		internalCluster.Stop()
		log.Println("Internal cluster stopped")
	}

	// Close transfer tracker
	if transferTracker != nil {
		log.Println("Closing transfer tracker...")
		if err := transferTracker.Close(); err != nil {
			log.Printf("Error closing transfer tracker: %v", err)
		}
	}

	if err := mongoClient.Disconnect(ctx); err != nil {
		log.Printf("Error disconnecting from MongoDB: %v", err)
	}

	log.Println("Server exited")
}

func loadConfig(filename string) error {
	data, err := os.ReadFile(filename)
	if err != nil {
		return err
	}
	return yaml.Unmarshal(data, &config)
}

// convertTrackingConfig converts models.TrackingConfig to tracking.TransferConfig
func convertTrackingConfig(modelConfig models.TrackingConfig) *tracking.TransferConfig {
	return &tracking.TransferConfig{
		Enabled:            modelConfig.Enabled,
		Database:           modelConfig.Database,
		TransferCollection: modelConfig.TransferCollection,
		StateCollection:    modelConfig.StateCollection,
		BatchCollection:    modelConfig.BatchCollection,
	}
}

func handleWebSocket(w http.ResponseWriter, r *http.Request) {
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Printf("WebSocket upgrade failed: %v", err)
		return
	}
	defer conn.Close()

	// Determine client type based on User-Agent or other headers
	clientType := "unknown"
	userAgent := r.Header.Get("User-Agent")
	if strings.Contains(userAgent, "vm-sync") {
		clientType = "vm-sync"
	} else if strings.Contains(r.Header.Get("Referer"), "/dashboard") || strings.Contains(userAgent, "Mozilla") {
		clientType = "dashboard"
	}

	clientInfo := ClientInfo{
		ClientType:  clientType,
		ClientID:    fmt.Sprintf("%s-%d", clientType, time.Now().Unix()),
		ConnectedAt: time.Now(),
	}

	// For vm-sync clients, validate license before allowing connection
	if clientType == "vm-sync" {
		// Wait for license information from vm-sync client
		conn.SetReadDeadline(time.Now().Add(30 * time.Second)) // 30 second timeout for license
		messageType, messageData, err := conn.ReadMessage()
		if err != nil {
			log.Printf("Failed to receive license from vm-sync client: %v", err)
			return
		}

		if messageType == websocket.TextMessage {
			var licenseMsg map[string]interface{}
			if err := json.Unmarshal(messageData, &licenseMsg); err != nil {
				log.Printf("Failed to parse license message: %v", err)
				conn.WriteMessage(websocket.TextMessage, []byte(`{"type":"error","message":"Invalid license format"}`))
				return
			}

			if msgType, ok := licenseMsg["type"].(string); !ok || msgType != "license" {
				log.Printf("Expected license message, got: %v", msgType)
				conn.WriteMessage(websocket.TextMessage, []byte(`{"type":"error","message":"License required for vm-sync connection"}`))
				return
			}

			// Extract license information
			licenseData, ok := licenseMsg["license"].(map[string]interface{})
			if !ok {
				log.Printf("Invalid license data format")
				conn.WriteMessage(websocket.TextMessage, []byte(`{"type":"error","message":"Invalid license data"}`))
				return
			}

			uuid, uuidOk := licenseData["uuid"].(string)
			if !uuidOk {
				log.Printf("Missing license uuid")
				conn.WriteMessage(websocket.TextMessage, []byte(`{"type":"error","message":"Missing license uuid"}`))
				return
			}

			vmLicense := &license.LicenseKey{
				UUID: uuid,
			}

			// Validate the license
			if err := licenseValidator.ValidateVMConnection(vmLicense); err != nil {
				log.Printf("License validation failed for vm-sync client: %v", err)
				conn.WriteMessage(websocket.TextMessage, []byte(fmt.Sprintf(`{"type":"error","message":"License validation failed: %s"}`, err.Error())))
				return
			}

			// License is valid, store it in client info
			clientInfo.License = vmLicense
			log.Printf("vm-sync client license validated successfully: %s", vmLicense.String())

			// Send success response
			conn.WriteMessage(websocket.TextMessage, []byte(`{"type":"license_accepted","message":"License validated successfully"}`))
		} else {
			log.Printf("Expected text message for license, got message type: %d", messageType)
			conn.WriteMessage(websocket.TextMessage, []byte(`{"type":"error","message":"License must be sent as text message"}`))
			return
		}

		// Clear read deadline after license validation
		conn.SetReadDeadline(time.Time{})
	}

	clients[conn] = clientInfo
	log.Printf("%s client connected. Total clients: %d", clientType, len(clients))
	if clientType == "vm-sync" && clientInfo.License != nil {
		appLogger.Info("websocket", "vm_sync_connected", "VM-sync client connected with valid license", map[string]interface{}{
			"client_id": clientInfo.ClientID,
			"license": clientInfo.License.String(),
		})
	}

	// Handle incoming messages from clients
	for {
		messageType, messageData, err := conn.ReadMessage()
		if err != nil {
			log.Printf("Client disconnected: %v", err)
			delete(clients, conn)
			log.Printf("Client removed. Total clients: %d", len(clients))
			break
		}

		// Handle acknowledgment messages
		if messageType == websocket.TextMessage {
			var ack map[string]interface{}
			if err := json.Unmarshal(messageData, &ack); err == nil {
				if ackType, ok := ack["type"].(string); ok && ackType == "ack" {
					if sequenceID, ok := ack["sequenceId"].(float64); ok {
						if clientID, ok := ack["clientId"].(string); ok {
							if collection, ok := ack["collection"].(string); ok {
								log.Printf("Received acknowledgment from client %s for sequence %.0f on %s", clientID, sequenceID, collection)
								// TODO: Update transfer tracking with acknowledgment
								if transferTracker != nil {
									// Mark sequence as acknowledged in transfer tracker
									log.Printf("Marking sequence %.0f as acknowledged for client %s", sequenceID, clientID)
								}
							}
						}
					}
				}
			}
		}
	}
}

func handleBroadcast() {
	for {
		select {
		case event := <-broadcast:
			// Handle change events
			handleChangeEvent(event)
		case eventPtr := <-func() <-chan *models.ChangeEvent {
			if config.InternalCluster.Enabled && internalCluster != nil {
				return internalCluster.GetOutputChannel()
			}
			return make(<-chan *models.ChangeEvent) // Return empty channel if not enabled
		}():
			// Handle internal cluster events
			if eventPtr != nil {
				handleChangeEvent(*eventPtr)
			}
		case statusUpdate := <-statusUpdates:
			// Handle status updates
			broadcastStatusUpdate(statusUpdate)
		}
	}
}

func handleChangeEvent(event models.ChangeEvent) {
	log.Printf("Broadcasting change event: %s on %s.%s", event.OperationType, event.Database, event.Collection)
	
	// Serialize event using BSON to preserve MongoDB types
	var messageData []byte
	var err error
	
	if encryptionMgr.IsEnabled() {
		// First marshal to BSON to preserve types, then encrypt
		bsonData, err := bson.Marshal(event)
		if err != nil {
			log.Printf("Error marshaling event to BSON: %v", err)
			return
		}
		messageData, err = encryptionMgr.Encrypt(bsonData)
		if err != nil {
			log.Printf("Error encrypting event: %v", err)
			return
		}
	} else {
		// Use BSON serialization to preserve MongoDB types
		messageData, err = bson.Marshal(event)
		if err != nil {
			log.Printf("Error marshaling event to BSON: %v", err)
			return
		}
	}
	
	for client := range clients {
		err := client.WriteMessage(websocket.BinaryMessage, messageData)
		if err != nil {
			log.Printf("Error writing to client: %v", err)
			client.Close()
			delete(clients, client)
		}
	}
}

func broadcastStatusUpdate(statusUpdate map[string]interface{}) {
	messageData, err := json.Marshal(statusUpdate)
	if err != nil {
		log.Printf("Error marshaling status update: %v", err)
		return
	}
	
	for client := range clients {
		err := client.WriteMessage(websocket.TextMessage, messageData)
		if err != nil {
			log.Printf("Error writing status update to client: %v", err)
			client.Close()
			delete(clients, client)
		}
	}
}

func broadcastHealthStatus() {
	healthStatus := getHealthStatus()
	statusUpdate := map[string]interface{}{
		"type": "status_update",
		"data": healthStatus,
		"timestamp": time.Now().Format(time.RFC3339),
	}
	
	select {
	case statusUpdates <- statusUpdate:
	default:
		// Channel is full, skip this update
	}
}

func broadcastMetricsUpdate() {
	if metricsCollector == nil {
		return
	}
	
	metricsData := metricsCollector.GetMetrics()
	
	// Calculate aggregated metrics from the collected data
	totalDocs := int64(0)
	syncRate := float64(0)
	avgLatency := float64(0)
	backlogSize := int64(0)
	
	// Get actual count of active watchers
	watchersMutex.RLock()
	activeWatchersCount := len(activeWatchers)
	watchersMutex.RUnlock()
	
	// Aggregate throughput metrics
	if throughputMetrics, ok := metricsData["throughput_metrics"].(map[string]*metrics.ThroughputMetrics); ok {
		for _, tm := range throughputMetrics {
			if tm != nil {
				// Use events per second as a proxy for sync rate
				syncRate += tm.EventsPerSecond
			}
		}
	}
	
	// Aggregate lag metrics for average latency
	lagCount := 0
	if lagMetrics, ok := metricsData["lag_metrics"].(map[string]*metrics.LagMetrics); ok {
		for _, lm := range lagMetrics {
			if lm != nil {
				avgLatency += float64(lm.ReplicationLag.Milliseconds())
				lagCount++
			}
		}
		if lagCount > 0 {
			avgLatency = avgLatency / float64(lagCount)
		}
	}
	
	// Calculate total documents from metrics collector
	if dashboardMetrics, ok := metricsData["dashboard_metrics"].(map[string]interface{}); ok {
		if totalDocsVal, exists := dashboardMetrics["total_documents"]; exists {
			if totalDocsInt, ok := totalDocsVal.(int64); ok {
				totalDocs = totalDocsInt
			}
		}
		if backlogVal, exists := dashboardMetrics["backlog_size"]; exists {
			if backlogInt, ok := backlogVal.(int64); ok {
				backlogSize = backlogInt
			}
		}
	}
	
	// Get detailed configuration information
	var lastCheckpointTime string
	if checkpointMgr != nil {
		checkpoints := checkpointMgr.GetAllCheckpoints()
		if len(checkpoints) > 0 {
			// Find the most recent checkpoint
			var mostRecent time.Time
			for _, cp := range checkpoints {
				if cp.LastUpdated.After(mostRecent) {
					mostRecent = cp.LastUpdated
				}
			}
			lastCheckpointTime = mostRecent.Format("2006-01-02 15:04:05")
		} else {
			lastCheckpointTime = "No checkpoints available"
		}
	} else {
		lastCheckpointTime = "Checkpoint manager not initialized"
	}

	// Build detailed sync mode description
	syncModeDetails := "Real-time Change Stream Sync"
	if config.Encryption.Enabled {
		syncModeDetails += " + AES-256-GCM Encryption"
	}
	if config.Tracking.Enabled {
		syncModeDetails += " + Transfer Tracking"
	}
	if config.Sequence.Enabled {
		syncModeDetails += " + Sequence Ordering"
	}

	metricsUpdate := map[string]interface{}{
		"type": "metrics_update",
		"metrics": map[string]interface{}{
			"total_documents": totalDocs,
			"today_documents": totalDocs, // For now, same as total
			"sync_rate": syncRate,
			"backlog_size": backlogSize,
			"avg_latency": avgLatency,
			"active_watchers": activeWatchersCount,
			"last_resume_token": time.Now().Format(time.RFC3339),
			"sync_mode": syncModeDetails,
			"last_checkpoint": lastCheckpointTime,
			"connected_clients": len(clients),
		},
		"timestamp": time.Now().Format(time.RFC3339),
	}
	
	select {
	case statusUpdates <- metricsUpdate:
	default:
		// Channel is full, skip this update
	}
}

func getHealthStatus() map[string]string {
	healthStatus := map[string]string{
		"source_mongo": "connected",
		"cloud_sync": "connected",
		"vm_sync": "connected",
		"target_mongo": "connected",
	}
	
	// Check MongoDB connection
	if mongoClient != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := mongoClient.Ping(ctx, nil); err != nil {
			healthStatus["source_mongo"] = "disconnected"
		}
	} else {
		healthStatus["source_mongo"] = "disconnected"
	}
	
	// Check internal cluster status
	if config.InternalCluster.Enabled {
		if internalCluster == nil {
			healthStatus["cloud_sync"] = "disconnected"
		}
	}
	
	return healthStatus
}

func startStatusBroadcaster() {
	go func() {
		ticker := time.NewTicker(5 * time.Second)
		defer ticker.Stop()
		
		for {
			select {
			case <-ticker.C:
				broadcastHealthStatus()
				broadcastMetricsUpdate()
			}
		}
	}()
}

// isInvalidateResumeTokenError checks if the error is due to trying to resume after an invalidate event
func isInvalidateResumeTokenError(err error) bool {
	if err == nil {
		return false
	}
	// Check for MongoDB's InvalidResumeToken error related to invalidate events
	errorStr := err.Error()
	return strings.Contains(errorStr, "InvalidResumeToken") && 
		   strings.Contains(errorStr, "invalidate notification")
}

func monitorChangeStreams() {
	log.Println("Starting change stream monitoring...")
	appLogger.Info("cloud-sync", "monitor_start", "Starting change stream monitoring", map[string]interface{}{
		"total_databases": len(config.MongoDB.Databases),
	})
	
	// Start restart handler goroutine
	go func() {
		for {
			select {
			case <-restartChan:
				log.Println("Restart signal received - restarting change stream monitoring")
				appLogger.Info("cloud-sync", "restart_triggered", "Restart signal received - restarting change stream monitoring", map[string]interface{}{
					"action": "restart_monitoring",
				})
				
				// Clear all active watchers
				watchersMutex.Lock()
				activeWatchers = make(map[string]bool)
				watchersMutex.Unlock()
				metricsCollector.SetActiveWatchersCount(0)
				
				// Restart monitoring
				go monitorChangeStreams()
				return
			case <-shutdownChan:
				log.Println("Shutdown signal received")
				return
			}
		}
	}()
	
	for _, database := range config.MongoDB.Databases {
		if !database.Enabled {
			continue
		}
		for _, collection := range database.Collections {
			if !collection.Enabled {
				continue
			}
			go func(dbName, collName string, collConfig models.CollectionConfig) {
				for {
					// Check if sync is paused
					syncPausedMutex.RLock()
					paused := syncPaused
					syncPausedMutex.RUnlock()
					
					if paused {
						time.Sleep(1 * time.Second)
						continue
					}
					
					if err := watchCollection(dbName, collName, collConfig); err != nil {
						// Check if this is an InvalidResumeToken error due to invalidate event
						if isInvalidateResumeTokenError(err) {
							log.Printf("Invalidate resume token error for %s.%s - clearing checkpoint and starting fresh", dbName, collName)
							// Clear the checkpoint to start fresh after invalidate
							if checkpointMgr != nil {
								if checkpoint := checkpointMgr.GetCheckpoint(dbName, collName); checkpoint != nil {
									checkpoint.ResumeToken = nil // Clear resume token
									log.Printf("Cleared resume token for %s.%s after invalidate event", dbName, collName)
								}
							}
						} else {
							log.Printf("Change stream error for %s.%s: %v. Retrying in 5 seconds...", dbName, collName, err)
							time.Sleep(5 * time.Second)
						}
					}
				}
			}(database.Name, collection.Name, collection)
		}
	}
}

func watchCollection(dbName, collName string, collConfig models.CollectionConfig) error {
	coll := mongoClient.Database(dbName).Collection(collName)
	
	// Create a context for the change stream
	ctx := context.Background()
	
	// Register this watcher as active
	watcherKey := dbName + "." + collName
	watchersMutex.Lock()
	activeWatchers[watcherKey] = true
	watcherCount := len(activeWatchers)
	watchersMutex.Unlock()
	
	// Update metrics collector with current count
	metricsCollector.SetActiveWatchersCount(watcherCount)
	
	// Log watcher registration
	appLogger.Info("cloud-sync", "watcher_start", fmt.Sprintf("Started change stream watcher for %s.%s", dbName, collName), map[string]interface{}{
		"database": dbName,
		"collection": collName,
		"total_watchers": watcherCount,
	})
	
	// Ensure watcher is unregistered when function exits
	defer func() {
		watchersMutex.Lock()
		delete(activeWatchers, watcherKey)
		watcherCount := len(activeWatchers)
		watchersMutex.Unlock()
		// Update metrics collector with new count
		metricsCollector.SetActiveWatchersCount(watcherCount)
		log.Printf("Unregistered watcher for %s.%s", dbName, collName)
		
		// Log watcher unregistration
		appLogger.Info("cloud-sync", "watcher_stop", fmt.Sprintf("Stopped change stream watcher for %s.%s", dbName, collName), map[string]interface{}{
			"database": dbName,
			"collection": collName,
			"total_watchers": watcherCount,
		})
	}()
	
	// Get resume token from checkpoint if available
	var watchOptions *options.ChangeStreamOptions
	if checkpointMgr != nil {
		if checkpoint := checkpointMgr.GetCheckpoint(dbName, collName); checkpoint != nil && len(checkpoint.ResumeToken) > 0 {
			watchOptions = options.ChangeStream().SetResumeAfter(checkpoint.ResumeToken).SetFullDocument(options.UpdateLookup)
			log.Printf("Resuming change stream for %s.%s from checkpoint", dbName, collName)
			appLogger.Info("cloud-sync", "resume_token", fmt.Sprintf("Resuming change stream for %s.%s from checkpoint", dbName, collName), map[string]interface{}{
				"database": dbName,
				"collection": collName,
				"has_checkpoint": true,
			})
		} else {
			watchOptions = options.ChangeStream().SetFullDocument(options.UpdateLookup)
			log.Printf("Starting new change stream for %s.%s", dbName, collName)
			appLogger.Info("cloud-sync", "new_stream", fmt.Sprintf("Starting new change stream for %s.%s", dbName, collName), map[string]interface{}{
				"database": dbName,
				"collection": collName,
				"has_checkpoint": false,
			})
		}
	} else {
		watchOptions = options.ChangeStream().SetFullDocument(options.UpdateLookup)
		log.Printf("Starting change stream for %s.%s (no checkpoint manager)", dbName, collName)
		appLogger.Info("cloud-sync", "new_stream", fmt.Sprintf("Starting change stream for %s.%s (no checkpoint manager)", dbName, collName), map[string]interface{}{
			"database": dbName,
			"collection": collName,
			"checkpoint_manager": false,
		})
	}
	
	// Build change stream pipeline with the same filters as initial dump
	var pipeline mongo.Pipeline
	
	// Add document filters to change stream pipeline
	if len(collConfig.DocumentFilter.Criteria) > 0 {
		docFilterPipeline := filterEngine.BuildDocumentFilterPipeline(&collConfig.DocumentFilter)
		for _, stage := range docFilterPipeline {
			// Convert bson.M to bson.D for pipeline
			var stageD bson.D
			for k, v := range stage {
				stageD = append(stageD, bson.E{Key: k, Value: v})
			}
			pipeline = append(pipeline, stageD)
		}
	}
	
	// Add field filters to change stream pipeline
	fieldFilterPipeline := filterEngine.BuildFieldFilterPipeline(&collConfig.FieldFilter)
	for _, stage := range fieldFilterPipeline {
		// Convert bson.M to bson.D for pipeline
		var stageD bson.D
		for k, v := range stage {
			stageD = append(stageD, bson.E{Key: k, Value: v})
		}
		pipeline = append(pipeline, stageD)
	}
	
	log.Printf("Starting change stream for %s.%s with %d pipeline stages", dbName, collName, len(pipeline))
	if len(pipeline) > 0 {
		appLogger.Info("cloud-sync", "stream_filters", fmt.Sprintf("Applied %d filter stages to change stream for %s.%s", len(pipeline), dbName, collName), map[string]interface{}{
			"database": dbName,
			"collection": collName,
			"filter_stages": len(pipeline),
			"has_document_filter": len(collConfig.DocumentFilter.Criteria) > 0,
			"has_field_filter": len(collConfig.FieldFilter.IncludeFields) > 0 || len(collConfig.FieldFilter.ExcludeFields) > 0,
		})
	}
	
	changeStream, err := coll.Watch(ctx, pipeline, watchOptions)
	if err != nil {
		appLogger.Error("cloud-sync", "stream_error", fmt.Sprintf("Failed to create change stream for %s.%s: %v", dbName, collName, err), map[string]interface{}{
			"database": dbName,
			"collection": collName,
			"error": err.Error(),
		})
		return fmt.Errorf("failed to create change stream: %v", err)
	}
	defer changeStream.Close(ctx)

	log.Printf("Watching changes for %s.%s", dbName, collName)
	log.Printf("Starting change stream loop for %s.%s", dbName, collName)

	// Add a goroutine to periodically check if change stream is alive
	go func() {
		ticker := time.NewTicker(30 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				log.Printf("Change stream for %s.%s is still active", dbName, collName)
			case <-ctx.Done():
				return
			}
		}
	}()

	log.Printf("Entering change stream loop for %s.%s", dbName, collName)
	for {
		// Check if sync is paused
		syncPausedMutex.RLock()
		if syncPaused {
			syncPausedMutex.RUnlock()
			time.Sleep(1 * time.Second) // Wait while paused
			continue
		}
		syncPausedMutex.RUnlock()

		log.Printf("About to call changeStream.Next() for %s.%s", dbName, collName)
		if changeStream.Next(ctx) {
			log.Printf("changeStream.Next() returned true for %s.%s", dbName, collName)
			// Get raw BSON data to preserve MongoDB types
			rawData := changeStream.Current
			var changeDoc bson.M
			if err := bson.Unmarshal(rawData, &changeDoc); err != nil {
				log.Printf("Error decoding change document: %v", err)
				appLogger.Error("cloud-sync", "decode_error", fmt.Sprintf("Error decoding change document for %s.%s: %v", dbName, collName, err), map[string]interface{}{
					"database": dbName,
					"collection": collName,
					"error": err.Error(),
				})
				if metricsCollector != nil {
					metricsCollector.RecordError(dbName, collName, "decode_error", err.Error(), nil)
				}
				continue
			}

			operationType := changeDoc["operationType"].(string)
			log.Printf("Change detected in %s.%s: %s", dbName, collName, operationType)
			
			// Log change event
			appLogger.Info("cloud-sync", "change_event", fmt.Sprintf("Change detected in %s.%s: %s", dbName, collName, operationType), map[string]interface{}{
				"database": dbName,
				"collection": collName,
				"operation_type": operationType,
			})

			event := models.ChangeEvent{
				OperationType: operationType,
				Database:      dbName,
				Collection:    collName,
				Timestamp:     time.Now(),
			}

			// Handle invalidate events (DDL operations, collection drops/renames)
			if operationType == "invalidate" {
				event.IsInvalidate = true
				
				// Determine invalidate reason from the change document
				if reason, ok := changeDoc["invalidateReason"]; ok {
					if reasonStr, ok := reason.(string); ok {
						event.InvalidateReason = reasonStr
					}
				} else {
					// Infer reason from context - could be collection drop, rename, or database drop
					event.InvalidateReason = "collection_invalidated"
				}
				
				log.Printf("INVALIDATE EVENT detected for %s.%s - Reason: %s", dbName, collName, event.InvalidateReason)
				appLogger.Warn("cloud-sync", "invalidate_event", fmt.Sprintf("INVALIDATE EVENT detected for %s.%s - Reason: %s", dbName, collName, event.InvalidateReason), map[string]interface{}{
					"database": dbName,
					"collection": collName,
					"invalidate_reason": event.InvalidateReason,
				})
				
				// For invalidate events, we need to trigger re-bootstrap
				// This will be handled by the VM client when it receives the invalidate event
			}

		// Capture resume token for resumable synchronization
		if resumeToken, ok := changeDoc["_id"]; ok {
			if tokenData, err := bson.Marshal(resumeToken); err == nil {
				event.ResumeToken = bson.Raw(tokenData)
			}
		}

		// Capture cluster time if available
		if clusterTime, ok := changeDoc["clusterTime"]; ok {
			if timeData, err := bson.Marshal(clusterTime); err == nil {
				event.ClusterTime = bson.Raw(timeData)
			}
		}

		// Preserve BSON types for DocumentKey and FullDocument
		if documentKey, ok := changeDoc["documentKey"]; ok {
			if keyData, err := bson.Marshal(documentKey); err == nil {
				event.DocumentKey = bson.Raw(keyData)
			}
		}

		if fullDocument, ok := changeDoc["fullDocument"]; ok {
			if docData, err := bson.Marshal(fullDocument); err == nil {
				event.FullDocument = bson.Raw(docData)
			}
		}

		// Generate sequence numbers for exactly-once delivery
		if sequenceGen != nil && sequenceGen.IsEnabled() {
			seqID, err := sequenceGen.NextSequence()
			if err != nil {
				log.Printf("Failed to generate sequence for event: %v", err)
			} else {
				event.SequenceID = seqID
				// Generate batch ID and event ID based on sequence and timestamp
				batchInfo := sequenceGen.GetBatchInfo()
				event.BatchID = fmt.Sprintf("%s-%d", batchInfo.NodeID, batchInfo.Current/batchInfo.BatchSize)
				event.EventID = fmt.Sprintf("%s-%d-%d", batchInfo.NodeID, seqID, event.Timestamp.UnixNano())
				log.Printf("Assigned sequence %d (batch: %s, event: %s) to %s.%s", seqID, event.BatchID, event.EventID, dbName, collName)
			}
		}

		// Record metrics for the event
		if metricsCollector != nil {
			// Calculate event size (approximate)
			eventSize := int64(len(event.FullDocument) + len(event.DocumentKey) + 100) // Base overhead
			metricsCollector.RecordEvent(dbName, collName, eventSize)

			// Calculate replication lag (time between event creation and processing)
			processingTime := time.Now()
			replicationLag := processingTime.Sub(event.Timestamp)
			processingLag := time.Duration(0) // Processing is immediate in this context
			metricsCollector.RecordLag(dbName, collName, replicationLag, processingLag)
		}

		// Update checkpoint with resume token
		if checkpointMgr != nil && len(event.ResumeToken) > 0 {
			if err := checkpointMgr.UpdateCheckpoint(dbName, collName, event.ResumeToken, event.Timestamp); err != nil {
				log.Printf("Failed to update checkpoint for %s.%s: %v", dbName, collName, err)
				appLogger.Error("cloud-sync", "checkpoint_error", fmt.Sprintf("Failed to update checkpoint for %s.%s: %v", dbName, collName, err), map[string]interface{}{
					"database": dbName,
					"collection": collName,
					"error": err.Error(),
				})
			}
		}

		// Send event through internal cluster if enabled, otherwise use broadcast channel
		if config.InternalCluster.Enabled && internalCluster != nil {
			if !internalCluster.ProcessEvent(&event) {
				log.Println("Internal cluster queue full, dropping event")
				appLogger.Warn("cloud-sync", "queue_full", fmt.Sprintf("Internal cluster queue full, dropping event for %s.%s", dbName, collName), map[string]interface{}{
					"database": dbName,
					"collection": collName,
					"operation_type": event.OperationType,
				})
			}
		} else {
			select {
			case broadcast <- event:
			default:
				log.Println("Broadcast channel full, dropping event")
				appLogger.Warn("cloud-sync", "broadcast_full", fmt.Sprintf("Broadcast channel full, dropping event for %s.%s", dbName, collName), map[string]interface{}{
					"database": dbName,
					"collection": collName,
					"operation_type": event.OperationType,
				})
			}
		}
		} else {
			log.Printf("changeStream.Next() returned false for %s.%s", dbName, collName)
			break
		}
	}

	if err := changeStream.Err(); err != nil {
		log.Printf("Change stream error for %s.%s: %v", dbName, collName, err)
		appLogger.Error("cloud-sync", "stream_error", fmt.Sprintf("Change stream error for %s.%s: %v", dbName, collName, err), map[string]interface{}{
			"database": dbName,
			"collection": collName,
			"error": err.Error(),
		})
		if metricsCollector != nil {
			metricsCollector.RecordError(dbName, collName, "change_stream_error", err.Error(), nil)
		}
		return err
	}

	return nil
}

func handleDataRequest(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	var req models.DataRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid request body", http.StatusBadRequest)
		return
	}

	// Extract client ID from request headers or generate one
	clientID := r.Header.Get("X-Client-ID")
	if clientID == "" {
		clientID = fmt.Sprintf("client_%d", time.Now().Unix())
		log.Printf("No client ID provided, generated: %s", clientID)
	}

	// Capture cluster time fence for snapshot consistency if enabled
	var snapshotFence *fence.SnapshotFence
	if clusterFence != nil && clusterFence.IsEnabled() {
		var err error
		snapshotFence, err = clusterFence.CaptureSnapshotFence(context.Background(), req.Database)
		if err != nil {
			log.Printf("Warning: Failed to capture snapshot fence: %v", err)
			// Continue without fence - degraded consistency
		} else {
			log.Printf("Captured snapshot fence for %s.%s - ClusterTime: %v", req.Database, req.Collection, snapshotFence.ClusterTime)
		}
	}

	// Find matching collection in config
	var collConfig *models.CollectionConfig
	for _, database := range config.MongoDB.Databases {
		if database.Name == req.Database && database.Enabled {
			for _, collection := range database.Collections {
				if collection.Name == req.Collection && collection.Enabled {
					collConfig = &collection
					break
				}
			}
			break
		}
	}

	if collConfig == nil {
		response := models.DataResponse{
			Database:   req.Database,
			Collection: req.Collection,
			Error:      "Collection not authorized for sync",
		}
		json.NewEncoder(w).Encode(response)
		return
	}

	coll := mongoClient.Database(req.Database).Collection(req.Collection)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if req.CountOnly {
		filter := bson.M{}
		if len(collConfig.DocumentFilter.Criteria) > 0 {
			docFilterPipeline := filterEngine.BuildDocumentFilterPipeline(&collConfig.DocumentFilter)
			if len(docFilterPipeline) > 0 {
				// Extract the $match stage content for CountDocuments
				if matchStage, ok := docFilterPipeline[0]["$match"]; ok {
					filter = matchStage.(bson.M)
				}
			}
		}
		count, err := coll.CountDocuments(ctx, filter)
		if err != nil {
			response := models.DataResponse{
				Database:   req.Database,
				Collection: req.Collection,
				Error:      fmt.Sprintf("Failed to count documents: %v", err),
			}
			json.NewEncoder(w).Encode(response)
			return
		}

		response := models.DataResponse{
			Database:   req.Database,
			Collection: req.Collection,
			Count:      count,
		}
		json.NewEncoder(w).Encode(response)
		return
	}

	// Build aggregation pipeline with filters
	var pipeline []bson.M
	
	// Add partition filter if requested
	if req.PartitionIndex != nil {
		partitions, err := partitioner.CreatePartitions(ctx, req.Database, req.Collection)
		if err != nil {
			response := models.DataResponse{
				Database:   req.Database,
				Collection: req.Collection,
				Error:      fmt.Sprintf("Failed to create partitions: %v", err),
			}
			json.NewEncoder(w).Encode(response)
			return
		}
		
		if *req.PartitionIndex >= len(partitions) {
			response := models.DataResponse{
				Database:   req.Database,
				Collection: req.Collection,
				Error:      fmt.Sprintf("Invalid partition index %d, only %d partitions available", *req.PartitionIndex, len(partitions)),
			}
			json.NewEncoder(w).Encode(response)
			return
		}
		
		partition := partitions[*req.PartitionIndex]
		partitionFilter := partitioner.BuildPartitionFilter(partition)
		pipeline = append(pipeline, bson.M{"$match": partitionFilter})
	}
	
	// Add document filter pipeline
	if len(collConfig.DocumentFilter.Criteria) > 0 {
		docFilterPipeline := filterEngine.BuildDocumentFilterPipeline(&collConfig.DocumentFilter)
		pipeline = append(pipeline, docFilterPipeline...)
	}
	
	// Add field filter pipeline
	if len(collConfig.FieldFilter.IncludeFields) > 0 || len(collConfig.FieldFilter.ExcludeFields) > 0 {
		fieldFilterPipeline := filterEngine.BuildFieldFilterPipeline(&collConfig.FieldFilter)
		pipeline = append(pipeline, fieldFilterPipeline...)
	}

	var documents []bson.Raw
	var count int64
	var batchID string

	// Configure read options for snapshot consistency
	var aggregateOpts *options.AggregateOptions
	var findOpts *options.FindOptions
	if snapshotFence != nil {
		// Use snapshot fence for consistent reads
		readConcern := clusterFence.GetReadConcernForSnapshot(snapshotFence)
		readPref := clusterFence.GetReadPreferenceForSnapshot()
		
		aggregateOpts = options.Aggregate()
		findOpts = options.Find().SetProjection(bson.M{"_id": 1})
		
		// Apply read concern and preference to the database/collection level
		if readConcern != nil {
			coll = coll.Database().Collection(req.Collection, options.Collection().SetReadConcern(readConcern))
		}
		if readPref != nil {
			coll = coll.Database().Collection(req.Collection, options.Collection().SetReadPreference(readPref))
		}
		
		log.Printf("Using snapshot fence for consistent reads - ClusterTime: %v", snapshotFence.ClusterTime)
	} else {
		aggregateOpts = options.Aggregate()
		findOpts = options.Find().SetProjection(bson.M{"_id": 1})
	}



	// Start transfer batch if tracking is enabled
	if transferTracker.IsEnabled() {
		// First, get all document IDs to check what needs to be transferred
		var allDocumentIDs []primitive.ObjectID
		if len(pipeline) > 0 {
			// Add projection to get only _id field for initial check
			idPipeline := append(pipeline, bson.M{"$project": bson.M{"_id": 1}})
			cursor, err := coll.Aggregate(ctx, idPipeline, aggregateOpts)
			if err != nil {
				response := models.DataResponse{
					Database:   req.Database,
					Collection: req.Collection,
					Error:      fmt.Sprintf("Failed to query document IDs: %v", err),
				}
				json.NewEncoder(w).Encode(response)
				return
			}
			defer cursor.Close(ctx)

			for cursor.Next(ctx) {
				var doc bson.M
				if err := cursor.Decode(&doc); err != nil {
					continue
				}
				if id, ok := doc["_id"].(primitive.ObjectID); ok {
					allDocumentIDs = append(allDocumentIDs, id)
				}
			}
		} else {
			cursor, err := coll.Find(ctx, bson.M{}, findOpts)
			if err != nil {
				response := models.DataResponse{
					Database:   req.Database,
					Collection: req.Collection,
					Error:      fmt.Sprintf("Failed to query document IDs: %v", err),
				}
				json.NewEncoder(w).Encode(response)
				return
			}
			defer cursor.Close(ctx)

			for cursor.Next(ctx) {
				var doc bson.M
				if err := cursor.Decode(&doc); err != nil {
					continue
				}
				if id, ok := doc["_id"].(primitive.ObjectID); ok {
					allDocumentIDs = append(allDocumentIDs, id)
				}
			}
		}

		// Get untransferred document IDs
		untransferredIDs, err := transferTracker.GetUntransferredDocuments(clientID, req.Database, req.Collection, allDocumentIDs)
		if err != nil {
			log.Printf("Error checking transferred documents: %v", err)
			// Continue without tracking if there's an error
			untransferredIDs = allDocumentIDs
		}

		log.Printf("Total documents: %d, Untransferred: %d for client %s", len(allDocumentIDs), len(untransferredIDs), clientID)

		// Start transfer batch
		batchID, err = transferTracker.StartTransferBatch(clientID, req.Database, req.Collection, len(untransferredIDs))
		if err != nil {
			log.Printf("Error starting transfer batch: %v", err)
		}

		// Query only untransferred documents
		if len(untransferredIDs) == 0 {
			// No new documents to transfer
			documents = []bson.Raw{}
			count = 0
		} else {
			// Add filter for untransferred documents
			idFilter := bson.M{"_id": bson.M{"$in": untransferredIDs}}
			if len(pipeline) > 0 {
				// Insert the ID filter at the beginning
				filteredPipeline := append([]bson.M{{"$match": idFilter}}, pipeline...)
				cursor, err := coll.Aggregate(ctx, filteredPipeline)
				if err != nil {
					response := models.DataResponse{
						Database:   req.Database,
						Collection: req.Collection,
						Error:      fmt.Sprintf("Failed to query documents: %v", err),
					}
					json.NewEncoder(w).Encode(response)
					return
				}
				defer cursor.Close(ctx)

				for cursor.Next(ctx) {
					rawDoc := cursor.Current
					documents = append(documents, rawDoc)

					// Record transfer
					var doc bson.M
					if err := bson.Unmarshal(rawDoc, &doc); err == nil {
						if id, ok := doc["_id"].(primitive.ObjectID); ok {
							if err := transferTracker.RecordTransfer(clientID, req.Database, req.Collection, id, batchID); err != nil {
								log.Printf("Error recording transfer: %v", err)
							}
						}
					}
				}
			} else {
				cursor, err := coll.Find(ctx, idFilter)
				if err != nil {
					response := models.DataResponse{
						Database:   req.Database,
						Collection: req.Collection,
						Error:      fmt.Sprintf("Failed to query documents: %v", err),
					}
					json.NewEncoder(w).Encode(response)
					return
				}
				defer cursor.Close(ctx)

				for cursor.Next(ctx) {
					rawDoc := cursor.Current
					documents = append(documents, rawDoc)

					// Record transfer
					var doc bson.M
					if err := bson.Unmarshal(rawDoc, &doc); err == nil {
						if id, ok := doc["_id"].(primitive.ObjectID); ok {
							if err := transferTracker.RecordTransfer(clientID, req.Database, req.Collection, id, batchID); err != nil {
								log.Printf("Error recording transfer: %v", err)
							}
						}
					}
				}
			}
			count = int64(len(documents))
		}
	} else {
		// Original logic when tracking is disabled
		if len(pipeline) > 0 {
			cursor, err := coll.Aggregate(ctx, pipeline)
			if err != nil {
				response := models.DataResponse{
					Database:   req.Database,
					Collection: req.Collection,
					Error:      fmt.Sprintf("Failed to query documents: %v", err),
				}
				json.NewEncoder(w).Encode(response)
				return
			}
			defer cursor.Close(ctx)

			for cursor.Next(ctx) {
				// Get raw BSON data to preserve MongoDB types
				rawDoc := cursor.Current
				documents = append(documents, rawDoc)
			}
		} else {
			cursor, err := coll.Find(ctx, bson.M{})
			if err != nil {
				response := models.DataResponse{
					Database:   req.Database,
					Collection: req.Collection,
					Error:      fmt.Sprintf("Failed to query documents: %v", err),
				}
				json.NewEncoder(w).Encode(response)
				return
			}
			defer cursor.Close(ctx)

			for cursor.Next(ctx) {
				// Get raw BSON data to preserve MongoDB types
				rawDoc := cursor.Current
				documents = append(documents, rawDoc)
			}
		}
		count = int64(len(documents))
	}

	response := models.DataResponse{
		Database:   req.Database,
		Collection: req.Collection,
		Documents:  documents,
		Count:      count,
	}

	// Collect indexes and collection metadata
	indexes, err := collectIndexes(ctx, coll)
	if err != nil {
		log.Printf("Warning: Failed to collect indexes for %s.%s: %v", req.Database, req.Collection, err)
	} else {
		response.Indexes = indexes
		log.Printf("Collected %d indexes for %s.%s", len(indexes), req.Database, req.Collection)
	}

	collectionOptions, err := collectCollectionOptions(ctx, mongoClient.Database(req.Database), req.Collection)
	if err != nil {
		log.Printf("Warning: Failed to collect collection options for %s.%s: %v", req.Database, req.Collection, err)
	} else {
		response.CollectionOptions = collectionOptions
		log.Printf("Collected collection options for %s.%s", req.Database, req.Collection)
	}

	// Include snapshot fence information if available
	if snapshotFence != nil {
		response.SnapshotFence = &models.SnapshotFenceInfo{
			ClusterTime:   snapshotFence.ClusterTime,
			OperationTime: snapshotFence.OperationTime,
			CapturedAt:    snapshotFence.CapturedAt,
		}
		log.Printf("Including snapshot fence in response for %s.%s - ClusterTime: %v", req.Database, req.Collection, snapshotFence.ClusterTime)
	}

	// Record metrics for data transfer
	if metricsCollector != nil {
		// Calculate total bytes transferred
		var totalBytes int64
		for _, doc := range documents {
			totalBytes += int64(len(doc))
		}
		// Record batch transfer metrics
		metricsCollector.RecordBatch(req.Database, req.Collection, count)
		// Record event for each document with average size
		if count > 0 {
			avgSize := totalBytes / count
			for i := int64(0); i < count; i++ {
				metricsCollector.RecordEvent(req.Database, req.Collection, avgSize)
			}
		}
	}

	// Complete transfer batch if tracking is enabled
	if transferTracker.IsEnabled() && batchID != "" {
		if err := transferTracker.CompleteTransferBatch(batchID); err != nil {
			log.Printf("Error completing transfer batch: %v", err)
		}
		
		// Update client sync state
		var lastDocID *primitive.ObjectID
		if len(documents) > 0 {
			var lastDoc bson.M
			if err := bson.Unmarshal(documents[len(documents)-1], &lastDoc); err == nil {
				if id, ok := lastDoc["_id"].(primitive.ObjectID); ok {
					lastDocID = &id
				}
			}
		}
		
		if err := transferTracker.UpdateClientSyncState(clientID, req.Database, req.Collection, lastDocID, count, true); err != nil {
			log.Printf("Error updating client sync state: %v", err)
		}
		
		log.Printf("Transfer completed for client %s: %d documents transferred", clientID, count)
	}

	// Encrypt response if encryption is enabled
	if encryptionMgr.IsEnabled() {
		w.Header().Set("Content-Type", "application/octet-stream")
		w.Header().Set("X-Encryption-Enabled", "true")
		w.Header().Set("X-Encryption-KeyID", encryptionMgr.GetKeyID())
		
		encryptedData, err := encryptionMgr.EncryptJSON(response)
		if err != nil {
			log.Printf("Error encrypting response: %v", err)
			http.Error(w, "Internal server error", http.StatusInternalServerError)
			// Mark batch as failed if tracking is enabled
			if transferTracker.IsEnabled() && batchID != "" {
				transferTracker.FailTransferBatch(batchID, fmt.Sprintf("Encryption error: %v", err))
			}
			return
		}
		w.Write(encryptedData)
	} else {
		json.NewEncoder(w).Encode(response)
	}
}

func handlePartitionsRequest(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	var req models.DataRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid request body", http.StatusBadRequest)
		return
	}

	// Find matching collection configuration
	var collConfig *models.CollectionConfig
	for _, dbConfig := range config.MongoDB.Databases {
		if dbConfig.Name == req.Database {
			for _, cc := range dbConfig.Collections {
				if cc.Name == req.Collection {
					collConfig = &cc
					break
				}
			}
			break
		}
	}

	if collConfig == nil {
		http.Error(w, "Collection not found in configuration", http.StatusNotFound)
		return
	}

	// Create partitions using the partitioner
	partitions, err := partitioner.CreatePartitions(context.Background(), req.Database, req.Collection)
	if err != nil {
		log.Printf("Error creating partitions for %s.%s: %v", req.Database, req.Collection, err)
		http.Error(w, "Failed to create partitions", http.StatusInternalServerError)
		return
	}

	// Convert to model partitions
	modelPartitions := make([]*models.PartitionInfo, len(partitions))
	for i, p := range partitions {
		modelPartitions[i] = &models.PartitionInfo{
			PartitionIndex:  p.ID,
			TotalPartitions: len(partitions),
			MinID:           p.MinID,
			MaxID:           p.MaxID,
			IsFirst:         p.IsFirst,
			IsLast:          p.IsLast,
			EstCount:        p.EstCount,
		}
	}

	response := models.DataResponse{
		Database:   req.Database,
		Collection: req.Collection,
		Partitions: modelPartitions,
	}

	json.NewEncoder(w).Encode(response)
}

// collectIndexes retrieves all indexes for a collection
func collectIndexes(ctx context.Context, coll *mongo.Collection) ([]models.IndexInfo, error) {
	indexView := coll.Indexes()
	cursor, err := indexView.List(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to list indexes: %w", err)
	}
	defer cursor.Close(ctx)

	var indexes []models.IndexInfo
	for cursor.Next(ctx) {
		var indexDoc bson.M
		if err := cursor.Decode(&indexDoc); err != nil {
			continue // Skip malformed index documents
		}

		// Extract index information
		indexInfo := models.IndexInfo{}
		
		if name, ok := indexDoc["name"].(string); ok {
			indexInfo.Name = name
		}
		
		if key, ok := indexDoc["key"]; ok {
			keyBytes, _ := bson.Marshal(key)
			indexInfo.Keys = bson.Raw(keyBytes)
		}
		
		if unique, ok := indexDoc["unique"].(bool); ok {
			indexInfo.Unique = unique
		}
		
		if sparse, ok := indexDoc["sparse"].(bool); ok {
			indexInfo.Sparse = sparse
		}
		
		if background, ok := indexDoc["background"].(bool); ok {
			indexInfo.Background = background
		}
		
		if ttl, ok := indexDoc["expireAfterSeconds"]; ok {
			if ttlInt, ok := ttl.(int32); ok {
				indexInfo.TTL = &ttlInt
			} else if ttlInt64, ok := ttl.(int64); ok {
				ttlInt32 := int32(ttlInt64)
				indexInfo.TTL = &ttlInt32
			}
		}
		
		if partialFilter, ok := indexDoc["partialFilterExpression"]; ok {
			partialBytes, _ := bson.Marshal(partialFilter)
			indexInfo.PartialFilterExpression = bson.Raw(partialBytes)
		}
		
		if collation, ok := indexDoc["collation"]; ok {
			collationBytes, _ := bson.Marshal(collation)
			indexInfo.Collation = bson.Raw(collationBytes)
		}
		
		// Store any additional options
		optionsMap := make(bson.M)
		for key, value := range indexDoc {
			switch key {
			case "name", "key", "unique", "sparse", "background", "expireAfterSeconds", "partialFilterExpression", "collation":
				// Skip already processed fields
			default:
				optionsMap[key] = value
			}
		}
		if len(optionsMap) > 0 {
			optionsBytes, _ := bson.Marshal(optionsMap)
			indexInfo.Options = bson.Raw(optionsBytes)
		}
		
		indexes = append(indexes, indexInfo)
	}

	return indexes, nil
}

// collectCollectionOptions retrieves collection-level options and metadata
func collectCollectionOptions(ctx context.Context, db *mongo.Database, collectionName string) (*models.CollectionOptions, error) {
	// Get collection info using listCollections command
	filter := bson.M{"name": collectionName}
	cursor, err := db.ListCollections(ctx, filter)
	if err != nil {
		return nil, fmt.Errorf("failed to list collections: %w", err)
	}
	defer cursor.Close(ctx)

	if !cursor.Next(ctx) {
		return nil, fmt.Errorf("collection %s not found", collectionName)
	}

	var collInfo bson.M
	if err := cursor.Decode(&collInfo); err != nil {
		return nil, fmt.Errorf("failed to decode collection info: %w", err)
	}

	collOptions := &models.CollectionOptions{}

	// Extract options from collection info
	if options, ok := collInfo["options"].(bson.M); ok {
		if capped, ok := options["capped"].(bool); ok {
			collOptions.Capped = capped
		}
		
		if size, ok := options["size"]; ok {
			if sizeInt64, ok := size.(int64); ok {
				collOptions.Size = &sizeInt64
			}
		}
		
		if max, ok := options["max"]; ok {
			if maxInt64, ok := max.(int64); ok {
				collOptions.Max = &maxInt64
			}
		}
		
		if validator, ok := options["validator"]; ok {
			validatorBytes, _ := bson.Marshal(validator)
			collOptions.Validator = bson.Raw(validatorBytes)
		}
		
		if validationLevel, ok := options["validationLevel"].(string); ok {
			collOptions.ValidationLevel = validationLevel
		}
		
		if validationAction, ok := options["validationAction"].(string); ok {
			collOptions.ValidationAction = validationAction
		}
		
		if collation, ok := options["collation"]; ok {
			collationBytes, _ := bson.Marshal(collation)
			collOptions.Collation = bson.Raw(collationBytes)
		}
		
		if changeStreamPreAndPostImages, ok := options["changeStreamPreAndPostImages"]; ok {
			imagesBytes, _ := bson.Marshal(changeStreamPreAndPostImages)
			collOptions.ChangeStreamPreAndPostImages = bson.Raw(imagesBytes)
		}
	}

	return collOptions, nil
}

func handleHealth(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	
	// Test MongoDB connection
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	
	sourceMongoStatus := "connected"
	if err := mongoClient.Ping(ctx, nil); err != nil {
		sourceMongoStatus = "disconnected"
	}
	
	// Check VM sync connection status based on connected VM sync clients only
	vmSyncStatus := "disconnected"
	vmSyncClients := 0
	for _, clientInfo := range clients {
		if clientInfo.ClientType == "vm-sync" {
			vmSyncClients++
		}
	}
	if vmSyncClients > 0 {
		vmSyncStatus = "connected"
	}
	
	// For now, we'll assume cloud_sync is connected since the service is running
	// In a real implementation, you'd check actual service health
	response := map[string]interface{}{
		"source_mongo": sourceMongoStatus,
		"cloud_sync":   "connected",
		"vm_sync":      vmSyncStatus,
		"target_mongo": "connected", // Mock status - would check target MongoDB
		"timestamp":    time.Now(),
		"vm_sync_info": map[string]interface{}{
			"connected_clients": vmSyncClients,
			"server_address":    fmt.Sprintf("http://%s:%d", config.Server.Host, config.Server.Port),
			"websocket_endpoint": config.WebSocket.Endpoint,
			"data_api_endpoint":  "/api/data",
		},
	}
	
	json.NewEncoder(w).Encode(response)
}

func handleDashboard(w http.ResponseWriter, r *http.Request) {
	http.ServeFile(w, r, "./web/dashboard.html")
}

func handleLogs(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	
	// Parse query parameters
	page := 1
	limit := 50
	stage := r.URL.Query().Get("stage")
	status := r.URL.Query().Get("status")
	search := r.URL.Query().Get("search")
	
	if p := r.URL.Query().Get("page"); p != "" {
		if parsed, err := strconv.Atoi(p); err == nil && parsed > 0 {
			page = parsed
		}
	}
	
	if l := r.URL.Query().Get("limit"); l != "" {
		if parsed, err := strconv.Atoi(l); err == nil && parsed > 0 && parsed <= 100 {
			limit = parsed
		}
	}
	
	// Get logs from the application logger
	logs, total := appLogger.GetLogs(stage, status, search, page, limit)

	// Convert to the expected format for the API
	paginatedLogs := make([]map[string]interface{}, len(logs))
	for i, logEntry := range logs {
		paginatedLogs[i] = map[string]interface{}{
			"timestamp": logEntry.Timestamp,
			"stage":     logEntry.Stage,
			"action":    logEntry.Action,
			"status":    logEntry.Status,
			"message":   logEntry.Message,
			"level":     logEntry.Level,
			"details":   logEntry.Details,
		}
	}

	totalPages := (total + limit - 1) / limit
	
	response := map[string]interface{}{
		"logs": paginatedLogs,
		"total": total,
		"page": page,
		"total_pages": totalPages,
	}
	
	json.NewEncoder(w).Encode(response)
}

func handleChartData(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	
	period := r.URL.Query().Get("period")
	if period == "" {
		period = "1h"
	}
	
	// Generate mock time series data
	now := time.Now()
	var duration time.Duration
	var interval time.Duration
	
	switch period {
	case "1h":
		duration = time.Hour
		interval = time.Minute * 5
	case "24h":
		duration = time.Hour * 24
		interval = time.Hour
	default:
		duration = time.Hour
		interval = time.Minute * 5
	}
	
	throughputData := []map[string]interface{}{}
	lagData := []map[string]interface{}{}
	errorData := []map[string]interface{}{}
	
	for i := duration; i >= 0; i -= interval {
		timestamp := now.Add(-i)
		
		throughputData = append(throughputData, map[string]interface{}{
			"x": timestamp.Format(time.RFC3339),
			"y": 100 + (i.Minutes() * 2) + (float64(timestamp.Unix()%10) * 10),
		})
		
		lagData = append(lagData, map[string]interface{}{
			"x": timestamp.Format(time.RFC3339),
			"y": 50 + (float64(timestamp.Unix()%20) * 5),
		})
		
		errorData = append(errorData, map[string]interface{}{
			"x": timestamp.Format(time.RFC3339),
			"y": timestamp.Unix() % 5,
		})
	}
	
	response := map[string]interface{}{
		"throughput": throughputData,
		"lag": lagData,
		"errors": errorData,
	}
	
	json.NewEncoder(w).Encode(response)
}

func handleControl(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	
	vars := mux.Vars(r)
	action := vars["action"]
	
	switch action {
	case "restart":
		log.Println("Restart action triggered")
		appLogger.Info("dashboard", "restart_sync", "Sync process restart initiated by dashboard", map[string]interface{}{
			"action": "restart",
			"initiated_by": "dashboard",
		})
		
		// Signal restart to monitoring goroutines
		select {
		case restartChan <- true:
			log.Println("Restart signal sent successfully")
		default:
			log.Println("Restart signal already pending")
		}
		
		json.NewEncoder(w).Encode(map[string]interface{}{
			"success": true,
			"message": "Sync process restart initiated",
			"timestamp": time.Now().Format(time.RFC3339),
		})
		
	case "pause":
		log.Println("Pause action triggered")
		appLogger.Info("dashboard", "pause_sync", "Sync process paused by dashboard", map[string]interface{}{
			"action": "pause",
			"initiated_by": "dashboard",
		})
		
		// Set pause state
		syncPausedMutex.Lock()
		syncPaused = true
		syncPausedMutex.Unlock()
		
		json.NewEncoder(w).Encode(map[string]interface{}{
			"success": true,
			"message": "Sync process paused",
			"timestamp": time.Now().Format(time.RFC3339),
		})
		
	case "resume":
		log.Println("Resume action triggered")
		appLogger.Info("dashboard", "resume_sync", "Sync process resumed by dashboard", map[string]interface{}{
			"action": "resume",
			"initiated_by": "dashboard",
		})
		
		// Clear pause state
		syncPausedMutex.Lock()
		syncPaused = false
		syncPausedMutex.Unlock()
		
		json.NewEncoder(w).Encode(map[string]interface{}{
			"success": true,
			"message": "Sync process resumed",
			"timestamp": time.Now().Format(time.RFC3339),
		})
		
	default:
		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(map[string]interface{}{
			"success": false,
			"message": "Invalid action. Supported actions: restart, pause, resume",
		})
	}
}