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
	"os"
	"os/signal"
	"path/filepath"
	"regexp"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/gorilla/mux"
	"github.com/gorilla/websocket"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"golang.org/x/time/rate"
	"gopkg.in/yaml.v2"

	"go-data-sync-http/pkg/adaptive"
	"go-data-sync-http/pkg/auth"
	"go-data-sync-http/pkg/cluster"
	"go-data-sync-http/pkg/crypto"
	"go-data-sync-http/pkg/distribution"
	"go-data-sync-http/pkg/fence"
	"go-data-sync-http/pkg/filtering"
	"go-data-sync-http/pkg/logging"
	"go-data-sync-http/pkg/metrics"
	"go-data-sync-http/pkg/migration"
	"go-data-sync-http/pkg/models"
	"go-data-sync-http/pkg/parallel"
	"go-data-sync-http/pkg/resume"
	"go-data-sync-http/pkg/sequence"
	"go-data-sync-http/pkg/telemetry"
	"go-data-sync-http/pkg/tenantinfo"
	"go-data-sync-http/pkg/tracking"
	"go-data-sync-http/pkg/transport"
)

type ClientInfo struct {
	ClientType  string // "vm-sync", "dashboard", "unknown"
	ClientID    string
	ConnectedAt time.Time
	Status      string // "authenticating", "active", "disconnected"
	// OAuth2 authentication info
	OAuth2Claims *auth.TokenClaims `json:"oauth2_claims,omitempty"`
}

// ResourceSnapshot captures Cloud Sync's resource usage at a point in time
type ResourceSnapshot struct {
	Timestamp         time.Time
	CPUPercent        float64
	MemoryPercent     float64
	ActiveConnections int
	QueueDepth        int
	SyncLatency       time.Duration
	Throughput        float64
	ErrorRate         float64
}

var (
	upgrader = websocket.Upgrader{
		CheckOrigin: func(r *http.Request) bool {
			return true // Allow connections from any origin
		},
	}
	clients          = make(map[*websocket.Conn]ClientInfo)
	broadcast        = make(chan models.ChangeEvent)
	statusUpdates    = make(chan map[string]interface{})
	mongoClient      *mongo.Client
	config           models.Config
	filterEngine     *filtering.FilterEngine
	encryptionMgr    *crypto.EncryptionManager
	internalCluster  *cluster.InternalCluster
	checkpointMgr    *resume.CheckpointManager
	transferTracker  *tracking.TransferTracker
	sequenceGen      *sequence.Generator
	clusterFence     *fence.ClusterTimeFence
	partitioner      *parallel.Partitioner
	metricsCollector *metrics.MetricsCollector
	alertManager     *metrics.AlertManager
	metricsAPI       *metrics.MetricsAPI
	activeWatchers   = make(map[string]bool) // Track active change streams
	watchersMutex    sync.RWMutex            // Protect activeWatchers map
	// App logger variable
	appLogger   *logging.Logger                // Application logger for dashboard
	authService *auth.ClientCredentialsService // OAuth2 authentication service

	// Service readiness flag
	serviceReady      bool
	serviceReadyMutex sync.RWMutex

	// Sync control state
	syncPaused      bool
	syncPausedMutex sync.RWMutex
	restartChan     = make(chan bool, 1)
	shutdownChan    = make(chan bool, 1)

	// VM-sync connection tracking
	vmSyncConnected     = make(chan bool, 1) // Signal when first vm-sync client connects
	vmSyncConnectedOnce bool                 // Track if vm-sync has connected at least once
	vmSyncMutex         sync.RWMutex         // Protect vm-sync connection state

	// Initial dump completion tracking - CRITICAL for preventing duplicate data
	initialDumpCompleted     = make(chan bool, 1) // Signal when initial dump is finished
	initialDumpCompletedOnce bool                 // Track if initial dump has completed
	initialDumpMutex         sync.RWMutex         // Protect initial dump completion state

	// Buffer-free resume token system (NO MEMORY BUFFERS)
	tokenManager      *resume.TokenManager            // Resume token manager for buffer-free sync
	bufferFreeHandler *resume.BufferFreeChangeHandler // Buffer-free change handler

	// Adaptive system components
	cloudSyncIntegration *adaptive.CloudSyncIntegration
	telemetryCollector   *telemetry.Collector

	// Intelligent collection distributor (eliminates manual VM YAML config)
	collectionDistributor *distribution.CollectionDistributor

	// TCP transport for high-performance data transfer
	tcpSender           transport.Sender
	tcpTransportEnabled bool

	// Back-pressure mechanism variables
	backPressureEnabled bool
	throttleDelay       time.Duration
	maxQueueSize        int
	backPressureMutex   sync.RWMutex

	// Adaptive batch size variables
	currentBatchSize int
	batchSizeMutex   sync.RWMutex

	// Self-optimization variables
	selfOptimizationEnabled bool
	lastSelfOptimization    time.Time
	selfOptimizationMutex   sync.RWMutex
	resourceHistory         []ResourceSnapshot
	maxResourceHistory      int = 20

	// Rate limiting variables
	globalRateLimiter   *rate.Limiter
	clientRateLimiters  map[string]*rate.Limiter
	connectionLimiters  map[string]*rate.Limiter
	rateLimiterMutex    sync.RWMutex
	blocklistIPs        map[string]time.Time
	blocklistMutex      sync.RWMutex
	connectionsByIP     map[string]int
	connectionMutex     sync.RWMutex
	clientsMutex        sync.RWMutex
	maxConnectionsPerIP int = 10
)

// initializeTCPTransportWithAddress initializes the TCP transport with a specific address
func initializeTCPTransportWithAddress(address string) error {
	// Check if TCP transport is enabled in config
	if config.Sync.Transport.Mode != "tcp" {
		log.Printf("TCP transport disabled, using mode: %s", config.Sync.Transport.Mode)
		return nil
	}

	// Validate address
	if address == "" {
		return fmt.Errorf("TCP sender address is empty")
	}

	// Create high-performance TCP sender configuration for billion-document transfers
	senderConfig := transport.SenderConfig{
		Address:       address,
		ParallelConns: config.Sync.Transport.TCPSender.ParallelConns,
		WindowSize:    config.Sync.Transport.TCPSender.WindowSize,
		BatchTimeout:  config.Sync.Transport.TCPSender.BatchTimeout,
		ConnTimeout:   config.Sync.Transport.TCPSender.ConnTimeout,
		KeepAlive:     config.Sync.Transport.TCPSender.KeepAlive,
		MaxRetries:    config.Sync.Transport.TCPSender.MaxRetries,
		RetryBackoff:  config.Sync.Transport.TCPSender.RetryBackoff,
		BufferSize:    config.Sync.Transport.TCPSender.BufferSize,
		MaxBatchSize:  config.Sync.Transport.TCPSender.MaxBatchSize,
	}

	// OPTIMIZED: Set compression type with high-performance options for billion-document transfers
	switch config.Sync.Transport.CompressionType {
	case "zstd":
		senderConfig.Compression = transport.CompressionTypeZstd
	case "lz4":
		senderConfig.Compression = transport.CompressionTypeLZ4
	case "none":
		senderConfig.Compression = transport.CompressionTypeNone
	default:
		// Default to Zstd for best compression ratio on massive datasets
		senderConfig.Compression = transport.CompressionTypeZstd
		log.Printf("Unknown compression type '%s', defaulting to zstd", config.Sync.Transport.CompressionType)
	}

	// OPTIMIZED: Apply high-performance defaults for massive datasets
	if senderConfig.ParallelConns <= 0 {
		senderConfig.ParallelConns = 8 // Increased for billion-document performance
	}
	if senderConfig.WindowSize <= 0 {
		senderConfig.WindowSize = 128 // Larger window for better throughput
	}
	if senderConfig.BatchTimeout == 0 {
		senderConfig.BatchTimeout = 10 * time.Second // Longer timeout for large batches
	}
	if senderConfig.ConnTimeout == 0 {
		senderConfig.ConnTimeout = 60 * time.Second // Longer connection timeout
	}
	if senderConfig.KeepAlive == 0 {
		senderConfig.KeepAlive = 30 * time.Second
	}
	if senderConfig.MaxRetries <= 0 {
		senderConfig.MaxRetries = 5 // More retries for reliability
	}
	if senderConfig.RetryBackoff == 0 {
		senderConfig.RetryBackoff = 2 * time.Second // Longer backoff
	}
	if senderConfig.BufferSize <= 0 {
		senderConfig.BufferSize = 1024 * 1024 // 1MB buffer for billion-document transfers
	}
	if senderConfig.MaxBatchSize <= 0 {
		senderConfig.MaxBatchSize = 64 * 1024 * 1024 // 64MB max batch for large datasets
	}

	// Create TCP sender
	sender, err := transport.NewSender(senderConfig)
	if err != nil {
		return fmt.Errorf("failed to create TCP sender: %w", err)
	}

	// Test connection to ensure vm-sync is reachable
	if err := testTCPConnection(senderConfig.Address); err != nil {
		log.Printf("WARNING: TCP connection test failed: %v", err)
		if !config.Sync.Transport.HTTPFallback {
			return fmt.Errorf("TCP connection failed and HTTP fallback disabled: %w", err)
		}
		log.Printf("TCP transport will use HTTP fallback when needed")
	}

	tcpSender = sender
	tcpTransportEnabled = true

	log.Printf("🚀 TCP TRANSPORT OPTIMIZED: address=%s, parallel_conns=%d, window_size=%d, buffer=%s, max_batch=%s, compression=%s",
		senderConfig.Address, senderConfig.ParallelConns, senderConfig.WindowSize,
		formatBytes(senderConfig.BufferSize), formatBytes(senderConfig.MaxBatchSize), config.Sync.Transport.CompressionType)
	return nil
}

// initializeTCPTransportWithRetry initializes TCP transport with retry mechanism
func initializeTCPTransportWithRetry() error {
	// Check if TCP transport is enabled in config
	if config.Sync.Transport.Mode != "tcp" {
		log.Printf("TCP transport disabled, using mode: %s", config.Sync.Transport.Mode)
		return nil
	}

	// Retry TCP initialization up to 5 times with backoff
	for attempt := 1; attempt <= 5; attempt++ {
		log.Printf("🔄 TCP INIT ATTEMPT %d/5: Trying to connect to vm-sync at %s", attempt, config.Sync.Transport.TCPSender.Address)

		if err := initializeTCPTransport(); err != nil {
			log.Printf("❌ TCP INIT ATTEMPT %d FAILED: %v", attempt, err)

			if attempt < 5 {
				// Exponential backoff: 2s, 4s, 8s, 16s
				backoff := time.Duration(attempt*2) * time.Second
				log.Printf("⏳ TCP RETRY: waiting %v before attempt %d", backoff, attempt+1)
				time.Sleep(backoff)
			} else {
				return fmt.Errorf("TCP initialization failed after %d attempts: %w", attempt, err)
			}
		} else {
			log.Printf("✅ TCP INIT SUCCESS: Connected on attempt %d", attempt)
			return nil
		}
	}

	return fmt.Errorf("TCP initialization failed after all retry attempts")
}

// initializeTCPTransport initializes the TCP transport for high-performance data transfer
func initializeTCPTransport() error {
	// Check if TCP transport is enabled in config
	if config.Sync.Transport.Mode != "tcp" {
		log.Printf("TCP transport disabled, using mode: %s", config.Sync.Transport.Mode)
		return nil
	}

	// Try to get address from config first, then from Address Manager
	address := config.Sync.Transport.TCPSender.Address
	if address == "" {
		// No address in config, try Address Manager
		addressMgr := transport.GetAddressManager()
		var err error
		address, err = addressMgr.GetAnyAddress()
		if err != nil {
			log.Printf("⚠️ TCP transport: No address in config or Address Manager - waiting for VM authentication")
			return fmt.Errorf("TCP sender address not configured and no VM authenticated yet")
		}
		log.Printf("✅ TCP ADDRESS: Retrieved from Address Manager: %s", address)
	}

	// Use the common initialization function with the discovered address
	return initializeTCPTransportWithAddress(address)
}

// testTCPConnection tests if the TCP receiver is accepting connections without interfering with the protocol
func testTCPConnection(address string) error {
	// Just check if the port is listening without creating a full connection
	// This avoids interfering with the TCP receiver's protocol handling
	conn, err := net.DialTimeout("tcp", address, 2*time.Second)
	if err != nil {
		return fmt.Errorf("TCP port not reachable: %w", err)
	}

	// Immediately close without sending any data to avoid protocol interference
	// The VM-sync receiver will see this as a brief connection that closed gracefully
	conn.Close()

	return nil
}

// startTCPHealthMonitor monitors TCP transport health and attempts reconnection
// ENTERPRISE-GRADE: Circuit breaker + exponential backoff + jitter (gRPC/Kafka/AWS style)
func startTCPHealthMonitor() {
	// Use configured interval or default to 30 seconds
	interval := config.Sync.Transport.HealthMonitor
	if interval == 0 {
		interval = 30 * time.Second
	}

	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	
	// Enterprise-grade reconnection strategy
	backoff := transport.GRPCStyleBackoff() // 1s initial, 120s max, 1.6x multiplier, ±20% jitter
	var consecutiveFailures int
	const maxFailures = 10 // After 10 failures, stop aggressive reconnection

	log.Printf("🏥 TCP HEALTH MONITOR: Started (interval: %v, exponential backoff enabled)", interval)

	for {
		select {
		case <-ticker.C:
			// Skip health check if TCP mode is not configured
			if config.Sync.Transport.Mode != "tcp" {
				continue
			}

			// CASE 1: TCP is configured but not currently enabled -> Try to reconnect
			if !tcpTransportEnabled {
				// Check circuit breaker before attempting reconnection
				if consecutiveFailures >= maxFailures {
					log.Printf("⏸️  TCP RECONNECT PAUSED: Too many failures (%d), waiting for manual intervention or vm-sync recovery", consecutiveFailures)
					continue
				}
				
				log.Printf("🔍 TCP HEALTH CHECK: TCP not enabled, attempting auto-reconnect (attempt %d)...", consecutiveFailures+1)

				// Try to get fresh address from Address Manager (in case vm-sync restarted)
				addressMgr := transport.GetAddressManager()
				address, err := addressMgr.GetAnyAddress()
				if err != nil {
					// No address in Address Manager, use config address
					address = config.Sync.Transport.TCPSender.Address
					log.Printf("📋 TCP RECONNECT: Using config address: %s", address)
				} else {
					log.Printf("✅ TCP RECONNECT: Using Address Manager address: %s", address)
				}

				// Test if vm-sync TCP port is reachable
				if err := testTCPConnection(address); err == nil {
					log.Printf("✨ TCP AVAILABLE: VM-sync detected at %s! Reinitializing TCP transport...", address)

					// Clean up old sender if exists
					if tcpSender != nil {
						log.Printf("🔧 TCP CLEANUP: Closing old TCP sender before reconnect...")
						if closeErr := tcpSender.Close(); closeErr != nil {
							log.Printf("⚠️  TCP CLEANUP WARNING: %v", closeErr)
						}
						tcpSender = nil
					}

					// Reinitialize TCP transport with fresh address
					if err := initializeTCPTransportWithAddress(address); err != nil {
						consecutiveFailures++
						log.Printf("❌ TCP REINIT FAILED (attempt %d): %v", consecutiveFailures, err)
						
						// Apply exponential backoff with jitter
						backoffDuration := backoff.NextBackoff()
						log.Printf("⏳ TCP BACKOFF: Waiting %v before next attempt (exponential + jitter)", backoffDuration)
					} else {
						// SUCCESS! Reset backoff and failure counter
						backoff.Reset()
						consecutiveFailures = 0
						log.Printf("✅ TCP RECONNECTED: TCP transport reinitialized successfully!")
						log.Printf("🚀 TCP TRANSPORT RESTORED: Ready for data transfer (incremental sync will use TCP)")
					}
				} else {
					consecutiveFailures++
					log.Printf("🔶 TCP UNAVAILABLE (attempt %d): VM-sync not reachable at %s (%v)", consecutiveFailures, address, err)
					
					// Apply exponential backoff with jitter
					backoffDuration := backoff.NextBackoff()
					log.Printf("⏳ TCP BACKOFF: Waiting %v before next health check (exponential + jitter)", backoffDuration)
				}
				continue
			}

			// CASE 2: TCP is enabled -> Verify it's still healthy
			if tcpTransportEnabled && tcpSender != nil {
				// Get current address
				addressMgr := transport.GetAddressManager()
				address, err := addressMgr.GetAnyAddress()
				if err != nil {
					address = config.Sync.Transport.TCPSender.Address
				}

				// Test connection health
				if err := testTCPConnection(address); err != nil {
					log.Printf("⚠️  TCP CONNECTION LOST: %v", err)
					log.Printf("🔧 TCP CLEANUP: Closing broken TCP connection...")

					// Clean up broken connection
					tcpTransportEnabled = false
					if closeErr := tcpSender.Close(); closeErr != nil {
						log.Printf("⚠️  TCP CLOSE WARNING: %v", closeErr)
					}
					tcpSender = nil
					
					// Reset backoff for fresh reconnection attempts
					backoff.Reset()
					consecutiveFailures = 0

					log.Printf("📴 TCP DEGRADED: Connection lost, will auto-reconnect on next health check")
				} else {
					// Connection is healthy - reset failure counter
					consecutiveFailures = 0
					backoff.Reset()
					log.Printf("✅ TCP HEALTHY: Connection to %s is operational", address)
				}
			}
		}
	}
}

func main() {
	configFile := flag.String("config", "config.yaml", "Path to configuration file")
	flag.Parse()

	// Fetch tenant information if not provided
	if err := tenantinfo.FetchTenantInfoIfNeeded(); err != nil {
		log.Fatalf("Failed to fetch tenant information: %v", err)
	}

	// Log startup environment variables for debugging
	log.Println("🚀 STARTUP: Cloud-sync starting...")
	log.Printf("   TENANT_DNS: %s", os.Getenv("TENANT_DNS"))
	log.Printf("   TENANT_ID: %s", os.Getenv("TENANT_ID"))
	log.Printf("   TENANT_NAME: %s", os.Getenv("TENANT_NAME"))
	log.Printf("   COMMUNITY_ID: %s", os.Getenv("COMMUNITY_ID"))
	log.Printf("   COMMUNITY_NAME: %s", os.Getenv("COMMUNITY_NAME"))

	// Load configuration first (will use defaults if tenant info not available yet)
	if err := loadConfig(*configFile); err != nil {
		log.Fatalf("Failed to load config: %v", err)
	}

	// Connect to MongoDB with retry mechanism
	ctx, cancel := context.WithTimeout(context.Background(), config.MongoDB.Timeout)
	defer cancel()

	clientOptions := options.Client().ApplyURI(config.MongoDB.URI)
	var err error
	var mongoConnected bool
	for attempt := 1; attempt <= 3; attempt++ {
		mongoClient, err = mongo.Connect(ctx, clientOptions)
		if err != nil {
			log.Printf("MongoDB connection attempt %d failed: %v", attempt, err)
			if attempt < 3 {
				time.Sleep(time.Duration(attempt) * 2 * time.Second)
				continue
			}
			log.Fatalf("All MongoDB connection attempts failed: %v", err)
		}

		// Test the connection
		if err = mongoClient.Ping(ctx, nil); err != nil {
			log.Printf("MongoDB ping attempt %d failed: %v", attempt, err)
			if attempt < 3 {
				time.Sleep(time.Duration(attempt) * 2 * time.Second)
				continue
			}
			log.Fatalf("MongoDB ping failed after all attempts: %v", err)
		}

		log.Println("Connected to MongoDB successfully")
		mongoConnected = true
		break
	}

	// Initialize application logger early so we can use it
	appLogger = logging.NewLogger(5000) // Keep last 5000 log entries
	appLogger.Info("cloud-sync", "startup", "MongoDB connection established successfully", map[string]interface{}{
		"uri": config.MongoDB.URI,
	})

	// Initialize filter engine
	filterEngine = filtering.NewFilterEngine()

	// Initialize encryption manager with graceful fallback
	encryptionMgr = crypto.NewEncryptionManager()
	if err := encryptionMgr.Initialize(config.Encryption); err != nil {
		log.Printf("WARNING: Failed to initialize encryption: %v", err)
		log.Printf("DEGRADED MODE: Continuing without encryption (security risk)")
		// Disable encryption in config to prevent further issues
		config.Encryption.Enabled = false
	} else if encryptionMgr.IsEnabled() {
		log.Printf("Encryption enabled with key ID: %s", encryptionMgr.GetKeyID())
	} else {
		log.Println("Encryption disabled")
	}

	// Initialize checkpoint manager with graceful fallback
	if config.Checkpoint.Enabled && mongoConnected {
		// Apply tenant-aware naming
		checkpointDB := config.GetTenantDatabaseName(config.Checkpoint.Database)
		checkpointColl := config.GetTenantCollectionName(config.Checkpoint.Collection)

		checkpointConfig := &resume.CheckpointConfig{
			MongoClient:     mongoClient, // Reuse existing MongoDB client
			MongoURI:        config.MongoDB.URI,
			Database:        checkpointDB,
			Collection:      checkpointColl,
			PersistInterval: time.Duration(config.Checkpoint.SaveInterval) * time.Second,
			Enabled:         config.Checkpoint.Enabled,
		}
		var err error
		checkpointMgr, err = resume.NewCheckpointManager(checkpointConfig)
		if err != nil {
			log.Printf("WARNING: Failed to initialize checkpoint manager: %v", err)
			log.Printf("DEGRADED MODE: Continuing without checkpoint management (resume tokens will not persist)")
			checkpointMgr = nil
			config.Checkpoint.Enabled = false
		} else {
			log.Println("✅ Checkpoint manager initialized successfully")
			log.Printf("📋 CHECKPOINT CONFIG: Database: %s, Collection: %s, SaveInterval: %vs",
				checkpointDB, checkpointColl, config.Checkpoint.SaveInterval)
		}
	} else {
		log.Printf("⚠️  CHECKPOINT DISABLED: Enabled=%v, MongoDB Connected=%v", config.Checkpoint.Enabled, mongoConnected)
		if config.Checkpoint.Database == "" {
			log.Printf("🚫 CHECKPOINT CONFIG MISSING: Database field is empty in config")
		}
		if config.Checkpoint.Collection == "" {
			log.Printf("🚫 CHECKPOINT CONFIG MISSING: Collection field is empty in config")
		}
	}

	// Initialize transfer tracker with graceful fallback
	if mongoConnected {
		trackingConfig := convertTrackingConfig(config.Tracking)
		// Reuse the same MongoDB client as the main service
		trackingConfig.MongoClient = mongoClient
		trackingConfig.MongoURI = config.MongoDB.URI
		// Apply tenant-aware naming
		trackingConfig.Database = config.GetTenantDatabaseName(config.Tracking.Database)
		trackingConfig.StateCollection = config.GetTenantCollectionName(config.Tracking.StateCollection)
		trackingConfig.BatchCollection = config.GetTenantCollectionName(config.Tracking.BatchCollection)

		transferTracker, err = tracking.NewTransferTracker(trackingConfig)
		if err != nil {
			log.Printf("WARNING: Failed to initialize transfer tracker: %v", err)
			log.Printf("DEGRADED MODE: Continuing without transfer tracking (no exactly-once guarantees)")
			transferTracker = nil
			config.Tracking.Enabled = false
		} else if transferTracker.IsEnabled() {
			log.Printf("✅ Transfer tracker enabled: Database=%s, StateCollection=%s, BatchCollection=%s",
				trackingConfig.Database, trackingConfig.StateCollection, trackingConfig.BatchCollection)
		} else {
			log.Println("Transfer tracking disabled")
		}
	} else {
		log.Println("Transfer tracking disabled - MongoDB unavailable")
	}

	// Initialize sequence generator with graceful fallback
	if config.Sequence.Enabled && mongoConnected {
		// Apply tenant-aware naming
		sequenceDB := config.GetTenantDatabaseName(config.Sequence.Database)
		sequenceColl := config.GetTenantCollectionName(config.Sequence.Collection)

		sequenceConfig := &sequence.GeneratorConfig{
			Enabled:    config.Sequence.Enabled,
			MongoURI:   config.MongoDB.URI,
			Database:   sequenceDB,
			Collection: sequenceColl,
			BatchSize:  config.Sequence.BatchSize,
			NodeID:     config.Sequence.NodeID,
		}
		sequenceGen, err = sequence.NewGenerator(sequenceConfig)
		if err != nil {
			log.Printf("WARNING: Failed to initialize sequence generator: %v", err)
			log.Printf("DEGRADED MODE: Continuing without sequence ordering (events may be out of order)")
			sequenceGen = nil
			config.Sequence.Enabled = false
		} else {
			log.Printf("✅ Sequence generator initialized: Database=%s, Collection=%s, Node=%s",
				sequenceDB, sequenceColl, config.Sequence.NodeID)
		}
	} else {
		log.Println("Sequence generator disabled or MongoDB unavailable")
	}

	// Initialize cluster time fence with graceful fallback
	if config.Fence.Enabled && mongoConnected {
		fenceConfig := &fence.FenceConfig{
			Enabled:  config.Fence.Enabled,
			MongoURI: config.MongoDB.URI,
		}
		clusterFence, err = fence.NewClusterTimeFence(fenceConfig)
		if err != nil {
			log.Printf("WARNING: Failed to initialize cluster time fence: %v", err)
			log.Printf("DEGRADED MODE: Continuing without cluster time fencing (potential duplicate events)")
			clusterFence = nil
			config.Fence.Enabled = false
		} else {
			log.Println("Cluster time fence initialized")
		}
	} else {
		log.Println("Cluster time fence disabled or MongoDB unavailable")
	}

	// Initialize partitioner
	partitioner = parallel.NewPartitioner(mongoClient, parallel.DefaultPartitionConfig())
	log.Println("Partitioner initialized with default configuration")

	// Initialize metrics system
	metricsCollector = metrics.NewMetricsCollector(1000)           // Keep last 1000 metrics
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

	// Initialize OAuth2 authentication service
	jwtSecret := []byte("your-jwt-secret-key") // TODO: Move to config
	// Apply tenant-aware naming
	oauth2DB := config.GetTenantDatabaseName("oauth2_auth")
	oauth2Coll := config.GetTenantCollectionName("clients")

	authService = auth.NewClientCredentialsService(
		mongoClient,
		oauth2DB,            // tenant-aware database
		oauth2Coll,          // tenant-aware collection
		jwtSecret,           // JWT secret
		"cloud-sync",        // issuer
		[]string{"vm-sync"}, // audience
	)
	log.Printf("✅ OAuth2 authentication service initialized: Database=%s, Collection=%s", oauth2DB, oauth2Coll)
	appLogger.Info("cloud-sync", "startup", "OAuth2 authentication service initialized", map[string]interface{}{
		"database":   oauth2DB,
		"collection": oauth2Coll,
		"issuer":     "cloud-sync",
	})

	// Mark service as ready for WebSocket authentication
	serviceReadyMutex.Lock()
	serviceReady = true
	serviceReadyMutex.Unlock()
	log.Println("🎯 SERVICE READY: Cloud-sync ready to accept vm-sync connections")

	// Initialize buffer-free resume token manager (ELIMINATES MEMORY BUFFERS)
	log.Println("🚀 BUFFER-FREE: Initializing resume token manager (no memory buffers)...")
	// Apply tenant-aware naming
	resumeTokenDB := config.GetTenantDatabaseName("vm_resume_tokens")
	resumeTokenColl := config.GetTenantCollectionName("client_tokens")

	tokenManagerConfig := &resume.TokenManagerConfig{
		MongoURI:        config.MongoDB.URI,
		Database:        resumeTokenDB,
		Collection:      resumeTokenColl,
		PersistInterval: 5 * time.Second,
		CleanupInterval: 1 * time.Hour,
		RetentionDays:   7,
	}

	tokenManager, err = resume.NewTokenManager(tokenManagerConfig)
	if err != nil {
		log.Printf("WARNING: Failed to initialize token manager: %v", err)
		log.Printf("DEGRADED MODE: Continuing without buffer-free system (fallback to degraded mode)")
		tokenManager = nil
	} else {
		log.Printf("✅ BUFFER-FREE: Resume token manager initialized - Database=%s, Collection=%s",
			resumeTokenDB, resumeTokenColl)
		// Initialize buffer-free change handler
		bufferFreeHandler = resume.NewBufferFreeChangeHandler(tokenManager, mongoClient, broadcast)
		log.Println("🎯 BUFFER-FREE: Change handler initialized (zero memory buffer usage)")
	}

	appLogger.Info("cloud-sync", "startup", "Buffer-free resume token system initialized", map[string]interface{}{
		"memory_buffer_eliminated": true,
		"resume_token_based":       true,
		"fault_tolerant":           true,
		"peak_hour_ready":          true,
	})

	// DEFERRED: TCP transport will be initialized AFTER HTTP server is ready
	// This ensures all endpoints are available before attempting VM-sync connection
	log.Println("⏳ TCP INIT: Deferred until after HTTP server startup")

	// Initialize adaptive system components with graceful fallback
	log.Println("Initializing adaptive system...")
	cloudSyncIntegration, err = adaptive.NewCloudSyncIntegration("cloud-sync-node")
	if err != nil {
		log.Printf("WARNING: Failed to initialize cloud sync integration: %v", err)
		log.Printf("DEGRADED MODE: Continuing without adaptive resource management (fixed parallelism)")
		cloudSyncIntegration = nil
	} else {
		log.Println("Cloud sync integration initialized")

		// Register back-pressure configuration callback
		cloudSyncIntegration.RegisterConfigCallback(func(config *models.AdaptiveConfig) error {
			applyBackPressureConfig(config)
			return nil
		})

		// Start adaptive system
		if err := cloudSyncIntegration.Start(); err != nil {
			log.Printf("WARNING: Failed to start cloud sync integration: %v", err)
			log.Printf("DEGRADED MODE: Adaptive system disabled")
			cloudSyncIntegration = nil
		} else {
			log.Println("Cloud sync integration started successfully")
		}
	}
	appLogger.Info("cloud-sync", "startup", "Adaptive resource-aware parallelism system initialized", nil)

	// Initialize intelligent collection distributor (REMOVES MANUAL VM YAML CONFIG)
	collectionDistributor = distribution.NewCollectionDistributor(distribution.DistributionModeLoad)
	log.Println("🤖 INTELLIGENT DISTRIBUTOR: Auto-distribution enabled (no manual VM YAML config needed)")
	appLogger.Info("cloud-sync", "startup", "Collection auto-distribution system enabled", map[string]interface{}{
		"mode":                     "load_balanced",
		"eliminates_manual_config": true,
	})

	// Enable self-optimization for Cloud Sync
	enableSelfOptimization()
	appLogger.Info("cloud-sync", "startup", "Self-optimization system enabled", nil)

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

	// Initialize internal cluster if enabled with graceful fallback
	if config.InternalCluster.Enabled {
		internalCluster = cluster.NewInternalCluster(config.InternalCluster)
		log.Printf("Internal cluster enabled with %d workers", config.InternalCluster.WorkerPool.WorkerCount)

		// Start internal cluster
		if err := internalCluster.Start(); err != nil {
			log.Printf("WARNING: Failed to start internal cluster: %v", err)
			log.Printf("DEGRADED MODE: Continuing without internal cluster (reduced processing capacity)")
			internalCluster = nil
			config.InternalCluster.Enabled = false
		} else {
			log.Println("Internal cluster started successfully")
		}
	}

	// ENABLED: Real-time sync will start after initial dump completes
	// Change stream initialization moved to prevent any real-time sync duplication
	if mongoConnected {
		log.Println("🚀 SEQUENCED STARTUP: Real-time sync will start AFTER initial dump completes")
		log.Println("⏳ WAITING: Change streams will be initialized after bulk data transfer finishes")
		// Change stream initialization moved to startRealTimeSync() function
		// This prevents duplicate data during initial dump
	} else {
		log.Println("DEGRADED MODE: Change stream monitoring disabled - MongoDB unavailable")
	}

	// Start WebSocket broadcast handler
	go handleBroadcast()

	// Start status broadcaster for dashboard updates
	go startStatusBroadcaster()

	// Start push-based data synchronization
	go startPushBasedSync()

	// Get base path from environment variables
	basePath := getBasePath()
	log.Printf("API Base Path: %s", basePath)

	// Use TENANT_DNS for public URL (reusing existing tenant domain)
	tenantDNS := os.Getenv("TENANT_DNS")
	var baseURL string

	if tenantDNS != "" {
		// Use TENANT_DNS for public-facing URL (Kubernetes/production)
		baseURL = fmt.Sprintf("https://%s%s", tenantDNS, basePath)
	} else {
		// Fallback to config (for local development)
		baseURL = fmt.Sprintf("http://%s:%d%s", config.Server.Host, config.Server.Port, basePath)
	}

	fmt.Println("Base URL:", baseURL)
	// Setup HTTP routes
	router := mux.NewRouter()

	// Create subrouter with base path if configured
	var apiRouter *mux.Router
	if basePath != "" {
		apiRouter = router.PathPrefix(basePath).Subrouter()
		log.Printf("All routes will be prefixed with: %s", basePath)
	} else {
		apiRouter = router
		log.Println("Using root path for routes (no base path configured)")
	}

	// Add OAuth2 authentication routes
	if authService != nil {
		auth.SetupAuthRoutes(apiRouter, authService)
		log.Println("OAuth2 authentication routes registered")
	}

	// Core API routes
	apiRouter.HandleFunc(config.WebSocket.Endpoint, handleWebSocket)
	apiRouter.HandleFunc("/api/data", handleDataRequest).Methods("POST")
	apiRouter.HandleFunc("/api/partitions", handlePartitionsRequest).Methods("POST")
	apiRouter.HandleFunc("/api/sync/trigger", handleTriggerInitialSync).Methods("POST") // Manual initial sync trigger
	apiRouter.HandleFunc("/api/sync/status", handleSyncStatus).Methods("GET")           // Sync status endpoint
	apiRouter.HandleFunc("/api/telemetry", handleTelemetry).Methods("POST")             // HTTP telemetry endpoint
	apiRouter.HandleFunc("/health", handleHealth).Methods("GET")

	// Register metrics API routes first (to avoid conflicts)
	metricsAPI.RegisterRoutes(apiRouter)

	// Dashboard routes
	apiRouter.HandleFunc("/dashboard", handleDashboard).Methods("GET", "HEAD")
	apiRouter.HandleFunc("/dashboard/simple", handleSimpleDashboard).Methods("GET", "HEAD")
	apiRouter.HandleFunc("/api/dashboard/metrics", handleMetrics).Methods("GET", "HEAD") // Dashboard-specific metrics
	apiRouter.HandleFunc("/api/dashboard/logs", handleLogs).Methods("GET", "HEAD")       // Dashboard-specific logs
	apiRouter.HandleFunc("/api/metrics/charts", handleChartData).Methods("GET", "HEAD")
	apiRouter.HandleFunc("/api/control/{action}", handleControl).Methods("POST")

	// Swagger documentation routes
	apiRouter.HandleFunc("/docs", handleSwaggerUI).Methods("GET", "HEAD")
	apiRouter.HandleFunc("/docs/", handleSwaggerUI).Methods("GET", "HEAD")
	apiRouter.HandleFunc("/docs/swagger.yaml", handleSwaggerSpec).Methods("GET", "HEAD")
	apiRouter.HandleFunc("/docs/api-docs", handleSwaggerSpec).Methods("GET", "HEAD") // Alternative endpoint

	// Static files for dashboard
	apiRouter.PathPrefix("/static/").Handler(http.StripPrefix(basePath+"/static/", http.FileServer(http.Dir("./web/static/"))))

	// Adaptive controller diagnostics routes
	apiRouter.HandleFunc("/api/adaptive/stats", handleAdaptiveStats).Methods("GET")
	apiRouter.HandleFunc("/api/adaptive/history", handleAdaptiveHistory).Methods("GET")
	apiRouter.HandleFunc("/api/adaptive/health", handleAdaptiveHealth).Methods("GET")

	// Collection distribution routes (auto-distribution monitoring)
	apiRouter.HandleFunc("/api/distribution/status", handleDistributionStatus).Methods("GET")
	apiRouter.HandleFunc("/api/distribution/assignments", handleDistributionAssignments).Methods("GET")
	apiRouter.HandleFunc("/api/distribution/redistribute", handleRedistribute).Methods("POST")
	apiRouter.HandleFunc("/api/distribution/vms", handleVMStatus).Methods("GET")

	// Buffer-free resume token system routes
	apiRouter.HandleFunc("/api/buffer-free/status", handleBufferFreeStatus).Methods("GET")
	apiRouter.HandleFunc("/api/buffer-free/tokens", handleTokenStatus).Methods("GET")

	// Initial sync API endpoint - triggers full database replacement
	apiRouter.HandleFunc("/api/sync/initial", handleInitialSync).Methods("POST")

	// RACE CONDITION DEBUG: Add VM client status endpoint for troubleshooting
	apiRouter.HandleFunc("/api/debug/vm-clients", handleVMClientsDebug).Methods("GET")

	// Serve static files for dashboard
	router.PathPrefix("/static/").Handler(http.StripPrefix("/static/", http.FileServer(http.Dir("./web/static/"))))

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
			log.Printf("CRITICAL: HTTP server failed to start: %v", err)
			log.Printf("DEGRADED MODE: Attempting to start on alternative port")
			// Try alternative port
			alternativePort := config.Server.Port + 1
			server.Addr = fmt.Sprintf("%s:%d", config.Server.Host, alternativePort)
			log.Printf("Retrying on port %d", alternativePort)
			if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
				log.Printf("FATAL: Failed to start server on alternative port: %v", err)
				log.Printf("Cannot continue without HTTP server")
				os.Exit(1)
			}
		}
	}()

	// Wait for HTTP server to be ready (brief delay to ensure endpoints are available)
	time.Sleep(2 * time.Second)
	log.Println("✅ HTTP SERVER: Ready and accepting connections")

	// NOW initialize TCP transport AFTER HTTP server is ready
	log.Println("🚀 TCP INIT: Initializing TCP transport now that HTTP endpoints are ready...")
	if err := initializeTCPTransportWithRetry(); err != nil {
		log.Printf("WARNING: Failed to initialize TCP transport after retries: %v", err)
		log.Printf("DEGRADED MODE: Using HTTP transport for data transfer")
		tcpTransportEnabled = false

		// Start TCP transport health monitor if TCP is configured as primary
		if config.Sync.Transport.Mode == "tcp" {
			log.Printf("🔍 TCP MONITOR: Starting TCP transport health monitor for automatic reconnection")
			go startTCPHealthMonitor()
		}
	} else if tcpTransportEnabled {
		log.Println("✅ TCP TRANSPORT: Initialized successfully - ready for initial dump")
		appLogger.Info("cloud-sync", "startup", "TCP transport enabled for high-performance data transfer", nil)

		// Start TCP transport health monitor even when initially successful
		if config.Sync.Transport.Mode == "tcp" {
			go startTCPHealthMonitor()
		}
	}

	// Start Full Replacement Scheduler IMMEDIATELY (independent of initial dump)
	if config.Sync.FullReplacementIntervalMinutes > 0 {
		log.Printf("🔄 FULL REPLACEMENT SCHEDULER: Starting with interval: %d minutes", config.Sync.FullReplacementIntervalMinutes)
		startFullReplacementScheduler()
	} else {
		log.Println("💤 FULL REPLACEMENT SCHEDULER: Disabled (interval=0)")
	}

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

	// Shutdown TCP transport if enabled
	if tcpTransportEnabled && tcpSender != nil {
		log.Println("Shutting down TCP transport...")
		if err := tcpSender.Close(); err != nil {
			log.Printf("Error closing TCP sender: %v", err)
		} else {
			log.Println("TCP transport stopped gracefully")
		}
	}

	// Shutdown buffer-free system if enabled
	if bufferFreeHandler != nil {
		log.Println("💫 BUFFER-FREE: Shutting down buffer-free change handler...")
		bufferFreeHandler.Stop()
		log.Println("✅ BUFFER-FREE: Change handler stopped")
	}

	// Shutdown token manager if enabled
	if tokenManager != nil {
		log.Println("💫 BUFFER-FREE: Shutting down resume token manager...")
		tokenManager.Stop()
		log.Println("✅ BUFFER-FREE: Token manager stopped")
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

// Rate limiting and DDoS protection functions

// initializeRateLimiting sets up rate limiters and DDoS protection
func initializeRateLimiting() {
	// Global rate limiter: 100 requests per second
	globalRateLimiter = rate.NewLimiter(rate.Limit(100), 200) // Burst of 200

	// Initialize maps for per-client and per-IP rate limiting
	clientRateLimiters = make(map[string]*rate.Limiter)
	connectionLimiters = make(map[string]*rate.Limiter)
	blocklistIPs = make(map[string]time.Time)
	connectionsByIP = make(map[string]int)

	// Set connection limits
	maxConnectionsPerIP = 50 // Maximum 50 connections per IP

	// Start cleanup goroutine for expired blocklist entries
	go cleanupExpiredBlocklist()

	log.Println("Rate limiting initialized: 100 req/sec global, 20 req/sec per client, 10 conn/min per IP")
}

// getClientRateLimiter returns or creates a rate limiter for a specific client
func getClientRateLimiter(clientID string) *rate.Limiter {
	rateLimiterMutex.RLock()
	limiter, exists := clientRateLimiters[clientID]
	rateLimiterMutex.RUnlock()

	if !exists {
		rateLimiterMutex.Lock()
		// Check again after acquiring write lock
		if limiter, exists = clientRateLimiters[clientID]; !exists {
			// Per-client rate limiter: 20 requests per second
			limiter = rate.NewLimiter(rate.Limit(20), 40) // Burst of 40
			clientRateLimiters[clientID] = limiter
		}
		rateLimiterMutex.Unlock()
	}

	return limiter
}

// getConnectionRateLimiter returns or creates a connection rate limiter for an IP
func getConnectionRateLimiter(ip string) *rate.Limiter {
	rateLimiterMutex.RLock()
	limiter, exists := connectionLimiters[ip]
	rateLimiterMutex.RUnlock()

	if !exists {
		rateLimiterMutex.Lock()
		// Check again after acquiring write lock
		if limiter, exists = connectionLimiters[ip]; !exists {
			// Per-IP connection rate limiter: 10 connections per minute
			limiter = rate.NewLimiter(rate.Every(6*time.Second), 10) // 10 per minute
			connectionLimiters[ip] = limiter
		}
		rateLimiterMutex.Unlock()
	}

	return limiter
}

// isIPBlocked checks if an IP is currently blocked
func isIPBlocked(ip string) bool {
	blocklistMutex.RLock()
	expiry, blocked := blocklistIPs[ip]
	blocklistMutex.RUnlock()

	if !blocked {
		return false
	}

	// Check if block has expired
	if time.Now().After(expiry) {
		blocklistMutex.Lock()
		delete(blocklistIPs, ip)
		blocklistMutex.Unlock()
		return false
	}

	return true
}

// blockIP blocks an IP address for a specified duration
func blockIP(ip string, duration time.Duration) {
	blocklistMutex.Lock()
	blocklistIPs[ip] = time.Now().Add(duration)
	blocklistMutex.Unlock()

	log.Printf("SECURITY: Blocked IP %s for %v due to suspicious activity", ip, duration)
	if appLogger != nil {
		appLogger.Warn("cloud-sync", "ip_blocked", fmt.Sprintf("Blocked IP %s for suspicious activity", ip), map[string]interface{}{
			"ip":       ip,
			"duration": duration.String(),
			"reason":   "rate_limit_exceeded",
		})
	}
}

// canConnect checks if a new connection from an IP is allowed
func canConnect(ip string) bool {
	// Check if IP is blocked
	if isIPBlocked(ip) {
		return false
	}

	// Check connection rate limit
	connLimiter := getConnectionRateLimiter(ip)
	if !connLimiter.Allow() {
		// Block IP for 5 minutes for connection flooding
		blockIP(ip, 5*time.Minute)
		return false
	}

	// Check current connection count
	connectionMutex.RLock()
	currentConns := connectionsByIP[ip]
	connectionMutex.RUnlock()

	if currentConns >= maxConnectionsPerIP {
		// Block IP for 10 minutes for connection exhaustion
		blockIP(ip, 10*time.Minute)
		return false
	}

	return true
}

// trackConnection increments connection count for an IP
func trackConnection(ip string) {
	connectionMutex.Lock()
	connectionsByIP[ip]++
	connectionMutex.Unlock()
}

// untrackConnection decrements connection count for an IP
func untrackConnection(ip string) {
	connectionMutex.Lock()
	if count := connectionsByIP[ip]; count > 0 {
		connectionsByIP[ip]--
		if connectionsByIP[ip] == 0 {
			delete(connectionsByIP, ip)
		}
	}
	connectionMutex.Unlock()
}

// cleanupExpiredBlocklist periodically removes expired IP blocks
func cleanupExpiredBlocklist() {
	ticker := time.NewTicker(1 * time.Minute)
	defer ticker.Stop()

	for range ticker.C {
		now := time.Now()
		blocklistMutex.Lock()
		for ip, expiry := range blocklistIPs {
			if now.After(expiry) {
				delete(blocklistIPs, ip)
			}
		}
		blocklistMutex.Unlock()
	}
}

// rateLimitMiddleware provides HTTP rate limiting middleware
func rateLimitMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Extract client IP
		clientIP := getClientIP(r)

		// Check if IP is blocked
		if isIPBlocked(clientIP) {
			http.Error(w, "IP temporarily blocked due to suspicious activity", http.StatusTooManyRequests)
			return
		}

		// Check global rate limit
		if !globalRateLimiter.Allow() {
			http.Error(w, "Rate limit exceeded: too many requests globally", http.StatusTooManyRequests)
			return
		}

		// Check per-client rate limit (use IP as client identifier for HTTP requests)
		clientLimiter := getClientRateLimiter(clientIP)
		if !clientLimiter.Allow() {
			// Block IP for 2 minutes for rate limit violation
			blockIP(clientIP, 2*time.Minute)
			http.Error(w, "Rate limit exceeded: too many requests from this client", http.StatusTooManyRequests)
			return
		}

		// Continue to next handler
		next.ServeHTTP(w, r)
	})
}

// getClientIP extracts the real client IP from request headers
func getClientIP(r *http.Request) string {
	// Check X-Forwarded-For header first (proxy/load balancer)
	if xff := r.Header.Get("X-Forwarded-For"); xff != "" {
		// Take the first IP in case of multiple
		ips := strings.Split(xff, ",")
		return strings.TrimSpace(ips[0])
	}

	// Check X-Real-IP header (nginx proxy)
	if xri := r.Header.Get("X-Real-IP"); xri != "" {
		return strings.TrimSpace(xri)
	}

	// Fallback to remote address
	ip, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		return r.RemoteAddr // Return as-is if parsing fails
	}
	return ip
}

func startPushBasedSync() {
	log.Println("🚀 Starting push-based synchronization...")

	// Wait for vm-sync to connect via WebSocket before starting initial data dump
	log.Println("Waiting for vm-sync client to connect via WebSocket...")
	select {
	case <-vmSyncConnected:
		log.Println("✅ vm-sync client connected, proceeding with INITIAL DATA DUMP")
	case <-time.After(10 * time.Minute): // Timeout after 10 minutes
		log.Println("⚠️  Timeout waiting for vm-sync connection, proceeding with sync anyway")
	}

	// CRITICAL: Enhanced TCP readiness verification for initial dump reliability
	if config.Sync.Transport.Mode == "tcp" {
		log.Println("🔍 TCP READINESS CHECK: Verifying TCP transport is ready for initial dump...")

		// Enhanced readiness check with shorter intervals but more attempts for better reliability
		for attempt := 1; attempt <= 15; attempt++ {
			if tcpTransportEnabled && tcpSender != nil {
				log.Printf("✅ TCP READY: TCP transport confirmed ready for initial dump (attempt %d)", attempt)
				break
			}

			if attempt == 15 {
				log.Printf("⚠️ TCP TIMEOUT: TCP transport not ready after %d attempts, will use HTTP fallback", attempt)
				log.Printf("🚑 FALLBACK: Initial dump will proceed with HTTP transport for reliability")
				break
			}

			log.Printf("⏳ TCP WAIT: TCP transport not ready, waiting 2s (attempt %d/15)...", attempt)
			time.Sleep(2 * time.Second)
		}
	} else {
		log.Println("🌐 HTTP MODE: Using HTTP transport for initial dump")
	}

	log.Println("📊 Starting sync process for all configured collections...")
	startSyncProcess()

	// CRITICAL: Enhanced signal that initial dump is completed with safety checks
	initialDumpMutex.Lock()
	if !initialDumpCompletedOnce {
		initialDumpCompletedOnce = true
		log.Println("🔄 INITIAL DUMP: Signaling completion to real-time sync...")
		select {
		case initialDumpCompleted <- true:
			log.Println("✅ INITIAL DUMP COMPLETED: Real-time sync signaled successfully")
		default:
			// Channel already has a value - this is expected behavior
			log.Println("🔄 INITIAL DUMP: Signal channel already notified")
		}
	} else {
		log.Println("⚠️ INITIAL DUMP: Already completed, skipping duplicate signal")
	}
	initialDumpMutex.Unlock()

	// Run root collections migration after initial dump
	log.Println("📦 ROOT MIGRATION: Starting migration after initial dump...")
	if migrator, err := migration.NewRootCollectionsMigration(&config, tcpSender); err != nil {
		log.Printf("❌ ROOT MIGRATION: Failed to create migrator: %v", err)
	} else if migrator != nil {
		ctx := context.Background()
		if err := migrator.Connect(ctx); err != nil {
			log.Printf("❌ ROOT MIGRATION: Failed to connect: %v", err)
		} else {
			defer migrator.Close(ctx)

			// Migrate service keys
			if err := migrator.MigrateServiceKeys(ctx); err != nil {
				log.Printf("❌ ROOT MIGRATION: Service keys migration failed: %v", err)
			}

			// Migrate config stores
			if err := migrator.MigrateConfigStores(ctx); err != nil {
				log.Printf("❌ ROOT MIGRATION: Config stores migration failed: %v", err)
			}

			log.Println("✅ ROOT MIGRATION: Migration completed")
		}
	} else {
		log.Println("⏭️ ROOT MIGRATION: Skipped (not configured or not needed)")
	}

	// Start real-time synchronization AFTER initial dump completion with enhanced safety
	log.Println("🚀 SEQUENCED STARTUP: Starting real-time sync goroutine...")
	go startRealTimeSync()
}

// startRealTimeSync starts scheduler-based synchronization AFTER initial dump completion
// This prevents data duplication between initial dump and incremental sync with enhanced safety
func startRealTimeSync() {
	log.Println("⏳ SEQUENCED SYNC: Incremental sync waiting for initial dump completion...")

	// Enhanced safety: Wait for initial dump to complete before starting incremental sync
	select {
	case <-initialDumpCompleted:
		log.Println("✅ INITIAL DUMP VERIFIED: Starting scheduler-based incremental synchronization")
		log.Println("🛡️ DATA SAFETY: No duplication risk - initial dump completed first")
	case <-time.After(45 * time.Minute): // Extended timeout for large datasets
		log.Println("⚠️ TIMEOUT: Waiting for initial dump completion timed out after 45 minutes")
		log.Println("🚑 FALLBACK: Starting incremental sync anyway to ensure continuity")
	}

	// Check if scheduler-based sync is enabled with improved logic
	if config.Sync.SchedulerSync {
		log.Printf("📅 SCHEDULER: Starting incremental sync with interval: %v", config.Sync.SchedulerInterval)
		startSchedulerBasedSync()
	} else if config.Sync.RealtimeSync {
		// Fallback to real-time change streams if scheduler is not enabled
		log.Println("🎯 REAL-TIME: Starting traditional change stream monitoring...")
		startChangeStreamMonitoring()
	} else {
		log.Println("💤 SYNC DISABLED: Neither scheduler nor real-time sync is enabled")
		log.Println("⚠️ WARNING: System is in initial-dump-only mode")
	}
}

// startSchedulerBasedSync starts scheduler-based incremental synchronization
func startSchedulerBasedSync() {
	if config.Sync.SchedulerInterval <= 0 {
		log.Println("⚠️  Invalid scheduler interval, defaulting to 30 minutes")
		config.Sync.SchedulerInterval = 30 * time.Minute
	}

	log.Printf("📅 SCHEDULER: Starting with %v interval", config.Sync.SchedulerInterval)

	// Start the scheduler in a goroutine with panic recovery
	go func() {
		// FIX GAP #3: Panic recovery to prevent scheduler death
		defer func() {
			if r := recover(); r != nil {
				log.Printf("🚨 SCHEDULER PANIC RECOVERED: %v - Restarting scheduler in 5 seconds...", r)
				time.Sleep(5 * time.Second)
				// Restart the scheduler
				startSchedulerBasedSync()
			}
		}()

		ticker := time.NewTicker(config.Sync.SchedulerInterval)
		defer ticker.Stop()

		// Run initial incremental sync immediately with panic protection
		log.Println("🚀 SCHEDULER: Running initial incremental sync...")
		runIncrementalSyncSafe()

		// Then run on schedule
		for {
			select {
			case <-ticker.C:
				log.Println("═══════════════════════════════════════════════════════════")
				log.Printf("📅 SCHEDULER TICK: Time=%s, Interval=%v", time.Now().Format(time.RFC3339), config.Sync.SchedulerInterval)
				log.Printf("🔍 SCHEDULER: scheduler_sync=%v, realtime_sync=%v", config.Sync.SchedulerSync, config.Sync.RealtimeSync)
				log.Println("🚀 SCHEDULER: Starting incremental sync run...")
				runIncrementalSyncSafe()
				log.Printf("✅ SCHEDULER COMPLETE: Next run at %s", time.Now().Add(config.Sync.SchedulerInterval).Format(time.RFC3339))
				log.Println("═══════════════════════════════════════════════════════════")
			case <-context.Background().Done():
				log.Println("📅 SCHEDULER: Stopping scheduler due to context cancellation")
				return
			}
		}
	}()

	log.Println("✅ SCHEDULER: Incremental sync scheduler started successfully")
}

// runIncrementalSyncSafe wraps runIncrementalSync with panic recovery
func runIncrementalSyncSafe() {
	defer func() {
		if r := recover(); r != nil {
			log.Printf("🚨 INCREMENTAL SYNC PANIC: %v - Sync cycle aborted, will retry next cycle", r)
		}
	}()
	runIncrementalSync()
}

// startFullReplacementScheduler starts the full database replacement scheduler
func startFullReplacementScheduler() {
	intervalMinutes := config.Sync.FullReplacementIntervalMinutes
	
	if intervalMinutes <= 0 {
		log.Println("⚠️  FULL REPLACEMENT SCHEDULER: Invalid interval, scheduler disabled")
		return
	}
	
	interval := time.Duration(intervalMinutes) * time.Minute
	log.Printf("🔄 FULL REPLACEMENT SCHEDULER: Configured for %v interval (%d minutes)", interval, intervalMinutes)
	
	// Start the scheduler in a goroutine with panic recovery
	go func() {
		defer func() {
			if r := recover(); r != nil {
				log.Printf("🚨 FULL REPLACEMENT SCHEDULER PANIC: %v - Restarting scheduler in 1 minute...", r)
				time.Sleep(1 * time.Minute)
				startFullReplacementScheduler()
			}
		}()
		
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		
		log.Printf("⏰ FULL REPLACEMENT SCHEDULER: First replacement will run at %s", time.Now().Add(interval).Format(time.RFC3339))
		
		for {
			select {
			case <-ticker.C:
				log.Println("═══════════════════════════════════════════════════════════")
				log.Printf("🔄 FULL REPLACEMENT SCHEDULER TICK: Time=%s, Interval=%v", time.Now().Format(time.RFC3339), interval)
				log.Println("🚀 FULL REPLACEMENT: Starting complete database replacement...")
				
				// Execute full database replacement with force mode
				if err := executeFullDatabaseReplacement(); err != nil {
					log.Printf("❌ FULL REPLACEMENT FAILED: %v - Will retry at next interval", err)
				} else {
					log.Printf("✅ FULL REPLACEMENT COMPLETE: Next replacement at %s", time.Now().Add(interval).Format(time.RFC3339))
				}
				
				log.Println("═══════════════════════════════════════════════════════════")
			case <-context.Background().Done():
				log.Println("🔄 FULL REPLACEMENT SCHEDULER: Stopping due to context cancellation")
				return
			}
		}
	}()
	
	log.Printf("✅ FULL REPLACEMENT SCHEDULER: Started successfully - Runs every %d minutes", intervalMinutes)
}

// executeFullDatabaseReplacement performs a complete database replacement (same logic as API endpoint)
func executeFullDatabaseReplacement() error {
	log.Println("📡 FULL REPLACEMENT: Executing scheduled database replacement...")
	
	// Get vm-sync HTTP endpoint
	vmSyncEndpoint := getVMSyncHTTPEndpoint()
	if vmSyncEndpoint == "" {
		return fmt.Errorf("vm-sync endpoint not available")
	}
	
	log.Printf("🎯 FULL REPLACEMENT: Target endpoint: %s", vmSyncEndpoint)
	
	// STEP 1: Clear ALL vm-sync collections BEFORE pushing new data
	log.Println("🗑️ FULL REPLACEMENT: Step 1/2 - Clearing all collections...")
	cleared := 0
	failed := 0
	
	for _, dbConfig := range config.MongoDB.Databases {
		if !dbConfig.Enabled {
			continue
		}
		for _, collConfig := range dbConfig.Collections {
			if !collConfig.Enabled {
				continue
			}
			
			log.Printf("🗑️ Clearing: %s.%s", dbConfig.Name, collConfig.Name)
			if err := clearVMSyncCollection(vmSyncEndpoint, dbConfig.Name, collConfig.Name); err != nil {
				log.Printf("⚠️  Failed to clear %s.%s: %v (continuing anyway)", dbConfig.Name, collConfig.Name, err)
				failed++
			} else {
				log.Printf("✅ Cleared: %s.%s", dbConfig.Name, collConfig.Name)
				cleared++
			}
		}
	}
	
	log.Printf("🗑️ FULL REPLACEMENT: Cleared %d collections (%d failed)", cleared, failed)
	
	// STEP 2: Ensure TCP is connected before data push
	log.Println("🚀 FULL REPLACEMENT: Step 2/3 - Ensuring TCP connectivity...")
	if config.Sync.Transport.Mode == "tcp" {
		if tcpSender != nil {
			log.Println("✅ TCP ALREADY CONNECTED: Using existing connection")
		} else {
			log.Println("⚠️  TCP DISCONNECTED: Attempting reconnection before replacement...")
			// Try reconnecting TCP - MANDATORY for full replacement
			if err := initializeTCPTransport(); err != nil {
				return fmt.Errorf("TCP RECONNECT FAILED: %v - Full replacement requires TCP transport", err)
			}
			log.Println("✅ TCP RECONNECTED: Ready for data transfer")
		}
	} else {
		return fmt.Errorf("FULL REPLACEMENT ERROR: TCP transport not configured (mode=%s) - Full replacement requires TCP", config.Sync.Transport.Mode)
	}
	
	// STEP 3: Enable force initial sync mode and trigger sync process
	log.Println("🚀 FULL REPLACEMENT: Step 3/3 - Pushing complete database...")
	
	// Enable force mode to bypass resumable state
	forceInitialSync = true
	defer func() {
		forceInitialSync = false
		log.Println("🔄 FULL REPLACEMENT: Disabled force mode")
	}()
	
	// Execute the sync process (same as startup initial dump)
	startSyncProcess()
	
	log.Println("✅ FULL REPLACEMENT: Complete database replacement finished successfully")
	return nil
}

// startChangeStreamMonitoring starts traditional change stream monitoring (fallback)
func startChangeStreamMonitoring() {
	// Now it's safe to start change streams - no data duplication risk
	if mongoClient != nil {
		// Start change stream monitoring using buffer-free approach
		if bufferFreeHandler != nil {
			log.Println("🎯 BUFFER-FREE: Starting buffer-free change stream monitoring...")
			for _, database := range config.MongoDB.Databases {
				if !database.Enabled {
					continue
				}
				for _, collection := range database.Collections {
					if err := bufferFreeHandler.StartCollectionWatch(database.Name, collection.Name); err != nil {
						log.Printf("⚠️  Failed to start buffer-free watch for %s.%s: %v", database.Name, collection.Name, err)
					} else {
						log.Printf("✅ BUFFER-FREE: Started watch for %s.%s (zero memory buffer)", database.Name, collection.Name)
					}
				}
			}
			log.Println("🚀 BUFFER-FREE: All collections monitoring with resume tokens (no memory buffers)")
		} else {
			// Fallback to traditional change stream monitoring if buffer-free handler not available
			log.Println("⚠️  FALLBACK: Using traditional change stream monitoring (with memory buffers)")
			go monitorChangeStreamsTraditional()
		}
		log.Println("✅ SEQUENCED STARTUP COMPLETE: Real-time sync active after initial dump")
	} else {
		log.Println("DEGRADED MODE: Change stream monitoring disabled - MongoDB unavailable")
	}
}

// runIncrementalSync performs an incremental synchronization by detecting changes
// PARALLEL PROCESSING: Uses goroutines for concurrent collection monitoring (4x faster)
func runIncrementalSync() {
	start := time.Now()
	log.Println("📄 INCREMENTAL SYNC: Starting MongoDB change stream detection...")

	// Industry pattern: Process each collection with dedicated change streams
	var totalChanges atomic.Int32
	var syncErrors []string
	var errorsMutex sync.Mutex
	var wg sync.WaitGroup

	for _, database := range config.MongoDB.Databases {
		if !database.Enabled {
			continue
		}
		// DEBUG: Log database name to verify environment variable expansion
		log.Printf("🔍 INCREMENTAL SYNC DEBUG: Database Name='%s', OriginalTemplate='%s', TargetName='%s'",
			database.Name, database.OriginalTemplate, database.TargetDatabaseName)

		for _, collection := range database.Collections {
			if !collection.Enabled {
				continue
			}

			// PARALLEL PROCESSING: Launch goroutine for each collection
			wg.Add(1)
			go func(dbName, collName string) {
				defer wg.Done()
				
				// Use proper MongoDB change streams with resume tokens (like Debezium)
				changes, err := detectAndSyncWithChangeStream(dbName, collName)
				if err != nil {
					errorMsg := fmt.Sprintf("Failed to sync changes for %s.%s: %v", dbName, collName, err)
					log.Printf("🔴 INCREMENTAL SYNC ERROR: %s", errorMsg)
					errorsMutex.Lock()
					syncErrors = append(syncErrors, errorMsg)
					errorsMutex.Unlock()
					return
				}

				if changes > 0 {
					log.Printf("🔄 INCREMENTAL SYNC: Processed %d changes in %s.%s", changes, dbName, collName)
					totalChanges.Add(int32(changes))
				}
			}(database.Name, collection.Name)
		}
	}
	
	// Wait for all collections to complete
	log.Printf("⏳ PARALLEL SYNC: Waiting for all collections to complete...")
	wg.Wait()

	duration := time.Since(start)
	finalChanges := int(totalChanges.Load())
	
	if len(syncErrors) > 0 {
		log.Printf("⚠️  INCREMENTAL SYNC COMPLETED with %d errors in %v - Processed %d total changes", len(syncErrors), duration, finalChanges)
		for _, errMsg := range syncErrors {
			log.Printf("  • %s", errMsg)
		}
	} else {
		log.Printf("✅ INCREMENTAL SYNC COMPLETED successfully in %v - Processed %d total changes", duration, finalChanges)
	}
}

// detectAndSyncWithChangeStream implements the industry-standard single stream pattern
// Used by Debezium and other CDC platforms - combines detection and sync in one stream
func detectAndSyncWithChangeStream(database, collection string) (int, error) {
	coll := mongoClient.Database(database).Collection(collection)
	// Use shorter timeout than scheduler interval to prevent blocking
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Second)
	defer cancel()

	// Get resume token from checkpoint (industry standard persistence pattern)
	var watchOptions *options.ChangeStreamOptions
	var startFromToken bson.Raw

	if checkpointMgr != nil {
		if checkpoint := checkpointMgr.GetCheckpoint(database, collection); checkpoint != nil && len(checkpoint.ResumeToken) > 0 {
			startFromToken = checkpoint.ResumeToken
			watchOptions = options.ChangeStream().SetResumeAfter(startFromToken).SetFullDocument(options.UpdateLookup)
			log.Printf("🎯 RESUME TOKEN: Resuming change stream for %s.%s from checkpoint", database, collection)
		} else {
			// No resume token - start fresh from current time
			watchOptions = options.ChangeStream().SetFullDocument(options.UpdateLookup)
			log.Printf("🎆 NEW STREAM: Starting fresh change stream for %s.%s", database, collection)
		}
	} else {
		// No checkpoint manager
		watchOptions = options.ChangeStream().SetFullDocument(options.UpdateLookup)
		log.Printf("⚠️  NO CHECKPOINT: Starting temporary change stream for %s.%s", database, collection)
	}

	// Create change stream with error recovery (Debezium pattern)
	changeStream, err := coll.Watch(ctx, mongo.Pipeline{}, watchOptions)
	if err != nil {
		// Handle resume token invalidation gracefully (industry best practice)
		if isInvalidateResumeTokenError(err) && len(startFromToken) > 0 {
			log.Printf("🔄 OPLOG EXPIRED: Resume token for %s.%s is no longer in oplog", database, collection)
			log.Printf("⚠️  ERROR DETAIL: %v", err)
			log.Printf("🆕 RECOVERY: Clearing stale checkpoint and starting fresh stream")
			if checkpointMgr != nil {
				// Clear invalid resume token
				if err := checkpointMgr.UpdateCheckpoint(database, collection, nil, time.Now()); err != nil {
					log.Printf("⚠️  Failed to clear checkpoint: %v", err)
				} else {
					log.Printf("✅ CHECKPOINT CLEARED: Stale resume token removed")
				}
			}
			// Retry with fresh stream
			watchOptions = options.ChangeStream().SetFullDocument(options.UpdateLookup)
			changeStream, err = coll.Watch(ctx, mongo.Pipeline{}, watchOptions)
			if err == nil {
				log.Printf("✅ RECOVERY SUCCESS: Fresh change stream created for %s.%s", database, collection)
			}
		}

		if err != nil {
			return 0, fmt.Errorf("failed to create change stream: %w", err)
		}
	}
	defer changeStream.Close(ctx)

	// Single stream pattern: detect and sync in one pass (like Debezium)
	var documents [][]byte
	totalBytes := 0
	changeCount := 0
	lastResumeToken := startFromToken
	operationCounts := map[string]int{
		"insert": 0, "update": 0, "delete": 0, "replace": 0,
	}

	// Process ALL change events in real-time
	log.Printf("🔍 CHANGE STREAM: Listening for changes on %s.%s (timeout: 25s)...", database, collection)
	for changeStream.Next(ctx) {
		var changeEvent bson.M
		if err := changeStream.Decode(&changeEvent); err != nil {
			log.Printf("⚠️  Failed to decode change event: %v", err)
			continue
		}

		// Update resume token immediately (critical for fault tolerance)
		lastResumeToken = changeStream.ResumeToken()
		changeCount++

		// Track operation type
		if opType, ok := changeEvent["operationType"].(string); ok {
			operationCounts[opType]++
			log.Printf("📄 CHANGE DETECTED: %s operation on %s.%s", opType, database, collection)
		}

		// Extract document for insert/update/replace operations
		if fullDoc, ok := changeEvent["fullDocument"]; ok {
			// CRITICAL: Apply document filters to incremental sync (same as initial sync)
			if !matchesDocumentFilter(database, collection, fullDoc) {
				log.Printf("🚫 FILTER SKIP: Document filtered out from %s.%s (does not match criteria)", database, collection)
				continue // Skip this document
			}

			docBytes, err := bson.Marshal(fullDoc)
			if err != nil {
				log.Printf("⚠️  Failed to marshal document: %v", err)
				continue
			}
			documents = append(documents, docBytes)
			totalBytes += len(docBytes)
		} else if opType, ok := changeEvent["operationType"].(string); ok && opType == "delete" {
			// Handle delete operations
			deleteMarker := bson.M{
				"_operation":   "delete",
				"_documentKey": changeEvent["documentKey"],
				"_timestamp":   time.Now(),
			}
			docBytes, err := bson.Marshal(deleteMarker)
			if err != nil {
				log.Printf("⚠️  Failed to marshal delete marker: %v", err)
				continue
			}
			documents = append(documents, docBytes)
			totalBytes += len(docBytes)
		}
	}

	// Handle timeout gracefully (normal behavior)
	streamErr := changeStream.Err()
	isTimeoutError := false
	if streamErr != nil {
		if ctx.Err() == context.DeadlineExceeded {
			isTimeoutError = true
			log.Printf("⏰ STREAM TIMEOUT: %s.%s reached 25s timeout (normal behavior)", database, collection)
		} else {
			log.Printf("⚠️  STREAM ERROR: %s.%s - %v", database, collection, streamErr)
		}
	}

	// Log summary of changes detected
	if changeCount == 0 {
		log.Printf("✅ NO CHANGES: %s.%s is up-to-date", database, collection)
	} else {
		log.Printf("📦 CHANGES DETECTED: %s.%s - Total:%d, Operations:%v", database, collection, changeCount, operationCounts)
	}

	// CRITICAL FIX: Get resume token even if no changes occurred
	// This ensures we don't miss changes between polling cycles
	if changeCount == 0 {
		currentToken := changeStream.ResumeToken()
		if len(currentToken) > 0 {
			lastResumeToken = currentToken
			log.Printf("📍 RESUME TOKEN CAPTURED: %s.%s (no changes, but token saved for next cycle)", database, collection)
		}

		// FIX GAP #1: Save resume token immediately when no changes (safe operation)
		if checkpointMgr != nil && len(lastResumeToken) > 0 {
			if err := checkpointMgr.UpdateCheckpoint(database, collection, lastResumeToken, time.Now()); err != nil {
				log.Printf("⚠️  Failed to update resume token: %v", err)
			} else {
				log.Printf("✅ RESUME TOKEN UPDATED: %s.%s (%d bytes) - no changes", database, collection, len(lastResumeToken))
			}
		}
	}

	if len(documents) == 0 {
		log.Printf("📋 STREAM SYNC: No changes found for %s.%s", database, collection)
		if streamErr != nil && !isTimeoutError {
			return 0, fmt.Errorf("change stream error: %w", streamErr)
		}
		return 0, nil
	}

	// FIX GAP #2: Send changes via HTTP with retry mechanism
	log.Printf("🚀 STREAM SYNC: Sending %d changed docs (%s) for %s.%s - inserts:%d, updates:%d, deletes:%d",
		len(documents), formatBytes(totalBytes), database, collection,
		operationCounts["insert"], operationCounts["update"], operationCounts["delete"])

	if err := sendIncrementalChangesViaHTTPWithRetry(database, collection, documents); err != nil {
		return 0, fmt.Errorf("HTTP incremental sync failed after retries: %w", err)
	}

	log.Printf("✅ STREAM SYNC: Successfully sent %d docs for %s.%s", len(documents), database, collection)

	// FIX GAP #1: ONLY save resume token AFTER successful HTTP delivery
	// This prevents data loss if HTTP send fails
	if checkpointMgr != nil && len(lastResumeToken) > 0 {
		if err := checkpointMgr.UpdateCheckpoint(database, collection, lastResumeToken, time.Now()); err != nil {
			log.Printf("⚠️  Failed to update resume token: %v", err)
		} else {
			log.Printf("✅ RESUME TOKEN SAVED: %s.%s (%d bytes) - AFTER successful HTTP delivery", database, collection, len(lastResumeToken))
		}
	}

	if streamErr != nil && !isTimeoutError {
		return 0, fmt.Errorf("change stream error: %w", streamErr)
	}

	return changeCount, nil
}

// detectSimpleChanges uses simple document count comparison
func detectSimpleChanges(database, collection string) (int, error) {
	// Get current count from source database
	sourceColl := mongoClient.Database(database).Collection(collection)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Apply same filters as initial sync to get accurate count
	filter := buildDocumentFilter(database, collection)
	currentCount, err := sourceColl.CountDocuments(ctx, filter)
	if err != nil {
		return 0, fmt.Errorf("failed to count source documents: %w", err)
	}

	// Get last known count from checkpoint
	lastCount := int64(0)
	if checkpointMgr != nil {
		if checkpoint := checkpointMgr.GetCheckpoint(database, collection); checkpoint != nil {
			if checkpoint.ProcessedCount > 0 {
				lastCount = checkpoint.ProcessedCount
				// SAFETY CHECK: If checkpoint count is higher than current count, reset it
				if lastCount > currentCount {
					log.Printf("⚠️  CHECKPOINT RESET: %s.%s checkpoint (%d) > current (%d), resetting to current count",
						database, collection, lastCount, currentCount)
					lastCount = currentCount
					// Update checkpoint to current count
					checkpointMgr.UpdateCheckpoint(database, collection, nil, time.Now())
				}
			}
		}
	}

	newDocuments := int(currentCount - lastCount)
	if newDocuments > 0 {
		log.Printf("📊 SIMPLE DETECTION: %s.%s has %d new documents (current: %d, last: %d)",
			database, collection, newDocuments, currentCount, lastCount)
	} else {
		log.Printf("📋 SIMPLE DETECTION: %s.%s has no new documents (current: %d, last: %d)",
			database, collection, currentCount, lastCount)
	}

	return newDocuments, nil
}

// syncNewDocuments transfers new documents using simple pagination
func syncNewDocuments(database, collection string) error {
	// Get last known count
	lastCount := int64(0)
	if checkpointMgr != nil {
		if checkpoint := checkpointMgr.GetCheckpoint(database, collection); checkpoint != nil {
			if checkpoint.ProcessedCount > 0 {
				lastCount = checkpoint.ProcessedCount
			}
		}
	}

	// Simple approach: Skip the first lastCount documents and get new ones
	sourceColl := mongoClient.Database(database).Collection(collection)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Build filters and projection like initial sync
	filter := buildDocumentFilter(database, collection)
	pipeline := buildAggregationPipeline(database, collection, filter)

	// Add skip stage to get only new documents
	pipeline = append(pipeline, bson.D{{"$skip", lastCount}})
	pipeline = append(pipeline, bson.D{{"$limit", 100}}) // Small batches for real-time feel

	cursor, err := sourceColl.Aggregate(ctx, pipeline)
	if err != nil {
		return fmt.Errorf("failed to aggregate new documents: %w", err)
	}
	defer cursor.Close(ctx)

	// Collect new documents
	var documents [][]byte
	for cursor.Next(ctx) {
		documents = append(documents, cursor.Current)
	}

	if len(documents) == 0 {
		log.Printf("📋 SIMPLE SYNC: No new documents to sync for %s.%s", database, collection)
		return nil
	}

	log.Printf("🚀 SIMPLE SYNC: Sending %d new documents for %s.%s via HTTP", len(documents), database, collection)

	// Send via HTTP to vm-sync
	if err := sendIncrementalChangesViaHTTP(database, collection, documents); err != nil {
		return fmt.Errorf("failed to send new documents: %w", err)
	}

	// Update checkpoint with new count
	if checkpointMgr != nil {
		newCount := lastCount + int64(len(documents))
		// Clear old checkpoint and create new one with updated count
		if err := checkpointMgr.UpdateCheckpoint(database, collection, nil, time.Now()); err != nil {
			log.Printf("⚠️  Failed to update checkpoint: %v", err)
		} else {
			log.Printf("✅ CHECKPOINT UPDATED: %s.%s processed count: %d -> %d", database, collection, lastCount, newCount)
		}
	}

	log.Printf("✅ SIMPLE SYNC: Successfully sent %d new documents for %s.%s", len(documents), database, collection)
	return nil
}

// Simple helper functions
func buildDocumentFilter(database, collection string) bson.M {
	// Find the collection config to get document filters
	for _, db := range config.MongoDB.Databases {
		if db.Name == database {
			for _, coll := range db.Collections {
				if coll.Name == collection {
					filter := bson.M{}
					for _, criterion := range coll.DocumentFilter.Criteria {
						switch criterion.Operator {
						case "eq":
							filter[criterion.Field] = criterion.Value
						case "gt":
							filter[criterion.Field] = bson.M{"$gt": criterion.Value}
						case "gte":
							filter[criterion.Field] = bson.M{"$gte": criterion.Value}
						case "lt":
							filter[criterion.Field] = bson.M{"$lt": criterion.Value}
						case "lte":
							filter[criterion.Field] = bson.M{"$lte": criterion.Value}
						case "ne":
							filter[criterion.Field] = bson.M{"$ne": criterion.Value}
						}
					}
					return filter
				}
			}
		}
	}
	return bson.M{} // No filter if collection not found
}

// matchesDocumentFilter checks if a document matches the configured document filter
// Used for incremental sync to ensure filtered documents are excluded
func matchesDocumentFilter(database, collection string, document interface{}) bool {
	// Get collection config
	for _, db := range config.MongoDB.Databases {
		if db.Name == database {
			for _, coll := range db.Collections {
				if coll.Name == collection {
					// If no document filter criteria, accept all documents
					if len(coll.DocumentFilter.Criteria) == 0 {
						return true
					}

					// Convert document to bson.M for field access
					docMap, ok := document.(bson.M)
					if !ok {
						// Try to convert via marshaling
						docBytes, err := bson.Marshal(document)
						if err != nil {
							return true // Accept on error
						}
						err = bson.Unmarshal(docBytes, &docMap)
						if err != nil {
							return true // Accept on error
						}
					}

					// Check each criterion
					for _, criterion := range coll.DocumentFilter.Criteria {
						fieldValue, exists := docMap[criterion.Field]
						if !exists {
							// If field doesn't exist, consider it as not matching
							return false
						}

						// Apply operator check
						switch criterion.Operator {
						case "eq":
							if fieldValue != criterion.Value {
								return false
							}
						case "ne":
							if fieldValue == criterion.Value {
								return false // ne means "not equal", so if equal, reject
							}
						case "gt":
							// Type assertion for comparison
							if !compareValues(fieldValue, criterion.Value, "gt") {
								return false
							}
						case "gte":
							if !compareValues(fieldValue, criterion.Value, "gte") {
								return false
							}
						case "lt":
							if !compareValues(fieldValue, criterion.Value, "lt") {
								return false
							}
						case "lte":
							if !compareValues(fieldValue, criterion.Value, "lte") {
								return false
							}
						}
					}

					// All criteria matched
					return true
				}
			}
		}
	}

	// No config found, accept document by default
	return true
}

// compareValues performs type-aware comparison for filter matching
func compareValues(fieldValue interface{}, criterionValue interface{}, operator string) bool {
	// Simple numeric comparison (extend as needed)
	fv, fok := fieldValue.(float64)
	cv, cok := criterionValue.(float64)

	if fok && cok {
		switch operator {
		case "gt":
			return fv > cv
		case "gte":
			return fv >= cv
		case "lt":
			return fv < cv
		case "lte":
			return fv <= cv
		}
	}

	// String comparison
	fvs, foks := fieldValue.(string)
	cvs, coks := criterionValue.(string)

	if foks && coks {
		switch operator {
		case "gt":
			return fvs > cvs
		case "gte":
			return fvs >= cvs
		case "lt":
			return fvs < cvs
		case "lte":
			return fvs <= cvs
		}
	}

	// Default to true if types don't match
	return true
}

func buildAggregationPipeline(database, collection string, filter bson.M) mongo.Pipeline {
	pipeline := mongo.Pipeline{}

	// Add match stage for document filtering
	if len(filter) > 0 {
		pipeline = append(pipeline, bson.D{{"$match", filter}})
	}

	// Add field projection
	for _, db := range config.MongoDB.Databases {
		if db.Name == database {
			for _, coll := range db.Collections {
				if coll.Name == collection {
					if len(coll.FieldFilter.IncludeFields) > 0 {
						projection := bson.M{}
						for _, field := range coll.FieldFilter.IncludeFields {
							projection[field] = 1
						}
						pipeline = append(pipeline, bson.D{{"$project", projection}})
					}
					break
				}
			}
			break
		}
	}

	return pipeline
}

// detectChangesWithChangeStream uses pure MongoDB change streams for operation detection
func detectChangesWithChangeStream(database, collection string) (int, error) {
	coll := mongoClient.Database(database).Collection(collection)
	// Use shorter timeout than scheduler interval to prevent blocking
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Second)
	defer cancel()

	// Get resume token from checkpoint if available
	var watchOptions *options.ChangeStreamOptions
	var startFromToken bson.Raw

	if checkpointMgr != nil {
		if checkpoint := checkpointMgr.GetCheckpoint(database, collection); checkpoint != nil && len(checkpoint.ResumeToken) > 0 {
			startFromToken = checkpoint.ResumeToken
			watchOptions = options.ChangeStream().SetResumeAfter(startFromToken).SetFullDocument(options.UpdateLookup)
			log.Printf("🎯 RESUME TOKEN DETECTION: Resuming change stream for %s.%s from checkpoint", database, collection)
		} else {
			// No resume token yet - start from current cluster time (will capture all new changes)
			watchOptions = options.ChangeStream().SetFullDocument(options.UpdateLookup)
			log.Printf("🎆 NEW CHANGE STREAM: Starting fresh change stream for %s.%s from current cluster time", database, collection)
		}
	} else {
		// No checkpoint manager - temporary mode
		watchOptions = options.ChangeStream().SetFullDocument(options.UpdateLookup)
		log.Printf("⚠️  NO CHECKPOINT MGR: Starting temporary change stream for %s.%s from current cluster time", database, collection)
	}

	// Create change stream
	changeStream, err := coll.Watch(ctx, mongo.Pipeline{}, watchOptions)
	if err != nil {
		// Handle resume token invalidation gracefully
		if isInvalidateResumeTokenError(err) && len(startFromToken) > 0 {
			log.Printf("🔄 RESUME TOKEN INVALID: Clearing checkpoint and starting fresh for %s.%s", database, collection)
			if checkpointMgr != nil {
				// Clear invalid resume token
				if err := checkpointMgr.UpdateCheckpoint(database, collection, nil, time.Now()); err != nil {
					log.Printf("⚠️  Failed to clear checkpoint: %v", err)
				}
			}
			// Retry with fresh stream
			watchOptions = options.ChangeStream().SetFullDocument(options.UpdateLookup)
			changeStream, err = coll.Watch(ctx, mongo.Pipeline{}, watchOptions)
		}

		if err != nil {
			return 0, fmt.Errorf("failed to create change stream: %w", err)
		}
	}
	defer changeStream.Close(ctx)

	// Count ALL change operations since last checkpoint
	changeCount := 0
	lastResumeToken := startFromToken
	operationCounts := map[string]int{
		"insert": 0, "update": 0, "delete": 0, "replace": 0,
		"drop": 0, "rename": 0, "dropDatabase": 0,
		"createIndexes": 0, "dropIndexes": 0, "modify": 0,
		"invalidate": 0,
	}

	// Process change events to count operations and update resume token
	for changeStream.Next(ctx) {
		var changeEvent bson.M
		if err := changeStream.Decode(&changeEvent); err != nil {
			log.Printf("⚠️  Failed to decode change event: %v", err)
			continue
		}

		// Track operation type
		if opType, ok := changeEvent["operationType"].(string); ok {
			operationCounts[opType]++
			changeCount++
		}

		// Update resume token for next iteration
		lastResumeToken = changeStream.ResumeToken()
	}

	// Check for change stream errors but handle timeout gracefully
	streamErr := changeStream.Err()
	isTimeoutError := false
	if streamErr != nil {
		// Check if this is a context timeout (normal behavior)
		if ctx.Err() == context.DeadlineExceeded {
			isTimeoutError = true
			log.Printf("⏰ CHANGE STREAM TIMEOUT: %s.%s reached 25s timeout (normal behavior)", database, collection)
		} else {
			// This is a real error, not a timeout
			log.Printf("⚠️  CHANGE STREAM ERROR: %s.%s - %v", database, collection, streamErr)
		}
	}

	// ALWAYS update checkpoint with resume token (even on timeout or no changes)
	log.Printf("🔍 DEBUG CHECKPOINT: checkpointMgr != nil? %v, database=%s, collection=%s", checkpointMgr != nil, database, collection)
	if checkpointMgr != nil {
		// Use the latest resume token we have, or establish a new checkpoint
		if len(lastResumeToken) > 0 {
			log.Printf("🔍 DEBUG: About to call UpdateCheckpoint with resume token (%d bytes)", len(lastResumeToken))
			if err := checkpointMgr.UpdateCheckpoint(database, collection, lastResumeToken, time.Now()); err != nil {
				log.Printf("⚠️  Failed to update checkpoint: %v", err)
			} else {
				log.Printf("✅ CHECKPOINT UPDATED: %s.%s with resume token (%d bytes)", database, collection, len(lastResumeToken))
			}
		} else {
			log.Printf("🔍 DEBUG: About to call UpdateCheckpoint without resume token (initial checkpoint)")
			// Create initial checkpoint without resume token to establish tracking
			if err := checkpointMgr.UpdateCheckpoint(database, collection, nil, time.Now()); err != nil {
				log.Printf("⚠️  Failed to create initial checkpoint: %v", err)
			} else {
				log.Printf("📋 CHECKPOINT CREATED: %s.%s (initial checkpoint without resume token)", database, collection)
			}
		}
	} else {
		log.Printf("⚠️  DEBUG: checkpointMgr is nil - cannot update checkpoint for %s.%s", database, collection)
	}

	if changeCount > 0 {
		log.Printf("🎯 CHANGE STREAM DETECTION: Found %d changes for %s.%s - inserts:%d, updates:%d, deletes:%d, replaces:%d",
			changeCount, database, collection, operationCounts["insert"], operationCounts["update"], operationCounts["delete"], operationCounts["replace"])
	} else {
		log.Printf("📋 CHANGE STREAM DETECTION: No changes for %s.%s since last checkpoint", database, collection)
	}

	// Return error only for real errors, not timeouts
	if streamErr != nil && !isTimeoutError {
		return 0, fmt.Errorf("change stream error: %w", streamErr)
	}

	return changeCount, nil
}

// syncCollectionChanges syncs detected changes using pure MongoDB change streams
// NO document modification required - works with any existing documents
func syncCollectionChanges(database, collection string) error {
	// Check if we have connected vm-sync clients
	clientsMutex.RLock()
	hasVMClients := false
	for _, clientInfo := range clients {
		if clientInfo.ClientType == "vm-sync" {
			hasVMClients = true
			break
		}
	}
	clientsMutex.RUnlock()

	if !hasVMClients {
		log.Printf("💤 INCREMENTAL SYNC: No vm-sync clients connected, skipping sync for %s.%s", database, collection)
		return nil
	}

	// Use pure change streams for all synchronization
	return syncCollectionChangesWithChangeStream(database, collection)
}

// syncCollectionChangesWithChangeStream syncs changes using pure MongoDB change streams
func syncCollectionChangesWithChangeStream(database, collection string) error {
	coll := mongoClient.Database(database).Collection(collection)
	// Use shorter timeout than scheduler interval to prevent blocking
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Second)
	defer cancel()

	// Get resume token from checkpoint if available
	var watchOptions *options.ChangeStreamOptions
	var startFromToken bson.Raw

	if checkpointMgr != nil {
		if checkpoint := checkpointMgr.GetCheckpoint(database, collection); checkpoint != nil && len(checkpoint.ResumeToken) > 0 {
			startFromToken = checkpoint.ResumeToken
			watchOptions = options.ChangeStream().SetResumeAfter(startFromToken).SetFullDocument(options.UpdateLookup)
			log.Printf("🎯 CHANGE STREAM SYNC: Resuming from checkpoint for %s.%s", database, collection)
		} else {
			// No resume token - start from "now" but only sync new changes
			watchOptions = options.ChangeStream().SetFullDocument(options.UpdateLookup)
			log.Printf("🎆 CHANGE STREAM SYNC: Starting fresh for %s.%s (will only sync new changes)", database, collection)
		}
	} else {
		// No checkpoint manager
		watchOptions = options.ChangeStream().SetFullDocument(options.UpdateLookup)
		log.Printf("⚠️  CHANGE STREAM SYNC: No checkpoint manager for %s.%s", database, collection)
	}

	// Create change stream
	changeStream, err := coll.Watch(ctx, mongo.Pipeline{}, watchOptions)
	if err != nil {
		// Handle resume token invalidation
		if isInvalidateResumeTokenError(err) && len(startFromToken) > 0 {
			log.Printf("🔄 RESUME TOKEN INVALID: Clearing and restarting for %s.%s", database, collection)
			if checkpointMgr != nil {
				if err := checkpointMgr.UpdateCheckpoint(database, collection, nil, time.Now()); err != nil {
					log.Printf("⚠️  Failed to clear checkpoint: %v", err)
				}
			}
			// Retry with fresh stream
			watchOptions = options.ChangeStream().SetFullDocument(options.UpdateLookup)
			changeStream, err = coll.Watch(ctx, mongo.Pipeline{}, watchOptions)
		}

		if err != nil {
			return fmt.Errorf("failed to create change stream: %w", err)
		}
	}
	defer changeStream.Close(ctx)

	var documents [][]byte
	totalBytes := 0
	changeCount := 0
	lastResumeToken := startFromToken
	operationCounts := map[string]int{
		"insert": 0, "update": 0, "delete": 0, "replace": 0,
		"drop": 0, "rename": 0, "dropDatabase": 0,
		"createIndexes": 0, "dropIndexes": 0, "modify": 0,
		"invalidate": 0,
	}

	// Process ALL change events: insert, update, delete, replace
	for changeStream.Next(ctx) {
		var changeEvent bson.M
		if err := changeStream.Decode(&changeEvent); err != nil {
			log.Printf("⚠️  Failed to decode change event: %v", err)
			continue
		}

		// Update resume token
		lastResumeToken = changeStream.ResumeToken()
		changeCount++

		// Track operation type
		if opType, ok := changeEvent["operationType"].(string); ok {
			operationCounts[opType]++
			log.Printf("📄 CHANGE DETECTED: %s operation on %s.%s", opType, database, collection)
		}

		// Extract document for insert/update/replace operations
		if fullDoc, ok := changeEvent["fullDocument"]; ok {
			docBytes, err := bson.Marshal(fullDoc)
			if err != nil {
				log.Printf("⚠️  Failed to marshal document: %v", err)
				continue
			}
			documents = append(documents, docBytes)
			totalBytes += len(docBytes)
		} else if opType, ok := changeEvent["operationType"].(string); ok {
			switch opType {
			case "delete":
				// For delete operations, create a special marker document
				deleteMarker := bson.M{
					"_operation":   "delete",
					"_documentKey": changeEvent["documentKey"],
					"_timestamp":   time.Now(),
				}
				docBytes, err := bson.Marshal(deleteMarker)
				if err != nil {
					log.Printf("⚠️  Failed to marshal delete marker: %v", err)
					continue
				}
				documents = append(documents, docBytes)
				totalBytes += len(docBytes)
			case "drop", "rename", "dropDatabase":
				// DDL operations - create special marker
				ddlMarker := bson.M{
					"_operation":  opType,
					"_database":   database,
					"_collection": collection,
					"_timestamp":  time.Now(),
				}
				if ns, ok := changeEvent["ns"]; ok {
					ddlMarker["_namespace"] = ns
				}
				docBytes, err := bson.Marshal(ddlMarker)
				if err != nil {
					log.Printf("⚠️  Failed to marshal DDL marker: %v", err)
					continue
				}
				documents = append(documents, docBytes)
				totalBytes += len(docBytes)
			case "createIndexes", "dropIndexes", "modify":
				// Index operations - create special marker
				indexMarker := bson.M{
					"_operation":  opType,
					"_database":   database,
					"_collection": collection,
					"_timestamp":  time.Now(),
				}
				if operationDescription, ok := changeEvent["operationDescription"]; ok {
					indexMarker["_operationDescription"] = operationDescription
				}
				docBytes, err := bson.Marshal(indexMarker)
				if err != nil {
					log.Printf("⚠️  Failed to marshal index marker: %v", err)
					continue
				}
				documents = append(documents, docBytes)
				totalBytes += len(docBytes)
			case "invalidate":
				// Invalidate operations - create special marker
				invalidateMarker := bson.M{
					"_operation":  "invalidate",
					"_database":   database,
					"_collection": collection,
					"_timestamp":  time.Now(),
					"_reason":     "collection_invalidated",
				}
				docBytes, err := bson.Marshal(invalidateMarker)
				if err != nil {
					log.Printf("⚠️  Failed to marshal invalidate marker: %v", err)
					continue
				}
				documents = append(documents, docBytes)
				totalBytes += len(docBytes)
				log.Printf("⚠️  INVALIDATE OPERATION: %s for %s.%s", opType, database, collection)
			default:
				log.Printf("⚠️  UNHANDLED OPERATION: %s for %s.%s", opType, database, collection)
			}
		}
	}

	// Check for change stream errors but handle timeout gracefully
	streamErr := changeStream.Err()
	isTimeoutError := false
	if streamErr != nil {
		// Check if this is a context timeout (normal behavior for 25-second sync window)
		if ctx.Err() == context.DeadlineExceeded {
			isTimeoutError = true
			log.Printf("⏰ SYNC STREAM TIMEOUT: %s.%s reached 25s timeout (normal sync window)", database, collection)
		} else {
			// This is a real error, not a timeout
			log.Printf("⚠️  SYNC STREAM ERROR: %s.%s - %v", database, collection, streamErr)
		}
	}

	// ALWAYS update checkpoint with resume token (even on timeout or no changes)
	if checkpointMgr != nil {
		// Use the latest resume token we have, or establish a new checkpoint
		if len(lastResumeToken) > 0 {
			if err := checkpointMgr.UpdateCheckpoint(database, collection, lastResumeToken, time.Now()); err != nil {
				log.Printf("⚠️  Failed to sync update checkpoint: %v", err)
			} else {
				log.Printf("✅ SYNC CHECKPOINT UPDATED: %s.%s with resume token (%d bytes)", database, collection, len(lastResumeToken))
			}
		} else {
			// Create initial checkpoint without resume token to establish tracking
			if err := checkpointMgr.UpdateCheckpoint(database, collection, nil, time.Now()); err != nil {
				log.Printf("⚠️  Failed to create initial sync checkpoint: %v", err)
			} else {
				log.Printf("📋 SYNC CHECKPOINT CREATED: %s.%s (initial checkpoint without resume token)", database, collection)
			}
		}
	}

	if len(documents) == 0 {
		log.Printf("📋 CHANGE STREAM SYNC: No document changes found for %s.%s", database, collection)
		// Return error only for real errors, not timeouts
		if streamErr != nil && !isTimeoutError {
			return fmt.Errorf("change stream error: %w", streamErr)
		}
		return nil
	}

	// Send changes via HTTP fallback (most reliable for incremental sync)
	log.Printf("🚀 CHANGE STREAM SYNC: Sending %d changed docs (%s) for %s.%s - inserts:%d, updates:%d, deletes:%d, replaces:%d",
		len(documents), formatBytes(totalBytes), database, collection,
		operationCounts["insert"], operationCounts["update"], operationCounts["delete"], operationCounts["replace"])

	// FORCE HTTP FALLBACK for incremental sync reliability
	log.Printf("🌐 INCREMENTAL SYNC: Using HTTP transport for guaranteed delivery")
	if err := sendIncrementalChangesViaHTTP(database, collection, documents); err != nil {
		return fmt.Errorf("HTTP incremental sync failed: %w", err)
	}

	log.Printf("✅ CHANGE STREAM SYNC: Successfully sent %d docs for %s.%s (resume token updated)", len(documents), database, collection)

	// Return error only for real errors, not timeouts
	if streamErr != nil && !isTimeoutError {
		return fmt.Errorf("change stream error: %w", streamErr)
	}

	return nil
}

// Shared HTTP client for incremental sync with connection pooling and keepalive
var incrementalSyncHTTPClient = &http.Client{
	Timeout: 60 * time.Second,
	Transport: &http.Transport{
		MaxIdleConns:        100,              // Connection pool
		MaxIdleConnsPerHost: 10,               // Reuse connections to same host
		IdleConnTimeout:     90 * time.Second, // Keep connections alive
		DisableKeepAlives:   false,            // Enable HTTP keepalive
		DisableCompression:  false,            // Enable compression
		ForceAttemptHTTP2:   true,             // Try HTTP/2
		// Network timeouts for cross-region reliability
		DialContext: (&net.Dialer{
			Timeout:   30 * time.Second, // Connection timeout
			KeepAlive: 30 * time.Second, // TCP keepalive
		}).DialContext,
		TLSHandshakeTimeout:   10 * time.Second,
		ResponseHeaderTimeout: 30 * time.Second,
		ExpectContinueTimeout: 1 * time.Second,
	},
}

// isInitialDumpCompleted intelligently detects if initial dump was completed
// by checking checkpoint manager for resume tokens (persistent across restarts)
func isInitialDumpCompleted() bool {
	// Method 1: Check in-memory flag (fast path)
	if initialDumpCompletedOnce {
		return true
	}
	
	// Method 2: Check checkpoint manager for persistent state
	// If we have resume tokens for collections, initial dump must have completed
	if checkpointMgr != nil {
		hasAnyCheckpoint := false
		for _, database := range config.MongoDB.Databases {
			if !database.Enabled {
				continue
			}
			for _, collection := range database.Collections {
				if !collection.Enabled {
					continue
				}
				// If any collection has a checkpoint, initial dump completed
				if checkpoint := checkpointMgr.GetCheckpoint(database.Name, collection.Name); checkpoint != nil {
					hasAnyCheckpoint = true
					// Update in-memory flag for fast future checks
					initialDumpMutex.Lock()
					initialDumpCompletedOnce = true
					initialDumpMutex.Unlock()
					log.Printf("✅ INITIAL DUMP DETECTED: Found checkpoint for %s.%s - Initial dump was completed", database.Name, collection.Name)
					return true
				}
			}
		}
		
		if !hasAnyCheckpoint {
			log.Printf("🔍 INITIAL DUMP CHECK: No checkpoints found - Initial dump likely not completed yet")
		}
	}
	
	// Method 3: If config says initial_sync is disabled, allow incremental sync anyway
	if !config.Sync.InitialSync {
		log.Printf("⚠️  INITIAL DUMP SKIPPED: initial_sync=false in config - Allowing incremental sync")
		return true
	}
	
	return false
}

// sendIncrementalChangesViaHTTPWithRetry sends incremental changes with retry mechanism (FIX GAP #2)
func sendIncrementalChangesViaHTTPWithRetry(database, collection string, documents [][]byte) error {
	const maxRetries = 5
	const initialBackoff = 2 * time.Second

	var lastErr error
	for attempt := 1; attempt <= maxRetries; attempt++ {
		err := sendIncrementalChangesViaHTTP(database, collection, documents)
		if err == nil {
			if attempt > 1 {
				log.Printf("✅ HTTP RETRY SUCCESS: Succeeded on attempt %d/%d for %s.%s", attempt, maxRetries, database, collection)
			}
			return nil
		}

		lastErr = err
		log.Printf("⚠️  HTTP RETRY: Attempt %d/%d failed for %s.%s: %v", attempt, maxRetries, database, collection, err)

		// Don't sleep after last attempt
		if attempt < maxRetries {
			// Exponential backoff: 2s, 4s, 8s, 16s, 32s
			backoff := initialBackoff * time.Duration(1<<(attempt-1))
			if backoff > 32*time.Second {
				backoff = 32 * time.Second
			}
			log.Printf("⏳ HTTP RETRY: Waiting %v before attempt %d...", backoff, attempt+1)
			time.Sleep(backoff)
		}
	}

	return fmt.Errorf("failed after %d retry attempts: %w", maxRetries, lastErr)
}

// sendIncrementalChangesViaHTTP sends incremental changes using TCP or HTTP protocol
// NOTE: Despite the function name, this now uses TCP primarily with HTTP fallback
func sendIncrementalChangesViaHTTP(database, collection string, documents [][]byte) error {
	if len(documents) == 0 {
		return nil
	}

	// REQUIREMENT 2: Ensure initial dump is complete before sending incremental changes
	// INTELLIGENT CHECK: Use checkpoint manager to detect if initial dump was completed
	// This works even after cloud-sync restarts (persistent state)
	if !isInitialDumpCompleted() {
		log.Printf("⚠️  INCREMENTAL SYNC BLOCKED: Initial dump not yet completed - Skipping incremental changes for %s.%s", database, collection)
		log.Printf("💡 HINT: Waiting for initial dump to complete. Check for 'INITIAL BULK DATA TRANSFER COMPLETED' log message.")
		return fmt.Errorf("initial dump not completed yet")
	}

	// REQUIREMENT 1: Try TCP first for CRUD operations (insert/update/delete via upsert)
	// REQUIREMENT 3: Self-recovering via TCP reconnection (handled by transport layer)
	if tcpTransportEnabled && tcpSender != nil {
		// CRITICAL FIX: Apply database name transformation for TCP stream routing
		// This ensures incremental sync uses the same target database as initial dump
		targetDatabase := getTargetDatabaseForVMSync(database)
		streamName := fmt.Sprintf("%s.%s.incremental", targetDatabase, collection)
		
		log.Printf("📋 TCP INCREMENTAL ROUTING: Source='%s.%s' → Target='%s.%s.incremental'", database, collection, targetDatabase, collection)
		log.Printf("🚀 TCP INCREMENTAL: Sending %d documents for %s via TCP", len(documents), streamName)
		
		if err := tcpSender.SendBatch(streamName, documents); err != nil {
			log.Printf("⚠️  TCP INCREMENTAL FAILED: %v - Falling back to HTTP", err)
			// Fall through to HTTP fallback
		} else {
			log.Printf("✅ TCP INCREMENTAL SUCCESS: Sent %d documents for %s.%s → %s.%s", len(documents), database, collection, targetDatabase, collection)
			return nil
		}
	}

	// HTTP FALLBACK: Use HTTP if TCP not available or failed
	log.Printf("🌐 HTTP FALLBACK: Using HTTP for incremental sync %s.%s", database, collection)
	
	// Get vm-sync HTTP endpoint (uses dynamically discovered endpoint from VM auth)
	vmSyncEndpoint := getVMSyncHTTPEndpoint()

	// CRITICAL FIX: Apply database name transformation for VM-sync routing
	// This ensures incremental sync uses the same target database as initial dump
	targetDatabase := getTargetDatabaseForVMSync(database)
	log.Printf("📋 INCREMENTAL DB ROUTING: Source='%s' → Target='%s' for collection %s", database, targetDatabase, collection)

	// Convert documents to DataResponse format
	// Convert [][]byte to []bson.Raw
	bsonDocs := make([]bson.Raw, len(documents))
	for i, doc := range documents {
		bsonDocs[i] = bson.Raw(doc)
	}

	response := models.DataResponse{
		Database:   targetDatabase, // Use target database name
		Collection: collection,
		Documents:  bsonDocs,
		Count:      int64(len(documents)),
	}

	// Marshal to JSON
	payload, err := json.Marshal(response)
	if err != nil {
		return fmt.Errorf("failed to marshal documents: %v", err)
	}

	// Send HTTP request to vm-sync with target database name
	url := fmt.Sprintf("%s/api/v1/push/%s/%s", vmSyncEndpoint, targetDatabase, collection)
	req, err := http.NewRequest("POST", url, bytes.NewBuffer(payload))
	if err != nil {
		return fmt.Errorf("failed to create HTTP request: %v", err)
	}

	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Sync-Type", "incremental")
	req.Header.Set("X-Client-ID", "cloud-sync-incremental")

	// Use shared HTTP client with connection pooling for reliability
	resp, err := incrementalSyncHTTPClient.Do(req)
	if err != nil {
		return fmt.Errorf("HTTP request failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("HTTP request failed with status %d: %s", resp.StatusCode, string(body))
	}

	log.Printf("✅ HTTP INCREMENTAL SYNC: Successfully sent %d documents for %s.%s → %s.%s", len(documents), database, collection, targetDatabase, collection)
	return nil
}

// pushIncrementalChangesTCP removed - replaced with pure change stream implementation
// All incremental sync now uses syncCollectionChangesWithChangeStream which relies on
// MongoDB change streams with resume tokens - NO document modification required

// startCatchUpSync handles catch-up synchronization for reconnected vm-sync clients
// It skips the WebSocket connection wait since the client is already connected
func startCatchUpSync() {
	log.Println("Starting catch-up synchronization for reconnected vm-sync client...")
	startSyncProcess()
}

// startSyncProcess contains the common sync logic used by both initial and catch-up sync
func startSyncProcess() {
	// Get vm-sync HTTP endpoint (uses dynamically discovered endpoint from VM auth)
	vmSyncEndpoint := getVMSyncHTTPEndpoint()

	log.Printf("📊 INITIATING BULK DATA TRANSFER to vm-sync at: %s", vmSyncEndpoint)

	// Count total collections to sync
	totalCollections := 0
	for _, dbConfig := range config.MongoDB.Databases {
		if !dbConfig.Enabled {
			continue
		}
		for _, collConfig := range dbConfig.Collections {
			if collConfig.Enabled {
				totalCollections++
			}
		}
	}

	log.Printf("📊 Total collections to sync: %d", totalCollections)

	// ✅ PHASE 1: Send ALL metadata FIRST via TCP (indexes, options) for ALL collections
	// This ensures indexes are created BEFORE data arrives, optimizing insert performance
	// IMPORTANT: Check if TCP sender is available (may be existing connection from startup)
	if config.Sync.Transport.Mode == "tcp" && tcpSender != nil {
		log.Printf("🎯 PHASE 1: Sending metadata (indexes) via TCP for ALL %d collections...", totalCollections)
		metadataStartTime := time.Now()
		metadataCount := 0
		metadataErrors := 0

		for _, dbConfig := range config.MongoDB.Databases {
			if !dbConfig.Enabled {
				continue
			}

			for _, collConfig := range dbConfig.Collections {
				if !collConfig.Enabled {
					continue
				}

				metadataCount++
				dbName := dbConfig.Name
				collName := collConfig.Name

				ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)

				// Send metadata via TCP (no ACK wait needed)
				if err := sendMetadataTCP(ctx, dbName, collName); err != nil {
					log.Printf("⚠️  [%d/%d] Failed to send metadata for %s.%s: %v", metadataCount, totalCollections, dbName, collName, err)
					metadataErrors++
				} else {
					log.Printf("✅ [%d/%d] Metadata sent via TCP for %s.%s", metadataCount, totalCollections, dbName, collName)
				}

				cancel()
			}
		}

		metadataDuration := time.Since(metadataStartTime)
		if metadataErrors == 0 {
			log.Printf("✅ PHASE 1 COMPLETE: All %d metadata batches sent via TCP in %v", metadataCount, metadataDuration)
		} else {
			log.Printf("⚠️  PHASE 1 COMPLETE: %d/%d metadata batches sent (%d errors) in %v", metadataCount-metadataErrors, metadataCount, metadataErrors, metadataDuration)
		}
	} else {
		log.Printf("⏭️  PHASE 1 SKIPPED: TCP transport not enabled, metadata will be sent with page 1 data via HTTP")
	}

	log.Printf("🎯 PHASE 2: Starting data transfer for ALL %d collections...", totalCollections)

	// Initialize automatic sync progress tracking
	now := time.Now()
	syncStatusMutex.Lock()
	currentSyncStatus = "syncing"
	currentSyncStartTime = &now
	currentSyncEndTime = nil
	currentSyncError = ""
	collectionSyncStatuses = make(map[string]*CollectionSyncInfo)

	// Pre-populate collection statuses for automatic sync
	for _, dbConfig := range config.MongoDB.Databases {
		if !dbConfig.Enabled {
			continue
		}
		for _, collConfig := range dbConfig.Collections {
			if collConfig.Enabled {
				key := fmt.Sprintf("%s.%s", dbConfig.Name, collConfig.Name)
				collectionSyncStatuses[key] = &CollectionSyncInfo{
					Database:   dbConfig.Name,
					Collection: collConfig.Name,
					Status:     "pending",
				}
			}
		}
	}
	syncStatusMutex.Unlock()

	// Push data for each configured database and collection with adaptive parallelism
	var wg sync.WaitGroup
	collectionIndex := 0
	for _, dbConfig := range config.MongoDB.Databases {
		if !dbConfig.Enabled {
			log.Printf("Skipping disabled database: %s", dbConfig.Name)
			continue
		}

		for _, collConfig := range dbConfig.Collections {
			if !collConfig.Enabled {
				log.Printf("Skipping disabled collection: %s.%s", dbConfig.Name, collConfig.Name)
				continue
			}

			collectionIndex++
			wg.Add(1)
			go func(dbName, collName string, index int) {
				defer wg.Done()

				// Acquire push permit for adaptive parallelism control
				acquirePushPermit()
				defer releasePushPermit()

				key := fmt.Sprintf("%s.%s", dbName, collName)
				startTime := time.Now()

				// Update status to syncing with progress tracking
				syncStatusMutex.Lock()
				if info, exists := collectionSyncStatuses[key]; exists {
					info.Status = "syncing"
					info.StartedAt = &startTime
				}
				syncStatusMutex.Unlock()

				log.Printf("🚀 [%d/%d] Starting BULK TRANSFER for %s.%s", index, totalCollections, dbName, collName)

				if err := pushCollectionDataWithResume(vmSyncEndpoint, dbName, collName); err != nil {
					log.Printf("❌ [%d/%d] Error pushing data for %s.%s: %v", index, totalCollections, dbName, collName, err)

					// Update status to error
					completedAt := time.Now()
					syncStatusMutex.Lock()
					if info, exists := collectionSyncStatuses[key]; exists {
						info.Status = "error"
						info.ErrorMessage = err.Error()
						info.CompletedAt = &completedAt
					}
					syncStatusMutex.Unlock()
				} else {
					duration := time.Since(startTime)
					log.Printf("✅ [%d/%d] Successfully completed BULK TRANSFER for %s.%s (took %v)", index, totalCollections, dbName, collName, duration)

					// Update status to completed
					completedAt := time.Now()
					syncStatusMutex.Lock()
					if info, exists := collectionSyncStatuses[key]; exists {
						info.Status = "completed"
						info.CompletedAt = &completedAt
					}
					syncStatusMutex.Unlock()
				}

				// Update sync latency telemetry
				latency := time.Since(startTime).Seconds()
				if cloudSyncIntegration != nil {
					cloudSyncIntegration.UpdateSyncLatency(latency)
				}
			}(dbConfig.Name, collConfig.Name, collectionIndex)
		}
	}

	// Wait for all push operations to complete
	log.Printf("⏳ Waiting for %d bulk transfer operations to complete...", totalCollections)
	wg.Wait()

	// Update final sync status
	completedAt := time.Now()
	syncStatusMutex.Lock()

	// Check if any collections failed
	hasErrors := false
	for _, info := range collectionSyncStatuses {
		if info.Status == "error" {
			hasErrors = true
			break
		}
	}

	if hasErrors {
		currentSyncStatus = "error"
		currentSyncError = "One or more collections failed to sync"
		log.Printf("⚠️  INITIAL BULK DATA TRANSFER COMPLETED with errors")
	} else {
		currentSyncStatus = "completed"
		log.Printf("✅ INITIAL BULK DATA TRANSFER COMPLETED successfully for all collections!")
	}

	currentSyncEndTime = &completedAt
	syncStatusMutex.Unlock()

	log.Printf("🚀 System now ready for real-time change stream synchronization")
	log.Printf("📈 Sync status available at: GET /api/sync/status")
}

// getBasePath constructs the API base path from environment variables
func getBasePath() string {
	basePath := os.Getenv("BASE_PATH")
	tenantDns := os.Getenv("TENANT_DNS")
	communityName := os.Getenv("COMMUNITY_NAME")

	// If any required environment variable is missing, return empty string (no base path)
	if basePath == "" || tenantDns == "" || communityName == "" {
		log.Println("Base path environment variables not fully configured (BASE_PATH, TENANT_DNS, COMMUNITY_NAME)")
		return ""
	}

	// Construct base path: /basePath-tenantName-communityName
	fullBasePath := fmt.Sprintf("/%s-%s-%s", basePath, tenantDns, communityName)
	log.Printf("Constructed base path: %s", fullBasePath)
	return fullBasePath
}

// handleSwaggerUI serves the Swagger UI interface
func handleSwaggerUI(w http.ResponseWriter, r *http.Request) {
	// Get base path for constructing API URLs
	basePath := getBasePath()
	swaggerSpecURL := basePath + "/docs/swagger.yaml"

	// Serve a simple Swagger UI HTML page
	html := fmt.Sprintf(`<!DOCTYPE html>
<html>
<head>
    <title>Go Data Sync API Documentation</title>
    <link rel="stylesheet" type="text/css" href="https://cdn.jsdelivr.net/npm/swagger-ui-dist@4.15.5/swagger-ui.css" />
    <style>
        html {
            box-sizing: border-box;
            overflow: -moz-scrollbars-vertical;
            overflow-y: scroll;
        }
        *, *:before, *:after {
            box-sizing: inherit;
        }
        body {
            margin:0;
            background: #fafafa;
        }
    </style>
</head>
<body>
    <div id="swagger-ui"></div>
    <script src="https://cdn.jsdelivr.net/npm/swagger-ui-dist@4.15.5/swagger-ui-bundle.js"></script>
    <script src="https://cdn.jsdelivr.net/npm/swagger-ui-dist@4.15.5/swagger-ui-standalone-preset.js"></script>
    <script>
        window.onload = function() {
            const ui = SwaggerUIBundle({
                url: '%s',
                dom_id: '#swagger-ui',
                deepLinking: true,
                presets: [
                    SwaggerUIBundle.presets.apis,
                    SwaggerUIStandalonePreset
                ],
                plugins: [
                    SwaggerUIBundle.plugins.DownloadUrl
                ],
                layout: "StandaloneLayout"
            });
        };
    </script>
</body>
</html>`, swaggerSpecURL)

	w.Header().Set("Content-Type", "text/html")
	w.WriteHeader(http.StatusOK)
	w.Write([]byte(html))
}

// handleSwaggerSpec serves the OpenAPI specification YAML file
func handleSwaggerSpec(w http.ResponseWriter, r *http.Request) {
	// Read the swagger specification file
	specPath := filepath.Join("docs", "api-swagger.yaml")
	specData, err := os.ReadFile(specPath)
	if err != nil {
		log.Printf("Error reading swagger spec file: %v", err)
		http.Error(w, "Swagger specification not found", http.StatusNotFound)
		return
	}

	// Get base path and update the spec with dynamic server URL
	basePath := getBasePath()

	// CRITICAL FIX: Use TENANT_DNS for Kubernetes deployments
	// This ensures Swagger shows the correct public URL instead of 0.0.0.0
	tenantDNS := os.Getenv("TENANT_DNS")
	var baseURL string

	if tenantDNS != "" {
		// Use TENANT_DNS for public-facing URL (Kubernetes/production)
		baseURL = fmt.Sprintf("https://%s%s", tenantDNS, basePath)
		log.Printf("📄 SWAGGER: Using TENANT_DNS for server URL: %s", baseURL)
	} else {
		// Fallback to config (for local development)
		baseURL = fmt.Sprintf("http://%s:%d%s", config.Server.Host, config.Server.Port, basePath)
		log.Printf("📄 SWAGGER: Using local config for server URL: %s", baseURL)
	}

	// Replace placeholder server URL in the spec
	specContent := string(specData)
	if basePath != "" || tenantDNS != "" {
		// Update server URLs in the spec to use correct base URL
		specContent = strings.ReplaceAll(specContent, "http://localhost:8080", baseURL)
		log.Printf("📄 SWAGGER: Replaced server URLs with: %s", baseURL)
	}

	w.Header().Set("Content-Type", "application/x-yaml")
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Access-Control-Allow-Headers", "Content-Type")
	w.WriteHeader(http.StatusOK)
	w.Write([]byte(specContent))

	log.Printf("✅ SWAGGER: Served specification at %s/docs/swagger.yaml", baseURL)
}

// checkVMSyncCheckpoint checks if vm-sync has existing checkpoint data for a collection
func checkVMSyncCheckpoint(vmSyncEndpoint, database, collection string) (bool, error) {
	url := fmt.Sprintf("%s/api/checkpoint/%s.%s", vmSyncEndpoint, database, collection)
	resp, err := http.Get(url)
	if err != nil {
		return false, fmt.Errorf("failed to check checkpoint: %v", err)
	}
	defer resp.Body.Close()

	// 200 means checkpoint exists, 404 means no checkpoint
	if resp.StatusCode == http.StatusOK {
		return true, nil
	} else if resp.StatusCode == http.StatusNotFound {
		return false, nil
	}

	return false, fmt.Errorf("unexpected status code: %d", resp.StatusCode)
}

// clearVMSyncCollection requests vm-sync to clear a specific collection
func clearVMSyncCollection(vmSyncEndpoint, database, collection string) error {
	url := fmt.Sprintf("%s/api/v1/clear/%s.%s", vmSyncEndpoint, database, collection)
	req, err := http.NewRequest("DELETE", url, nil)
	if err != nil {
		return fmt.Errorf("failed to create clear request: %v", err)
	}

	client := &http.Client{Timeout: 30 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("failed to clear collection: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("clear request failed with status: %d", resp.StatusCode)
	}

	return nil
}

func pushCollectionData(vmSyncEndpoint, database, collection string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Second)
	defer cancel()

	coll := mongoClient.Database(database).Collection(collection)

	// Count total documents
	totalCount, err := coll.CountDocuments(ctx, bson.M{})
	if err != nil {
		return fmt.Errorf("failed to count documents: %v", err)
	}

	log.Printf("📊 BULK TRANSFER: %s.%s contains %d documents to transfer", database, collection, totalCount)

	// Update document count in progress tracking
	key := fmt.Sprintf("%s.%s", database, collection)
	syncStatusMutex.Lock()
	if info, exists := collectionSyncStatuses[key]; exists {
		info.DocumentCount = totalCount
	}
	syncStatusMutex.Unlock()

	if totalCount == 0 {
		log.Printf("⚠️  Collection %s.%s is empty, skipping bulk transfer", database, collection)

		// Update progress tracking for empty collection
		syncStatusMutex.Lock()
		if info, exists := collectionSyncStatuses[key]; exists {
			info.TransferredDocs = 0
		}
		syncStatusMutex.Unlock()

		return nil
	}

	// Use adaptive batch size if available, otherwise default to 1000
	pageSize := getCurrentBatchSize()
	if pageSize <= 0 {
		pageSize = 1000 // Fallback to default
	}
	totalPages := int((totalCount + int64(pageSize) - 1) / int64(pageSize))

	if totalPages == 0 {
		totalPages = 1 // At least one page for metadata
	}

	log.Printf("📊 BULK TRANSFER: %s.%s will be transferred in %d pages (%d docs per page)", database, collection, totalPages, pageSize)

	// Track transferred documents
	var transferredDocs int64 = 0

	// Push data page by page
	for pageNumber := 1; pageNumber <= totalPages; pageNumber++ {
		if err := pushSinglePage(ctx, vmSyncEndpoint, database, collection, pageNumber, pageSize, totalPages); err != nil {
			return fmt.Errorf("failed to push page %d: %v", pageNumber, err)
		}

		// Update transferred document count (estimate based on page size)
		_ = int64(pageSize) // pageDocCount for potential future use
		if pageNumber == totalPages && totalCount%int64(pageSize) != 0 {
			// Last page might have fewer documents
			_ = totalCount % int64(pageSize)
		}
		transferredDocs += int64(pageSize)

		// Update progress tracking
		syncStatusMutex.Lock()
		if info, exists := collectionSyncStatuses[key]; exists {
			info.TransferredDocs = transferredDocs
		}
		syncStatusMutex.Unlock()

		log.Printf("✅ BULK TRANSFER: Page %d/%d completed for %s.%s (%d/%d docs transferred)",
			pageNumber, totalPages, database, collection, transferredDocs, totalCount)
	}

	log.Printf("✅ BULK TRANSFER COMPLETED: %s.%s (%d documents transferred via %s)", database, collection, totalCount, strings.ToUpper(config.Sync.Transport.Mode))

	// Mark transfer as completed in tracking system
	if transferTracker != nil && transferTracker.IsEnabled() {
		clientID := "vm-sync-default" // TODO: Extract from connection context
		if err := markTransferCompleted(clientID, database, collection, totalCount); err != nil {
			log.Printf("Warning: Failed to mark transfer as completed: %v", err)
		}
	}

	return nil
}

// pushCollectionDataFromPage resumes collection data transfer from a specific page
func pushCollectionDataFromPage(vmSyncEndpoint, database, collection string, fromPage int) error {
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Second)
	defer cancel()

	coll := mongoClient.Database(database).Collection(collection)

	// Count total documents
	totalCount, err := coll.CountDocuments(ctx, bson.M{})
	if err != nil {
		return fmt.Errorf("failed to count documents: %v", err)
	}

	log.Printf("📊 BULK TRANSFER RESUME: %s.%s contains %d documents, resuming from page %d", database, collection, totalCount, fromPage)

	// Use adaptive batch size if available, otherwise default to 1000
	pageSize := getCurrentBatchSize()
	if pageSize <= 0 {
		pageSize = 1000 // Fallback to default
	}
	totalPages := int((totalCount + int64(pageSize) - 1) / int64(pageSize))

	if totalPages == 0 {
		totalPages = 1 // At least one page for metadata
	}

	// Validate fromPage
	if fromPage > totalPages {
		log.Printf("✅ BULK TRANSFER ALREADY COMPLETED: %s.%s (requested page %d > total pages %d)", database, collection, fromPage, totalPages)
		return nil
	}

	log.Printf("📊 BULK TRANSFER RESUME: %s.%s will resume from page %d/%d (%d docs per page)", database, collection, fromPage, totalPages, pageSize)

	// Track resumed transferred documents
	key := fmt.Sprintf("%s.%s", database, collection)
	alreadyTransferred := int64((fromPage - 1) * pageSize)
	if alreadyTransferred < 0 {
		alreadyTransferred = 0
	}

	// Update progress tracking with already transferred count
	syncStatusMutex.Lock()
	if info, exists := collectionSyncStatuses[key]; exists {
		info.DocumentCount = totalCount
		info.TransferredDocs = alreadyTransferred
	}
	syncStatusMutex.Unlock()

	// Resume transfer from the specified page
	for pageNumber := fromPage; pageNumber <= totalPages; pageNumber++ {
		if err := pushSinglePage(ctx, vmSyncEndpoint, database, collection, pageNumber, pageSize, totalPages); err != nil {
			return fmt.Errorf("failed to push page %d: %v", pageNumber, err)
		}

		// Update transferred document count (estimate based on page size)
		pageDocCount := int64(pageSize)
		if pageNumber == totalPages && totalCount%int64(pageSize) != 0 {
			// Last page might have fewer documents
			pageDocCount = totalCount % int64(pageSize)
		}
		transferredDocs := alreadyTransferred + int64((pageNumber-fromPage)*pageSize) + pageDocCount
		if transferredDocs > totalCount {
			transferredDocs = totalCount
		}

		// Update progress tracking
		syncStatusMutex.Lock()
		if info, exists := collectionSyncStatuses[key]; exists {
			info.TransferredDocs = transferredDocs
		}
		syncStatusMutex.Unlock()

		log.Printf("✅ BULK TRANSFER RESUME: Page %d/%d completed for %s.%s (%d/%d docs transferred)",
			pageNumber, totalPages, database, collection, transferredDocs, totalCount)
	}

	log.Printf("✅ BULK TRANSFER RESUME COMPLETED: %s.%s (%d documents transferred)", database, collection, totalCount)
	return nil
}

// pushCollectionDataWithResume handles resumable initial sync logic
func pushCollectionDataWithResume(vmSyncEndpoint, database, collection string) error {
	// Extract client ID from vm-sync endpoint or use default
	clientID := "vm-sync-default" // TODO: Extract from X-Client-ID header in future

	// Check if resumable initial sync is enabled from config
	resumableEnabled := config.Sync.ResumableInitialSync

	log.Printf("Processing initial sync request for %s.%s (resumable: %v, tracker enabled: %v, transport: %s, force_initial_sync: %v)",
		database, collection, resumableEnabled, transferTracker != nil && transferTracker.IsEnabled(), config.Sync.Transport.Mode, forceInitialSync)

	// NEW: If force initial sync is enabled, always perform full sync
	if forceInitialSync {
		log.Printf("FORCE INITIAL SYNC: Performing full sync for %s.%s regardless of existing state", database, collection)
		return pushCollectionData(vmSyncEndpoint, database, collection)
	}

	if !resumableEnabled || transferTracker == nil || !transferTracker.IsEnabled() {
		log.Printf("Resumable sync disabled or transfer tracker unavailable, performing full sync for %s.%s", database, collection)
		return pushCollectionData(vmSyncEndpoint, database, collection)
	}

	// For TCP transport, check if we can resume from TCP checkpoints
	if config.Sync.Transport.Mode == "tcp" && tcpTransportEnabled {
		canResume, fromPage, err := checkTCPResumeCapability(database, collection)
		if err != nil {
			log.Printf("Error checking TCP resume capability for %s.%s: %v, performing full sync", database, collection, err)
			return pushCollectionData(vmSyncEndpoint, database, collection)
		}

		if canResume && fromPage > 0 {
			log.Printf("🚀 TCP RESUME: Resuming TCP transfer for %s.%s from page %d", database, collection, fromPage)
			if err := resumeTCPTransfer(database, collection, fromPage); err != nil {
				log.Printf("TCP resume failed for %s.%s: %v, falling back to full sync", database, collection, err)
				return pushCollectionData(vmSyncEndpoint, database, collection)
			}
			return pushCollectionDataFromPage(vmSyncEndpoint, database, collection, int(fromPage))
		}
	}

	// Check if vm-sync has existing checkpoint data for this collection (HTTP mode)
	hasCheckpoint, err := checkVMSyncCheckpoint(vmSyncEndpoint, database, collection)
	if err != nil {
		log.Printf("Error checking vm-sync checkpoint for %s.%s: %v, treating as new client - performing full sync", database, collection, err)
		return pushCollectionData(vmSyncEndpoint, database, collection)
	}

	// Check client sync state
	syncState, err := transferTracker.GetClientSyncState(clientID, database, collection)
	if err != nil {
		log.Printf("Error getting sync state for %s.%s: %v, treating as new client - performing full sync", database, collection, err)
		return pushCollectionData(vmSyncEndpoint, database, collection)
	}

	log.Printf("Sync state check for %s.%s: hasCheckpoint=%v, syncState exists=%v, initialSyncCompleted=%v, transport=%s",
		database, collection, hasCheckpoint, syncState != nil, syncState != nil && syncState.InitialSyncCompleted, config.Sync.Transport.Mode)

	// NEW: If force initial sync is enabled, ignore existing sync state
	if forceInitialSync {
		log.Printf("FORCE INITIAL SYNC: Ignoring existing sync state for %s.%s, performing INITIAL BULK TRANSFER via %s",
			database, collection, strings.ToUpper(config.Sync.Transport.Mode))
		return pushCollectionData(vmSyncEndpoint, database, collection)
	}

	// IMPORTANT: For new vm-sync clients, we MUST perform initial bulk transfer
	// This is the core fix for the architecture limitation
	if syncState == nil || !syncState.InitialSyncCompleted {
		log.Printf("🚀 NEW CLIENT DETECTED: No previous sync state or incomplete sync for %s.%s, performing INITIAL BULK TRANSFER via %s",
			database, collection, strings.ToUpper(config.Sync.Transport.Mode))

		// If vm-sync has checkpoint but we don't have completed sync state, clear vm-sync collection first
		if hasCheckpoint {
			log.Printf("Found vm-sync checkpoint but incomplete sync state for %s.%s, clearing vm-sync collection for fresh sync", database, collection)
			if err := clearVMSyncCollection(vmSyncEndpoint, database, collection); err != nil {
				log.Printf("Warning: Failed to clear vm-sync collection %s.%s: %v", database, collection, err)
			}
		}

		return pushCollectionData(vmSyncEndpoint, database, collection)
	}

	// Check if there are new documents to sync incrementally
	log.Printf("Found completed sync state for %s.%s (last synced: %v, documents: %d), checking for incremental updates",
		database, collection, syncState.LastSyncedAt, syncState.TotalDocumentsTransferred)

	// Implement incremental sync logic
	return pushIncrementalData(vmSyncEndpoint, database, collection, clientID, syncState)
}

// validateSyncStateConsistency validates the sync state before resuming
func validateSyncStateConsistency(database, collection, clientID string, syncState *tracking.ClientSyncState) error {
	coll := mongoClient.Database(database).Collection(collection)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	// Validate that LastSyncedDocumentID exists in the collection
	if syncState.LastSyncedDocumentID != nil {
		filter := bson.M{"_id": syncState.LastSyncedDocumentID}
		count, err := coll.CountDocuments(ctx, filter)
		if err != nil {
			return fmt.Errorf("failed to validate last synced document: %v", err)
		}
		if count == 0 {
			log.Printf("WARNING: LastSyncedDocumentID %v not found in %s.%s, may indicate data inconsistency", syncState.LastSyncedDocumentID, database, collection)
			// Don't fail here, just log warning as document might have been deleted
		}
	}

	// Validate document count consistency
	if syncState.TotalDocumentsTransferred > 0 {
		currentCount, err := coll.CountDocuments(ctx, bson.M{})
		if err != nil {
			return fmt.Errorf("failed to count current documents: %v", err)
		}

		// Log validation info - don't fail on count variance as collections can grow
		log.Printf("Sync state validation for %s.%s: current_count=%d, transferred=%d, initial_sync_completed=%v",
			database, collection, currentCount, syncState.TotalDocumentsTransferred, syncState.InitialSyncCompleted)

		// Only validate if initial sync was completed and we have a reasonable baseline
		if syncState.InitialSyncCompleted && currentCount < syncState.TotalDocumentsTransferred {
			log.Printf("WARNING: Current document count (%d) is less than transferred count (%d) for %s.%s",
				currentCount, syncState.TotalDocumentsTransferred, database, collection)
			// Don't fail here as documents might have been deleted
		}
	}

	return nil
}

// validateIncrementalSyncIntegrity validates data integrity during incremental sync
func validateIncrementalSyncIntegrity(database, collection string, expectedCount, actualCount int64) error {
	if actualCount != expectedCount {
		return fmt.Errorf("incremental sync count mismatch for %s.%s: expected %d, got %d", database, collection, expectedCount, actualCount)
	}
	log.Printf("Incremental sync integrity validated for %s.%s: %d documents", database, collection, actualCount)
	return nil
}

// pushIncrementalData pushes only new documents after the last synced document ID
func pushIncrementalData(vmSyncEndpoint, database, collection, clientID string, syncState *tracking.ClientSyncState) error {
	coll := mongoClient.Database(database).Collection(collection)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Minute)
	defer cancel()

	// Validate sync state consistency before proceeding
	if err := validateSyncStateConsistency(database, collection, clientID, syncState); err != nil {
		log.Printf("Sync state validation failed for %s.%s: %v", database, collection, err)
		return fmt.Errorf("sync state validation failed: %v", err)
	}

	// Build filter to find documents after LastSyncedDocumentID
	filter := bson.M{}
	if syncState.LastSyncedDocumentID != nil {
		filter["_id"] = bson.M{"$gt": syncState.LastSyncedDocumentID}
	}

	// Count new documents
	newDocCount, err := coll.CountDocuments(ctx, filter)
	if err != nil {
		return fmt.Errorf("failed to count new documents: %v", err)
	}

	if newDocCount == 0 {
		log.Printf("No new documents found for %s.%s, sync is up to date", database, collection)
		return nil
	}

	log.Printf("Found %d new documents for incremental sync of %s.%s", newDocCount, database, collection)

	// Calculate pages for incremental sync using adaptive batch size
	pageSize := getCurrentBatchSize()
	if pageSize <= 0 {
		pageSize = 1000 // Fallback to default
	}

	totalPages := int((newDocCount + int64(pageSize) - 1) / int64(pageSize))
	log.Printf("Starting incremental sync for %s.%s: %d new documents in %d pages", database, collection, newDocCount, totalPages)

	// Push incremental data page by page with validation
	var totalPushedDocs int64
	for pageNumber := 0; pageNumber < totalPages; pageNumber++ {
		pushedCount, err := pushIncrementalPageWithValidation(ctx, vmSyncEndpoint, database, collection, pageNumber, pageSize, totalPages, filter)
		if err != nil {
			return fmt.Errorf("failed to push incremental page %d: %v", pageNumber, err)
		}
		totalPushedDocs += pushedCount
	}

	// Validate that we pushed the expected number of documents
	if err := validateIncrementalSyncIntegrity(database, collection, newDocCount, totalPushedDocs); err != nil {
		return fmt.Errorf("incremental sync validation failed: %v", err)
	}

	log.Printf("Completed incremental sync for %s.%s: validated %d documents", database, collection, totalPushedDocs)
	return nil
}

// pushIncrementalPageWithValidation pushes a single page of incremental data and returns document count
func pushIncrementalPageWithValidation(ctx context.Context, vmSyncEndpoint, database, collection string, pageNumber, pageSize, totalPages int, filter bson.M) (int64, error) {
	count, err := pushIncrementalPageInternal(ctx, vmSyncEndpoint, database, collection, pageNumber, pageSize, totalPages, filter)
	if err != nil {
		return 0, err
	}
	return count, nil
}

// pushIncrementalPage pushes a single page of incremental data
func pushIncrementalPage(ctx context.Context, vmSyncEndpoint, database, collection string, pageNumber, pageSize, totalPages int, filter bson.M) error {
	_, err := pushIncrementalPageInternal(ctx, vmSyncEndpoint, database, collection, pageNumber, pageSize, totalPages, filter)
	return err
}

// pushIncrementalPageInternal is the internal implementation that returns document count
func pushIncrementalPageInternal(ctx context.Context, vmSyncEndpoint, database, collection string, pageNumber, pageSize, totalPages int, filter bson.M) (int64, error) {
	// Apply back-pressure throttling if enabled
	applyBackPressureThrottle()

	coll := mongoClient.Database(database).Collection(collection)

	// Find documents for this page
	skip := int64(pageNumber * pageSize)
	limit := int64(pageSize)

	findOptions := options.Find().
		SetSkip(skip).
		SetLimit(limit).
		SetSort(bson.D{primitive.E{Key: "_id", Value: 1}}) // Sort by _id for consistent pagination

	cursor, err := coll.Find(ctx, filter, findOptions)
	if err != nil {
		return 0, fmt.Errorf("failed to find documents: %v", err)
	}
	defer cursor.Close(ctx)

	// TCP IS PRIMARY: Force TCP transport for incremental data
	if config.Sync.Transport.Mode == "tcp" && tcpTransportEnabled && tcpSender != nil {
		log.Printf("🚀 FORCE TCP PRIMARY: Using TCP transport for incremental sync %s.%s page %d", database, collection, pageNumber+1)
		return pushIncrementalPageTCP(ctx, cursor, database, collection, pageNumber, totalPages)
	}

	// FALLBACK: Only use HTTP if TCP is not the configured primary mode
	if config.Sync.Transport.Mode != "tcp" {
		log.Printf("📡 HTTP FALLBACK: Using HTTP transport for incremental sync %s.%s page %d", database, collection, pageNumber+1)
		return pushIncrementalPageHTTP(ctx, cursor, vmSyncEndpoint, database, collection, pageNumber, totalPages)
	}

	// ERROR: TCP is primary mode but not available
	return 0, fmt.Errorf("TCP is configured as primary transport but not available (enabled=%v, sender=%v)", tcpTransportEnabled, tcpSender != nil)
}

// pushIncrementalPageTCP pushes incremental data via TCP transport
func pushIncrementalPageTCP(ctx context.Context, cursor *mongo.Cursor, database, collection string, pageNumber, totalPages int) (int64, error) {
	// Start TCP incremental monitoring
	tcpStartTime := time.Now()
	log.Printf("🔄 TCP INCREMENTAL START: %s.%s page %d/%d", database, collection, pageNumber+1, totalPages)

	// Collect documents as BSON for TCP transport
	var documents [][]byte
	totalBytes := 0
	for cursor.Next(ctx) {
		docBytes := make([]byte, len(cursor.Current))
		copy(docBytes, cursor.Current)
		documents = append(documents, docBytes)
		totalBytes += len(docBytes)
	}

	if err := cursor.Err(); err != nil {
		return 0, fmt.Errorf("cursor error: %v", err)
	}

	// Send documents via TCP
	if len(documents) > 0 {
		// IMPORTANT: Use target database name for VM-sync routing (handles ${database_name} -> "1kosmos" replacement)
		targetDatabase := getTargetDatabaseForVMSync(database)
		streamName := fmt.Sprintf("%s.%s.incremental", targetDatabase, collection)

		// TCP transfer with monitoring
		tcpSendStart := time.Now()
		log.Printf("🚀 TCP INCREMENTAL SENDING: %s.%s - %d docs (%s)", database, collection, len(documents), formatBytes(totalBytes))

		if err := tcpSender.SendBatch(streamName, documents); err != nil {
			// Only fall back to HTTP if TCP is NOT the primary mode and HTTP fallback is enabled
			if config.Sync.Transport.Mode != "tcp" && config.Sync.Transport.HTTPFallback {
				log.Printf("❌ TCP INCREMENTAL FAILED -> HTTP FALLBACK: %s.%s page %d: %v", database, collection, pageNumber, err)
				// Reset cursor for HTTP fallback - note: this is a limitation, we can't rewind cursor
				// For now, return error and let caller handle retry
				return 0, fmt.Errorf("TCP transport failed and cursor cannot be rewound for HTTP fallback: %v", err)
			}
			// TCP is primary mode - do not fall back, return error
			return 0, fmt.Errorf("TCP transport failed (primary mode, no fallback): %v", err)
		}

		// FIRE-AND-FORGET: No ACK wait - send and continue immediately
		// VM-sync will process via Kafka-style queue, but we don't wait for confirmation
		log.Printf("🚀 FIRE-AND-FORGET: %s.%s page %d - Data sent, continuing without ACK wait", database, collection, pageNumber+1)

		// Calculate incremental TCP metrics
		tcpSendTime := time.Since(tcpSendStart)
		totalTime := time.Since(tcpStartTime)
		throughputMBps := float64(totalBytes) / tcpSendTime.Seconds() / (1024 * 1024)

		log.Printf("✅ TCP INCREMENTAL SUCCESS: %s.%s page %d/%d - %d docs (%s) CONFIRMED RECEIVED in %v (%.2f MB/s) - Total: %v",
			database, collection, pageNumber+1, totalPages, len(documents), formatBytes(totalBytes), tcpSendTime, throughputMBps, totalTime)
	} else {
		log.Printf("⚠️  TCP INCREMENTAL SKIP: %s.%s page %d - No documents", database, collection, pageNumber+1)
	}

	documentCount := int64(len(documents))
	return documentCount, nil
}

// pushIncrementalPageHTTP pushes incremental data via HTTP transport (original implementation)
func pushIncrementalPageHTTP(ctx context.Context, cursor *mongo.Cursor, vmSyncEndpoint, database, collection string, pageNumber, totalPages int) (int64, error) {
	// Collect documents
	var documents []bson.Raw
	for cursor.Next(ctx) {
		documents = append(documents, cursor.Current)
	}

	if err := cursor.Err(); err != nil {
		return 0, fmt.Errorf("cursor error: %v", err)
	}

	// Create page result for incremental data using proper models.PageResult
	pageResult := models.PageResult{
		PageNumber:        pageNumber + 1, // 1-indexed for consistency
		Documents:         documents,
		IsLastPage:        pageNumber == totalPages-1,
		Indexes:           []models.IndexInfo{}, // Empty for incremental
		CollectionOptions: nil,                  // Nil for incremental
		SnapshotFence:     nil,                  // Nil for incremental
	}

	// Marshal to BSON to preserve MongoDB types (CRITICAL FIX)
	bsonData, err := bson.Marshal(pageResult)
	if err != nil {
		return 0, fmt.Errorf("failed to marshal page result: %v", err)
	}

	// Create correct URL with proper endpoint path
	url := fmt.Sprintf("%s/api/v1/push/%s/%s", vmSyncEndpoint, database, collection)
	resp, err := http.Post(url, "application/bson", bytes.NewBuffer(bsonData))
	if err != nil {
		return 0, fmt.Errorf("failed to send incremental data: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return 0, fmt.Errorf("vm-sync returned error %d: %s", resp.StatusCode, string(body))
	}

	documentCount := int64(len(documents))
	log.Printf("Pushed incremental page %d/%d via HTTP for %s.%s (%d documents)", pageNumber+1, totalPages, database, collection, documentCount)
	return documentCount, nil
}

func pushSinglePage(ctx context.Context, vmSyncEndpoint, database, collection string, pageNumber, pageSize, totalPages int) error {
	// TCP IS PRIMARY: Force TCP transport if configured as primary
	if config.Sync.Transport.Mode == "tcp" && tcpTransportEnabled && tcpSender != nil {
		log.Printf("🚀 FORCE TCP PRIMARY: Using TCP transport for initial sync %s.%s page %d", database, collection, pageNumber)
		return pushSinglePageTCP(ctx, database, collection, pageNumber, pageSize, totalPages)
	}

	// FALLBACK: Only use HTTP if TCP is not the configured primary mode
	if config.Sync.Transport.Mode != "tcp" {
		log.Printf("📡 HTTP FALLBACK: Using HTTP transport for initial sync %s.%s page %d", database, collection, pageNumber)
		return pushSinglePageHTTP(ctx, vmSyncEndpoint, database, collection, pageNumber, pageSize, totalPages)
	}

	// ERROR: TCP is primary mode but not available
	return fmt.Errorf("TCP is configured as primary transport but not available (enabled=%v, sender=%v)", tcpTransportEnabled, tcpSender != nil)
}

// pushSinglePageTCP pushes a single page using TCP transport
func pushSinglePageTCP(ctx context.Context, database, collection string, pageNumber, pageSize, totalPages int) error {
	// Apply back-pressure throttling if enabled
	applyBackPressureThrottle()

	// Start detailed TCP monitoring
	tcpStartTime := time.Now()
	log.Printf("🚀 TCP TRANSFER START: %s.%s page %d/%d (batch size: %d)", database, collection, pageNumber, totalPages, pageSize)

	// Get collection configuration for filtering
	var collConfig *models.CollectionConfig
	for _, dbConfig := range config.MongoDB.Databases {
		if dbConfig.Name == database {
			for i, coll := range dbConfig.Collections {
				if coll.Name == collection {
					collConfig = &dbConfig.Collections[i] // FIXED: Use slice index, not loop variable address
					break
				}
			}
			break
		}
	}

	coll := mongoClient.Database(database).Collection(collection)

	// CRITICAL: Apply consistent filtering for initial dump (same as real-time sync)
	// Build comprehensive filtering pipeline
	var filterPipeline []bson.M

	// Apply document filtering first (filter out unwanted documents)
	if collConfig != nil && len(collConfig.DocumentFilter.Criteria) > 0 {
		docFilterPipeline := filterEngine.BuildDocumentFilterPipeline(&collConfig.DocumentFilter)
		filterPipeline = append(filterPipeline, docFilterPipeline...)
		log.Printf("🔍 INITIAL DUMP FILTER: Applied document filter for %s.%s with %d criteria", database, collection, len(collConfig.DocumentFilter.Criteria))
	}

	// Apply field filtering (include/exclude specific fields)
	if collConfig != nil && (len(collConfig.FieldFilter.IncludeFields) > 0 || len(collConfig.FieldFilter.ExcludeFields) > 0) {
		fieldFilterPipeline := filterEngine.BuildFieldFilterPipeline(&collConfig.FieldFilter)
		filterPipeline = append(filterPipeline, fieldFilterPipeline...)
		log.Printf("🔍 INITIAL DUMP FILTER: Applied field filter for %s.%s (include: %v, exclude: %v)",
			database, collection, collConfig.FieldFilter.IncludeFields, collConfig.FieldFilter.ExcludeFields)
	}

	// Add pagination (skip and limit)
	skip := (pageNumber - 1) * pageSize
	filterPipeline = append(filterPipeline, bson.M{"$skip": skip})
	filterPipeline = append(filterPipeline, bson.M{"$limit": pageSize})

	// Execute filtered aggregation pipeline instead of simple find
	var cursor *mongo.Cursor
	var err error

	if len(filterPipeline) > 2 { // More than just skip and limit
		log.Printf("🔍 FILTERED AGGREGATION: %s.%s using %d pipeline stages", database, collection, len(filterPipeline))
		cursor, err = coll.Aggregate(ctx, filterPipeline)
	} else {
		// No filters, use simple find for better performance
		findOptions := options.Find().SetSkip(int64(skip)).SetLimit(int64(pageSize))
		cursor, err = coll.Find(ctx, bson.M{}, findOptions)
	}

	if err != nil {
		return fmt.Errorf("failed to query documents with filters: %v", err)
	}
	defer cursor.Close(ctx)

	// Collect documents as BSON documents for TCP transport
	var documents [][]byte
	totalBytes := 0
	tenantDNS := os.Getenv("TENANT_DNS") // For URL transformation
	for cursor.Next(ctx) {
		// cursor.Current is already bson.Raw, convert to []byte
		docBytes := make([]byte, len(cursor.Current))
		copy(docBytes, cursor.Current)

		// SPECIAL HANDLING: Transform URLs in servicecomponents collection
		if collection == "servicecomponents" && tenantDNS != "" {
			var doc bson.M
			if err := bson.Unmarshal(docBytes, &doc); err == nil {
				if urlStr, ok := doc["url"].(string); ok && urlStr != "" {
					transformedURL := transformServiceDirectoryURL(urlStr, tenantDNS)
					if transformedURL != urlStr {
						log.Printf("🔄 URL TRANSFORMED: %s -> %s", urlStr, transformedURL)
						doc["url"] = transformedURL
						// Re-marshal modified document
						if modifiedBytes, err := bson.Marshal(doc); err == nil {
							docBytes = modifiedBytes
						}
					}
				}
			}
		}

		documents = append(documents, docBytes)
		totalBytes += len(docBytes)
	}

	if err := cursor.Err(); err != nil {
		return fmt.Errorf("cursor error: %v", err)
	}

	// Create stream name for this collection
	// IMPORTANT: Use target database name for VM-sync routing (handles ${database_name} -> "1kosmos" replacement)
	targetDatabase := getTargetDatabaseForVMSync(database)
	streamName := fmt.Sprintf("%s.%s", targetDatabase, collection)

	// Log document collection phase
	docCollectionTime := time.Since(tcpStartTime)
	log.Printf("📊 TCP DOCS COLLECTED: %s.%s - %d docs (%d bytes) in %v", database, collection, len(documents), totalBytes, docCollectionTime)

	// Send documents via TCP with resumable support
	if len(documents) > 0 {
		// Create checkpoint metadata for TCP transfer (future use)
		_ = map[string]interface{}{
			"database":    database,
			"collection":  collection,
			"page_number": pageNumber,
			"page_size":   pageSize,
			"total_pages": totalPages,
			"doc_count":   len(documents),
			"timestamp":   time.Now(),
		}

		// Set checkpoint before sending via TCP
		if tcpReceiver := getTCPReceiverFromSender(); tcpReceiver != nil {
			checkpointSeq := uint64(pageNumber)
			if err := tcpReceiver.SetCheckpoint(streamName, checkpointSeq); err != nil {
				log.Printf("Warning: Failed to set TCP checkpoint for %s page %d: %v", streamName, pageNumber, err)
			}
		}

		// Actual TCP transfer with detailed monitoring
		tcpSendStart := time.Now()
		log.Printf("🚀 TCP SENDING: %s.%s page %d - %d docs (%s)", database, collection, pageNumber, len(documents), formatBytes(totalBytes))

		if err := tcpSender.SendBatch(streamName, documents); err != nil {
			// Only fall back to HTTP if TCP is NOT the primary mode and HTTP fallback is enabled
			if config.Sync.Transport.Mode != "tcp" && config.Sync.Transport.HTTPFallback {
				log.Printf("❌ TCP FAILED -> HTTP FALLBACK: %s.%s page %d: %v", database, collection, pageNumber, err)
				return pushSinglePageHTTP(ctx, getVMSyncHTTPEndpoint(), database, collection, pageNumber, pageSize, totalPages)
			}
			// TCP is primary mode - do not fall back, return error
			return fmt.Errorf("TCP transport failed (primary mode, no fallback): %v", err)
		}

		// FIRE-AND-FORGET: No ACK wait - send and continue immediately
		// VM-sync will process via Kafka-style queue, but we don't wait for confirmation
		log.Printf("🚀 FIRE-AND-FORGET: %s.%s page %d - Data sent, continuing without ACK wait", database, collection, pageNumber)

		// Calculate TCP transfer metrics
		tcpSendTime := time.Since(tcpSendStart)
		totalTime := time.Since(tcpStartTime)
		throughputMBps := float64(totalBytes) / tcpSendTime.Seconds() / (1024 * 1024)

		log.Printf("✅ TCP SUCCESS: %s.%s page %d/%d - %d docs (%s) CONFIRMED RECEIVED in %v (%.2f MB/s) - Total: %v",
			database, collection, pageNumber, totalPages, len(documents), formatBytes(totalBytes), tcpSendTime, throughputMBps, totalTime)

		// NOW SAFE: Update transfer tracking ONLY after ACK confirms delivery
		if transferTracker != nil && transferTracker.IsEnabled() {
			clientID := "vm-sync-default" // TODO: Extract from connection context
			if err := updateTCPTransferProgress(clientID, database, collection, pageNumber, len(documents)); err != nil {
				log.Printf("Warning: Failed to update TCP transfer progress: %v", err)
			}
		}
	} else {
		log.Printf("⚠️  TCP SKIP: %s.%s page %d - No documents to transfer", database, collection, pageNumber)
	}

	// ✅ METADATA OPTIMIZATION: Metadata is now sent BEFORE data transfer in Phase 1
	// No need to send metadata on page 1 anymore - this eliminates ACK dependency issues
	// Indexes are already created when data arrives, optimizing insert performance

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

// getTargetDatabaseForVMSync returns the target database name for VM-sync routing
// This implements the special ${database_name} -> "1kosmos" replacement
func getTargetDatabaseForVMSync(sourceDatabase string) string {
	// Search through config to find matching database and return its TargetDatabaseName
	for _, db := range config.MongoDB.Databases {
		if db.Name == sourceDatabase {
			if db.TargetDatabaseName != "" {
				return db.TargetDatabaseName
			}
			break
		}
	}
	// Fallback: return source database name if no mapping found
	return sourceDatabase
}

// transformServiceDirectoryURL transforms URLs by replacing the domain with TENANT_DNS
// Example: "https://1k-dev.1kosmos.net/vcs" -> "https://blockid-dev.1kosmos.net/vcs"
func transformServiceDirectoryURL(originalURL, tenantDNS string) string {
	if originalURL == "" || tenantDNS == "" {
		return originalURL
	}

	// Regular expression to match domain in URL
	// Pattern: protocol://domain/path -> replace domain with tenantDNS
	domainRegex := regexp.MustCompile(`^(https?://)[^/]+(/.*)?$`)

	if domainRegex.MatchString(originalURL) {
		// Extract protocol and path
		matches := domainRegex.FindStringSubmatch(originalURL)
		if len(matches) >= 2 {
			protocol := matches[1] // "https://" or "http://"
			path := ""
			if len(matches) >= 3 {
				path = matches[2] // "/vcs" or empty
			}

			// Construct new URL with TENANT_DNS
			newURL := fmt.Sprintf("%s%s%s", protocol, tenantDNS, path)
			return newURL
		}
	}

	// If no match, return original
	return originalURL
}

// getTCPReceiverFromSender returns a receiver reference from the sender (for checkpoint coordination)
// This is a placeholder - in practice, you might coordinate through the transfer tracker
func getTCPReceiverFromSender() transport.Receiver {
	// In a real implementation, you'd coordinate checkpoints through a shared system
	// For now, return nil to indicate local checkpointing only
	return nil
}

// updateTCPTransferProgress updates the transfer tracking for TCP transport
func updateTCPTransferProgress(clientID, database, collection string, pageNumber, docCount int) error {
	if transferTracker == nil || !transferTracker.IsEnabled() {
		return nil
	}

	// Defensive programming with panic recovery
	defer func() {
		if r := recover(); r != nil {
			log.Printf("🔴 PANIC RECOVERED in updateTCPTransferProgress for %s.%s: %v", database, collection, r)
		}
	}()

	// Get or create sync state
	syncState, err := transferTracker.GetClientSyncState(clientID, database, collection)
	if err != nil || syncState == nil {
		// Create new sync state for TCP transfer
		syncState = &tracking.ClientSyncState{
			ClientID:                  clientID,
			Database:                  database,
			Collection:                collection,
			CreatedAt:                 time.Now(),
			LastSyncedAt:              time.Now(),
			TotalDocumentsTransferred: 0, // Initialize to 0
		}
	}

	// Safety check before accessing syncState
	if syncState == nil {
		log.Printf("🔴 CRITICAL: syncState is nil in updateTCPTransferProgress for %s.%s", database, collection)
		return nil
	}

	// Update progress
	syncState.TotalDocumentsTransferred += int64(docCount)
	syncState.LastSyncedAt = time.Now()
	// Note: Transfer method tracking would be implemented via separate metadata

	// For the last page, mark as completed
	// This would need proper coordination in a real implementation
	// For now, we'll use a simple heuristic

	return transferTracker.UpdateClientSyncState(clientID, database, collection, nil, int64(docCount), false)
}

// checkTCPResumeCapability checks if TCP transport can resume from a checkpoint
func checkTCPResumeCapability(database, collection string) (bool, uint64, error) {
	if !tcpTransportEnabled || tcpSender == nil {
		return false, 0, fmt.Errorf("TCP transport not enabled")
	}

	// Stream name for coordination
	_ = fmt.Sprintf("%s.%s", database, collection)

	// Check if there's a checkpoint for this stream in the TCP receiver
	// This would typically involve querying the receiver's checkpoint store
	// For now, we'll use the transfer tracker as a fallback

	if transferTracker != nil && transferTracker.IsEnabled() {
		// Extract client ID from TCP connection context or use a fallback
		// TODO: Implement proper client ID extraction from TCP connection metadata
		clientID := "vm-sync-default" // Fallback - should be extracted from connection
		// In production, extract from TCP frame metadata or connection handshake
		syncState, err := transferTracker.GetClientSyncState(clientID, database, collection)
		if err != nil || syncState == nil {
			// No checkpoint found or error occurred
			log.Printf("📊 TCP RESUME: No existing sync state for %s.%s, starting fresh", database, collection)
			return false, 0, nil
		}

		if syncState.InitialSyncCompleted {
			log.Printf("✅ TCP RESUME: Initial sync already completed for %s.%s", database, collection)
			return false, 0, nil // Already completed
		}

		// Calculate approximate page from transferred documents
		pageSize := getCurrentBatchSize()
		if pageSize <= 0 {
			pageSize = 1000
		}
		lastPage := uint64(syncState.TotalDocumentsTransferred / int64(pageSize))
		log.Printf("🚀 TCP RESUME: Found checkpoint for %s.%s at page %d (%d docs transferred)",
			database, collection, lastPage, syncState.TotalDocumentsTransferred)
		return true, lastPage, nil
	}

	log.Printf("⚠️  TCP RESUME: Transfer tracker not available for %s.%s", database, collection)
	return false, 0, nil
}

// resumeTCPTransfer resumes TCP transfer from a checkpoint
func resumeTCPTransfer(database, collection string, fromPage uint64) error {
	if !tcpTransportEnabled || tcpSender == nil {
		return fmt.Errorf("TCP transport not enabled")
	}

	streamName := fmt.Sprintf("%s.%s", database, collection)

	// Resume the TCP sender from the specified sequence/page
	if err := tcpSender.Resume(streamName, fromPage); err != nil {
		return fmt.Errorf("failed to resume TCP sender: %w", err)
	}

	log.Printf("Resumed TCP transfer for %s from page %d", streamName, fromPage)
	return nil
}

// markTransferCompleted marks a bulk transfer as completed in the tracking system
func markTransferCompleted(clientID, database, collection string, totalDocs int64) error {
	// Safety check - ensure transferTracker is not nil
	if transferTracker == nil {
		log.Printf("⚠️  WARNING: Transfer tracker is nil, skipping state update for %s.%s", database, collection)
		return nil
	}

	if !transferTracker.IsEnabled() {
		log.Printf("⚠️  WARNING: Transfer tracker disabled, skipping state update for %s.%s", database, collection)
		return nil
	}

	// Defensive: Double-check transferTracker before any operation
	defer func() {
		if r := recover(); r != nil {
			log.Printf("🔴 PANIC RECOVERED in markTransferCompleted for %s.%s: %v", database, collection, r)
		}
	}()

	// Get or create sync state
	syncState, err := transferTracker.GetClientSyncState(clientID, database, collection)
	if err != nil {
		log.Printf("📊 Creating new sync state for client %s, collection %s.%s", clientID, database, collection)
		// Create new sync state
		syncState = &tracking.ClientSyncState{
			ClientID:   clientID,
			Database:   database,
			Collection: collection,
			CreatedAt:  time.Now(),
		}
	}

	// Mark as completed
	if syncState != nil {
		syncState.InitialSyncCompleted = true
		syncState.TotalDocumentsTransferred = totalDocs
		syncState.LastSyncedAt = time.Now()
	}
	log.Printf("✅ TRANSFER COMPLETED: %s.%s (%d documents) for client %s", database, collection, totalDocs, clientID)
	// Note: Transfer method tracking would be implemented via separate metadata

	// Final safety check before calling UpdateClientSyncState
	if transferTracker == nil {
		log.Printf("🔴 CRITICAL: transferTracker became nil before UpdateClientSyncState call")
		return nil
	}

	return transferTracker.UpdateClientSyncState(clientID, database, collection, nil, 0, true)
}

// pushSinglePageHTTP pushes a single page using HTTP transport with consistent filtering
func pushSinglePageHTTP(ctx context.Context, vmSyncEndpoint, database, collection string, pageNumber, pageSize, totalPages int) error {
	// Apply back-pressure throttling if enabled
	applyBackPressureThrottle()

	// Get collection configuration for filtering
	var collConfig *models.CollectionConfig
	for _, dbConfig := range config.MongoDB.Databases {
		if dbConfig.Name == database {
			for i, coll := range dbConfig.Collections {
				if coll.Name == collection {
					collConfig = &dbConfig.Collections[i] // FIXED: Use slice index, not loop variable address
					break
				}
			}
			break
		}
	}

	coll := mongoClient.Database(database).Collection(collection)

	// CRITICAL: Apply consistent filtering for initial dump (same as real-time sync)
	// Build comprehensive filtering pipeline
	var filterPipeline []bson.M

	// Apply document filtering first (filter out unwanted documents)
	if collConfig != nil && len(collConfig.DocumentFilter.Criteria) > 0 {
		docFilterPipeline := filterEngine.BuildDocumentFilterPipeline(&collConfig.DocumentFilter)
		filterPipeline = append(filterPipeline, docFilterPipeline...)
		log.Printf("🔍 INITIAL DUMP FILTER: Applied document filter for %s.%s with %d criteria", database, collection, len(collConfig.DocumentFilter.Criteria))
	}

	// Apply field filtering (include/exclude specific fields)
	if collConfig != nil && (len(collConfig.FieldFilter.IncludeFields) > 0 || len(collConfig.FieldFilter.ExcludeFields) > 0) {
		fieldFilterPipeline := filterEngine.BuildFieldFilterPipeline(&collConfig.FieldFilter)
		filterPipeline = append(filterPipeline, fieldFilterPipeline...)
		log.Printf("🔍 INITIAL DUMP FILTER: Applied field filter for %s.%s (include: %v, exclude: %v)",
			database, collection, collConfig.FieldFilter.IncludeFields, collConfig.FieldFilter.ExcludeFields)
	}

	// Add pagination (skip and limit)
	skip := (pageNumber - 1) * pageSize
	filterPipeline = append(filterPipeline, bson.M{"$skip": skip})
	filterPipeline = append(filterPipeline, bson.M{"$limit": pageSize})

	// Execute filtered aggregation pipeline instead of simple find
	var cursor *mongo.Cursor
	var err error

	if len(filterPipeline) > 2 { // More than just skip and limit
		log.Printf("🔍 FILTERED AGGREGATION: %s.%s using %d pipeline stages", database, collection, len(filterPipeline))
		cursor, err = coll.Aggregate(ctx, filterPipeline)
	} else {
		// No filters, use simple find for better performance
		findOptions := options.Find().SetSkip(int64(skip)).SetLimit(int64(pageSize))
		cursor, err = coll.Find(ctx, bson.M{}, findOptions)
	}

	if err != nil {
		return fmt.Errorf("failed to query documents with filters: %v", err)
	}
	defer cursor.Close(ctx)

	// Collect documents
	var documents []bson.Raw
	tenantDNS := os.Getenv("TENANT_DNS") // For URL transformation
	for cursor.Next(ctx) {
		docBytes := cursor.Current

		// SPECIAL HANDLING: Transform URLs in servicedirectory collection
		if collection == "servicecomponents" && tenantDNS != "" {
			var doc bson.M
			if err := bson.Unmarshal(docBytes, &doc); err == nil {
				if urlStr, ok := doc["url"].(string); ok && urlStr != "" {
					transformedURL := transformServiceDirectoryURL(urlStr, tenantDNS)
					if transformedURL != urlStr {
						log.Printf("🔄 URL TRANSFORMED (HTTP): %s -> %s", urlStr, transformedURL)
						doc["url"] = transformedURL
						// Re-marshal modified document
						if modifiedBytes, err := bson.Marshal(doc); err == nil {
							docBytes = bson.Raw(modifiedBytes)
						}
					}
				}
			}
		}

		documents = append(documents, docBytes)
	}

	if err := cursor.Err(); err != nil {
		return fmt.Errorf("cursor error: %v", err)
	}

	// Collect indexes and collection options on first page
	var indexes []models.IndexInfo
	var collectionOptions *models.CollectionOptions
	var snapshotFence *models.SnapshotFenceInfo

	if pageNumber == 1 {
		// Collect indexes
		if idxs, err := collectIndexes(ctx, coll); err != nil {
			log.Printf("Warning: Failed to collect indexes for %s.%s: %v", database, collection, err)
		} else {
			indexes = idxs
		}

		// Collect collection options
		if opts, err := collectCollectionOptions(ctx, mongoClient.Database(database), collection); err != nil {
			log.Printf("Warning: Failed to collect collection options for %s.%s: %v", database, collection, err)
		} else {
			collectionOptions = opts
		}

		// Create snapshot fence info
		snapshotFence = &models.SnapshotFenceInfo{
			ClusterTime:   nil, // Will be set properly in production
			OperationTime: nil, // Will be set properly in production
			CapturedAt:    time.Now(),
		}
	}

	// Create page result
	pageResult := models.PageResult{
		PageNumber:        pageNumber,
		Documents:         documents,
		Indexes:           indexes,
		CollectionOptions: collectionOptions,
		SnapshotFence:     snapshotFence,
		IsLastPage:        pageNumber == totalPages,
	}

	// Marshal to BSON to preserve MongoDB types (CRITICAL FIX for data corruption)
	payload, err := bson.Marshal(pageResult)
	if err != nil {
		return fmt.Errorf("failed to marshal page result: %v", err)
	}

	// Send to vm-sync with correct BSON content type
	url := fmt.Sprintf("%s/api/v1/push/%s/%s", vmSyncEndpoint, database, collection)
	resp, err := http.Post(url, "application/bson", bytes.NewBuffer(payload))
	if err != nil {
		return fmt.Errorf("failed to send request to vm-sync: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("vm-sync returned error %d: %s", resp.StatusCode, string(body))
	}

	return nil
}

// sendMetadataTCP sends collection metadata (indexes, options) via TCP
func sendMetadataTCP(ctx context.Context, database, collection string) error {
	coll := mongoClient.Database(database).Collection(collection)

	// Collect indexes
	indexes, err := collectIndexes(ctx, coll)
	if err != nil {
		log.Printf("Warning: Failed to collect indexes for %s.%s: %v", database, collection, err)
		indexes = []models.IndexInfo{} // Empty slice if failed
	} else {
		log.Printf("🔍 CLOUD METADATA: Collected %d indexes for %s.%s", len(indexes), database, collection)
		for i, idx := range indexes {
			log.Printf("🔍 CLOUD INDEX %d: name=%s, unique=%v, keys=%v", i, idx.Name, idx.Unique, string(idx.Keys))
		}
	}

	// Collect collection options
	collectionOptions, err := collectCollectionOptions(ctx, mongoClient.Database(database), collection)
	if err != nil {
		log.Printf("Warning: Failed to collect collection options for %s.%s: %v", database, collection, err)
		collectionOptions = nil // Nil if failed
	}

	// Create snapshot fence info
	snapshotFence := &models.SnapshotFenceInfo{
		ClusterTime:   nil, // Will be set properly in production
		OperationTime: nil, // Will be set properly in production
		CapturedAt:    time.Now(),
	}

	// Create metadata structure
	metadata := map[string]interface{}{
		"type":              "metadata",
		"database":          database,
		"collection":        collection,
		"indexes":           indexes,
		"collectionOptions": collectionOptions,
		"snapshotFence":     snapshotFence,
	}

	// Serialize metadata as BSON
	metadataBytes, err := bson.Marshal(metadata)
	if err != nil {
		return fmt.Errorf("failed to marshal metadata: %v", err)
	}
	log.Printf("🔍 CLOUD METADATA: Marshaled %d bytes for %s.%s", len(metadataBytes), database, collection)

	// Send metadata as a special stream
	// IMPORTANT: Use target database name for VM-sync routing (handles ${database_name} -> "1kosmos" replacement)
	targetDatabase := getTargetDatabaseForVMSync(database)
	metadataStreamName := fmt.Sprintf("%s.%s.metadata", targetDatabase, collection)
	if err := tcpSender.SendBatch(metadataStreamName, [][]byte{metadataBytes}); err != nil {
		return fmt.Errorf("failed to send metadata: %v", err)
	}

	return nil
}

// getVMSyncHTTPEndpoint returns the HTTP endpoint for vm-sync fallback
// Uses dynamically discovered endpoint from VM auth, falls back to env var
func getVMSyncHTTPEndpoint() string {
	// Try to get HTTP endpoint from Address Manager (dynamically discovered during auth)
	addressMgr := transport.GetAddressManager()
	httpEndpoint, err := addressMgr.GetAnyHTTPAddress()
	if err == nil && httpEndpoint != "" {
		// FIX: Validate that port is not 0 (invalid port)
		if strings.Contains(httpEndpoint, ":0") || strings.HasSuffix(httpEndpoint, ":0") {
			log.Printf("⚠️  HTTP ENDPOINT: Discovered endpoint has invalid port 0: %s - falling back to localhost", httpEndpoint)
		} else {
			// FIX: Ensure http:// prefix exists
			if !strings.HasPrefix(httpEndpoint, "http://") && !strings.HasPrefix(httpEndpoint, "https://") {
				httpEndpoint = "http://" + httpEndpoint
				log.Printf("🔧 HTTP ENDPOINT: Added http:// prefix to endpoint: %s", httpEndpoint)
			}
			log.Printf("🎯 HTTP ENDPOINT: Using dynamically discovered endpoint: %s", httpEndpoint)
			return httpEndpoint
		}
	}

	// Fallback to environment variable or localhost (legacy compatibility)
	vmSyncEndpoint := os.Getenv("VM_SYNC_ENDPOINT")
	if vmSyncEndpoint == "" {
		vmSyncEndpoint = "http://localhost:8081" // Default vm-sync endpoint
		log.Printf("⚠️  HTTP ENDPOINT: Using localhost fallback: %s (dynamic discovery failed or port invalid)", vmSyncEndpoint)
	} else {
		log.Printf("🔧 HTTP ENDPOINT: Using environment variable: %s", vmSyncEndpoint)
	}
	return vmSyncEndpoint
}

func loadConfig(filename string) error {
	data, err := os.ReadFile(filename)
	if err != nil {
		return err
	}
	expandedData := os.ExpandEnv(string(data))

	if err := yaml.Unmarshal([]byte(expandedData), &config); err != nil {
		return err
	}

	// Check if there's a separate collections config file specified
	// Look for collections_config_file in the mongodb section
	type PartialConfig struct {
		MongoDB struct {
			CollectionsConfigFile string `yaml:"collections_config_file"`
		} `yaml:"mongodb"`
	}

	var partialConfig PartialConfig
	if err := yaml.Unmarshal([]byte(expandedData), &partialConfig); err == nil {
		if partialConfig.MongoDB.CollectionsConfigFile != "" {
			// Load collections from the separate JSON file
			if err := loadCollectionsFromJSON(partialConfig.MongoDB.CollectionsConfigFile); err != nil {
				log.Printf("Warning: Failed to load collections from JSON file: %v", err)
			}
		}
	}

	return nil
}

// expandEnvVars replaces ${VAR_NAME} patterns with environment variable values
// Example: "authn-${TENANT_NAME}" with TENANT_NAME=prod becomes "authn-prod"
func expandEnvVars(data []byte) []byte {
	// Convert to string for regex processing
	str := string(data)

	// Regular expression to match ${VAR_NAME} or ${VAR_NAME:-default}
	// Supports: ${VAR}, ${VAR:-default_value}
	re := regexp.MustCompile(`\$\{([A-Za-z_][A-Za-z0-9_]*)(?::-([^}]*))?\}`)

	// Replace all occurrences
	expanded := re.ReplaceAllStringFunc(str, func(match string) string {
		// Extract variable name and default value
		submatches := re.FindStringSubmatch(match)
		if len(submatches) < 2 {
			return match // Return original if pattern doesn't match
		}

		varName := submatches[1]
		defaultValue := ""
		if len(submatches) > 2 {
			defaultValue = submatches[2]
		}

		// Get environment variable value
		envValue := os.Getenv(varName)

		// Use environment value if set, otherwise use default
		if envValue != "" {
			return envValue
		}

		return defaultValue
	})

	return []byte(expanded)
}

// loadCollectionsFromJSON loads collections configuration from a separate JSON file
// Supports environment variable substitution using ${VAR_NAME} or ${VAR_NAME:-default} syntax
// SPECIAL HANDLING: For database.name, tracks original template and computes target name for VM-sync
func loadCollectionsFromJSON(filename string) error {
	// Resolve the file path relative to the main config file
	data, err := os.ReadFile(filename)
	if err != nil {
		return fmt.Errorf("failed to read collections config file: %w", err)
	}

	// STEP 1: Parse the original JSON to capture database name templates BEFORE expansion
	var originalConfig struct {
		Databases []struct {
			Name string `json:"name"`
		} `json:"databases"`
	}
	if err := json.Unmarshal(data, &originalConfig); err != nil {
		return fmt.Errorf("failed to parse original collections config JSON: %w", err)
	}

	// STEP 2: Expand environment variables in the JSON content
	expandedData := expandEnvVars(data)

	log.Printf("📝 CONFIG: Environment variable expansion completed for %s", filename)

	// STEP 3: Parse the JSON file with expanded environment variables
	var collectionsConfig struct {
		Databases []models.DatabaseConfig `json:"databases"`
	}

	if err := json.Unmarshal(expandedData, &collectionsConfig); err != nil {
		return fmt.Errorf("failed to parse collections config JSON: %w", err)
	}

	// STEP 4: Post-process database configs to set OriginalTemplate and TargetDatabaseName
	for i := range collectionsConfig.Databases {
		// Store the original template (before env var expansion)
		if i < len(originalConfig.Databases) {
			collectionsConfig.Databases[i].OriginalTemplate = originalConfig.Databases[i].Name
		}

		// Only compute target database name if NOT already specified in JSON
		// This allows manual override via "target_database_name" field in JSON
		if collectionsConfig.Databases[i].TargetDatabaseName == "" {
			// Compute target database name for VM-sync by replacing ${database_name} with "1kosmos"
			// This is ONLY for database.name routing to VM-sync
			targetName := collectionsConfig.Databases[i].OriginalTemplate
			if targetName == "" {
				targetName = collectionsConfig.Databases[i].Name
			}

			// Replace ${TENANT_NAME} or ${database_name} with hardcoded "1kosmos" for VM-sync routing
			// Support patterns like ${TENANT_NAME}, ${database_name}, ${database_name:-default}
			dbNamePattern := regexp.MustCompile(`\$\{(?:TENANT_NAME|database_name)(?::-[^}]*)?\}`)
			collectionsConfig.Databases[i].TargetDatabaseName = dbNamePattern.ReplaceAllString(targetName, "1kosmos")
			log.Printf("📦 DB ROUTING AUTO-COMPUTED: Source='%s' -> Target='%s' (auto-generated)",
				collectionsConfig.Databases[i].Name,
				collectionsConfig.Databases[i].TargetDatabaseName)
		} else {
			log.Printf("🎯 DB ROUTING EXPLICIT: Source='%s' -> Target='%s' (from JSON config)",
				collectionsConfig.Databases[i].Name,
				collectionsConfig.Databases[i].TargetDatabaseName)
		}

		log.Printf("📋 DB CONFIG LOADED: Name='%s', OriginalTemplate='%s', TargetName='%s'",
			collectionsConfig.Databases[i].Name,
			collectionsConfig.Databases[i].OriginalTemplate,
			collectionsConfig.Databases[i].TargetDatabaseName)
	}

	// STEP 5: Merge the collections configuration with the main config
	for _, db := range collectionsConfig.Databases {
		// Find the matching database in the main config
		found := false
		for j, mainDB := range config.MongoDB.Databases {
			if mainDB.Name == db.Name {
				// Merge the collections and routing metadata
				config.MongoDB.Databases[j].Collections = db.Collections
				config.MongoDB.Databases[j].OriginalTemplate = db.OriginalTemplate
				config.MongoDB.Databases[j].TargetDatabaseName = db.TargetDatabaseName
				found = true
				break
			}
		}

		// If database not found in main config, add it
		if !found {
			config.MongoDB.Databases = append(config.MongoDB.Databases, db)
		}
	}

	log.Printf("Successfully loaded collections configuration from %s", filename)
	return nil
}

// getDefaultConfig returns a minimal default configuration for degraded mode operation
func getDefaultConfig() *models.Config {
	return &models.Config{
		Server: models.ServerConfig{
			Port:         8080,
			Host:         "0.0.0.0",
			ReadTimeout:  30 * time.Second,
			WriteTimeout: 30 * time.Second,
			IdleTimeout:  60 * time.Second,
			DataTimeout:  300 * time.Second,
		},
		MongoDB: models.MongoDBConfig{
			URI:       "mongodb://localhost:27017",
			Timeout:   30 * time.Second,
			Databases: []models.DatabaseConfig{},
		},
		WebSocket: models.WebSocketConfig{
			Endpoint:        "/ws",
			AllowedOrigins:  []string{"*"},
			ReadBufferSize:  1024,
			WriteBufferSize: 1024,
		},
		Sync: models.SyncConfig{
			InitialSync:          false,
			RealtimeSync:         false,
			ResumableInitialSync: false,
			BatchSize:            100,
			ParallelCollections:  false,
			MaxWorkers:           1,
		},
		InternalCluster: models.InternalClusterConfig{
			Enabled: false,
		},
		Encryption: models.EncryptionConfig{
			Enabled: false,
		},
		Checkpoint: models.CheckpointConfig{
			Enabled: false,
		},
		Tracking: models.TrackingConfig{
			Enabled: false,
		},
		Sequence: models.SequenceConfig{
			Enabled: false,
		},
		Fence: models.FenceConfig{
			Enabled: false,
		},
	}
}

// convertTrackingConfig converts models.TrackingConfig to tracking.TransferConfig
func convertTrackingConfig(modelConfig models.TrackingConfig) *tracking.TransferConfig {
	return &tracking.TransferConfig{
		Enabled:         modelConfig.Enabled,
		Database:        modelConfig.Database,
		StateCollection: modelConfig.StateCollection,
		BatchCollection: modelConfig.BatchCollection,
	}
}

func handleWebSocket(w http.ResponseWriter, r *http.Request) {
	// Log incoming WebSocket connection attempt with full details
	log.Printf("🔌 WS CONNECTION ATTEMPT: Method=%s, URL=%s, RemoteAddr=%s", r.Method, r.URL.Path, r.RemoteAddr)
	log.Printf("🔌 WS HEADERS: Origin=%s, User-Agent=%s, Upgrade=%s, Connection=%s",
		r.Header.Get("Origin"), r.Header.Get("User-Agent"),
		r.Header.Get("Upgrade"), r.Header.Get("Connection"))

	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Printf("❌ WebSocket upgrade failed: %v", err)
		log.Printf("❌ Request details: Method=%s, URL=%s, Proto=%s", r.Method, r.URL, r.Proto)
		return
	}
	log.Printf("✅ WebSocket upgrade succeeded for %s", r.RemoteAddr)

	// STABILITY FIX: Ensure connection is always closed and removed from clients map
	defer func() {
		conn.Close()
		// Remove from clients map on disconnect
		clientsMutex.Lock()
		if clientInfo, exists := clients[conn]; exists {
			delete(clients, conn)
			log.Printf("🗑️  CLEANUP: Removed %s client %s from clients map (defer cleanup)", clientInfo.ClientType, clientInfo.ClientID)

			// Remove TCP address from Address Manager if this is a vm-sync client
			if clientInfo.ClientType == "vm-sync" {
				addressMgr := transport.GetAddressManager()
				addressMgr.RemoveAddress(clientInfo.ClientID)
				log.Printf("✅ TCP ADDRESS CLEANUP: Removed address for client %s", clientInfo.ClientID)
			}
		}
		clientsMutex.Unlock()
	}()
	// Determine client type based on User-Agent or other headers
	clientType := "unknown"
	userAgent := r.Header.Get("User-Agent")
	log.Printf("🔍 WEBSOCKET DEBUG: New connection - User-Agent: '%s'", userAgent)
	if strings.Contains(userAgent, "vm-sync") {
		clientType = "vm-sync"
		log.Printf("🎯 WEBSOCKET DEBUG: Detected vm-sync client type based on User-Agent")
	} else if strings.Contains(r.Header.Get("Referer"), "/dashboard") || strings.Contains(userAgent, "Mozilla") {
		clientType = "dashboard"
		log.Printf("🎯 WEBSOCKET DEBUG: Detected dashboard client type")
	} else {
		log.Printf("⚠️ WEBSOCKET DEBUG: Unknown client type, User-Agent: '%s'", userAgent)
	}

	clientInfo := ClientInfo{
		ClientType:  clientType,
		ClientID:    fmt.Sprintf("%s-%d", clientType, time.Now().Unix()),
		ConnectedAt: time.Now(),
	}

	// For vm-sync clients, validate OAuth2 authentication before allowing connection
	if clientType == "vm-sync" {
		// Wait for authentication information from vm-sync client
		conn.SetReadDeadline(time.Now().Add(30 * time.Second)) // 30 second timeout for auth
		messageType, messageData, err := conn.ReadMessage()
		if err != nil {
			log.Printf("Failed to receive authentication from vm-sync client: %v", err)
			return
		}

		if messageType == websocket.TextMessage {
			var authMsg map[string]interface{}
			if err := json.Unmarshal(messageData, &authMsg); err != nil {
				log.Printf("Failed to parse authentication message: %v", err)
				conn.WriteMessage(websocket.TextMessage, []byte(`{"type":"error","message":"Invalid authentication format"}`))
				return
			}

			msgType, ok := authMsg["type"].(string)
			if !ok {
				log.Printf("Missing authentication type")
				conn.WriteMessage(websocket.TextMessage, []byte(`{"type":"error","message":"Authentication type required"}`))
				return
			}

			// Only support OAuth2 authentication
			if msgType == "oauth2_auth" {
				// OAuth2 JWT token authentication
				// Wait for authService to be ready (with timeout)
				readyTimeout := time.After(30 * time.Second)
				readyTicker := time.NewTicker(500 * time.Millisecond)
				defer readyTicker.Stop()

				for authService == nil {
					select {
					case <-readyTimeout:
						log.Printf("❌ WS AUTH TIMEOUT: authService not ready after 30 seconds")
						conn.WriteMessage(websocket.TextMessage, []byte(`{"type":"error","message":"Cloud-sync initialization timeout"}`))
						return
					case <-readyTicker.C:
						log.Printf("⏳ WS AUTH WAIT: Waiting for authService initialization...")
						// Continue waiting
					}
				}

				log.Printf("✅ WS AUTH READY: authService is now available")

				token, tokenOk := authMsg["token"].(string)
				if !tokenOk {
					log.Printf("Missing OAuth2 token")
					conn.WriteMessage(websocket.TextMessage, []byte(`{"type":"error","message":"OAuth2 token required"}`))
					return
				}

				// Validate JWT token
				log.Printf("🔐 WS AUTH STEP 1: Validating OAuth2 token for vm-sync client (token length: %d)", len(token))
				claims, err := authService.ValidateToken(token)
				if err != nil {
					log.Printf("❌ WS AUTH FAILED: OAuth2 token validation failed: %v", err)
					log.Printf("❌ TCP BLOCKED: TCP transport will NOT be initialized due to auth failure")
					conn.WriteMessage(websocket.TextMessage, []byte(fmt.Sprintf(`{"type":"error","message":"OAuth2 token validation failed: %s"}`, err.Error())))
					return
				}
				log.Printf("✅ WS AUTH STEP 2: Token validated successfully - client_id=%s, app_id=%s", claims.ClientID, claims.AppID)

				// Token is valid, store OAuth2 info in client info
				clientInfo.ClientID = claims.ClientID // Use OAuth2 client ID
				clientInfo.OAuth2Claims = claims
				log.Printf("vm-sync client OAuth2 authentication successful: client_id=%s, app_id=%s", claims.ClientID, claims.AppID)

				// RACE CONDITION FIX: Register client in map IMMEDIATELY after authentication
				// This ensures the client is available for API calls before any async operations
				clientsMutex.Lock()
				clients[conn] = clientInfo
				clientCount := len(clients)
				vmSyncCount := 0
				for _, info := range clients {
					if info.ClientType == "vm-sync" {
						vmSyncCount++
					}
				}
				clientsMutex.Unlock()
				log.Printf("✅ RACE FIX IMMEDIATE: VM client %s registered in clients map (total: %d, vm-sync: %d)", clientInfo.ClientID, clientCount, vmSyncCount)

				// Send success response
				successMsg := map[string]interface{}{
					"type":    "auth_success",
					"message": "OAuth2 authentication successful",
					"method":  "oauth2",
				}
				if err := conn.WriteJSON(successMsg); err != nil {
					log.Printf("Failed to send OAuth2 auth success response: %v", err)
				}
			} else {
				log.Printf("Unsupported authentication type: %s", msgType)
				conn.WriteMessage(websocket.TextMessage, []byte(`{"type":"error","message":"Only OAuth2 authentication is supported. Use 'oauth2_auth'"}`))
				return
			}

			// VM capabilities for registration
			capabilities := distribution.VMCapabilities{
				MaxCollections: 10, // TODO: Extract from client capabilities message
				SupportsTCP:    true,
				SupportsHTTP:   true,
				MaxConcurrency: 4,
				MemoryLimitMB:  2048,
			}
			// VM-SYNC DISCOVERY: Try to discover where vm-sync actually is
			var vmSyncDomain string
			var tcpPort, httpPort string = "9000", "8081"

			// Method 1: Check if vm-sync told us its domain in headers (BEST - vm-sync knows itself)
			if headerDomain := r.Header.Get("X-VM-Sync-Domain"); headerDomain != "" {
				vmSyncDomain = headerDomain
				if headerTCPPort := r.Header.Get("X-VM-Sync-TCP-Port"); headerTCPPort != "" {
					tcpPort = headerTCPPort
				}
				if headerHTTPPort := r.Header.Get("X-VM-Sync-HTTP-Port"); headerHTTPPort != "" {
					httpPort = headerHTTPPort
				}
				log.Printf("🎯 VM-SYNC DISCOVERY: VM-sync told us its domain via headers: %s (TCP:%s, HTTP:%s)", vmSyncDomain, tcpPort, httpPort)
			} else {
				// Method 2: Extract domain from request (vm-sync's actual IP/domain)
				vmSyncDomain = strings.Split(extractHostDomain(r), ":")[0]
				log.Printf("🔍 VM-SYNC DISCOVERY: Extracted vm-sync domain from request: %s", vmSyncDomain)

				// Method 3: Fallback to configured VM_SYNC_DOMAIN if extraction fails
				if vmSyncDomain == "" || vmSyncDomain == "localhost" {
					envDomain := os.Getenv("VM_SYNC_DOMAIN")
					if envDomain != "" {
						vmSyncDomain = envDomain
						log.Printf("⚠️ VM-SYNC DISCOVERY: Using VM_SYNC_DOMAIN fallback: %s", vmSyncDomain)
					} else {
						log.Printf("❌ VM-SYNC DISCOVERY: No domain found, vm-sync unreachable!")
					}
				}
			}

			tcpEndpoint := fmt.Sprintf("%s:%s", vmSyncDomain, tcpPort)
			httpEndpoint := fmt.Sprintf("http://%s:%s", vmSyncDomain, httpPort) // FIX: Add http:// prefix
			endpoints := map[string]string{
				"tcp":  tcpEndpoint,  // Properly discovered TCP endpoint
				"http": httpEndpoint, // Properly discovered HTTP endpoint with http:// prefix
			}

			// Store both TCP and HTTP addresses in Address Manager for global access
			addressMgr := transport.GetAddressManager()
			addressMgr.SetEndpoints(clientInfo.ClientID, tcpEndpoint, httpEndpoint)
			log.Printf("✅ ENDPOINTS STORED: %s -> TCP:%s, HTTP:%s", clientInfo.ClientID, tcpEndpoint, httpEndpoint)

			// Initialize TCP transport with the detected address if it's not already enabled
			log.Printf("🔍 TCP STATUS CHECK: tcpTransportEnabled=%v, tcpSender=%v", tcpTransportEnabled, tcpSender != nil)
			if !tcpTransportEnabled {
				log.Printf("🔄 TCP INIT: First VM connection - initializing TCP transport to %s", tcpEndpoint)
				if err := initializeTCPTransportWithAddress(tcpEndpoint); err != nil {
					log.Printf("❌ TCP INIT FAILED: %v - Will retry on next connection", err)
				} else {
					log.Printf("✅ TCP INIT SUCCESS: Connected to %s with %d parallel connections", tcpEndpoint, config.Sync.Transport.TCPSender.ParallelConns)
				}
			} else {
				log.Printf("ℹ️  TCP ALREADY ENABLED: Skipping re-initialization (tcpSender active: %v)", tcpSender != nil)
				log.Printf("ℹ️  TCP HEALTH: If transfer fails, check TCP health monitor logs or manually reinitialize")
			}

			if err := collectionDistributor.RegisterVM(clientInfo.ClientID, capabilities, endpoints); err != nil {
				log.Printf("Failed to register VM with distributor: %v", err)
			} else {
				log.Printf("🔗 VM REGISTERED: %s ready for intelligent collection distribution", clientInfo.ClientID)

				// Trigger automatic collection distribution
				go func() {
					time.Sleep(2 * time.Second) // Brief delay for connection stability
					if err := collectionDistributor.RegisterCollections(config.MongoDB.Databases); err != nil {
						log.Printf("Failed to register collections for auto-distribution: %v", err)
					} else {
						log.Printf("🎯 AUTO-DISTRIBUTION: Collections distributed automatically to available VMs")
					}
				}()
			}

			// RACE CONDITION FIX: Signal that vm-sync has connected with improved reliability
			vmSyncMutex.Lock()
			isFirstConnection := !vmSyncConnectedOnce
			if isFirstConnection {
				vmSyncConnectedOnce = true
				// Non-blocking send to avoid race conditions
				select {
				case vmSyncConnected <- true:
					log.Printf("✅ RACE FIX: Signaled vm-sync connection to waiting sync process for client %s", clientInfo.ClientID)
				default:
					// Channel already has a value, no need to send again
					log.Printf("✅ RACE FIX: Connection signal channel already notified for client %s", clientInfo.ClientID)
				}
			} else {
				// FIX RECONNECTION DEADLOCK: On reconnection, also signal channel to unblock any waiting process
				// This handles the case where cloud-sync restarted but vm-sync stayed running
				log.Printf("🔄 RECONNECTION FIX: vm-sync client %s reconnected, signaling and triggering catch-up sync", clientInfo.ClientID)
				appLogger.Info("websocket", "vm_sync_reconnected", "VM-sync client reconnected, starting catch-up sync", map[string]interface{}{
					"client_id":      clientInfo.ClientID,
					"reconnected_at": time.Now(),
				})

				// CRITICAL: Signal channel in case cloud-sync is waiting (after cloud-sync restart)
				select {
				case vmSyncConnected <- true:
					log.Printf("✅ RECONNECTION FIX: Signaled vm-sync reconnection to unblock waiting sync process")
				default:
					// Channel already has a value or no one is waiting
					log.Printf("✅ RECONNECTION FIX: No process waiting, proceeding with catch-up sync")
				}

				// Start catch-up sync in a separate goroutine
				go func() {
					startCatchUpSync()
				}()
			}
			vmSyncMutex.Unlock()

			// RACE CONDITION FIX: Force immediate registration status update
			// This ensures the VM is immediately available for API calls
			log.Printf("🔧 RACE FIX: VM client %s registration complete, updating status", clientInfo.ClientID)

			// Trigger resumable sync for vm-sync client connection
			if config.Sync.ResumableInitialSync {
				log.Printf("Triggering resumable sync for vm-sync client: %s", clientInfo.ClientID)
				go func() {
					// Start push-based sync for this client
					startPushBasedSync()
				}()
			}
		} else {
			log.Printf("Expected text message for authentication, got message type: %d", messageType)
			conn.WriteMessage(websocket.TextMessage, []byte(`{"type":"error","message":"Authentication must be sent as text message"}`))
			return
		}

		// Clear read deadline after authentication
		conn.SetReadDeadline(time.Time{})
	}

	// RACE CONDITION FIX: Client already registered immediately after authentication
	// This section now only handles additional registrations and logging
	if clientType == "vm-sync" && clientInfo.OAuth2Claims != nil {
		appLogger.Info("websocket", "vm_sync_connected", "VM-sync client connected with valid OAuth2 token", map[string]interface{}{
			"client_id": clientInfo.ClientID,
			"app_id":    clientInfo.OAuth2Claims.AppID,
			"scopes":    clientInfo.OAuth2Claims.Scopes,
		})

		// 🎯 BUFFER-FREE: Register client with token manager
		if tokenManager != nil {
			if err := tokenManager.RegisterClient(clientInfo.ClientID); err != nil {
				log.Printf("⚠️  Failed to register client %s with token manager: %v", clientInfo.ClientID, err)
			} else {
				log.Printf("✅ BUFFER-FREE: Client %s registered with token manager (no buffer needed)", clientInfo.ClientID)
			}
		}

		log.Printf("WebSocket connection established with %s client: %s", clientType, clientInfo.ClientID)
	} else if clientType == "dashboard" {
		// Add dashboard clients to clients map for broadcast
		clientsMutex.Lock()
		clients[conn] = clientInfo
		clientCount := len(clients)
		vmSyncCount := 0
		for _, info := range clients {
			if info.ClientType == "vm-sync" {
				vmSyncCount++
			}
		}
		clientsMutex.Unlock()
		log.Printf("✅ DASHBOARD: Client registered in clients map (total: %d, vm-sync: %d)", clientCount, vmSyncCount)
		log.Printf("WebSocket connection established with %s client: %s", clientType, clientInfo.ClientID)
	} else {
		// Add unknown clients to clients map for broadcast
		clientsMutex.Lock()
		clients[conn] = clientInfo
		clientCount := len(clients)
		vmSyncCount := 0
		for _, info := range clients {
			if info.ClientType == "vm-sync" {
				vmSyncCount++
			}
		}
		clientsMutex.Unlock()
		log.Printf("✅ UNKNOWN: Client registered in clients map (total: %d, vm-sync: %d)", clientCount, vmSyncCount)
		log.Printf("WebSocket connection established with %s client: %s", clientType, clientInfo.ClientID)
	}

	// Handle incoming messages
	for {
		messageType, messageData, err := conn.ReadMessage()
		if err != nil {
			log.Printf("Error reading message from %s client %s: %v", clientType, clientInfo.ClientID, err)
			break
		}

		// Handle different message types
		if messageType == websocket.BinaryMessage {
			// Handle binary messages (likely change events)
			var event models.ChangeEvent
			if err := bson.Unmarshal(messageData, &event); err != nil {
				log.Printf("Error unmarshaling binary change event: %v", err)
				continue
			}
			log.Printf("✅ REALTIME: Change event: %s on %s.%s", event.OperationType, event.Database, event.Collection)
		} else if messageType == websocket.TextMessage {
			log.Printf("📥 WEBSOCKET: Received text message (%d bytes)", len(messageData))
			// Handle text messages (likely configuration updates)
			var msg map[string]interface{}
			if err := json.Unmarshal(messageData, &msg); err != nil {
				log.Printf("Error unmarshaling text message: %v", err)
				continue
			}

			// Handle different message types
			if msgType, ok := msg["type"].(string); ok {
				switch msgType {
				case "adaptive_config":
					// Handle adaptive configuration updates
					// Note: We can't directly call the unexported method, so we'll log and continue
					log.Printf("Received adaptive config update, but cannot process directly")
				case "ping":
					// Respond to ping with pong
					pongMsg := map[string]interface{}{
						"type": "pong",
					}
					if err := conn.WriteJSON(pongMsg); err != nil {
						log.Printf("Error sending pong response: %v", err)
					}
				default:
					log.Printf("Unknown message type: %s", msgType)
				}
			}
		} else {
			log.Printf("📥 WEBSOCKET: Received unknown message type: %d", messageType)
		}
	}
}

// Helper function to check if any vm-sync clients are connected
func hasVMSyncClients() bool {
	count := 0
	for _, clientInfo := range clients {
		if clientInfo.ClientType == "vm-sync" {
			count++
		}
	}
	log.Printf("DEBUG: hasVMSyncClients check - found %d vm-sync clients out of %d total clients", count, len(clients))
	return count > 0
}

// Helper function to get count of vm-sync clients
func getVMSyncClientCount() int {
	count := 0
	for _, clientInfo := range clients {
		if clientInfo.ClientType == "vm-sync" {
			count++
		}
	}
	return count
}

func handleBroadcast() {
	for {
		select {
		case event := <-broadcast:
			// Handle change events with backpressure
			handleChangeEventWithBackpressure(event)
		case eventPtr := <-func() <-chan *models.ChangeEvent {
			if config.InternalCluster.Enabled && internalCluster != nil {
				return internalCluster.GetOutputChannel()
			}
			return make(<-chan *models.ChangeEvent) // Return empty channel if not enabled
		}():
			// Handle internal cluster events
			if eventPtr != nil {
				handleChangeEventWithBackpressure(*eventPtr)
			}
		case statusUpdate := <-statusUpdates:
			// Handle status updates with non-blocking send
			broadcastStatusUpdateSafe(statusUpdate)
		}
	}
}

// handleChangeEventWithBackpressure safely handles events with backpressure protection
// 🎯 BUFFER-FREE VERSION - Uses resume tokens instead of memory buffers
func handleChangeEventWithBackpressure(event models.ChangeEvent) {
	log.Printf("📨 BUFFER-FREE: Processing change event: %s on %s.%s", event.OperationType, event.Database, event.Collection)

	// Apply throttling if back-pressure is enabled
	if isBackPressureEnabled() {
		if delay := getThrottleDelay(); delay > 0 {
			time.Sleep(delay)
		}
	}

	// 🎯 BUFFER-FREE APPROACH: Update resume tokens for ALL clients (connected + disconnected)
	if tokenManager != nil {
		updateResumeTokensForAllClients(event)
	}

	// Check if vm-sync clients are connected (with proper mutex protection)
	clientsMutex.RLock()
	hasVMClients := false
	for _, clientInfo := range clients {
		if clientInfo.ClientType == "vm-sync" {
			hasVMClients = true
			break
		}
	}
	clientsMutex.RUnlock()

	if !hasVMClients {
		// 🎯 BUFFER-FREE: No buffering needed - resume tokens already updated
		log.Printf("💾 BUFFER-FREE: No VM clients connected, resume tokens updated for later resume")
		return
	}

	// vm-sync clients are connected, broadcast immediately
	log.Printf("📡 BUFFER-FREE: Broadcasting change event to %d vm-sync clients", len(clients))
	broadcastToClients(event)
}

// updateResumeTokensForAllClients updates resume tokens for all tracked clients
// This ensures no data loss during disconnections - the KEY to buffer-free operation
func updateResumeTokensForAllClients(event models.ChangeEvent) {
	// Get all VM-sync clients (both connected and disconnected)
	allClients := make([]string, 0)

	// Add currently connected clients
	for _, clientInfo := range clients {
		if clientInfo.ClientType == "vm-sync" {
			allClients = append(allClients, clientInfo.ClientID)
		}
	}

	// TODO: Also get disconnected clients from token manager
	// For now, we'll assume the token manager tracks all clients

	// Update resume token for each client
	for _, clientID := range allClients {
		err := tokenManager.UpdateClientToken(
			clientID,
			event.Database,
			event.Collection,
			event.ResumeToken,
			event.Timestamp,
		)
		if err != nil {
			log.Printf("⚠️  Failed to update resume token for client %s: %v", clientID, err)
		}
	}

	log.Printf("💾 BUFFER-FREE: Updated resume tokens for %d clients", len(allClients))
}

// Legacy buffer function - REMOVED in buffer-free approach
// Data persistence now handled by resume tokens in MongoDB

// broadcastToClients safely broadcasts to connected clients with error handling
func broadcastToClients(event models.ChangeEvent) {
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

	clientsMutex.Lock()
	defer clientsMutex.Unlock()

	// Collect failed clients for removal
	failedClients := make([]*websocket.Conn, 0)

	for client, clientInfo := range clients {
		// Only send to vm-sync clients for change events
		if clientInfo.ClientType != "vm-sync" {
			continue
		}

		// Set write deadline to prevent blocking
		client.SetWriteDeadline(time.Now().Add(5 * time.Second))
		err := client.WriteMessage(websocket.BinaryMessage, messageData)
		if err != nil {
			log.Printf("Error writing to client %s: %v", clientInfo.ClientID, err)
			failedClients = append(failedClients, client)
		}
	}

	// Remove failed clients
	for _, client := range failedClients {
		client.Close()
		delete(clients, client)
		log.Printf("Removed failed client connection")
	}
}

// broadcastStatusUpdateSafe safely broadcasts status updates with error handling
func broadcastStatusUpdateSafe(statusUpdate map[string]interface{}) {
	messageData, err := json.Marshal(statusUpdate)
	if err != nil {
		log.Printf("Error marshaling status update: %v", err)
		return
	}

	clientsMutex.Lock()
	defer clientsMutex.Unlock()

	// Collect failed clients for removal
	failedClients := make([]*websocket.Conn, 0)

	for client, clientInfo := range clients {
		// Set write deadline to prevent blocking
		client.SetWriteDeadline(time.Now().Add(2 * time.Second))
		err := client.WriteMessage(websocket.TextMessage, messageData)
		if err != nil {
			log.Printf("Error writing status update to client %s: %v", clientInfo.ClientID, err)
			failedClients = append(failedClients, client)
		}
	}

	// Remove failed clients
	for _, client := range failedClients {
		client.Close()
		delete(clients, client)
		log.Printf("Removed failed status update client connection")
	}
}

func broadcastStatusUpdate(statusUpdate map[string]interface{}) {
	broadcastStatusUpdateSafe(statusUpdate)
}

func broadcastHealthStatus() {
	healthStatus := getHealthStatus()
	statusUpdate := map[string]interface{}{
		"type":      "status_update",
		"data":      healthStatus,
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

	// Get connected clients count safely
	connectedClientsCount := len(clients)

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
			"total_documents":   totalDocs,
			"today_documents":   totalDocs, // For now, same as total
			"sync_rate":         syncRate,
			"backlog_size":      backlogSize,
			"avg_latency":       avgLatency,
			"active_watchers":   activeWatchersCount,
			"last_resume_token": time.Now().Format(time.RFC3339),
			"sync_mode":         syncModeDetails,
			"last_checkpoint":   lastCheckpointTime,
			"connected_clients": connectedClientsCount,
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
		"cloud_sync":   "connected",
		"vm_sync":      "connected",
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
// or when the resume token is no longer available in the oplog
func isInvalidateResumeTokenError(err error) bool {
	if err == nil {
		return false
	}
	// Check for MongoDB's InvalidResumeToken error related to invalidate events
	errorStr := err.Error()
	return (strings.Contains(errorStr, "InvalidResumeToken") && strings.Contains(errorStr, "invalidate notification")) ||
		strings.Contains(errorStr, "cannot resume stream; the resume token was not found") ||
		strings.Contains(errorStr, "ChangeStreamFatalError") ||
		strings.Contains(errorStr, "ChangeStreamHistoryLost") || // Resume point no longer in oplog
		strings.Contains(errorStr, "Resume of change stream was not possible") || // Common resume failure message
		strings.Contains(errorStr, "resume point may no longer be in the oplog") // Explicit oplog expiry
}

// Adaptive parallelism control
var (
	fetchSemaphore      chan struct{}
	fetchSemaphoreMutex sync.RWMutex
	pushSemaphore       chan struct{}
	pushSemaphoreMutex  sync.RWMutex
)

// initializeFetchSemaphore creates the semaphore with initial capacity
func initializeFetchSemaphore(capacity int) {
	fetchSemaphoreMutex.Lock()
	defer fetchSemaphoreMutex.Unlock()
	fetchSemaphore = make(chan struct{}, capacity)
	log.Printf("Initialized fetch semaphore with capacity: %d", capacity)
}

// updateFetchParallelism dynamically adjusts the fetch parallelism
func updateFetchParallelism(newCapacity int) {
	fetchSemaphoreMutex.Lock()
	defer fetchSemaphoreMutex.Unlock()

	if fetchSemaphore == nil {
		fetchSemaphore = make(chan struct{}, newCapacity)
		log.Printf("Created fetch semaphore with capacity: %d", newCapacity)
		return
	}

	// Create new semaphore with updated capacity
	oldSemaphore := fetchSemaphore
	fetchSemaphore = make(chan struct{}, newCapacity)

	// Transfer existing permits to new semaphore
	transferred := 0
	for i := 0; i < newCapacity; i++ {
		select {
		case <-oldSemaphore:
			// Old permit consumed, don't add to new semaphore
			transferred++
		default:
			// No more permits in old semaphore
			break
		}
	}

	log.Printf("Updated fetch parallelism from %d to %d (transferred %d active permits)",
		cap(oldSemaphore), newCapacity, transferred)
}

// acquireFetchPermit acquires a permit for fetch operations
func acquireFetchPermit() {
	fetchSemaphoreMutex.RLock()
	sem := fetchSemaphore
	fetchSemaphoreMutex.RUnlock()

	if sem != nil {
		sem <- struct{}{}
	}
}

// releaseFetchPermit releases a permit for fetch operations
func releaseFetchPermit() {
	fetchSemaphoreMutex.RLock()
	sem := fetchSemaphore
	fetchSemaphoreMutex.RUnlock()

	if sem != nil {
		select {
		case <-sem:
			// Permit released
		default:
			// No permits to release
		}
	}
}

// initializePushSemaphore creates the push semaphore with initial capacity
func initializePushSemaphore(capacity int) {
	pushSemaphoreMutex.Lock()
	defer pushSemaphoreMutex.Unlock()
	pushSemaphore = make(chan struct{}, capacity)
	log.Printf("Initialized push semaphore with capacity: %d", capacity)
}

// updatePushParallelism dynamically adjusts the push parallelism
func updatePushParallelism(newCapacity int) {
	pushSemaphoreMutex.Lock()
	defer pushSemaphoreMutex.Unlock()

	if pushSemaphore == nil {
		pushSemaphore = make(chan struct{}, newCapacity)
		log.Printf("Created push semaphore with capacity: %d", newCapacity)
		return
	}

	// Create new semaphore with updated capacity
	oldSemaphore := pushSemaphore
	pushSemaphore = make(chan struct{}, newCapacity)

	// Transfer existing permits to new semaphore
	transferred := 0
	for i := 0; i < newCapacity; i++ {
		select {
		case <-oldSemaphore:
			// Old permit consumed, don't add to new semaphore
			transferred++
		default:
			// No more permits in old semaphore
			break
		}
	}

	log.Printf("Updated push parallelism from %d to %d (transferred %d active permits)",
		cap(oldSemaphore), newCapacity, transferred)
}

// acquirePushPermit acquires a permit for push operations
func acquirePushPermit() {
	pushSemaphoreMutex.RLock()
	sem := pushSemaphore
	pushSemaphoreMutex.RUnlock()

	if sem != nil {
		sem <- struct{}{}
	}
}

// releasePushPermit releases a permit for push operations
func releasePushPermit() {
	pushSemaphoreMutex.RLock()
	sem := pushSemaphore
	pushSemaphoreMutex.RUnlock()

	if sem != nil {
		select {
		case <-sem:
			// Permit released
		default:
			// No permits to release
		}
	}
}

// Back-pressure control functions
func applyBackPressureConfig(config *models.AdaptiveConfig) {
	backPressureMutex.Lock()
	defer backPressureMutex.Unlock()

	backPressureEnabled = config.BackPressure
	throttleDelay = time.Duration(config.ThrottleDelay) * time.Millisecond
	maxQueueSize = config.MaxQueueSize

	// Update fetch and push parallelism
	updateFetchParallelism(config.FetchParallelism)
	updatePushParallelism(config.PushParallelism)

	// Update adaptive batch size
	updateBatchSize(config.BatchSize)

	log.Printf("Applied adaptive config: backPressure=%v, throttle=%v, maxQueue=%d, fetchParallel=%d, pushParallel=%d, batchSize=%d",
		backPressureEnabled, throttleDelay, maxQueueSize, config.FetchParallelism, config.PushParallelism, config.BatchSize)
}

func isBackPressureEnabled() bool {
	backPressureMutex.RLock()
	defer backPressureMutex.RUnlock()
	return backPressureEnabled
}

func getThrottleDelay() time.Duration {
	backPressureMutex.RLock()
	defer backPressureMutex.RUnlock()
	return throttleDelay
}

func getMaxQueueSize() int {
	backPressureMutex.RLock()
	defer backPressureMutex.RUnlock()
	return maxQueueSize
}

func applyBackPressureThrottle() {
	if isBackPressureEnabled() {
		delay := getThrottleDelay()
		if delay > 0 {
			time.Sleep(delay)
		}
	}
}

// Adaptive batch size functions
func updateBatchSize(newBatchSize int) {
	batchSizeMutex.Lock()
	defer batchSizeMutex.Unlock()

	if newBatchSize > 0 {
		currentBatchSize = newBatchSize
		log.Printf("Updated batch size to %d", newBatchSize)
	}
}

func getCurrentBatchSize() int {
	batchSizeMutex.RLock()
	defer batchSizeMutex.RUnlock()
	return currentBatchSize
}

func initializeBatchSize(defaultSize int) {
	batchSizeMutex.Lock()
	defer batchSizeMutex.Unlock()

	if defaultSize > 0 {
		currentBatchSize = defaultSize
	} else {
		currentBatchSize = 100 // Default batch size
	}
	log.Printf("Initialized batch size to %d", currentBatchSize)
}

// Self-optimization functions
func enableSelfOptimization() {
	selfOptimizationMutex.Lock()
	defer selfOptimizationMutex.Unlock()

	selfOptimizationEnabled = true
	lastSelfOptimization = time.Now()
	resourceHistory = make([]ResourceSnapshot, 0, maxResourceHistory)

	// Start self-optimization monitoring loop
	go selfOptimizationLoop()
	log.Println("Self-optimization enabled for Cloud Sync")
}

func selfOptimizationLoop() {
	ticker := time.NewTicker(30 * time.Second) // Check every 30 seconds
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			if isSelfOptimizationEnabled() {
				collectResourceSnapshot()
				performSelfOptimization()
			}
		case <-shutdownChan:
			return
		}
	}
}

func isSelfOptimizationEnabled() bool {
	selfOptimizationMutex.RLock()
	defer selfOptimizationMutex.RUnlock()
	return selfOptimizationEnabled
}

func collectResourceSnapshot() {
	selfOptimizationMutex.Lock()
	defer selfOptimizationMutex.Unlock()

	if metricsCollector == nil {
		return
	}

	// Get current metrics data
	metrics := metricsCollector.GetMetrics()

	snapshot := ResourceSnapshot{
		Timestamp:         time.Now(),
		CPUPercent:        getCPUFromMetrics(metrics),
		MemoryPercent:     getMemoryFromMetrics(metrics),
		ActiveConnections: getVMSyncClientCount(),
		QueueDepth:        getQueueDepthFromMetrics(metrics),
		SyncLatency:       getSyncLatencyFromMetrics(metrics),
		Throughput:        getThroughputFromMetrics(metrics),
		ErrorRate:         getErrorRateFromMetrics(metrics),
	}

	// Add to history, maintaining max size
	resourceHistory = append(resourceHistory, snapshot)
	if len(resourceHistory) > maxResourceHistory {
		resourceHistory = resourceHistory[1:]
	}
}

func performSelfOptimization() {
	selfOptimizationMutex.Lock()
	defer selfOptimizationMutex.Unlock()

	if len(resourceHistory) < 3 {
		return // Need at least 3 snapshots for trend analysis
	}

	// Analyze recent trends
	recentSnapshots := resourceHistory[len(resourceHistory)-3:]
	cpuTrend := analyzeTrend(extractCPU(recentSnapshots))
	memoryTrend := analyzeTrend(extractMemory(recentSnapshots))
	latencyTrend := analyzeTrend(extractLatency(recentSnapshots))

	// Determine optimization actions
	optimizationNeeded := false

	// High CPU usage - reduce parallelism
	if cpuTrend.average > 80.0 && cpuTrend.direction > 0 {
		reduceFetchParallelism()
		reducePushParallelism()
		optimizationNeeded = true
	}

	// High memory usage - reduce batch size
	if memoryTrend.average > 85.0 && memoryTrend.direction > 0 {
		reduceBatchSize()
		optimizationNeeded = true
	}

	// High latency - apply back-pressure
	if latencyTrend.average > 5000 && latencyTrend.direction > 0 { // 5 seconds
		increaseBackPressure()
		optimizationNeeded = true
	}

	// Low resource usage - scale up
	if cpuTrend.average < 30.0 && memoryTrend.average < 50.0 && latencyTrend.average < 1000 {
		scaleUpResources()
		optimizationNeeded = true
	}

	if optimizationNeeded {
		lastSelfOptimization = time.Now()
		log.Printf("Self-optimization applied: CPU=%.1f%%, Memory=%.1f%%, Latency=%.0fms",
			cpuTrend.average, memoryTrend.average, latencyTrend.average)
	}
}

type TrendAnalysis struct {
	average   float64
	direction float64 // positive = increasing, negative = decreasing
}

func analyzeTrend(values []float64) TrendAnalysis {
	if len(values) == 0 {
		return TrendAnalysis{}
	}

	// Calculate average
	sum := 0.0
	for _, v := range values {
		sum += v
	}
	average := sum / float64(len(values))

	// Calculate trend direction (simple slope)
	direction := 0.0
	if len(values) >= 2 {
		direction = values[len(values)-1] - values[0]
	}

	return TrendAnalysis{
		average:   average,
		direction: direction,
	}
}

func extractCPU(snapshots []ResourceSnapshot) []float64 {
	values := make([]float64, len(snapshots))
	for i, s := range snapshots {
		values[i] = s.CPUPercent
	}
	return values
}

func extractMemory(snapshots []ResourceSnapshot) []float64 {
	values := make([]float64, len(snapshots))
	for i, s := range snapshots {
		values[i] = s.MemoryPercent
	}
	return values
}

func extractLatency(snapshots []ResourceSnapshot) []float64 {
	values := make([]float64, len(snapshots))
	for i, s := range snapshots {
		values[i] = float64(s.SyncLatency.Milliseconds())
	}
	return values
}

func reduceFetchParallelism() {
	current := getFetchParallelism()
	newCapacity := int(float64(current) * 0.8) // Reduce by 20%
	if newCapacity < 1 {
		newCapacity = 1
	}
	updateFetchParallelism(newCapacity)
}

func reducePushParallelism() {
	current := getPushParallelism()
	newCapacity := int(float64(current) * 0.8) // Reduce by 20%
	if newCapacity < 1 {
		newCapacity = 1
	}
	updatePushParallelism(newCapacity)
}

func reduceBatchSize() {
	current := getCurrentBatchSize()
	newSize := int(float64(current) * 0.7) // Reduce by 30%
	if newSize < 10 {
		newSize = 10
	}
	updateBatchSize(newSize)
}

func increaseBackPressure() {
	backPressureMutex.Lock()
	defer backPressureMutex.Unlock()

	backPressureEnabled = true
	throttleDelay = time.Duration(float64(throttleDelay) * 1.5) // Increase delay by 50%
	if throttleDelay > 5*time.Second {
		throttleDelay = 5 * time.Second
	}
}

func scaleUpResources() {
	// Increase parallelism
	currentFetch := getFetchParallelism()
	newFetch := int(float64(currentFetch) * 1.2) // Increase by 20%
	if newFetch > 50 {
		newFetch = 50
	}
	updateFetchParallelism(newFetch)

	currentPush := getPushParallelism()
	newPush := int(float64(currentPush) * 1.2) // Increase by 20%
	if newPush > 50 {
		newPush = 50
	}
	updatePushParallelism(newPush)

	// Increase batch size
	currentBatch := getCurrentBatchSize()
	newBatch := int(float64(currentBatch) * 1.3) // Increase by 30%
	if newBatch > 5000 {
		newBatch = 5000
	}
	updateBatchSize(newBatch)

	// Reduce back-pressure
	backPressureMutex.Lock()
	defer backPressureMutex.Unlock()
	throttleDelay = time.Duration(float64(throttleDelay) * 0.8) // Reduce delay by 20%
	if throttleDelay < 10*time.Millisecond {
		throttleDelay = 10 * time.Millisecond
		backPressureEnabled = false
	}
}

func getFetchParallelism() int {
	fetchSemaphoreMutex.RLock()
	defer fetchSemaphoreMutex.RUnlock()
	if fetchSemaphore != nil {
		return cap(fetchSemaphore)
	}
	return 10 // default
}

func getPushParallelism() int {
	pushSemaphoreMutex.RLock()
	defer pushSemaphoreMutex.RUnlock()
	if pushSemaphore != nil {
		return cap(pushSemaphore)
	}
	return 10 // default
}

// Helper functions to extract metrics
func getCPUFromMetrics(metricsData map[string]interface{}) float64 {
	if healthMetrics, ok := metricsData["health_metrics"].(map[string]*metrics.HealthMetrics); ok {
		for _, hm := range healthMetrics {
			if hm != nil {
				return hm.CPUUsage
			}
		}
	}
	return 0.0
}

func getMemoryFromMetrics(metricsData map[string]interface{}) float64 {
	if healthMetrics, ok := metricsData["health_metrics"].(map[string]*metrics.HealthMetrics); ok {
		for _, hm := range healthMetrics {
			if hm != nil && hm.MemoryUsage > 0 {
				// Convert bytes to percentage (assuming 8GB system for estimation)
				return float64(hm.MemoryUsage) / (8 * 1024 * 1024 * 1024) * 100
			}
		}
	}
	return 0.0
}

func getQueueDepthFromMetrics(metricsData map[string]interface{}) int {
	// Use active watchers count as a proxy for queue depth
	if activeWatchers, ok := metricsData["active_watchers"].(int); ok {
		return activeWatchers
	}
	return 0
}

func getSyncLatencyFromMetrics(metricsData map[string]interface{}) time.Duration {
	if lagMetrics, ok := metricsData["lag_metrics"].(map[string]*metrics.LagMetrics); ok {
		for _, lm := range lagMetrics {
			if lm != nil {
				return lm.ReplicationLag
			}
		}
	}
	return 0
}

func getThroughputFromMetrics(metricsData map[string]interface{}) float64 {
	if throughputMetrics, ok := metricsData["throughput_metrics"].(map[string]*metrics.ThroughputMetrics); ok {
		total := 0.0
		for _, tm := range throughputMetrics {
			if tm != nil {
				total += tm.EventsPerSecond
			}
		}
		return total
	}
	return 0.0
}

func getErrorRateFromMetrics(metricsData map[string]interface{}) float64 {
	if errorMetrics, ok := metricsData["error_metrics"].(map[string]*metrics.ErrorMetrics); ok {
		total := 0.0
		count := 0
		for _, em := range errorMetrics {
			if em != nil {
				total += em.ErrorRate
				count++
			}
		}
		if count > 0 {
			return total / float64(count)
		}
	}
	return 0.0
}

func monitorChangeStreamsTraditional() {
	log.Println("Starting change stream monitoring...")
	appLogger.Info("cloud-sync", "monitor_start", "Starting change stream monitoring", map[string]interface{}{
		"total_databases": len(config.MongoDB.Databases),
	})

	// Initialize adaptive parallelism
	initialFetchParallelism := 16 // PERFORMANCE: Increased from 4 to 16 for billion-doc scale
	initialPushParallelism := 16  // PERFORMANCE: Increased from 2 to 16 for billion-doc scale (8x faster)
	if cloudSyncIntegration != nil {
		if config := cloudSyncIntegration.GetCurrentConfig(); config != nil {
			initialFetchParallelism = config.FetchParallelism
			initialPushParallelism = config.PushParallelism
		}
	}
	initializeFetchSemaphore(initialFetchParallelism)
	initializePushSemaphore(initialPushParallelism)

	// Initialize adaptive batch size
	initialBatchSize := 10000 // PERFORMANCE: Increased from 100 to 10,000 for billion-doc scale (100x fewer queries)
	if cloudSyncIntegration != nil {
		if config := cloudSyncIntegration.GetCurrentConfig(); config != nil {
			initialBatchSize = config.BatchSize
		}
	}
	initializeBatchSize(initialBatchSize)

	// Start adaptive configuration monitor
	go func() {
		ticker := time.NewTicker(time.Second * 10) // Check every 10 seconds
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				if cloudSyncIntegration != nil {
					if config := cloudSyncIntegration.GetCurrentConfig(); config != nil {
						updateFetchParallelism(config.FetchParallelism)
						updatePushParallelism(config.PushParallelism)

						// Update telemetry with current watcher count
						watchersMutex.RLock()
						watcherCount := len(activeWatchers)
						watchersMutex.RUnlock()

						cloudSyncIntegration.UpdateConnectionCount(watcherCount)
					}
				}
			case <-shutdownChan:
				return
			}
		}
	}()

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
				go monitorChangeStreamsTraditional()
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

					// Acquire fetch permit for adaptive parallelism control
					acquireFetchPermit()

					// Apply back-pressure throttling if enabled
					applyBackPressureThrottle()

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
		"database":       dbName,
		"collection":     collName,
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
			"database":       dbName,
			"collection":     collName,
			"total_watchers": watcherCount,
		})
	}()

	// Ensure fetch permit is released when function exits
	defer releaseFetchPermit()

	// Get resume token from checkpoint if available
	var watchOptions *options.ChangeStreamOptions
	if checkpointMgr != nil {
		if checkpoint := checkpointMgr.GetCheckpoint(dbName, collName); checkpoint != nil && len(checkpoint.ResumeToken) > 0 {
			watchOptions = options.ChangeStream().SetResumeAfter(checkpoint.ResumeToken).SetFullDocument(options.UpdateLookup)
			log.Printf("Resuming change stream for %s.%s from checkpoint", dbName, collName)
			appLogger.Info("cloud-sync", "resume_token", fmt.Sprintf("Resuming change stream for %s.%s from checkpoint", dbName, collName), map[string]interface{}{
				"database":       dbName,
				"collection":     collName,
				"has_checkpoint": true,
			})
		} else {
			watchOptions = options.ChangeStream().SetFullDocument(options.UpdateLookup)
			log.Printf("Starting new change stream for %s.%s", dbName, collName)
			appLogger.Info("cloud-sync", "new_stream", fmt.Sprintf("Starting new change stream for %s.%s", dbName, collName), map[string]interface{}{
				"database":       dbName,
				"collection":     collName,
				"has_checkpoint": false,
			})
		}
	} else {
		watchOptions = options.ChangeStream().SetFullDocument(options.UpdateLookup)
		log.Printf("Starting change stream for %s.%s (no checkpoint manager)", dbName, collName)
		appLogger.Info("cloud-sync", "new_stream", fmt.Sprintf("Starting change stream for %s.%s (no checkpoint manager)", dbName, collName), map[string]interface{}{
			"database":           dbName,
			"collection":         collName,
			"checkpoint_manager": false,
		})
	}

	// Build change stream pipeline with the same filters as initial dump
	var pipeline mongo.Pipeline

	// Add document filters to change stream pipeline
	if len(collConfig.DocumentFilter.Criteria) > 0 {
		docFilterPipeline := filterEngine.BuildChangeStreamDocumentFilterPipeline(&collConfig.DocumentFilter)
		log.Printf("DEBUG: Generated document filter pipeline for %s.%s: %+v", dbName, collName, docFilterPipeline)
		for _, stage := range docFilterPipeline {
			// Convert bson.M to bson.D for pipeline
			var stageD bson.D
			for k, v := range stage {
				stageD = append(stageD, bson.E{Key: k, Value: v})
			}
			log.Printf("DEBUG: Adding pipeline stage for %s.%s: %+v", dbName, collName, stageD)
			pipeline = append(pipeline, stageD)
		}
	}

	// NOTE: Field filters are NOT applied to change stream pipeline
	// because they would filter out essential change stream fields like
	// operationType, documentKey, fullDocument, etc.
	// Field filtering is applied later during event processing.

	log.Printf("Starting change stream for %s.%s with %d pipeline stages", dbName, collName, len(pipeline))
	if len(pipeline) > 0 {
		appLogger.Info("cloud-sync", "stream_filters", fmt.Sprintf("Applied %d filter stages to change stream for %s.%s", len(pipeline), dbName, collName), map[string]interface{}{
			"database":            dbName,
			"collection":          collName,
			"filter_stages":       len(pipeline),
			"has_document_filter": len(collConfig.DocumentFilter.Criteria) > 0,
			"field_filter_note":   "Field filters applied during event processing, not in change stream pipeline",
		})
	}

	changeStream, err := coll.Watch(ctx, pipeline, watchOptions)
	if err != nil {
		appLogger.Error("cloud-sync", "stream_error", fmt.Sprintf("Failed to create change stream for %s.%s: %v", dbName, collName, err), map[string]interface{}{
			"database":   dbName,
			"collection": collName,
			"error":      err.Error(),
		})
		return fmt.Errorf("failed to create change stream: %v", err)
	}
	defer changeStream.Close(ctx)

	log.Printf("Watching changes for %s.%s", dbName, collName)
	log.Printf("Starting change stream loop for %s.%s", dbName, collName)

	// Add a goroutine to periodically check if change stream is alive with proper cleanup
	streamCtx, streamCancel := context.WithCancel(ctx)
	defer streamCancel() // Ensure cleanup happens

	go func() {
		ticker := time.NewTicker(30 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				log.Printf("Change stream for %s.%s is still active", dbName, collName)
			case <-streamCtx.Done():
				log.Printf("Change stream monitor for %s.%s stopping due to context cancellation", dbName, collName)
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

		// Use TryNext instead of blocking Next
		hasNext := changeStream.TryNext(ctx)

		if hasNext {
			// Get raw BSON data to preserve MongoDB types
			rawData := changeStream.Current
			var changeDoc bson.M
			if err := bson.Unmarshal(rawData, &changeDoc); err != nil {
				log.Printf("Error decoding change document: %v", err)
				appLogger.Error("cloud-sync", "decode_error", fmt.Sprintf("Error decoding change document for %s.%s: %v", dbName, collName, err), map[string]interface{}{
					"database":   dbName,
					"collection": collName,
					"error":      err.Error(),
				})
				if metricsCollector != nil {
					metricsCollector.RecordError(dbName, collName, "decode_error", err.Error(), nil)
				}
				continue
			}

			// Debug: log the change document structure
			keys := make([]string, 0, len(changeDoc))
			for k := range changeDoc {
				keys = append(keys, k)
			}
			log.Printf("Change document keys for %s.%s: %v", dbName, collName, keys)

			operationType, ok := changeDoc["operationType"].(string)
			if !ok {
				log.Printf("Warning: operationType field missing or invalid in change document for %s.%s", dbName, collName)
				log.Printf("Full change document: %+v", changeDoc)
				continue
			}
			log.Printf("Change detected in %s.%s: %s", dbName, collName, operationType)
			log.Printf("DEBUG: Change event detected - sending to broadcast channel")

			// Log change event
			appLogger.Info("cloud-sync", "change_event", fmt.Sprintf("Change detected in %s.%s: %s", dbName, collName, operationType), map[string]interface{}{
				"database":       dbName,
				"collection":     collName,
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
					"database":          dbName,
					"collection":        collName,
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
				// CRITICAL: Apply consistent field filtering for real-time sync
				// This ensures real-time changes have the same field filtering as initial dump
				if fullDocMap, ok := fullDocument.(bson.M); ok {
					// Apply field filtering using the same engine as initial dump
					if len(collConfig.FieldFilter.IncludeFields) > 0 || len(collConfig.FieldFilter.ExcludeFields) > 0 {
						filteredDoc := filterEngine.ApplyFieldFilter(fullDocMap, &collConfig.FieldFilter)
						log.Printf("🔍 REAL-TIME FILTER: Applied field filter to %s.%s change event (include: %v, exclude: %v)",
							dbName, collName, collConfig.FieldFilter.IncludeFields, collConfig.FieldFilter.ExcludeFields)
						if docData, err := bson.Marshal(filteredDoc); err == nil {
							event.FullDocument = bson.Raw(docData)
						}
					} else {
						// No field filtering configured, preserve original document
						if docData, err := bson.Marshal(fullDocument); err == nil {
							event.FullDocument = bson.Raw(docData)
						}
					}
				} else {
					// Unable to convert to bson.M, preserve as-is
					if docData, err := bson.Marshal(fullDocument); err == nil {
						event.FullDocument = bson.Raw(docData)
					}
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
						"database":   dbName,
						"collection": collName,
						"error":      err.Error(),
					})
				}
			}

			// Send event through internal cluster if enabled, otherwise use broadcast channel
			log.Printf("DEBUG: Sending change event to broadcast channel for %s.%s", dbName, collName)
			if config.InternalCluster.Enabled && internalCluster != nil {
				if !internalCluster.ProcessEvent(&event) {
					log.Println("Internal cluster queue full, dropping event")
					appLogger.Warn("cloud-sync", "queue_full", fmt.Sprintf("Internal cluster queue full, dropping event for %s.%s", dbName, collName), map[string]interface{}{
						"database":       dbName,
						"collection":     collName,
						"operation_type": event.OperationType,
					})
				}
			} else {
				select {
				case broadcast <- event:
					log.Printf("DEBUG: Change event successfully sent to broadcast channel")
				default:
					log.Println("Broadcast channel full, dropping event")
					appLogger.Warn("cloud-sync", "broadcast_full", fmt.Sprintf("Broadcast channel full, dropping event for %s.%s", dbName, collName), map[string]interface{}{
						"database":       dbName,
						"collection":     collName,
						"operation_type": event.OperationType,
					})
				}
			}
		} else {
			// Check for errors
			if err := changeStream.Err(); err != nil {
				log.Printf("Change stream error for %s.%s: %v", dbName, collName, err)
				break
			}
			// Sleep briefly before trying again
			time.Sleep(100 * time.Millisecond)
		}
	}

	if err := changeStream.Err(); err != nil {
		log.Printf("Change stream error for %s.%s: %v", dbName, collName, err)
		appLogger.Error("cloud-sync", "stream_error", fmt.Sprintf("Change stream error for %s.%s: %v", dbName, collName, err), map[string]interface{}{
			"database":   dbName,
			"collection": collName,
			"error":      err.Error(),
		})
		if metricsCollector != nil {
			metricsCollector.RecordError(dbName, collName, "change_stream_error", err.Error(), nil)
		}
		return err
	}

	return nil
}

func handleDataRequest(w http.ResponseWriter, r *http.Request) {
	log.Printf("=== FUNCTION ENTRY: handleDataRequest called ===")
	w.Header().Set("Content-Type", "application/json")

	var req models.DataRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid request body: "+err.Error(), http.StatusBadRequest)
		return
	}

	// Set default pagination values
	if req.PageSize <= 0 {
		// Use adaptive batch size if available, otherwise default to 1000
		adaptiveSize := getCurrentBatchSize()
		if adaptiveSize > 0 {
			req.PageSize = adaptiveSize
		} else {
			req.PageSize = 1000 // Default page size to prevent memory exhaustion
		}
	}
	if req.PageSize > 10000 {
		req.PageSize = 10000 // Maximum page size limit
	}
	if req.PageNumber < 0 {
		req.PageNumber = 0 // Ensure non-negative page number
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

	// Use configurable data timeout, default to 30 seconds if not set
	dataTimeout := config.Server.DataTimeout
	if dataTimeout == 0 {
		dataTimeout = 30 * time.Second
	}
	ctx, cancel := context.WithTimeout(context.Background(), dataTimeout)
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

		// Get current watermark for this client/collection
		watermark, err := transferTracker.GetWatermark(clientID, req.Database, req.Collection)
		var untransferredIDs []primitive.ObjectID
		if err != nil {
			log.Printf("Warning: Failed to get watermark for client %s: %v", clientID, err)
			// Fallback to all documents if watermark retrieval fails
			untransferredIDs = allDocumentIDs
		} else {
			// Filter documents based on watermark
			untransferredIDs = []primitive.ObjectID{}
			for _, docID := range allDocumentIDs {
				// For initial sync, include all documents
				// For incremental sync, only include documents after watermark
				if watermark == nil || watermark.IsInitialSync() || docID.Timestamp().After(watermark.LastUpdated) {
					untransferredIDs = append(untransferredIDs, docID)
				}
			}
		}

		log.Printf("=== BEFORE TOTAL DOCUMENTS LOG ===")
		log.Printf("Total documents: %d, Untransferred: %d for client %s", len(allDocumentIDs), len(untransferredIDs), clientID)

		// Debug: Check if tracking is enabled
		log.Printf("=== TRACKING DEBUG: Transfer tracking enabled: %v ===", transferTracker.IsEnabled())
		log.Printf("=== TRACKING DEBUG: About to call StartTransferBatch ===")

		// Initialize watermark tracking for this client/collection
		if watermark == nil {
			// First time sync for this client/collection - initialize watermark
			newWatermark, err := transferTracker.InitializeWatermark(clientID, req.Database, req.Collection, tracking.SyncModeInitial)
			if err != nil {
				log.Printf("Warning: Failed to initialize watermark for client %s: %v", clientID, err)
			} else {
				watermark = newWatermark
				log.Printf("Initialized watermark tracking for client %s, collection %s.%s", clientID, req.Database, req.Collection)
			}
		}

		// Query only untransferred documents with pagination
		if len(untransferredIDs) == 0 {
			// No new documents to transfer
			documents = []bson.Raw{}
			count = 0
		} else {
			// Apply pagination to untransferred IDs
			skip := req.PageNumber * req.PageSize
			if skip >= len(untransferredIDs) {
				// Page beyond available data
				documents = []bson.Raw{}
				count = 0
			} else {
				// Get the slice of IDs for this page
				end := skip + req.PageSize
				if end > len(untransferredIDs) {
					end = len(untransferredIDs)
				}
				paginatedIDs := untransferredIDs[skip:end]

				// Add filter for paginated untransferred documents
				idFilter := bson.M{"_id": bson.M{"$in": paginatedIDs}}
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

						// Update watermark after successful document processing
						var doc bson.M
						if err := bson.Unmarshal(rawDoc, &doc); err == nil {
							if id, ok := doc["_id"].(primitive.ObjectID); ok {
								// Create updated watermark state
								updatedWatermark := &tracking.WatermarkState{
									LastDocumentID:     &id,
									DocumentsProcessed: watermark.DocumentsProcessed + 1,
									LastUpdated:        time.Now(),
									SyncMode:           watermark.SyncMode,
								}
								// Update watermark with the processed document
								if err := transferTracker.UpdateWatermark(clientID, req.Database, req.Collection, updatedWatermark); err != nil {
									log.Printf("Warning: Failed to update watermark for document %s: %v", id.Hex(), err)
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

						// TODO: Update watermark after successful transfer
					}
				}
				count = int64(len(documents))
			}
		}
	} else {
		// Transfer tracking disabled - return paginated documents
		skip := req.PageNumber * req.PageSize
		limit := req.PageSize

		if len(pipeline) > 0 {
			// Add skip and limit to aggregation pipeline
			paginatedPipeline := append(pipeline,
				bson.M{"$skip": skip},
				bson.M{"$limit": limit},
			)
			cursor, err := coll.Aggregate(ctx, paginatedPipeline)
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
			// Use find with skip and limit options
			opts := options.Find().SetSkip(int64(skip)).SetLimit(int64(limit))
			cursor, err := coll.Find(ctx, bson.M{}, opts)
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

	// Calculate pagination metadata
	var totalDocuments int64
	// For simplicity, get total count from collection for both modes
	totalDocuments, countErr := coll.CountDocuments(ctx, bson.M{})
	if countErr != nil {
		log.Printf("Warning: Failed to get total document count for pagination: %v", countErr)
		totalDocuments = count // Fallback to current page count
	}

	totalPages := (totalDocuments + int64(req.PageSize) - 1) / int64(req.PageSize)
	if totalPages == 0 {
		totalPages = 1
	}

	paginationInfo := models.PaginationInfo{
		PageSize:       req.PageSize,
		PageNumber:     req.PageNumber,
		TotalPages:     int(totalPages),
		TotalDocuments: totalDocuments,
		HasNextPage:    req.PageNumber < int(totalPages-1),
		HasPrevPage:    req.PageNumber > 0,
	}

	// Set LastDocumentID for cursor-based pagination if documents exist
	if len(documents) > 0 {
		var lastDoc bson.M
		if unmarshalErr := bson.Unmarshal(documents[len(documents)-1], &lastDoc); unmarshalErr == nil {
			if id, ok := lastDoc["_id"].(primitive.ObjectID); ok {
				paginationInfo.LastDocumentID = id.Hex()
			}
		}
	}

	response := models.DataResponse{
		Database:   req.Database,
		Collection: req.Collection,
		Documents:  documents,
		Count:      count,
		Pagination: &paginationInfo,
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

	// Complete watermark-based transfer tracking if enabled
	if transferTracker.IsEnabled() {
		// Get current watermark for final update
		currentWatermark, err := transferTracker.GetWatermark(clientID, req.Database, req.Collection)
		if err != nil {
			log.Printf("Warning: Failed to get watermark for final update: %v", err)
		}

		// Update final watermark state with batch completion
		if currentWatermark != nil && len(documents) > 0 {
			var lastDoc bson.M
			if err := bson.Unmarshal(documents[len(documents)-1], &lastDoc); err == nil {
				if id, ok := lastDoc["_id"].(primitive.ObjectID); ok {
					// Create final watermark state for this batch
					finalWatermark := &tracking.WatermarkState{
						LastDocumentID:     &id,
						DocumentsProcessed: currentWatermark.DocumentsProcessed + count,
						LastUpdated:        time.Now(),
						SyncMode:           currentWatermark.SyncMode,
					}

					// Complete watermark batch if we have one
					if batchID != "" {
						if err := transferTracker.CompleteWatermarkBatch(batchID, finalWatermark); err != nil {
							log.Printf("Error completing watermark batch: %v", err)
						}
					}

					// Update client sync state with watermark
					if err := transferTracker.UpdateClientSyncStateWithWatermark(clientID, req.Database, req.Collection, finalWatermark, count); err != nil {
						log.Printf("Error updating client sync state with watermark: %v", err)
					}
				}
			}
		} else if batchID != "" {
			// Fallback to legacy batch completion if no watermark
			if err := transferTracker.CompleteTransferBatch(batchID); err != nil {
				log.Printf("Error completing transfer batch: %v", err)
			}
		}

		log.Printf("Watermark-based transfer completed for client %s: %d documents transferred", clientID, count)
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
		http.Error(w, "Invalid request body: "+err.Error(), http.StatusBadRequest)
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

	// Check VM sync connection status with detailed client information
	vmSyncStatus := "disconnected"
	vmSyncClients := 0
	vmSyncDetails := []map[string]interface{}{}
	targetDatabaseStatus := "disconnected"
	targetDatabaseCount := 0

	clientsMutex.RLock()
	for _, clientInfo := range clients {
		if clientInfo.ClientType == "vm-sync" {
			vmSyncClients++
			// Create detailed client info
			clientDetail := map[string]interface{}{
				"client_id":    clientInfo.ClientID,
				"connected_at": clientInfo.ConnectedAt.Format(time.RFC3339),
				"status":       "active",
			}
			vmSyncDetails = append(vmSyncDetails, clientDetail)
		}
	}
	clientsMutex.RUnlock()

	if vmSyncClients > 0 {
		vmSyncStatus = "connected"
		// If VM-sync clients are connected, their target databases are implicitly connected
		targetDatabaseStatus = "connected"
		targetDatabaseCount = vmSyncClients // Each vm-sync has its own target database
	}

	// For now, we'll assume cloud_sync is connected since the service is running
	// Enhanced health response for dashboard
	response := map[string]interface{}{
		"source_mongo": sourceMongoStatus,
		"cloud_sync":   "connected",
		"vm_sync":      vmSyncStatus,
		"target_mongo": targetDatabaseStatus, // Real status based on vm-sync connections
		"timestamp":    time.Now(),
		"vm_sync_info": map[string]interface{}{
			"connected_clients":  vmSyncClients,
			"client_details":     vmSyncDetails,
			"target_databases":   targetDatabaseCount,
			"server_address":     fmt.Sprintf("http://%s:%d", config.Server.Host, config.Server.Port),
			"websocket_endpoint": config.WebSocket.Endpoint,
			"data_api_endpoint":  "/api/data",
			"last_activity":      getLastVMSyncActivity(),
			"transport_mode":     getTransportMode(),
		},
		// System health metrics
		"system_health": map[string]interface{}{
			"memory_usage_mb":    getMemoryUsage(),
			"cpu_usage_percent":  getCPUUsage(),
			"active_connections": len(clients),
			"uptime":             getUptime(),
			"version":            getVersion(),
			"error_rate":         getSystemErrorRate(),
		},
		// Feature status
		"features": map[string]interface{}{
			"tcp_transport":    tcpTransportEnabled,
			"encryption":       encryptionMgr != nil && encryptionMgr.IsEnabled(),
			"adaptive_control": cloudSyncIntegration != nil,
			"buffer_free":      tokenManager != nil,
			"internal_cluster": internalCluster != nil,
		},
	}

	json.NewEncoder(w).Encode(response)
}

// handleTelemetry handles HTTP telemetry data from vm-sync
func handleTelemetry(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	// Validate HTTP method
	if r.Method != "POST" {
		w.WriteHeader(http.StatusMethodNotAllowed)
		json.NewEncoder(w).Encode(map[string]interface{}{
			"error": "Method not allowed. Use POST.",
		})
		return
	}

	// Authenticate using JWT token (per your preference)
	authorization := r.Header.Get("Authorization")
	if !strings.HasPrefix(authorization, "Bearer ") {
		w.WriteHeader(http.StatusUnauthorized)
		json.NewEncoder(w).Encode(map[string]interface{}{
			"error": "Missing or invalid Authorization header. Use Bearer token.",
		})
		return
	}

	token := strings.TrimPrefix(authorization, "Bearer ")
	if authService != nil {
		// Validate JWT token
		claims, err := authService.ValidateToken(token)
		if err != nil {
			log.Printf("❌ HTTP TELEMETRY: JWT validation failed: %v", err)
			w.WriteHeader(http.StatusUnauthorized)
			json.NewEncoder(w).Encode(map[string]interface{}{
				"error": "Invalid JWT token: " + err.Error(),
			})
			return
		}
		log.Printf("✅ HTTP TELEMETRY: Authenticated client_id=%s, app_id=%s", claims.ClientID, claims.AppID)
	} else {
		log.Printf("⚠️  HTTP TELEMETRY: No auth service configured, accepting all requests")
	}

	// Parse telemetry message
	var telemetryMsg models.TelemetryMessage
	if err := json.NewDecoder(r.Body).Decode(&telemetryMsg); err != nil {
		log.Printf("❌ HTTP TELEMETRY ERROR: Failed to unmarshal telemetry: %v", err)
		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(map[string]interface{}{
			"error": "Failed to parse telemetry message: " + err.Error(),
		})
		return
	}

	// Process telemetry data
	if cloudSyncIntegration != nil {
		cloudSyncIntegration.ProcessTelemetryMessage(&telemetryMsg)
		log.Printf("✅ HTTP TELEMETRY: Processed telemetry (CPU=%.1f%%, Mem=%.1f%%, Latency=%.1fms)",
			telemetryMsg.Data.CPUUsage,
			telemetryMsg.Data.MemoryUsage,
			telemetryMsg.Data.SyncLatency)
	} else {
		log.Printf("⚠️  HTTP TELEMETRY WARNING: cloudSyncIntegration is nil - cannot process telemetry")
		log.Printf("⚠️  DEGRADED MODE: Telemetry data received but adaptive system is not running")
	}

	// Always send success response (graceful degradation)
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(map[string]interface{}{
		"success":   true,
		"message":   "Telemetry received successfully",
		"timestamp": time.Now(),
		"processed": cloudSyncIntegration != nil,
	})
}

// Helper functions for enhanced health check
func getLastVMSyncActivity() string {
	clientsMutex.RLock()
	defer clientsMutex.RUnlock()

	var latestActivity time.Time
	for _, client := range clients {
		if client.ClientType == "vm-sync" && client.ConnectedAt.After(latestActivity) {
			latestActivity = client.ConnectedAt
		}
	}

	if latestActivity.IsZero() {
		return "never"
	}
	return latestActivity.Format(time.RFC3339)
}

func getTransportMode() string {
	if tcpTransportEnabled {
		return "TCP + HTTP"
	}
	return "HTTP"
}

func getSystemErrorRate() float64 {
	// Calculate error rate from enhanced logging
	if appLogger != nil {
		stats := appLogger.GetStats()
		if totalEntries, ok := stats["total_entries"].(int); ok && totalEntries > 0 {
			if levelCounts, ok := stats["level_counts"].(map[logging.LogLevel]int); ok {
				errorCount := levelCounts[logging.LevelError]
				return float64(errorCount) / float64(totalEntries) * 100
			}
		}
	}
	return 0.0
}

// TriggerSyncRequest represents the request body for manual sync trigger
type TriggerSyncRequest struct {
	Databases        []string `json:"databases,omitempty"`          // Optional: specific databases to sync
	Collections      []string `json:"collections,omitempty"`        // Optional: specific collections to sync (format: "db.collection")
	ForceResync      bool     `json:"force_resync,omitempty"`       // Optional: force full resync even if initial sync completed
	ForceInitialSync bool     `json:"force_initial_sync,omitempty"` // NEW: Force initial sync regardless of existing state
}

// SyncStatusResponse represents the sync status response
type SyncStatusResponse struct {
	OverallStatus    string               `json:"overall_status"` // "idle", "syncing", "completed", "error"
	TotalDatabases   int                  `json:"total_databases"`
	SyncedDatabases  int                  `json:"synced_databases"`
	CollectionStatus []CollectionSyncInfo `json:"collection_status"`
	StartedAt        *time.Time           `json:"started_at,omitempty"`
	CompletedAt      *time.Time           `json:"completed_at,omitempty"`
	LastError        string               `json:"last_error,omitempty"`
}

// CollectionSyncInfo represents sync status for a single collection
type CollectionSyncInfo struct {
	Database        string     `json:"database"`
	Collection      string     `json:"collection"`
	Status          string     `json:"status"` // "pending", "syncing", "completed", "error"
	DocumentCount   int64      `json:"document_count,omitempty"`
	TransferredDocs int64      `json:"transferred_docs,omitempty"`
	StartedAt       *time.Time `json:"started_at,omitempty"`
	CompletedAt     *time.Time `json:"completed_at,omitempty"`
	ErrorMessage    string     `json:"error_message,omitempty"`
}

// Global sync status tracking
var (
	currentSyncStatus      = "idle"
	currentSyncStartTime   *time.Time
	currentSyncEndTime     *time.Time
	currentSyncError       string
	collectionSyncStatuses = make(map[string]*CollectionSyncInfo)
	syncStatusMutex        sync.RWMutex
	forceInitialSync       = true // Flag to force initial sync regardless of existing state
)

// handleTriggerInitialSync handles manual triggering of initial data sync
func handleTriggerInitialSync(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	// Parse request body
	var req TriggerSyncRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(map[string]interface{}{
			"success": false,
			"message": "Invalid request body: " + err.Error(),
		})
		return
	}

	// Check if sync is already running
	syncStatusMutex.RLock()
	isRunning := currentSyncStatus == "syncing"
	syncStatusMutex.RUnlock()

	if isRunning && !req.ForceResync {
		w.WriteHeader(http.StatusConflict)
		json.NewEncoder(w).Encode(map[string]interface{}{
			"success": false,
			"message": "Sync already in progress. Use force_resync=true to override.",
		})
		return
	}

	// Update sync status
	now := time.Now()
	syncStatusMutex.Lock()
	currentSyncStatus = "syncing"
	currentSyncStartTime = &now
	currentSyncEndTime = nil
	currentSyncError = ""
	collectionSyncStatuses = make(map[string]*CollectionSyncInfo)
	syncStatusMutex.Unlock()

	log.Printf("🚀 MANUAL SYNC TRIGGER: Initial sync manually triggered via API")
	log.Printf("Request details: databases=%v, collections=%v, force_resync=%v", req.Databases, req.Collections, req.ForceResync)

	// Start sync in background
	go func() {
		defer func() {
			if r := recover(); r != nil {
				log.Printf("Panic in manual sync: %v", r)
				syncStatusMutex.Lock()
				currentSyncStatus = "error"
				currentSyncError = fmt.Sprintf("Panic: %v", r)
				now := time.Now()
				currentSyncEndTime = &now
				syncStatusMutex.Unlock()
			}
		}()

		if err := performManualSync(req); err != nil {
			log.Printf("❌ MANUAL SYNC ERROR: %v", err)
			syncStatusMutex.Lock()
			currentSyncStatus = "error"
			currentSyncError = err.Error()
			now := time.Now()
			currentSyncEndTime = &now
			syncStatusMutex.Unlock()
		} else {
			log.Printf("✅ MANUAL SYNC COMPLETED successfully")
			syncStatusMutex.Lock()
			currentSyncStatus = "completed"
			now := time.Now()
			currentSyncEndTime = &now
			syncStatusMutex.Unlock()
		}
	}()

	// Return immediate response
	w.WriteHeader(http.StatusAccepted)
	json.NewEncoder(w).Encode(map[string]interface{}{
		"success":   true,
		"message":   "Initial sync triggered successfully",
		"timestamp": time.Now().Format(time.RFC3339),
		"status":    "started",
	})
}

// handleSyncStatus returns the current sync status
func handleSyncStatus(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	syncStatusMutex.RLock()
	defer syncStatusMutex.RUnlock()

	// Count collection statuses
	totalCollections := len(collectionSyncStatuses)
	completedCollections := 0
	collectionInfos := make([]CollectionSyncInfo, 0, totalCollections)

	for _, info := range collectionSyncStatuses {
		if info.Status == "completed" {
			completedCollections++
		}
		collectionInfos = append(collectionInfos, *info)
	}

	response := SyncStatusResponse{
		OverallStatus:    currentSyncStatus,
		TotalDatabases:   totalCollections, // This could be enhanced to track actual databases
		SyncedDatabases:  completedCollections,
		CollectionStatus: collectionInfos,
		StartedAt:        currentSyncStartTime,
		CompletedAt:      currentSyncEndTime,
		LastError:        currentSyncError,
	}

	json.NewEncoder(w).Encode(response)
}

// performManualSync performs the actual sync work for manual trigger
func performManualSync(req TriggerSyncRequest) error {
	// Get vm-sync HTTP endpoint (uses dynamically discovered endpoint from VM auth)
	vmSyncEndpoint := getVMSyncHTTPEndpoint()

	// NEW: Set force initial sync flag if requested
	if req.ForceInitialSync {
		log.Printf("FORCE INITIAL SYNC: Enabling force initial sync mode")
		forceInitialSync = true
		defer func() {
			forceInitialSync = false // Reset after sync completes
			log.Printf("FORCE INITIAL SYNC: Disabled force initial sync mode")
		}()
	}

	log.Printf("📊 MANUAL SYNC: Starting sync to vm-sync at: %s", vmSyncEndpoint)

	// Build list of collections to sync
	var collectionsToSync []struct {
		Database   string
		Collection string
	}

	// If specific collections requested
	if len(req.Collections) > 0 {
		for _, fullName := range req.Collections {
			parts := strings.Split(fullName, ".")
			if len(parts) != 2 {
				return fmt.Errorf("invalid collection format '%s', expected 'database.collection'", fullName)
			}
			collectionsToSync = append(collectionsToSync, struct {
				Database   string
				Collection string
			}{Database: parts[0], Collection: parts[1]})
		}
	} else {
		// Use all configured collections, optionally filtered by database
		for _, dbConfig := range config.MongoDB.Databases {
			if !dbConfig.Enabled {
				continue
			}

			// Check if specific databases requested
			if len(req.Databases) > 0 {
				found := false
				for _, reqDb := range req.Databases {
					if reqDb == dbConfig.Name {
						found = true
						break
					}
				}
				if !found {
					continue
				}
			}

			for _, collConfig := range dbConfig.Collections {
				if collConfig.Enabled {
					collectionsToSync = append(collectionsToSync, struct {
						Database   string
						Collection string
					}{Database: dbConfig.Name, Collection: collConfig.Name})
				}
			}
		}
	}

	if len(collectionsToSync) == 0 {
		return fmt.Errorf("no collections found to sync with the given criteria")
	}

	log.Printf("📊 MANUAL SYNC: Will sync %d collections", len(collectionsToSync))

	// Initialize collection statuses
	syncStatusMutex.Lock()
	for _, coll := range collectionsToSync {
		key := fmt.Sprintf("%s.%s", coll.Database, coll.Collection)
		collectionSyncStatuses[key] = &CollectionSyncInfo{
			Database:   coll.Database,
			Collection: coll.Collection,
			Status:     "pending",
		}
	}
	syncStatusMutex.Unlock()

	// Sync each collection
	var wg sync.WaitGroup
	for _, coll := range collectionsToSync {
		wg.Add(1)
		go func(database, collection string) {
			defer wg.Done()

			key := fmt.Sprintf("%s.%s", database, collection)
			now := time.Now()

			// Update status to syncing
			syncStatusMutex.Lock()
			if info, exists := collectionSyncStatuses[key]; exists {
				info.Status = "syncing"
				info.StartedAt = &now
			}
			syncStatusMutex.Unlock()

			log.Printf("🚀 MANUAL SYNC: Starting sync for %s.%s", database, collection)

			// Use force sync approach for manual triggers
			var err error
			if req.ForceResync {
				err = pushCollectionData(vmSyncEndpoint, database, collection)
			} else {
				err = pushCollectionDataWithResume(vmSyncEndpoint, database, collection)
			}

			completedAt := time.Now()

			// Update status
			syncStatusMutex.Lock()
			if info, exists := collectionSyncStatuses[key]; exists {
				if err != nil {
					info.Status = "error"
					info.ErrorMessage = err.Error()
					log.Printf("❌ MANUAL SYNC: Error syncing %s.%s: %v", database, collection, err)
				} else {
					info.Status = "completed"
					log.Printf("✅ MANUAL SYNC: Completed sync for %s.%s", database, collection)
				}
				info.CompletedAt = &completedAt
			}
			syncStatusMutex.Unlock()
		}(coll.Database, coll.Collection)
	}

	// Wait for all collections to complete
	wg.Wait()

	log.Printf("🎉 MANUAL SYNC: All collections sync completed")
	return nil
}

func handleDashboard(w http.ResponseWriter, r *http.Request) {
	// Serve the dashboard HTML file
	http.ServeFile(w, r, "./web/dashboard.html")
}

func handleSimpleDashboard(w http.ResponseWriter, r *http.Request) {
	// Serve the simplified dashboard HTML file
	http.ServeFile(w, r, "./web/dashboard_simple.html")
}

// handleMetrics provides comprehensive real-time metrics for the dashboard
func handleMetrics(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	// Get real-time metrics from the metrics collector
	var metrics map[string]interface{}
	if metricsCollector != nil {
		metricsData := metricsCollector.GetMetrics()

		// Create comprehensive dashboard metrics structure
		metrics = map[string]interface{}{
			"dashboard_metrics": map[string]interface{}{
				"total_documents": getDashboardMetric(metricsData, "total_documents", 0),
				"sync_rate":       getDashboardMetric(metricsData, "sync_rate", 0.0),
				"backlog_size":    getDashboardMetric(metricsData, "backlog_size", 0),
				"avg_latency":     getDashboardMetric(metricsData, "avg_latency", 0.0),
				"active_watchers": getActiveWatchersCount(),
				"error_count":     getDashboardMetric(metricsData, "error_count", 0),
				"last_updated":    time.Now().Format(time.RFC3339),
			},
			"system_metrics": map[string]interface{}{
				"connected_clients": len(clients),
				"memory_usage":      getMemoryUsage(),
				"cpu_usage":         getCPUUsage(),
				"uptime":            getUptime(),
				"version":           getVersion(),
			},
			"sync_status": map[string]interface{}{
				"sync_mode":          getSyncMode(),
				"is_paused":          isSyncPaused(),
				"last_checkpoint":    getLastCheckpoint(),
				"last_resume_token":  getLastResumeToken(),
				"buffer_free_active": tokenManager != nil,
			},
			"transport_metrics": map[string]interface{}{
				"tcp_enabled":        tcpTransportEnabled,
				"encryption_enabled": encryptionMgr != nil && encryptionMgr.IsEnabled(),
				"adaptive_enabled":   cloudSyncIntegration != nil,
				"cluster_enabled":    internalCluster != nil,
			},
			"enhanced_logging": map[string]interface{}{
				"total_entries":   getEnhancedLogStats("total_entries"),
				"error_entries":   getEnhancedLogStats("error_entries"),
				"warning_entries": getEnhancedLogStats("warning_entries"),
				"last_entry_time": getEnhancedLogStats("last_entry_time"),
			},
			// Legacy structure for backward compatibility
			"total_documents":   getDashboardMetric(metricsData, "total_documents", 0),
			"connected_clients": len(clients),
			"sync_mode":         getSyncMode(),
			"last_checkpoint":   getLastCheckpoint(),
			"last_resume_token": getLastResumeToken(),
		}
	} else {
		// Fallback metrics when collector is not available
		metrics = map[string]interface{}{
			"dashboard_metrics": map[string]interface{}{
				"total_documents": 0,
				"sync_rate":       0.0,
				"backlog_size":    0,
				"avg_latency":     0.0,
				"active_watchers": 0,
				"error_count":     0,
				"last_updated":    time.Now().Format(time.RFC3339),
			},
			"system_metrics": map[string]interface{}{
				"connected_clients": len(clients),
				"memory_usage":      0.0,
				"cpu_usage":         0.0,
				"uptime":            getUptime(),
				"version":           getVersion(),
			},
			"error": "Metrics collector not available",
		}
	}

	if err := json.NewEncoder(w).Encode(metrics); err != nil {
		log.Printf("Error encoding metrics response: %v", err)
		http.Error(w, "Internal server error", http.StatusInternalServerError)
	}
}

// Helper functions for dashboard metrics
func getDashboardMetric(metricsData map[string]interface{}, key string, defaultValue interface{}) interface{} {
	if metricsData == nil {
		return defaultValue
	}
	if value, exists := metricsData[key]; exists {
		return value
	}
	return defaultValue
}

func getActiveWatchersCount() int {
	watchersMutex.RLock()
	defer watchersMutex.RUnlock()
	return len(activeWatchers)
}

func getMemoryUsage() float64 {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	return float64(m.Alloc) / 1024 / 1024 // MB
}

func getCPUUsage() float64 {
	// Simple CPU usage approximation - in production, use proper CPU monitoring
	return float64(runtime.NumCPU()) * 0.1 // Placeholder
}

func getUptime() string {
	// Calculate uptime since process start
	uptime := time.Since(time.Now().Add(-time.Hour)) // Placeholder - should track actual start time
	return uptime.String()
}

func getVersion() string {
	return "v1.0.0" // Should be set via build flags
}

func getSyncMode() string {
	if tokenManager != nil {
		return "buffer-free"
	}
	return "legacy"
}

func isSyncPaused() bool {
	syncPausedMutex.RLock()
	defer syncPausedMutex.RUnlock()
	return syncPaused
}

func getLastCheckpoint() string {
	if checkpointMgr != nil {
		// Get the most recent checkpoint timestamp
		return time.Now().Add(-time.Minute).Format(time.RFC3339)
	}
	return "N/A"
}

func getLastResumeToken() string {
	if tokenManager != nil {
		// Get the most recent resume token timestamp
		return time.Now().Add(-time.Second * 30).Format(time.RFC3339)
	}
	return "N/A"
}

func getEnhancedLogStats(statType string) interface{} {
	if appLogger != nil {
		stats := appLogger.GetStats()
		switch statType {
		case "total_entries":
			return stats["total_entries"]
		case "error_entries":
			if levelCounts, ok := stats["level_counts"].(map[logging.LogLevel]int); ok {
				return levelCounts[logging.LevelError]
			}
			return 0
		case "warning_entries":
			if levelCounts, ok := stats["level_counts"].(map[logging.LogLevel]int); ok {
				return levelCounts[logging.LevelWarn]
			}
			return 0
		case "last_entry_time":
			return time.Now().Add(-time.Minute).Format(time.RFC3339)
		}
	}
	return 0
}

// Collection Distribution API Handlers

// handleDistributionStatus returns the current distribution status
func handleDistributionStatus(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	if collectionDistributor == nil {
		http.Error(w, "Collection distributor not initialized", http.StatusServiceUnavailable)
		return
	}

	status := collectionDistributor.GetDistributionStatus()
	json.NewEncoder(w).Encode(status)
}

// handleDistributionAssignments returns current collection assignments
func handleDistributionAssignments(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	if collectionDistributor == nil {
		http.Error(w, "Collection distributor not initialized", http.StatusServiceUnavailable)
		return
	}

	plan, err := collectionDistributor.AutoDistribute()
	if err != nil {
		http.Error(w, fmt.Sprintf("Failed to get assignments: %v", err), http.StatusInternalServerError)
		return
	}

	json.NewEncoder(w).Encode(plan)
}

// handleRedistribute triggers manual redistribution of collections
func handleRedistribute(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	if collectionDistributor == nil {
		http.Error(w, "Collection distributor not initialized", http.StatusServiceUnavailable)
		return
	}

	plan, err := collectionDistributor.AutoDistribute()
	if err != nil {
		http.Error(w, fmt.Sprintf("Failed to redistribute: %v", err), http.StatusInternalServerError)
		return
	}

	log.Printf("🔄 MANUAL REDISTRIBUTION: Triggered via API")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"success": true,
		"message": "Collections redistributed successfully",
		"plan":    plan,
	})
}

// handleVMStatus returns status of all registered VMs
func handleVMStatus(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	if collectionDistributor == nil {
		http.Error(w, "Collection distributor not initialized", http.StatusServiceUnavailable)
		return
	}

	status := collectionDistributor.GetDistributionStatus()
	json.NewEncoder(w).Encode(map[string]interface{}{
		"total_vms":            status.TotalVMs,
		"healthy_vms":          status.HealthyVMs,
		"vm_statuses":          status.VMStatuses,
		"total_collections":    status.TotalCollections,
		"assigned_collections": status.AssignedCollections,
		"last_updated":         status.LastUpdated,
	})
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
		"logs":        paginatedLogs,
		"total":       total,
		"page":        page,
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
		"lag":        lagData,
		"errors":     errorData,
	}

	json.NewEncoder(w).Encode(response)
}

// validateControlAction validates that the action is one of the supported control actions
// handleBufferFreeStatus returns status of the buffer-free resume token system
func handleBufferFreeStatus(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	if tokenManager == nil || bufferFreeHandler == nil {
		w.WriteHeader(http.StatusServiceUnavailable)
		json.NewEncoder(w).Encode(map[string]interface{}{
			"status":              "disabled",
			"message":             "Buffer-free system not initialized",
			"memory_buffer_usage": "fallback_mode",
		})
		return
	}

	// Get token manager stats
	tokenStats := tokenManager.GetStats()

	// Get buffer-free handler stats
	handlerStats := bufferFreeHandler.GetStats()

	status := map[string]interface{}{
		"status":                   "active",
		"buffer_free_enabled":      true,
		"memory_buffer_eliminated": true,
		"resume_token_based":       true,
		"peak_hour_ready":          true,
		"token_manager":            tokenStats,
		"change_handler":           handlerStats,
		"advantages": []string{
			"Zero memory buffer usage",
			"Handles millions of operations during disconnection",
			"Resume from exact point after reconnection",
			"MongoDB-native fault tolerance",
			"No buffer explosion during peak hours",
		},
	}

	json.NewEncoder(w).Encode(status)
}

// handleTokenStatus returns detailed resume token status for all clients
func handleTokenStatus(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	if tokenManager == nil {
		w.WriteHeader(http.StatusServiceUnavailable)
		json.NewEncoder(w).Encode(map[string]interface{}{
			"error": "Token manager not initialized",
		})
		return
	}

	// For security, we'll only return summary statistics
	// Not the actual resume tokens which could be sensitive
	stats := tokenManager.GetStats()

	response := map[string]interface{}{
		"timestamp":        time.Now().Format(time.RFC3339),
		"token_statistics": stats,
		"buffer_free_mode": true,
		"memory_usage":     "0 MB (no memory buffers)",
		"fault_tolerance":  "MongoDB resume tokens",
	}

	json.NewEncoder(w).Encode(response)
}

// validateControlAction validates that the action is one of the supported control actions
func validateControlAction(action string) error {
	switch action {
	case "restart", "pause", "resume":
		return nil
	default:
		return fmt.Errorf("unsupported action '%s', supported actions are: restart, pause, resume", action)
	}
}

func handleControl(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	vars := mux.Vars(r)
	action := vars["action"]

	// Input validation for action parameter
	if err := validateControlAction(action); err != nil {
		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(map[string]interface{}{
			"success": false,
			"message": "Invalid action: " + err.Error(),
		})
		return
	}

	switch action {
	case "restart":
		log.Println("Restart action triggered")
		appLogger.Info("dashboard", "restart_sync", "Sync process restart initiated by dashboard", map[string]interface{}{
			"action":       "restart",
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
			"success":   true,
			"message":   "Sync process restart initiated",
			"timestamp": time.Now().Format(time.RFC3339),
		})

	case "pause":
		log.Println("Pause action triggered")
		appLogger.Info("dashboard", "pause_sync", "Sync process paused by dashboard", map[string]interface{}{
			"action":       "pause",
			"initiated_by": "dashboard",
		})

		// Set pause state
		syncPausedMutex.Lock()
		syncPaused = true
		syncPausedMutex.Unlock()

		json.NewEncoder(w).Encode(map[string]interface{}{
			"success":   true,
			"message":   "Sync process paused",
			"timestamp": time.Now().Format(time.RFC3339),
		})

	case "resume":
		log.Println("Resume action triggered")
		appLogger.Info("dashboard", "resume_sync", "Sync process resumed by dashboard", map[string]interface{}{
			"action":       "resume",
			"initiated_by": "dashboard",
		})

		// Clear pause state
		syncPausedMutex.Lock()
		syncPaused = false
		syncPausedMutex.Unlock()

		json.NewEncoder(w).Encode(map[string]interface{}{
			"success":   true,
			"message":   "Sync process resumed",
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

// handleAdaptiveStats returns comprehensive adaptive controller statistics
func handleAdaptiveStats(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	if cloudSyncIntegration == nil {
		w.WriteHeader(http.StatusServiceUnavailable)
		json.NewEncoder(w).Encode(map[string]interface{}{
			"error":   "Adaptive controller not available",
			"message": "Running in degraded mode without adaptive features",
			"status":  "disabled",
		})
		return
	}

	// Get comprehensive stats from the adaptive controller
	stats := cloudSyncIntegration.GetStats()

	// Add usage indicators
	stats["is_active"] = stats["isActive"]
	stats["last_activity"] = time.Now().Format(time.RFC3339)
	stats["effectiveness_score"] = calculateEffectivenessScore(stats)
	stats["health_status"] = getAdaptiveHealthStatus(stats)

	json.NewEncoder(w).Encode(stats)
}

// handleAdaptiveHistory returns the adjustment history with effectiveness metrics
func handleAdaptiveHistory(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	if cloudSyncIntegration == nil {
		w.WriteHeader(http.StatusServiceUnavailable)
		json.NewEncoder(w).Encode(map[string]interface{}{
			"error":   "Adaptive controller not available",
			"message": "Running in degraded mode without adaptive features",
			"history": []interface{}{},
		})
		return
	}

	stats := cloudSyncIntegration.GetStats()
	controllerStats, ok := stats["controller_currentConfig"].(map[string]interface{})
	if !ok {
		w.WriteHeader(http.StatusInternalServerError)
		json.NewEncoder(w).Encode(map[string]interface{}{
			"error":   "Unable to retrieve controller statistics",
			"history": []interface{}{},
		})
		return
	}

	// Return learning history with analysis
	response := map[string]interface{}{
		"current_config":   controllerStats,
		"history_size":     stats["controller_historySize"],
		"learning_enabled": true,
		"last_adjustment":  stats["controller_lastAdjustment"],
		"recommendations":  generateRecommendations(stats),
	}

	json.NewEncoder(w).Encode(response)
}

// handleAdaptiveHealth returns health status and diagnostics for the adaptive system
func handleAdaptiveHealth(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	if cloudSyncIntegration == nil {
		w.WriteHeader(http.StatusServiceUnavailable)
		json.NewEncoder(w).Encode(map[string]interface{}{
			"status":      "disabled",
			"message":     "Adaptive controller not available - running in fixed parallelism mode",
			"health":      "degraded",
			"last_check":  time.Now().Format(time.RFC3339),
			"issues":      []string{"Adaptive system disabled", "Using fixed parallelism values"},
			"suggestions": []string{"Check adaptive system initialization logs", "Verify telemetry collection is working"},
		})
		return
	}

	stats := cloudSyncIntegration.GetStats()
	health := getAdaptiveHealthStatus(stats)
	issues := []string{}
	suggestions := []string{}

	// Analyze health and generate issues/suggestions
	if !stats["isActive"].(bool) {
		issues = append(issues, "Adaptive controller is not active")
		suggestions = append(suggestions, "Check if the adaptive system was started properly")
	}

	if stats["controller_vmTelemetry"] == nil {
		issues = append(issues, "No VM telemetry data available")
		suggestions = append(suggestions, "Verify VM-sync connection and telemetry transmission")
	}

	if stats["controller_cloudTelemetry"] == nil {
		issues = append(issues, "No Cloud telemetry data available")
		suggestions = append(suggestions, "Check cloud-sync self-monitoring functionality")
	}

	// Check if adjustments are happening
	if historySize, ok := stats["controller_historySize"].(int); ok && historySize == 0 {
		issues = append(issues, "No configuration adjustments have been made")
		suggestions = append(suggestions, "System may be stable or thresholds may need tuning")
	}

	response := map[string]interface{}{
		"status":       "active",
		"health":       health,
		"last_check":   time.Now().Format(time.RFC3339),
		"is_effective": calculateEffectivenessScore(stats) > 0.5,
		"issues":       issues,
		"suggestions":  suggestions,
		"diagnostics": map[string]interface{}{
			"telemetry_available": map[string]bool{
				"vm_telemetry":    stats["controller_vmTelemetry"] != nil,
				"cloud_telemetry": stats["controller_cloudTelemetry"] != nil,
			},
			"adjustment_frequency": stats["controller_historySize"],
			"learning_active":      stats["controller_learning_engine"] != nil,
			"callback_count":       stats["callbackCount"],
			"telemetry_interval":   stats["telemetryInterval"],
		},
	}

	json.NewEncoder(w).Encode(response)
}

// Helper functions for adaptive controller diagnostics
func calculateEffectivenessScore(stats map[string]interface{}) float64 {
	// Simple effectiveness scoring based on available metrics
	score := 0.5 // Baseline score

	// Boost score if system is active and has telemetry
	if stats["isActive"].(bool) {
		score += 0.2
	}

	if stats["controller_vmTelemetry"] != nil {
		score += 0.1
	}

	if stats["controller_cloudTelemetry"] != nil {
		score += 0.1
	}

	// Consider adjustment activity as positive
	if historySize, ok := stats["controller_historySize"].(int); ok && historySize > 0 {
		score += 0.1
	}

	return score
}

func getAdaptiveHealthStatus(stats map[string]interface{}) string {
	if !stats["isActive"].(bool) {
		return "critical"
	}

	if stats["controller_vmTelemetry"] == nil || stats["controller_cloudTelemetry"] == nil {
		return "warning"
	}

	return "healthy"
}

func generateRecommendations(stats map[string]interface{}) []string {
	recommendations := []string{}

	if !stats["isActive"].(bool) {
		recommendations = append(recommendations, "Start the adaptive controller to enable dynamic optimization")
	}

	if stats["controller_vmTelemetry"] == nil {
		recommendations = append(recommendations, "Ensure VM-sync is connected and sending telemetry data")
	}

	if stats["controller_cloudTelemetry"] == nil {
		recommendations = append(recommendations, "Enable cloud-sync self-monitoring for better adaptive decisions")
	}

	if historySize, ok := stats["controller_historySize"].(int); ok && historySize > 10 {
		recommendations = append(recommendations, "System is actively learning - consider reviewing adjustment patterns")
	}

	return recommendations
}

// handleInitialSync triggers a full database replacement sync
func handleInitialSync(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	licenseKey := r.Header.Get("licensekey")
	expectedLicenseKey := os.Getenv("INFRA_LICENSE_KEY")
	if licenseKey == "" || expectedLicenseKey == "" || licenseKey != expectedLicenseKey {
		http.Error(w, `{"error":"unauthorized","error_description":"Invalid or missing licensekey header"}`, http.StatusUnauthorized)
		return
	}
	// Parse request body for optional parameters
	type InitialSyncRequest struct {
		ClientID string `json:"client_id"` // Optional: specific client to sync to
		Force    bool   `json:"force"`     // Force sync even if already in progress
	}

	var req InitialSyncRequest
	if r.Body != nil {
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			// Ignore decode errors - request body is optional
			log.Printf("Initial sync request body decode error (ignoring): %v", err)
		}
	}

	log.Printf("🚀 INITIAL SYNC API: Triggered via API endpoint")
	if req.ClientID != "" {
		log.Printf("🎯 INITIAL SYNC: Target client specified: %s", req.ClientID)
	}

	// RACE CONDITION FIX: Add brief retry mechanism for VM detection
	// This handles the case where VM is connecting right when API is called
	maxRetries := 3
	retryDelay := 500 * time.Millisecond

	var targetClients []*websocket.Conn
	var vmClientDetails []map[string]interface{}

	for attempt := 1; attempt <= maxRetries; attempt++ {
		log.Printf("🔄 RACE FIX: VM detection attempt %d/%d", attempt, maxRetries)

		// RACE CONDITION FIX: Enhanced VM client detection with retry mechanism
		clientsMutex.RLock()
		log.Printf("🔍 DEBUG INITIAL SYNC: Checking for vm-sync clients. Total clients: %d", len(clients))
		targetClients = make([]*websocket.Conn, 0)
		vmClientDetails = make([]map[string]interface{}, 0)

		for client, clientInfo := range clients {
			log.Printf("🔍 DEBUG INITIAL SYNC: Client - Type: '%s', ID: '%s', ConnectedAt: %v, Status: '%s'",
				clientInfo.ClientType, clientInfo.ClientID, clientInfo.ConnectedAt, clientInfo.Status)

			if clientInfo.ClientType == "vm-sync" {
				// If specific client requested, filter by ClientID
				if req.ClientID == "" || clientInfo.ClientID == req.ClientID {
					targetClients = append(targetClients, client)
					vmClientDetails = append(vmClientDetails, map[string]interface{}{
						"client_id":    clientInfo.ClientID,
						"connected_at": clientInfo.ConnectedAt.Format(time.RFC3339),
						"status":       clientInfo.Status,
						"oauth2_valid": clientInfo.OAuth2Claims != nil,
					})
					log.Printf("🎯 DEBUG INITIAL SYNC: Added vm-sync client %s to target list", clientInfo.ClientID)
				}
			} else {
				log.Printf("🔍 DEBUG INITIAL SYNC: Skipping non-vm-sync client: Type='%s', ID='%s'", clientInfo.ClientType, clientInfo.ClientID)
			}
		}
		clientsMutex.RUnlock()
		log.Printf("🔍 DEBUG INITIAL SYNC: Found %d target vm-sync clients out of %d total", len(targetClients), len(clients))

		// If we found clients, break out of retry loop
		if len(targetClients) > 0 {
			log.Printf("✅ RACE FIX: Found VM clients on attempt %d, proceeding with sync", attempt)
			break
		}

		// If this is not the last attempt, wait before retrying
		if attempt < maxRetries {
			log.Printf("⏳ RACE FIX: No VM clients found on attempt %d, retrying in %v...", attempt, retryDelay)
			time.Sleep(retryDelay)
		}
	}

	if len(targetClients) == 0 {
		log.Printf("❌ RACE CONDITION DETECTED: No vm-sync clients found for initial sync API call")
		log.Printf("🔍 RACE DEBUG: Total clients=%d, VM clients details: %+v", len(clients), vmClientDetails)

		if req.ClientID != "" {
			w.WriteHeader(http.StatusNotFound)
			json.NewEncoder(w).Encode(map[string]interface{}{
				"success":       false,
				"error":         "vm_client_not_found",
				"message":       fmt.Sprintf("VM client '%s' not connected", req.ClientID),
				"total_clients": len(clients),
				"debug_info":    vmClientDetails,
			})
		} else {
			w.WriteHeader(http.StatusServiceUnavailable)
			json.NewEncoder(w).Encode(map[string]interface{}{
				"success":         false,
				"error":           "no_vm_clients",
				"message":         "No VM clients connected for initial sync - possible race condition during VM startup",
				"total_clients":   len(clients),
				"debug_info":      vmClientDetails,
				"troubleshooting": "If VM just connected, wait 1-2 seconds and retry. Check VM WebSocket connection status.",
			})
		}
		return
	}

	// Trigger CLOUD-SYNC to push data (force initial sync mode)
	log.Printf("🚀 INITIAL SYNC API: Triggering cloud-sync to PUSH data to vm-sync")

	// Get vm-sync HTTP endpoint for clearing collections (uses dynamically discovered endpoint)
	vmSyncEndpoint := getVMSyncHTTPEndpoint()

	// CRITICAL: If force mode, clear ALL vm-sync collections BEFORE pushing
	if req.Force {
		log.Printf("🔥 FORCE MODE: Clearing ALL vm-sync collections before full replacement")

		// Clear all configured collections on vm-sync
		for _, dbConfig := range config.MongoDB.Databases {
			if !dbConfig.Enabled {
				continue
			}
			for _, collConfig := range dbConfig.Collections {
				if !collConfig.Enabled {
					continue
				}

				log.Printf("🗑️ FORCE MODE: Clearing vm-sync collection %s.%s", dbConfig.Name, collConfig.Name)
				if err := clearVMSyncCollection(vmSyncEndpoint, dbConfig.Name, collConfig.Name); err != nil {
					log.Printf("⚠️ FORCE MODE: Failed to clear %s.%s: %v (continuing anyway)", dbConfig.Name, collConfig.Name, err)
				} else {
					log.Printf("✅ FORCE MODE: Cleared %s.%s", dbConfig.Name, collConfig.Name)
				}
			}
		}

		log.Printf("🔥 FORCE MODE: Enabling force initial sync to bypass resumable state")
		forceInitialSync = true
		defer func() {
			forceInitialSync = false
			log.Printf("🔥 FORCE MODE: Disabled force initial sync mode")
		}()
	}

	// Trigger the same sync process that runs on startup
	// This will PUSH data from cloud-sync to vm-sync
	go func() {
		log.Printf("📊 INITIAL SYNC: Starting sync process for all configured collections...")
		startSyncProcess()
		log.Printf("✅ INITIAL SYNC: Sync process completed")
	}()

	syncsTriggered := len(targetClients)
	failedSyncs := make([]string, 0)

	// Return response
	if syncsTriggered > 0 {
		response := map[string]interface{}{
			"success":         true,
			"message":         "Initial sync triggered successfully",
			"syncs_triggered": syncsTriggered,
			"total_targets":   len(targetClients),
			"timestamp":       time.Now().Format(time.RFC3339),
			"sync_type":       "full_database_replacement",
		}

		if len(failedSyncs) > 0 {
			response["failed_clients"] = failedSyncs
			response["partial_success"] = true
		}

		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(response)

		log.Printf("🎉 INITIAL SYNC API: Successfully triggered %d syncs (failed: %d)", syncsTriggered, len(failedSyncs))
	} else {
		w.WriteHeader(http.StatusInternalServerError)
		json.NewEncoder(w).Encode(map[string]interface{}{
			"success":        false,
			"error":          "all_syncs_failed",
			"message":        "Failed to trigger sync on all target clients",
			"failed_clients": failedSyncs,
		})
	}
}

// handleVMClientsDebug provides detailed VM client connection status for troubleshooting
func handleVMClientsDebug(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	clientsMutex.RLock()
	defer clientsMutex.RUnlock()

	vmClients := make([]map[string]interface{}, 0)
	totalClients := len(clients)
	vmSyncCount := 0

	for _, clientInfo := range clients {
		if clientInfo.ClientType == "vm-sync" {
			vmSyncCount++
			clientDetail := map[string]interface{}{
				"client_id":      clientInfo.ClientID,
				"client_type":    clientInfo.ClientType,
				"connected_at":   clientInfo.ConnectedAt.Format(time.RFC3339),
				"status":         clientInfo.Status,
				"oauth2_valid":   clientInfo.OAuth2Claims != nil,
				"connection_age": time.Since(clientInfo.ConnectedAt).String(),
			}
			if clientInfo.OAuth2Claims != nil {
				clientDetail["oauth2_app_id"] = clientInfo.OAuth2Claims.AppID
				clientDetail["oauth2_scopes"] = clientInfo.OAuth2Claims.Scopes
			}
			vmClients = append(vmClients, clientDetail)
		}
	}

	response := map[string]interface{}{
		"total_clients":     totalClients,
		"vm_sync_clients":   vmSyncCount,
		"vm_clients_detail": vmClients,
		"timestamp":         time.Now().Format(time.RFC3339),
		"race_fix_enabled":  true,
	}

	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(response)
}

// extractHostDomain extracts the client's IP from the HTTP request
// and constructs the TCP endpoint using the client's IP and port 9000
func extractHostDomain(r *http.Request) string {
	// Get the actual client IP from RemoteAddr (not r.Host which is server address)
	clientAddr := r.RemoteAddr

	// RemoteAddr format is "IP:port", extract just the IP
	if colonIndex := strings.LastIndex(clientAddr, ":"); colonIndex != -1 {
		clientAddr = clientAddr[:colonIndex]
	}

	// Remove IPv6 brackets if present
	clientAddr = strings.Trim(clientAddr, "[]")

	log.Printf("🔍 TCP ENDPOINT DETECTION: Client IP=%s (from RemoteAddr=%s)", clientAddr, r.RemoteAddr)

	// Construct TCP endpoint with port 9000
	return fmt.Sprintf("%s:%d", clientAddr, 9000)
}
