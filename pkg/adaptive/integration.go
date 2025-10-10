package adaptive

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/gorilla/websocket"

	"go-data-sync-http/pkg/auth"
	"go-data-sync-http/pkg/models"
	"go-data-sync-http/pkg/telemetry"
)

// VMSyncIntegration integrates adaptive features into VM Sync
type VMSyncIntegration struct {
	collector         *telemetry.Collector
	transmitter       *telemetry.Transmitter
	httpTransmitter   *telemetry.HTTPTransmitter
	conn              *websocket.Conn
	ctx               context.Context
	cancel            context.CancelFunc
	wg                sync.WaitGroup
	mutex             sync.RWMutex
	isActive          bool
	nodeID            string
	telemetryInterval time.Duration
	useHTTP           bool // Flag to determine which transmitter to use
}

// CloudSyncIntegration integrates adaptive features into Cloud Sync
type CloudSyncIntegration struct {
	controller        *AdaptiveController
	collector         *telemetry.Collector
	ctx               context.Context
	cancel            context.CancelFunc
	wg                sync.WaitGroup
	mutex             sync.RWMutex
	isActive          bool
	nodeID            string
	telemetryInterval time.Duration
	configCallbacks   []ConfigCallback
	lastConfigSent    time.Time
}

// ConfigCallback is called when configuration changes
type ConfigCallback func(*models.AdaptiveConfig) error

// NewVMSyncIntegration creates a new VM Sync integration (WebSocket-based, deprecated)
func NewVMSyncIntegration(nodeID string, conn *websocket.Conn) (*VMSyncIntegration, error) {
	collector, err := telemetry.NewCollector(nodeID)
	if err != nil {
		return nil, fmt.Errorf("failed to create telemetry collector: %w", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	transmitter := telemetry.NewTransmitter(collector, conn, time.Second*10) // 10s interval

	return &VMSyncIntegration{
		collector:         collector,
		transmitter:       transmitter,
		conn:              conn,
		ctx:               ctx,
		cancel:            cancel,
		nodeID:            nodeID,
		telemetryInterval: time.Second * 10,
		useHTTP:           false,
	}, nil
}

// NewHTTPVMSyncIntegration creates a new VM Sync integration using HTTP REST telemetry
func NewHTTPVMSyncIntegration(nodeID string, cloudSyncURL string, tokenManager *auth.VMTokenManager) (*VMSyncIntegration, error) {
	collector, err := telemetry.NewCollector(nodeID)
	if err != nil {
		return nil, fmt.Errorf("failed to create telemetry collector: %w", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	httpTransmitter := telemetry.NewHTTPTransmitter(collector, cloudSyncURL, tokenManager, time.Second*10) // 10s interval

	return &VMSyncIntegration{
		collector:         collector,
		httpTransmitter:   httpTransmitter,
		ctx:               ctx,
		cancel:            cancel,
		nodeID:            nodeID,
		telemetryInterval: time.Second * 10,
		useHTTP:           true,
	}, nil
}

// NewCloudSyncIntegration creates a new Cloud Sync integration
func NewCloudSyncIntegration(nodeID string) (*CloudSyncIntegration, error) {
	collector, err := telemetry.NewCollector(nodeID)
	if err != nil {
		return nil, fmt.Errorf("failed to create telemetry collector: %w", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	controller := NewAdaptiveController()

	return &CloudSyncIntegration{
		controller:        controller,
		collector:         collector,
		ctx:               ctx,
		cancel:            cancel,
		nodeID:            nodeID,
		telemetryInterval: time.Second * 5, // 5s interval for cloud
		configCallbacks:   make([]ConfigCallback, 0),
	}, nil
}

// Start begins the VM Sync adaptive integration
func (vsi *VMSyncIntegration) Start() error {
	vsi.mutex.Lock()
	defer vsi.mutex.Unlock()

	if vsi.isActive {
		return fmt.Errorf("VM Sync integration already active")
	}

	vsi.isActive = true

	// Start the appropriate transmitter based on the integration type
	if vsi.useHTTP && vsi.httpTransmitter != nil {
		vsi.httpTransmitter.Start()
		log.Printf("VM Sync HTTP telemetry integration started for node %s", vsi.nodeID)
	} else if vsi.transmitter != nil {
		vsi.transmitter.Start()
		log.Printf("VM Sync WebSocket telemetry integration started for node %s", vsi.nodeID)

		// Start configuration listener for WebSocket-based integration
		vsi.wg.Add(1)
		go vsi.configListener()
	} else {
		return fmt.Errorf("no transmitter available for VM Sync integration")
	}

	return nil
}

// Stop stops the VM Sync adaptive integration
func (vsi *VMSyncIntegration) Stop() {
	vsi.mutex.Lock()
	defer vsi.mutex.Unlock()

	if !vsi.isActive {
		return
	}

	vsi.isActive = false
	vsi.cancel()

	// Stop the appropriate transmitter
	if vsi.useHTTP && vsi.httpTransmitter != nil {
		vsi.httpTransmitter.Stop()
		log.Printf("VM Sync HTTP telemetry integration stopped for node %s", vsi.nodeID)
	} else if vsi.transmitter != nil {
		vsi.transmitter.Stop()
		log.Printf("VM Sync WebSocket telemetry integration stopped for node %s", vsi.nodeID)
	}

	vsi.wg.Wait()
}

// Start begins the Cloud Sync adaptive integration
func (csi *CloudSyncIntegration) Start() error {
	csi.mutex.Lock()
	defer csi.mutex.Unlock()

	if csi.isActive {
		return fmt.Errorf("Cloud Sync integration already active")
	}

	csi.isActive = true

	// Start telemetry collection for self-monitoring
	csi.wg.Add(1)
	go csi.selfMonitoringLoop()

	// Start adaptive control loop
	csi.wg.Add(1)
	go csi.adaptiveControlLoop()

	log.Printf("Cloud Sync adaptive integration started for node %s", csi.nodeID)
	return nil
}

// Stop stops the Cloud Sync adaptive integration
func (csi *CloudSyncIntegration) Stop() {
	csi.mutex.Lock()
	defer csi.mutex.Unlock()

	if !csi.isActive {
		return
	}

	csi.isActive = false
	csi.cancel()
	csi.wg.Wait()

	log.Printf("Cloud Sync adaptive integration stopped for node %s", csi.nodeID)
}

// UpdateConnectionCount updates the database connection count for VM Sync
func (vsi *VMSyncIntegration) UpdateConnectionCount(count int) {
	vsi.collector.SetConnectionCount(count)
}

// UpdateSyncLatency updates the sync latency for VM Sync
func (vsi *VMSyncIntegration) UpdateSyncLatency(latency float64) {
	vsi.collector.SetSyncLatency(latency)
}

// UpdateQueueDepth updates the processing queue depth for VM Sync
func (vsi *VMSyncIntegration) UpdateQueueDepth(depth int) {
	vsi.collector.SetQueueDepth(depth)
}

// UpdateConnectionCount updates the database connection count for Cloud Sync
func (csi *CloudSyncIntegration) UpdateConnectionCount(count int) {
	csi.collector.SetConnectionCount(count)
}

// UpdateSyncLatency updates the sync latency for Cloud Sync
func (csi *CloudSyncIntegration) UpdateSyncLatency(latency float64) {
	csi.collector.SetSyncLatency(latency)
}

// UpdateQueueDepth updates the processing queue depth for Cloud Sync
func (csi *CloudSyncIntegration) UpdateQueueDepth(depth int) {
	csi.collector.SetQueueDepth(depth)
}

// ProcessTelemetryMessage processes incoming telemetry from VM Sync
func (csi *CloudSyncIntegration) ProcessTelemetryMessage(message *models.TelemetryMessage) {
	if message.Type == "telemetry" && message.Data != nil {
		csi.controller.UpdateVMTelemetry(message.Data)
		log.Printf("Received telemetry from VM node %s: CPU=%.1f%%, Memory=%.1f%%, Latency=%.1fms",
			message.Data.NodeID, message.Data.CPUUsage, message.Data.MemoryUsage, message.Data.SyncLatency)
	}
}

// RegisterConfigCallback registers a callback for configuration changes
func (csi *CloudSyncIntegration) RegisterConfigCallback(callback ConfigCallback) {
	csi.mutex.Lock()
	defer csi.mutex.Unlock()
	csi.configCallbacks = append(csi.configCallbacks, callback)
}

// GetCurrentConfig returns the current adaptive configuration
func (csi *CloudSyncIntegration) GetCurrentConfig() *models.AdaptiveConfig {
	return csi.controller.GetCurrentConfig()
}

// configListener listens for configuration updates from Cloud Sync
func (vsi *VMSyncIntegration) configListener() {
	defer vsi.wg.Done()

	for {
		select {
		case <-vsi.ctx.Done():
			return
		default:
			// Check if connection is still valid
			if vsi.conn == nil {
				log.Printf("WebSocket connection is nil, stopping config listener")
				return
			}

			// Set read deadline
			vsi.conn.SetReadDeadline(time.Now().Add(time.Second * 30))

			// Read message
			_, messageBytes, err := vsi.conn.ReadMessage()
			if err != nil {
				if websocket.IsUnexpectedCloseError(err, websocket.CloseGoingAway, websocket.CloseAbnormalClosure) {
					log.Printf("WebSocket read error: %v", err)
				}
				// Exit the loop on any WebSocket error to prevent repeated reads on failed connection
				log.Printf("Stopping config listener due to WebSocket error")
				return
			}

			// Parse message
			var configMsg models.AdaptiveConfigMessage
			if err := json.Unmarshal(messageBytes, &configMsg); err != nil {
				continue // Not a config message, ignore
			}

			if configMsg.Type == "adaptive_config" && configMsg.Config != nil {
				vsi.handleConfigUpdate(configMsg.Config)
			}
		}
	}
}

// handleConfigUpdate handles configuration updates from Cloud Sync
func (vsi *VMSyncIntegration) handleConfigUpdate(config *models.AdaptiveConfig) {
	log.Printf("Received adaptive config update: BackPressure=%v, ThrottleDelay=%dms, MaxQueueSize=%d",
		config.BackPressure, config.ThrottleDelay, config.MaxQueueSize)

	// Apply back-pressure if requested
	if config.BackPressure {
		log.Printf("Applying back-pressure: throttle delay %dms", config.ThrottleDelay)
		// Implementation would integrate with VM Sync's processing pipeline
		// This is where you'd slow down processing, reduce batch sizes, etc.
	}

	// Adjust telemetry frequency based on system state
	if config.BackPressure {
		// Send telemetry more frequently when under pressure
		vsi.transmitter.UpdateInterval(time.Second * 5)
	} else {
		// Normal telemetry interval
		vsi.transmitter.UpdateInterval(time.Second * 10)
	}
}

// selfMonitoringLoop collects telemetry for Cloud Sync itself
func (csi *CloudSyncIntegration) selfMonitoringLoop() {
	defer csi.wg.Done()

	ticker := time.NewTicker(csi.telemetryInterval)
	defer ticker.Stop()

	for {
		select {
		case <-csi.ctx.Done():
			return
		case <-ticker.C:
			telemetry, err := csi.collector.Collect(csi.ctx)
			if err != nil {
				log.Printf("Failed to collect Cloud Sync telemetry: %v", err)
				continue
			}

			csi.controller.UpdateCloudTelemetry(telemetry)
		}
	}
}

// adaptiveControlLoop runs the adaptive control algorithm
func (csi *CloudSyncIntegration) adaptiveControlLoop() {
	defer csi.wg.Done()

	// Panic recovery to prevent system crash
	defer func() {
		if r := recover(); r != nil {
			log.Printf("🔴 PANIC RECOVERED in adaptive control loop: %v", r)
			log.Printf("Adaptive control loop will restart after recovery")
			// Restart the control loop after a brief delay
			time.Sleep(5 * time.Second)
			go csi.adaptiveControlLoop()
		}
	}()

	ticker := time.NewTicker(time.Second * 15) // Check every 15 seconds
	defer ticker.Stop()

	for {
		select {
		case <-csi.ctx.Done():
			return
		case <-ticker.C:
			// Defensive programming: wrapped in another recovery
			func() {
				defer func() {
					if r := recover(); r != nil {
						log.Printf("🔴 PANIC RECOVERED in AnalyzeAndAdjust: %v", r)
						log.Printf("Skipping this adjustment cycle")
					}
				}()

				newConfig, changed := csi.controller.AnalyzeAndAdjust(csi.ctx)
				if changed {
					csi.applyConfiguration(newConfig)
				}
			}()
		}
	}
}

// applyConfiguration applies a new adaptive configuration
func (csi *CloudSyncIntegration) applyConfiguration(config *models.AdaptiveConfig) {
	csi.mutex.Lock()
	defer csi.mutex.Unlock()

	// Call registered callbacks
	for _, callback := range csi.configCallbacks {
		if err := callback(config); err != nil {
			log.Printf("Configuration callback error: %v", err)
		}
	}

	csi.lastConfigSent = time.Now()
	log.Printf("Applied adaptive configuration: %s", config.Reason)
}

// SendConfigToVM sends configuration updates to VM Sync nodes
func (csi *CloudSyncIntegration) SendConfigToVM(conn *websocket.Conn, config *models.AdaptiveConfig) error {
	message := &models.AdaptiveConfigMessage{
		Type:      "adaptive_config",
		Config:    config,
		Timestamp: time.Now(),
	}

	msgBytes, err := json.Marshal(message)
	if err != nil {
		return fmt.Errorf("failed to marshal config message: %w", err)
	}

	return conn.WriteMessage(websocket.TextMessage, msgBytes)
}

// GetTransmitter returns the WebSocket telemetry transmitter (deprecated)
func (vsi *VMSyncIntegration) GetTransmitter() *telemetry.Transmitter {
	vsi.mutex.RLock()
	defer vsi.mutex.RUnlock()
	return vsi.transmitter
}

// GetHTTPTransmitter returns the HTTP telemetry transmitter
func (vsi *VMSyncIntegration) GetHTTPTransmitter() *telemetry.HTTPTransmitter {
	vsi.mutex.RLock()
	defer vsi.mutex.RUnlock()
	return vsi.httpTransmitter
}

// IsUsingHTTP returns true if this integration uses HTTP telemetry
func (vsi *VMSyncIntegration) IsUsingHTTP() bool {
	vsi.mutex.RLock()
	defer vsi.mutex.RUnlock()
	return vsi.useHTTP
}

// GetStats returns integration statistics
func (vsi *VMSyncIntegration) GetStats() map[string]interface{} {
	vsi.mutex.RLock()
	defer vsi.mutex.RUnlock()

	stats := map[string]interface{}{
		"nodeID":            vsi.nodeID,
		"isActive":          vsi.isActive,
		"telemetryInterval": vsi.telemetryInterval,
		"useHTTP":           vsi.useHTTP,
	}

	// Add transmitter-specific stats
	if vsi.useHTTP && vsi.httpTransmitter != nil {
		transmitterStats := vsi.httpTransmitter.GetStats()
		for k, v := range transmitterStats {
			stats["http_transmitter_"+k] = v
		}
	} else if vsi.transmitter != nil {
		transmitterStats := vsi.transmitter.GetStats()
		for k, v := range transmitterStats {
			stats["websocket_transmitter_"+k] = v
		}
	}

	return stats
}

// GetStats returns integration statistics
func (csi *CloudSyncIntegration) GetStats() map[string]interface{} {
	csi.mutex.RLock()
	defer csi.mutex.RUnlock()

	stats := map[string]interface{}{
		"nodeID":            csi.nodeID,
		"isActive":          csi.isActive,
		"telemetryInterval": csi.telemetryInterval,
		"lastConfigSent":    csi.lastConfigSent,
		"callbackCount":     len(csi.configCallbacks),
	}

	if csi.controller != nil {
		controllerStats := csi.controller.GetStats()
		for k, v := range controllerStats {
			stats["controller_"+k] = v
		}
	}

	return stats
}
