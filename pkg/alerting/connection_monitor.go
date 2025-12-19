package alerting

import (
	"context"
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"
)

// ConnectionStatus represents the health status of a connection
type ConnectionStatus string

const (
	ConnectionHealthy     ConnectionStatus = "healthy"
	ConnectionDegraded    ConnectionStatus = "degraded"
	ConnectionDisconnected ConnectionStatus = "disconnected"
	ConnectionFailed       ConnectionStatus = "failed"
)

// ConnectionType identifies the type of connection being monitored
type ConnectionType string

const (
	ConnectionTypeTCP       ConnectionType = "TCP"
	ConnectionTypeWebSocket ConnectionType = "WebSocket"
	ConnectionTypeMongoDB   ConnectionType = "MongoDB"
	ConnectionTypeHTTP      ConnectionType = "HTTP"
)

// ConnectionEvent represents a connection state change event
type ConnectionEvent struct {
	Type           ConnectionType
	Status         ConnectionStatus
	PreviousStatus ConnectionStatus
	Timestamp      time.Time
	Component      string // "cloud-sync" or "vm-sync"
	Address        string // Connection address
	Error          error  // Error if status is failed/disconnected
	Metadata       map[string]interface{}
}

// AlertCallback is called when an alert should be triggered
type AlertCallback func(event *ConnectionEvent)

// ConnectionMonitor monitors connection health and triggers alerts
type ConnectionMonitor struct {
	mu sync.RWMutex

	// State tracking
	connections map[string]*ConnectionState
	
	// Alert configuration
	tcpFailureThreshold       int           // Number of consecutive TCP failures before alert
	wsDisconnectTimeout       time.Duration // Time before WebSocket disconnect triggers alert
	reconnectGracePeriod      time.Duration // Grace period for reconnection before alert
	alertCooldown             time.Duration // Minimum time between duplicate alerts
	
	// Alert callbacks
	callbacks []AlertCallback
	
	// Last alert time (for cooldown)
	lastAlerts map[string]time.Time
	
	// Metrics
	totalAlerts atomic.Uint64
	
	// Lifecycle
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

// ConnectionState tracks the state of a single connection
type ConnectionState struct {
	Type              ConnectionType
	Status            ConnectionStatus
	Component         string
	Address           string
	LastHealthy       time.Time
	LastDisconnect    time.Time
	LastReconnect     time.Time
	ConsecutiveFailures int
	TotalFailures     int
	TotalReconnects   int
	CurrentError      error
}

// ConnectionMonitorConfig configures the connection monitor
type ConnectionMonitorConfig struct {
	TCPFailureThreshold  int
	WSDisconnectTimeout  time.Duration
	ReconnectGracePeriod time.Duration
	AlertCooldown        time.Duration
}

// DefaultConnectionMonitorConfig returns default configuration
func DefaultConnectionMonitorConfig() *ConnectionMonitorConfig {
	return &ConnectionMonitorConfig{
		TCPFailureThreshold:  3,                // Alert after 3 consecutive TCP failures
		WSDisconnectTimeout:  30 * time.Second, // Alert if WebSocket down for 30s
		ReconnectGracePeriod: 60 * time.Second, // 1 minute grace period for reconnection
		AlertCooldown:        5 * time.Minute,  // Minimum 5 minutes between duplicate alerts
	}
}

// NewConnectionMonitor creates a new connection monitor
func NewConnectionMonitor(config *ConnectionMonitorConfig) *ConnectionMonitor {
	if config == nil {
		config = DefaultConnectionMonitorConfig()
	}

	ctx, cancel := context.WithCancel(context.Background())

	cm := &ConnectionMonitor{
		connections:           make(map[string]*ConnectionState),
		tcpFailureThreshold:   config.TCPFailureThreshold,
		wsDisconnectTimeout:   config.WSDisconnectTimeout,
		reconnectGracePeriod:  config.ReconnectGracePeriod,
		alertCooldown:         config.AlertCooldown,
		callbacks:             make([]AlertCallback, 0),
		lastAlerts:            make(map[string]time.Time),
		ctx:                   ctx,
		cancel:                cancel,
	}

	// Start background monitor
	cm.wg.Add(1)
	go cm.monitorLoop()

	return cm
}

// RegisterCallback adds an alert callback
func (cm *ConnectionMonitor) RegisterCallback(callback AlertCallback) {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	cm.callbacks = append(cm.callbacks, callback)
	log.Printf("✅ CALLBACK REGISTERED: Total callbacks now: %d", len(cm.callbacks))
}

// RecordTCPHealthy records a successful TCP connection
func (cm *ConnectionMonitor) RecordTCPHealthy(component, address string) {
	cm.recordConnectionState(ConnectionTypeTCP, ConnectionHealthy, component, address, nil)
}

// RecordTCPFailure records a TCP connection failure
func (cm *ConnectionMonitor) RecordTCPFailure(component, address string, err error) {
	cm.recordConnectionState(ConnectionTypeTCP, ConnectionFailed, component, address, err)
}

// RecordTCPDisconnected records a TCP disconnection
func (cm *ConnectionMonitor) RecordTCPDisconnected(component, address string, err error) {
	cm.recordConnectionState(ConnectionTypeTCP, ConnectionDisconnected, component, address, err)
}

// RecordTCPReconnected records a TCP reconnection
func (cm *ConnectionMonitor) RecordTCPReconnected(component, address string) {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	key := fmt.Sprintf("%s_%s_%s", ConnectionTypeTCP, component, address)
	state, exists := cm.connections[key]

	if exists {
		previousStatus := state.Status
		state.Status = ConnectionHealthy
		state.LastReconnect = time.Now()
		state.LastHealthy = time.Now()
		state.ConsecutiveFailures = 0
		state.TotalReconnects++
		state.CurrentError = nil

		// Trigger reconnection success event
		if previousStatus != ConnectionHealthy {
			event := &ConnectionEvent{
				Type:           ConnectionTypeTCP,
				Status:         ConnectionHealthy,
				PreviousStatus: previousStatus,
				Timestamp:      time.Now(),
				Component:      component,
				Address:        address,
				Metadata: map[string]interface{}{
					"total_reconnects": state.TotalReconnects,
					"downtime_seconds": time.Since(state.LastDisconnect).Seconds(),
				},
			}
			cm.triggerAlert(event)
		}
	} else {
		cm.recordConnectionState(ConnectionTypeTCP, ConnectionHealthy, component, address, nil)
	}
}

// RecordWebSocketConnected records a WebSocket connection
func (cm *ConnectionMonitor) RecordWebSocketConnected(component, address string) {
	cm.recordConnectionState(ConnectionTypeWebSocket, ConnectionHealthy, component, address, nil)
}

// RecordWebSocketDisconnected records a WebSocket disconnection
func (cm *ConnectionMonitor) RecordWebSocketDisconnected(component, address string, err error) {
	cm.recordConnectionState(ConnectionTypeWebSocket, ConnectionDisconnected, component, address, err)
}

// RecordWebSocketReconnected records a WebSocket reconnection
func (cm *ConnectionMonitor) RecordWebSocketReconnected(component, address string) {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	key := fmt.Sprintf("%s_%s_%s", ConnectionTypeWebSocket, component, address)
	state, exists := cm.connections[key]

	if exists {
		previousStatus := state.Status
		state.Status = ConnectionHealthy
		state.LastReconnect = time.Now()
		state.LastHealthy = time.Now()
		state.ConsecutiveFailures = 0
		state.TotalReconnects++
		state.CurrentError = nil

		// Trigger reconnection success event
		if previousStatus != ConnectionHealthy {
			event := &ConnectionEvent{
				Type:           ConnectionTypeWebSocket,
				Status:         ConnectionHealthy,
				PreviousStatus: previousStatus,
				Timestamp:      time.Now(),
				Component:      component,
				Address:        address,
				Metadata: map[string]interface{}{
					"total_reconnects": state.TotalReconnects,
					"downtime_seconds": time.Since(state.LastDisconnect).Seconds(),
				},
			}
			cm.triggerAlert(event)
		}
	} else {
		cm.recordConnectionState(ConnectionTypeWebSocket, ConnectionHealthy, component, address, nil)
	}
}

// recordConnectionState records a connection state change
func (cm *ConnectionMonitor) recordConnectionState(connType ConnectionType, status ConnectionStatus, component, address string, err error) {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	key := fmt.Sprintf("%s_%s_%s", connType, component, address)
	state, exists := cm.connections[key]

	if !exists {
		state = &ConnectionState{
			Type:       connType,
			Status:     status,
			Component:  component,
			Address:    address,
			LastHealthy: time.Now(),
		}
		cm.connections[key] = state
	}

	previousStatus := state.Status
	state.Status = status
	state.CurrentError = err

	switch status {
	case ConnectionHealthy:
		state.LastHealthy = time.Now()
		state.ConsecutiveFailures = 0

	case ConnectionFailed:
		state.ConsecutiveFailures++
		state.TotalFailures++

		// Check if we should trigger an alert for TCP
		if connType == ConnectionTypeTCP && state.ConsecutiveFailures >= cm.tcpFailureThreshold {
			event := &ConnectionEvent{
				Type:           connType,
				Status:         status,
				PreviousStatus: previousStatus,
				Timestamp:      time.Now(),
				Component:      component,
				Address:        address,
				Error:          err,
				Metadata: map[string]interface{}{
					"consecutive_failures": state.ConsecutiveFailures,
					"total_failures":       state.TotalFailures,
					"threshold":            cm.tcpFailureThreshold,
				},
			}
			cm.triggerAlert(event)
		}

	case ConnectionDisconnected:
		state.LastDisconnect = time.Now()
		state.ConsecutiveFailures++
		state.TotalFailures++

		// Immediate alert for disconnection
		event := &ConnectionEvent{
			Type:           connType,
			PreviousStatus: previousStatus,
			Status:         status,
			Timestamp:      time.Now(),
			Component:      component,
			Address:        address,
			Error:          err,
			Metadata: map[string]interface{}{
				"last_healthy":    state.LastHealthy.Format(time.RFC3339),
				"downtime_seconds": time.Since(state.LastHealthy).Seconds(),
			},
		}
		cm.triggerAlert(event)
	}
}

// monitorLoop continuously monitors connections for extended disconnections
func (cm *ConnectionMonitor) monitorLoop() {
	defer cm.wg.Done()

	ticker := time.NewTicker(10 * time.Second) // Check every 10 seconds
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			cm.checkExtendedDisconnections()
		case <-cm.ctx.Done():
			return
		}
	}
}

// checkExtendedDisconnections checks for connections that have been disconnected for too long
func (cm *ConnectionMonitor) checkExtendedDisconnections() {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	now := time.Now()

	for key, state := range cm.connections {
		if state.Status == ConnectionDisconnected {
			downtime := now.Sub(state.LastDisconnect)

			// Check if downtime exceeds grace period
			if downtime > cm.reconnectGracePeriod {
				// Check cooldown to avoid alert spam
				lastAlert, exists := cm.lastAlerts[key]
				if !exists || now.Sub(lastAlert) > cm.alertCooldown {
					event := &ConnectionEvent{
						Type:           state.Type,
						Status:         ConnectionDegraded,
						PreviousStatus: state.Status,
						Timestamp:      now,
						Component:      state.Component,
						Address:        state.Address,
						Error:          state.CurrentError,
						Metadata: map[string]interface{}{
							"downtime_seconds":      downtime.Seconds(),
							"grace_period_seconds":  cm.reconnectGracePeriod.Seconds(),
							"consecutive_failures":  state.ConsecutiveFailures,
							"total_failures":        state.TotalFailures,
						},
					}
					cm.triggerAlertLocked(event)
				}
			}
		}
	}
}

// triggerAlert triggers an alert (with locking)
func (cm *ConnectionMonitor) triggerAlert(event *ConnectionEvent) {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	cm.triggerAlertLocked(event)
}

// triggerAlertLocked triggers an alert (without locking - caller must hold lock)
func (cm *ConnectionMonitor) triggerAlertLocked(event *ConnectionEvent) {
	key := fmt.Sprintf("%s_%s_%s", event.Type, event.Component, event.Address)

	log.Printf("🔍 DEBUG ALERT: Triggering alert for key=%s, status=%s, callbacks=%d", key, event.Status, len(cm.callbacks))

	// Check cooldown
	lastAlert, exists := cm.lastAlerts[key]
	now := time.Now()

	// Skip if in cooldown period (except for reconnection success)
	if exists && now.Sub(lastAlert) < cm.alertCooldown && event.Status != ConnectionHealthy {
		log.Printf("⏸️ DEBUG ALERT: Skipping alert - in cooldown period (last: %v, cooldown: %v)", lastAlert, cm.alertCooldown)
		return
	}

	// Update last alert time
	cm.lastAlerts[key] = now
	cm.totalAlerts.Add(1)

	log.Printf("📢 DEBUG ALERT: Calling %d alert callbacks for %s %s", len(cm.callbacks), event.Type, event.Status)

	// Call all registered callbacks (synchronously for reliable logging)
	for i, callback := range cm.callbacks {
		log.Printf("📡 DEBUG ALERT: Invoking callback %d/%d", i+1, len(cm.callbacks))
		callback(event) // Synchronous call
		log.Printf("✅ DEBUG ALERT: Callback %d/%d completed", i+1, len(cm.callbacks))
	}

	log.Printf("✅ DEBUG ALERT: All %d callbacks completed", len(cm.callbacks))
}

// GetConnectionStates returns current state of all connections
func (cm *ConnectionMonitor) GetConnectionStates() map[string]*ConnectionState {
	cm.mu.RLock()
	defer cm.mu.RUnlock()

	states := make(map[string]*ConnectionState)
	for k, v := range cm.connections {
		stateCopy := *v
		states[k] = &stateCopy
	}
	return states
}

// GetStats returns monitoring statistics
func (cm *ConnectionMonitor) GetStats() map[string]interface{} {
	cm.mu.RLock()
	defer cm.mu.RUnlock()

	tcpHealthy := 0
	tcpDisconnected := 0
	wsHealthy := 0
	wsDisconnected := 0

	for _, state := range cm.connections {
		switch state.Type {
		case ConnectionTypeTCP:
			if state.Status == ConnectionHealthy {
				tcpHealthy++
			} else {
				tcpDisconnected++
			}
		case ConnectionTypeWebSocket:
			if state.Status == ConnectionHealthy {
				wsHealthy++
			} else {
				wsDisconnected++
			}
		}
	}

	return map[string]interface{}{
		"total_alerts":        cm.totalAlerts.Load(),
		"tcp_healthy":         tcpHealthy,
		"tcp_disconnected":    tcpDisconnected,
		"ws_healthy":          wsHealthy,
		"ws_disconnected":     wsDisconnected,
		"total_connections":   len(cm.connections),
	}
}

// Stop stops the connection monitor
func (cm *ConnectionMonitor) Stop() {
	cm.cancel()
	cm.wg.Wait()
}

// LogAlertHandler is a simple alert handler that logs to stdout
func LogAlertHandler(event *ConnectionEvent) {
	log.Printf("🔔 LOG ALERT HANDLER CALLED: type=%s status=%s component=%s", event.Type, event.Status, event.Component)
	switch event.Status {
	case ConnectionHealthy:
		log.Printf("🟢 [CONNECTION RECOVERED] %s %s (%s) - RECONNECTED after %.1fs downtime",
			event.Type, event.Component, event.Address,
			event.Metadata["downtime_seconds"])

	case ConnectionDisconnected:
		log.Printf("🔴 [CONNECTION LOST] %s %s (%s) - DISCONNECTED: %v",
			event.Type, event.Component, event.Address, event.Error)

	case ConnectionFailed:
		log.Printf("⚠️  [CONNECTION FAILURE] %s %s (%s) - FAILED %d consecutive times: %v",
			event.Type, event.Component, event.Address,
			event.Metadata["consecutive_failures"], event.Error)

	case ConnectionDegraded:
		log.Printf("🟡 [CONNECTION DEGRADED] %s %s (%s) - DOWN for %.1fs (grace period exceeded)",
			event.Type, event.Component, event.Address,
			event.Metadata["downtime_seconds"])
	}
}

// CriticalAlertHandler logs critical alerts with special formatting for DevOps attention
func CriticalAlertHandler(event *ConnectionEvent) {
	log.Printf("🚨 CRITICAL ALERT HANDLER CALLED: type=%s status=%s component=%s", event.Type, event.Status, event.Component)
	if event.Status == ConnectionHealthy {
		return // Skip recovered connections for critical alerts
	}

	log.Println("═══════════════════════════════════════════════════════════")
	log.Printf("🚨 CRITICAL ALERT - DEVOPS ACTION REQUIRED")
	log.Println("═══════════════════════════════════════════════════════════")
	log.Printf("Type:      %s Connection", event.Type)
	log.Printf("Component: %s", event.Component)
	log.Printf("Address:   %s", event.Address)
	log.Printf("Status:    %s", event.Status)
	log.Printf("Time:      %s", event.Timestamp.Format(time.RFC3339))

	if event.Error != nil {
		log.Printf("Error:     %v", event.Error)
	}

	if event.Metadata != nil {
		log.Println("\nDetails:")
		for k, v := range event.Metadata {
			log.Printf("  %s: %v", k, v)
		}
	}

	switch event.Type {
	case ConnectionTypeTCP:
		log.Println("\nRecommended Actions:")
		log.Println("  1. Check if vm-sync service is running")
		log.Println("  2. Verify network connectivity between cloud-sync and vm-sync")
		log.Println("  3. Check firewall rules for TCP port")
		log.Println("  4. Review vm-sync logs for errors")
		log.Println("  5. Restart vm-sync if necessary")

	case ConnectionTypeWebSocket:
		log.Println("\nRecommended Actions:")
		log.Println("  1. Check if cloud-sync or vm-sync service is running")
		log.Println("  2. Verify WebSocket endpoint is accessible")
		log.Println("  3. Check for network issues or proxy problems")
		log.Println("  4. Review service logs for connection errors")
		log.Println("  5. Restart affected service if necessary")
	}

	log.Println("═══════════════════════════════════════════════════════════")
}
