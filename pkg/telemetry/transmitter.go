package telemetry

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"math"
	"sync"
	"time"

	"github.com/gorilla/websocket"

	"go-data-sync-http/pkg/models"
)

// Transmitter handles sending telemetry data to Cloud Sync
type Transmitter struct {
	collector        *Collector
	conn             *websocket.Conn
	interval         time.Duration
	ctx              context.Context
	cancel           context.CancelFunc
	wg               sync.WaitGroup
	mutex            sync.RWMutex
	isConnected      bool
	lastTransmission time.Time
	errorCount       int
	maxRetries       int
	retryDelay       time.Duration
	// Enhanced error handling
	consecutiveFailures int
	lastFailureTime     time.Time
	backoffMultiplier   float64
	maxBackoffDelay     time.Duration
	circuitBreakerOpen  bool
	circuitBreakerUntil time.Time
}

// NewTransmitter creates a new telemetry transmitter
func NewTransmitter(collector *Collector, conn *websocket.Conn, interval time.Duration) *Transmitter {
	ctx, cancel := context.WithCancel(context.Background())
	return &Transmitter{
		collector:         collector,
		conn:              conn,
		interval:          interval,
		ctx:               ctx,
		cancel:            cancel,
		isConnected:       true,
		maxRetries:        3,
		retryDelay:        time.Second * 5,
		backoffMultiplier: 2.0,
		maxBackoffDelay:   time.Minute * 5,
	}
}

// Start begins periodic telemetry transmission
func (t *Transmitter) Start() {
	t.wg.Add(1)
	go t.transmissionLoop()
}

// Stop stops telemetry transmission
func (t *Transmitter) Stop() {
	t.cancel()
	t.wg.Wait()

	t.mutex.Lock()
	defer t.mutex.Unlock()
	t.isConnected = false
}

// UpdateConnection updates the WebSocket connection
// STABILITY FIX: Reset circuit breaker and error counts when connection is restored
func (t *Transmitter) UpdateConnection(conn *websocket.Conn) {
	t.mutex.Lock()
	defer t.mutex.Unlock()
	t.conn = conn
	t.isConnected = true
	t.errorCount = 0
	// STABILITY FIX: Reset circuit breaker on successful reconnection
	t.consecutiveFailures = 0
	t.circuitBreakerOpen = false
	log.Printf("✅ TELEMETRY RECONNECTED: WebSocket connection updated, circuit breaker reset")
}

// MarkDisconnected marks the connection as disconnected
func (t *Transmitter) MarkDisconnected() {
	t.mutex.Lock()
	defer t.mutex.Unlock()
	t.isConnected = false
	log.Printf("Telemetry connection marked as disconnected")
}

// IsConnected returns the current connection status
func (t *Transmitter) IsConnected() bool {
	t.mutex.RLock()
	defer t.mutex.RUnlock()
	return t.isConnected
}

// GetLastTransmission returns the timestamp of the last successful transmission
func (t *Transmitter) GetLastTransmission() time.Time {
	t.mutex.RLock()
	defer t.mutex.RUnlock()
	return t.lastTransmission
}

// GetErrorCount returns the current error count
func (t *Transmitter) GetErrorCount() int {
	t.mutex.RLock()
	defer t.mutex.RUnlock()
	return t.errorCount
}

// transmissionLoop runs the periodic telemetry transmission
func (t *Transmitter) transmissionLoop() {
	defer t.wg.Done()

	ticker := time.NewTicker(t.interval)
	defer ticker.Stop()

	log.Printf("🟢 TELEMETRY TRANSMITTER: Started with interval %v", t.interval)

	for {
		select {
		case <-t.ctx.Done():
			log.Printf("🛑 TELEMETRY TRANSMITTER: Stopped (context cancelled)")
			return
		case <-ticker.C:
			if err := t.transmitTelemetry(); err != nil {
				log.Printf("❌ TELEMETRY TRANSMIT FAILED: %v", err)
				t.handleTransmissionError(err)
			} else {
				t.handleTransmissionSuccess()
			}
		}
	}
}

// transmitTelemetry collects and sends telemetry data
// STABILITY FIX: Improved error handling and logging for telemetry transmission
func (t *Transmitter) transmitTelemetry() error {
	// Check circuit breaker status
	t.mutex.RLock()
	conn := t.conn
	isConnected := t.isConnected
	circuitBreakerOpen := t.circuitBreakerOpen
	circuitBreakerUntil := t.circuitBreakerUntil
	t.mutex.RUnlock()

	// Check if circuit breaker should be closed
	if circuitBreakerOpen {
		if time.Now().Before(circuitBreakerUntil) {
			return fmt.Errorf("circuit breaker open - waiting until %v", circuitBreakerUntil)
		}
		// Try to close circuit breaker
		t.mutex.Lock()
		t.circuitBreakerOpen = false
		t.isConnected = true
		log.Printf("🟡 TELEMETRY: Circuit breaker half-open - attempting transmission")
		t.mutex.Unlock()
	}

	if !isConnected {
		return fmt.Errorf("transmitter marked as disconnected")
	}
	
	if conn == nil {
		return fmt.Errorf("no active WebSocket connection (conn is nil)")
	}

	// Collect telemetry data
	telemetryData, err := t.collector.Collect(t.ctx)
	if err != nil {
		return fmt.Errorf("failed to collect telemetry: %w", err)
	}

	// Create telemetry message
	message := &models.TelemetryMessage{
		Type:      "telemetry",
		Data:      telemetryData,
		Timestamp: time.Now(),
	}

	// Serialize message
	msgBytes, err := json.Marshal(message)
	if err != nil {
		return fmt.Errorf("failed to marshal telemetry message: %w", err)
	}

	log.Printf("📤 TELEMETRY: Sending data (CPU=%.1f%%, Mem=%.1f%%, Latency=%.1fms)",
		telemetryData.CPUUsage, telemetryData.MemoryUsage, telemetryData.SyncLatency)

	// STABILITY FIX: Send message with timeout and better error handling
	ctx, cancel := context.WithTimeout(t.ctx, time.Second*10)
	defer cancel()

	done := make(chan error, 1)
	go func() {
		writeErr := conn.WriteMessage(websocket.TextMessage, msgBytes)
		if writeErr != nil {
			log.Printf("❌ TELEMETRY: WebSocket write failed: %v", writeErr)
		}
		done <- writeErr
	}()

	select {
	case err := <-done:
		if err != nil {
			return fmt.Errorf("failed to send telemetry message: %w", err)
		}
		log.Printf("✅ TELEMETRY: Message sent successfully via WebSocket")
	case <-ctx.Done():
		return fmt.Errorf("telemetry transmission timeout (10s)")
	}

	return nil
}

// handleTransmissionError handles transmission errors with enhanced resilience
func (t *Transmitter) handleTransmissionError(err error) {
	t.mutex.Lock()
	defer t.mutex.Unlock()

	t.errorCount++
	t.consecutiveFailures++
	t.lastFailureTime = time.Now()

	log.Printf("❌ TELEMETRY ERROR (total: %d, consecutive: %d): %v",
		t.errorCount, t.consecutiveFailures, err)

	// Calculate exponential backoff delay
	backoffDelay := time.Duration(float64(t.retryDelay) * math.Pow(t.backoffMultiplier, float64(t.consecutiveFailures-1)))
	if backoffDelay > t.maxBackoffDelay {
		backoffDelay = t.maxBackoffDelay
	}

	// Implement circuit breaker pattern
	if t.consecutiveFailures >= t.maxRetries {
		t.isConnected = false
		t.circuitBreakerOpen = true
		t.circuitBreakerUntil = time.Now().Add(backoffDelay)
		log.Printf("🔴 CIRCUIT BREAKER OPENED: Telemetry paused until %v (backoff: %v)", t.circuitBreakerUntil, backoffDelay)
	} else {
		log.Printf("⏱️  BACKOFF: Next telemetry retry will wait %v", backoffDelay)
	}
}

// handleTransmissionSuccess handles successful transmission
func (t *Transmitter) handleTransmissionSuccess() {
	t.mutex.Lock()
	defer t.mutex.Unlock()

	t.lastTransmission = time.Now()
	wasOpen := t.circuitBreakerOpen
	t.errorCount = 0             // Reset error count on success
	t.consecutiveFailures = 0    // Reset consecutive failures
	t.circuitBreakerOpen = false // Close circuit breaker
	
	if wasOpen {
		log.Printf("🟢 CIRCUIT BREAKER CLOSED: Telemetry transmission recovered")
	} else {
		log.Printf("✅ TELEMETRY SUCCESS: Transmission healthy (no errors)")
	}
}

// SendImmediateTelemetry sends telemetry data immediately (outside of the regular interval)
func (t *Transmitter) SendImmediateTelemetry() error {
	return t.transmitTelemetry()
}

// UpdateInterval changes the transmission interval
func (t *Transmitter) UpdateInterval(newInterval time.Duration) {
	t.mutex.Lock()
	defer t.mutex.Unlock()
	t.interval = newInterval
	log.Printf("Updated telemetry transmission interval to %v", newInterval)
}

// GetStats returns transmission statistics
func (t *Transmitter) GetStats() map[string]interface{} {
	t.mutex.RLock()
	defer t.mutex.RUnlock()

	return map[string]interface{}{
		"isConnected":         t.isConnected,
		"lastTransmission":    t.lastTransmission,
		"errorCount":          t.errorCount,
		"consecutiveFailures": t.consecutiveFailures,
		"lastFailureTime":     t.lastFailureTime,
		"circuitBreakerOpen":  t.circuitBreakerOpen,
		"circuitBreakerUntil": t.circuitBreakerUntil,
		"interval":            t.interval,
		"maxRetries":          t.maxRetries,
		"backoffMultiplier":   t.backoffMultiplier,
		"maxBackoffDelay":     t.maxBackoffDelay,
	}
}
