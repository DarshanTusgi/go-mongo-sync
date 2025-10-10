package telemetry

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"math"
	"net/http"
	"sync"
	"time"

	"go-data-sync-http/pkg/models"
	"go-data-sync-http/pkg/auth"
)

// HTTPTransmitter handles sending telemetry data via HTTP REST API
type HTTPTransmitter struct {
	collector        *Collector
	cloudSyncURL     string
	tokenManager     *auth.VMTokenManager
	httpClient       *http.Client
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

// NewHTTPTransmitter creates a new HTTP-based telemetry transmitter
func NewHTTPTransmitter(collector *Collector, cloudSyncURL string, tokenManager *auth.VMTokenManager, interval time.Duration) *HTTPTransmitter {
	ctx, cancel := context.WithCancel(context.Background())
	
	// Create HTTP client with reasonable timeouts
	httpClient := &http.Client{
		Timeout: 30 * time.Second,
		Transport: &http.Transport{
			MaxIdleConns:        10,
			IdleConnTimeout:     30 * time.Second,
			DisableCompression:  false,
			MaxIdleConnsPerHost: 2,
		},
	}
	
	return &HTTPTransmitter{
		collector:         collector,
		cloudSyncURL:      cloudSyncURL,
		tokenManager:      tokenManager,
		httpClient:        httpClient,
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

// Start begins periodic telemetry transmission via HTTP
func (h *HTTPTransmitter) Start() {
	h.wg.Add(1)
	go h.transmissionLoop()
}

// Stop stops telemetry transmission
func (h *HTTPTransmitter) Stop() {
	h.cancel()
	h.wg.Wait()

	h.mutex.Lock()
	defer h.mutex.Unlock()
	h.isConnected = false
}

// MarkDisconnected marks the transmitter as disconnected
func (h *HTTPTransmitter) MarkDisconnected() {
	h.mutex.Lock()
	defer h.mutex.Unlock()
	h.isConnected = false
	log.Printf("🔴 HTTP telemetry transmitter marked as disconnected")
}

// IsConnected returns the current connection status
func (h *HTTPTransmitter) IsConnected() bool {
	h.mutex.RLock()
	defer h.mutex.RUnlock()
	return h.isConnected
}

// GetLastTransmission returns the timestamp of the last successful transmission
func (h *HTTPTransmitter) GetLastTransmission() time.Time {
	h.mutex.RLock()
	defer h.mutex.RUnlock()
	return h.lastTransmission
}

// GetErrorCount returns the current error count
func (h *HTTPTransmitter) GetErrorCount() int {
	h.mutex.RLock()
	defer h.mutex.RUnlock()
	return h.errorCount
}

// transmissionLoop runs the periodic telemetry transmission
func (h *HTTPTransmitter) transmissionLoop() {
	defer h.wg.Done()

	ticker := time.NewTicker(h.interval)
	defer ticker.Stop()

	log.Printf("🟢 HTTP TELEMETRY TRANSMITTER: Started with interval %v", h.interval)

	for {
		select {
		case <-h.ctx.Done():
			log.Printf("🛑 HTTP TELEMETRY TRANSMITTER: Stopped (context cancelled)")
			return
		case <-ticker.C:
			if err := h.transmitTelemetry(); err != nil {
				log.Printf("❌ HTTP TELEMETRY TRANSMIT FAILED: %v", err)
				h.handleTransmissionError(err)
			} else {
				h.handleTransmissionSuccess()
			}
		}
	}
}

// transmitTelemetry collects and sends telemetry data via HTTP REST
func (h *HTTPTransmitter) transmitTelemetry() error {
	// Check circuit breaker status
	h.mutex.RLock()
	isConnected := h.isConnected
	circuitBreakerOpen := h.circuitBreakerOpen
	circuitBreakerUntil := h.circuitBreakerUntil
	h.mutex.RUnlock()

	// Check if circuit breaker should be closed
	if circuitBreakerOpen {
		if time.Now().Before(circuitBreakerUntil) {
			return fmt.Errorf("circuit breaker open - waiting until %v", circuitBreakerUntil)
		}
		// Try to close circuit breaker
		h.mutex.Lock()
		h.circuitBreakerOpen = false
		h.isConnected = true
		log.Printf("🟡 HTTP TELEMETRY: Circuit breaker half-open - attempting transmission")
		h.mutex.Unlock()
	}

	if !isConnected {
		return fmt.Errorf("transmitter marked as disconnected")
	}

	// Collect telemetry data
	telemetryData, err := h.collector.Collect(h.ctx)
	if err != nil {
		return fmt.Errorf("failed to collect telemetry: %w", err)
	}

	// Create telemetry message
	message := &models.TelemetryMessage{
		Type:      "telemetry",
		Data:      telemetryData,
		Timestamp: time.Now(),
	}

	log.Printf("📤 HTTP TELEMETRY: Sending data (CPU=%.1f%%, Mem=%.1f%%, Latency=%.1fms)",
		telemetryData.CPUUsage, telemetryData.MemoryUsage, telemetryData.SyncLatency)

	// Get fresh JWT token (per your preference)
	ctx, cancel := context.WithTimeout(h.ctx, 10*time.Second)
	token, err := h.tokenManager.GetFreshToken(ctx)
	cancel()
	if err != nil {
		return fmt.Errorf("failed to get fresh JWT token: %w", err)
	}

	// Serialize message
	msgBytes, err := json.Marshal(message)
	if err != nil {
		return fmt.Errorf("failed to marshal telemetry message: %w", err)
	}

	// Create HTTP request
	url := fmt.Sprintf("%s/api/telemetry", h.cloudSyncURL)
	req, err := http.NewRequestWithContext(h.ctx, "POST", url, bytes.NewBuffer(msgBytes))
	if err != nil {
		return fmt.Errorf("failed to create HTTP request: %w", err)
	}

	// Set headers
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("User-Agent", "vm-sync-telemetry/2.0")

	// Send HTTP request with timeout
	resp, err := h.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("HTTP request failed: %w", err)
	}
	defer resp.Body.Close()

	// Check HTTP status
	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusAccepted {
		return fmt.Errorf("HTTP error: %d %s", resp.StatusCode, resp.Status)
	}

	log.Printf("✅ HTTP TELEMETRY: Successfully sent to %s (status: %d)", url, resp.StatusCode)
	return nil
}

// handleTransmissionError handles transmission errors with enhanced resilience
func (h *HTTPTransmitter) handleTransmissionError(err error) {
	h.mutex.Lock()
	defer h.mutex.Unlock()

	h.errorCount++
	h.consecutiveFailures++
	h.lastFailureTime = time.Now()

	log.Printf("❌ HTTP TELEMETRY ERROR (total: %d, consecutive: %d): %v",
		h.errorCount, h.consecutiveFailures, err)

	// Calculate exponential backoff delay
	backoffDelay := time.Duration(float64(h.retryDelay) * math.Pow(h.backoffMultiplier, float64(h.consecutiveFailures-1)))
	if backoffDelay > h.maxBackoffDelay {
		backoffDelay = h.maxBackoffDelay
	}

	// Implement circuit breaker pattern
	if h.consecutiveFailures >= h.maxRetries {
		h.isConnected = false
		h.circuitBreakerOpen = true
		h.circuitBreakerUntil = time.Now().Add(backoffDelay)
		log.Printf("🔴 HTTP CIRCUIT BREAKER OPENED: Telemetry paused until %v (backoff: %v)", h.circuitBreakerUntil, backoffDelay)
	} else {
		log.Printf("⏱️  HTTP BACKOFF: Next telemetry retry will wait %v", backoffDelay)
	}
}

// handleTransmissionSuccess handles successful transmission
func (h *HTTPTransmitter) handleTransmissionSuccess() {
	h.mutex.Lock()
	defer h.mutex.Unlock()

	h.lastTransmission = time.Now()
	wasOpen := h.circuitBreakerOpen
	h.errorCount = 0             // Reset error count on success
	h.consecutiveFailures = 0    // Reset consecutive failures
	h.circuitBreakerOpen = false // Close circuit breaker
	h.isConnected = true         // Mark as connected

	if wasOpen {
		log.Printf("🟢 HTTP CIRCUIT BREAKER CLOSED: Telemetry transmission recovered")
	} else {
		log.Printf("✅ HTTP TELEMETRY SUCCESS: Transmission healthy (no errors)")
	}
}

// SendImmediateTelemetry sends telemetry data immediately (outside of the regular interval)
func (h *HTTPTransmitter) SendImmediateTelemetry() error {
	return h.transmitTelemetry()
}

// UpdateInterval changes the transmission interval
func (h *HTTPTransmitter) UpdateInterval(newInterval time.Duration) {
	h.mutex.Lock()
	defer h.mutex.Unlock()
	h.interval = newInterval
	log.Printf("Updated HTTP telemetry transmission interval to %v", newInterval)
}

// GetStats returns transmission statistics
func (h *HTTPTransmitter) GetStats() map[string]interface{} {
	h.mutex.RLock()
	defer h.mutex.RUnlock()

	return map[string]interface{}{
		"isConnected":         h.isConnected,
		"lastTransmission":    h.lastTransmission,
		"errorCount":          h.errorCount,
		"consecutiveFailures": h.consecutiveFailures,
		"lastFailureTime":     h.lastFailureTime,
		"circuitBreakerOpen":  h.circuitBreakerOpen,
		"circuitBreakerUntil": h.circuitBreakerUntil,
		"interval":            h.interval,
		"maxRetries":          h.maxRetries,
		"backoffMultiplier":   h.backoffMultiplier,
		"maxBackoffDelay":     h.maxBackoffDelay,
		"transport":           "HTTP",
	}
}