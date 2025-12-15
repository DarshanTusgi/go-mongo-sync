package transport

import (
	"context"
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"
)

// ConnectionManager implements enterprise-grade connection lifecycle management
// Integrates: Circuit Breaker + Exponential Backoff + Health Monitoring + Rebootstrap
type ConnectionManager struct {
	// Configuration
	config SenderConfig
	
	// Components
	sender         Sender
	circuitBreaker *CircuitBreaker
	backoff        *BackoffStrategy
	
	// State
	mu              sync.RWMutex
	reconnecting    atomic.Bool
	shutdownSignal  chan struct{}
	reconnectSignal chan struct{}
	
	// Metrics
	reconnectAttempts atomic.Uint64
	reconnectSuccess  atomic.Uint64
	reconnectFailures atomic.Uint64
	totalDowntime     atomic.Int64 // nanoseconds
	lastDowntime      atomic.Int64 // UnixNano timestamp
	
	// Lifecycle
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

// NewConnectionManager creates an enterprise-grade connection manager
func NewConnectionManager(config SenderConfig) (*ConnectionManager, error) {
	ctx, cancel := context.WithCancel(context.Background())
	
	// Initialize circuit breaker
	circuitBreaker := NewCircuitBreaker(CircuitBreakerConfig{
		FailureThreshold:       5,
		SuccessThreshold:       2,
		CooldownPeriod:         30 * time.Second,
		MaxConsecutiveFailures: 3,
	})
	
	// Initialize backoff strategy (gRPC-style)
	backoff := GRPCStyleBackoff()
	
	cm := &ConnectionManager{
		config:          config,
		circuitBreaker:  circuitBreaker,
		backoff:         backoff,
		shutdownSignal:  make(chan struct{}),
		reconnectSignal: make(chan struct{}, 1),
		ctx:             ctx,
		cancel:          cancel,
	}
	
	// Register circuit breaker callbacks
	circuitBreaker.OnStateChange(func(old, new ConnectionState) {
		log.Printf("🔄 CONNECTION STATE: %s → %s", old, new)
		
		// Trigger reconnection on TRANSIENT_FAILURE
		if new == StateTransientFailure {
			cm.lastDowntime.Store(time.Now().UnixNano())
			select {
			case cm.reconnectSignal <- struct{}{}:
			default:
			}
		}
		
		// Track uptime recovery
		if old == StateTransientFailure && new == StateReady {
			downtime := time.Since(time.Unix(0, cm.lastDowntime.Load()))
			cm.totalDowntime.Add(int64(downtime))
			log.Printf("⏱️  RECOVERY: Connection restored after %v downtime", downtime)
		}
	})
	
	// Initial connection
	if err := cm.connect(); err != nil {
		cancel()
		return nil, fmt.Errorf("initial connection failed: %w", err)
	}
	
	// Start background health monitor
	cm.wg.Add(1)
	go cm.healthMonitor()
	
	return cm, nil
}

// connect establishes a new connection
func (cm *ConnectionManager) connect() error {
	cm.circuitBreaker.SetState(StateConnecting)
	
	sender, err := NewSender(cm.config)
	if err != nil {
		cm.circuitBreaker.RecordFailure()
		return fmt.Errorf("failed to create sender: %w", err)
	}
	
	cm.mu.Lock()
	cm.sender = sender
	cm.mu.Unlock()
	
	cm.circuitBreaker.RecordSuccess()
	cm.backoff.Reset()
	
	log.Printf("✅ CONNECTION ESTABLISHED: %s", cm.config.Address)
	return nil
}

// reconnect attempts to reconnect with exponential backoff
func (cm *ConnectionManager) reconnect() error {
	if !cm.reconnecting.CompareAndSwap(false, true) {
		return fmt.Errorf("reconnection already in progress")
	}
	defer cm.reconnecting.Store(false)
	
	cm.reconnectAttempts.Add(1)
	
	// Check circuit breaker
	if !cm.circuitBreaker.ShouldAttemptConnection() {
		return fmt.Errorf("circuit breaker prevents connection attempt (state: %s)", cm.circuitBreaker.GetState())
	}
	
	// Close existing connection
	cm.mu.Lock()
	if cm.sender != nil {
		cm.sender.Close()
		cm.sender = nil
	}
	cm.mu.Unlock()
	
	// Exponential backoff with jitter
	backoffDuration := cm.backoff.NextBackoff()
	log.Printf("⏳ RECONNECT BACKOFF: Waiting %v (attempt %d)", backoffDuration, cm.backoff.GetAttempts())
	
	select {
	case <-time.After(backoffDuration):
	case <-cm.ctx.Done():
		return fmt.Errorf("shutdown during backoff")
	}
	
	// Attempt connection
	if err := cm.connect(); err != nil {
		cm.reconnectFailures.Add(1)
		return fmt.Errorf("reconnection failed: %w", err)
	}
	
	cm.reconnectSuccess.Add(1)
	log.Printf("✅ RECONNECTION SUCCESS: Restored after %d attempts", cm.backoff.GetAttempts())
	
	return nil
}

// healthMonitor continuously monitors connection health
func (cm *ConnectionManager) healthMonitor() {
	defer cm.wg.Done()
	
	// Health check interval (default: 30s)
	healthCheckInterval := 30 * time.Second
	if cm.config.ConnTimeout > 0 {
		healthCheckInterval = cm.config.ConnTimeout
	}
	
	ticker := time.NewTicker(healthCheckInterval)
	defer ticker.Stop()
	
	log.Printf("🏥 HEALTH MONITOR: Started (interval: %v)", healthCheckInterval)
	
	for {
		select {
		case <-ticker.C:
			// Check if sender is still alive
			cm.mu.RLock()
			sender := cm.sender
			cm.mu.RUnlock()
			
			if sender == nil {
				// No connection, trigger reconnect
				cm.circuitBreaker.SetState(StateTransientFailure)
			}
			
		case <-cm.reconnectSignal:
			// Manual reconnection trigger
			log.Printf("🔄 RECONNECT TRIGGER: Manual reconnection requested")
			if err := cm.reconnect(); err != nil {
				log.Printf("⚠️  RECONNECT FAILED: %v", err)
			}
			
		case <-cm.shutdownSignal:
			log.Printf("🛑 HEALTH MONITOR: Shutting down")
			return
			
		case <-cm.ctx.Done():
			return
		}
	}
}

// GetSender returns the current sender (may be nil if disconnected)
func (cm *ConnectionManager) GetSender() Sender {
	cm.mu.RLock()
	defer cm.mu.RUnlock()
	return cm.sender
}

// GetState returns current connection state
func (cm *ConnectionManager) GetState() ConnectionState {
	return cm.circuitBreaker.GetState()
}

// GetMetrics returns connection metrics
func (cm *ConnectionManager) GetMetrics() map[string]interface{} {
	cbStats := cm.circuitBreaker.GetStats()
	
	return map[string]interface{}{
		"state":               cm.GetState().String(),
		"reconnect_attempts":  cm.reconnectAttempts.Load(),
		"reconnect_successes": cm.reconnectSuccess.Load(),
		"reconnect_failures":  cm.reconnectFailures.Load(),
		"total_downtime_ms":   cm.totalDowntime.Load() / 1e6,
		"circuit_breaker":     cbStats,
		"backoff_attempts":    cm.backoff.GetAttempts(),
	}
}

// TriggerReconnect manually triggers a reconnection attempt
func (cm *ConnectionManager) TriggerReconnect() {
	select {
	case cm.reconnectSignal <- struct{}{}:
		log.Printf("📣 RECONNECT TRIGGERED: Manual reconnection queued")
	default:
		log.Printf("⏭️  RECONNECT SKIPPED: Reconnection already queued")
	}
}

// Close gracefully shuts down the connection manager
func (cm *ConnectionManager) Close() error {
	log.Printf("🛑 CONNECTION MANAGER: Shutting down")
	
	// Signal shutdown
	cm.circuitBreaker.Shutdown()
	close(cm.shutdownSignal)
	cm.cancel()
	
	// Close sender
	cm.mu.Lock()
	if cm.sender != nil {
		cm.sender.Close()
		cm.sender = nil
	}
	cm.mu.Unlock()
	
	// Wait for health monitor to stop
	cm.wg.Wait()
	
	log.Printf("✅ CONNECTION MANAGER: Shutdown complete")
	return nil
}
