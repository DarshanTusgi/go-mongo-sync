package transport

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

// ConnectionState represents the circuit breaker state (gRPC-style)
type ConnectionState int32

const (
	// StateIdle means no active connection attempts
	StateIdle ConnectionState = iota
	// StateConnecting means actively trying to establish connection
	StateConnecting
	// StateReady means connection is healthy and operational
	StateReady
	// StateTransientFailure means temporary failure, will retry with backoff
	StateTransientFailure
	// StateShutdown means shutting down, no new attempts
	StateShutdown
)

func (s ConnectionState) String() string {
	switch s {
	case StateIdle:
		return "IDLE"
	case StateConnecting:
		return "CONNECTING"
	case StateReady:
		return "READY"
	case StateTransientFailure:
		return "TRANSIENT_FAILURE"
	case StateShutdown:
		return "SHUTDOWN"
	default:
		return fmt.Sprintf("UNKNOWN(%d)", s)
	}
}

// CircuitBreakerConfig defines circuit breaker behavior
type CircuitBreakerConfig struct {
	// Failure threshold to open circuit (default: 5)
	FailureThreshold int
	// Success threshold to close circuit from half-open (default: 2)
	SuccessThreshold int
	// Cooldown period before trying half-open (default: 30s)
	CooldownPeriod time.Duration
	// Max consecutive failures before TRANSIENT_FAILURE (default: 3)
	MaxConsecutiveFailures int
}

// CircuitBreaker implements enterprise-grade connection state management
type CircuitBreaker struct {
	config CircuitBreakerConfig
	
	// Current state (atomic)
	state atomic.Int32
	
	// Counters
	consecutiveFailures atomic.Int32
	consecutiveSuccesses atomic.Int32
	totalFailures atomic.Uint64
	totalSuccesses atomic.Uint64
	
	// Timestamps
	lastStateChange atomic.Int64 // UnixNano
	lastFailure     atomic.Int64
	lastSuccess     atomic.Int64
	
	// State change callbacks
	mu        sync.RWMutex
	callbacks []func(old, new ConnectionState)
}

// NewCircuitBreaker creates a circuit breaker with default config
func NewCircuitBreaker(config CircuitBreakerConfig) *CircuitBreaker {
	// Set defaults
	if config.FailureThreshold <= 0 {
		config.FailureThreshold = 5
	}
	if config.SuccessThreshold <= 0 {
		config.SuccessThreshold = 2
	}
	if config.CooldownPeriod == 0 {
		config.CooldownPeriod = 30 * time.Second
	}
	if config.MaxConsecutiveFailures <= 0 {
		config.MaxConsecutiveFailures = 3
	}
	
	cb := &CircuitBreaker{
		config:    config,
		callbacks: make([]func(old, new ConnectionState), 0),
	}
	cb.state.Store(int32(StateIdle))
	cb.lastStateChange.Store(time.Now().UnixNano())
	
	return cb
}

// GetState returns current state
func (cb *CircuitBreaker) GetState() ConnectionState {
	return ConnectionState(cb.state.Load())
}

// SetState transitions to new state with callback notification
func (cb *CircuitBreaker) SetState(newState ConnectionState) {
	oldState := ConnectionState(cb.state.Swap(int32(newState)))
	if oldState != newState {
		cb.lastStateChange.Store(time.Now().UnixNano())
		
		// Notify callbacks
		cb.mu.RLock()
		callbacks := cb.callbacks
		cb.mu.RUnlock()
		
		for _, callback := range callbacks {
			callback(oldState, newState)
		}
	}
}

// OnStateChange registers a callback for state transitions
func (cb *CircuitBreaker) OnStateChange(callback func(old, new ConnectionState)) {
	cb.mu.Lock()
	defer cb.mu.Unlock()
	cb.callbacks = append(cb.callbacks, callback)
}

// RecordSuccess records successful operation
func (cb *CircuitBreaker) RecordSuccess() {
	cb.consecutiveFailures.Store(0)
	cb.consecutiveSuccesses.Add(1)
	cb.totalSuccesses.Add(1)
	cb.lastSuccess.Store(time.Now().UnixNano())
	
	currentState := cb.GetState()
	
	// State transitions on success
	switch currentState {
	case StateConnecting:
		// Successfully connected!
		cb.SetState(StateReady)
		
	case StateTransientFailure:
		// Recovered from transient failure
		successes := cb.consecutiveSuccesses.Load()
		if successes >= int32(cb.config.SuccessThreshold) {
			cb.SetState(StateReady)
		}
	}
}

// RecordFailure records failed operation
func (cb *CircuitBreaker) RecordFailure() {
	cb.consecutiveSuccesses.Store(0)
	cb.consecutiveFailures.Add(1)
	cb.totalFailures.Add(1)
	cb.lastFailure.Store(time.Now().UnixNano())
	
	currentState := cb.GetState()
	failures := cb.consecutiveFailures.Load()
	
	// State transitions on failure
	switch currentState {
	case StateConnecting, StateReady:
		if failures >= int32(cb.config.MaxConsecutiveFailures) {
			cb.SetState(StateTransientFailure)
		}
		
	case StateTransientFailure:
		// Already in failure state, stay there
		// Backoff will continue increasing
	}
}

// ShouldAttemptConnection checks if connection attempt is allowed
func (cb *CircuitBreaker) ShouldAttemptConnection() bool {
	currentState := cb.GetState()
	
	switch currentState {
	case StateIdle, StateConnecting:
		// Always allow in these states
		return true
		
	case StateReady:
		// Connection already established
		return false
		
	case StateTransientFailure:
		// Check if cooldown period has passed
		lastChange := time.Unix(0, cb.lastStateChange.Load())
		if time.Since(lastChange) >= cb.config.CooldownPeriod {
			// Try half-open: allow one connection attempt
			cb.SetState(StateConnecting)
			return true
		}
		return false
		
	case StateShutdown:
		// Never allow during shutdown
		return false
		
	default:
		return false
	}
}

// GetStats returns circuit breaker statistics
func (cb *CircuitBreaker) GetStats() map[string]interface{} {
	return map[string]interface{}{
		"state":                 cb.GetState().String(),
		"consecutive_failures":  cb.consecutiveFailures.Load(),
		"consecutive_successes": cb.consecutiveSuccesses.Load(),
		"total_failures":        cb.totalFailures.Load(),
		"total_successes":       cb.totalSuccesses.Load(),
		"last_state_change":     time.Unix(0, cb.lastStateChange.Load()),
		"last_failure":          time.Unix(0, cb.lastFailure.Load()),
		"last_success":          time.Unix(0, cb.lastSuccess.Load()),
	}
}

// Reset resets the circuit breaker to idle state
func (cb *CircuitBreaker) Reset() {
	cb.consecutiveFailures.Store(0)
	cb.consecutiveSuccesses.Store(0)
	cb.SetState(StateIdle)
}

// Shutdown transitions to shutdown state
func (cb *CircuitBreaker) Shutdown() {
	cb.SetState(StateShutdown)
}
