package resilience

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

// State represents the circuit breaker state
type State int32

const (
	StateClosed State = iota
	StateHalfOpen
	StateOpen
)

func (s State) String() string {
	switch s {
	case StateClosed:
		return "CLOSED"
	case StateHalfOpen:
		return "HALF_OPEN"
	case StateOpen:
		return "OPEN"
	default:
		return "UNKNOWN"
	}
}

// CircuitBreakerConfig holds configuration for the circuit breaker
type CircuitBreakerConfig struct {
	Name                string        `yaml:"name" json:"name"`
	MaxRequests         uint32        `yaml:"max_requests" json:"max_requests"`
	Interval            time.Duration `yaml:"interval" json:"interval"`
	Timeout             time.Duration `yaml:"timeout" json:"timeout"`
	ReadyToTrip         func(counts Counts) bool
	OnStateChange       func(name string, from State, to State)
	IsSuccessful        func(err error) bool
	Fallback            func(ctx context.Context, err error) (interface{}, error)
	MaxConcurrentCalls  int32         `yaml:"max_concurrent_calls" json:"max_concurrent_calls"`
	SlowCallThreshold   time.Duration `yaml:"slow_call_threshold" json:"slow_call_threshold"`
	SlowCallRateThreshold float64     `yaml:"slow_call_rate_threshold" json:"slow_call_rate_threshold"`
}

// DefaultCircuitBreakerConfig returns default configuration
func DefaultCircuitBreakerConfig(name string) *CircuitBreakerConfig {
	return &CircuitBreakerConfig{
		Name:                  name,
		MaxRequests:           5,
		Interval:              60 * time.Second,
		Timeout:               30 * time.Second,
		MaxConcurrentCalls:    100,
		SlowCallThreshold:     5 * time.Second,
		SlowCallRateThreshold: 0.5,
		ReadyToTrip: func(counts Counts) bool {
			return counts.Requests >= 5 && counts.FailureRate() > 0.6
		},
		IsSuccessful: func(err error) bool {
			return err == nil
		},
	}
}

// Counts holds the statistics for the circuit breaker
type Counts struct {
	Requests             uint32
	TotalSuccesses       uint32
	TotalFailures        uint32
	ConsecutiveSuccesses uint32
	ConsecutiveFailures  uint32
	SlowCalls            uint32
}

// FailureRate returns the failure rate
func (c Counts) FailureRate() float64 {
	if c.Requests == 0 {
		return 0.0
	}
	return float64(c.TotalFailures) / float64(c.Requests)
}

// SlowCallRate returns the slow call rate
func (c Counts) SlowCallRate() float64 {
	if c.Requests == 0 {
		return 0.0
	}
	return float64(c.SlowCalls) / float64(c.Requests)
}

// CircuitBreaker implements the circuit breaker pattern
type CircuitBreaker struct {
	config           *CircuitBreakerConfig
	mu               sync.RWMutex
	state            State
	generation       uint64
	counts           Counts
	expiry           time.Time
	concurrentCalls  int32
	lastFailureTime  time.Time
	lastSuccessTime  time.Time
	stateChangeTime  time.Time
	metrics          *CircuitBreakerMetrics
}

// CircuitBreakerMetrics holds metrics for the circuit breaker
type CircuitBreakerMetrics struct {
	TotalRequests       uint64    `json:"total_requests"`
	SuccessfulRequests  uint64    `json:"successful_requests"`
	FailedRequests      uint64    `json:"failed_requests"`
	RejectedRequests    uint64    `json:"rejected_requests"`
	SlowRequests        uint64    `json:"slow_requests"`
	StateChanges        uint64    `json:"state_changes"`
	LastStateChange     time.Time `json:"last_state_change"`
	AverageResponseTime float64   `json:"average_response_time_ms"`
	Uptime              float64   `json:"uptime_percent"`
}

// NewCircuitBreaker creates a new circuit breaker
func NewCircuitBreaker(config *CircuitBreakerConfig) *CircuitBreaker {
	if config == nil {
		config = DefaultCircuitBreakerConfig("default")
	}

	cb := &CircuitBreaker{
		config:          config,
		state:           StateClosed,
		generation:      0,
		stateChangeTime: time.Now(),
		metrics:         &CircuitBreakerMetrics{},
	}

	return cb
}

// Execute executes the given function with circuit breaker protection
func (cb *CircuitBreaker) Execute(ctx context.Context, fn func() (interface{}, error)) (interface{}, error) {
	generation, err := cb.beforeRequest()
	if err != nil {
		// Circuit is open, try fallback
		if cb.config.Fallback != nil {
			return cb.config.Fallback(ctx, err)
		}
		return nil, err
	}

	atomic.AddUint64(&cb.metrics.TotalRequests, 1)
	atomic.AddInt32(&cb.concurrentCalls, 1)
	defer atomic.AddInt32(&cb.concurrentCalls, -1)

	start := time.Now()
	result, fnErr := fn()
	duration := time.Since(start)

	cb.afterRequest(generation, fnErr, duration)

	return result, fnErr
}

// ExecuteWithTimeout executes the function with a timeout
func (cb *CircuitBreaker) ExecuteWithTimeout(ctx context.Context, fn func() (interface{}, error)) (interface{}, error) {
	if cb.config.Timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, cb.config.Timeout)
		defer cancel()
	}

	done := make(chan struct{})
	var result interface{}
	var err error

	go func() {
		defer close(done)
		result, err = cb.Execute(ctx, fn)
	}()

	select {
	case <-done:
		return result, err
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// GetState returns the current state of the circuit breaker
func (cb *CircuitBreaker) GetState() State {
	cb.mu.RLock()
	defer cb.mu.RUnlock()
	return cb.state
}

// GetCounts returns the current counts
func (cb *CircuitBreaker) GetCounts() Counts {
	cb.mu.RLock()
	defer cb.mu.RUnlock()
	return cb.counts
}

// GetMetrics returns the current metrics
func (cb *CircuitBreaker) GetMetrics() *CircuitBreakerMetrics {
	cb.mu.RLock()
	defer cb.mu.RUnlock()

	metrics := *cb.metrics
	metrics.LastStateChange = cb.stateChangeTime

	// Calculate uptime percentage
	totalTime := time.Since(cb.stateChangeTime).Seconds()
	if totalTime > 0 {
		openTime := 0.0
		if cb.state == StateOpen {
			openTime = time.Since(cb.stateChangeTime).Seconds()
		}
		metrics.Uptime = ((totalTime - openTime) / totalTime) * 100
	}

	return &metrics
}

// Reset resets the circuit breaker to closed state
func (cb *CircuitBreaker) Reset() {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	cb.toNewGeneration(time.Now())
	cb.setState(StateClosed, time.Now())
}

// beforeRequest checks if the request can proceed
func (cb *CircuitBreaker) beforeRequest() (uint64, error) {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	now := time.Now()
	state, generation := cb.currentState(now)

	if state == StateOpen {
		atomic.AddUint64(&cb.metrics.RejectedRequests, 1)
		return generation, errors.New("circuit breaker is open")
	}

	// Check concurrent calls limit
	if cb.config.MaxConcurrentCalls > 0 && atomic.LoadInt32(&cb.concurrentCalls) >= cb.config.MaxConcurrentCalls {
		atomic.AddUint64(&cb.metrics.RejectedRequests, 1)
		return generation, errors.New("too many concurrent calls")
	}

	if state == StateHalfOpen && cb.counts.Requests >= cb.config.MaxRequests {
		atomic.AddUint64(&cb.metrics.RejectedRequests, 1)
		return generation, errors.New("too many requests in half-open state")
	}

	cb.counts.Requests++
	return generation, nil
}

// afterRequest handles the result of a request
func (cb *CircuitBreaker) afterRequest(generation uint64, err error, duration time.Duration) {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	now := time.Now()
	if generation != cb.generation {
		return // Ignore if generation has changed
	}

	// Check if call was slow
	isSlowCall := cb.config.SlowCallThreshold > 0 && duration > cb.config.SlowCallThreshold
	if isSlowCall {
		cb.counts.SlowCalls++
		atomic.AddUint64(&cb.metrics.SlowRequests, 1)
	}

	if cb.config.IsSuccessful(err) {
		cb.onSuccess(now)
	} else {
		cb.onFailure(now)
	}

	// Update average response time
	cb.updateAverageResponseTime(duration)
}

// onSuccess handles successful requests
func (cb *CircuitBreaker) onSuccess(now time.Time) {
	cb.counts.TotalSuccesses++
	cb.counts.ConsecutiveSuccesses++
	cb.counts.ConsecutiveFailures = 0
	cb.lastSuccessTime = now
	atomic.AddUint64(&cb.metrics.SuccessfulRequests, 1)

	if cb.state == StateHalfOpen && cb.counts.ConsecutiveSuccesses >= cb.config.MaxRequests {
		cb.setState(StateClosed, now)
	}
}

// onFailure handles failed requests
func (cb *CircuitBreaker) onFailure(now time.Time) {
	cb.counts.TotalFailures++
	cb.counts.ConsecutiveFailures++
	cb.counts.ConsecutiveSuccesses = 0
	cb.lastFailureTime = now
	atomic.AddUint64(&cb.metrics.FailedRequests, 1)

	if cb.config.ReadyToTrip(cb.counts) {
		cb.setState(StateOpen, now)
	}
}

// currentState returns the current state and generation
func (cb *CircuitBreaker) currentState(now time.Time) (State, uint64) {
	switch cb.state {
	case StateClosed:
		if !cb.expiry.IsZero() && cb.expiry.Before(now) {
			cb.toNewGeneration(now)
		}
	case StateOpen:
		if cb.expiry.Before(now) {
			cb.setState(StateHalfOpen, now)
		}
	}
	return cb.state, cb.generation
}

// setState changes the state of the circuit breaker
func (cb *CircuitBreaker) setState(state State, now time.Time) {
	if cb.state == state {
		return
	}

	prev := cb.state
	cb.state = state
	cb.stateChangeTime = now
	atomic.AddUint64(&cb.metrics.StateChanges, 1)

	switch state {
	case StateClosed:
		cb.toNewGeneration(now)
	case StateOpen:
		cb.expiry = now.Add(cb.config.Timeout)
	case StateHalfOpen:
		cb.expiry = time.Time{}
		cb.counts = Counts{}
	}

	if cb.config.OnStateChange != nil {
		go cb.config.OnStateChange(cb.config.Name, prev, state)
	}
}

// toNewGeneration resets counts and starts a new generation
func (cb *CircuitBreaker) toNewGeneration(now time.Time) {
	cb.generation++
	cb.counts = Counts{}

	var zero time.Time
	switch cb.state {
	case StateClosed:
		if cb.config.Interval == 0 {
			cb.expiry = zero
		} else {
			cb.expiry = now.Add(cb.config.Interval)
		}
	case StateHalfOpen:
		cb.expiry = zero
	}
}

// updateAverageResponseTime updates the average response time metric
func (cb *CircuitBreaker) updateAverageResponseTime(duration time.Duration) {
	// Simple moving average calculation
	currentAvg := cb.metrics.AverageResponseTime
	totalRequests := atomic.LoadUint64(&cb.metrics.TotalRequests)

	if totalRequests == 1 {
		cb.metrics.AverageResponseTime = float64(duration.Nanoseconds()) / 1e6
	} else {
		// Weighted average
		weight := 1.0 / float64(totalRequests)
		newDuration := float64(duration.Nanoseconds()) / 1e6
		cb.metrics.AverageResponseTime = currentAvg*(1-weight) + newDuration*weight
	}
}

// String returns a string representation of the circuit breaker
func (cb *CircuitBreaker) String() string {
	cb.mu.RLock()
	defer cb.mu.RUnlock()

	return fmt.Sprintf("CircuitBreaker{name=%s, state=%s, counts=%+v}",
		cb.config.Name, cb.state.String(), cb.counts)
}