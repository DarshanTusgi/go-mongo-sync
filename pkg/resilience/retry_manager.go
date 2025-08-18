package resilience

import (
	"context"
	"errors"
	"fmt"
	"math"
	"math/rand"
	"sync"
	"time"
)

// RetryConfig holds configuration for retry logic
type RetryConfig struct {
	MaxAttempts      int           `yaml:"max_attempts" json:"max_attempts"`
	InitialDelay     time.Duration `yaml:"initial_delay" json:"initial_delay"`
	MaxDelay         time.Duration `yaml:"max_delay" json:"max_delay"`
	BackoffFactor    float64       `yaml:"backoff_factor" json:"backoff_factor"`
	JitterEnabled    bool          `yaml:"jitter_enabled" json:"jitter_enabled"`
	RetryableErrors  []string      `yaml:"retryable_errors" json:"retryable_errors"`
	NonRetryableErrors []string    `yaml:"non_retryable_errors" json:"non_retryable_errors"`
	OnRetry          func(attempt int, err error, delay time.Duration)
	IsRetryable      func(error) bool
}

// DefaultRetryConfig returns default retry configuration
func DefaultRetryConfig() *RetryConfig {
	return &RetryConfig{
		MaxAttempts:   3,
		InitialDelay:  100 * time.Millisecond,
		MaxDelay:      30 * time.Second,
		BackoffFactor: 2.0,
		JitterEnabled: true,
		RetryableErrors: []string{
			"connection refused",
			"timeout",
			"temporary failure",
			"network unreachable",
		},
		NonRetryableErrors: []string{
			"authentication failed",
			"authorization denied",
			"invalid request",
			"not found",
		},
		IsRetryable: func(err error) bool {
			return err != nil
		},
	}
}

// RetryStats holds statistics about retry operations
type RetryStats struct {
	TotalAttempts    uint64    `json:"total_attempts"`
	SuccessfulRetries uint64   `json:"successful_retries"`
	FailedRetries    uint64    `json:"failed_retries"`
	AverageAttempts  float64   `json:"average_attempts"`
	LastRetry        time.Time `json:"last_retry"`
	TotalDelay       time.Duration `json:"total_delay"`
}

// RetryManager manages retry logic with exponential backoff
type RetryManager struct {
	config *RetryConfig
	stats  *RetryStats
	mu     sync.RWMutex
	rng    *rand.Rand
}

// NewRetryManager creates a new retry manager
func NewRetryManager(config *RetryConfig) *RetryManager {
	if config == nil {
		config = DefaultRetryConfig()
	}

	return &RetryManager{
		config: config,
		stats:  &RetryStats{},
		rng:    rand.New(rand.NewSource(time.Now().UnixNano())),
	}
}

// Execute executes a function with retry logic
func (rm *RetryManager) Execute(ctx context.Context, fn func() error) error {
	return rm.ExecuteWithResult(ctx, func() (interface{}, error) {
		return nil, fn()
	})
}

// ExecuteWithResult executes a function with retry logic and returns result
func (rm *RetryManager) ExecuteWithResult(ctx context.Context, fn func() (interface{}, error)) error {
	var lastErr error
	totalDelay := time.Duration(0)

	for attempt := 1; attempt <= rm.config.MaxAttempts; attempt++ {
		rm.mu.Lock()
		rm.stats.TotalAttempts++
		rm.mu.Unlock()

		_, err := fn()
		if err == nil {
			if attempt > 1 {
				rm.mu.Lock()
				rm.stats.SuccessfulRetries++
				rm.stats.TotalDelay += totalDelay
				rm.updateAverageAttempts(attempt)
				rm.mu.Unlock()
			}
			return nil
		}

		lastErr = err

		// Check if error is retryable
		if !rm.isRetryable(err) {
			rm.mu.Lock()
			rm.stats.FailedRetries++
			rm.mu.Unlock()
			return fmt.Errorf("non-retryable error: %w", err)
		}

		// Don't delay after the last attempt
		if attempt == rm.config.MaxAttempts {
			break
		}

		// Calculate delay with exponential backoff
		delay := rm.calculateDelay(attempt)
		totalDelay += delay

		// Call retry callback if configured
		if rm.config.OnRetry != nil {
			rm.config.OnRetry(attempt, err, delay)
		}

		rm.mu.Lock()
		rm.stats.LastRetry = time.Now()
		rm.mu.Unlock()

		// Wait for delay or context cancellation
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(delay):
			// Continue to next attempt
		}
	}

	rm.mu.Lock()
	rm.stats.FailedRetries++
	rm.stats.TotalDelay += totalDelay
	rm.updateAverageAttempts(rm.config.MaxAttempts)
	rm.mu.Unlock()

	return fmt.Errorf("max retry attempts (%d) exceeded: %w", rm.config.MaxAttempts, lastErr)
}

// ExecuteAsync executes a function asynchronously with retry logic
func (rm *RetryManager) ExecuteAsync(ctx context.Context, fn func() error, callback func(error)) {
	go func() {
		err := rm.Execute(ctx, fn)
		if callback != nil {
			callback(err)
		}
	}()
}

// GetStats returns current retry statistics
func (rm *RetryManager) GetStats() *RetryStats {
	rm.mu.RLock()
	defer rm.mu.RUnlock()

	stats := *rm.stats
	return &stats
}

// Reset resets the retry statistics
func (rm *RetryManager) Reset() {
	rm.mu.Lock()
	defer rm.mu.Unlock()

	rm.stats = &RetryStats{}
}

// isRetryable checks if an error is retryable
func (rm *RetryManager) isRetryable(err error) bool {
	if rm.config.IsRetryable != nil {
		return rm.config.IsRetryable(err)
	}

	errStr := err.Error()

	// Check non-retryable errors first
	for _, nonRetryable := range rm.config.NonRetryableErrors {
		if contains(errStr, nonRetryable) {
			return false
		}
	}

	// Check retryable errors
	for _, retryable := range rm.config.RetryableErrors {
		if contains(errStr, retryable) {
			return true
		}
	}

	// Default to retryable if not explicitly non-retryable
	return true
}

// calculateDelay calculates the delay for the given attempt
func (rm *RetryManager) calculateDelay(attempt int) time.Duration {
	// Exponential backoff: delay = initial_delay * (backoff_factor ^ (attempt - 1))
	delay := float64(rm.config.InitialDelay) * math.Pow(rm.config.BackoffFactor, float64(attempt-1))

	// Apply maximum delay limit
	if time.Duration(delay) > rm.config.MaxDelay {
		delay = float64(rm.config.MaxDelay)
	}

	// Add jitter if enabled
	if rm.config.JitterEnabled {
		// Add random jitter up to 25% of the delay
		rm.mu.Lock()
		jitter := rm.rng.Float64() * 0.25 * delay
		rm.mu.Unlock()
		delay += jitter
	}

	return time.Duration(delay)
}

// updateAverageAttempts updates the average attempts statistic
func (rm *RetryManager) updateAverageAttempts(attempts int) {
	totalOperations := rm.stats.SuccessfulRetries + rm.stats.FailedRetries
	if totalOperations == 0 {
		rm.stats.AverageAttempts = float64(attempts)
	} else {
		// Calculate weighted average
		weight := 1.0 / float64(totalOperations)
		rm.stats.AverageAttempts = rm.stats.AverageAttempts*(1-weight) + float64(attempts)*weight
	}
}

// contains checks if a string contains a substring (case-insensitive)
func contains(s, substr string) bool {
	return len(s) >= len(substr) && 
		(s == substr || 
		 len(s) > len(substr) && 
		 (s[:len(substr)] == substr || 
		  s[len(s)-len(substr):] == substr ||
		  findSubstring(s, substr)))
}

// findSubstring performs case-insensitive substring search
func findSubstring(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}

// RetryableError wraps an error to indicate it's retryable
type RetryableError struct {
	Err error
}

func (e RetryableError) Error() string {
	return e.Err.Error()
}

func (e RetryableError) Unwrap() error {
	return e.Err
}

// NonRetryableError wraps an error to indicate it's not retryable
type NonRetryableError struct {
	Err error
}

func (e NonRetryableError) Error() string {
	return e.Err.Error()
}

func (e NonRetryableError) Unwrap() error {
	return e.Err
}

// IsRetryableError checks if an error is explicitly marked as retryable
func IsRetryableError(err error) bool {
	var retryableErr RetryableError
	return errors.As(err, &retryableErr)
}

// IsNonRetryableError checks if an error is explicitly marked as non-retryable
func IsNonRetryableError(err error) bool {
	var nonRetryableErr NonRetryableError
	return errors.As(err, &nonRetryableErr)
}

// NewRetryableError creates a new retryable error
func NewRetryableError(err error) error {
	return RetryableError{Err: err}
}

// NewNonRetryableError creates a new non-retryable error
func NewNonRetryableError(err error) error {
	return NonRetryableError{Err: err}
}