package transport

import (
	"math"
	"math/rand"
	"time"
)

// BackoffConfig defines exponential backoff behavior (gRPC/Kafka style)
type BackoffConfig struct {
	// InitialBackoff is the wait time after first failure (default: 1s)
	InitialBackoff time.Duration
	// MaxBackoff is the maximum wait time (default: 120s like gRPC)
	MaxBackoff time.Duration
	// Multiplier for exponential growth (default: 1.6 like gRPC)
	Multiplier float64
	// Jitter adds randomization to prevent thundering herd (default: 0.2 = ±20%)
	Jitter float64
}

// BackoffStrategy implements enterprise-grade exponential backoff with jitter
// Based on gRPC, Kafka, and AWS best practices
type BackoffStrategy struct {
	config BackoffConfig
	
	// Current attempt counter (resets on success)
	attempts int
	
	// Random source for jitter
	rand *rand.Rand
}

// NewBackoffStrategy creates a backoff strategy with default config
func NewBackoffStrategy(config BackoffConfig) *BackoffStrategy {
	// Set defaults (gRPC-style)
	if config.InitialBackoff == 0 {
		config.InitialBackoff = 1 * time.Second
	}
	if config.MaxBackoff == 0 {
		config.MaxBackoff = 120 * time.Second
	}
	if config.Multiplier == 0 {
		config.Multiplier = 1.6
	}
	if config.Jitter == 0 {
		config.Jitter = 0.2 // ±20%
	}
	
	return &BackoffStrategy{
		config: config,
		rand:   rand.New(rand.NewSource(time.Now().UnixNano())),
	}
}

// NextBackoff calculates the next backoff duration with exponential growth and jitter
// Formula (gRPC-style): min(InitialBackoff * Multiplier^attempts, MaxBackoff) * (1 + random(-Jitter, +Jitter))
func (b *BackoffStrategy) NextBackoff() time.Duration {
	// Calculate exponential backoff
	backoff := float64(b.config.InitialBackoff) * math.Pow(b.config.Multiplier, float64(b.attempts))
	
	// Cap at max backoff
	if backoff > float64(b.config.MaxBackoff) {
		backoff = float64(b.config.MaxBackoff)
	}
	
	// Add jitter (±20% by default)
	// This prevents synchronized reconnection attempts (thundering herd)
	jitter := 1.0 + (b.rand.Float64()*2-1)*b.config.Jitter // Random in range [1-Jitter, 1+Jitter]
	backoff = backoff * jitter
	
	// Increment attempts for next time
	b.attempts++
	
	return time.Duration(backoff)
}

// Reset resets the backoff counter (call on successful connection)
func (b *BackoffStrategy) Reset() {
	b.attempts = 0
}

// GetAttempts returns current attempt count
func (b *BackoffStrategy) GetAttempts() int {
	return b.attempts
}

// Sleep waits for the next backoff duration
func (b *BackoffStrategy) Sleep() time.Duration {
	duration := b.NextBackoff()
	time.Sleep(duration)
	return duration
}

// KafkaStyleBackoff creates a Kafka-style backoff (faster initial, capped at 1s)
func KafkaStyleBackoff() *BackoffStrategy {
	return NewBackoffStrategy(BackoffConfig{
		InitialBackoff: 50 * time.Millisecond,
		MaxBackoff:     1000 * time.Millisecond, // Kafka default
		Multiplier:     2.0,
		Jitter:         0.2,
	})
}

// GRPCStyleBackoff creates a gRPC-style backoff (standard enterprise)
func GRPCStyleBackoff() *BackoffStrategy {
	return NewBackoffStrategy(BackoffConfig{
		InitialBackoff: 1 * time.Second,
		MaxBackoff:     120 * time.Second,
		Multiplier:     1.6,
		Jitter:         0.2,
	})
}

// AWSStyleBackoff creates AWS-style backoff with full jitter
func AWSStyleBackoff() *BackoffStrategy {
	return NewBackoffStrategy(BackoffConfig{
		InitialBackoff: 1 * time.Second,
		MaxBackoff:     60 * time.Second,
		Multiplier:     2.0,
		Jitter:         1.0, // Full jitter (0-200%)
	})
}
