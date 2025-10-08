package cluster

import (
	"container/ring"
	"fmt"
	"go-data-sync-http/pkg/models"
	"sync"
	"time"
)

// EventBuffer provides in-memory buffering with deduplication for change events
type EventBuffer struct {
	mu            sync.RWMutex
	events        *ring.Ring
	seenEvents    map[string]time.Time // event ID -> timestamp for deduplication
	maxSize       int
	ttl           time.Duration
	cleanupTicker *time.Ticker
	stopCleanup   chan struct{}
}

// EventBufferConfig holds configuration for the event buffer
type EventBufferConfig struct {
	MaxSize         int           `yaml:"max_size" json:"max_size"`
	TTL             time.Duration `yaml:"ttl" json:"ttl"`
	CleanupInterval time.Duration `yaml:"cleanup_interval" json:"cleanup_interval"`
}

// NewEventBuffer creates a new event buffer with the given configuration
func NewEventBuffer(config EventBufferConfig) *EventBuffer {
	if config.MaxSize <= 0 {
		config.MaxSize = 10000
	}
	if config.TTL <= 0 {
		config.TTL = 5 * time.Minute
	}
	if config.CleanupInterval <= 0 {
		config.CleanupInterval = 1 * time.Minute
	}

	buffer := &EventBuffer{
		events:        ring.New(config.MaxSize),
		seenEvents:    make(map[string]time.Time),
		maxSize:       config.MaxSize,
		ttl:           config.TTL,
		cleanupTicker: time.NewTicker(config.CleanupInterval),
		stopCleanup:   make(chan struct{}),
	}

	// Start cleanup goroutine
	go buffer.cleanupExpiredEvents()

	return buffer
}

// AddEvent adds an event to the buffer if it hasn't been seen recently
// Returns true if the event was added (not a duplicate), false otherwise
func (eb *EventBuffer) AddEvent(event *models.ChangeEvent) bool {
	eb.mu.Lock()
	defer eb.mu.Unlock()

	// Generate event ID for deduplication
	eventID := eb.generateEventID(event)

	// Check if we've seen this event recently
	if lastSeen, exists := eb.seenEvents[eventID]; exists {
		if time.Since(lastSeen) < eb.ttl {
			return false // Duplicate event
		}
	}

	// Add to seen events
	eb.seenEvents[eventID] = time.Now()

	// Add to ring buffer
	eb.events.Value = event
	eb.events = eb.events.Next()

	return true
}

// GetRecentEvents returns events from the buffer within the specified duration
func (eb *EventBuffer) GetRecentEvents(since time.Duration) []*models.ChangeEvent {
	eb.mu.RLock()
	defer eb.mu.RUnlock()

	var events []*models.ChangeEvent
	cutoff := time.Now().Add(-since)

	eb.events.Do(func(v interface{}) {
		if v != nil {
			if event, ok := v.(*models.ChangeEvent); ok {
				if event.Timestamp.After(cutoff) {
					events = append(events, event)
				}
			}
		}
	})

	return events
}

// generateEventID creates a unique identifier for an event based on its content
func (eb *EventBuffer) generateEventID(event *models.ChangeEvent) string {
	docKeyStr := fmt.Sprintf("%x", event.DocumentKey)
	return event.Database + "|" + event.Collection + "|" +
		event.OperationType + "|" + docKeyStr + "|" +
		event.Timestamp.Format(time.RFC3339Nano)
}

// cleanupExpiredEvents removes old events from the seen events map
func (eb *EventBuffer) cleanupExpiredEvents() {
	for {
		select {
		case <-eb.cleanupTicker.C:
			eb.mu.Lock()
			now := time.Now()
			for eventID, timestamp := range eb.seenEvents {
				if now.Sub(timestamp) > eb.ttl {
					delete(eb.seenEvents, eventID)
				}
			}
			eb.mu.Unlock()
		case <-eb.stopCleanup:
			return
		}
	}
}

// Close stops the event buffer and cleanup goroutine
func (eb *EventBuffer) Close() {
	eb.cleanupTicker.Stop()
	close(eb.stopCleanup)
}

// Stats returns buffer statistics
func (eb *EventBuffer) Stats() map[string]interface{} {
	eb.mu.RLock()
	defer eb.mu.RUnlock()

	return map[string]interface{}{
		"seen_events_count": len(eb.seenEvents),
		"max_size":          eb.maxSize,
		"ttl_seconds":       eb.ttl.Seconds(),
	}
}
