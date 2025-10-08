package memory

// DEPRECATED: This buffer system has been replaced by the buffer-free
// resume token approach in pkg/resume/. This file is kept for backward
// compatibility during migration period.
//
// The buffer-free approach eliminates memory explosion during peak hours
// by using MongoDB-native resume tokens instead of in-memory buffering.
// See docs/BUFFER_FREE_ARCHITECTURE.md for details.

import (
	"bytes"
	"compress/gzip"
	"encoding/json"
	"sync"
	"time"
)

// ChangeEvent represents a change event in the buffer
type ChangeEvent struct {
	ID            string                 `json:"id"`
	OperationType string                 `json:"operation_type"`
	Namespace     string                 `json:"namespace"`
	DocumentKey   map[string]interface{} `json:"document_key"`
	FullDocument  map[string]interface{} `json:"full_document,omitempty"`
	Timestamp     time.Time              `json:"timestamp"`
	Size          int                    `json:"size"`
}

// Buffer manages a collection of change events with memory-efficient storage
type Buffer struct {
	key           string
	mu            sync.RWMutex
	events        []*ChangeEvent
	maxEvents     int
	flushSize     int
	totalSize     int
	compressed    bool
	flushCallback func([]*ChangeEvent)
}

// NewBuffer creates a new buffer
func NewBuffer(key string, maxEvents, flushSize int) *Buffer {
	return &Buffer{
		key:        key,
		events:     make([]*ChangeEvent, 0, maxEvents),
		maxEvents:  maxEvents,
		flushSize:  flushSize,
		compressed: true,
	}
}

// Add adds a change event to the buffer
func (b *Buffer) Add(event *ChangeEvent) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	// Calculate event size
	eventData, err := json.Marshal(event)
	if err != nil {
		return err
	}
	event.Size = len(eventData)

	// Check if buffer is full
	if len(b.events) >= b.maxEvents {
		// Remove oldest event
		oldEvent := b.events[0]
		b.events = b.events[1:]
		b.totalSize -= oldEvent.Size
	}

	// Add new event
	b.events = append(b.events, event)
	b.totalSize += event.Size

	// Check if flush is needed
	if len(b.events) >= b.flushSize && b.flushCallback != nil {
		go b.triggerFlush()
	}

	return nil
}

// Get returns events from the buffer
func (b *Buffer) Get(limit int) []*ChangeEvent {
	b.mu.RLock()
	defer b.mu.RUnlock()

	if limit <= 0 || limit > len(b.events) {
		limit = len(b.events)
	}

	// Return copy of events
	result := make([]*ChangeEvent, limit)
	copy(result, b.events[:limit])
	return result
}

// GetAll returns all events from the buffer
func (b *Buffer) GetAll() []*ChangeEvent {
	return b.Get(0)
}

// Size returns the total size of the buffer in bytes
func (b *Buffer) Size() int {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return b.totalSize
}

// Count returns the number of events in the buffer
func (b *Buffer) Count() int {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return len(b.events)
}

// Clear removes all events from the buffer
func (b *Buffer) Clear() {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.events = b.events[:0]
	b.totalSize = 0
}

// Flush triggers the flush callback with current events
func (b *Buffer) Flush() {
	b.mu.Lock()
	defer b.mu.Unlock()

	if len(b.events) > 0 && b.flushCallback != nil {
		// Create copy of events for callback
		eventsCopy := make([]*ChangeEvent, len(b.events))
		copy(eventsCopy, b.events)

		// Clear buffer
		b.events = b.events[:0]
		b.totalSize = 0

		// Call flush callback
		go b.flushCallback(eventsCopy)
	}
}

// SetFlushCallback sets the callback function for buffer flush
func (b *Buffer) SetFlushCallback(callback func([]*ChangeEvent)) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.flushCallback = callback
}

// triggerFlush triggers a flush if conditions are met
func (b *Buffer) triggerFlush() {
	b.mu.Lock()
	defer b.mu.Unlock()

	if len(b.events) >= b.flushSize && b.flushCallback != nil {
		// Create copy of events for callback
		eventsCopy := make([]*ChangeEvent, len(b.events))
		copy(eventsCopy, b.events)

		// Clear buffer
		b.events = b.events[:0]
		b.totalSize = 0

		// Call flush callback
		b.flushCallback(eventsCopy)
	}
}

// Compress compresses the buffer data
func (b *Buffer) Compress() ([]byte, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	data, err := json.Marshal(b.events)
	if err != nil {
		return nil, err
	}

	if !b.compressed {
		return data, nil
	}

	var buf bytes.Buffer
	gzWriter := gzip.NewWriter(&buf)

	if _, err := gzWriter.Write(data); err != nil {
		return nil, err
	}

	if err := gzWriter.Close(); err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}

// Decompress decompresses buffer data
func (b *Buffer) Decompress(data []byte) error {
	if !b.compressed {
		return json.Unmarshal(data, &b.events)
	}

	gzReader, err := gzip.NewReader(bytes.NewReader(data))
	if err != nil {
		return err
	}
	defer gzReader.Close()

	var buf bytes.Buffer
	if _, err := buf.ReadFrom(gzReader); err != nil {
		return err
	}

	return json.Unmarshal(buf.Bytes(), &b.events)
}

// GetMemoryUsage returns detailed memory usage information
func (b *Buffer) GetMemoryUsage() map[string]interface{} {
	b.mu.RLock()
	defer b.mu.RUnlock()

	return map[string]interface{}{
		"key":           b.key,
		"event_count":   len(b.events),
		"total_size":    b.totalSize,
		"max_events":    b.maxEvents,
		"flush_size":    b.flushSize,
		"compressed":    b.compressed,
		"capacity_used": float64(len(b.events)) / float64(b.maxEvents),
	}
}
