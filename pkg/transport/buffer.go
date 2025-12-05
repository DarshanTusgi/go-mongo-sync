package transport

import (
	"context"
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"
)

// BatchQueueItem represents a queued batch waiting for processing
type BatchQueueItem struct {
	StreamName string
	BatchSeq   uint64
	Documents  [][]byte
	ReceivedAt time.Time
}

// BufferedBatchQueue implements a high-performance ring buffer for batches
// Inspired by Kafka's producer buffer and Disruptor pattern
type BufferedBatchQueue struct {
	// Ring buffer
	buffer        []*BatchQueueItem
	capacity      int
	readIdx       atomic.Uint64
	writeIdx      atomic.Uint64
	size          atomic.Int64
	
	// Flow control
	highWaterMark float64 // Trigger slow-down (0.75 = 75%)
	lowWaterMark  float64 // Resume normal speed (0.50 = 50%)
	
	// Synchronization
	notEmpty      chan struct{} // Signal for consumers
	notFull       chan struct{} // Signal for producers
	mu            sync.RWMutex
	
	// Statistics
	stats         BufferStats
	
	// Lifecycle
	ctx           context.Context
	cancel        context.CancelFunc
	wg            sync.WaitGroup
}

// BufferStats tracks buffer performance metrics
type BufferStats struct {
	EnqueuedTotal    atomic.Uint64
	DequeuedTotal    atomic.Uint64
	DroppedTotal     atomic.Uint64
	HighWaterReached atomic.Uint64
	FullBlockCount   atomic.Uint64
}

// NewBufferedBatchQueue creates a new buffered batch queue
func NewBufferedBatchQueue(capacity int) *BufferedBatchQueue {
	if capacity <= 0 {
		capacity = 100000 // Default: 100K batches
	}
	
	ctx, cancel := context.WithCancel(context.Background())
	
	queue := &BufferedBatchQueue{
		buffer:        make([]*BatchQueueItem, capacity),
		capacity:      capacity,
		highWaterMark: 0.75, // 75% full → slow down
		lowWaterMark:  0.50, // 50% full → resume
		notEmpty:      make(chan struct{}, 1),
		notFull:       make(chan struct{}, 1),
		ctx:           ctx,
		cancel:        cancel,
	}
	
	// Initialize size to 0
	queue.size.Store(0)
	
	log.Printf("📦 BUFFER QUEUE CREATED: capacity=%d high_water=%.0f%% low_water=%.0f%%", 
		capacity, queue.highWaterMark*100, queue.lowWaterMark*100)
	
	return queue
}

// Enqueue adds a batch to the queue (non-blocking with timeout)
func (q *BufferedBatchQueue) Enqueue(item *BatchQueueItem, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	
	for {
		// Check if we have space
		currentSize := q.size.Load()
		if currentSize < int64(q.capacity) {
			// Try to claim a slot
			writeIdx := q.writeIdx.Load()
			nextWriteIdx := (writeIdx + 1) % uint64(q.capacity)
			
			// Write to buffer
			q.buffer[writeIdx] = item
			q.writeIdx.Store(nextWriteIdx)
			
			// Increment size
			newSize := q.size.Add(1)
			q.stats.EnqueuedTotal.Add(1)
			
			// Signal waiting consumers
			select {
			case q.notEmpty <- struct{}{}:
			default:
			}
			
			// Check flow control thresholds
			usage := float64(newSize) / float64(q.capacity)
			if usage >= q.highWaterMark {
				q.stats.HighWaterReached.Add(1)
				log.Printf("⚠️ BUFFER HIGH WATER: %.1f%% full (%d/%d batches)", 
					usage*100, newSize, q.capacity)
			}
			
			return nil
		}
		
		// Buffer full - wait or timeout
		if time.Now().After(deadline) {
			q.stats.DroppedTotal.Add(1)
			return fmt.Errorf("buffer full: timeout after %v (capacity=%d)", timeout, q.capacity)
		}
		
		q.stats.FullBlockCount.Add(1)
		log.Printf("🔴 BUFFER FULL: waiting for space (size=%d capacity=%d)", currentSize, q.capacity)
		
		// Wait for space with timeout
		select {
		case <-q.notFull:
			// Space available, retry
			continue
		case <-time.After(100 * time.Millisecond):
			// Timeout, check again
			continue
		case <-q.ctx.Done():
			return fmt.Errorf("queue shutting down")
		}
	}
}

// Dequeue removes a batch from the queue (blocking until available or timeout)
func (q *BufferedBatchQueue) Dequeue(timeout time.Duration) (*BatchQueueItem, error) {
	deadline := time.Now().Add(timeout)
	
	for {
		// Check if we have data
		currentSize := q.size.Load()
		if currentSize > 0 {
			// Try to read a slot
			readIdx := q.readIdx.Load()
			nextReadIdx := (readIdx + 1) % uint64(q.capacity)
			
			// Read from buffer
			item := q.buffer[readIdx]
			q.buffer[readIdx] = nil // Clear reference
			q.readIdx.Store(nextReadIdx)
			
			// Decrement size
			newSize := q.size.Add(-1)
			q.stats.DequeuedTotal.Add(1)
			
			// Signal waiting producers
			select {
			case q.notFull <- struct{}{}:
			default:
			}
			
			// Check if we dropped below low water mark
			usage := float64(newSize) / float64(q.capacity)
			if usage < q.lowWaterMark && currentSize >= int64(float64(q.capacity)*q.highWaterMark) {
				log.Printf("✅ BUFFER LOW WATER: %.1f%% full (%d/%d batches) - resumed normal speed", 
					usage*100, newSize, q.capacity)
			}
			
			return item, nil
		}
		
		// Buffer empty - wait or timeout
		if time.Now().After(deadline) {
			return nil, fmt.Errorf("buffer empty: timeout after %v", timeout)
		}
		
		// Wait for data with timeout
		select {
		case <-q.notEmpty:
			// Data available, retry
			continue
		case <-time.After(100 * time.Millisecond):
			// Timeout, check again
			continue
		case <-q.ctx.Done():
			return nil, fmt.Errorf("queue shutting down")
		}
	}
}

// Size returns the current number of items in the queue
func (q *BufferedBatchQueue) Size() int64 {
	return q.size.Load()
}

// Usage returns the buffer usage as a percentage (0.0 to 1.0)
func (q *BufferedBatchQueue) Usage() float64 {
	return float64(q.size.Load()) / float64(q.capacity)
}

// IsHighWater returns true if buffer usage is above high water mark
func (q *BufferedBatchQueue) IsHighWater() bool {
	return q.Usage() >= q.highWaterMark
}

// GetFlowControlStatus returns current flow control status
func (q *BufferedBatchQueue) GetFlowControlStatus() string {
	usage := q.Usage()
	switch {
	case usage >= 0.95:
		return "CRITICAL" // 95%+ full
	case usage >= 0.90:
		return "SLOW"     // 90-95% full
	case usage >= q.highWaterMark:
		return "THROTTLE" // 75-90% full
	case usage < q.lowWaterMark:
		return "READY"    // <50% full
	default:
		return "NORMAL"   // 50-75% full
	}
}

// GetStats returns buffer statistics
func (q *BufferedBatchQueue) GetStats() map[string]interface{} {
	return map[string]interface{}{
		"capacity":           q.capacity,
		"size":               q.Size(),
		"usage_percent":      q.Usage() * 100,
		"enqueued_total":     q.stats.EnqueuedTotal.Load(),
		"dequeued_total":     q.stats.DequeuedTotal.Load(),
		"dropped_total":      q.stats.DroppedTotal.Load(),
		"high_water_reached": q.stats.HighWaterReached.Load(),
		"full_block_count":   q.stats.FullBlockCount.Load(),
		"flow_control":       q.GetFlowControlStatus(),
	}
}

// Close shuts down the queue
func (q *BufferedBatchQueue) Close() error {
	q.cancel()
	
	// Drain remaining items
	remaining := q.Size()
	if remaining > 0 {
		log.Printf("⚠️ BUFFER DRAIN: %d batches remaining in queue", remaining)
	}
	
	return nil
}
