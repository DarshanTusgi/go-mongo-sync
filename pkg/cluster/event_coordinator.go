package cluster

import (
	"context"
	"go-data-sync-http/pkg/models"
	"log"
	"sync"
	"time"
)

// EventCoordinator manages event distribution and deduplication across workers
type EventCoordinator struct {
	mu          sync.RWMutex
	eventBuffer *EventBuffer
	workerPool  *WorkerPool
	inputQueue  chan *models.ChangeEvent
	outputQueue chan *models.ChangeEvent
	ctx         context.Context
	cancel      context.CancelFunc
	wg          sync.WaitGroup
	config      EventCoordinatorConfig
	stats       EventCoordinatorStats
	processors  []EventProcessor
	roundRobin  int
}

// EventCoordinatorConfig holds configuration for the event coordinator
type EventCoordinatorConfig struct {
	InputQueueSize   int               `yaml:"input_queue_size" json:"input_queue_size"`
	OutputQueueSize  int               `yaml:"output_queue_size" json:"output_queue_size"`
	BatchSize        int               `yaml:"batch_size" json:"batch_size"`
	BatchTimeout     time.Duration     `yaml:"batch_timeout" json:"batch_timeout"`
	DistributionMode string            `yaml:"distribution_mode" json:"distribution_mode"` // "broadcast", "round_robin", "hash"
	EnableDedup      bool              `yaml:"enable_dedup" json:"enable_dedup"`
	BufferConfig     EventBufferConfig `yaml:"buffer" json:"buffer"`
	WorkerPoolConfig WorkerPoolConfig  `yaml:"worker_pool" json:"worker_pool"`
}

// EventCoordinatorStats holds statistics for the event coordinator
type EventCoordinatorStats struct {
	mu                 sync.RWMutex
	TotalReceived      int64 `json:"total_received"`
	TotalProcessed     int64 `json:"total_processed"`
	TotalDuplicates    int64 `json:"total_duplicates"`
	TotalDropped       int64 `json:"total_dropped"`
	CurrentInputQueue  int   `json:"current_input_queue"`
	CurrentOutputQueue int   `json:"current_output_queue"`
}

// NewEventCoordinator creates a new event coordinator
func NewEventCoordinator(config EventCoordinatorConfig) *EventCoordinator {
	if config.InputQueueSize <= 0 {
		config.InputQueueSize = 10000
	}
	if config.OutputQueueSize <= 0 {
		config.OutputQueueSize = 10000
	}
	if config.BatchSize <= 0 {
		config.BatchSize = 100
	}
	if config.BatchTimeout <= 0 {
		config.BatchTimeout = 100 * time.Millisecond
	}
	if config.DistributionMode == "" {
		config.DistributionMode = "round_robin"
	}

	ctx, cancel := context.WithCancel(context.Background())

	coordinator := &EventCoordinator{
		inputQueue:  make(chan *models.ChangeEvent, config.InputQueueSize),
		outputQueue: make(chan *models.ChangeEvent, config.OutputQueueSize),
		ctx:         ctx,
		cancel:      cancel,
		config:      config,
	}

	// Initialize event buffer if deduplication is enabled
	if config.EnableDedup {
		coordinator.eventBuffer = NewEventBuffer(config.BufferConfig)
	}

	return coordinator
}

// AddProcessor adds an event processor to the coordinator
func (ec *EventCoordinator) AddProcessor(processor EventProcessor) {
	ec.mu.Lock()
	defer ec.mu.Unlock()
	ec.processors = append(ec.processors, processor)
}

// Start starts the event coordinator
func (ec *EventCoordinator) Start() {
	// Start input processor
	ec.wg.Add(1)
	go ec.processInput()

	// Start output processor
	ec.wg.Add(1)
	go ec.processOutput()

	// Start worker pool if configured
	if ec.config.WorkerPoolConfig.WorkerCount > 0 {
		ec.workerPool = NewWorkerPool(ec.config.WorkerPoolConfig, ec)
		ec.workerPool.Start()
	}

	log.Printf("Event coordinator started with %d processors", len(ec.processors))
}

// Stop stops the event coordinator
func (ec *EventCoordinator) Stop() {
	ec.cancel()
	close(ec.inputQueue)

	// Stop worker pool
	if ec.workerPool != nil {
		ec.workerPool.Stop()
	}

	// Stop event buffer
	if ec.eventBuffer != nil {
		ec.eventBuffer.Close()
	}

	ec.wg.Wait()
	close(ec.outputQueue)

	log.Println("Event coordinator stopped")
}

// SubmitEvent submits an event for processing
func (ec *EventCoordinator) SubmitEvent(event *models.ChangeEvent) bool {
	select {
	case ec.inputQueue <- event:
		ec.stats.mu.Lock()
		ec.stats.TotalReceived++
		ec.stats.mu.Unlock()
		return true
	default:
		ec.stats.mu.Lock()
		ec.stats.TotalDropped++
		ec.stats.mu.Unlock()
		return false // Queue is full
	}
}

// GetOutputChannel returns the output channel for processed events
func (ec *EventCoordinator) GetOutputChannel() <-chan *models.ChangeEvent {
	return ec.outputQueue
}

// processInput processes incoming events
func (ec *EventCoordinator) processInput() {
	defer ec.wg.Done()

	batch := make([]*models.ChangeEvent, 0, ec.config.BatchSize)
	ticker := time.NewTicker(ec.config.BatchTimeout)
	defer ticker.Stop()

	for {
		select {
		case event, ok := <-ec.inputQueue:
			if !ok {
				// Process remaining batch
				if len(batch) > 0 {
					ec.processBatch(batch)
				}
				return
			}

			// Check for duplicates if deduplication is enabled
			if ec.config.EnableDedup && ec.eventBuffer != nil {
				if !ec.eventBuffer.AddEvent(event) {
					ec.stats.mu.Lock()
					ec.stats.TotalDuplicates++
					ec.stats.mu.Unlock()
					continue // Skip duplicate
				}
			}

			batch = append(batch, event)

			// Process batch if it's full
			if len(batch) >= ec.config.BatchSize {
				ec.processBatch(batch)
				batch = batch[:0] // Reset batch
				ticker.Reset(ec.config.BatchTimeout)
			}

		case <-ticker.C:
			// Process batch on timeout
			if len(batch) > 0 {
				ec.processBatch(batch)
				batch = batch[:0] // Reset batch
			}

		case <-ec.ctx.Done():
			return
		}
	}
}

// processBatch processes a batch of events
func (ec *EventCoordinator) processBatch(batch []*models.ChangeEvent) {
	for _, event := range batch {
		ec.distributeEvent(event)
	}
}

// distributeEvent distributes an event based on the distribution mode
func (ec *EventCoordinator) distributeEvent(event *models.ChangeEvent) {
	switch ec.config.DistributionMode {
	case "broadcast":
		ec.broadcastEvent(event)
	case "round_robin":
		ec.roundRobinEvent(event)
	case "hash":
		ec.hashEvent(event)
	default:
		ec.roundRobinEvent(event)
	}
}

// broadcastEvent sends the event to all processors
func (ec *EventCoordinator) broadcastEvent(event *models.ChangeEvent) {
	ec.mu.RLock()
	processors := ec.processors
	ec.mu.RUnlock()

	for _, processor := range processors {
		go func(p EventProcessor) {
			ctx, cancel := context.WithTimeout(ec.ctx, 30*time.Second)
			defer cancel()
			if err := p.ProcessEvent(ctx, event); err != nil {
				log.Printf("Error processing event in broadcast mode: %v", err)
			}
		}(processor)
	}

	// Also send to output queue
	select {
	case ec.outputQueue <- event:
		ec.stats.mu.Lock()
		ec.stats.TotalProcessed++
		ec.stats.mu.Unlock()
	default:
		// Output queue is full, drop event
		ec.stats.mu.Lock()
		ec.stats.TotalDropped++
		ec.stats.mu.Unlock()
	}
}

// roundRobinEvent sends the event to processors in round-robin fashion
func (ec *EventCoordinator) roundRobinEvent(event *models.ChangeEvent) {
	ec.mu.Lock()
	if len(ec.processors) == 0 {
		ec.mu.Unlock()
		// No processors, send to output queue
		select {
		case ec.outputQueue <- event:
			ec.stats.mu.Lock()
			ec.stats.TotalProcessed++
			ec.stats.mu.Unlock()
		default:
			ec.stats.mu.Lock()
			ec.stats.TotalDropped++
			ec.stats.mu.Unlock()
		}
		return
	}

	processor := ec.processors[ec.roundRobin%len(ec.processors)]
	ec.roundRobin++
	ec.mu.Unlock()

	go func() {
		ctx, cancel := context.WithTimeout(ec.ctx, 30*time.Second)
		defer cancel()
		if err := processor.ProcessEvent(ctx, event); err != nil {
			log.Printf("Error processing event in round-robin mode: %v", err)
		}
	}()

	// Send to output queue
	select {
	case ec.outputQueue <- event:
		ec.stats.mu.Lock()
		ec.stats.TotalProcessed++
		ec.stats.mu.Unlock()
	default:
		ec.stats.mu.Lock()
		ec.stats.TotalDropped++
		ec.stats.mu.Unlock()
	}
}

// hashEvent sends the event to a processor based on hash of document key
func (ec *EventCoordinator) hashEvent(event *models.ChangeEvent) {
	ec.mu.RLock()
	if len(ec.processors) == 0 {
		ec.mu.RUnlock()
		// No processors, send to output queue
		select {
		case ec.outputQueue <- event:
			ec.stats.mu.Lock()
			ec.stats.TotalProcessed++
			ec.stats.mu.Unlock()
		default:
			ec.stats.mu.Lock()
			ec.stats.TotalDropped++
			ec.stats.mu.Unlock()
		}
		return
	}

	// Simple hash based on document key
	hash := 0
	for _, b := range event.DocumentKey {
		hash = hash*31 + int(b)
	}
	processorIndex := hash % len(ec.processors)
	processor := ec.processors[processorIndex]
	ec.mu.RUnlock()

	go func() {
		ctx, cancel := context.WithTimeout(ec.ctx, 30*time.Second)
		defer cancel()
		if err := processor.ProcessEvent(ctx, event); err != nil {
			log.Printf("Error processing event in hash mode: %v", err)
		}
	}()

	// Send to output queue
	select {
	case ec.outputQueue <- event:
		ec.stats.mu.Lock()
		ec.stats.TotalProcessed++
		ec.stats.mu.Unlock()
	default:
		ec.stats.mu.Lock()
		ec.stats.TotalDropped++
		ec.stats.mu.Unlock()
	}
}

// processOutput processes output events (placeholder for future extensions)
func (ec *EventCoordinator) processOutput() {
	defer ec.wg.Done()

	for {
		select {
		case event, ok := <-ec.outputQueue:
			if !ok {
				return
			}
			// For now, just log the event
			// In the future, this could handle additional processing
			_ = event

		case <-ec.ctx.Done():
			return
		}
	}
}

// ProcessEvent implements EventProcessor interface for worker pool integration
func (ec *EventCoordinator) ProcessEvent(ctx context.Context, event *models.ChangeEvent) error {
	// This method is called by worker pool
	ec.distributeEvent(event)
	return nil
}

// GetStats returns current coordinator statistics
func (ec *EventCoordinator) GetStats() EventCoordinatorStats {
	ec.stats.mu.RLock()
	defer ec.stats.mu.RUnlock()

	stats := ec.stats
	stats.CurrentInputQueue = len(ec.inputQueue)
	stats.CurrentOutputQueue = len(ec.outputQueue)

	return stats
}

// GetDetailedStats returns detailed statistics including buffer and worker pool stats
func (ec *EventCoordinator) GetDetailedStats() map[string]interface{} {
	stats := map[string]interface{}{
		"coordinator": ec.GetStats(),
	}

	if ec.eventBuffer != nil {
		stats["buffer"] = ec.eventBuffer.Stats()
	}

	if ec.workerPool != nil {
		stats["worker_pool"] = ec.workerPool.GetStats()
		stats["workers"] = ec.workerPool.GetWorkerStats()
	}

	return stats
}
