package cluster

import (
	"context"
	"go-data-sync-http/pkg/models"
	"sync"
	"time"
)

// WorkerPool manages a pool of workers for processing change events
type WorkerPool struct {
	mu          sync.RWMutex
	workers     []*Worker
	workQueue   chan *models.ChangeEvent
	resultQueue chan WorkerResult
	ctx         context.Context
	cancel      context.CancelFunc
	wg          sync.WaitGroup
	config      WorkerPoolConfig
	stats       WorkerPoolStats
}

// WorkerPoolConfig holds configuration for the worker pool
type WorkerPoolConfig struct {
	WorkerCount    int           `yaml:"worker_count" json:"worker_count"`
	QueueSize      int           `yaml:"queue_size" json:"queue_size"`
	ProcessTimeout time.Duration `yaml:"process_timeout" json:"process_timeout"`
	LoadBalancing  string        `yaml:"load_balancing" json:"load_balancing"` // "round_robin", "least_busy", "random"
}

// WorkerPoolStats holds statistics for the worker pool
type WorkerPoolStats struct {
	mu                 sync.RWMutex
	TotalProcessed     int64         `json:"total_processed"`
	TotalErrors        int64         `json:"total_errors"`
	CurrentQueueSize   int           `json:"current_queue_size"`
	ActiveWorkers      int           `json:"active_workers"`
	AverageProcessTime time.Duration `json:"average_process_time"`
}

// Worker represents a single worker in the pool
type Worker struct {
	id          int
	workQueue   <-chan *models.ChangeEvent
	resultQueue chan<- WorkerResult
	ctx         context.Context
	processor   EventProcessor
	stats       WorkerStats
}

// WorkerStats holds statistics for a single worker
type WorkerStats struct {
	mu            sync.RWMutex
	Processed     int64         `json:"processed"`
	Errors        int64         `json:"errors"`
	LastProcessed time.Time     `json:"last_processed"`
	IsBusy        bool          `json:"is_busy"`
	ProcessTime   time.Duration `json:"average_process_time"`
}

// WorkerResult represents the result of processing an event
type WorkerResult struct {
	WorkerID    int
	Event       *models.ChangeEvent
	Error       error
	ProcessTime time.Duration
}

// EventProcessor defines the interface for processing events
type EventProcessor interface {
	ProcessEvent(ctx context.Context, event *models.ChangeEvent) error
}

// NewWorkerPool creates a new worker pool with the given configuration
func NewWorkerPool(config WorkerPoolConfig, processor EventProcessor) *WorkerPool {
	if config.WorkerCount <= 0 {
		config.WorkerCount = 4
	}
	if config.QueueSize <= 0 {
		config.QueueSize = 1000
	}
	if config.ProcessTimeout <= 0 {
		config.ProcessTimeout = 30 * time.Second
	}
	if config.LoadBalancing == "" {
		config.LoadBalancing = "round_robin"
	}

	ctx, cancel := context.WithCancel(context.Background())

	pool := &WorkerPool{
		workQueue:   make(chan *models.ChangeEvent, config.QueueSize),
		resultQueue: make(chan WorkerResult, config.QueueSize),
		ctx:         ctx,
		cancel:      cancel,
		config:      config,
	}

	// Create workers
	for i := 0; i < config.WorkerCount; i++ {
		worker := &Worker{
			id:          i,
			workQueue:   pool.workQueue,
			resultQueue: pool.resultQueue,
			ctx:         ctx,
			processor:   processor,
		}
		pool.workers = append(pool.workers, worker)
	}

	return pool
}

// Start starts all workers in the pool
func (wp *WorkerPool) Start() {
	// Start workers
	for _, worker := range wp.workers {
		wp.wg.Add(1)
		go wp.runWorker(worker)
	}

	// Start result processor
	wp.wg.Add(1)
	go wp.processResults()
}

// Stop stops all workers in the pool
func (wp *WorkerPool) Stop() {
	wp.cancel()
	close(wp.workQueue)
	wp.wg.Wait()
	close(wp.resultQueue)
}

// SubmitEvent submits an event for processing
func (wp *WorkerPool) SubmitEvent(event *models.ChangeEvent) bool {
	select {
	case wp.workQueue <- event:
		return true
	default:
		return false // Queue is full
	}
}

// runWorker runs a single worker
func (wp *WorkerPool) runWorker(worker *Worker) {
	defer wp.wg.Done()

	for {
		select {
		case event, ok := <-worker.workQueue:
			if !ok {
				return // Channel closed
			}

			worker.stats.mu.Lock()
			worker.stats.IsBusy = true
			worker.stats.mu.Unlock()

			startTime := time.Now()
			processCtx, cancel := context.WithTimeout(worker.ctx, wp.config.ProcessTimeout)
			err := worker.processor.ProcessEvent(processCtx, event)
			processTime := time.Since(startTime)
			cancel()

			// Update worker stats
			worker.stats.mu.Lock()
			worker.stats.Processed++
			worker.stats.LastProcessed = time.Now()
			worker.stats.IsBusy = false
			if err != nil {
				worker.stats.Errors++
			}
			// Update average process time
			if worker.stats.ProcessTime == 0 {
				worker.stats.ProcessTime = processTime
			} else {
				worker.stats.ProcessTime = (worker.stats.ProcessTime + processTime) / 2
			}
			worker.stats.mu.Unlock()

			// Send result
			select {
			case worker.resultQueue <- WorkerResult{
				WorkerID:    worker.id,
				Event:       event,
				Error:       err,
				ProcessTime: processTime,
			}:
			case <-worker.ctx.Done():
				return
			}

		case <-worker.ctx.Done():
			return
		}
	}
}

// processResults processes worker results and updates pool stats
func (wp *WorkerPool) processResults() {
	defer wp.wg.Done()

	for {
		select {
		case result, ok := <-wp.resultQueue:
			if !ok {
				return // Channel closed
			}

			// Update pool stats
			wp.stats.mu.Lock()
			wp.stats.TotalProcessed++
			if result.Error != nil {
				wp.stats.TotalErrors++
			}
			// Update average process time
			if wp.stats.AverageProcessTime == 0 {
				wp.stats.AverageProcessTime = result.ProcessTime
			} else {
				wp.stats.AverageProcessTime = (wp.stats.AverageProcessTime + result.ProcessTime) / 2
			}
			wp.stats.mu.Unlock()

		case <-wp.ctx.Done():
			return
		}
	}
}

// GetStats returns current pool statistics
func (wp *WorkerPool) GetStats() WorkerPoolStats {
	wp.stats.mu.RLock()
	defer wp.stats.mu.RUnlock()

	stats := wp.stats
	stats.CurrentQueueSize = len(wp.workQueue)

	// Count active workers
	activeCount := 0
	for _, worker := range wp.workers {
		worker.stats.mu.RLock()
		if worker.stats.IsBusy {
			activeCount++
		}
		worker.stats.mu.RUnlock()
	}
	stats.ActiveWorkers = activeCount

	return stats
}

// GetWorkerStats returns statistics for all workers
func (wp *WorkerPool) GetWorkerStats() []WorkerStats {
	stats := make([]WorkerStats, len(wp.workers))
	for i, worker := range wp.workers {
		worker.stats.mu.RLock()
		stats[i] = worker.stats
		worker.stats.mu.RUnlock()
	}
	return stats
}
