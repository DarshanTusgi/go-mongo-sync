package parallel

import (
	"context"
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"time"
)

// Task represents a unit of work
type Task struct {
	ID       string
	Data     interface{}
	Callback func(interface{}) error
	Retries  int
	Created  time.Time
}

// WorkerPoolConfig holds configuration for the worker pool
type WorkerPoolConfig struct {
	WorkerCount    int           `yaml:"worker_count" json:"worker_count"`
	QueueSize      int           `yaml:"queue_size" json:"queue_size"`
	MaxRetries     int           `yaml:"max_retries" json:"max_retries"`
	RetryDelay     time.Duration `yaml:"retry_delay" json:"retry_delay"`
	TaskTimeout    time.Duration `yaml:"task_timeout" json:"task_timeout"`
	EnableMetrics  bool          `yaml:"enable_metrics" json:"enable_metrics"`
	ShutdownTimeout time.Duration `yaml:"shutdown_timeout" json:"shutdown_timeout"`
}

// DefaultWorkerPoolConfig returns default configuration
func DefaultWorkerPoolConfig() *WorkerPoolConfig {
	return &WorkerPoolConfig{
		WorkerCount:     runtime.NumCPU(),
		QueueSize:       1000,
		MaxRetries:      3,
		RetryDelay:      time.Second,
		TaskTimeout:     30 * time.Second,
		EnableMetrics:   true,
		ShutdownTimeout: 30 * time.Second,
	}
}

// WorkerPoolStats holds statistics about the worker pool
type WorkerPoolStats struct {
	ActiveWorkers   int32     `json:"active_workers"`
	QueuedTasks     int32     `json:"queued_tasks"`
	ProcessedTasks  int64     `json:"processed_tasks"`
	FailedTasks     int64     `json:"failed_tasks"`
	RetryTasks      int64     `json:"retry_tasks"`
	AverageLatency  float64   `json:"average_latency_ms"`
	LastProcessed   time.Time `json:"last_processed"`
	StartTime       time.Time `json:"start_time"`
}

// WorkerPool manages a pool of workers for parallel task processing
type WorkerPool struct {
	config       *WorkerPoolConfig
	taskQueue    chan *Task
	retryQueue   chan *Task
	workers      []*Worker
	ctx          context.Context
	cancel       context.CancelFunc
	wg           sync.WaitGroup
	stats        *WorkerPoolStats
	mu           sync.RWMutex
	latencies    []time.Duration
	errorHandler func(error, *Task)
}

// Worker represents a single worker in the pool
type Worker struct {
	id       int
	pool     *WorkerPool
	active   int32
	ctx      context.Context
	cancel   context.CancelFunc
}

// NewWorkerPool creates a new worker pool
func NewWorkerPool(config *WorkerPoolConfig) *WorkerPool {
	if config == nil {
		config = DefaultWorkerPoolConfig()
	}

	ctx, cancel := context.WithCancel(context.Background())

	pool := &WorkerPool{
		config:     config,
		taskQueue:  make(chan *Task, config.QueueSize),
		retryQueue: make(chan *Task, config.QueueSize/2),
		workers:    make([]*Worker, config.WorkerCount),
		ctx:        ctx,
		cancel:     cancel,
		stats: &WorkerPoolStats{
			StartTime: time.Now(),
		},
		latencies: make([]time.Duration, 0, 1000),
	}

	// Create workers
	for i := 0; i < config.WorkerCount; i++ {
		workerCtx, workerCancel := context.WithCancel(ctx)
		worker := &Worker{
			id:     i,
			pool:   pool,
			ctx:    workerCtx,
			cancel: workerCancel,
		}
		pool.workers[i] = worker
	}

	return pool
}

// Start starts the worker pool
func (wp *WorkerPool) Start() {
	// Start workers
	for _, worker := range wp.workers {
		wp.wg.Add(1)
		go worker.run()
	}

	// Start retry handler
	wp.wg.Add(1)
	go wp.retryHandler()

	// Start metrics collector if enabled
	if wp.config.EnableMetrics {
		wp.wg.Add(1)
		go wp.metricsCollector()
	}
}

// Submit submits a task to the worker pool
func (wp *WorkerPool) Submit(task *Task) error {
	select {
	case wp.taskQueue <- task:
		atomic.AddInt32(&wp.stats.QueuedTasks, 1)
		return nil
	case <-wp.ctx.Done():
		return fmt.Errorf("worker pool is shutting down")
	default:
		return fmt.Errorf("task queue is full")
	}
}

// SubmitFunc submits a function as a task
func (wp *WorkerPool) SubmitFunc(id string, data interface{}, fn func(interface{}) error) error {
	task := &Task{
		ID:       id,
		Data:     data,
		Callback: fn,
		Created:  time.Now(),
	}
	return wp.Submit(task)
}

// GetStats returns current worker pool statistics
func (wp *WorkerPool) GetStats() *WorkerPoolStats {
	wp.mu.RLock()
	defer wp.mu.RUnlock()

	stats := *wp.stats
	stats.QueuedTasks = int32(len(wp.taskQueue))

	// Calculate average latency
	if len(wp.latencies) > 0 {
		var total time.Duration
		for _, latency := range wp.latencies {
			total += latency
		}
		stats.AverageLatency = float64(total.Nanoseconds()) / float64(len(wp.latencies)) / 1e6
	}

	return &stats
}

// SetErrorHandler sets the error handler function
func (wp *WorkerPool) SetErrorHandler(handler func(error, *Task)) {
	wp.mu.Lock()
	defer wp.mu.Unlock()
	wp.errorHandler = handler
}

// Stop stops the worker pool gracefully
func (wp *WorkerPool) Stop() error {
	// Close task queue
	close(wp.taskQueue)

	// Wait for workers to finish with timeout
	done := make(chan struct{})
	go func() {
		wp.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		// All workers finished gracefully
	case <-time.After(wp.config.ShutdownTimeout):
		// Timeout reached, force shutdown
		wp.cancel()
		<-done
	}

	return nil
}

// run starts the worker's main loop
func (w *Worker) run() {
	defer w.pool.wg.Done()

	for {
		select {
		case task, ok := <-w.pool.taskQueue:
			if !ok {
				return // Task queue closed
			}
			w.processTask(task)

		case task, ok := <-w.pool.retryQueue:
			if !ok {
				return // Retry queue closed
			}
			w.processTask(task)

		case <-w.ctx.Done():
			return // Worker cancelled
		}
	}
}

// processTask processes a single task
func (w *Worker) processTask(task *Task) {
	atomic.AddInt32(&w.active, 1)
	atomic.AddInt32(&w.pool.stats.ActiveWorkers, 1)
	defer func() {
		atomic.AddInt32(&w.active, -1)
		atomic.AddInt32(&w.pool.stats.ActiveWorkers, -1)
		atomic.AddInt32(&w.pool.stats.QueuedTasks, -1)
	}()

	start := time.Now()

	// Create context with timeout
	ctx, cancel := context.WithTimeout(w.ctx, w.pool.config.TaskTimeout)
	defer cancel()

	// Process task in goroutine to handle timeout
	done := make(chan error, 1)
	go func() {
		done <- task.Callback(task.Data)
	}()

	select {
	case err := <-done:
		latency := time.Since(start)
		w.pool.recordLatency(latency)

		if err != nil {
			w.pool.handleTaskError(task, err)
		} else {
			atomic.AddInt64(&w.pool.stats.ProcessedTasks, 1)
			w.pool.stats.LastProcessed = time.Now()
		}

	case <-ctx.Done():
		// Task timeout
		err := fmt.Errorf("task %s timed out after %v", task.ID, w.pool.config.TaskTimeout)
		w.pool.handleTaskError(task, err)
	}
}

// retryHandler handles task retries
func (wp *WorkerPool) retryHandler() {
	defer wp.wg.Done()

	for {
		select {
		case <-wp.ctx.Done():
			return
		default:
			// Process retry queue periodically
			time.Sleep(wp.config.RetryDelay)
		}
	}
}

// handleTaskError handles task errors and retries
func (wp *WorkerPool) handleTaskError(task *Task, err error) {
	atomic.AddInt64(&wp.stats.FailedTasks, 1)

	if task.Retries < wp.config.MaxRetries {
		task.Retries++
		atomic.AddInt64(&wp.stats.RetryTasks, 1)

		// Add to retry queue with delay
		go func() {
			time.Sleep(wp.config.RetryDelay)
			select {
			case wp.retryQueue <- task:
			case <-wp.ctx.Done():
			}
		}()
	} else {
		// Max retries reached, call error handler
		wp.mu.RLock()
		handler := wp.errorHandler
		wp.mu.RUnlock()

		if handler != nil {
			go handler(err, task)
		}
	}
}

// recordLatency records task processing latency
func (wp *WorkerPool) recordLatency(latency time.Duration) {
	wp.mu.Lock()
	defer wp.mu.Unlock()

	wp.latencies = append(wp.latencies, latency)

	// Keep only last 1000 latencies
	if len(wp.latencies) > 1000 {
		wp.latencies = wp.latencies[1:]
	}
}

// metricsCollector collects and updates metrics periodically
func (wp *WorkerPool) metricsCollector() {
	defer wp.wg.Done()

	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-wp.ctx.Done():
			return
		case <-ticker.C:
			// Update active workers count
			var activeCount int32
			for _, worker := range wp.workers {
				activeCount += atomic.LoadInt32(&worker.active)
			}
			atomic.StoreInt32(&wp.stats.ActiveWorkers, activeCount)
		}
	}
}