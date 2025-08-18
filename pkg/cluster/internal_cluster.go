package cluster

import (
	"context"
	"sync"
	"time"
	"log"
	"go-data-sync-http/pkg/models"
)

// InternalCluster manages internal clustering with worker pools and event coordination
type InternalCluster struct {
	mu               sync.RWMutex
	eventCoordinator *EventCoordinator
	processors       []EventProcessor
	metrics          *MetricsCollector
	ctx              context.Context
	cancel           context.CancelFunc
	wg               sync.WaitGroup
	config           models.InternalClusterConfig
	running          bool
}

// NewInternalCluster creates a new internal cluster manager
func NewInternalCluster(config models.InternalClusterConfig) *InternalCluster {
	ctx, cancel := context.WithCancel(context.Background())

	cluster := &InternalCluster{
		ctx:    ctx,
		cancel: cancel,
		config: config,
	}

	// Initialize event coordinator
	coordinatorConfig := EventCoordinatorConfig{
		InputQueueSize:   config.EventCoordinator.InputQueueSize,
		OutputQueueSize:  config.EventCoordinator.OutputQueueSize,
		BatchSize:        config.EventCoordinator.BatchSize,
		BatchTimeout:     config.EventCoordinator.BatchTimeout,
		DistributionMode: config.EventCoordinator.DistributionMode,
		EnableDedup:      config.EventCoordinator.EnableDedup,
		BufferConfig: EventBufferConfig{
			MaxSize:         config.EventBuffer.MaxSize,
			TTL:             config.EventBuffer.TTL,
			CleanupInterval: config.EventBuffer.CleanupInterval,
		},
		WorkerPoolConfig: WorkerPoolConfig{
			WorkerCount:    config.WorkerPool.WorkerCount,
			QueueSize:      config.WorkerPool.QueueSize,
			ProcessTimeout: config.WorkerPool.ProcessTimeout,
			LoadBalancing:  config.WorkerPool.LoadBalancing,
		},
	}

	cluster.eventCoordinator = NewEventCoordinator(coordinatorConfig)

	// Initialize metrics collector if enabled
	if config.Metrics.Enabled {
		cluster.metrics = NewMetricsCollector(config.Metrics)
	}

	return cluster
}

// AddProcessor adds an event processor to the cluster
func (ic *InternalCluster) AddProcessor(processor EventProcessor) {
	ic.mu.Lock()
	defer ic.mu.Unlock()

	ic.processors = append(ic.processors, processor)
	if ic.eventCoordinator != nil {
		ic.eventCoordinator.AddProcessor(processor)
	}
}

// Start starts the internal cluster
func (ic *InternalCluster) Start() error {
	ic.mu.Lock()
	defer ic.mu.Unlock()

	if ic.running {
		return nil
	}

	// Start event coordinator
	if ic.eventCoordinator != nil {
		ic.eventCoordinator.Start()
	}

	// Start metrics collector
	if ic.metrics != nil {
		ic.wg.Add(1)
		go ic.runMetricsCollector()
	}

	ic.running = true
	log.Printf("Internal cluster started with %d processors", len(ic.processors))

	return nil
}

// Stop stops the internal cluster
func (ic *InternalCluster) Stop() {
	ic.mu.Lock()
	defer ic.mu.Unlock()

	if !ic.running {
		return
	}

	ic.cancel()

	// Stop event coordinator
	if ic.eventCoordinator != nil {
		ic.eventCoordinator.Stop()
	}

	// Stop metrics collector
	if ic.metrics != nil {
		ic.metrics.Stop()
	}

	ic.wg.Wait()
	ic.running = false

	log.Println("Internal cluster stopped")
}

// ProcessEvent processes a change event through the internal cluster
func (ic *InternalCluster) ProcessEvent(event *models.ChangeEvent) bool {
	if ic.eventCoordinator == nil {
		return false
	}

	return ic.eventCoordinator.SubmitEvent(event)
}

// GetOutputChannel returns the output channel for processed events
func (ic *InternalCluster) GetOutputChannel() <-chan *models.ChangeEvent {
	if ic.eventCoordinator == nil {
		return nil
	}
	return ic.eventCoordinator.GetOutputChannel()
}

// GetStats returns comprehensive cluster statistics
func (ic *InternalCluster) GetStats() map[string]interface{} {
	stats := map[string]interface{}{
		"running":           ic.running,
		"processor_count":   len(ic.processors),
		"config":            ic.config,
	}

	if ic.eventCoordinator != nil {
		stats["coordinator"] = ic.eventCoordinator.GetDetailedStats()
	}

	if ic.metrics != nil {
		stats["metrics"] = ic.metrics.GetStats()
	}

	return stats
}

// runMetricsCollector runs the metrics collection loop
func (ic *InternalCluster) runMetricsCollector() {
	defer ic.wg.Done()

	ticker := time.NewTicker(ic.config.Metrics.CollectionInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			if ic.metrics != nil {
				ic.collectMetrics()
			}

		case <-ic.ctx.Done():
			return
		}
	}
}

// collectMetrics collects metrics from all components
func (ic *InternalCluster) collectMetrics() {
	if ic.metrics == nil {
		return
	}

	// Collect coordinator metrics
	if ic.eventCoordinator != nil {
		coordinatorStats := ic.eventCoordinator.GetStats()
		ic.metrics.RecordMetric("coordinator.total_received", float64(coordinatorStats.TotalReceived))
		ic.metrics.RecordMetric("coordinator.total_processed", float64(coordinatorStats.TotalProcessed))
		ic.metrics.RecordMetric("coordinator.total_duplicates", float64(coordinatorStats.TotalDuplicates))
		ic.metrics.RecordMetric("coordinator.total_dropped", float64(coordinatorStats.TotalDropped))
		ic.metrics.RecordMetric("coordinator.input_queue_size", float64(coordinatorStats.CurrentInputQueue))
		ic.metrics.RecordMetric("coordinator.output_queue_size", float64(coordinatorStats.CurrentOutputQueue))
	}

	// Collect processor count
	ic.metrics.RecordMetric("cluster.processor_count", float64(len(ic.processors)))
}

// IsRunning returns whether the cluster is currently running
func (ic *InternalCluster) IsRunning() bool {
	ic.mu.RLock()
	defer ic.mu.RUnlock()
	return ic.running
}

// GetConfig returns the cluster configuration
func (ic *InternalCluster) GetConfig() models.InternalClusterConfig {
	return ic.config
}

// UpdateConfig updates the cluster configuration (requires restart)
func (ic *InternalCluster) UpdateConfig(config models.InternalClusterConfig) {
	ic.mu.Lock()
	defer ic.mu.Unlock()
	ic.config = config
}

// GetProcessorCount returns the number of registered processors
func (ic *InternalCluster) GetProcessorCount() int {
	ic.mu.RLock()
	defer ic.mu.RUnlock()
	return len(ic.processors)
}

// RemoveProcessor removes a processor from the cluster
func (ic *InternalCluster) RemoveProcessor(processor EventProcessor) bool {
	ic.mu.Lock()
	defer ic.mu.Unlock()

	for i, p := range ic.processors {
		if p == processor {
			// Remove processor from slice
			ic.processors = append(ic.processors[:i], ic.processors[i+1:]...)
			return true
		}
	}
	return false
}

// ClearProcessors removes all processors from the cluster
func (ic *InternalCluster) ClearProcessors() {
	ic.mu.Lock()
	defer ic.mu.Unlock()
	ic.processors = nil
}