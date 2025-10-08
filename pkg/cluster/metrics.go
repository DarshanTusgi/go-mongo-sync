package cluster

import (
	"go-data-sync-http/pkg/models"
	"sync"
	"time"
)

// MetricsCollector collects and manages cluster metrics
type MetricsCollector struct {
	mu      sync.RWMutex
	metrics map[string]MetricValue
	config  models.MetricsConfig
	running bool
	stopCh  chan struct{}
}

// MetricValue represents a metric with timestamp
type MetricValue struct {
	Value     float64   `json:"value"`
	Timestamp time.Time `json:"timestamp"`
}

// MetricsStats holds aggregated metrics statistics
type MetricsStats struct {
	TotalMetrics   int                    `json:"total_metrics"`
	LastCollection time.Time              `json:"last_collection"`
	Metrics        map[string]MetricValue `json:"metrics"`
}

// NewMetricsCollector creates a new metrics collector
func NewMetricsCollector(config models.MetricsConfig) *MetricsCollector {
	if config.CollectionInterval <= 0 {
		config.CollectionInterval = 30 * time.Second
	}
	if config.RetentionPeriod <= 0 {
		config.RetentionPeriod = 24 * time.Hour
	}

	return &MetricsCollector{
		metrics: make(map[string]MetricValue),
		config:  config,
		stopCh:  make(chan struct{}),
	}
}

// Start starts the metrics collector
func (mc *MetricsCollector) Start() {
	mc.mu.Lock()
	defer mc.mu.Unlock()

	if mc.running {
		return
	}

	mc.running = true
	go mc.cleanupLoop()
}

// Stop stops the metrics collector
func (mc *MetricsCollector) Stop() {
	mc.mu.Lock()
	defer mc.mu.Unlock()

	if !mc.running {
		return
	}

	mc.running = false
	close(mc.stopCh)
}

// RecordMetric records a metric value
func (mc *MetricsCollector) RecordMetric(name string, value float64) {
	mc.mu.Lock()
	defer mc.mu.Unlock()

	mc.metrics[name] = MetricValue{
		Value:     value,
		Timestamp: time.Now(),
	}
}

// GetMetric gets a specific metric value
func (mc *MetricsCollector) GetMetric(name string) (MetricValue, bool) {
	mc.mu.RLock()
	defer mc.mu.RUnlock()

	value, exists := mc.metrics[name]
	return value, exists
}

// GetStats returns all metrics statistics
func (mc *MetricsCollector) GetStats() MetricsStats {
	mc.mu.RLock()
	defer mc.mu.RUnlock()

	// Copy metrics map
	metricsCopy := make(map[string]MetricValue)
	var lastCollection time.Time

	for name, value := range mc.metrics {
		metricsCopy[name] = value
		if value.Timestamp.After(lastCollection) {
			lastCollection = value.Timestamp
		}
	}

	return MetricsStats{
		TotalMetrics:   len(mc.metrics),
		LastCollection: lastCollection,
		Metrics:        metricsCopy,
	}
}

// cleanupLoop periodically cleans up old metrics
func (mc *MetricsCollector) cleanupLoop() {
	ticker := time.NewTicker(mc.config.CollectionInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			mc.cleanup()
		case <-mc.stopCh:
			return
		}
	}
}

// cleanup removes old metrics based on retention period
func (mc *MetricsCollector) cleanup() {
	mc.mu.Lock()
	defer mc.mu.Unlock()

	cutoff := time.Now().Add(-mc.config.RetentionPeriod)

	for name, value := range mc.metrics {
		if value.Timestamp.Before(cutoff) {
			delete(mc.metrics, name)
		}
	}
}

// GetMetricNames returns all metric names
func (mc *MetricsCollector) GetMetricNames() []string {
	mc.mu.RLock()
	defer mc.mu.RUnlock()

	names := make([]string, 0, len(mc.metrics))
	for name := range mc.metrics {
		names = append(names, name)
	}
	return names
}

// ClearMetrics clears all metrics
func (mc *MetricsCollector) ClearMetrics() {
	mc.mu.Lock()
	defer mc.mu.Unlock()
	mc.metrics = make(map[string]MetricValue)
}

// IsRunning returns whether the metrics collector is running
func (mc *MetricsCollector) IsRunning() bool {
	mc.mu.RLock()
	defer mc.mu.RUnlock()
	return mc.running
}
