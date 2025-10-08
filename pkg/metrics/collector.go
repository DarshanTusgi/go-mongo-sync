package metrics

import (
	"sync"
	"time"
)

// MetricType represents different types of metrics
type MetricType string

const (
	MetricTypeLag        MetricType = "lag"
	MetricTypeThroughput MetricType = "throughput"
	MetricTypeHealth     MetricType = "health"
	MetricTypeError      MetricType = "error"
)

// Metric represents a single metric data point
type Metric struct {
	Type      MetricType             `json:"type" bson:"type"`
	Name      string                 `json:"name" bson:"name"`
	Value     float64                `json:"value" bson:"value"`
	Labels    map[string]string      `json:"labels" bson:"labels"`
	Timestamp time.Time              `json:"timestamp" bson:"timestamp"`
	Metadata  map[string]interface{} `json:"metadata,omitempty" bson:"metadata,omitempty"`
}

// LagMetrics tracks synchronization lag
type LagMetrics struct {
	Database           string        `json:"database"`
	Collection         string        `json:"collection"`
	ReplicationLag     time.Duration `json:"replication_lag"`
	EventProcessingLag time.Duration `json:"event_processing_lag"`
	LastEventTime      time.Time     `json:"last_event_time"`
	LastProcessedTime  time.Time     `json:"last_processed_time"`
}

// ThroughputMetrics tracks data transfer rates
type ThroughputMetrics struct {
	Database         string  `json:"database"`
	Collection       string  `json:"collection"`
	EventsPerSecond  float64 `json:"events_per_second"`
	BytesPerSecond   float64 `json:"bytes_per_second"`
	DocsPerSecond    float64 `json:"docs_per_second"`
	BatchesPerSecond float64 `json:"batches_per_second"`
}

// HealthMetrics tracks operational health
type HealthMetrics struct {
	Component         string        `json:"component"`
	Status            string        `json:"status"` // healthy, degraded, unhealthy
	Uptime            time.Duration `json:"uptime"`
	LastHealthCheck   time.Time     `json:"last_health_check"`
	ConnectedClients  int           `json:"connected_clients"`
	ActiveConnections int           `json:"active_connections"`
	MemoryUsage       int64         `json:"memory_usage"`
	CPUUsage          float64       `json:"cpu_usage"`
}

// ErrorMetrics tracks error rates and types
type ErrorMetrics struct {
	Database      string                 `json:"database"`
	Collection    string                 `json:"collection"`
	ErrorType     string                 `json:"error_type"`
	ErrorCount    int64                  `json:"error_count"`
	ErrorRate     float64                `json:"error_rate"`
	LastError     string                 `json:"last_error"`
	LastErrorTime time.Time              `json:"last_error_time"`
	ErrorDetails  map[string]interface{} `json:"error_details,omitempty"`
}

// MetricsCollector collects and aggregates metrics
type MetricsCollector struct {
	mu                sync.RWMutex
	lagMetrics        map[string]*LagMetrics
	throughputMetrics map[string]*ThroughputMetrics
	healthMetrics     map[string]*HealthMetrics
	errorMetrics      map[string]*ErrorMetrics
	metricHistory     []Metric
	maxHistorySize    int
	startTime         time.Time

	// Event counters for throughput calculation
	eventCounters map[string]*EventCounter
	bytesCounters map[string]*BytesCounter
	docsCounters  map[string]*DocsCounter
	batchCounters map[string]*BatchCounter

	// External metrics
	activeWatchersCount int
}

// EventCounter tracks event counts over time windows
type EventCounter struct {
	mu           sync.Mutex
	totalEvents  int64
	recentEvents []timestampedCount
	windowSize   time.Duration
}

// BytesCounter tracks bytes transferred over time windows
type BytesCounter struct {
	mu          sync.Mutex
	totalBytes  int64
	recentBytes []timestampedBytes
	windowSize  time.Duration
}

// DocsCounter tracks documents processed over time windows
type DocsCounter struct {
	mu         sync.Mutex
	totalDocs  int64
	recentDocs []timestampedCount
	windowSize time.Duration
}

// BatchCounter tracks batches processed over time windows
type BatchCounter struct {
	mu            sync.Mutex
	totalBatches  int64
	recentBatches []timestampedCount
	windowSize    time.Duration
}

type timestampedCount struct {
	count     int64
	timestamp time.Time
}

type timestampedBytes struct {
	bytes     int64
	timestamp time.Time
}

// NewMetricsCollector creates a new metrics collector
func NewMetricsCollector(maxHistorySize int) *MetricsCollector {
	return &MetricsCollector{
		lagMetrics:        make(map[string]*LagMetrics),
		throughputMetrics: make(map[string]*ThroughputMetrics),
		healthMetrics:     make(map[string]*HealthMetrics),
		errorMetrics:      make(map[string]*ErrorMetrics),
		metricHistory:     make([]Metric, 0, maxHistorySize),
		maxHistorySize:    maxHistorySize,
		startTime:         time.Now(),
		eventCounters:     make(map[string]*EventCounter),
		bytesCounters:     make(map[string]*BytesCounter),
		docsCounters:      make(map[string]*DocsCounter),
		batchCounters:     make(map[string]*BatchCounter),
	}
}

// RecordLag records replication lag metrics
func (mc *MetricsCollector) RecordLag(database, collection string, replicationLag, processingLag time.Duration) {
	mc.mu.Lock()
	defer mc.mu.Unlock()

	key := database + "." + collection
	mc.lagMetrics[key] = &LagMetrics{
		Database:           database,
		Collection:         collection,
		ReplicationLag:     replicationLag,
		EventProcessingLag: processingLag,
		LastEventTime:      time.Now().Add(-replicationLag),
		LastProcessedTime:  time.Now(),
	}

	// Add to history
	mc.addToHistory(Metric{
		Type:      MetricTypeLag,
		Name:      "replication_lag",
		Value:     replicationLag.Seconds(),
		Labels:    map[string]string{"database": database, "collection": collection},
		Timestamp: time.Now(),
	})
}

// RecordEvent records an event for throughput calculation
func (mc *MetricsCollector) RecordEvent(database, collection string, eventSize int64) {
	key := database + "." + collection

	// Update event counter
	mc.getOrCreateEventCounter(key).Increment()

	// Update bytes counter
	mc.getOrCreateBytesCounter(key).Add(eventSize)
}

// RecordBatch records a batch for throughput calculation
func (mc *MetricsCollector) RecordBatch(database, collection string, docCount int64) {
	key := database + "." + collection

	// Update batch counter
	mc.getOrCreateBatchCounter(key).Increment()

	// Update docs counter
	mc.getOrCreateDocsCounter(key).Add(docCount)
}

// RecordError records error metrics
func (mc *MetricsCollector) RecordError(database, collection, errorType, errorMsg string, details map[string]interface{}) {
	mc.mu.Lock()
	defer mc.mu.Unlock()

	key := database + "." + collection + "." + errorType
	if existing, exists := mc.errorMetrics[key]; exists {
		existing.ErrorCount++
		existing.LastError = errorMsg
		existing.LastErrorTime = time.Now()
		if details != nil {
			existing.ErrorDetails = details
		}
	} else {
		mc.errorMetrics[key] = &ErrorMetrics{
			Database:      database,
			Collection:    collection,
			ErrorType:     errorType,
			ErrorCount:    1,
			LastError:     errorMsg,
			LastErrorTime: time.Now(),
			ErrorDetails:  details,
		}
	}

	// Add to history
	mc.addToHistory(Metric{
		Type:      MetricTypeError,
		Name:      "error_count",
		Value:     1,
		Labels:    map[string]string{"database": database, "collection": collection, "error_type": errorType},
		Timestamp: time.Now(),
		Metadata:  map[string]interface{}{"error_message": errorMsg},
	})
}

// UpdateHealth updates health metrics for a component
func (mc *MetricsCollector) UpdateHealth(component, status string, connectedClients, activeConnections int, memoryUsage int64, cpuUsage float64) {
	mc.mu.Lock()
	defer mc.mu.Unlock()

	mc.healthMetrics[component] = &HealthMetrics{
		Component:         component,
		Status:            status,
		Uptime:            time.Since(mc.startTime),
		LastHealthCheck:   time.Now(),
		ConnectedClients:  connectedClients,
		ActiveConnections: activeConnections,
		MemoryUsage:       memoryUsage,
		CPUUsage:          cpuUsage,
	}

	// Add to history
	mc.addToHistory(Metric{
		Type:      MetricTypeHealth,
		Name:      "component_health",
		Value:     mc.healthStatusToValue(status),
		Labels:    map[string]string{"component": component},
		Timestamp: time.Now(),
		Metadata: map[string]interface{}{
			"connected_clients":  connectedClients,
			"active_connections": activeConnections,
			"memory_usage":       memoryUsage,
			"cpu_usage":          cpuUsage,
		},
	})
}

// SetActiveWatchersCount sets the count of active watchers
func (mc *MetricsCollector) SetActiveWatchersCount(count int) {
	mc.mu.Lock()
	defer mc.mu.Unlock()
	mc.activeWatchersCount = count
}

// CalculateThroughput calculates and updates throughput metrics
func (mc *MetricsCollector) CalculateThroughput() {
	mc.mu.Lock()
	defer mc.mu.Unlock()

	for key, eventCounter := range mc.eventCounters {
		parts := splitKey(key)
		if len(parts) != 2 {
			continue
		}
		database, collection := parts[0], parts[1]

		eventsPerSec := eventCounter.GetRate()
		bytesPerSec := mc.getBytesCounter(key).GetRate()
		docsPerSec := mc.getDocsCounter(key).GetRate()
		batchesPerSec := mc.getBatchCounter(key).GetRate()

		mc.throughputMetrics[key] = &ThroughputMetrics{
			Database:         database,
			Collection:       collection,
			EventsPerSecond:  eventsPerSec,
			BytesPerSecond:   bytesPerSec,
			DocsPerSecond:    docsPerSec,
			BatchesPerSecond: batchesPerSec,
		}

		// Add to history
		mc.addToHistory(Metric{
			Type:      MetricTypeThroughput,
			Name:      "events_per_second",
			Value:     eventsPerSec,
			Labels:    map[string]string{"database": database, "collection": collection},
			Timestamp: time.Now(),
		})
	}
}

// GetMetrics returns current metrics snapshot
func (mc *MetricsCollector) GetMetrics() map[string]interface{} {
	mc.mu.RLock()
	defer mc.mu.RUnlock()

	// Calculate aggregated dashboard metrics
	dashboardMetrics := mc.calculateDashboardMetrics()

	return map[string]interface{}{
		"lag_metrics":        mc.lagMetrics,
		"throughput_metrics": mc.throughputMetrics,
		"health_metrics":     mc.healthMetrics,
		"error_metrics":      mc.errorMetrics,
		"uptime":             time.Since(mc.startTime),
		"timestamp":          time.Now(),
		"dashboard_metrics":  dashboardMetrics,
	}
}

// calculateDashboardMetrics calculates aggregated metrics for the dashboard
func (mc *MetricsCollector) calculateDashboardMetrics() map[string]interface{} {
	totalDocs := int64(0)
	syncRate := float64(0)
	avgLatency := float64(0)
	backlogSize := int64(0)
	activeWatchers := 0

	// Aggregate throughput metrics
	activeWatchers = mc.activeWatchersCount
	for _, tm := range mc.throughputMetrics {
		if tm != nil {
			// Use events per second as a proxy for sync rate
			syncRate += tm.EventsPerSecond
		}
	}

	// Aggregate lag metrics for average latency
	lagCount := 0
	for _, lm := range mc.lagMetrics {
		if lm != nil {
			avgLatency += float64(lm.ReplicationLag.Milliseconds())
			lagCount++
		}
	}
	if lagCount > 0 {
		avgLatency = avgLatency / float64(lagCount)
	}

	// Calculate total documents from doc counters
	for _, dc := range mc.docsCounters {
		if dc != nil {
			totalDocs += dc.totalDocs
		}
	}

	// Estimate backlog size from error metrics (simplified)
	for _, em := range mc.errorMetrics {
		if em != nil {
			backlogSize += em.ErrorCount
		}
	}

	return map[string]interface{}{
		"total_documents": totalDocs,
		"sync_rate":       syncRate,
		"avg_latency":     avgLatency,
		"backlog_size":    backlogSize,
		"active_watchers": activeWatchers,
	}
}

// GetMetricHistory returns recent metric history
func (mc *MetricsCollector) GetMetricHistory(limit int) []Metric {
	mc.mu.RLock()
	defer mc.mu.RUnlock()

	if limit <= 0 || limit > len(mc.metricHistory) {
		limit = len(mc.metricHistory)
	}

	start := len(mc.metricHistory) - limit
	return mc.metricHistory[start:]
}

// Helper methods

func (mc *MetricsCollector) addToHistory(metric Metric) {
	if len(mc.metricHistory) >= mc.maxHistorySize {
		// Remove oldest metric
		mc.metricHistory = mc.metricHistory[1:]
	}
	mc.metricHistory = append(mc.metricHistory, metric)
}

func (mc *MetricsCollector) healthStatusToValue(status string) float64 {
	switch status {
	case "healthy":
		return 1.0
	case "degraded":
		return 0.5
	case "unhealthy":
		return 0.0
	default:
		return -1.0
	}
}

func (mc *MetricsCollector) getOrCreateEventCounter(key string) *EventCounter {
	if counter, exists := mc.eventCounters[key]; exists {
		return counter
	}
	mc.eventCounters[key] = &EventCounter{
		windowSize: 60 * time.Second, // 1 minute window
	}
	return mc.eventCounters[key]
}

func (mc *MetricsCollector) getOrCreateBytesCounter(key string) *BytesCounter {
	if counter, exists := mc.bytesCounters[key]; exists {
		return counter
	}
	mc.bytesCounters[key] = &BytesCounter{
		windowSize: 60 * time.Second,
	}
	return mc.bytesCounters[key]
}

func (mc *MetricsCollector) getOrCreateDocsCounter(key string) *DocsCounter {
	if counter, exists := mc.docsCounters[key]; exists {
		return counter
	}
	mc.docsCounters[key] = &DocsCounter{
		windowSize: 60 * time.Second,
	}
	return mc.docsCounters[key]
}

func (mc *MetricsCollector) getOrCreateBatchCounter(key string) *BatchCounter {
	if counter, exists := mc.batchCounters[key]; exists {
		return counter
	}
	mc.batchCounters[key] = &BatchCounter{
		windowSize: 60 * time.Second,
	}
	return mc.batchCounters[key]
}

func (mc *MetricsCollector) getBytesCounter(key string) *BytesCounter {
	if counter, exists := mc.bytesCounters[key]; exists {
		return counter
	}
	return &BytesCounter{windowSize: 60 * time.Second}
}

func (mc *MetricsCollector) getDocsCounter(key string) *DocsCounter {
	if counter, exists := mc.docsCounters[key]; exists {
		return counter
	}
	return &DocsCounter{windowSize: 60 * time.Second}
}

func (mc *MetricsCollector) getBatchCounter(key string) *BatchCounter {
	if counter, exists := mc.batchCounters[key]; exists {
		return counter
	}
	return &BatchCounter{windowSize: 60 * time.Second}
}

func splitKey(key string) []string {
	parts := make([]string, 0, 2)
	lastDot := -1
	for i := len(key) - 1; i >= 0; i-- {
		if key[i] == '.' {
			lastDot = i
			break
		}
	}
	if lastDot > 0 {
		parts = append(parts, key[:lastDot])
		parts = append(parts, key[lastDot+1:])
	}
	return parts
}

// Counter methods

func (ec *EventCounter) Increment() {
	ec.mu.Lock()
	defer ec.mu.Unlock()

	ec.totalEvents++
	now := time.Now()
	ec.recentEvents = append(ec.recentEvents, timestampedCount{count: 1, timestamp: now})
	ec.cleanupOldEvents(now)
}

func (ec *EventCounter) GetRate() float64 {
	ec.mu.Lock()
	defer ec.mu.Unlock()

	now := time.Now()
	ec.cleanupOldEvents(now)

	var totalCount int64
	for _, event := range ec.recentEvents {
		totalCount += event.count
	}

	return float64(totalCount) / ec.windowSize.Seconds()
}

func (ec *EventCounter) cleanupOldEvents(now time.Time) {
	cutoff := now.Add(-ec.windowSize)
	for i, event := range ec.recentEvents {
		if event.timestamp.After(cutoff) {
			ec.recentEvents = ec.recentEvents[i:]
			return
		}
	}
	ec.recentEvents = ec.recentEvents[:0]
}

func (bc *BytesCounter) Add(bytes int64) {
	bc.mu.Lock()
	defer bc.mu.Unlock()

	bc.totalBytes += bytes
	now := time.Now()
	bc.recentBytes = append(bc.recentBytes, timestampedBytes{bytes: bytes, timestamp: now})
	bc.cleanupOldBytes(now)
}

func (bc *BytesCounter) GetRate() float64 {
	bc.mu.Lock()
	defer bc.mu.Unlock()

	now := time.Now()
	bc.cleanupOldBytes(now)

	var totalBytes int64
	for _, entry := range bc.recentBytes {
		totalBytes += entry.bytes
	}

	return float64(totalBytes) / bc.windowSize.Seconds()
}

func (bc *BytesCounter) cleanupOldBytes(now time.Time) {
	cutoff := now.Add(-bc.windowSize)
	for i, entry := range bc.recentBytes {
		if entry.timestamp.After(cutoff) {
			bc.recentBytes = bc.recentBytes[i:]
			return
		}
	}
	bc.recentBytes = bc.recentBytes[:0]
}

func (dc *DocsCounter) Add(docs int64) {
	dc.mu.Lock()
	defer dc.mu.Unlock()

	dc.totalDocs += docs
	now := time.Now()
	dc.recentDocs = append(dc.recentDocs, timestampedCount{count: docs, timestamp: now})
	dc.cleanupOldDocs(now)
}

func (dc *DocsCounter) GetRate() float64 {
	dc.mu.Lock()
	defer dc.mu.Unlock()

	now := time.Now()
	dc.cleanupOldDocs(now)

	var totalDocs int64
	for _, entry := range dc.recentDocs {
		totalDocs += entry.count
	}

	return float64(totalDocs) / dc.windowSize.Seconds()
}

func (dc *DocsCounter) cleanupOldDocs(now time.Time) {
	cutoff := now.Add(-dc.windowSize)
	for i, entry := range dc.recentDocs {
		if entry.timestamp.After(cutoff) {
			dc.recentDocs = dc.recentDocs[i:]
			return
		}
	}
	dc.recentDocs = dc.recentDocs[:0]
}

func (bc *BatchCounter) Increment() {
	bc.mu.Lock()
	defer bc.mu.Unlock()

	bc.totalBatches++
	now := time.Now()
	bc.recentBatches = append(bc.recentBatches, timestampedCount{count: 1, timestamp: now})
	bc.cleanupOldBatches(now)
}

func (bc *BatchCounter) GetRate() float64 {
	bc.mu.Lock()
	defer bc.mu.Unlock()

	now := time.Now()
	bc.cleanupOldBatches(now)

	var totalBatches int64
	for _, entry := range bc.recentBatches {
		totalBatches += entry.count
	}

	return float64(totalBatches) / bc.windowSize.Seconds()
}

func (bc *BatchCounter) cleanupOldBatches(now time.Time) {
	cutoff := now.Add(-bc.windowSize)
	for i, entry := range bc.recentBatches {
		if entry.timestamp.After(cutoff) {
			bc.recentBatches = bc.recentBatches[i:]
			return
		}
	}
	bc.recentBatches = bc.recentBatches[:0]
}
