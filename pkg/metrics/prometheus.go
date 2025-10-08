package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"sync"
	"time"
)

// PrometheusExporter exports metrics to Prometheus format
type PrometheusExporter struct {
	mu               sync.RWMutex
	metricsCollector *MetricsCollector

	// Prometheus metrics
	lagGauge         *prometheus.GaugeVec
	throughputGauge  *prometheus.GaugeVec
	errorCounter     *prometheus.CounterVec
	healthGauge      *prometheus.GaugeVec
	uptimeGauge      prometheus.Gauge
	syncStatusGauge  *prometheus.GaugeVec
	clientCountGauge prometheus.Gauge
}

// NewPrometheusExporter creates a new Prometheus exporter
func NewPrometheusExporter(metricsCollector *MetricsCollector) *PrometheusExporter {
	exporter := &PrometheusExporter{
		metricsCollector: metricsCollector,
	}

	// Initialize Prometheus metrics
	exporter.lagGauge = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "mongodb_sync_lag_seconds",
			Help: "MongoDB synchronization lag in seconds",
		},
		[]string{"database", "collection", "type"},
	)

	exporter.throughputGauge = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "mongodb_sync_throughput",
			Help: "MongoDB synchronization throughput",
		},
		[]string{"database", "collection", "metric"},
	)

	exporter.errorCounter = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "mongodb_sync_errors_total",
			Help: "Total number of MongoDB synchronization errors",
		},
		[]string{"database", "collection", "error_type"},
	)

	exporter.healthGauge = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "mongodb_sync_health_status",
			Help: "MongoDB synchronization health status (1=healthy, 0=unhealthy)",
		},
		[]string{"database", "collection", "component"},
	)

	exporter.uptimeGauge = promauto.NewGauge(
		prometheus.GaugeOpts{
			Name: "mongodb_sync_uptime_seconds",
			Help: "MongoDB synchronization service uptime in seconds",
		},
	)

	exporter.syncStatusGauge = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "mongodb_sync_status",
			Help: "MongoDB synchronization status (1=syncing, 0=not syncing)",
		},
		[]string{"database", "collection", "client_id"},
	)

	exporter.clientCountGauge = promauto.NewGauge(
		prometheus.GaugeOpts{
			Name: "mongodb_sync_connected_clients",
			Help: "Number of connected MongoDB sync clients",
		},
	)

	return exporter
}

// UpdateMetrics updates Prometheus metrics from the metrics collector
func (pe *PrometheusExporter) UpdateMetrics() {
	pe.mu.Lock()
	defer pe.mu.Unlock()

	metrics := pe.metricsCollector.GetMetrics()

	// Update lag metrics
	if lagMetrics, ok := metrics["lag_metrics"].(map[string]*LagMetrics); ok {
		for key, lag := range lagMetrics {
			parts := splitKey(key)
			if len(parts) == 2 {
				database, collection := parts[0], parts[1]
				pe.lagGauge.WithLabelValues(database, collection, "replication").Set(lag.ReplicationLag.Seconds())
				pe.lagGauge.WithLabelValues(database, collection, "event_processing").Set(lag.EventProcessingLag.Seconds())
			}
		}
	}

	// Update throughput metrics
	if throughputMetrics, ok := metrics["throughput_metrics"].(map[string]*ThroughputMetrics); ok {
		for key, throughput := range throughputMetrics {
			parts := splitKey(key)
			if len(parts) == 2 {
				database, collection := parts[0], parts[1]
				pe.throughputGauge.WithLabelValues(database, collection, "events_per_second").Set(throughput.EventsPerSecond)
				pe.throughputGauge.WithLabelValues(database, collection, "bytes_per_second").Set(throughput.BytesPerSecond)
				pe.throughputGauge.WithLabelValues(database, collection, "docs_per_second").Set(throughput.DocsPerSecond)
				pe.throughputGauge.WithLabelValues(database, collection, "batches_per_second").Set(throughput.BatchesPerSecond)
			}
		}
	}

	// Update error metrics
	if errorMetrics, ok := metrics["error_metrics"].(map[string]*ErrorMetrics); ok {
		for _, errorMetric := range errorMetrics {
			pe.errorCounter.WithLabelValues(errorMetric.Database, errorMetric.Collection, errorMetric.ErrorType).Add(float64(errorMetric.ErrorCount))
		}
	}

	// Update health metrics
	if healthMetrics, ok := metrics["health_metrics"].(map[string]*HealthMetrics); ok {
		for key, health := range healthMetrics {
			parts := splitKey(key)
			if len(parts) == 2 {
				database, collection := parts[0], parts[1]
				healthValue := 0.0
				if health.Status == "healthy" {
					healthValue = 1.0
				}
				pe.healthGauge.WithLabelValues(database, collection, health.Component).Set(healthValue)
			}
		}
	}

	// Update uptime
	if uptime, ok := metrics["uptime"].(time.Duration); ok {
		pe.uptimeGauge.Set(uptime.Seconds())
	}
}

// SetConnectedClients sets the number of connected clients
func (pe *PrometheusExporter) SetConnectedClients(count int) {
	pe.clientCountGauge.Set(float64(count))
}

// SetSyncStatus sets the sync status for a specific database/collection/client
func (pe *PrometheusExporter) SetSyncStatus(database, collection, clientID string, syncing bool) {
	syncValue := 0.0
	if syncing {
		syncValue = 1.0
	}
	pe.syncStatusGauge.WithLabelValues(database, collection, clientID).Set(syncValue)
}

// StartMetricsUpdater starts a goroutine that periodically updates Prometheus metrics
func (pe *PrometheusExporter) StartMetricsUpdater(interval time.Duration) {
	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()

		for range ticker.C {
			pe.UpdateMetrics()
		}
	}()
}
