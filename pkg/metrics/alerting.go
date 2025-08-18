package metrics

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"
)

// AlertLevel represents the severity of an alert
type AlertLevel string

const (
	AlertLevelInfo     AlertLevel = "info"
	AlertLevelWarning  AlertLevel = "warning"
	AlertLevelCritical AlertLevel = "critical"
)

// Alert represents a system alert
type Alert struct {
	ID          string                 `json:"id"`
	Level       AlertLevel             `json:"level"`
	Title       string                 `json:"title"`
	Message     string                 `json:"message"`
	Component   string                 `json:"component"`
	Database    string                 `json:"database,omitempty"`
	Collection  string                 `json:"collection,omitempty"`
	MetricType  MetricType             `json:"metric_type"`
	MetricValue float64                `json:"metric_value"`
	Threshold   float64                `json:"threshold"`
	Timestamp   time.Time              `json:"timestamp"`
	Resolved    bool                   `json:"resolved"`
	ResolvedAt  *time.Time             `json:"resolved_at,omitempty"`
	Metadata    map[string]interface{} `json:"metadata,omitempty"`
}

// AlertRule defines conditions for triggering alerts
type AlertRule struct {
	ID          string     `json:"id"`
	Name        string     `json:"name"`
	Description string     `json:"description"`
	MetricType  MetricType `json:"metric_type"`
	MetricName  string     `json:"metric_name"`
	Condition   string     `json:"condition"` // "gt", "lt", "eq", "gte", "lte"
	Threshold   float64    `json:"threshold"`
	Level       AlertLevel `json:"level"`
	Enabled     bool       `json:"enabled"`
	Cooldown    time.Duration `json:"cooldown"` // Minimum time between alerts
	Component   string     `json:"component,omitempty"`
	Database    string     `json:"database,omitempty"`
	Collection  string     `json:"collection,omitempty"`
}

// AlertHandler defines how alerts should be handled
type AlertHandler interface {
	HandleAlert(ctx context.Context, alert *Alert) error
}

// LogAlertHandler logs alerts to the standard logger
type LogAlertHandler struct{}

func (h *LogAlertHandler) HandleAlert(ctx context.Context, alert *Alert) error {
	log.Printf("[ALERT-%s] %s: %s (Value: %.2f, Threshold: %.2f)",
		alert.Level, alert.Title, alert.Message, alert.MetricValue, alert.Threshold)
	return nil
}

// AlertManager manages alert rules and handles alert generation
type AlertManager struct {
	mu               sync.RWMutex
	rules            map[string]*AlertRule
	activeAlerts     map[string]*Alert
	alertHistory     []*Alert
	maxHistorySize   int
	handlers         []AlertHandler
	lastAlertTime    map[string]time.Time // For cooldown tracking
	metricsCollector *MetricsCollector
	stopChan         chan struct{}
	running          bool
}

// NewAlertManager creates a new alert manager
func NewAlertManager(metricsCollector *MetricsCollector, maxHistorySize int) *AlertManager {
	return &AlertManager{
		rules:            make(map[string]*AlertRule),
		activeAlerts:     make(map[string]*Alert),
		alertHistory:     make([]*Alert, 0, maxHistorySize),
		maxHistorySize:   maxHistorySize,
		handlers:         []AlertHandler{&LogAlertHandler{}},
		lastAlertTime:    make(map[string]time.Time),
		metricsCollector: metricsCollector,
		stopChan:         make(chan struct{}),
	}
}

// AddRule adds an alert rule
func (am *AlertManager) AddRule(rule *AlertRule) {
	am.mu.Lock()
	defer am.mu.Unlock()
	am.rules[rule.ID] = rule
}

// RemoveRule removes an alert rule
func (am *AlertManager) RemoveRule(ruleID string) {
	am.mu.Lock()
	defer am.mu.Unlock()
	delete(am.rules, ruleID)
}

// AddHandler adds an alert handler
func (am *AlertManager) AddHandler(handler AlertHandler) {
	am.mu.Lock()
	defer am.mu.Unlock()
	am.handlers = append(am.handlers, handler)
}

// Start begins monitoring metrics and generating alerts
func (am *AlertManager) Start(ctx context.Context, checkInterval time.Duration) {
	am.mu.Lock()
	if am.running {
		am.mu.Unlock()
		return
	}
	am.running = true
	am.mu.Unlock()

	ticker := time.NewTicker(checkInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-am.stopChan:
			return
		case <-ticker.C:
			am.checkAlerts(ctx)
		}
	}
}

// Stop stops the alert manager
func (am *AlertManager) Stop() {
	am.mu.Lock()
	defer am.mu.Unlock()

	if am.running {
		close(am.stopChan)
		am.running = false
	}
}

// checkAlerts evaluates all rules against current metrics
func (am *AlertManager) checkAlerts(ctx context.Context) {
	am.mu.RLock()
	rules := make([]*AlertRule, 0, len(am.rules))
	for _, rule := range am.rules {
		if rule.Enabled {
			rules = append(rules, rule)
		}
	}
	am.mu.RUnlock()

	metrics := am.metricsCollector.GetMetrics()

	for _, rule := range rules {
		am.evaluateRule(ctx, rule, metrics)
	}
}

// evaluateRule evaluates a single rule against metrics
func (am *AlertManager) evaluateRule(ctx context.Context, rule *AlertRule, metrics map[string]interface{}) {
	var metricValue float64
	var found bool

	// Extract metric value based on rule type
	switch rule.MetricType {
	case MetricTypeLag:
		if lagMetrics, ok := metrics["lag_metrics"].(map[string]*LagMetrics); ok {
			key := rule.Database + "." + rule.Collection
			if lag, exists := lagMetrics[key]; exists {
				switch rule.MetricName {
				case "replication_lag":
					metricValue = lag.ReplicationLag.Seconds()
					found = true
				case "event_processing_lag":
					metricValue = lag.EventProcessingLag.Seconds()
					found = true
				}
			}
		}

	case MetricTypeThroughput:
		if throughputMetrics, ok := metrics["throughput_metrics"].(map[string]*ThroughputMetrics); ok {
			key := rule.Database + "." + rule.Collection
			if throughput, exists := throughputMetrics[key]; exists {
				switch rule.MetricName {
				case "events_per_second":
					metricValue = throughput.EventsPerSecond
					found = true
				case "bytes_per_second":
					metricValue = throughput.BytesPerSecond
					found = true
				case "docs_per_second":
					metricValue = throughput.DocsPerSecond
					found = true
				case "batches_per_second":
					metricValue = throughput.BatchesPerSecond
					found = true
				}
			}
		}

	case MetricTypeHealth:
		if healthMetrics, ok := metrics["health_metrics"].(map[string]*HealthMetrics); ok {
			if health, exists := healthMetrics[rule.Component]; exists {
				switch rule.MetricName {
				case "connected_clients":
					metricValue = float64(health.ConnectedClients)
					found = true
				case "active_connections":
					metricValue = float64(health.ActiveConnections)
					found = true
				case "memory_usage":
					metricValue = float64(health.MemoryUsage)
					found = true
				case "cpu_usage":
					metricValue = health.CPUUsage
					found = true
				case "uptime":
					metricValue = health.Uptime.Seconds()
					found = true
				}
			}
		}

	case MetricTypeError:
		if errorMetrics, ok := metrics["error_metrics"].(map[string]*ErrorMetrics); ok {
			for _, errorMetric := range errorMetrics {
				if (rule.Database == "" || errorMetric.Database == rule.Database) &&
					(rule.Collection == "" || errorMetric.Collection == rule.Collection) {
					switch rule.MetricName {
					case "error_count":
						metricValue = float64(errorMetric.ErrorCount)
						found = true
					case "error_rate":
						metricValue = errorMetric.ErrorRate
						found = true
					}
					if found {
						break
					}
				}
			}
		}
	}

	if !found {
		return
	}

	// Check if condition is met
	conditionMet := am.evaluateCondition(rule.Condition, metricValue, rule.Threshold)

	// Generate alert ID
	alertID := fmt.Sprintf("%s_%s_%s", rule.ID, rule.Database, rule.Collection)

	am.mu.Lock()
	defer am.mu.Unlock()

	if conditionMet {
		// Check cooldown
		if lastAlert, exists := am.lastAlertTime[alertID]; exists {
			if time.Since(lastAlert) < rule.Cooldown {
				return // Still in cooldown
			}
		}

		// Create or update alert
		if _, exists := am.activeAlerts[alertID]; !exists {
			alert := &Alert{
				ID:          alertID,
				Level:       rule.Level,
				Title:       rule.Name,
				Message:     fmt.Sprintf("%s: %s %s %.2f (threshold: %.2f)", rule.Description, rule.MetricName, rule.Condition, metricValue, rule.Threshold),
				Component:   rule.Component,
				Database:    rule.Database,
				Collection:  rule.Collection,
				MetricType:  rule.MetricType,
				MetricValue: metricValue,
				Threshold:   rule.Threshold,
				Timestamp:   time.Now(),
				Resolved:    false,
			}

			am.activeAlerts[alertID] = alert
			am.addToHistory(alert)
			am.lastAlertTime[alertID] = time.Now()

			// Send alert to handlers
			go am.sendAlert(ctx, alert)
		}
	} else {
		// Resolve alert if it exists
		if alert, exists := am.activeAlerts[alertID]; exists && !alert.Resolved {
			now := time.Now()
			alert.Resolved = true
			alert.ResolvedAt = &now
			alert.Message += " [RESOLVED]"

			// Send resolved alert to handlers
			go am.sendAlert(ctx, alert)

			delete(am.activeAlerts, alertID)
		}
	}
}

// evaluateCondition evaluates a condition against a metric value
func (am *AlertManager) evaluateCondition(condition string, value, threshold float64) bool {
	switch condition {
	case "gt":
		return value > threshold
	case "gte":
		return value >= threshold
	case "lt":
		return value < threshold
	case "lte":
		return value <= threshold
	case "eq":
		return value == threshold
	default:
		return false
	}
}

// sendAlert sends an alert to all handlers
func (am *AlertManager) sendAlert(ctx context.Context, alert *Alert) {
	for _, handler := range am.handlers {
		if err := handler.HandleAlert(ctx, alert); err != nil {
			log.Printf("Failed to handle alert %s: %v", alert.ID, err)
		}
	}
}

// addToHistory adds an alert to the history
func (am *AlertManager) addToHistory(alert *Alert) {
	if len(am.alertHistory) >= am.maxHistorySize {
		// Remove oldest alert
		am.alertHistory = am.alertHistory[1:]
	}
	am.alertHistory = append(am.alertHistory, alert)
}

// GetActiveAlerts returns currently active alerts
func (am *AlertManager) GetActiveAlerts() []*Alert {
	am.mu.RLock()
	defer am.mu.RUnlock()

	alerts := make([]*Alert, 0, len(am.activeAlerts))
	for _, alert := range am.activeAlerts {
		alerts = append(alerts, alert)
	}
	return alerts
}

// GetAlertHistory returns recent alert history
func (am *AlertManager) GetAlertHistory(limit int) []*Alert {
	am.mu.RLock()
	defer am.mu.RUnlock()

	if limit <= 0 || limit > len(am.alertHistory) {
		limit = len(am.alertHistory)
	}

	start := len(am.alertHistory) - limit
	return am.alertHistory[start:]
}

// GetRules returns all alert rules
func (am *AlertManager) GetRules() []*AlertRule {
	am.mu.RLock()
	defer am.mu.RUnlock()

	rules := make([]*AlertRule, 0, len(am.rules))
	for _, rule := range am.rules {
		rules = append(rules, rule)
	}
	return rules
}

// DefaultAlertRules returns a set of default alert rules
func DefaultAlertRules() []*AlertRule {
	return []*AlertRule{
		{
			ID:          "high_replication_lag",
			Name:        "High Replication Lag",
			Description: "Replication lag is too high",
			MetricType:  MetricTypeLag,
			MetricName:  "replication_lag",
			Condition:   "gt",
			Threshold:   30.0, // 30 seconds
			Level:       AlertLevelWarning,
			Enabled:     true,
			Cooldown:    5 * time.Minute,
		},
		{
			ID:          "critical_replication_lag",
			Name:        "Critical Replication Lag",
			Description: "Replication lag is critically high",
			MetricType:  MetricTypeLag,
			MetricName:  "replication_lag",
			Condition:   "gt",
			Threshold:   120.0, // 2 minutes
			Level:       AlertLevelCritical,
			Enabled:     true,
			Cooldown:    2 * time.Minute,
		},
		{
			ID:          "low_throughput",
			Name:        "Low Throughput",
			Description: "Event throughput is too low",
			MetricType:  MetricTypeThroughput,
			MetricName:  "events_per_second",
			Condition:   "lt",
			Threshold:   1.0, // Less than 1 event per second
			Level:       AlertLevelWarning,
			Enabled:     true,
			Cooldown:    10 * time.Minute,
		},
		{
			ID:          "high_error_rate",
			Name:        "High Error Rate",
			Description: "Error rate is too high",
			MetricType:  MetricTypeError,
			MetricName:  "error_count",
			Condition:   "gt",
			Threshold:   10.0, // More than 10 errors
			Level:       AlertLevelCritical,
			Enabled:     true,
			Cooldown:    5 * time.Minute,
		},
		{
			ID:          "component_unhealthy",
			Name:        "Component Unhealthy",
			Description: "Component health status is unhealthy",
			MetricType:  MetricTypeHealth,
			MetricName:  "component_health",
			Condition:   "lt",
			Threshold:   0.5, // Less than 0.5 (degraded or unhealthy)
			Level:       AlertLevelCritical,
			Enabled:     true,
			Cooldown:    1 * time.Minute,
		},
		{
			ID:          "high_memory_usage",
			Name:        "High Memory Usage",
			Description: "Memory usage is too high",
			MetricType:  MetricTypeHealth,
			MetricName:  "memory_usage",
			Condition:   "gt",
			Threshold:   1024 * 1024 * 1024, // 1GB
			Level:       AlertLevelWarning,
			Enabled:     true,
			Cooldown:    10 * time.Minute,
		},
	}
}