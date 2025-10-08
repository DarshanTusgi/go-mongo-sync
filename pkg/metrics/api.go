package metrics

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/gorilla/mux"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// MetricsAPI provides HTTP endpoints for metrics and alerts
type MetricsAPI struct {
	metricsCollector   *MetricsCollector
	alertManager       *AlertManager
	prometheusExporter *PrometheusExporter
}

// NewMetricsAPI creates a new metrics API
func NewMetricsAPI(metricsCollector *MetricsCollector, alertManager *AlertManager) *MetricsAPI {
	prometheusExporter := NewPrometheusExporter(metricsCollector)
	prometheusExporter.StartMetricsUpdater(time.Second * 10) // Update every 10 seconds

	return &MetricsAPI{
		metricsCollector:   metricsCollector,
		alertManager:       alertManager,
		prometheusExporter: prometheusExporter,
	}
}

// RegisterRoutes registers metrics API routes
func (api *MetricsAPI) RegisterRoutes(router *mux.Router) {
	// Prometheus metrics endpoint
	router.Handle("/metrics", promhttp.Handler()).Methods("GET")

	// JSON Metrics endpoints
	router.HandleFunc("/api/metrics", api.handleGetMetrics).Methods("GET")
	router.HandleFunc("/api/metrics/lag", api.handleGetLagMetrics).Methods("GET")
	router.HandleFunc("/api/metrics/throughput", api.handleGetThroughputMetrics).Methods("GET")
	router.HandleFunc("/api/metrics/health", api.handleGetHealthMetrics).Methods("GET")
	router.HandleFunc("/api/metrics/errors", api.handleGetErrorMetrics).Methods("GET")

	// Alert endpoints
	router.HandleFunc("/api/alerts", api.handleGetActiveAlerts).Methods("GET")
	router.HandleFunc("/api/alerts/history", api.handleGetAlertHistory).Methods("GET")
	router.HandleFunc("/api/alerts/rules", api.handleGetAlertRules).Methods("GET")
	router.HandleFunc("/api/alerts/rules", api.handleCreateAlertRule).Methods("POST")
	router.HandleFunc("/api/alerts/rules/{id}", api.handleUpdateAlertRule).Methods("PUT")
	router.HandleFunc("/api/alerts/rules/{id}", api.handleDeleteAlertRule).Methods("DELETE")

	// Health check endpoint
	router.HandleFunc("/api/health", api.handleHealthCheck).Methods("GET")
}

// handleGetMetrics returns all metrics
func (api *MetricsAPI) handleGetMetrics(w http.ResponseWriter, r *http.Request) {
	metrics := api.metricsCollector.GetMetrics()

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(metrics); err != nil {
		http.Error(w, fmt.Sprintf("Failed to encode metrics: %v", err), http.StatusInternalServerError)
		return
	}
}

// handleGetLagMetrics returns lag metrics
func (api *MetricsAPI) handleGetLagMetrics(w http.ResponseWriter, r *http.Request) {
	metrics := api.metricsCollector.GetMetrics()
	lagMetrics := metrics["lag_metrics"]

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(lagMetrics); err != nil {
		http.Error(w, fmt.Sprintf("Failed to encode lag metrics: %v", err), http.StatusInternalServerError)
		return
	}
}

// handleGetThroughputMetrics returns throughput metrics
func (api *MetricsAPI) handleGetThroughputMetrics(w http.ResponseWriter, r *http.Request) {
	metrics := api.metricsCollector.GetMetrics()
	throughputMetrics := metrics["throughput_metrics"]

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(throughputMetrics); err != nil {
		http.Error(w, fmt.Sprintf("Failed to encode throughput metrics: %v", err), http.StatusInternalServerError)
		return
	}
}

// handleGetHealthMetrics returns health metrics
func (api *MetricsAPI) handleGetHealthMetrics(w http.ResponseWriter, r *http.Request) {
	metrics := api.metricsCollector.GetMetrics()
	healthMetrics := metrics["health_metrics"]

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(healthMetrics); err != nil {
		http.Error(w, fmt.Sprintf("Failed to encode health metrics: %v", err), http.StatusInternalServerError)
		return
	}
}

// handleGetErrorMetrics returns error metrics
func (api *MetricsAPI) handleGetErrorMetrics(w http.ResponseWriter, r *http.Request) {
	metrics := api.metricsCollector.GetMetrics()
	errorMetrics := metrics["error_metrics"]

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(errorMetrics); err != nil {
		http.Error(w, fmt.Sprintf("Failed to encode error metrics: %v", err), http.StatusInternalServerError)
		return
	}
}

// handleGetActiveAlerts returns currently active alerts
func (api *MetricsAPI) handleGetActiveAlerts(w http.ResponseWriter, r *http.Request) {
	alerts := api.alertManager.GetActiveAlerts()

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(map[string]interface{}{
		"alerts": alerts,
		"count":  len(alerts),
	}); err != nil {
		http.Error(w, fmt.Sprintf("Failed to encode alerts: %v", err), http.StatusInternalServerError)
		return
	}
}

// handleGetAlertHistory returns alert history
func (api *MetricsAPI) handleGetAlertHistory(w http.ResponseWriter, r *http.Request) {
	limitStr := r.URL.Query().Get("limit")
	limit := 50 // Default limit

	if limitStr != "" {
		if parsedLimit, err := strconv.Atoi(limitStr); err == nil && parsedLimit > 0 {
			limit = parsedLimit
		}
	}

	alerts := api.alertManager.GetAlertHistory(limit)

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(map[string]interface{}{
		"alerts": alerts,
		"count":  len(alerts),
		"limit":  limit,
	}); err != nil {
		http.Error(w, fmt.Sprintf("Failed to encode alert history: %v", err), http.StatusInternalServerError)
		return
	}
}

// handleGetAlertRules returns all alert rules
func (api *MetricsAPI) handleGetAlertRules(w http.ResponseWriter, r *http.Request) {
	rules := api.alertManager.GetRules()

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(map[string]interface{}{
		"rules": rules,
		"count": len(rules),
	}); err != nil {
		http.Error(w, fmt.Sprintf("Failed to encode alert rules: %v", err), http.StatusInternalServerError)
		return
	}
}

// handleCreateAlertRule creates a new alert rule
func (api *MetricsAPI) handleCreateAlertRule(w http.ResponseWriter, r *http.Request) {
	var rule AlertRule
	if err := json.NewDecoder(r.Body).Decode(&rule); err != nil {
		http.Error(w, fmt.Sprintf("Invalid request body: %v", err), http.StatusBadRequest)
		return
	}

	// Validate rule
	if err := api.validateAlertRule(&rule); err != nil {
		http.Error(w, fmt.Sprintf("Invalid alert rule: %v", err), http.StatusBadRequest)
		return
	}

	// Generate ID if not provided
	if rule.ID == "" {
		rule.ID = fmt.Sprintf("%s_%d", strings.ReplaceAll(rule.Name, " ", "_"), time.Now().Unix())
	}

	api.alertManager.AddRule(&rule)

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	if err := json.NewEncoder(w).Encode(map[string]interface{}{
		"message": "Alert rule created successfully",
		"rule_id": rule.ID,
	}); err != nil {
		http.Error(w, fmt.Sprintf("Failed to encode response: %v", err), http.StatusInternalServerError)
		return
	}
}

// handleUpdateAlertRule updates an existing alert rule
func (api *MetricsAPI) handleUpdateAlertRule(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	ruleID := vars["id"]

	var rule AlertRule
	if err := json.NewDecoder(r.Body).Decode(&rule); err != nil {
		http.Error(w, fmt.Sprintf("Invalid request body: %v", err), http.StatusBadRequest)
		return
	}

	// Validate rule
	if err := api.validateAlertRule(&rule); err != nil {
		http.Error(w, fmt.Sprintf("Invalid alert rule: %v", err), http.StatusBadRequest)
		return
	}

	// Set the ID from URL
	rule.ID = ruleID

	api.alertManager.AddRule(&rule) // AddRule will overwrite existing rule with same ID

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(map[string]interface{}{
		"message": "Alert rule updated successfully",
		"rule_id": rule.ID,
	}); err != nil {
		http.Error(w, fmt.Sprintf("Failed to encode response: %v", err), http.StatusInternalServerError)
		return
	}
}

// handleDeleteAlertRule deletes an alert rule
func (api *MetricsAPI) handleDeleteAlertRule(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	ruleID := vars["id"]

	api.alertManager.RemoveRule(ruleID)

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(map[string]interface{}{
		"message": "Alert rule deleted successfully",
		"rule_id": ruleID,
	}); err != nil {
		http.Error(w, fmt.Sprintf("Failed to encode response: %v", err), http.StatusInternalServerError)
		return
	}
}

// handleHealthCheck returns API health status
func (api *MetricsAPI) handleHealthCheck(w http.ResponseWriter, r *http.Request) {
	health := map[string]interface{}{
		"status":    "healthy",
		"timestamp": time.Now(),
		"version":   "1.0.0",
		"uptime":    time.Since(time.Now()), // This would be calculated from service start time
	}

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(health); err != nil {
		http.Error(w, fmt.Sprintf("Failed to encode health status: %v", err), http.StatusInternalServerError)
		return
	}
}

// validateAlertRule validates an alert rule
func (api *MetricsAPI) validateAlertRule(rule *AlertRule) error {
	if rule.Name == "" {
		return fmt.Errorf("rule name is required")
	}

	if rule.MetricType == "" {
		return fmt.Errorf("metric type is required")
	}

	if rule.MetricName == "" {
		return fmt.Errorf("metric name is required")
	}

	if rule.Condition == "" {
		return fmt.Errorf("condition is required")
	}

	// Validate condition
	validConditions := []string{"gt", "gte", "lt", "lte", "eq"}
	validCondition := false
	for _, validCond := range validConditions {
		if rule.Condition == validCond {
			validCondition = true
			break
		}
	}
	if !validCondition {
		return fmt.Errorf("invalid condition: %s. Valid conditions are: %v", rule.Condition, validConditions)
	}

	// Validate alert level
	validLevels := []AlertLevel{AlertLevelInfo, AlertLevelWarning, AlertLevelCritical}
	validLevel := false
	for _, validLvl := range validLevels {
		if rule.Level == validLvl {
			validLevel = true
			break
		}
	}
	if !validLevel {
		return fmt.Errorf("invalid alert level: %s. Valid levels are: %v", rule.Level, validLevels)
	}

	// Validate metric type
	validMetricTypes := []MetricType{MetricTypeLag, MetricTypeThroughput, MetricTypeHealth, MetricTypeError}
	validMetricType := false
	for _, validType := range validMetricTypes {
		if rule.MetricType == validType {
			validMetricType = true
			break
		}
	}
	if !validMetricType {
		return fmt.Errorf("invalid metric type: %s. Valid types are: %v", rule.MetricType, validMetricTypes)
	}

	// Set default cooldown if not specified
	if rule.Cooldown == 0 {
		rule.Cooldown = 5 * time.Minute
	}

	return nil
}

// MetricsResponse represents a standardized metrics response
type MetricsResponse struct {
	Timestamp time.Time              `json:"timestamp"`
	Metrics   map[string]interface{} `json:"metrics"`
	Status    string                 `json:"status"`
}

// AlertsResponse represents a standardized alerts response
type AlertsResponse struct {
	Timestamp time.Time `json:"timestamp"`
	Alerts    []*Alert  `json:"alerts"`
	Count     int       `json:"count"`
	Status    string    `json:"status"`
}

// RulesResponse represents a standardized rules response
type RulesResponse struct {
	Timestamp time.Time    `json:"timestamp"`
	Rules     []*AlertRule `json:"rules"`
	Count     int          `json:"count"`
	Status    string       `json:"status"`
}
