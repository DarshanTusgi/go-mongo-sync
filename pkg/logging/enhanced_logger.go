package logging

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"time"
)

// EnhancedLogger provides comprehensive debugging and monitoring capabilities
type EnhancedLogger struct {
	mu            sync.RWMutex
	entries       []LogEntry
	maxSize       int
	level         LogLevel
	outputs       []io.Writer
	hooks         []LogHook
	correlations  map[string][]LogEntry
	contextLogger *log.Logger
	enableTrace   bool
	enableMetrics bool
	metrics       *LogMetrics
}

// LogHook is called for each log entry
type LogHook func(entry LogEntry)

// LogMetrics tracks logging performance
type LogMetrics struct {
	TotalEntries   int64              `json:"total_entries"`
	EntriesByLevel map[LogLevel]int64 `json:"entries_by_level"`
	EntriesByStage map[string]int64   `json:"entries_by_stage"`
	ErrorCount     int64              `json:"error_count"`
	WarningCount   int64              `json:"warning_count"`
	LastEntry      time.Time          `json:"last_entry"`
	AverageLatency time.Duration      `json:"average_latency"`
}

// EnhancedLoggerConfig configures the enhanced logger
type EnhancedLoggerConfig struct {
	MaxSize       int      `json:"max_size"`
	Level         LogLevel `json:"level"`
	EnableTrace   bool     `json:"enable_trace"`
	EnableMetrics bool     `json:"enable_metrics"`
	OutputFile    string   `json:"output_file,omitempty"`
	JSONFormat    bool     `json:"json_format"`
}

// NewEnhancedLogger creates a new enhanced logger
func NewEnhancedLogger(config EnhancedLoggerConfig) *EnhancedLogger {
	if config.MaxSize <= 0 {
		config.MaxSize = 5000 // Increased default
	}

	logger := &EnhancedLogger{
		entries:       make([]LogEntry, 0, config.MaxSize),
		maxSize:       config.MaxSize,
		level:         config.Level,
		outputs:       []io.Writer{os.Stdout},
		correlations:  make(map[string][]LogEntry),
		enableTrace:   config.EnableTrace,
		enableMetrics: config.EnableMetrics,
		metrics: &LogMetrics{
			EntriesByLevel: make(map[LogLevel]int64),
			EntriesByStage: make(map[string]int64),
		},
	}

	// Add file output if specified
	if config.OutputFile != "" {
		if file, err := os.OpenFile(config.OutputFile, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666); err == nil {
			logger.outputs = append(logger.outputs, file)
		}
	}

	logger.contextLogger = log.New(io.MultiWriter(logger.outputs...), "", 0)

	return logger
}

// getCallerInfo extracts file, function, and line information
func getCallerInfo(skip int) LogContext {
	pc, file, line, ok := runtime.Caller(skip + 1)
	if !ok {
		return LogContext{File: "unknown", Function: "unknown", Line: 0}
	}

	funcName := "unknown"
	if fn := runtime.FuncForPC(pc); fn != nil {
		funcName = fn.Name()
		// Extract just the function name from full path
		if idx := strings.LastIndex(funcName, "."); idx != -1 {
			funcName = funcName[idx+1:]
		}
	}

	return LogContext{
		File:     filepath.Base(file),
		Function: funcName,
		Line:     line,
	}
}

// getStackTrace captures the current stack trace
func getStackTrace(skip int) []string {
	var stack []string
	for i := skip; i < skip+10; i++ { // Capture up to 10 frames
		pc, file, line, ok := runtime.Caller(i)
		if !ok {
			break
		}

		funcName := "unknown"
		if fn := runtime.FuncForPC(pc); fn != nil {
			funcName = fn.Name()
		}

		frame := fmt.Sprintf("%s:%d %s", filepath.Base(file), line, funcName)
		stack = append(stack, frame)
	}
	return stack
}

// LogWithContext logs with full context and debugging information
func (el *EnhancedLogger) LogWithContext(ctx context.Context, stage, action, status, message string, level LogLevel, details map[string]interface{}, options ...LogOption) {
	start := time.Now()

	// Skip if level is below threshold
	if !el.shouldLog(level) {
		return
	}

	entry := LogEntry{
		Timestamp: time.Now().Format(time.RFC3339Nano),
		Stage:     stage,
		Action:    action,
		Status:    status,
		Message:   message,
		Level:     level,
		Details:   details,
		Context:   getCallerInfo(1),
	}

	// Apply options
	for _, opt := range options {
		opt(&entry)
	}

	// Extract context information
	if ctx != nil {
		if correlationID := ctx.Value("correlation_id"); correlationID != nil {
			if id, ok := correlationID.(string); ok {
				entry.CorrelationID = id
			}
		}
		if userID := ctx.Value("user_id"); userID != nil {
			if id, ok := userID.(string); ok {
				entry.UserID = id
			}
		}
		if clientID := ctx.Value("client_id"); clientID != nil {
			if id, ok := clientID.(string); ok {
				entry.ClientID = id
			}
		}
	}

	// Add stack trace for errors
	if level == LevelError || level == LevelFatal {
		entry.StackTrace = getStackTrace(1)
	}

	el.addEntry(entry)
	el.updateMetrics(entry, time.Since(start))
	el.writeToOutputs(entry)
}

// LogOption allows customization of log entries
type LogOption func(*LogEntry)

// WithCorrelation adds correlation ID
func WithCorrelation(id string) LogOption {
	return func(entry *LogEntry) {
		entry.CorrelationID = id
	}
}

// WithDuration adds duration information
func WithDuration(duration time.Duration) LogOption {
	return func(entry *LogEntry) {
		entry.Duration = &duration
	}
}

// WithClient adds client ID
func WithClient(clientID string) LogOption {
	return func(entry *LogEntry) {
		entry.ClientID = clientID
	}
}

// shouldLog checks if the entry should be logged based on level
func (el *EnhancedLogger) shouldLog(level LogLevel) bool {
	levelOrder := map[LogLevel]int{
		LevelTrace: 0,
		LevelDebug: 1,
		LevelInfo:  2,
		LevelWarn:  3,
		LevelError: 4,
		LevelFatal: 5,
	}

	currentLevel := levelOrder[el.level]
	entryLevel := levelOrder[level]

	return entryLevel >= currentLevel
}

// addEntry adds an entry to the in-memory store
func (el *EnhancedLogger) addEntry(entry LogEntry) {
	el.mu.Lock()
	defer el.mu.Unlock()

	// Add to main entries (newest first)
	el.entries = append([]LogEntry{entry}, el.entries...)

	// Trim if exceeds max size
	if len(el.entries) > el.maxSize {
		el.entries = el.entries[:el.maxSize]
	}

	// Add to correlations if correlation ID exists
	if entry.CorrelationID != "" {
		el.correlations[entry.CorrelationID] = append(el.correlations[entry.CorrelationID], entry)
	}
}

// updateMetrics updates logging metrics
func (el *EnhancedLogger) updateMetrics(entry LogEntry, processingTime time.Duration) {
	if !el.enableMetrics {
		return
	}

	el.mu.Lock()
	defer el.mu.Unlock()

	el.metrics.TotalEntries++
	el.metrics.EntriesByLevel[entry.Level]++
	el.metrics.EntriesByStage[entry.Stage]++
	el.metrics.LastEntry = time.Now()

	if entry.Level == LevelError || entry.Level == LevelFatal {
		el.metrics.ErrorCount++
	}
	if entry.Level == LevelWarn {
		el.metrics.WarningCount++
	}

	// Update average latency
	if el.metrics.TotalEntries == 1 {
		el.metrics.AverageLatency = processingTime
	} else {
		el.metrics.AverageLatency = (el.metrics.AverageLatency + processingTime) / 2
	}
}

// writeToOutputs writes the entry to all configured outputs
func (el *EnhancedLogger) writeToOutputs(entry LogEntry) {
	jsonData, _ := json.Marshal(entry)
	message := string(jsonData) + "\n"

	for _, output := range el.outputs {
		fmt.Fprint(output, message)
	}

	// Call hooks
	for _, hook := range el.hooks {
		go hook(entry) // Async to avoid blocking
	}
}

// Convenience methods with enhanced context
func (el *EnhancedLogger) TraceCtx(ctx context.Context, stage, action, message string, details map[string]interface{}, opts ...LogOption) {
	el.LogWithContext(ctx, stage, action, "trace", message, LevelTrace, details, opts...)
}

func (el *EnhancedLogger) DebugCtx(ctx context.Context, stage, action, message string, details map[string]interface{}, opts ...LogOption) {
	el.LogWithContext(ctx, stage, action, "debug", message, LevelDebug, details, opts...)
}

func (el *EnhancedLogger) InfoCtx(ctx context.Context, stage, action, message string, details map[string]interface{}, opts ...LogOption) {
	el.LogWithContext(ctx, stage, action, "success", message, LevelInfo, details, opts...)
}

func (el *EnhancedLogger) WarnCtx(ctx context.Context, stage, action, message string, details map[string]interface{}, opts ...LogOption) {
	el.LogWithContext(ctx, stage, action, "warning", message, LevelWarn, details, opts...)
}

func (el *EnhancedLogger) ErrorCtx(ctx context.Context, stage, action, message string, details map[string]interface{}, opts ...LogOption) {
	el.LogWithContext(ctx, stage, action, "error", message, LevelError, details, opts...)
}

func (el *EnhancedLogger) FatalCtx(ctx context.Context, stage, action, message string, details map[string]interface{}, opts ...LogOption) {
	el.LogWithContext(ctx, stage, action, "fatal", message, LevelFatal, details, opts...)
}

// GetCorrelatedLogs returns all logs for a correlation ID
func (el *EnhancedLogger) GetCorrelatedLogs(correlationID string) []LogEntry {
	el.mu.RLock()
	defer el.mu.RUnlock()

	if logs, exists := el.correlations[correlationID]; exists {
		return logs
	}
	return []LogEntry{}
}

// GetMetrics returns current logging metrics
func (el *EnhancedLogger) GetMetrics() LogMetrics {
	el.mu.RLock()
	defer el.mu.RUnlock()

	// Create a copy to avoid race conditions
	metrics := LogMetrics{
		TotalEntries:   el.metrics.TotalEntries,
		EntriesByLevel: make(map[LogLevel]int64),
		EntriesByStage: make(map[string]int64),
		ErrorCount:     el.metrics.ErrorCount,
		WarningCount:   el.metrics.WarningCount,
		LastEntry:      el.metrics.LastEntry,
		AverageLatency: el.metrics.AverageLatency,
	}

	for k, v := range el.metrics.EntriesByLevel {
		metrics.EntriesByLevel[k] = v
	}
	for k, v := range el.metrics.EntriesByStage {
		metrics.EntriesByStage[k] = v
	}

	return metrics
}

// AddHook adds a logging hook
func (el *EnhancedLogger) AddHook(hook LogHook) {
	el.mu.Lock()
	defer el.mu.Unlock()
	el.hooks = append(el.hooks, hook)
}

// SetLevel sets the minimum logging level
func (el *EnhancedLogger) SetLevel(level LogLevel) {
	el.mu.Lock()
	defer el.mu.Unlock()
	el.level = level
}

// ExportLogs exports logs to a file
func (el *EnhancedLogger) ExportLogs(filename string) error {
	el.mu.RLock()
	defer el.mu.RUnlock()

	file, err := os.Create(filename)
	if err != nil {
		return err
	}
	defer file.Close()

	encoder := json.NewEncoder(file)
	encoder.SetIndent("", "  ")

	return encoder.Encode(map[string]interface{}{
		"export_time":   time.Now(),
		"total_entries": len(el.entries),
		"entries":       el.entries,
		"metrics":       el.metrics,
	})
}
