package logging

import (
	"strings"
	"sync"
	"time"
)

// LogLevel represents the severity of a log entry
type LogLevel string

const (
	LevelDebug LogLevel = "debug"
	LevelInfo  LogLevel = "info"
	LevelWarn  LogLevel = "warn"
	LevelError LogLevel = "error"
)

// LogEntry represents a single log entry
type LogEntry struct {
	Timestamp string    `json:"timestamp"`
	Stage     string    `json:"stage"`
	Action    string    `json:"action"`
	Status    string    `json:"status"`
	Message   string    `json:"message"`
	Level     LogLevel  `json:"level"`
	Details   map[string]interface{} `json:"details,omitempty"`
}

// Logger handles structured logging for the dashboard
type Logger struct {
	mu      sync.RWMutex
	entries []LogEntry
	maxSize int
}

// NewLogger creates a new logger instance
func NewLogger(maxSize int) *Logger {
	if maxSize <= 0 {
		maxSize = 1000 // Default max size
	}
	return &Logger{
		entries: make([]LogEntry, 0, maxSize),
		maxSize: maxSize,
	}
}

// Log adds a new log entry
func (l *Logger) Log(stage, action, status, message string, level LogLevel, details map[string]interface{}) {
	l.mu.Lock()
	defer l.mu.Unlock()

	entry := LogEntry{
		Timestamp: time.Now().Format(time.RFC3339),
		Stage:     stage,
		Action:    action,
		Status:    status,
		Message:   message,
		Level:     level,
		Details:   details,
	}

	// Add to the beginning of the slice (newest first)
	l.entries = append([]LogEntry{entry}, l.entries...)

	// Trim if exceeds max size
	if len(l.entries) > l.maxSize {
		l.entries = l.entries[:l.maxSize]
	}
}

// Info logs an info level message
func (l *Logger) Info(stage, action, message string, details map[string]interface{}) {
	l.Log(stage, action, "success", message, LevelInfo, details)
}

// Warn logs a warning level message
func (l *Logger) Warn(stage, action, message string, details map[string]interface{}) {
	l.Log(stage, action, "warning", message, LevelWarn, details)
}

// Error logs an error level message
func (l *Logger) Error(stage, action, message string, details map[string]interface{}) {
	l.Log(stage, action, "error", message, LevelError, details)
}

// Debug logs a debug level message
func (l *Logger) Debug(stage, action, message string, details map[string]interface{}) {
	l.Log(stage, action, "debug", message, LevelDebug, details)
}

// GetLogs returns filtered and paginated logs
func (l *Logger) GetLogs(stage, status, search string, page, limit int) ([]LogEntry, int) {
	l.mu.RLock()
	defer l.mu.RUnlock()

	// Filter logs
	filteredLogs := make([]LogEntry, 0)
	for _, entry := range l.entries {
		// Stage filter
		if stage != "" && entry.Stage != stage {
			continue
		}

		// Status filter
		if status != "" && entry.Status != status {
			continue
		}

		// Search filter
		if search != "" {
			found := false
			// Search in message
			if containsIgnoreCase(entry.Message, search) {
				found = true
			}
			// Search in action
			if containsIgnoreCase(entry.Action, search) {
				found = true
			}
			// Search in details
			if entry.Details != nil {
				for _, v := range entry.Details {
					if str, ok := v.(string); ok && containsIgnoreCase(str, search) {
						found = true
						break
					}
				}
			}
			if !found {
				continue
			}
		}

		filteredLogs = append(filteredLogs, entry)
	}

	// Pagination
	total := len(filteredLogs)
	start := (page - 1) * limit
	end := start + limit

	if start >= total {
		return []LogEntry{}, total
	}

	if end > total {
		end = total
	}

	return filteredLogs[start:end], total
}

// Clear removes all log entries
func (l *Logger) Clear() {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.entries = l.entries[:0]
}

// GetStats returns logging statistics
func (l *Logger) GetStats() map[string]interface{} {
	l.mu.RLock()
	defer l.mu.RUnlock()

	stats := map[string]interface{}{
		"total_entries": len(l.entries),
		"max_size":      l.maxSize,
	}

	// Count by level
	levelCounts := make(map[LogLevel]int)
	for _, entry := range l.entries {
		levelCounts[entry.Level]++
	}
	stats["level_counts"] = levelCounts

	// Count by stage
	stageCounts := make(map[string]int)
	for _, entry := range l.entries {
		stageCounts[entry.Stage]++
	}
	stats["stage_counts"] = stageCounts

	return stats
}

// containsIgnoreCase performs case-insensitive substring search
func containsIgnoreCase(str, substr string) bool {
	return strings.Contains(strings.ToLower(str), strings.ToLower(substr))
}