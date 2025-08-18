package memory

import (
	"context"
	"fmt"
	"runtime"
	"sync"
	"time"
)

// Config holds memory management configuration
type Config struct {
	MaxBufferSize     int           `yaml:"max_buffer_size" json:"max_buffer_size"`         // Maximum buffer size in bytes
	GCInterval        time.Duration `yaml:"gc_interval" json:"gc_interval"`                 // Garbage collection interval
	MemoryThreshold   float64       `yaml:"memory_threshold" json:"memory_threshold"`       // Memory usage threshold (0.0-1.0)
	MonitorInterval   time.Duration `yaml:"monitor_interval" json:"monitor_interval"`       // Memory monitoring interval
	MaxChangeEvents   int           `yaml:"max_change_events" json:"max_change_events"`     // Max change events in buffer
	BufferFlushSize   int           `yaml:"buffer_flush_size" json:"buffer_flush_size"`     // Size to trigger buffer flush
	EnableCompression bool          `yaml:"enable_compression" json:"enable_compression"`   // Enable data compression
}

// DefaultConfig returns default memory management configuration
func DefaultConfig() *Config {
	return &Config{
		MaxBufferSize:     50 * 1024 * 1024, // 50MB
		GCInterval:        5 * time.Minute,
		MemoryThreshold:   0.8, // 80%
		MonitorInterval:   30 * time.Second,
		MaxChangeEvents:   10000,
		BufferFlushSize:   1000,
		EnableCompression: true,
	}
}

// Stats holds memory usage statistics
type Stats struct {
	AllocatedBytes   uint64    `json:"allocated_bytes"`
	TotalAllocBytes  uint64    `json:"total_alloc_bytes"`
	SystemBytes      uint64    `json:"system_bytes"`
	GCCount          uint32    `json:"gc_count"`
	BufferSize       int       `json:"buffer_size"`
	ChangeEventCount int       `json:"change_event_count"`
	LastGC           time.Time `json:"last_gc"`
	MemoryUsage      float64   `json:"memory_usage_percent"`
}

// Manager handles memory management for the sync system
type Manager struct {
	config    *Config
	mu        sync.RWMutex
	stats     *Stats
	buffers   map[string]*Buffer
	ctx       context.Context
	cancel    context.CancelFunc
	callbacks []func(*Stats)
}

// NewManager creates a new memory manager
func NewManager(config *Config) *Manager {
	if config == nil {
		config = DefaultConfig()
	}

	ctx, cancel := context.WithCancel(context.Background())
	m := &Manager{
		config:  config,
		stats:   &Stats{},
		buffers: make(map[string]*Buffer),
		ctx:     ctx,
		cancel:  cancel,
	}

	// Start monitoring goroutine
	go m.monitor()
	go m.gcScheduler()

	return m
}

// GetBuffer returns or creates a buffer for the given key
func (m *Manager) GetBuffer(key string) *Buffer {
	m.mu.Lock()
	defer m.mu.Unlock()

	if buffer, exists := m.buffers[key]; exists {
		return buffer
	}

	buffer := NewBuffer(key, m.config.MaxChangeEvents, m.config.BufferFlushSize)
	m.buffers[key] = buffer
	return buffer
}

// RemoveBuffer removes a buffer
func (m *Manager) RemoveBuffer(key string) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if buffer, exists := m.buffers[key]; exists {
		buffer.Clear()
		delete(m.buffers, key)
	}
}

// GetStats returns current memory statistics
func (m *Manager) GetStats() *Stats {
	m.mu.RLock()
	defer m.mu.RUnlock()

	stats := *m.stats
	return &stats
}

// AddCallback adds a callback function for memory stats updates
func (m *Manager) AddCallback(callback func(*Stats)) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.callbacks = append(m.callbacks, callback)
}

// ForceGC triggers garbage collection
func (m *Manager) ForceGC() {
	runtime.GC()
	m.updateStats()
}

// Close stops the memory manager
func (m *Manager) Close() {
	m.cancel()
	m.mu.Lock()
	defer m.mu.Unlock()

	for _, buffer := range m.buffers {
		buffer.Clear()
	}
	m.buffers = make(map[string]*Buffer)
}

// monitor runs the memory monitoring loop
func (m *Manager) monitor() {
	ticker := time.NewTicker(m.config.MonitorInterval)
	defer ticker.Stop()

	for {
		select {
		case <-m.ctx.Done():
			return
		case <-ticker.C:
			m.updateStats()
			m.checkMemoryThreshold()
		}
	}
}

// gcScheduler runs the garbage collection scheduler
func (m *Manager) gcScheduler() {
	ticker := time.NewTicker(m.config.GCInterval)
	defer ticker.Stop()

	for {
		select {
		case <-m.ctx.Done():
			return
		case <-ticker.C:
			runtime.GC()
			m.updateStats()
		}
	}
}

// updateStats updates memory statistics
func (m *Manager) updateStats() {
	var memStats runtime.MemStats
	runtime.ReadMemStats(&memStats)

	m.mu.Lock()
	defer m.mu.Unlock()

	totalBufferSize := 0
	totalChangeEvents := 0
	for _, buffer := range m.buffers {
		totalBufferSize += buffer.Size()
		totalChangeEvents += buffer.Count()
	}

	m.stats = &Stats{
		AllocatedBytes:   memStats.Alloc,
		TotalAllocBytes:  memStats.TotalAlloc,
		SystemBytes:      memStats.Sys,
		GCCount:          memStats.NumGC,
		BufferSize:       totalBufferSize,
		ChangeEventCount: totalChangeEvents,
		LastGC:           time.Now(),
		MemoryUsage:      float64(memStats.Alloc) / float64(memStats.Sys),
	}

	// Notify callbacks
	for _, callback := range m.callbacks {
		go callback(m.stats)
	}
}

// checkMemoryThreshold checks if memory usage exceeds threshold
func (m *Manager) checkMemoryThreshold() {
	m.mu.RLock()
	usage := m.stats.MemoryUsage
	m.mu.RUnlock()

	if usage > m.config.MemoryThreshold {
		// Trigger emergency cleanup
		m.emergencyCleanup()
	}
}

// emergencyCleanup performs emergency memory cleanup
func (m *Manager) emergencyCleanup() {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Flush all buffers
	for _, buffer := range m.buffers {
		buffer.Flush()
	}

	// Force garbage collection
	runtime.GC()

	fmt.Printf("Emergency memory cleanup triggered - usage: %.2f%%\n", m.stats.MemoryUsage*100)
}