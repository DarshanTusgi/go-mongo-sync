package telemetry

import (
	"context"
	"fmt"
	"os"
	"runtime"
	"sync"
	"time"

	"github.com/shirou/gopsutil/v3/cpu"
	"github.com/shirou/gopsutil/v3/disk"
	"github.com/shirou/gopsutil/v3/load"
	"github.com/shirou/gopsutil/v3/mem"
	"github.com/shirou/gopsutil/v3/net"
	"github.com/shirou/gopsutil/v3/process"

	"go-data-sync-http/pkg/models"
)

// Collector gathers system telemetry data
type Collector struct {
	nodeID          string
	process         *process.Process
	lastIOStats     *process.IOCountersStat
	lastNetStats    []net.IOCountersStat
	lastCPUTime     time.Time
	lastCPUPercent  float64
	gcPauseHistory  []float64
	mutex           sync.RWMutex
	connectionCount int
	syncLatency     float64
	queueDepth      int
}

// NewCollector creates a new telemetry collector
func NewCollector(nodeID string) (*Collector, error) {
	pid := os.Getpid()
	proc, err := process.NewProcess(int32(pid))
	if err != nil {
		return nil, fmt.Errorf("failed to create process handle: %w", err)
	}

	// Initialize network stats
	netStats, err := net.IOCounters(false)
	if err != nil {
		netStats = []net.IOCountersStat{} // Continue with empty stats if unavailable
	}

	// Initialize IO stats
	ioStats, err := proc.IOCounters()
	if err != nil {
		ioStats = &process.IOCountersStat{} // Continue with empty stats if unavailable
	}

	return &Collector{
		nodeID:         nodeID,
		process:        proc,
		lastIOStats:    ioStats,
		lastNetStats:   netStats,
		lastCPUTime:    time.Now(),
		gcPauseHistory: make([]float64, 0, 10), // Keep last 10 GC pauses
	}, nil
}

// SetConnectionCount updates the current database connection count
func (c *Collector) SetConnectionCount(count int) {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	c.connectionCount = count
}

// SetSyncLatency updates the current sync latency
func (c *Collector) SetSyncLatency(latency float64) {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	c.syncLatency = latency
}

// SetQueueDepth updates the current processing queue depth
func (c *Collector) SetQueueDepth(depth int) {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	c.queueDepth = depth
}

// Collect gathers current telemetry data
func (c *Collector) Collect(ctx context.Context) (*models.TelemetryData, error) {
	c.mutex.RLock()
	connCount := c.connectionCount
	syncLat := c.syncLatency
	queueDep := c.queueDepth
	c.mutex.RUnlock()

	now := time.Now()
	telemetry := &models.TelemetryData{
		NodeID:          c.nodeID,
		Timestamp:       now,
		ConnectionCount: connCount,
		SyncLatency:     syncLat,
		QueueDepth:      queueDep,
	}

	// Collect CPU usage
	if cpuPercent, err := c.getCPUUsage(ctx); err == nil {
		telemetry.CPUUsage = cpuPercent
	}

	// Collect memory usage
	if memInfo, err := c.getMemoryUsage(); err == nil {
		telemetry.MemoryUsage = memInfo.UsedPercent
		telemetry.MemoryTotal = memInfo.Total
		telemetry.MemoryUsed = memInfo.Used
	}

	// Collect I/O stats
	if ioStats, err := c.getIOStats(); err == nil {
		telemetry.IOReadBytes = ioStats.ReadBytes
		telemetry.IOWriteBytes = ioStats.WriteBytes
		telemetry.IOReadOps = ioStats.ReadCount
		telemetry.IOWriteOps = ioStats.WriteCount
	}

	// Collect network stats
	if netStats, err := c.getNetworkStats(); err == nil {
		telemetry.NetworkRxBytes = netStats.BytesRecv
		telemetry.NetworkTxBytes = netStats.BytesSent
		telemetry.NetworkRxPackets = netStats.PacketsRecv
		telemetry.NetworkTxPackets = netStats.PacketsSent
	}

	// Collect runtime stats
	telemetry.Goroutines = runtime.NumGoroutine()
	telemetry.GCPauses = c.getGCPauses()

	// Collect system load average
	if loadAvg, err := load.Avg(); err == nil {
		telemetry.LoadAverage = []float64{loadAvg.Load1, loadAvg.Load5, loadAvg.Load15}
	}

	// Collect disk usage
	if diskUsage, err := c.getDiskUsage(); err == nil {
		telemetry.DiskUsage = diskUsage
	}

	return telemetry, nil
}

// getCPUUsage returns CPU usage percentage
func (c *Collector) getCPUUsage(ctx context.Context) (float64, error) {
	// Use process-specific CPU usage for more accurate VM Sync metrics
	if percent, err := c.process.CPUPercentWithContext(ctx); err == nil {
		return percent, nil
	}

	// Fallback to system-wide CPU usage
	percents, err := cpu.PercentWithContext(ctx, time.Second, false)
	if err != nil || len(percents) == 0 {
		return 0, err
	}
	return percents[0], nil
}

// getMemoryUsage returns memory usage information
func (c *Collector) getMemoryUsage() (*mem.VirtualMemoryStat, error) {
	return mem.VirtualMemory()
}

// getIOStats returns I/O statistics
func (c *Collector) getIOStats() (*process.IOCountersStat, error) {
	currentStats, err := c.process.IOCounters()
	if err != nil {
		return nil, err
	}

	// Calculate delta from last measurement
	if c.lastIOStats != nil {
		delta := &process.IOCountersStat{
			ReadCount:  currentStats.ReadCount - c.lastIOStats.ReadCount,
			WriteCount: currentStats.WriteCount - c.lastIOStats.WriteCount,
			ReadBytes:  currentStats.ReadBytes - c.lastIOStats.ReadBytes,
			WriteBytes: currentStats.WriteBytes - c.lastIOStats.WriteBytes,
		}
		c.lastIOStats = currentStats
		return delta, nil
	}

	c.lastIOStats = currentStats
	return currentStats, nil
}

// getNetworkStats returns network I/O statistics
func (c *Collector) getNetworkStats() (*net.IOCountersStat, error) {
	currentStats, err := net.IOCounters(false)
	if err != nil || len(currentStats) == 0 {
		return nil, err
	}

	// Aggregate all network interfaces
	aggregated := &net.IOCountersStat{}
	for _, stat := range currentStats {
		aggregated.BytesRecv += stat.BytesRecv
		aggregated.BytesSent += stat.BytesSent
		aggregated.PacketsRecv += stat.PacketsRecv
		aggregated.PacketsSent += stat.PacketsSent
	}

	// Calculate delta from last measurement
	if len(c.lastNetStats) > 0 {
		lastAggregated := &net.IOCountersStat{}
		for _, stat := range c.lastNetStats {
			lastAggregated.BytesRecv += stat.BytesRecv
			lastAggregated.BytesSent += stat.BytesSent
			lastAggregated.PacketsRecv += stat.PacketsRecv
			lastAggregated.PacketsSent += stat.PacketsSent
		}

		delta := &net.IOCountersStat{
			BytesRecv:   aggregated.BytesRecv - lastAggregated.BytesRecv,
			BytesSent:   aggregated.BytesSent - lastAggregated.BytesSent,
			PacketsRecv: aggregated.PacketsRecv - lastAggregated.PacketsRecv,
			PacketsSent: aggregated.PacketsSent - lastAggregated.PacketsSent,
		}
		c.lastNetStats = currentStats
		return delta, nil
	}

	c.lastNetStats = currentStats
	return aggregated, nil
}

// getGCPauses returns recent GC pause times
func (c *Collector) getGCPauses() []float64 {
	var memStats runtime.MemStats
	runtime.ReadMemStats(&memStats)

	// Convert nanoseconds to milliseconds
	pauseMs := float64(memStats.PauseNs[(memStats.NumGC+255)%256]) / 1e6

	c.mutex.Lock()
	defer c.mutex.Unlock()

	// Add new pause time
	if len(c.gcPauseHistory) >= 10 {
		c.gcPauseHistory = c.gcPauseHistory[1:] // Remove oldest
	}
	c.gcPauseHistory = append(c.gcPauseHistory, pauseMs)

	// Return copy of history
	result := make([]float64, len(c.gcPauseHistory))
	copy(result, c.gcPauseHistory)
	return result
}

// getDiskUsage returns disk usage percentage for the current working directory
func (c *Collector) getDiskUsage() (float64, error) {
	wd, err := os.Getwd()
	if err != nil {
		return 0, err
	}

	usage, err := disk.Usage(wd)
	if err != nil {
		return 0, err
	}

	return usage.UsedPercent, nil
}
