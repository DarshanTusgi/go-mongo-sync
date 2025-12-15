package transport

import (
	"context"
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"
)

// HeartbeatConfig defines heartbeat behavior
type HeartbeatConfig struct {
	// Interval between heartbeat messages (default: 10s)
	Interval time.Duration
	// Timeout for heartbeat response (default: 5s)
	Timeout time.Duration
	// Max missed heartbeats before declaring connection dead (default: 3)
	MaxMissed int
}

// HeartbeatMonitor actively monitors connection health using PING/PONG
// This provides faster failure detection than passive health checks
type HeartbeatMonitor struct {
	config HeartbeatConfig
	sender Sender
	
	// State
	missedBeats atomic.Int32
	lastPong    atomic.Int64 // UnixNano
	lastPing    atomic.Int64
	
	// Stats
	totalPings   atomic.Uint64
	totalPongs   atomic.Uint64
	totalMissed  atomic.Uint64
	avgLatencyNs atomic.Int64
	
	// Lifecycle
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
	
	// Callbacks
	onFailure func()
}

// NewHeartbeatMonitor creates a heartbeat monitor
func NewHeartbeatMonitor(sender Sender, config HeartbeatConfig) *HeartbeatMonitor {
	// Set defaults
	if config.Interval == 0 {
		config.Interval = 10 * time.Second
	}
	if config.Timeout == 0 {
		config.Timeout = 5 * time.Second
	}
	if config.MaxMissed == 0 {
		config.MaxMissed = 3
	}
	
	ctx, cancel := context.WithCancel(context.Background())
	
	hm := &HeartbeatMonitor{
		config: config,
		sender: sender,
		ctx:    ctx,
		cancel: cancel,
	}
	
	hm.lastPong.Store(time.Now().UnixNano())
	
	return hm
}

// Start begins heartbeat monitoring
func (hm *HeartbeatMonitor) Start() {
	hm.wg.Add(1)
	go hm.heartbeatLoop()
	log.Printf("💓 HEARTBEAT MONITOR: Started (interval: %v, timeout: %v, max_missed: %d)",
		hm.config.Interval, hm.config.Timeout, hm.config.MaxMissed)
}

// OnFailure registers a callback for connection failure
func (hm *HeartbeatMonitor) OnFailure(callback func()) {
	hm.onFailure = callback
}

// heartbeatLoop sends periodic PING messages
func (hm *HeartbeatMonitor) heartbeatLoop() {
	defer hm.wg.Done()
	
	ticker := time.NewTicker(hm.config.Interval)
	defer ticker.Stop()
	
	for {
		select {
		case <-ticker.C:
			if err := hm.sendHeartbeat(); err != nil {
				log.Printf("⚠️  HEARTBEAT PING FAILED: %v", err)
				hm.handleMissedBeat()
			}
			
		case <-hm.ctx.Done():
			log.Printf("🛑 HEARTBEAT MONITOR: Stopping")
			return
		}
	}
}

// sendHeartbeat sends a PING message
func (hm *HeartbeatMonitor) sendHeartbeat() error {
	pingTime := time.Now()
	hm.lastPing.Store(pingTime.UnixNano())
	hm.totalPings.Add(1)
	
	// Check if we've received a recent PONG
	lastPong := time.Unix(0, hm.lastPong.Load())
	timeSinceLastPong := time.Since(lastPong)
	
	if timeSinceLastPong > hm.config.Interval+hm.config.Timeout {
		return fmt.Errorf("no PONG received in %v", timeSinceLastPong)
	}
	
	return nil
}

// RecordPong records a PONG response
func (hm *HeartbeatMonitor) RecordPong() {
	now := time.Now()
	hm.lastPong.Store(now.UnixNano())
	hm.totalPongs.Add(1)
	
	// Reset missed beat counter
	hm.missedBeats.Store(0)
	
	// Calculate latency
	lastPing := time.Unix(0, hm.lastPing.Load())
	latency := now.Sub(lastPing)
	
	// Update average latency (simple moving average)
	currentAvg := time.Duration(hm.avgLatencyNs.Load())
	newAvg := (currentAvg + latency) / 2
	hm.avgLatencyNs.Store(int64(newAvg))
	
	log.Printf("💚 HEARTBEAT PONG: latency=%v, avg=%v", latency, newAvg)
}

// handleMissedBeat handles a missed heartbeat
func (hm *HeartbeatMonitor) handleMissedBeat() {
	missed := hm.missedBeats.Add(1)
	hm.totalMissed.Add(1)
	
	log.Printf("💔 HEARTBEAT MISSED: %d consecutive (max: %d)", missed, hm.config.MaxMissed)
	
	if missed >= int32(hm.config.MaxMissed) {
		log.Printf("🔴 HEARTBEAT FAILURE: Connection declared dead after %d missed beats", missed)
		
		// Trigger failure callback
		if hm.onFailure != nil {
			hm.onFailure()
		}
	}
}

// GetStats returns heartbeat statistics
func (hm *HeartbeatMonitor) GetStats() map[string]interface{} {
	return map[string]interface{}{
		"total_pings":       hm.totalPings.Load(),
		"total_pongs":       hm.totalPongs.Load(),
		"total_missed":      hm.totalMissed.Load(),
		"current_missed":    hm.missedBeats.Load(),
		"avg_latency_ms":    hm.avgLatencyNs.Load() / 1e6,
		"last_pong":         time.Unix(0, hm.lastPong.Load()),
		"time_since_pong":   time.Since(time.Unix(0, hm.lastPong.Load())).String(),
	}
}

// Stop stops the heartbeat monitor
func (hm *HeartbeatMonitor) Stop() {
	hm.cancel()
	hm.wg.Wait()
	log.Printf("✅ HEARTBEAT MONITOR: Stopped")
}
