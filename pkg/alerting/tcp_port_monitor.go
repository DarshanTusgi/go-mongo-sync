package alerting

import (
	"fmt"
	"log"
	"net"
	"sync"
	"time"
)

// MonitorState tracks the state of a connection
type MonitorState struct {
	Connected     bool
	LastChange    time.Time
	DisconnectedSince *time.Time
}

// SimpleTCPPortMonitor monitors a TCP port and logs alerts
type SimpleTCPPortMonitor struct {
	address       string
	checkInterval time.Duration
	stopChan      chan struct{}
	state         MonitorState
	stateMu       sync.RWMutex
	onStateChange func(connected bool, downtime time.Duration)
}

// NewSimpleTCPPortMonitor creates a new simple TCP port monitor
func NewSimpleTCPPortMonitor(address string, checkInterval time.Duration, onStateChange func(connected bool, downtime time.Duration)) *SimpleTCPPortMonitor {
	return &SimpleTCPPortMonitor{
		address:       address,
		checkInterval: checkInterval,
		stopChan:      make(chan struct{}),
		onStateChange: onStateChange,
	}
}

// Start begins monitoring the TCP port
func (m *SimpleTCPPortMonitor) Start() {
	ticker := time.NewTicker(m.checkInterval)
	defer ticker.Stop()

	log.Printf("🏥 Simple TCP Port Monitor started: checking %s every %v", m.address, m.checkInterval)

	// Initialize state
	m.stateMu.Lock()
	m.state = MonitorState{
		Connected:  true,
		LastChange: time.Now(),
	}
	m.stateMu.Unlock()

	for {
		select {
		case <-ticker.C:
			m.checkPort()
		case <-m.stopChan:
			log.Printf("🛑 Simple TCP Port Monitor stopped")
			return
		}
	}
}

// checkPort checks if the TCP port is accessible
func (m *SimpleTCPPortMonitor) checkPort() {
	conn, err := net.DialTimeout("tcp", m.address, 3*time.Second)
	
	m.stateMu.Lock()
	defer m.stateMu.Unlock()
	
	currentConnected := err == nil
	
	if conn != nil {
		conn.Close()
	}
	
	// Check if state changed
	if currentConnected != m.state.Connected {
		previousTime := m.state.LastChange
		
		// Update state
		m.state.Connected = currentConnected
		m.state.LastChange = time.Now()
		
		if currentConnected {
			// Connection re-established
			var downtime time.Duration
			if m.state.DisconnectedSince != nil {
				downtime = time.Since(*m.state.DisconnectedSince)
				m.state.DisconnectedSince = nil
			} else {
				downtime = time.Since(previousTime)
			}
			
			log.Printf("✅ TCP CONNECTION RE-ESTABLISHED: %s after %v downtime", m.address, downtime)
			
			// Notify about state change
			if m.onStateChange != nil {
				m.onStateChange(true, downtime)
			}
		} else {
			// Connection lost
			disconnectedTime := time.Now()
			m.state.DisconnectedSince = &disconnectedTime
			
			log.Printf("[ALERT] TCP CONNECTION LOST: %s", m.address)
			
			// Notify about state change
			if m.onStateChange != nil {
				m.onStateChange(false, 0) // downtime is 0 when just disconnected
			}
		}
	} else {
		// State hasn't changed, but if currently disconnected, log periodically
		if !currentConnected {
			log.Printf("[ALERT] TCP PORT STILL NOT ACCESSIBLE: %s", m.address)
		}
	}
}

// GetDowntime returns the current downtime duration if disconnected
func (m *SimpleTCPPortMonitor) GetDowntime() (bool, time.Duration) {
	m.stateMu.RLock()
	defer m.stateMu.RUnlock()
	
	if m.state.Connected {
		return true, 0
	}
	
	if m.state.DisconnectedSince != nil {
		return false, time.Since(*m.state.DisconnectedSince)
	}
	
	return false, time.Since(m.state.LastChange)
}

// Stop stops the monitor
func (m *SimpleTCPPortMonitor) Stop() {
	close(m.stopChan)
}

// CheckOnce performs a single port check (useful for testing)
func CheckTCPPortOnce(address string) error {
	conn, err := net.DialTimeout("tcp", address, 3*time.Second)
	if err != nil {
		return fmt.Errorf("TCP port not accessible: %v", err)
	}
	conn.Close()
	return nil
}