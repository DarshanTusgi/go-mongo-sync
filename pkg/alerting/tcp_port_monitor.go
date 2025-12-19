package alerting

import (
	"fmt"
	"log"
	"net"
	"time"
)

// SimpleTCPPortMonitor monitors a TCP port and logs alerts
type SimpleTCPPortMonitor struct {
	address       string
	checkInterval time.Duration
	stopChan      chan struct{}
}

// NewSimpleTCPPortMonitor creates a new simple TCP port monitor
func NewSimpleTCPPortMonitor(address string, checkInterval time.Duration) *SimpleTCPPortMonitor {
	return &SimpleTCPPortMonitor{
		address:       address,
		checkInterval: checkInterval,
		stopChan:      make(chan struct{}),
	}
}

// Start begins monitoring the TCP port
func (m *SimpleTCPPortMonitor) Start() {
	ticker := time.NewTicker(m.checkInterval)
	defer ticker.Stop()

	log.Printf("🏥 Simple TCP Port Monitor started: checking %s every %v", m.address, m.checkInterval)

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
	if err != nil {
		// PORT NOT ACCESSIBLE - LOG ALERT
		log.Printf("[ALERT] TCP PORT NOT ACCESSIBLE: %s - Error: %v", m.address, err)
		log.Printf("[ALERT] VM-SYNC MAY BE DOWN - Please check vm-sync service at %s", m.address)
		log.Printf("[ALERT] Recommended Actions:")
		log.Printf("[ALERT]   1. Check if vm-sync service is running")
		log.Printf("[ALERT]   2. Verify network connectivity")
		log.Printf("[ALERT]   3. Check firewall rules for port %s", m.address)
		log.Printf("[ALERT]   4. Review vm-sync logs for errors")
	} else {
		// Port is accessible - silently close connection (no log spam)
		conn.Close()
	}
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
