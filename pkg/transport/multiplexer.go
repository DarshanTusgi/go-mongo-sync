package transport

import (
	"bufio"
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

// ProtocolType represents the detected protocol
type ProtocolType int

const (
	ProtocolHTTP ProtocolType = iota
	ProtocolWebSocket
	ProtocolTCP
	ProtocolTLS
	ProtocolUnknown
)

// MultiplexerConfig holds configuration for the protocol multiplexer
type MultiplexerConfig struct {
	// Address to listen on (e.g., "0.0.0.0:8080")
	ListenAddr string

	// TLS configuration (optional)
	TLSConfig *tls.Config

	// HTTP handler for HTTP requests
	HTTPHandler http.Handler

	// WebSocket upgrader
	WSUpgrader websocket.Upgrader

	// TCP handler for raw TCP connections
	TCPHandler func(conn net.Conn)

	// Read timeout for protocol detection
	DetectionTimeout time.Duration

	// Maximum connections
	MaxConnections int
}

// ProtocolMultiplexer handles multiple protocols on a single port
type ProtocolMultiplexer struct {
	config    MultiplexerConfig
	listener  net.Listener
	connCount int64
	maxConns  int64
	mu        sync.RWMutex
	ctx       context.Context
	cancel    context.CancelFunc
	wg        sync.WaitGroup
}

// NewProtocolMultiplexer creates a new protocol multiplexer
func NewProtocolMultiplexer(config MultiplexerConfig) *ProtocolMultiplexer {
	if config.DetectionTimeout == 0 {
		config.DetectionTimeout = 5 * time.Second
	}
	if config.MaxConnections == 0 {
		config.MaxConnections = 1000
	}

	ctx, cancel := context.WithCancel(context.Background())

	return &ProtocolMultiplexer{
		config:   config,
		maxConns: int64(config.MaxConnections),
		ctx:      ctx,
		cancel:   cancel,
	}
}

// Start begins listening and handling connections
func (pm *ProtocolMultiplexer) Start() error {
	var err error

	if pm.config.TLSConfig != nil {
		pm.listener, err = tls.Listen("tcp", pm.config.ListenAddr, pm.config.TLSConfig)
	} else {
		pm.listener, err = net.Listen("tcp", pm.config.ListenAddr)
	}

	if err != nil {
		return fmt.Errorf("failed to start listener: %w", err)
	}

	fmt.Printf("Protocol multiplexer listening on %s\n", pm.config.ListenAddr)

	pm.wg.Add(1)
	go pm.acceptLoop()

	return nil
}

// Stop gracefully shuts down the multiplexer
func (pm *ProtocolMultiplexer) Stop() error {
	pm.cancel()

	if pm.listener != nil {
		pm.listener.Close()
	}

	pm.wg.Wait()
	return nil
}

// acceptLoop handles incoming connections
func (pm *ProtocolMultiplexer) acceptLoop() {
	defer pm.wg.Done()

	for {
		select {
		case <-pm.ctx.Done():
			return
		default:
		}

		conn, err := pm.listener.Accept()
		if err != nil {
			if pm.ctx.Err() != nil {
				return // Shutting down
			}
			fmt.Printf("Accept error: %v\n", err)
			continue
		}

		// Check connection limit
		pm.mu.Lock()
		if pm.connCount >= pm.maxConns {
			pm.mu.Unlock()
			conn.Close()
			fmt.Printf("Connection limit reached, rejecting connection\n")
			continue
		}
		pm.connCount++
		pm.mu.Unlock()

		// Handle connection in goroutine
		pm.wg.Add(1)
		go pm.handleConnection(conn)
	}
}

// handleConnection detects protocol and routes to appropriate handler
func (pm *ProtocolMultiplexer) handleConnection(conn net.Conn) {
	defer pm.wg.Done()
	defer func() {
		conn.Close()
		pm.mu.Lock()
		pm.connCount--
		pm.mu.Unlock()
	}()

	// Set detection timeout
	conn.SetReadDeadline(time.Now().Add(pm.config.DetectionTimeout))

	// Detect protocol
	protocol, reader, err := pm.detectProtocol(conn)
	if err != nil {
		fmt.Printf("Protocol detection failed: %v\n", err)
		return
	}

	// Remove read deadline for actual handling
	conn.SetReadDeadline(time.Time{})

	// Route to appropriate handler
	switch protocol {
	case ProtocolHTTP, ProtocolWebSocket:
		pm.handleHTTP(conn, reader)
	case ProtocolTCP:
		pm.handleTCP(conn, reader)
	case ProtocolTLS:
		pm.handleTLS(conn, reader)
	default:
		fmt.Printf("Unknown protocol detected\n")
	}
}

// detectProtocol analyzes the first few bytes to determine protocol
func (pm *ProtocolMultiplexer) detectProtocol(conn net.Conn) (ProtocolType, *bufio.Reader, error) {
	reader := bufio.NewReader(conn)

	// Peek at first few bytes
	peek, err := reader.Peek(16)
	if err != nil {
		return ProtocolUnknown, reader, err
	}

	peekStr := string(peek)

	// Check for HTTP methods
	if strings.HasPrefix(peekStr, "GET ") ||
		strings.HasPrefix(peekStr, "POST ") ||
		strings.HasPrefix(peekStr, "PUT ") ||
		strings.HasPrefix(peekStr, "DELETE ") ||
		strings.HasPrefix(peekStr, "HEAD ") ||
		strings.HasPrefix(peekStr, "OPTIONS ") {

		// Check if it's a WebSocket upgrade by reading more
		line, err := reader.ReadString('\n')
		if err != nil {
			return ProtocolHTTP, reader, nil
		}

		// Look for WebSocket upgrade headers
		for {
			line, err = reader.ReadString('\n')
			if err != nil || line == "\r\n" {
				break
			}
			if strings.Contains(strings.ToLower(line), "upgrade: websocket") {
				return ProtocolWebSocket, reader, nil
			}
		}

		return ProtocolHTTP, reader, nil
	}

	// Check for TLS handshake
	if len(peek) >= 3 && peek[0] == 0x16 && peek[1] == 0x03 {
		return ProtocolTLS, reader, nil
	}

	// Default to TCP for binary data
	return ProtocolTCP, reader, nil
}

// handleHTTP processes HTTP/WebSocket requests
func (pm *ProtocolMultiplexer) handleHTTP(conn net.Conn, reader *bufio.Reader) {
	if pm.config.HTTPHandler == nil {
		fmt.Printf("No HTTP handler configured\n")
		return
	}

	// Create a connection that uses our buffered reader
	bufferedConn := &bufferedConnection{
		Conn:   conn,
		reader: reader,
	}

	// Serve HTTP
	server := &http.Server{
		Handler: pm.config.HTTPHandler,
	}

	server.Serve(&singleConnListener{conn: bufferedConn})
}

// handleTCP processes raw TCP connections
func (pm *ProtocolMultiplexer) handleTCP(conn net.Conn, reader *bufio.Reader) {
	if pm.config.TCPHandler == nil {
		fmt.Printf("No TCP handler configured\n")
		return
	}

	// Create a connection that uses our buffered reader
	bufferedConn := &bufferedConnection{
		Conn:   conn,
		reader: reader,
	}

	pm.config.TCPHandler(bufferedConn)
}

// handleTLS processes TLS connections
func (pm *ProtocolMultiplexer) handleTLS(conn net.Conn, reader *bufio.Reader) {
	// For TLS, we'd need to handle the handshake and then detect the inner protocol
	fmt.Printf("TLS protocol handling not implemented yet\n")
}

// bufferedConnection wraps a connection with a buffered reader
type bufferedConnection struct {
	net.Conn
	reader *bufio.Reader
}

func (bc *bufferedConnection) Read(p []byte) (int, error) {
	return bc.reader.Read(p)
}

// singleConnListener implements net.Listener for a single connection
type singleConnListener struct {
	conn net.Conn
	once sync.Once
}

func (scl *singleConnListener) Accept() (net.Conn, error) {
	var conn net.Conn
	scl.once.Do(func() {
		conn = scl.conn
	})
	if conn != nil {
		return conn, nil
	}
	return nil, io.EOF
}

func (scl *singleConnListener) Close() error {
	return nil
}

func (scl *singleConnListener) Addr() net.Addr {
	return scl.conn.LocalAddr()
}
