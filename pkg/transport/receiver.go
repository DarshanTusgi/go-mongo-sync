package transport

import (
	"context"
	"crypto/tls"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"
)

// tcpReceiver implements the Receiver interface using TCP connections
type tcpReceiver struct {
	config       ReceiverConfig
	listener     net.Listener
	connections  map[string]*receiverConnection
	streamStates map[uint64]*receiverStreamState
	batchHandler BatchHandler
	errorHandler ErrorHandler
	compressors  map[int]Compressor
	stats        receiverStats
	checkpoints  map[string]uint64
	mu           sync.RWMutex
	ctx          context.Context
	cancel       context.CancelFunc
	wg           sync.WaitGroup
	running      atomic.Bool
}

// receiverStreamState tracks the state of a stream on the receiver side
type receiverStreamState struct {
	streamID         uint64
	streamName       string
	lastSeq          uint64
	duplicateBatches map[uint64]bool
	mu               sync.RWMutex
}

// receiverConnection represents a connection from a sender
type receiverConnection struct {
	conn     net.Conn
	id       string
	receiver *tcpReceiver
	ctx      context.Context
	cancel   context.CancelFunc
}

// receiverStats holds atomic counters for statistics
type receiverStats struct {
	bytesReceived     atomic.Uint64
	batchesReceived   atomic.Uint64
	documentsReceived atomic.Uint64
	errorCount        atomic.Uint64
	decompressedBytes atomic.Uint64
}

// newTCPReceiver creates a new TCP receiver
func newTCPReceiver(config ReceiverConfig) (*tcpReceiver, error) {
	// Create compressors for all types
	compressors := make(map[int]Compressor)

	noComp := &NoCompression{}
	compressors[noComp.Type()] = noComp

	if zstdComp, err := NewZstdCompressor(); err == nil {
		compressors[zstdComp.Type()] = zstdComp
	}

	lz4Comp := NewLZ4Compressor()
	compressors[lz4Comp.Type()] = lz4Comp

	ctx, cancel := context.WithCancel(context.Background())

	receiver := &tcpReceiver{
		config:       config,
		connections:  make(map[string]*receiverConnection),
		streamStates: make(map[uint64]*receiverStreamState),
		compressors:  compressors,
		checkpoints:  make(map[string]uint64),
		ctx:          ctx,
		cancel:       cancel,
	}

	// Load checkpoints if disk-based checkpointing is enabled
	if config.DiskCheckpoint {
		if err := receiver.loadCheckpoints(); err != nil {
			return nil, fmt.Errorf("failed to load checkpoints: %w", err)
		}
	}

	return receiver, nil
}

// OnBatch registers a handler for incoming document batches
func (r *tcpReceiver) OnBatch(handler BatchHandler) {
	r.batchHandler = handler
}

// OnError registers a handler for errors
func (r *tcpReceiver) OnError(handler ErrorHandler) {
	r.errorHandler = handler
}

// Start begins listening for incoming connections
func (r *tcpReceiver) Start() error {
	if r.running.Swap(true) {
		return fmt.Errorf("receiver is already running")
	}

	var err error
	if r.config.TLSConfig != nil {
		r.listener, err = tls.Listen("tcp", r.config.ListenAddr, r.config.TLSConfig)
	} else {
		r.listener, err = net.Listen("tcp", r.config.ListenAddr)
	}

	if err != nil {
		r.running.Store(false)
		return fmt.Errorf("failed to listen on %s: %w", r.config.ListenAddr, err)
	}

	r.wg.Add(1)
	go r.acceptLoop()

	// Start heartbeat goroutine
	r.wg.Add(1)
	go r.heartbeatLoop()

	return nil
}

// Stop gracefully shuts down the receiver
func (r *tcpReceiver) Stop() error {
	if !r.running.Swap(false) {
		return nil // Already stopped
	}

	// Cancel context to signal shutdown
	r.cancel()

	// Close listener
	if r.listener != nil {
		r.listener.Close()
	}

	// Close all connections
	r.mu.Lock()
	for _, conn := range r.connections {
		conn.cancel()
		conn.conn.Close()
	}
	r.mu.Unlock()

	// Wait for goroutines to finish
	r.wg.Wait()

	// Save checkpoints if disk-based checkpointing is enabled
	if r.config.DiskCheckpoint {
		if err := r.saveCheckpoints(); err != nil {
			r.handleError(fmt.Errorf("failed to save checkpoints: %w", err))
		}
	}

	// Close compressors
	for _, comp := range r.compressors {
		if closer, ok := comp.(interface{ Close() }); ok {
			closer.Close()
		}
	}

	return nil
}

// acceptLoop accepts incoming connections
func (r *tcpReceiver) acceptLoop() {
	defer r.wg.Done()

	for {
		select {
		case <-r.ctx.Done():
			return
		default:
			conn, err := r.listener.Accept()
			if err != nil {
				if r.running.Load() {
					r.handleError(fmt.Errorf("failed to accept connection: %w", err))
				}
				continue
			}

			// Check connection limit
			r.mu.RLock()
			connCount := len(r.connections)
			r.mu.RUnlock()

			if connCount >= r.config.MaxConnections {
				conn.Close()
				r.handleError(fmt.Errorf("connection limit reached: %d", r.config.MaxConnections))
				continue
			}

			// Handle new connection
			go r.handleConnection(conn)
		}
	}
}

// handleConnection handles a new connection
func (r *tcpReceiver) handleConnection(conn net.Conn) {
	// Set TCP options
	if tcpConn, ok := conn.(*net.TCPConn); ok {
		tcpConn.SetNoDelay(true)
		tcpConn.SetKeepAlive(true)
		tcpConn.SetKeepAlivePeriod(30 * time.Second)
	}

	connID := conn.RemoteAddr().String()
	ctx, cancel := context.WithCancel(r.ctx)

	receiverConn := &receiverConnection{
		conn:     conn,
		id:       connID,
		receiver: r,
		ctx:      ctx,
		cancel:   cancel,
	}

	// Add to connections map
	r.mu.Lock()
	r.connections[connID] = receiverConn
	r.mu.Unlock()

	// Start handling this connection
	r.wg.Add(1)
	go receiverConn.handleLoop()
}

// heartbeatLoop sends periodic heartbeat messages
func (r *tcpReceiver) heartbeatLoop() {
	defer r.wg.Done()

	ticker := time.NewTicker(r.config.HeartbeatInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			r.sendHeartbeats()
		case <-r.ctx.Done():
			return
		}
	}
}

// sendHeartbeats sends heartbeat to all connections
func (r *tcpReceiver) sendHeartbeats() {
	frame := NewHeartbeatFrame()
	data := frame.EncodeFrame()

	r.mu.RLock()
	connections := make([]*receiverConnection, 0, len(r.connections))
	for _, conn := range r.connections {
		connections = append(connections, conn)
	}
	r.mu.RUnlock()

	for _, conn := range connections {
		conn.conn.SetWriteDeadline(time.Now().Add(r.config.WriteTimeout))
		conn.conn.Write(data)
	}
}

// Stats returns current receiver statistics
func (r *tcpReceiver) Stats() ReceiverStats {
	r.mu.RLock()
	connCount := len(r.connections)
	activeStreams := len(r.streamStates)
	r.mu.RUnlock()

	return ReceiverStats{
		BytesReceived:     r.stats.bytesReceived.Load(),
		BatchesReceived:   r.stats.batchesReceived.Load(),
		DocumentsReceived: r.stats.documentsReceived.Load(),
		ConnectionCount:   connCount,
		ErrorCount:        r.stats.errorCount.Load(),
		DecompressedBytes: r.stats.decompressedBytes.Load(),
		ActiveStreams:     activeStreams,
	}
}

// GetCheckpoint returns the current checkpoint for a stream
func (r *tcpReceiver) GetCheckpoint(stream string) uint64 {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.checkpoints[stream]
}

// SetCheckpoint sets the checkpoint for a stream
func (r *tcpReceiver) SetCheckpoint(stream string, seq uint64) error {
	r.mu.Lock()
	r.checkpoints[stream] = seq
	r.mu.Unlock()

	if r.config.DiskCheckpoint {
		return r.saveCheckpoints()
	}
	return nil
}

// handleLoop handles messages from a connection
func (rc *receiverConnection) handleLoop() {
	defer func() {
		rc.receiver.wg.Done()
		rc.conn.Close()

		// Remove from connections map
		rc.receiver.mu.Lock()
		delete(rc.receiver.connections, rc.id)
		rc.receiver.mu.Unlock()
	}()

	buf := make([]byte, rc.receiver.config.BufferSize)
	headerBuf := make([]byte, FrameHeaderSize)

	for {
		select {
		case <-rc.ctx.Done():
			return
		default:
			// Set read timeout
			rc.conn.SetReadDeadline(time.Now().Add(rc.receiver.config.ReadTimeout))

			// Read header
			n, err := rc.conn.Read(headerBuf)
			if err != nil {
				// Handle EOF gracefully - this happens when health checks connect and immediately close
				if err == io.EOF && n == 0 {
					// Connection closed cleanly without sending data (likely a health check)
					// Don't log this as an error since it's expected behavior
					return
				}
				rc.receiver.handleError(fmt.Errorf("failed to read header: %w", err))
				return
			}

			header, err := DecodeHeader(headerBuf)
			if err != nil {
				rc.receiver.handleError(fmt.Errorf("failed to decode header: %w", err))
				continue
			}

			if err := header.Validate(); err != nil {
				rc.receiver.handleError(fmt.Errorf("invalid header: %w", err))
				continue
			}

			// Read payload if present
			payloadSize := header.PayloadSize()
			var payload []byte
			if payloadSize > 0 {
				if payloadSize > uint32(rc.receiver.config.MaxBatchSize) {
					rc.receiver.handleError(fmt.Errorf("payload too large: %d bytes", payloadSize))
					continue
				}

				if payloadSize > uint32(len(buf)) {
					buf = make([]byte, payloadSize)
				}
				payload = buf[:payloadSize]
				_, err = rc.conn.Read(payload)
				if err != nil {
					rc.receiver.handleError(fmt.Errorf("failed to read payload: %w", err))
					return
				}
			}

			// Create frame and verify checksum
			frame := &Frame{Header: *header, Payload: payload}
			if !frame.VerifyChecksum() {
				rc.receiver.handleError(fmt.Errorf("checksum verification failed"))
				continue
			}

			// Handle different message types
			switch header.MsgType {
			case MsgTypeDocBatch:
				rc.handleDocBatch(frame)
			case MsgTypeResumeRequest:
				rc.handleResumeRequest(frame)
			case MsgTypeHeartbeat:
				// Heartbeat received, no action needed
			}
		}
	}
}

// handleDocBatch processes a document batch
func (rc *receiverConnection) handleDocBatch(frame *Frame) {
	// Get or create stream state
	state := rc.receiver.getOrCreateStreamState(frame.Header.StreamID)

	// Check for duplicates
	state.mu.Lock()
	if state.duplicateBatches[frame.Header.BatchSeq] {
		state.mu.Unlock()
		// Send ACK for duplicate
		rc.sendAck(frame.Header.StreamID, frame.Header.BatchSeq)
		return
	}
	state.duplicateBatches[frame.Header.BatchSeq] = true
	state.lastSeq = frame.Header.BatchSeq
	state.mu.Unlock()

	// Decompress payload if compressed
	payload := frame.Payload
	if frame.Header.IsCompressed() {
		// Determine compression type from frame or use heuristic
		compType := CompressionZstd // Default assumption
		if compressor, exists := rc.receiver.compressors[compType]; exists {
			decompressed, err := compressor.Decompress(payload)
			if err != nil {
				// Try other compression types
				for cType, comp := range rc.receiver.compressors {
					if cType == CompressionNone {
						continue
					}
					if decompressed, err = comp.Decompress(payload); err == nil {
						payload = decompressed
						rc.receiver.stats.decompressedBytes.Add(uint64(len(payload)))
						break
					}
				}
				if err != nil {
					rc.receiver.handleError(fmt.Errorf("decompression failed: %w", err))
					return
				}
			} else {
				payload = decompressed
				rc.receiver.stats.decompressedBytes.Add(uint64(len(payload)))
			}
		}
	}

	// CRITICAL FIX: Extract stream name from payload prefix
	// Format: [stream_name_length:4][stream_name][bson_documents]
	var actualStreamName string
	var actualPayload []byte
	
	if len(payload) >= 4 {
		// Try to extract stream name from prefix
		streamNameLength := binary.BigEndian.Uint32(payload[:4])
		if streamNameLength > 0 && streamNameLength < 1000 && len(payload) >= int(4+streamNameLength) {
			// Extract stream name
			actualStreamName = string(payload[4 : 4+streamNameLength])
			actualPayload = payload[4+streamNameLength:]
			
			// Update stream state with actual name
			state.mu.Lock()
			if state.streamName != actualStreamName {
				log.Printf("📋 STREAM NAME DISCOVERED: %s (was %s)", actualStreamName, state.streamName)
				state.streamName = actualStreamName
			}
			state.mu.Unlock()
		} else {
			// No stream name prefix, use original payload
			actualStreamName = state.streamName
			actualPayload = payload
		}
	} else {
		// Payload too short, use original
		actualStreamName = state.streamName
		actualPayload = payload
	}

	// Parse BSON documents from actual payload
	docs, err := rc.parseBSONDocs(actualPayload)
	if err != nil {
		rc.receiver.handleError(fmt.Errorf("failed to parse BSON documents: %w", err))
		return
	}

	// Update statistics
	rc.receiver.stats.bytesReceived.Add(uint64(len(frame.Payload)))
	rc.receiver.stats.batchesReceived.Add(1)
	rc.receiver.stats.documentsReceived.Add(uint64(len(docs)))

	// Call batch handler
	if rc.receiver.batchHandler != nil {
		if err := rc.receiver.batchHandler(actualStreamName, frame.Header.BatchSeq, docs); err != nil {
			rc.receiver.handleError(fmt.Errorf("batch handler error: %w", err))
			return
		}
	}

	// Update checkpoint with actual stream name
	rc.receiver.SetCheckpoint(actualStreamName, frame.Header.BatchSeq)

	// Send ACK
	rc.sendAck(frame.Header.StreamID, frame.Header.BatchSeq)
}

// handleResumeRequest processes a resume request
func (rc *receiverConnection) handleResumeRequest(frame *Frame) {
	msg, err := DecodeResumeMessage(frame.Payload, false)
	if err != nil {
		rc.receiver.handleError(fmt.Errorf("failed to decode resume message: %w", err))
		return
	}

	// Check if we can resume from the requested sequence
	state := rc.receiver.getOrCreateStreamState(msg.StreamID)
	state.mu.RLock()
	canResume := msg.FromSeq <= state.lastSeq+1
	state.mu.RUnlock()

	// Send resume response
	response := NewResumeResponseFrame(msg.StreamID, msg.FromSeq, canResume)
	data := response.EncodeFrame()

	rc.conn.SetWriteDeadline(time.Now().Add(rc.receiver.config.WriteTimeout))
	rc.conn.Write(data)
}

// sendAck sends an acknowledgment for a batch
func (rc *receiverConnection) sendAck(streamID, batchSeq uint64) {
	ack := NewAckFrame(streamID, batchSeq)
	data := ack.EncodeFrame()

	rc.conn.SetWriteDeadline(time.Now().Add(rc.receiver.config.WriteTimeout))
	rc.conn.Write(data)
}

// parseBSONDocs parses concatenated BSON documents
func (rc *receiverConnection) parseBSONDocs(payload []byte) ([][]byte, error) {
	var docs [][]byte
	offset := 0

	for offset < len(payload) {
		if offset+4 > len(payload) {
			return nil, fmt.Errorf("incomplete BSON document header")
		}

		// Read BSON document size (first 4 bytes, little endian)
		docSize := binary.LittleEndian.Uint32(payload[offset : offset+4])

		if offset+int(docSize) > len(payload) {
			return nil, fmt.Errorf("incomplete BSON document: expected %d bytes", docSize)
		}

		doc := make([]byte, docSize)
		copy(doc, payload[offset:offset+int(docSize)])
		docs = append(docs, doc)

		offset += int(docSize)
	}

	return docs, nil
}

// getOrCreateStreamState gets or creates a stream state
func (r *tcpReceiver) getOrCreateStreamState(streamID uint64) *receiverStreamState {
	r.mu.Lock()
	defer r.mu.Unlock()

	state, exists := r.streamStates[streamID]
	if !exists {
		state = &receiverStreamState{
			streamID:         streamID,
			streamName:       fmt.Sprintf("stream_%d", streamID),
			lastSeq:          0,
			duplicateBatches: make(map[uint64]bool),
		}
		r.streamStates[streamID] = state
	}

	return state
}

// handleError handles errors
func (r *tcpReceiver) handleError(err error) {
	r.stats.errorCount.Add(1)
	if r.errorHandler != nil {
		r.errorHandler(err)
	}
}

// loadCheckpoints loads checkpoints from disk for resumable transfers
func (r *tcpReceiver) loadCheckpoints() error {
	if r.config.CheckpointDir == "" {
		return nil
	}

	checkpointFile := filepath.Join(r.config.CheckpointDir, "tcp_checkpoints.json")
	if _, err := os.Stat(checkpointFile); os.IsNotExist(err) {
		return nil // No checkpoint file exists yet
	}

	// Create directory if it doesn't exist
	if err := os.MkdirAll(r.config.CheckpointDir, 0755); err != nil {
		return fmt.Errorf("failed to create checkpoint directory: %w", err)
	}

	// Read checkpoint file
	data, err := os.ReadFile(checkpointFile)
	if err != nil {
		return fmt.Errorf("failed to read checkpoint file: %w", err)
	}

	// Parse JSON checkpoints
	var checkpoints map[string]uint64
	if err := json.Unmarshal(data, &checkpoints); err != nil {
		return fmt.Errorf("failed to parse checkpoint file: %w", err)
	}

	// Load checkpoints into memory
	r.mu.Lock()
	r.checkpoints = checkpoints
	r.mu.Unlock()

	log.Printf("🚀 CHECKPOINTS LOADED: %d stream checkpoints from %s", len(checkpoints), checkpointFile)
	return nil
}

// saveCheckpoints saves checkpoints to disk for resumable transfers
func (r *tcpReceiver) saveCheckpoints() error {
	if r.config.CheckpointDir == "" {
		return nil
	}

	// Ensure directory exists
	if err := os.MkdirAll(r.config.CheckpointDir, 0755); err != nil {
		return fmt.Errorf("failed to create checkpoint directory: %w", err)
	}

	// Get current checkpoints
	r.mu.RLock()
	checkpoints := make(map[string]uint64)
	for k, v := range r.checkpoints {
		checkpoints[k] = v
	}
	r.mu.RUnlock()

	// Convert to JSON
	data, err := json.MarshalIndent(checkpoints, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal checkpoints: %w", err)
	}

	// Write to temporary file first, then rename (atomic operation)
	checkpointFile := filepath.Join(r.config.CheckpointDir, "tcp_checkpoints.json")
	tempFile := checkpointFile + ".tmp"

	if err := os.WriteFile(tempFile, data, 0644); err != nil {
		return fmt.Errorf("failed to write temporary checkpoint file: %w", err)
	}

	if err := os.Rename(tempFile, checkpointFile); err != nil {
		return fmt.Errorf("failed to rename checkpoint file: %w", err)
	}

	log.Printf("💾 CHECKPOINTS SAVED: %d stream checkpoints to %s", len(checkpoints), checkpointFile)
	return nil
}
