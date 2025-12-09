package transport

import (
	"context"
	"crypto/tls"
	"encoding/binary"
	"fmt"
	"io"
	"log"
	"net"
	"sync"
	"sync/atomic"
	"time"
)

// tcpSender implements the Sender interface using TCP connections
type tcpSender struct {
	config       SenderConfig
	connections  []*senderConnection
	streamStates map[string]*streamState
	compressor   Compressor
	stats        senderStats
	mu           sync.RWMutex
	ctx          context.Context
	cancel       context.CancelFunc
	wg           sync.WaitGroup
	closed       atomic.Bool
}

// streamState tracks the state of a specific stream
type streamState struct {
	streamID       uint64
	nextSeq        uint64
	pendingBatches map[uint64]*pendingBatch
	ackCh          chan uint64
	mu             sync.RWMutex
}

// pendingBatch represents a batch waiting for acknowledgment
type pendingBatch struct {
	data     []byte
	sentTime time.Time
	retries  int
}

// senderConnection represents a single TCP connection
type senderConnection struct {
	conn    net.Conn
	id      int
	sender  *tcpSender
	writeCh chan *Frame
	ackCh   chan *AckMessage
	ctx     context.Context
	cancel  context.CancelFunc
	mu      sync.Mutex
}

// senderStats holds atomic counters for statistics
type senderStats struct {
	bytesSent       atomic.Uint64
	batchesSent     atomic.Uint64
	documentsSent   atomic.Uint64
	errorCount      atomic.Uint64
	retryCount      atomic.Uint64
	compressedBytes atomic.Uint64
}

// newTCPSender creates a new TCP sender
func newTCPSender(config SenderConfig) (*tcpSender, error) {
	compressor, err := GetCompressor(config.Compression)
	if err != nil {
		return nil, fmt.Errorf("failed to create compressor: %w", err)
	}

	ctx, cancel := context.WithCancel(context.Background())

	sender := &tcpSender{
		config:       config,
		connections:  make([]*senderConnection, 0, config.ParallelConns),
		streamStates: make(map[string]*streamState),
		compressor:   compressor,
		ctx:          ctx,
		cancel:       cancel,
	}

	// Create parallel connections
	log.Printf("🔌 TCP SENDER: Creating %d parallel connections to %s", config.ParallelConns, config.Address)
	for i := 0; i < config.ParallelConns; i++ {
		log.Printf("🔌 TCP SENDER: Creating connection %d/%d to %s", i+1, config.ParallelConns, config.Address)
		conn, err := sender.createConnection(i)
		if err != nil {
			log.Printf("❌ TCP SENDER: Failed to create connection %d: %v", i, err)
			// Clean up existing connections
			sender.Close()
			return nil, fmt.Errorf("failed to create connection %d: %w", i, err)
		}
		log.Printf("✅ TCP SENDER: Connection %d created successfully", i+1)
		sender.connections = append(sender.connections, conn)
	}

	return sender, nil
}

// createConnection creates a new TCP connection with billion-document optimizations
func (s *tcpSender) createConnection(id int) (*senderConnection, error) {
	var conn net.Conn
	var err error

	// OPTIMIZED: Connect with extended timeout for billion-document reliability
	dialer := &net.Dialer{
		Timeout:   s.config.ConnTimeout,
		KeepAlive: s.config.KeepAlive,
	}

	log.Printf("🔌 TCP CONNECTION %d: Dialing %s (timeout=%v)", id, s.config.Address, s.config.ConnTimeout)
	if s.config.TLSConfig != nil {
		conn, err = tls.DialWithDialer(dialer, "tcp", s.config.Address, s.config.TLSConfig)
	} else {
		conn, err = dialer.Dial("tcp", s.config.Address)
	}

	if err != nil {
		log.Printf("❌ TCP CONNECTION %d: Failed to dial %s: %v", id, s.config.Address, err)
		return nil, fmt.Errorf("failed to connect to %s: %w", s.config.Address, err)
	}
	log.Printf("✅ TCP CONNECTION %d: Successfully connected to %s", id, s.config.Address)

	// OPTIMIZED: Set TCP options for billion-document performance
	if tcpConn, ok := conn.(*net.TCPConn); ok {
		tcpConn.SetNoDelay(true)                       // Disable Nagle's algorithm for low latency
		tcpConn.SetKeepAlive(true)                     // Enable keep-alive
		tcpConn.SetKeepAlivePeriod(s.config.KeepAlive) // Set keep-alive period

		// OPTIMIZED: Set socket buffer sizes for high throughput
		if err := tcpConn.SetReadBuffer(s.config.BufferSize); err != nil {
			log.Printf("Warning: Failed to set read buffer size: %v", err)
		}
		if err := tcpConn.SetWriteBuffer(s.config.BufferSize); err != nil {
			log.Printf("Warning: Failed to write buffer size: %v", err)
		}
	}

	ctx, cancel := context.WithCancel(s.ctx)

	senderConn := &senderConnection{
		conn:    conn,
		id:      id,
		sender:  s,
		writeCh: make(chan *Frame, 1000),      // OPTIMIZED: Larger channel for billion-document throughput
		ackCh:   make(chan *AckMessage, 1000), // OPTIMIZED: Larger ACK channel
		ctx:     ctx,
		cancel:  cancel,
	}

	s.wg.Add(2)
	go senderConn.writeLoopOptimized() // Use optimized write loop
	go senderConn.readLoopOptimized()  // Use optimized read loop

	return senderConn, nil
}

// writeLoopOptimized handles writing frames with billion-document optimizations
func (sc *senderConnection) writeLoopOptimized() {
	defer sc.sender.wg.Done()
	defer log.Printf("🔧 TCP WRITE LOOP EXIT: connection %d", sc.id)

	// OPTIMIZED: Batch multiple frames for better throughput
	batchBuffer := make([]*Frame, 0, 100)             // Buffer up to 100 frames
	flushTimer := time.NewTimer(5 * time.Millisecond) // Flush every 5ms
	defer flushTimer.Stop()

	log.Printf("🚀 TCP WRITE LOOP: connection %d started", sc.id)

	for {
		select {
		case frame := <-sc.writeCh:
			batchBuffer = append(batchBuffer, frame)

			// Flush if buffer is full or it's a high-priority frame
			if len(batchBuffer) >= 100 || frame.Header.MsgType == MsgTypeAck {
				sc.flushFrameBatch(batchBuffer)
				batchBuffer = batchBuffer[:0] // Reset buffer
				flushTimer.Reset(5 * time.Millisecond)
			}

		case <-flushTimer.C:
			// Periodic flush to ensure frames don't get stuck
			if len(batchBuffer) > 0 {
				sc.flushFrameBatch(batchBuffer)
				batchBuffer = batchBuffer[:0]
			}
			flushTimer.Reset(5 * time.Millisecond)

		case <-sc.ctx.Done():
			// Final flush before exit
			if len(batchBuffer) > 0 {
				sc.flushFrameBatch(batchBuffer)
			}
			log.Printf("🔧 TCP WRITE LOOP: connection %d shutting down", sc.id)
			return
		}
	}
}

// flushFrameBatch sends a batch of frames in one operation
func (sc *senderConnection) flushFrameBatch(frames []*Frame) {
	if len(frames) == 0 {
		return
	}

	// Calculate total size
	totalSize := 0
	for _, frame := range frames {
		totalSize += len(frame.EncodeFrame())
	}

	// Create combined buffer
	combinedData := make([]byte, 0, totalSize)
	for _, frame := range frames {
		frameData := frame.EncodeFrame()
		combinedData = append(combinedData, frameData...)
	}

	// Send all frames in one write operation for better performance
	sc.mu.Lock()
	sc.conn.SetWriteDeadline(time.Now().Add(sc.sender.config.ConnTimeout))
	_, err := sc.conn.Write(combinedData)
	sc.mu.Unlock()

	if err != nil {
		sc.sender.stats.errorCount.Add(uint64(len(frames)))
		log.Printf("❌ BATCH WRITE ERROR: Failed to send %d frames: %v", len(frames), err)
		// Don't crash the goroutine - continue processing
		sc.sender.handleConnectionError(sc, err)
	} else {
		log.Printf("📤 BATCH SENT: %d frames (%s) via connection %d", len(frames), formatBytes(totalSize), sc.id)
	}
}

// readLoopOptimized handles reading ACKs with ultra-stable production architecture
func (sc *senderConnection) readLoopOptimized() {
	defer sc.sender.wg.Done()

	// ULTRA-STABLE: Enhanced buffer and comprehensive error tracking
	buf := make([]byte, sc.sender.config.BufferSize*3) // Triple buffer size for stability
	headerBuf := make([]byte, FrameHeaderSize)
	var consecutiveErrors, timeoutCount, ioErrors int
	var lastActivity = time.Now()
	maxConsecutiveErrors, maxTimeoutErrors, maxIOErrors := 20, 15, 8 // Increased thresholds
	baseTimeout := sc.sender.config.ConnTimeout
	// STABILITY FIX: Disable idle timeout - rely on heartbeats instead
	// TCP connections should stay open indefinitely if heartbeats are working
	// maxIdleTime := 10 * time.Minute // Extended idle timeout
	var maxIdleTime time.Duration = 0 // No idle timeout - connections stay open

	log.Printf("🚀 TCP SENDER READ LOOP: connection %d started (ultra-stable)", sc.id)

	for {
		select {
		case <-sc.ctx.Done():
			log.Printf("📋 TCP SENDER SHUTDOWN: connection %d", sc.id)
			return
		default:
			// STABILITY FIX: Only check idle timeout if it's actually configured
			if maxIdleTime > 0 && time.Since(lastActivity) > maxIdleTime {
				log.Printf("⏰ TCP SENDER IDLE: connection %d (idle %v)", sc.id, time.Since(lastActivity))
				sc.sender.handleConnectionError(sc, fmt.Errorf("connection idle timeout"))
				return
			}

			// ULTRA-STABLE: Intelligent timeout adaptation
			readTimeout := baseTimeout
			if consecutiveErrors > 0 {
				// Adaptive timeout: base * (1 + errors * 0.25)
				backoffMultiplier := 1.0 + float64(consecutiveErrors)*0.25
				readTimeout = time.Duration(float64(baseTimeout) * backoffMultiplier)
				if readTimeout > 200*time.Second {
					readTimeout = 200 * time.Second // Cap at 200 seconds
				}
			}

			// STABILITY FIX: Don't set immediate read timeout on new connections
			// VM-sync doesn't send data until it receives data first
			if time.Since(lastActivity) < 5*time.Second {
				// New connection - give it time before expecting data
				time.Sleep(100 * time.Millisecond)
				continue
			}

			// Set read timeout with safety margin
			sc.conn.SetReadDeadline(time.Now().Add(readTimeout))

			// ULTRA-STABLE: Header read with comprehensive error classification
			n, err := sc.conn.Read(headerBuf)
			if err != nil {
				// Handle timeout errors with progressive backoff
				if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
					timeoutCount++
					consecutiveErrors++
					if timeoutCount >= maxTimeoutErrors || consecutiveErrors >= maxConsecutiveErrors {
						log.Printf("🔴 TCP SENDER ERROR LIMIT: connection %d (timeouts: %d, errors: %d)", sc.id, timeoutCount, consecutiveErrors)
						sc.sender.handleConnectionError(sc, fmt.Errorf("too many errors: timeouts=%d, total=%d", timeoutCount, consecutiveErrors))
						return
					}
					// Progressive exponential backoff with randomization
					backoffBase := time.Duration(200*consecutiveErrors*consecutiveErrors) * time.Millisecond
					if backoffBase > 8*time.Second {
						backoffBase = 8 * time.Second
					}
					log.Printf("⏱️ TCP SENDER TIMEOUT: connection %d (attempt %d/%d, backoff %v)", sc.id, timeoutCount, maxTimeoutErrors, backoffBase)
					time.Sleep(backoffBase)
					continue
				}

				// Handle EOF and I/O errors
				if err == io.EOF {
					log.Printf("📋 TCP SENDER EOF: connection %d", sc.id)
				} else {
					ioErrors++
					consecutiveErrors++
					if ioErrors >= maxIOErrors {
						log.Printf("🔴 TCP SENDER IO LIMIT: connection %d (%d io errors)", sc.id, ioErrors)
					}
					log.Printf("🔴 TCP SENDER READ ERROR: connection %d: %v", sc.id, err)
				}
				sc.sender.handleConnectionError(sc, err)
				return
			}

			// ULTRA-STABLE: Reset all error counters on successful read
			consecutiveErrors = 0
			timeoutCount = 0
			ioErrors = 0
			lastActivity = time.Now()

			// ULTRA-STABLE: Validate header size
			if n != FrameHeaderSize {
				log.Printf("⚠️ TCP SENDER INVALID HEADER: connection %d (got %d bytes)", sc.id, n)
				continue
			}

			// ULTRA-STABLE: Decode and validate header
			header, err := DecodeHeader(headerBuf)
			if err != nil {
				log.Printf("🔴 TCP SENDER DECODE ERROR: connection %d: %v", sc.id, err)
				sc.sender.stats.errorCount.Add(1)
				continue
			}
			if err := header.Validate(); err != nil {
				log.Printf("🔴 TCP SENDER VALIDATION ERROR: connection %d: %v", sc.id, err)
				sc.sender.stats.errorCount.Add(1)
				continue
			}

			// ULTRA-STABLE: Handle payload with comprehensive validation
			payloadSize := header.PayloadSize()
			var payload []byte
			if payloadSize > 0 {
				if payloadSize > uint32(sc.sender.config.MaxBatchSize) {
					log.Printf("🔴 TCP SENDER PAYLOAD TOO LARGE: connection %d (%d bytes)", sc.id, payloadSize)
					sc.sender.stats.errorCount.Add(1)
					continue
				}
				if payloadSize > uint32(len(buf)) {
					log.Printf("📦 TCP SENDER BUFFER RESIZE: connection %d (to %d bytes)", sc.id, payloadSize)
					buf = make([]byte, payloadSize*2)
				}
				payload = buf[:payloadSize]

				// ULTRA-STABLE: Complete payload read with timeout handling
				sc.conn.SetReadDeadline(time.Now().Add(readTimeout))
				bytesRead := 0
				for bytesRead < int(payloadSize) {
					n, err := sc.conn.Read(payload[bytesRead:])
					if err != nil {
						log.Printf("🔴 TCP SENDER PAYLOAD READ ERROR: connection %d: %v", sc.id, err)
						sc.sender.handleConnectionError(sc, err)
						return
					}
					bytesRead += n
				}
				lastActivity = time.Now()
			}

			// ULTRA-STABLE: Handle message types with enhanced error resilience
			switch header.MsgType {
			case MsgTypeAck:
				sc.handleAckOptimized(payload)
			case MsgTypeResumeResponse:
				sc.handleResumeResponse(payload)
			case MsgTypeHeartbeat:
				lastActivity = time.Now()
			default:
				log.Printf("⚠️ TCP SENDER UNKNOWN MESSAGE: connection %d (type %d)", sc.id, header.MsgType)
			}
		}
	}
}

// handleAckOptimized processes acknowledgments with better performance
func (sc *senderConnection) handleAckOptimized(payload []byte) {
	ack, err := DecodeAckMessage(payload)
	if err != nil {
		sc.sender.stats.errorCount.Add(1)
		return
	}

	// Find the stream and update pending batches
	sc.sender.mu.RLock()
	var targetState *streamState
	for _, state := range sc.sender.streamStates {
		if state.streamID == ack.StreamID {
			targetState = state
			break
		}
	}
	sc.sender.mu.RUnlock()

	if targetState != nil {
		targetState.mu.Lock()
		// Remove acknowledged batches
		ackedCount := 0
		for seq := range targetState.pendingBatches {
			if seq <= ack.AckUpTo {
				delete(targetState.pendingBatches, seq)
				ackedCount++
			}
		}
		targetState.mu.Unlock()

		if ackedCount > 0 {
			log.Printf("✅ ACK PROCESSED: stream=%d, acked_up_to=%d, freed=%d_batches",
				ack.StreamID, ack.AckUpTo, ackedCount)
		}

		// Notify waiters
		select {
		case targetState.ackCh <- ack.AckUpTo:
		default:
		}
	}
}

// SendBatch sends a batch of BSON documents
func (s *tcpSender) SendBatch(stream string, batch [][]byte) error {
	log.Printf("📤 TCP SEND: Sending batch for stream %s (%d documents)", stream, len(batch))
	
	// ❗ CRITICAL: Check if this is an incremental stream (needs ACK for durability)
	isIncremental := len(stream) > 12 && stream[len(stream)-12:] == ".incremental"
	
	if isIncremental {
		// Incremental sync: Wait for ACK before returning (at-least-once delivery)
		log.Printf("🔥 INCREMENTAL SYNC: Will wait for ACK before returning (durability guarantee)")
		return s.sendBatchWithAck(stream, batch)
	} else {
		// Initial dump: Fire-and-forget (high performance)
		log.Printf("⚡ INITIAL SYNC: Fire-and-forget mode (high performance)")
		return s.sendBatch(stream, batch, false)
	}
}

// SendBatchAsync sends a batch asynchronously
func (s *tcpSender) SendBatchAsync(stream string, batch [][]byte) error {
	return s.sendBatch(stream, batch, true)
}

// sendBatchWithAck sends a batch and waits for ACK (for incremental sync durability)
func (s *tcpSender) sendBatchWithAck(stream string, batch [][]byte) error {
	if s.closed.Load() {
		return fmt.Errorf("sender is closed")
	}
	
	if len(batch) == 0 {
		return fmt.Errorf("empty batch")
	}
	
	// Get or create stream state
	state := s.getOrCreateStreamState(stream)
	
	// Send the batch (fire-and-forget style initially)
	if err := s.sendBatch(stream, batch, false); err != nil {
		return fmt.Errorf("failed to send batch: %w", err)
	}
	
	// Get the sequence number we just sent
	state.mu.RLock()
	sentSeq := state.nextSeq - 1 // We just incremented nextSeq in sendBatch
	state.mu.RUnlock()
	
	// Wait for ACK with timeout (30 seconds for incremental)
	ackTimeout := 30 * time.Second
	ackDeadline := time.Now().Add(ackTimeout)
	
	log.Printf("⏳ WAITING FOR ACK: stream=%s, seq=%d, timeout=%v", stream, sentSeq, ackTimeout)
	
	for time.Now().Before(ackDeadline) {
		// Check if this sequence has been acknowledged
		state.mu.RLock()
		_, stillPending := state.pendingBatches[sentSeq]
		state.mu.RUnlock()
		
		if !stillPending {
			// ACK received!
			log.Printf("✅ ACK RECEIVED: stream=%s, seq=%d", stream, sentSeq)
			return nil
		}
		
		// Wait for ACK notification or timeout
		select {
		case ackedUpTo := <-state.ackCh:
			if ackedUpTo >= sentSeq {
				log.Printf("✅ ACK RECEIVED: stream=%s, seq=%d (acked_up_to=%d)", stream, sentSeq, ackedUpTo)
				return nil
			}
			// Continue waiting if ACK is for an older batch
			
		case <-time.After(100 * time.Millisecond):
			// Check again
			
		case <-s.ctx.Done():
			return fmt.Errorf("sender is shutting down")
		}
	}
	
	// Timeout - no ACK received
	log.Printf("🔴 ACK TIMEOUT: stream=%s, seq=%d after %v", stream, sentSeq, ackTimeout)
	return fmt.Errorf("ACK timeout after %v for stream %s seq %d", ackTimeout, stream, sentSeq)
}

// sendBatch implements the core batch sending logic with streaming optimization
func (s *tcpSender) sendBatch(stream string, batch [][]byte, async bool) error {
	if s.closed.Load() {
		return fmt.Errorf("sender is closed")
	}

	if len(batch) == 0 {
		return fmt.Errorf("empty batch")
	}

	// Get or create stream state
	state := s.getOrCreateStreamState(stream)

	// OPTIMIZED: Stream large batches to prevent memory exhaustion
	if len(batch) > 10000 { // For very large batches (billions scenario)
		return s.sendLargeBatchStreaming(stream, batch, async, state)
	}

	// Standard batch processing for smaller batches
	return s.sendStandardBatch(stream, batch, async, state)
}

// sendLargeBatchStreaming handles massive batches with streaming to prevent memory issues
func (s *tcpSender) sendLargeBatchStreaming(stream string, batch [][]byte, async bool, state *streamState) error {
	// Process in chunks to manage memory usage for billion-document transfers
	const chunkSize = 5000 // Process 5K documents at a time
	totalChunks := (len(batch) + chunkSize - 1) / chunkSize

	log.Printf("🚀 STREAMING LARGE BATCH: %s - %d documents in %d chunks", stream, len(batch), totalChunks)

	for i := 0; i < len(batch); i += chunkSize {
		end := i + chunkSize
		if end > len(batch) {
			end = len(batch)
		}

		chunk := batch[i:end]
		chunkNumber := (i / chunkSize) + 1

		log.Printf("📦 CHUNK %d/%d: Processing %d documents", chunkNumber, totalChunks, len(chunk))

		if err := s.sendStandardBatch(stream, chunk, async, state); err != nil {
			return fmt.Errorf("failed to send chunk %d/%d: %w", chunkNumber, totalChunks, err)
		}

		// Brief pause between chunks to prevent overwhelming the system
		if !async && chunkNumber < totalChunks {
			time.Sleep(10 * time.Millisecond)
		}
	}

	log.Printf("✅ STREAMING COMPLETE: %s - %d documents sent successfully", stream, len(batch))
	return nil
}

// sendStandardBatch handles normal-sized batches
func (s *tcpSender) sendStandardBatch(stream string, batch [][]byte, async bool, state *streamState) error {
	// Concatenate BSON documents
	payload, err := s.concatenateBSONDocs(batch)
	if err != nil {
		return fmt.Errorf("failed to concatenate BSON documents: %w", err)
	}

	// CRITICAL FIX: Prepend stream name to payload so receiver can identify the collection
	// Format: [stream_name_length:4][stream_name][bson_documents]
	streamNameBytes := []byte(stream)
	streamNameLength := uint32(len(streamNameBytes))

	// Create payload with stream name prefix
	lengthBytes := make([]byte, 4)
	binary.BigEndian.PutUint32(lengthBytes, streamNameLength)

	finalPayload := make([]byte, 0, 4+len(streamNameBytes)+len(payload))
	finalPayload = append(finalPayload, lengthBytes...)
	finalPayload = append(finalPayload, streamNameBytes...)
	finalPayload = append(finalPayload, payload...)

	payload = finalPayload

	// Check batch size limit
	if len(payload) > s.config.MaxBatchSize {
		return fmt.Errorf("batch size %d exceeds limit %d", len(payload), s.config.MaxBatchSize)
	}

	// Compress if configured
	compressed := false
	if s.config.Compression != CompressionTypeNone {
		compressedPayload, err := s.compressor.Compress(payload)
		if err != nil {
			return fmt.Errorf("compression failed: %w", err)
		}

		// Use compression only if it reduces size significantly (10% threshold)
		if len(compressedPayload) < int(float64(len(payload))*0.9) {
			payload = compressedPayload
			compressed = true
			s.stats.compressedBytes.Add(uint64(len(payload)))
			log.Printf("📦 COMPRESSION: %d docs compressed from %d to %d bytes (%.1f%% reduction)",
				len(batch), len(compressedPayload), len(payload),
				(1.0-float64(len(payload))/float64(len(compressedPayload)))*100)
		}
	}

	// Get next sequence number
	state.mu.Lock()
	seq := state.nextSeq
	state.nextSeq++
	state.mu.Unlock()

	// Create frame
	frame := NewDocBatchFrame(state.streamID, seq, payload, compressed, true)

	// Wait for window space if not async
	if !async {
		if err := s.waitForWindow(state); err != nil {
			return err
		}
	}

	// Add to pending batches
	state.mu.Lock()
	if state.pendingBatches == nil {
		state.pendingBatches = make(map[uint64]*pendingBatch)
	}
	state.pendingBatches[seq] = &pendingBatch{
		data:     frame.EncodeFrame(),
		sentTime: time.Now(),
		retries:  0,
	}
	state.mu.Unlock()

	// Send frame to a connection (round-robin)
	connIndex := int(seq) % len(s.connections)
	conn := s.connections[connIndex]

	select {
	case conn.writeCh <- frame:
		// Update stats
		s.stats.bytesSent.Add(uint64(len(payload)))
		s.stats.batchesSent.Add(1)
		s.stats.documentsSent.Add(uint64(len(batch)))
		return nil
	case <-s.ctx.Done():
		return fmt.Errorf("sender is shutting down")
	case <-time.After(s.config.BatchTimeout):
		return fmt.Errorf("timeout sending batch")
	}
}

// concatenateBSONDocs concatenates multiple BSON documents into a single payload
func (s *tcpSender) concatenateBSONDocs(docs [][]byte) ([]byte, error) {
	totalSize := 0
	for _, doc := range docs {
		totalSize += len(doc)
	}

	result := make([]byte, 0, totalSize)
	for _, doc := range docs {
		result = append(result, doc...)
	}

	return result, nil
}

// getOrCreateStreamState gets or creates a stream state
func (s *tcpSender) getOrCreateStreamState(stream string) *streamState {
	s.mu.Lock()
	defer s.mu.Unlock()

	state, exists := s.streamStates[stream]
	if !exists {
		state = &streamState{
			streamID:       s.hashStreamName(stream),
			nextSeq:        1,
			pendingBatches: make(map[uint64]*pendingBatch),
			ackCh:          make(chan uint64, 100),
		}
		s.streamStates[stream] = state
	}

	return state
}

// hashStreamName creates a hash of the stream name for the stream ID
func (s *tcpSender) hashStreamName(stream string) uint64 {
	// Simple hash function - in production, consider using a proper hash
	var hash uint64 = 5381
	for i := 0; i < len(stream); i++ {
		hash = ((hash << 5) + hash) + uint64(stream[i])
	}
	return hash
}

// waitForWindow waits until there's space in the sliding window
func (s *tcpSender) waitForWindow(state *streamState) error {
	for {
		state.mu.RLock()
		pendingCount := len(state.pendingBatches)
		state.mu.RUnlock()

		if pendingCount < s.config.WindowSize {
			return nil
		}

		// Wait for an ACK or timeout
		select {
		case <-state.ackCh:
			// Continue loop to check window size again
		case <-time.After(s.config.BatchTimeout):
			return fmt.Errorf("timeout waiting for window space")
		case <-s.ctx.Done():
			return fmt.Errorf("sender is shutting down")
		}
	}
}

// WaitForAcks waits for all pending batches to be acknowledged
func (s *tcpSender) WaitForAcks(timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	startTime := time.Now()

	for time.Now().Before(deadline) {
		allAcked := true

		s.mu.RLock()
		for _, state := range s.streamStates {
			state.mu.RLock()
			if len(state.pendingBatches) > 0 {
				allAcked = false
			}
			state.mu.RUnlock()
			if !allAcked {
				break
			}
		}
		s.mu.RUnlock()

		if allAcked {
			log.Printf("✅ WAIT FOR ACKS: All batches acknowledged in %v", time.Since(startTime))
			return nil
		}

		time.Sleep(100 * time.Millisecond)
	}

	// ENHANCED DIAGNOSTICS: Log which streams still have pending batches
	s.mu.RLock()
	var pendingStreams []string
	var totalPending int
	for streamName, state := range s.streamStates {
		state.mu.RLock()
		pendingCount := len(state.pendingBatches)
		if pendingCount > 0 {
			pendingStreams = append(pendingStreams, fmt.Sprintf("%s(%d)", streamName, pendingCount))
			totalPending += pendingCount
		}
		state.mu.RUnlock()
	}
	s.mu.RUnlock()

	if len(pendingStreams) > 0 {
		log.Printf("❌ ACK TIMEOUT DIAGNOSTICS: %d streams with %d pending batches after %v: %v",
			len(pendingStreams), totalPending, timeout, pendingStreams)
	}

	return fmt.Errorf("timeout waiting for acknowledgments")
}

// Resume resumes sending from a specific sequence number with billion-document optimizations
func (s *tcpSender) Resume(stream string, fromSeq uint64) error {
	if s.closed.Load() {
		return fmt.Errorf("sender is closed")
	}

	log.Printf("🔄 RESUME REQUEST: stream=%s, from_seq=%d", stream, fromSeq)

	state := s.getOrCreateStreamState(stream)

	// Update stream state for resume
	state.mu.Lock()
	state.nextSeq = fromSeq
	// Clear pending batches older than resume point
	for seq := range state.pendingBatches {
		if seq < fromSeq {
			delete(state.pendingBatches, seq)
		}
	}
	state.mu.Unlock()

	// Send resume request to all connections for reliability
	frame := NewResumeRequestFrame(state.streamID, fromSeq)
	errors := make([]error, 0)

	for i, conn := range s.connections {
		select {
		case conn.writeCh <- frame:
			log.Printf("🔄 RESUME SENT: stream=%s, from_seq=%d via connection %d", stream, fromSeq, i)
		case <-time.After(s.config.BatchTimeout):
			errors = append(errors, fmt.Errorf("timeout sending resume request to connection %d", i))
		case <-s.ctx.Done():
			errors = append(errors, fmt.Errorf("sender shutting down during resume"))
		}
	}

	// If any connection succeeded, consider resume successful
	if len(errors) < len(s.connections) {
		log.Printf("✅ RESUME SUCCESS: stream=%s, from_seq=%d (%d/%d connections)",
			stream, fromSeq, len(s.connections)-len(errors), len(s.connections))
		return nil
	}

	// All connections failed
	return fmt.Errorf("all connections failed during resume: %v", errors)
}

// handleConnectionError handles connection-specific errors with proper logging
func (s *tcpSender) handleConnectionError(conn *senderConnection, err error) {
	log.Printf("🔴 TCP CONNECTION ERROR: connection %d: %v", conn.id, err)
	s.stats.errorCount.Add(1)

	// Cancel the connection context to trigger cleanup
	conn.cancel()

	// Log for monitoring and debugging
	log.Printf("🔧 TCP CONNECTION CLEANUP: connection %d initiated", conn.id)
}

// ResumeWithRetry implements automatic retry logic for resume operations
func (s *tcpSender) ResumeWithRetry(stream string, fromSeq uint64, maxRetries int) error {
	var lastErr error

	for attempt := 1; attempt <= maxRetries; attempt++ {
		log.Printf("🔄 RESUME ATTEMPT %d/%d: stream=%s, from_seq=%d", attempt, maxRetries, stream, fromSeq)

		if err := s.Resume(stream, fromSeq); err == nil {
			log.Printf("✅ RESUME SUCCESS: stream=%s after %d attempts", stream, attempt)
			return nil
		} else {
			lastErr = err
			log.Printf("❌ RESUME ATTEMPT %d FAILED: %v", attempt, err)

			if attempt < maxRetries {
				backoff := time.Duration(attempt) * s.config.RetryBackoff
				log.Printf("⏳ RESUME RETRY: waiting %v before attempt %d", backoff, attempt+1)
				time.Sleep(backoff)
			}
		}
	}

	return fmt.Errorf("resume failed after %d attempts: %w", maxRetries, lastErr)
}

// formatBytes formats byte count as human readable string
func formatBytes(bytes int) string {
	if bytes < 1024 {
		return fmt.Sprintf("%d B", bytes)
	} else if bytes < 1024*1024 {
		return fmt.Sprintf("%.1f KB", float64(bytes)/1024)
	} else {
		return fmt.Sprintf("%.2f MB", float64(bytes)/(1024*1024))
	}
}

// Stats returns current sender statistics
func (s *tcpSender) Stats() SenderStats {
	s.mu.RLock()
	activeStreams := len(s.streamStates)
	s.mu.RUnlock()

	return SenderStats{
		BytesSent:       s.stats.bytesSent.Load(),
		BatchesSent:     s.stats.batchesSent.Load(),
		DocumentsSent:   s.stats.documentsSent.Load(),
		ConnectionCount: len(s.connections),
		ErrorCount:      s.stats.errorCount.Load(),
		RetryCount:      s.stats.retryCount.Load(),
		CompressedBytes: s.stats.compressedBytes.Load(),
		ActiveStreams:   activeStreams,
	}
}

// Close gracefully shuts down the sender
func (s *tcpSender) Close() error {
	if s.closed.Swap(true) {
		return nil // Already closed
	}

	// Cancel context to signal shutdown
	s.cancel()

	// Close all connections
	for _, conn := range s.connections {
		conn.cancel()
		conn.conn.Close()
	}

	// Wait for goroutines to finish
	s.wg.Wait()

	// Close compressor if it supports closing
	if closer, ok := s.compressor.(interface{ Close() }); ok {
		closer.Close()
	}

	return nil
}

// writeLoop handles writing frames to the connection
func (sc *senderConnection) writeLoop() {
	defer sc.sender.wg.Done()

	for {
		select {
		case frame := <-sc.writeCh:
			if err := sc.writeFrame(frame); err != nil {
				sc.sender.stats.errorCount.Add(1)
				// TODO: Implement retry logic
			}
		case <-sc.ctx.Done():
			return
		}
	}
}

// readLoop handles reading ACKs and responses from the connection
func (sc *senderConnection) readLoop() {
	defer sc.sender.wg.Done()

	buf := make([]byte, sc.sender.config.BufferSize)
	headerBuf := make([]byte, FrameHeaderSize)

	for {
		select {
		case <-sc.ctx.Done():
			return
		default:
			// Set read timeout
			sc.conn.SetReadDeadline(time.Now().Add(sc.sender.config.ConnTimeout))

			// Read header
			_, err := sc.conn.Read(headerBuf)
			if err != nil {
				sc.sender.stats.errorCount.Add(1)
				continue
			}

			header, err := DecodeHeader(headerBuf)
			if err != nil {
				sc.sender.stats.errorCount.Add(1)
				continue
			}

			if err := header.Validate(); err != nil {
				sc.sender.stats.errorCount.Add(1)
				continue
			}

			// Read payload if present
			payloadSize := header.PayloadSize()
			var payload []byte
			if payloadSize > 0 {
				if payloadSize > uint32(len(buf)) {
					buf = make([]byte, payloadSize)
				}
				payload = buf[:payloadSize]
				_, err = sc.conn.Read(payload)
				if err != nil {
					sc.sender.stats.errorCount.Add(1)
					continue
				}
			}

			// Handle different message types
			switch header.MsgType {
			case MsgTypeAck:
				sc.handleAck(payload)
			case MsgTypeResumeResponse:
				sc.handleResumeResponse(payload)
			}
		}
	}
}

// writeFrame writes a frame to the connection
func (sc *senderConnection) writeFrame(frame *Frame) error {
	sc.mu.Lock()
	defer sc.mu.Unlock()

	data := frame.EncodeFrame()
	sc.conn.SetWriteDeadline(time.Now().Add(sc.sender.config.ConnTimeout))

	_, err := sc.conn.Write(data)
	return err
}

// handleAck processes an acknowledgment message
func (sc *senderConnection) handleAck(payload []byte) {
	ack, err := DecodeAckMessage(payload)
	if err != nil {
		sc.sender.stats.errorCount.Add(1)
		return
	}

	// Find the stream and update pending batches
	sc.sender.mu.RLock()
	var targetState *streamState
	for _, state := range sc.sender.streamStates {
		if state.streamID == ack.StreamID {
			targetState = state
			break
		}
	}
	sc.sender.mu.RUnlock()

	if targetState != nil {
		targetState.mu.Lock()
		// Remove acknowledged batches
		for seq := range targetState.pendingBatches {
			if seq <= ack.AckUpTo {
				delete(targetState.pendingBatches, seq)
			}
		}
		targetState.mu.Unlock()

		// Notify waiters
		select {
		case targetState.ackCh <- ack.AckUpTo:
		default:
		}
	}
}

// handleResumeResponse processes a resume response message
func (sc *senderConnection) handleResumeResponse(payload []byte) {
	msg, err := DecodeResumeMessage(payload, true)
	if err != nil {
		sc.sender.stats.errorCount.Add(1)
		return
	}

	// TODO: Handle resume response
	_ = msg
}
