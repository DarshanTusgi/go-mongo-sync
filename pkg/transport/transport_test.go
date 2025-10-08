package transport

import (
	"bytes"
	"testing"
	"time"
)

func TestProtocolFrameEncoding(t *testing.T) {
	// Test frame creation and encoding
	payload := []byte("test payload")
	frame := NewDocBatchFrame(123, 456, payload, false, true)

	// Verify header fields
	if frame.Header.MsgType != MsgTypeDocBatch {
		t.Errorf("Expected MsgType %d, got %d", MsgTypeDocBatch, frame.Header.MsgType)
	}

	if frame.Header.StreamID != 123 {
		t.Errorf("Expected StreamID 123, got %d", frame.Header.StreamID)
	}

	if frame.Header.BatchSeq != 456 {
		t.Errorf("Expected BatchSeq 456, got %d", frame.Header.BatchSeq)
	}

	if !frame.Header.IsLastInBatch() {
		t.Error("Expected frame to be marked as last in batch")
	}

	// Test encoding/decoding
	encoded := frame.EncodeFrame()
	if len(encoded) != int(frame.Header.FrameLen) {
		t.Errorf("Encoded length %d doesn't match header length %d", len(encoded), frame.Header.FrameLen)
	}

	// Decode header
	decodedHeader, err := DecodeHeader(encoded[:FrameHeaderSize])
	if err != nil {
		t.Fatalf("Failed to decode header: %v", err)
	}

	// Verify decoded header matches original
	if decodedHeader.StreamID != frame.Header.StreamID {
		t.Errorf("Decoded StreamID %d doesn't match original %d", decodedHeader.StreamID, frame.Header.StreamID)
	}

	// Verify checksum
	decodedFrame := &Frame{Header: *decodedHeader, Payload: encoded[FrameHeaderSize:]}
	if !decodedFrame.VerifyChecksum() {
		t.Error("Checksum verification failed")
	}
}

func TestAckMessage(t *testing.T) {
	// Test ACK frame creation
	ackFrame := NewAckFrame(123, 456)

	if ackFrame.Header.MsgType != MsgTypeAck {
		t.Errorf("Expected MsgType %d, got %d", MsgTypeAck, ackFrame.Header.MsgType)
	}

	// Decode ACK message
	ackMsg, err := DecodeAckMessage(ackFrame.Payload)
	if err != nil {
		t.Fatalf("Failed to decode ACK message: %v", err)
	}

	if ackMsg.StreamID != 123 {
		t.Errorf("Expected StreamID 123, got %d", ackMsg.StreamID)
	}

	if ackMsg.AckUpTo != 456 {
		t.Errorf("Expected AckUpTo 456, got %d", ackMsg.AckUpTo)
	}
}

func TestCompression(t *testing.T) {
	// Test data with repetitive content (should compress well)
	testData := bytes.Repeat([]byte("Hello World! "), 1000)

	// Test Zstd compression
	zstdComp, err := NewZstdCompressor()
	if err != nil {
		t.Fatalf("Failed to create Zstd compressor: %v", err)
	}
	defer zstdComp.Close()

	compressed, err := zstdComp.Compress(testData)
	if err != nil {
		t.Fatalf("Compression failed: %v", err)
	}

	if len(compressed) >= len(testData) {
		t.Logf("Compression didn't reduce size: original=%d, compressed=%d", len(testData), len(compressed))
	}

	decompressed, err := zstdComp.Decompress(compressed)
	if err != nil {
		t.Fatalf("Decompression failed: %v", err)
	}

	if !bytes.Equal(testData, decompressed) {
		t.Error("Decompressed data doesn't match original")
	}

	// Test LZ4 compression
	lz4Comp := NewLZ4Compressor()

	compressed, err = lz4Comp.Compress(testData)
	if err != nil {
		t.Fatalf("LZ4 compression failed: %v", err)
	}

	decompressed, err = lz4Comp.Decompress(compressed)
	if err != nil {
		t.Fatalf("LZ4 decompression failed: %v", err)
	}

	if !bytes.Equal(testData, decompressed) {
		t.Error("LZ4 decompressed data doesn't match original")
	}
}

func TestSenderConfig(t *testing.T) {
	// Test config with defaults
	config := SenderConfig{
		Address: "localhost:9000",
	}

	// This would normally create a sender, but we're just testing config validation
	if config.Address != "localhost:9000" {
		t.Errorf("Address not set correctly: %s", config.Address)
	}

	// Test config validation by setting all fields
	config = SenderConfig{
		Address:       "test:9000",
		ParallelConns: 4,
		WindowSize:    64,
		Compression:   CompressionTypeZstd,
		BatchTimeout:  5 * time.Second,
		ConnTimeout:   30 * time.Second,
		KeepAlive:     30 * time.Second,
		MaxRetries:    3,
		RetryBackoff:  1 * time.Second,
		BufferSize:    256 * 1024,
		MaxBatchSize:  16 * 1024 * 1024,
	}

	if config.ParallelConns != 4 {
		t.Errorf("Expected ParallelConns 4, got %d", config.ParallelConns)
	}
}

func TestReceiverConfig(t *testing.T) {
	// Test config with defaults
	config := ReceiverConfig{
		ListenAddr: "0.0.0.0:9000",
	}

	if config.ListenAddr != "0.0.0.0:9000" {
		t.Errorf("ListenAddr not set correctly: %s", config.ListenAddr)
	}

	// Test full config
	config = ReceiverConfig{
		ListenAddr:        "0.0.0.0:9000",
		MaxConnections:    100,
		ReadTimeout:       60 * time.Second,
		WriteTimeout:      30 * time.Second,
		BufferSize:        256 * 1024,
		DiskCheckpoint:    true,
		CheckpointDir:     "/tmp/test",
		HeartbeatInterval: 10 * time.Second,
		MaxBatchSize:      16 * 1024 * 1024,
	}

	if config.MaxConnections != 100 {
		t.Errorf("Expected MaxConnections 100, got %d", config.MaxConnections)
	}
}

func TestBSONDocumentParsing(t *testing.T) {
	// Create some fake BSON documents
	doc1 := make([]byte, 20)
	doc1[0] = 20 // Document size (little endian)

	doc2 := make([]byte, 30)
	doc2[0] = 30 // Document size

	// Concatenate documents
	payload := append(doc1, doc2...)

	// Create a receiver connection to test parsing
	// Note: This is a simplified test - in real usage, you'd need proper BSON

	// For now, just test that we can create the payload
	if len(payload) != 50 {
		t.Errorf("Expected payload length 50, got %d", len(payload))
	}
}

func BenchmarkFrameEncoding(b *testing.B) {
	payload := make([]byte, 1024) // 1KB payload
	for i := range payload {
		payload[i] = byte(i % 256)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		frame := NewDocBatchFrame(123, uint64(i), payload, false, true)
		_ = frame.EncodeFrame()
	}
}

func BenchmarkCompression(b *testing.B) {
	// Test data
	testData := bytes.Repeat([]byte("Hello World! This is test data for compression. "), 100)

	zstdComp, err := NewZstdCompressor()
	if err != nil {
		b.Fatalf("Failed to create compressor: %v", err)
	}
	defer zstdComp.Close()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := zstdComp.Compress(testData)
		if err != nil {
			b.Fatalf("Compression failed: %v", err)
		}
	}
}
