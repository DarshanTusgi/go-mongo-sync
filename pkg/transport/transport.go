// Package transport provides a high-performance TCP-based bulk data transfer library
// for transferring billions of MongoDB documents (BSON encoded) between clusters.
//
// Features:
// - Raw TCP with length-framed binary messages
// - Compression support (zstd, lz4, none)
// - Multiple parallel TCP connections
// - Sliding window protocol with ACKs
// - Resume capability for interrupted transfers
// - Production-grade reliability and fault tolerance
//
// Example usage:
//
//	// Sender
//	sender, err := transport.NewSender(transport.SenderConfig{
//		Address:       "receiver:9000",
//		ParallelConns: 4,
//		WindowSize:    64,
//		Compression:   transport.CompressionZstd,
//	})
//	if err != nil {
//		panic(err)
//	}
//	defer sender.Close()
//
//	batch := [][]byte{bsonDoc1, bsonDoc2, bsonDoc3}
//	err = sender.SendBatch("users", batch)
//	if err != nil {
//		log.Fatal(err)
//	}
//
//	// Receiver
//	receiver, err := transport.NewReceiver(transport.ReceiverConfig{
//		ListenAddr: "0.0.0.0:9000",
//	})
//	if err != nil {
//		panic(err)
//	}
//
//	receiver.OnBatch(func(stream string, batchSeq uint64, docs [][]byte) error {
//		// Apply docs to MongoDB via bulkWrite
//		return nil
//	})
//
//	receiver.Start()
package transport

import (
	"crypto/tls"
	"time"
)

// CompressionType defines the compression algorithm to use
type CompressionType int

const (
	CompressionTypeNone CompressionType = CompressionNone
	CompressionTypeZstd CompressionType = CompressionZstd
	CompressionTypeLZ4  CompressionType = CompressionLZ4
)

// SenderConfig holds configuration for the sender
type SenderConfig struct {
	// Address of the receiver (e.g., "receiver:9000")
	Address string

	// Number of parallel TCP connections to use (default: 4)
	ParallelConns int

	// Sliding window size for unacknowledged batches (default: 64)
	WindowSize int

	// Compression algorithm to use (default: none)
	Compression CompressionType

	// TLS configuration (optional)
	TLSConfig *tls.Config

	// Batch timeout for sending (default: 5s)
	BatchTimeout time.Duration

	// Connection timeout (default: 30s)
	ConnTimeout time.Duration

	// Keep alive interval (default: 30s)
	KeepAlive time.Duration

	// Maximum retry attempts (default: 3)
	MaxRetries int

	// Retry backoff (default: 1s)
	RetryBackoff time.Duration

	// Buffer size for each connection (default: 256KB)
	BufferSize int

	// Maximum batch size in bytes (default: 16MB)
	MaxBatchSize int
}

// ReceiverConfig holds configuration for the receiver
type ReceiverConfig struct {
	// Address to listen on (e.g., "0.0.0.0:9000")
	ListenAddr string

	// TLS configuration (optional)
	TLSConfig *tls.Config

	// Maximum number of concurrent connections (default: 100)
	MaxConnections int

	// Read timeout (default: 60s)
	ReadTimeout time.Duration

	// Write timeout (default: 30s)
	WriteTimeout time.Duration

	// Buffer size for incoming data (default: 256KB)
	BufferSize int

	// Enable disk-based checkpointing (default: false)
	DiskCheckpoint bool

	// Checkpoint directory (when disk checkpoint enabled)
	CheckpointDir string

	// Heartbeat interval (default: 10s)
	HeartbeatInterval time.Duration

	// Maximum batch size in bytes (default: 16MB)
	MaxBatchSize int
}

// BatchHandler is called when a batch of documents is received
// stream: the stream identifier
// batchSeq: sequence number of this batch
// docs: slice of BSON documents
type BatchHandler func(stream string, batchSeq uint64, docs [][]byte) error

// ErrorHandler is called when an error occurs
type ErrorHandler func(err error)

// SenderStats provides statistics about the sender
type SenderStats struct {
	BytesSent       uint64 // Total bytes sent
	BatchesSent     uint64 // Total batches sent
	DocumentsSent   uint64 // Total documents sent
	ConnectionCount int    // Current active connections
	ErrorCount      uint64 // Total errors encountered
	RetryCount      uint64 // Total retry attempts
	CompressedBytes uint64 // Total compressed bytes
	ActiveStreams   int    // Number of active streams
}

// ReceiverStats provides statistics about the receiver
type ReceiverStats struct {
	BytesReceived     uint64 // Total bytes received
	BatchesReceived   uint64 // Total batches received
	DocumentsReceived uint64 // Total documents received
	ConnectionCount   int    // Current active connections
	ErrorCount        uint64 // Total errors encountered
	DecompressedBytes uint64 // Total decompressed bytes
	ActiveStreams     int    // Number of active streams
}

// Sender provides high-performance bulk data sending capabilities
type Sender interface {
	// SendBatch sends a batch of BSON documents for a specific stream
	SendBatch(stream string, batch [][]byte) error

	// SendBatchAsync sends a batch asynchronously and returns immediately
	SendBatchAsync(stream string, batch [][]byte) error

	// WaitForAcks waits for all pending batches to be acknowledged
	WaitForAcks(timeout time.Duration) error

	// Resume resumes sending from a specific sequence number for a stream
	Resume(stream string, fromSeq uint64) error

	// Stats returns current sender statistics
	Stats() SenderStats

	// Close gracefully shuts down the sender
	Close() error
}

// Receiver provides high-performance bulk data receiving capabilities
type Receiver interface {
	// OnBatch registers a handler for incoming document batches
	OnBatch(handler BatchHandler)

	// OnError registers a handler for errors
	OnError(handler ErrorHandler)

	// Start begins listening for incoming connections
	Start() error

	// Stop gracefully shuts down the receiver
	Stop() error

	// Stats returns current receiver statistics
	Stats() ReceiverStats

	// GetCheckpoint returns the current checkpoint for a stream
	GetCheckpoint(stream string) uint64

	// SetCheckpoint sets the checkpoint for a stream
	SetCheckpoint(stream string, seq uint64) error
}

// NewSender creates a new sender instance with the given configuration
func NewSender(config SenderConfig) (Sender, error) {
	// Set defaults
	if config.ParallelConns <= 0 {
		config.ParallelConns = 4
	}
	if config.WindowSize <= 0 {
		config.WindowSize = 64
	}
	if config.BatchTimeout == 0 {
		config.BatchTimeout = 5 * time.Second
	}
	if config.ConnTimeout == 0 {
		config.ConnTimeout = 30 * time.Second
	}
	if config.KeepAlive == 0 {
		config.KeepAlive = 30 * time.Second
	}
	if config.MaxRetries == 0 {
		config.MaxRetries = 3
	}
	if config.RetryBackoff == 0 {
		config.RetryBackoff = 1 * time.Second
	}
	if config.BufferSize <= 0 {
		config.BufferSize = 256 * 1024 // 256KB
	}
	if config.MaxBatchSize <= 0 {
		config.MaxBatchSize = 16 * 1024 * 1024 // 16MB
	}

	return newTCPSender(config)
}

// NewReceiver creates a new receiver instance with the given configuration
func NewReceiver(config ReceiverConfig) (Receiver, error) {
	// Set defaults
	if config.MaxConnections <= 0 {
		config.MaxConnections = 100
	}
	if config.ReadTimeout == 0 {
		config.ReadTimeout = 60 * time.Second
	}
	if config.WriteTimeout == 0 {
		config.WriteTimeout = 30 * time.Second
	}
	if config.BufferSize <= 0 {
		config.BufferSize = 256 * 1024 // 256KB
	}
	if config.HeartbeatInterval == 0 {
		config.HeartbeatInterval = 10 * time.Second
	}
	if config.MaxBatchSize <= 0 {
		config.MaxBatchSize = 16 * 1024 * 1024 // 16MB
	}

	return newTCPReceiver(config)
}
