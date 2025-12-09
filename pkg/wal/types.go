package wal

import (
	"errors"
	"time"
)

// WAL constants - sensible defaults optimized for thousands of documents
const (
	// Segment settings (Kafka-style)
	MaxSegmentSize   = 64 * 1024 * 1024 // 64MB segments (like RocksDB, etcd)
	BlockSize        = 32 * 1024        // 32KB blocks (like RocksDB)
	MaxSegments      = 1000              // Alert threshold
	
	// Performance - optimized for high throughput
	GroupCommitMaxEntries = 100                   // Batch up to 100 entries before fsync
	GroupCommitMaxDelay   = 10 * time.Millisecond // Max 10ms wait before forcing fsync
	
	// Compaction (like Kafka log retention)
	CompactionInterval    = 5 * time.Minute // Compact every 5 minutes
	MinAppliedRatio       = 0.90            // Compact if > 90% applied
	
	// Apply workers (parallel MongoDB writes)
	ApplyWorkerCount = 4      // 4 parallel MongoDB workers
	ApplyQueueSize   = 10000  // Buffer 10K entries for async processing
	
	// Recovery
	RecoveryBatchSize = 1000 // Replay in batches of 1000 documents
)

// RecordType defines the type of WAL record (like RocksDB)
type RecordType byte

const (
	RecordTypeData     RecordType = 0x01 // Incremental data batch
	RecordTypeApplied  RecordType = 0x02 // Mark as applied (for compaction)
	RecordTypeChecksum RecordType = 0xFF // Checksum block marker
)

// EntryStatus represents the state of a WAL entry
type EntryStatus byte

const (
	StatusPending EntryStatus = 0x00 // Written to WAL, not yet applied
	StatusApplied EntryStatus = 0x01 // Successfully applied to MongoDB
)

// Entry represents a single WAL entry
type Entry struct {
	// Header
	EntryID    uint64      // Monotonic sequence number
	Timestamp  int64       // Unix timestamp (nanoseconds)
	RecordType RecordType  // Type of record
	Status     EntryStatus // Current status
	
	// Payload
	Database   string // Target database
	Collection string // Target collection
	Documents  []byte // Serialized documents (BSON array)
	
	// Checksum (CRC32C like Kafka, RocksDB)
	CRC32 uint32 // Checksum of entire entry
}

// WAL errors
var (
	ErrWALClosed       = errors.New("WAL is closed")
	ErrCorruptedEntry  = errors.New("corrupted WAL entry (checksum mismatch)")
	ErrSegmentTooLarge = errors.New("segment size exceeded")
	ErrInvalidEntry    = errors.New("invalid entry format")
)

// Stats holds WAL statistics
type Stats struct {
	TotalEntries   uint64 // Total entries written
	PendingEntries uint64 // Entries not yet applied
	AppliedEntries uint64 // Entries successfully applied
	TotalSegments  int    // Number of segment files
	TotalBytes     int64  // Total WAL size in bytes
	OldestEntry    uint64 // Oldest unapplied entry ID
	NewestEntry    uint64 // Newest entry ID
	LastCompaction time.Time
}
