package wal

import (
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

// WAL implements a write-ahead log with group commit (Kafka-style)
type WAL struct {
	directory      string
	activeSegment  *Segment
	segmentNum     uint64
	nextEntryID    uint64
	
	// Group commit (batching for performance)
	commitQueue    chan *commitRequest
	commitTimer    *time.Timer
	pendingCommits []*commitRequest
	commitMu       sync.Mutex
	
	// Compaction
	compactionTicker *time.Ticker
	compactionDone   chan struct{}
	
	// Stats
	stats      Stats
	statsMu    sync.RWMutex
	
	// Lifecycle
	closed     atomic.Bool
	wg         sync.WaitGroup
}

// commitRequest represents a pending write waiting for group commit
type commitRequest struct {
	entry    *Entry
	resultCh chan error
}

// New creates a new WAL instance
func New(directory string) (*WAL, error) {
	// Create WAL directory if not exists
	if err := os.MkdirAll(directory, 0755); err != nil {
		return nil, fmt.Errorf("create WAL directory: %w", err)
	}
	
	wal := &WAL{
		directory:        directory,
		commitQueue:      make(chan *commitRequest, GroupCommitMaxEntries),
		commitTimer:      time.NewTimer(GroupCommitMaxDelay),
		pendingCommits:   make([]*commitRequest, 0, GroupCommitMaxEntries),
		compactionTicker: time.NewTicker(CompactionInterval),
		compactionDone:   make(chan struct{}),
	}
	
	// Recover from existing segments
	if err := wal.recover(); err != nil {
		return nil, fmt.Errorf("recover WAL: %w", err)
	}
	
	// Start group commit worker
	wal.wg.Add(1)
	go wal.groupCommitWorker()
	
	// Start compaction worker
	wal.wg.Add(1)
	go wal.compactionWorker()
	
	log.Printf("✅ WAL INITIALIZED: directory=%s, nextEntryID=%d, segments=%d", 
		directory, wal.nextEntryID, wal.stats.TotalSegments)
	
	return wal, nil
}

// Append adds an entry to the WAL (returns after durable write)
func (w *WAL) Append(database, collection string, documents []byte) (uint64, error) {
	if w.closed.Load() {
		return 0, ErrWALClosed
	}
	
	// Create entry
	entryID := atomic.AddUint64(&w.nextEntryID, 1)
	entry := &Entry{
		EntryID:    entryID,
		Timestamp:  time.Now().UnixNano(),
		RecordType: RecordTypeData,
		Status:     StatusPending,
		Database:   database,
		Collection: collection,
		Documents:  documents,
	}
	
	// Send to group commit queue
	req := &commitRequest{
		entry:    entry,
		resultCh: make(chan error, 1),
	}
	
	select {
	case w.commitQueue <- req:
		// Wait for commit result
		err := <-req.resultCh
		if err != nil {
			return 0, err
		}
		return entryID, nil
	case <-time.After(5 * time.Second):
		return 0, fmt.Errorf("commit queue full (timeout)")
	}
}

// MarkApplied marks an entry as applied (for compaction)
func (w *WAL) MarkApplied(entryID uint64) error {
	if w.closed.Load() {
		return ErrWALClosed
	}
	
	// Create marker entry
	entry := &Entry{
		EntryID:    atomic.AddUint64(&w.nextEntryID, 1),
		Timestamp:  time.Now().UnixNano(),
		RecordType: RecordTypeApplied,
		Status:     StatusApplied,
	}
	
	// Append to WAL (marks previous entry as applied)
	req := &commitRequest{
		entry:    entry,
		resultCh: make(chan error, 1),
	}
	
	select {
	case w.commitQueue <- req:
		return <-req.resultCh
	case <-time.After(5 * time.Second):
		return fmt.Errorf("commit queue full (timeout)")
	}
}

// groupCommitWorker batches writes and commits together (like Kafka)
func (w *WAL) groupCommitWorker() {
	defer w.wg.Done()
	
	for {
		select {
		case req := <-w.commitQueue:
			// Add to pending batch
			w.commitMu.Lock()
			w.pendingCommits = append(w.pendingCommits, req)
			batchSize := len(w.pendingCommits)
			w.commitMu.Unlock()
			
			// Flush if batch full
			if batchSize >= GroupCommitMaxEntries {
				w.commitTimer.Reset(GroupCommitMaxDelay)
				w.flushPendingCommits()
			}
			
		case <-w.commitTimer.C:
			// Timeout - flush pending commits
			w.flushPendingCommits()
			w.commitTimer.Reset(GroupCommitMaxDelay)
			
		case <-w.compactionDone:
			// Flush before shutdown
			w.flushPendingCommits()
			return
		}
	}
}

// flushPendingCommits writes all pending entries in one fsync
func (w *WAL) flushPendingCommits() {
	w.commitMu.Lock()
	batch := w.pendingCommits
	w.pendingCommits = make([]*commitRequest, 0, GroupCommitMaxEntries)
	w.commitMu.Unlock()
	
	if len(batch) == 0 {
		return
	}
	
	// Write all entries to active segment
	var err error
	for _, req := range batch {
		if err == nil {
			// Check if segment rotation needed
			if w.activeSegment != nil && w.activeSegment.Size() >= MaxSegmentSize {
				if rotateErr := w.rotateSegment(); rotateErr != nil {
					err = rotateErr
					break
				}
			}
			
			// Append to segment
			if appendErr := w.activeSegment.Append(req.entry); appendErr != nil {
				err = appendErr
				break
			}
		}
	}
	
	// Single fsync for entire batch (group commit!)
	if err == nil && w.activeSegment != nil {
		err = w.activeSegment.Sync()
	}
	
	// Update stats
	if err == nil {
		w.statsMu.Lock()
		w.stats.TotalEntries += uint64(len(batch))
		w.stats.PendingEntries += uint64(len(batch))
		w.stats.NewestEntry = batch[len(batch)-1].entry.EntryID
		w.statsMu.Unlock()
	}
	
	// Send results to all requests
	for _, req := range batch {
		req.resultCh <- err
		close(req.resultCh)
	}
	
	if err == nil {
		log.Printf("🔥 GROUP COMMIT: %d entries fsynced in single batch", len(batch))
	} else {
		log.Printf("⚠️  GROUP COMMIT FAILED: %v", err)
	}
}

// rotateSegment creates a new segment when current one is full
func (w *WAL) rotateSegment() error {
	// Close old segment
	if w.activeSegment != nil {
		if err := w.activeSegment.Close(); err != nil {
			return fmt.Errorf("close segment: %w", err)
		}
	}
	
	// Create new segment
	w.segmentNum++
	newSegment, err := NewSegment(w.directory, w.segmentNum)
	if err != nil {
		return fmt.Errorf("create segment: %w", err)
	}
	
	w.activeSegment = newSegment
	
	w.statsMu.Lock()
	w.stats.TotalSegments++
	w.statsMu.Unlock()
	
	log.Printf("🔄 SEGMENT ROTATED: new segment %d created", w.segmentNum)
	
	return nil
}

// recover loads existing WAL segments and rebuilds state
func (w *WAL) recover() error {
	// Find all segment files
	files, err := filepath.Glob(filepath.Join(w.directory, "*.wal"))
	if err != nil {
		return fmt.Errorf("list segments: %w", err)
	}
	
	if len(files) == 0 {
		// No existing WAL - create first segment
		w.segmentNum = 0
		w.nextEntryID = 1
		
		segment, err := NewSegment(w.directory, 0)
		if err != nil {
			return fmt.Errorf("create first segment: %w", err)
		}
		
		w.activeSegment = segment
		w.stats.TotalSegments = 1
		
		log.Printf("🆕 FRESH WAL: No existing segments, created segment 0")
		return nil
	}
	
	// Sort segments by number
	sort.Strings(files)
	
	log.Printf("🔍 RECOVERING WAL: Found %d segment files", len(files))
	
	// Read all segments to rebuild state
	var maxEntryID uint64
	var totalEntries uint64
	var pendingEntries uint64
	
	for _, file := range files {
		entries, err := ReadSegment(file)
		if err != nil {
			return fmt.Errorf("read segment %s: %w", file, err)
		}
		
		for _, entry := range entries {
			if entry.EntryID > maxEntryID {
				maxEntryID = entry.EntryID
			}
			totalEntries++
			if entry.Status == StatusPending {
				pendingEntries++
			}
		}
	}
	
	// Set next entry ID
	w.nextEntryID = maxEntryID + 1
	
	// Get last segment number from filename
	lastFile := files[len(files)-1]
	var lastSegmentNum uint64
	_, err = fmt.Sscanf(filepath.Base(lastFile), "%020d.wal", &lastSegmentNum)
	if err != nil {
		return fmt.Errorf("parse segment number: %w", err)
	}
	
	w.segmentNum = lastSegmentNum
	
	// Open last segment for appending
	segment, err := NewSegment(w.directory, lastSegmentNum)
	if err != nil {
		return fmt.Errorf("open last segment: %w", err)
	}
	w.activeSegment = segment
	
	// Update stats
	w.stats = Stats{
		TotalEntries:   totalEntries,
		PendingEntries: pendingEntries,
		TotalSegments:  len(files),
		NewestEntry:    maxEntryID,
	}
	
	log.Printf("✅ WAL RECOVERED: %d entries (%d pending), next ID=%d", 
		totalEntries, pendingEntries, w.nextEntryID)
	
	return nil
}

// compactionWorker removes applied entries (like Kafka log retention)
func (w *WAL) compactionWorker() {
	defer w.wg.Done()
	
	for {
		select {
		case <-w.compactionTicker.C:
			w.compact()
			
		case <-w.compactionDone:
			return
		}
	}
}

// compact removes old segments with all applied entries
func (w *WAL) compact() {
	// TODO: Implement compaction logic
	// For now, just log that compaction is running
	log.Printf("🧹 COMPACTION: Checking for segments to compact...")
}

// GetPendingEntries returns all pending (unapplied) entries
func (w *WAL) GetPendingEntries() ([]*Entry, error) {
	files, err := filepath.Glob(filepath.Join(w.directory, "*.wal"))
	if err != nil {
		return nil, fmt.Errorf("list segments: %w", err)
	}
	
	sort.Strings(files)
	
	var pending []*Entry
	for _, file := range files {
		entries, err := ReadSegment(file)
		if err != nil {
			return nil, fmt.Errorf("read segment %s: %w", file, err)
		}
		
		for _, entry := range entries {
			if entry.RecordType == RecordTypeData && entry.Status == StatusPending {
				pending = append(pending, entry)
			}
		}
	}
	
	return pending, nil
}

// Stats returns current WAL statistics
func (w *WAL) Stats() Stats {
	w.statsMu.RLock()
	defer w.statsMu.RUnlock()
	return w.stats
}

// Close closes the WAL and flushes all pending writes
func (w *WAL) Close() error {
	if !w.closed.CompareAndSwap(false, true) {
		return nil // Already closed
	}
	
	log.Printf("🛑 CLOSING WAL...")
	
	// Stop workers
	w.compactionTicker.Stop()
	close(w.compactionDone)
	
	// Wait for workers to finish
	w.wg.Wait()
	
	// Close active segment
	if w.activeSegment != nil {
		if err := w.activeSegment.Close(); err != nil {
			return fmt.Errorf("close active segment: %w", err)
		}
	}
	
	log.Printf("✅ WAL CLOSED: %d total entries, %d pending", 
		w.stats.TotalEntries, w.stats.PendingEntries)
	
	return nil
}
